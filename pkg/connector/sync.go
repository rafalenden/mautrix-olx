package connector

import (
	"context"
	"errors"
	"fmt"
	"html"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"

	"github.com/rafalenden/mautrix-olx/pkg/olx"
)

func (c *Client) pollInterval() time.Duration {
	if seconds := c.main.Config.PollInterval; seconds >= 5 {
		return time.Duration(seconds) * time.Second
	}
	return 15 * time.Second
}

func (c *Client) pollLoop(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	log := zerolog.Ctx(ctx)
	var failureDelay time.Duration
	connected := false
	for {
		delay := c.pollInterval()
		err := c.poll(ctx)
		switch {
		case err == nil:
			failureDelay = 0
			if !connected {
				connected = true
				c.login.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})
			}
		case ctx.Err() != nil:
			return
		case errors.Is(err, olx.ErrSignedOut):
			log.Warn().Err(err).Msg("OLX session ended")
			c.reportSignedOut(err)
			return
		default:
			log.Warn().Err(err).Msg("Couldn't check OLX for messages")
			connected = false
			failureDelay = retryDelay(err, failureDelay)
			delay = failureDelay
			c.login.BridgeState.Send(status.BridgeState{
				StateEvent: status.StateTransientDisconnect,
				Error:      "olx-unreachable",
				Message:    "Can't reach OLX right now",
				Info:       map[string]any{"go_error": err.Error()},
			})
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// poll brings in whatever changed since the last one. The thread list is one
// request, so threads whose counts haven't moved cost nothing further.
func (c *Client) poll(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	threads, err := c.api.ListThreads(ctx)
	if err != nil {
		return err
	}
	updates := map[string]ThreadProgress{}
	defer func() { c.setProgress(ctx, updates) }()
	c.fixOfferLinksOnce(ctx, threads)
	c.rewriteOffersIfLanguageChanged(ctx, threads)
	for _, thread := range threads {
		key := thread.Key()
		previous, known := c.progress(key)
		c.updateRoomInfoIfChanged(ctx, thread)
		messagesChanged := !known || previous.TotalCount != thread.TotalCount ||
			(thread.LatestKey != "" && networkid.MessageID(thread.LatestKey) != previous.LastMessage)
		readChanged := thread.ReadByThem != "" && networkid.MessageID(thread.ReadByThem) != previous.ReadByThem
		// The newest message of yours that hasn't been marked delivered, wherever
		// it came from: sent on OLX itself, or history brought in with the chat.
		// One they've read needs no mark.
		deliveredChanged := thread.LatestOwn != "" && thread.LatestOwn != thread.ReadByThem &&
			networkid.MessageID(thread.LatestOwn) != previous.Delivered
		// OLX can list an offer a moment before marking it as one, and it's then
		// bridged as plain words. So once a thread has settled, their newest offer
		// is checked: bridged with its link, or given it now.
		offer := thread.LatestTheirOffer
		offerUnchecked := !messagesChanged && offer != nil && networkid.MessageID(offer.Key()) != previous.LinkedOffer
		statesDue := c.offerStatesDue(thread, previous, messagesChanged)
		if !messagesChanged && !readChanged && !deliveredChanged && !offerUnchecked && !statesDue && previous.UnreadCount == thread.UnreadCount {
			continue
		}
		next := previous
		next.TotalCount, next.UnreadCount = thread.TotalCount, thread.UnreadCount
		states := previous.OfferStates
		if statesDue {
			if states, err = c.offerStates(ctx, thread, previous.OfferStates); errors.Is(err, olx.ErrSignedOut) || ctx.Err() != nil {
				return err
			}
			next.OfferStates = states
		}
		withStates(thread.Offers, states)
		// The offers bridged in this poll, which carry their state from the start.
		bridgedNow := map[string]bool{}
		if messagesChanged {
			messages, err := c.api.ListMessages(ctx, key)
			if errors.Is(err, olx.ErrSignedOut) || ctx.Err() != nil {
				return err
			} else if err != nil {
				// One bad thread shouldn't hold up the others. It's tried again next time.
				zerolog.Ctx(ctx).Warn().Err(err).Str("thread", key).Msg("Couldn't read OLX thread")
				continue
			}
			sortMessages(messages, c.location)
			withStates(messages, states)
			fresh, ok := newSince(messages, previous.LastMessage)
			if !known || !ok {
				fresh = messages
			}
			for _, message := range fresh {
				bridgedNow[message.Key()] = true
			}
			c.queueMessages(thread, previous, known, messages)
			if len(messages) > 0 {
				next.LastMessage = networkid.MessageID(messages[len(messages)-1].Key())
			}
		}
		// An offer bridged earlier is edited once something has become of it.
		for _, message := range thread.Offers {
			if message.OfferState != previous.OfferStates[message.Key()] && !bridgedNow[message.Key()] &&
				(message.OfferState.Settled() || previous.OfferStates[message.Key()].Settled()) {
				c.queueOfferEdit(thread, message)
				bridgedNow[message.Key()] = true
			}
		}
		if known && previous.UnreadCount > 0 && thread.UnreadCount == 0 && next.LastMessage != "" {
			c.queueReadOnOLX(thread, next.LastMessage)
		}
		if deliveredChanged {
			// Queued after the thread's messages, so the one it's about is there.
			c.queueDelivered(c.portalKey(key), c.sender(olx.MessageReceived, thread), networkid.MessageID(thread.LatestOwn))
			next.Delivered = networkid.MessageID(thread.LatestOwn)
		}
		if offerUnchecked {
			offer.OfferState = states[offer.Key()]
			if _, linked := c.linked.Load(offer.Key()); !linked && !bridgedNow[offer.Key()] {
				c.queueOfferEdit(thread, *offer)
			}
			next.LinkedOffer = networkid.MessageID(offer.Key())
		}
		if readChanged {
			c.queueReadByThem(thread)
			next.ReadByThem = networkid.MessageID(thread.ReadByThem)
		}
		updates[key] = next
	}
	return nil
}

// How often to ask again about offers that are still waiting for an answer. One
// can be accepted or replaced without the chat itself changing.
const offerStateInterval = 5 * time.Minute

// offerStatesDue is whether to ask OLX what became of a thread's offers: when
// its messages change, as a new offer replaces the one before, and now and then
// while any of them is still waiting.
func (c *Client) offerStatesDue(thread olx.Thread, previous ThreadProgress, messagesChanged bool) bool {
	waiting := slices.ContainsFunc(thread.Offers, func(offer olx.Message) bool {
		return offer.NegotiationID != "" && !previous.OfferStates[offer.Key()].Settled()
	})
	if !waiting {
		return false
	}
	checked, ok := c.offersChecked[thread.Key()]
	return messagesChanged || !ok || time.Since(checked) >= offerStateInterval
}

// offerStates asks what became of a thread's offers, and returns it along with
// what was known before. A negotiation that can't be read keeps what was known,
// and is asked about again later: the chat matters more than the labels on it.
func (c *Client) offerStates(ctx context.Context, thread olx.Thread, known map[string]olx.OfferState) (map[string]olx.OfferState, error) {
	if c.offersChecked == nil {
		c.offersChecked = map[string]time.Time{}
	}
	c.offersChecked[thread.Key()] = time.Now()
	states := map[string]olx.OfferState{}
	for id, state := range known {
		states[id] = state
	}
	asked := map[string]bool{}
	for _, offer := range thread.Offers {
		if offer.NegotiationID == "" || asked[offer.NegotiationID] || known[offer.Key()].Settled() {
			continue
		}
		asked[offer.NegotiationID] = true
		found, err := c.api.OfferStates(ctx, offer.NegotiationID)
		if errors.Is(err, olx.ErrSignedOut) || ctx.Err() != nil {
			return states, err
		} else if err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Str("thread", thread.Key()).Msg("Couldn't read what became of an OLX offer")
			continue
		}
		for id, state := range found {
			states[id] = state
		}
	}
	return states, nil
}

// withStates fills in what became of each offer among the messages.
func withStates(messages []olx.Message, states map[string]olx.OfferState) {
	for index := range messages {
		if messages[index].IsOffer {
			messages[index].OfferState = states[messages[index].Key()]
		}
	}
}

// sortMessages puts messages oldest first, because the API doesn't promise an order.
func sortMessages(messages []olx.Message, location *time.Location) {
	slices.SortStableFunc(messages, func(a, b olx.Message) int {
		return olx.ParseTime(a.CreatedAt, location).Compare(olx.ParseTime(b.CreatedAt, location))
	})
}

// newSince returns the messages after the one with the given ID. It reports false
// if that message isn't in the list, in which case they can't be told apart.
func newSince(messages []olx.Message, last networkid.MessageID) ([]olx.Message, bool) {
	if last == "" {
		return nil, false
	}
	index := slices.IndexFunc(messages, func(m olx.Message) bool { return networkid.MessageID(m.Key()) == last })
	if index < 0 {
		return nil, false
	}
	return messages[index+1:], true
}

// queueMessages delivers new messages live, so they count as new in the client.
// A thread seen for the first time goes through a chat resync instead, which
// creates its room and brings in its history.
func (c *Client) queueMessages(thread olx.Thread, previous ThreadProgress, known bool, messages []olx.Message) {
	if len(messages) == 0 {
		return
	}
	fresh, ok := newSince(messages, previous.LastMessage)
	if !known || !ok {
		c.queueResync(thread, messages)
		return
	}
	for _, message := range fresh {
		timestamp := olx.ParseTime(message.CreatedAt, c.location)
		c.main.Bridge.QueueRemoteEvent(c.login, &simplevent.Message[olx.Message]{
			EventMeta: simplevent.EventMeta{
				Type:         bridgev2.RemoteEventMessage,
				PortalKey:    c.portalKey(thread.Key()),
				Sender:       c.messageSender(message, thread),
				CreatePortal: true,
				Timestamp:    timestamp,
				StreamOrder:  timestamp.UnixMilli(),
			},
			Data: message,
			ID:   networkid.MessageID(message.Key()),
			ConvertMessageFunc: func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data olx.Message) (*bridgev2.ConvertedMessage, error) {
				return c.convertMessage(ctx, portal, intent, data), nil
			},
		})
	}
}

func (c *Client) queueResync(thread olx.Thread, messages []olx.Message) {
	latest := messages[len(messages)-1]
	c.main.Bridge.QueueRemoteEvent(c.login, &simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventChatResync,
			PortalKey:    c.portalKey(thread.Key()),
			CreatePortal: true,
		},
		GetChatInfoFunc: func(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
			return c.chatInfo(ctx, thread), nil
		},
		LatestMessageTS: olx.ParseTime(latest.CreatedAt, c.location),
		CheckNeedsBackfillFunc: func(ctx context.Context, latestBridged *database.Message) (bool, error) {
			return latestBridged == nil || latestBridged.ID != networkid.MessageID(latest.Key()), nil
		},
		BundledBackfillData: &backfillBundle{thread: thread, messages: messages},
	})
}

// queueReadOnOLX reflects a chat being read on OLX itself.
func (c *Client) queueReadOnOLX(thread olx.Thread, last networkid.MessageID) {
	c.main.Bridge.QueueRemoteEvent(c.login, &simplevent.Receipt{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventReadReceipt,
			PortalKey: c.portalKey(thread.Key()),
			Sender:    c.sender(olx.MessageSent, thread),
			Timestamp: time.Now(),
		},
		LastTarget: last,
	})
}

// queueReadByThem passes on the other person reading your messages, as their
// read receipt.
func (c *Client) queueReadByThem(thread olx.Thread) {
	readAt := olx.ParseTime(thread.ReadByThemAt, time.UTC)
	if readAt.IsZero() {
		readAt = time.Now()
	}
	c.main.Bridge.QueueRemoteEvent(c.login, &simplevent.Receipt{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventReadReceipt,
			PortalKey: c.portalKey(thread.Key()),
			Sender:    c.sender(olx.MessageReceived, thread),
			Timestamp: readAt,
		},
		LastTarget: networkid.MessageID(thread.ReadByThem),
		ReadUpTo:   readAt,
	})
}

// queueDelivered marks a message of yours as delivered. OLX has no devices to
// reach: a message it has accepted is in the other person's inbox. Saying so
// lets every Matrix client show it as delivered rather than only sent.
func (c *Client) queueDelivered(portal networkid.PortalKey, to bridgev2.EventSender, message networkid.MessageID) {
	if to.Sender == "" || message == "" {
		return
	}
	c.main.Bridge.QueueRemoteEvent(c.login, &simplevent.Receipt{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventDeliveryReceipt,
			PortalKey: portal,
			Sender:    to,
			Timestamp: time.Now(),
		},
		Targets: []networkid.MessageID{message},
	})
}

type backfillBundle struct {
	thread   olx.Thread
	messages []olx.Message
}

// FetchMessages supplies history. OLX returns a thread's messages all together,
// so there are never further pages to ask for.
func (c *Client) FetchMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	threadKey := string(params.Portal.ID)
	bundle, _ := params.BundledData.(*backfillBundle)
	if bundle == nil {
		thread, err := c.api.GetThread(ctx, threadKey)
		if err != nil {
			return nil, err
		}
		messages, err := c.api.ListMessages(ctx, threadKey)
		if err != nil {
			return nil, err
		}
		sortMessages(messages, c.location)
		progress, _ := c.progress(threadKey)
		withStates(messages, progress.OfferStates)
		bundle = &backfillBundle{thread: *thread, messages: messages}
	}
	selected := selectForBackfill(bundle.messages, params.AnchorMessage, params.Forward, c.location)
	if params.Count > 0 && len(selected) > params.Count {
		selected = selected[len(selected)-params.Count:]
	}
	response := &bridgev2.FetchMessagesResponse{
		Forward:  params.Forward,
		MarkRead: bundle.thread.UnreadCount == 0,
	}
	for _, message := range selected {
		sender := c.messageSender(message, bundle.thread)
		intent, ok := params.Portal.GetIntentFor(ctx, sender, c.login, bridgev2.RemoteEventBackfill)
		if !ok {
			continue
		}
		timestamp := olx.ParseTime(message.CreatedAt, c.location)
		response.Messages = append(response.Messages, &bridgev2.BackfillMessage{
			ConvertedMessage: c.convertMessage(ctx, params.Portal, intent, message),
			Sender:           sender,
			ID:               networkid.MessageID(message.Key()),
			Timestamp:        timestamp,
			StreamOrder:      timestamp.UnixMilli(),
		})
	}
	return response, nil
}

func selectForBackfill(messages []olx.Message, anchor *database.Message, forward bool, location *time.Location) []olx.Message {
	if anchor == nil {
		return messages
	}
	index := slices.IndexFunc(messages, func(m olx.Message) bool { return networkid.MessageID(m.Key()) == anchor.ID })
	if index >= 0 {
		if forward {
			return messages[index+1:]
		}
		return messages[:index]
	}
	return slices.DeleteFunc(slices.Clone(messages), func(m olx.Message) bool {
		timestamp := olx.ParseTime(m.CreatedAt, location)
		if forward {
			return !timestamp.After(anchor.Timestamp)
		}
		return !timestamp.Before(anchor.Timestamp)
	})
}

// convertMessage turns a message into Matrix events: its text, then each file.
// A file that can't be fetched becomes a note saying so, so the message's place
// in the conversation isn't lost.
func (c *Client) convertMessage(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, message olx.Message) *bridgev2.ConvertedMessage {
	converted := &bridgev2.ConvertedMessage{}
	if message.Text != "" {
		part := &bridgev2.ConvertedMessagePart{
			ID:      "text",
			Type:    event.EventMessage,
			Content: &event.MessageEventContent{MsgType: event.MsgText, Body: message.Text},
		}
		if message.IsSystem {
			part.Content.MsgType = event.MsgNotice
		}
		if answerable(message) {
			addRespondLink(part.Content, c.api.ConversationURL(message.ThreadUUID), c.texts.under(message.Text).Respond)
			c.linked.Store(message.Key(), true)
		} else if message.IsOffer && message.OfferState.Settled() {
			addOfferState(part.Content, message.OfferState, c.texts.under(message.Text))
		}
		converted.Parts = append(converted.Parts, part)
	}
	for index, file := range message.Attachments {
		converted.Parts = append(converted.Parts, &bridgev2.ConvertedMessagePart{
			ID:      networkid.PartID(fmt.Sprintf("file%d", index)),
			Type:    event.EventMessage,
			Content: c.convertFile(ctx, portal, intent, file),
		})
	}
	if len(converted.Parts) == 0 {
		converted.Parts = append(converted.Parts, &bridgev2.ConvertedMessagePart{
			ID:      "text",
			Type:    event.EventMessage,
			Content: &event.MessageEventContent{MsgType: event.MsgNotice, Body: c.texts.NoContent},
		})
	}
	return converted
}

func (c *Client) convertFile(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, file olx.Attachment) *event.MessageEventContent {
	failed := &event.MessageEventContent{MsgType: event.MsgNotice, Body: c.texts.attachmentFailed(file.Name)}
	name := file.Name
	if name == "" {
		name = "attachment"
	}
	data, err := c.api.Download(ctx, file.URL)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("Couldn't download OLX attachment")
		return failed
	}
	mimeType := http.DetectContentType(data)
	uploaded, encrypted, err := intent.UploadMedia(ctx, portal.MXID, data, name, mimeType)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("Couldn't upload OLX attachment")
		return failed
	}
	return &event.MessageEventContent{
		MsgType:  msgTypeFor(mimeType),
		Body:     name,
		FileName: name,
		URL:      uploaded,
		File:     encrypted,
		Info:     &event.FileInfo{MimeType: mimeType, Size: len(data)},
	}
}

// answerable is whether a message is an offer for you to answer: the other
// person's, as there's nothing to answer on your own, and one that hasn't been
// accepted, turned down or replaced since.
func answerable(message olx.Message) bool {
	return message.IsOffer && message.Type != olx.MessageSent && message.ThreadUUID != "" && !message.OfferState.Settled()
}

// addOfferState says what became of an offer, in the formatted body: a rejected
// one is struck through, as on OLX's site. The plain body stays OLX's own text,
// so an offer edited when its state changes isn't shown as reworded.
func addOfferState(content *event.MessageEventContent, state olx.OfferState, words texts) {
	text := strings.ReplaceAll(html.EscapeString(content.Body), "\n", "<br>")
	content.Format = event.FormatHTML
	switch state {
	case olx.OfferRejected:
		content.FormattedBody = "<del>" + text + "</del><br><strong>" + html.EscapeString(words.Rejected) + "</strong>"
	case olx.OfferAccepted:
		content.FormattedBody = text + "<br><strong>" + html.EscapeString(words.Accepted) + "</strong>"
	}
}

// addRespondLink lets an offer be answered on OLX, as a link in the formatted body,
// which Matrix clients show. The plain body stays OLX's own text, so adding the link
// to an offer bridged before doesn't change what it says.
func addRespondLink(content *event.MessageEventContent, conversationURL, label string) {
	text := content.Body
	content.Format = event.FormatHTML
	content.FormattedBody = strings.ReplaceAll(html.EscapeString(text), "\n", "<br>") +
		`<br><a href="` + html.EscapeString(conversationURL) + `">` + html.EscapeString(label) + `</a>`
}

// listingTopic is the room topic: the advert, its price and a link to it.
func listingTopic(listing olx.Listing) string {
	var lines []string
	if listing.Title != "" {
		line := listing.Title
		if listing.PriceMinor != nil {
			line += " · " + money(*listing.PriceMinor, listing.Currency)
		}
		lines = append(lines, line)
	}
	if listing.AdvertURL != "" {
		lines = append(lines, listing.AdvertURL)
	}
	return strings.Join(lines, "\n")
}

// money writes a price as OLX does: "1500 zł", or "1299,50 zł" with minor units.
func money(minor int64, currency string) string {
	text := strconv.FormatInt(minor/100, 10)
	if cents := minor % 100; cents != 0 {
		text += fmt.Sprintf(",%02d", cents)
	}
	symbols := map[string]string{"PLN": "zł", "RON": "lei", "BGN": "лв.", "UAH": "грн", "KZT": "₸", "EUR": "€"}
	if symbol, ok := symbols[currency]; ok {
		return text + " " + symbol
	} else if currency != "" {
		return text + " " + currency
	}
	return text
}

// updateRoomInfoIfChanged refreshes a room's name, picture and topic when the advert
// changes, such as a new price. A room that doesn't exist yet gets them when it's created.
func (c *Client) updateRoomInfoIfChanged(ctx context.Context, thread olx.Thread) {
	key := thread.Key()
	current := thread.Listing.Title + "\x00" + listingTopic(thread.Listing) + "\x00" + thread.Listing.ImageURL
	c.metaLock.Lock()
	unchanged := c.meta.RoomInfo[key] == current
	c.metaLock.Unlock()
	if unchanged {
		return
	}
	c.main.Bridge.QueueRemoteEvent(c.login, &simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventChatResync,
			PortalKey: c.portalKey(key),
		},
		GetChatInfoFunc: func(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
			return c.chatInfo(ctx, thread), nil
		},
		CheckNeedsBackfillFunc: func(ctx context.Context, latestBridged *database.Message) (bool, error) {
			return false, nil
		},
	})
	c.metaLock.Lock()
	if c.meta.RoomInfo == nil {
		c.meta.RoomInfo = map[string]string{}
	}
	c.meta.RoomInfo[key] = current
	err := c.login.Save(ctx)
	c.metaLock.Unlock()
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to save OLX room info")
	}
}

// fixOfferLinksOnce edits offers bridged under an earlier rule, once: those from
// before offers carried a link to answer them on OLX gain it, and your own
// offers, which carried one for a while, lose it. Chats bridged from now on
// need neither.
func (c *Client) fixOfferLinksOnce(ctx context.Context, threads []olx.Thread) {
	c.metaLock.Lock()
	added, removed, known := c.meta.OfferLinksAdded, c.meta.OwnOfferLinksRemoved, len(c.meta.Threads) > 0
	c.metaLock.Unlock()
	if added && removed {
		return
	}
	if known {
		for _, thread := range threads {
			if _, bridged := c.progress(thread.Key()); !bridged {
				continue
			}
			messages, err := c.api.ListMessages(ctx, thread.Key())
			if err != nil {
				zerolog.Ctx(ctx).Warn().Err(err).Str("thread", thread.Key()).Msg("Couldn't read OLX thread to fix its offers' links")
				return
			}
			for _, message := range messages {
				if needsLinkFix(message, added) {
					c.queueOfferEdit(thread, message)
				}
			}
		}
	}
	c.metaLock.Lock()
	defer c.metaLock.Unlock()
	c.meta.OfferLinksAdded, c.meta.OwnOfferLinksRemoved = true, true
	if err := c.login.Save(ctx); err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to save OLX offer links")
	}
}

// rewriteOffersIfLanguageChanged edits the offers that carry words of the bridge's
// own, the link to answer one or what became of it, when those were written in
// another language: English, before the bridge wrote in the site's.
func (c *Client) rewriteOffersIfLanguageChanged(ctx context.Context, threads []olx.Thread) {
	c.metaLock.Lock()
	written, known := c.meta.TextLanguage, len(c.meta.Threads) > 0
	c.metaLock.Unlock()
	if written == c.texts.Language {
		return
	}
	// Chats bridged before the language was recorded were written in English.
	if known && (written != "" || c.texts.Language != english.Language) {
		for _, thread := range threads {
			progress, bridged := c.progress(thread.Key())
			if !bridged {
				continue
			}
			withStates(thread.Offers, progress.OfferStates)
			for _, offer := range thread.Offers {
				if answerable(offer) || offer.OfferState.Settled() {
					c.queueOfferEdit(thread, offer)
				}
			}
		}
	}
	c.metaLock.Lock()
	defer c.metaLock.Unlock()
	c.meta.TextLanguage = c.texts.Language
	if err := c.login.Save(ctx); err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to save the language of OLX offers")
	}
}

// needsLinkFix is whether an offer bridged earlier has to be edited: every offer
// if links were never added, otherwise only your own, to take theirs off.
func needsLinkFix(message olx.Message, linksAdded bool) bool {
	if !message.IsOffer {
		return false
	}
	return !linksAdded || message.Type == olx.MessageSent
}

// queueOfferEdit edits one bridged offer to match what it is now: with its link
// or without, and saying what became of it. A message that was never bridged is
// skipped by the bridge, which finds nothing to edit.
func (c *Client) queueOfferEdit(thread olx.Thread, message olx.Message) {
	id := networkid.MessageID(message.Key())
	c.main.Bridge.QueueRemoteEvent(c.login, &simplevent.Message[olx.Message]{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventEdit,
			PortalKey: c.portalKey(thread.Key()),
			Sender:    c.sender(message.Type, thread),
			Timestamp: time.Now(),
		},
		ID:            id,
		TargetMessage: id,
		Data:          message,
		ConvertEditFunc: func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message, data olx.Message) (*bridgev2.ConvertedEdit, error) {
			converted := c.convertMessage(ctx, portal, intent, data)
			edit := &bridgev2.ConvertedEdit{}
			for _, part := range converted.Parts {
				for _, old := range existing {
					if old.PartID == part.ID && part.ID == "text" {
						edit.ModifiedParts = append(edit.ModifiedParts, part.ToEditPart(old))
					}
				}
			}
			return edit, nil
		},
	})
}

func msgTypeFor(mimeType string) event.MessageType {
	switch {
	case len(mimeType) >= 6 && mimeType[:6] == "image/":
		return event.MsgImage
	case len(mimeType) >= 6 && mimeType[:6] == "video/":
		return event.MsgVideo
	case len(mimeType) >= 6 && mimeType[:6] == "audio/":
		return event.MsgAudio
	default:
		return event.MsgFile
	}
}
