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
	c.addOfferLinksOnce(ctx, threads)
	for _, thread := range threads {
		key := thread.Key()
		previous, known := c.progress(key)
		c.updateRoomInfoIfChanged(ctx, thread)
		messagesChanged := !known || previous.TotalCount != thread.TotalCount ||
			(thread.LatestKey != "" && networkid.MessageID(thread.LatestKey) != previous.LastMessage)
		readChanged := thread.ReadByThem != "" && networkid.MessageID(thread.ReadByThem) != previous.ReadByThem
		if !messagesChanged && !readChanged && previous.UnreadCount == thread.UnreadCount {
			continue
		}
		next := previous
		next.TotalCount, next.UnreadCount = thread.TotalCount, thread.UnreadCount
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
			c.queueMessages(thread, previous, known, messages)
			if len(messages) > 0 {
				next.LastMessage = networkid.MessageID(messages[len(messages)-1].Key())
			}
		}
		if known && previous.UnreadCount > 0 && thread.UnreadCount == 0 && next.LastMessage != "" {
			c.queueReadOnOLX(thread, next.LastMessage)
		}
		if readChanged {
			c.queueReadByThem(thread)
			next.ReadByThem = networkid.MessageID(thread.ReadByThem)
		}
		updates[key] = next
	}
	return nil
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
		// Sent on OLX itself: it's there for them as soon as it's there for you.
		if message.Type == olx.MessageSent && !message.IsSystem {
			c.queueDelivered(c.portalKey(thread.Key()), c.sender(olx.MessageReceived, thread), networkid.MessageID(message.Key()))
		}
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
		if message.IsOffer && message.ThreadUUID != "" {
			addRespondLink(part.Content, c.api.ConversationURL(message.ThreadUUID))
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
			Content: &event.MessageEventContent{MsgType: event.MsgNotice, Body: "This message has no content that can be shown here."},
		})
	}
	return converted
}

func (c *Client) convertFile(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, file olx.Attachment) *event.MessageEventContent {
	name := file.Name
	if name == "" {
		name = "attachment"
	}
	failed := &event.MessageEventContent{MsgType: event.MsgNotice, Body: "Couldn't load the attachment " + name}
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

// addRespondLink lets an offer be answered on OLX, as a link in the formatted body,
// which Matrix clients show. The plain body stays OLX's own text, so adding the link
// to an offer bridged before doesn't change what it says.
func addRespondLink(content *event.MessageEventContent, conversationURL string) {
	text := content.Body
	content.Format = event.FormatHTML
	content.FormattedBody = strings.ReplaceAll(html.EscapeString(text), "\n", "<br>") +
		`<br><a href="` + html.EscapeString(conversationURL) + `">Respond on OLX</a>`
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

// addOfferLinksOnce edits offers bridged before they carried a link to answer them
// on OLX. It runs on the first poll after an update, and again later if it couldn't
// read every thread. The text is unchanged: only the link is added.
func (c *Client) addOfferLinksOnce(ctx context.Context, threads []olx.Thread) {
	c.metaLock.Lock()
	done, known := c.meta.OfferLinksAdded, len(c.meta.Threads) > 0
	c.metaLock.Unlock()
	if !done && known {
		for _, thread := range threads {
			if _, bridged := c.progress(thread.Key()); !bridged {
				continue
			}
			messages, err := c.api.ListMessages(ctx, thread.Key())
			if err != nil {
				zerolog.Ctx(ctx).Warn().Err(err).Str("thread", thread.Key()).Msg("Couldn't read OLX thread to link its offers")
				return
			}
			for _, message := range messages {
				if message.IsOffer {
					c.queueOfferLink(thread, message)
				}
			}
		}
	}
	c.metaLock.Lock()
	defer c.metaLock.Unlock()
	if c.meta.OfferLinksAdded {
		return
	}
	c.meta.OfferLinksAdded = true
	if err := c.login.Save(ctx); err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to save OLX offer links")
	}
}

// queueOfferLink edits one bridged offer to carry its link. A message that was never
// bridged is skipped by the bridge, which finds nothing to edit.
func (c *Client) queueOfferLink(thread olx.Thread, message olx.Message) {
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
