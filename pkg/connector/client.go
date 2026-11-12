package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"

	"github.com/rafalenden/mautrix-olx/pkg/olx"
)

// UserLoginMetadata is what the bridge keeps for an account between runs.
type UserLoginMetadata struct {
	Tokens olx.Tokens `json:"tokens"`
	// Site is the OLX site chosen at sign-in.
	Site string `json:"site,omitempty"`
	// OLX's own client ID for the site, needed to renew the session.
	WebsiteClientID string `json:"website_client_id,omitempty"`
	// Whose messages count as sent.
	UserUUID string `json:"user_uuid,omitempty"`
	// Threads records how far each thread has been bridged, so a restart only
	// fetches the ones that changed while the bridge was off.
	Threads map[string]ThreadProgress `json:"threads,omitempty"`
	// RoomInfo is what each room was last given as its name, picture and topic, so
	// a change on OLX, such as a new price, reaches the room.
	RoomInfo map[string]string `json:"room_info,omitempty"`
	// OfferLinksAdded records that offers bridged before they carried a link to
	// answer them on OLX have been edited to carry one.
	OfferLinksAdded bool `json:"offer_links_added,omitempty"`
	// OwnOfferLinksRemoved records that your own offers, which carried that link
	// for a while though there's nothing to answer on them, have lost it.
	OwnOfferLinksRemoved bool `json:"own_offer_links_removed,omitempty"`
}

type ThreadProgress struct {
	TotalCount  int                 `json:"total_count"`
	UnreadCount int                 `json:"unread_count"`
	LastMessage networkid.MessageID `json:"last_message,omitempty"`
	// ReadByThem is the newest of your messages the other person has been
	// reported to have read.
	ReadByThem networkid.MessageID `json:"read_by_them,omitempty"`
	// Delivered is the newest of your messages that's been marked delivered.
	Delivered networkid.MessageID `json:"delivered,omitempty"`
	// LinkedOffer is the other person's newest offer known to carry its link.
	LinkedOffer networkid.MessageID `json:"linked_offer,omitempty"`
}

// Client is one signed-in OLX account.
type Client struct {
	main  *OLXConnector
	login *bridgev2.UserLogin
	api   *olx.Client
	// The time zone of the site's timestamps.
	location *time.Location

	// Held while talking to OLX about messages, so a poll and a send can't overlap.
	lock sync.Mutex
	// Guards meta against token refreshes, which can happen during any request.
	metaLock sync.Mutex
	meta     *UserLoginMetadata

	// Offers given their link since the bridge started, by message ID.
	linked sync.Map

	stop context.CancelFunc
	done chan struct{}
}

var (
	_ bridgev2.NetworkAPI                    = (*Client)(nil)
	_ bridgev2.BackfillingNetworkAPI         = (*Client)(nil)
	_ bridgev2.ReadReceiptHandlingNetworkAPI = (*Client)(nil)
)

func newClient(main *OLXConnector, login *bridgev2.UserLogin) (*Client, error) {
	meta := login.Metadata.(*UserLoginMetadata)
	address := meta.Site
	if main.Config.Site != "" {
		address = main.Config.Site
	} else if address == "" {
		address = defaultSite
	}
	site, err := olx.SiteURL(address)
	if err != nil {
		return nil, err
	}
	client := &Client{main: main, login: login, meta: meta, location: siteLocation(address)}
	client.api = olx.NewClient(site, meta.WebsiteClientID, meta.UserUUID, meta.Tokens)
	client.api.OnTokens = client.saveTokens
	return client, nil
}

func (c *Client) saveTokens(tokens olx.Tokens) {
	c.metaLock.Lock()
	defer c.metaLock.Unlock()
	c.meta.Tokens = tokens
	if err := c.login.Save(c.main.Bridge.BackgroundCtx); err != nil {
		c.login.Log.Err(err).Msg("Failed to save refreshed OLX tokens")
	}
}

func (c *Client) progress(key string) (ThreadProgress, bool) {
	c.metaLock.Lock()
	defer c.metaLock.Unlock()
	progress, ok := c.meta.Threads[key]
	return progress, ok
}

func (c *Client) setProgress(ctx context.Context, updates map[string]ThreadProgress) {
	if len(updates) == 0 {
		return
	}
	c.metaLock.Lock()
	defer c.metaLock.Unlock()
	if c.meta.Threads == nil {
		c.meta.Threads = map[string]ThreadProgress{}
	}
	for key, progress := range updates {
		c.meta.Threads[key] = progress
	}
	if err := c.login.Save(ctx); err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to save OLX sync progress")
	}
}

// Connect starts polling for new messages.
func (c *Client) Connect(ctx context.Context) {
	c.Disconnect()
	c.login.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnecting})
	loopCtx, cancel := context.WithCancel(c.login.Log.WithContext(c.main.Bridge.BackgroundCtx))
	c.stop = cancel
	c.done = make(chan struct{})
	go c.pollLoop(loopCtx, c.done)
}

func (c *Client) Disconnect() {
	if c.stop == nil {
		return
	}
	c.stop()
	<-c.done
	c.stop, c.done = nil, nil
}

// LogoutRemote only forgets the tokens here.
func (c *Client) LogoutRemote(ctx context.Context) {
	c.Disconnect()
}

func (c *Client) IsLoggedIn() bool {
	tokens := c.api.Tokens()
	return tokens.AccessToken != "" || tokens.RefreshToken != ""
}

func (c *Client) IsThisUser(ctx context.Context, userID networkid.UserID) bool {
	return string(userID) == string(c.login.ID)
}

// The kinds of file OLX's site lets you attach, with a caption as the message's
// words. Its site shrinks pictures over 5 MB itself, which the bridge doesn't,
// so that's the most it takes.
var (
	sendablePictures = &event.FileFeatures{
		MimeTypes: map[string]event.CapabilitySupportLevel{
			"image/jpeg": event.CapLevelFullySupported,
			"image/png":  event.CapLevelFullySupported,
		},
		Caption: event.CapLevelFullySupported,
		MaxSize: maxUploadSize,
	}
	sendableFiles = &event.FileFeatures{
		MimeTypes: map[string]event.CapabilitySupportLevel{
			"image/jpeg":               event.CapLevelFullySupported,
			"image/png":                event.CapLevelFullySupported,
			"application/pdf":          event.CapLevelFullySupported,
			"application/msword":       event.CapLevelFullySupported,
			"application/vnd.ms-excel": event.CapLevelFullySupported,
			"application/vnd.openxmlformats-officedocument.wordprocessingml.document": event.CapLevelFullySupported,
			"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":       event.CapLevelFullySupported,
		},
		Caption: event.CapLevelFullySupported,
		MaxSize: maxUploadSize,
	}
)

const maxUploadSize = 5 * 1000 * 1000

var roomFeatures = &event.RoomFeatures{
	ID:           "com.enden.servuso.olx.capabilities.2026_11_12",
	ReadReceipts: true,
	File: event.FileFeatureMap{
		event.MsgImage: sendablePictures,
		event.MsgFile:  sendableFiles,
	},
}

func (c *Client) GetCapabilities(ctx context.Context, portal *bridgev2.Portal) *event.RoomFeatures {
	return roomFeatures
}

func (c *Client) GetUserInfo(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.UserInfo, error) {
	// This account's own ID is a UUID, unlike the numbers other people have.
	if c.IsThisUser(ctx, networkid.UserID(ghost.ID)) {
		return &bridgev2.UserInfo{Name: &c.login.RemoteName}, nil
	}
	id, err := strconv.ParseInt(string(ghost.ID), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid OLX user ID %q: %w", ghost.ID, err)
	}
	user, err := c.api.GetUser(ctx, id)
	if olx.IsNotFound(err) {
		return &bridgev2.UserInfo{}, nil
	} else if err != nil {
		return nil, err
	}
	info := &bridgev2.UserInfo{Identifiers: []string{"olx:" + string(ghost.ID)}}
	if user.Name != "" {
		info.Name = &user.Name
	}
	return info, nil
}

func (c *Client) GetChatInfo(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	thread, err := c.api.GetThread(ctx, string(portal.ID))
	if err != nil {
		return nil, err
	}
	return c.chatInfo(ctx, *thread), nil
}

// chatInfo describes a thread as a DM. Several threads can be with the same person
// about different adverts, so the room is named after the advert as well and
// has its photo as its picture, as on OLX.
func (c *Client) chatInfo(ctx context.Context, thread olx.Thread) *bridgev2.ChatInfo {
	otherID := networkid.UserID(strconv.FormatInt(thread.InterlocutorID, 10))
	roomType := database.RoomTypeDM
	info := &bridgev2.ChatInfo{
		Type:                       &roomType,
		CanBackfill:                true,
		ExcludeChangesFromTimeline: true,
		Members: &bridgev2.ChatMemberList{
			IsFull:      true,
			OtherUserID: otherID,
			MemberMap: bridgev2.ChatMemberMap{}.
				Set(bridgev2.ChatMember{EventSender: c.sender(olx.MessageSent, thread)}).
				Set(bridgev2.ChatMember{EventSender: c.sender(olx.MessageReceived, thread)}),
		},
	}
	if url := thread.Listing.ImageURL; url != "" {
		info.Avatar = &bridgev2.Avatar{
			ID:  networkid.AvatarID(url),
			Get: func(ctx context.Context) ([]byte, error) { return c.api.Download(ctx, url) },
		}
	}
	if topic := listingTopic(thread.Listing); topic != "" {
		info.Topic = &topic
	}
	if title := thread.Listing.Title; title != "" {
		name := title
		if user, err := c.api.GetUser(ctx, thread.InterlocutorID); err == nil && user.Name != "" {
			name = user.Name + " · " + title
		}
		info.Name = &name
		// Otherwise the bridge keeps renaming the DM after the other person.
		info.ExtraUpdates = func(ctx context.Context, portal *bridgev2.Portal) bool {
			changed := !portal.NameIsCustom
			portal.NameIsCustom = true
			return changed
		}
	}
	return info
}

// messageSender is who a message comes from in Matrix. OLX's own lines, such as a
// price cut, come from the bridge bot rather than from the other person, so every
// client shows them as an automated notice.
func (c *Client) messageSender(message olx.Message, thread olx.Thread) bridgev2.EventSender {
	if message.IsSystem {
		return bridgev2.EventSender{}
	}
	return c.sender(message.Type, thread)
}

func (c *Client) sender(kind olx.MessageType, thread olx.Thread) bridgev2.EventSender {
	if kind == olx.MessageSent {
		return bridgev2.EventSender{
			IsFromMe:    true,
			Sender:      networkid.UserID(c.login.ID),
			SenderLogin: c.login.ID,
		}
	}
	return bridgev2.EventSender{Sender: networkid.UserID(strconv.FormatInt(thread.InterlocutorID, 10))}
}

func (c *Client) portalKey(threadKey string) networkid.PortalKey {
	return networkid.PortalKey{ID: networkid.PortalID(threadKey), Receiver: c.login.ID}
}

// HandleMatrixMessage sends text to OLX. The poll that later sees this message
// finds its ID already stored and skips it.
func (c *Client) HandleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	var text string
	var file *event.MessageEventContent
	switch msg.Content.MsgType {
	case event.MsgText, event.MsgNotice:
		text = msg.Content.Body
	case event.MsgEmote:
		text = "* " + c.login.RemoteName + " " + msg.Content.Body
	case event.MsgImage, event.MsgFile:
		file = msg.Content
		text = caption(file)
	default:
		return nil, bridgev2.ErrUnsupportedMessageType
	}
	if file == nil && strings.TrimSpace(text) == "" {
		return nil, bridgev2.ErrUnsupportedMessageType
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	threadKey := string(msg.Portal.ID)
	var attachments []olx.SentAttachment
	if file != nil {
		uploaded, err := c.upload(ctx, msg, file)
		if errors.Is(err, olx.ErrSignedOut) {
			c.reportSignedOut(err)
			return nil, errSignedOutSend(err)
		} else if err != nil {
			return nil, err
		}
		attachments = append(attachments, uploaded)
	}
	sending := time.Now()
	sentID, err := c.api.SendMessage(ctx, threadKey, text, attachments...)
	if err != nil {
		if errors.Is(err, olx.ErrSignedOut) {
			c.reportSignedOut(err)
			return nil, errSignedOutSend(err)
		}
		return nil, err
	}
	sent := c.findSent(ctx, threadKey, text, sentID, sending)
	timestamp := olx.ParseTime(sent.CreatedAt, c.location)
	if timestamp.IsZero() {
		timestamp = sending
	}
	id := networkid.MessageID(sent.Key())
	if id == "" {
		// Kept under the ID it was sent with, which OLX appears to keep, so the
		// poll that brings it in recognises it rather than bridging it again.
		zerolog.Ctx(ctx).Warn().Msg("Couldn't find a sent OLX message yet, so it's kept under the ID it was sent with")
		id = networkid.MessageID(sentID)
	}
	// Handled after this message is saved, as the portal takes events in turn.
	c.queueDelivered(msg.Portal.PortalKey, bridgev2.EventSender{Sender: msg.Portal.OtherUserID}, id)
	if progress, known := c.progress(threadKey); known {
		progress.Delivered = id
		c.setProgress(ctx, map[string]ThreadProgress{threadKey: progress})
	}
	return &bridgev2.MatrixMessageResponse{
		DB: &database.Message{
			ID:        id,
			SenderID:  networkid.UserID(c.login.ID),
			Timestamp: timestamp,
		},
	}, nil
}

// caption is the words sent with a file: its body, when that isn't just its name.
func caption(file *event.MessageEventContent) string {
	if file.FileName != "" && file.Body != file.FileName {
		return file.Body
	}
	return ""
}

// upload takes a file from Matrix and puts it on OLX, ready to attach.
func (c *Client) upload(ctx context.Context, msg *bridgev2.MatrixMessage, file *event.MessageEventContent) (olx.SentAttachment, error) {
	name := file.FileName
	if name == "" {
		name = file.Body
	}
	unsendable := func(err error) error {
		return bridgev2.WrapErrorInStatus(err).WithStatus(event.MessageStatusFail).
			WithErrorReason(event.MessageStatusUnsupported).WithMessage("Not sent: " + err.Error() + ".").
			WithIsCertain(true).WithSendNotice(true)
	}
	if !olx.CanSend(name) {
		return olx.SentAttachment{}, unsendable(olx.ErrUnsendableFile)
	}
	data, err := msg.Portal.Bridge.Bot.DownloadMedia(ctx, file.URL, file.File)
	if err != nil {
		return olx.SentAttachment{}, fmt.Errorf("couldn't read the file to send: %w", err)
	}
	if len(data) > maxUploadSize {
		return olx.SentAttachment{}, unsendable(errors.New("OLX takes files up to 5 MB"))
	}
	mimeType := http.DetectContentType(data)
	if file.Info != nil && file.Info.MimeType != "" {
		mimeType = file.Info.MimeType
	}
	return c.api.Upload(ctx, name, mimeType, data)
}

// sentLookupDelays are the waits before each look for a message just sent. OLX's
// list can lag a send by a moment: one was missing half a second after OLX made it.
var sentLookupDelays = []time.Duration{0, 500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second}

// findSent looks up a message just sent, whose POST response doesn't identify it:
// by the ID it was sent with, or else by its words, among messages no older than
// the send, so an earlier message saying the same can't be taken for it.
func (c *Client) findSent(ctx context.Context, threadKey, text, sentID string, sending time.Time) *olx.Message {
	// Clocks differ, so allow some slack.
	earliest := sending.Add(-time.Minute)
	for _, delay := range sentLookupDelays {
		select {
		case <-ctx.Done():
			return &olx.Message{}
		case <-time.After(delay):
		}
		messages, err := c.api.ListMessages(ctx, threadKey)
		if err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Msg("Couldn't look up a sent OLX message")
			continue
		}
		sortMessages(messages, c.location)
		for i := range messages {
			if messages[i].Key() == sentID {
				return &messages[i]
			}
		}
		for i := len(messages) - 1; i >= 0; i-- {
			m := &messages[i]
			if text != "" && m.Type == olx.MessageSent && m.Text == text && !olx.ParseTime(m.CreatedAt, c.location).Before(earliest) {
				return m
			}
		}
	}
	return &olx.Message{}
}

func (c *Client) HandleMatrixReadReceipt(ctx context.Context, msg *bridgev2.MatrixReadReceipt) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	threadKey := string(msg.Portal.ID)
	progress, known := c.progress(threadKey)
	// Don't spend a request on a thread OLX already has as read.
	if known && progress.UnreadCount == 0 {
		return nil
	}
	if err := c.api.MarkThreadRead(ctx, threadKey); err != nil {
		return err
	}
	if known {
		progress.UnreadCount = 0
		c.setProgress(ctx, map[string]ThreadProgress{threadKey: progress})
	}
	return nil
}

// errSignedOutSend tells the person a message didn't go out because the session
// ended, as a status every Matrix client shows: the message is marked not sent
// and the bridge bot says why.
func errSignedOutSend(err error) error {
	return bridgev2.WrapErrorInStatus(err).
		WithStatus(event.MessageStatusRetriable).
		WithErrorReason(event.MessageStatusNetworkError).
		WithMessage("Not sent: you're signed out of OLX. Sign in again, then resend.").
		WithIsCertain(true).
		WithSendNotice(true)
}

func (c *Client) reportSignedOut(err error) {
	c.login.BridgeState.Send(status.BridgeState{
		StateEvent: status.StateBadCredentials,
		Error:      "olx-signed-out",
		Message:    "OLX ended the session. Sign in again.",
		Info:       map[string]any{"go_error": err.Error()},
	})
}

func retryDelay(err error, previous time.Duration) time.Duration {
	var apiErr *olx.Error
	if errors.As(err, &apiErr) && apiErr.RetryAfter > 0 {
		return apiErr.RetryAfter
	}
	delay := previous * 2
	if apiErr != nil && apiErr.Status == http.StatusTooManyRequests && delay < 2*time.Minute {
		delay = 2 * time.Minute
	}
	if delay < 15*time.Second {
		delay = 15 * time.Second
	}
	if delay > 10*time.Minute {
		delay = 10 * time.Minute
	}
	return delay
}
