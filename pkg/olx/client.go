// Package olx is a client for the API behind OLX's website, which its sites in
// Bulgaria, Kazakhstan, Poland, Portugal, Romania and Ukraine share.
package olx

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	pageSize = 100
	// How long before an access token expires to replace it.
	expiryMargin = time.Minute
	// More than any attachment the bridge should hold in memory.
	maxDownloadSize = 100 * 1024 * 1024
)

// ErrSignedOut means the user's tokens no longer work, so they have to sign in again.
var ErrSignedOut = errors.New("the OLX session has ended")

// Error is a failed API request.
type Error struct {
	Status      int
	Code        string
	Description string
	// RetryAfter is what the server asked for on a 429 or 503, if anything.
	RetryAfter time.Duration
}

func (e *Error) Error() string {
	if e.Description != "" {
		return fmt.Sprintf("OLX API returned %d: %s", e.Status, e.Description)
	}
	return fmt.Sprintf("OLX API returned %d", e.Status)
}

// IsNotFound says whether err is a 404.
func IsNotFound(err error) bool {
	var apiErr *Error
	return errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound
}

// SiteURL turns the configured site ("www.olx.pl") into a base URL.
func SiteURL(site string) (*url.URL, error) {
	site = strings.TrimRight(strings.TrimSpace(site), "/")
	if site == "" {
		return nil, errors.New("no OLX site is configured")
	}
	if !strings.Contains(site, "://") {
		site = "https://" + site
	}
	parsed, err := url.Parse(site)
	if err != nil {
		return nil, fmt.Errorf("invalid OLX site: %w", err)
	}
	return parsed, nil
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
}

// SessionClaims is what the bridge reads from a website access token. They only
// label the account: OLX is what checks the token.
type SessionClaims struct {
	UserUUID  string
	Email     string
	ExpiresAt time.Time
}

// ParseSession reads the claims of a website access token.
func ParseSession(accessToken string) (SessionClaims, error) {
	parts := strings.Split(accessToken, ".")
	if len(parts) != 3 {
		return SessionClaims{}, errors.New("the OLX session token isn't in the expected format")
	}
	data, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return SessionClaims{}, errors.New("the OLX session token isn't in the expected format")
	}
	var claims struct {
		Subject string `json:"sub"`
		Email   string `json:"email"`
		Expiry  int64  `json:"exp"`
	}
	if err = json.Unmarshal(data, &claims); err != nil || claims.Subject == "" {
		return SessionClaims{}, errors.New("the OLX session token isn't in the expected format")
	}
	return SessionClaims{UserUUID: claims.Subject, Email: claims.Email, ExpiresAt: time.Unix(claims.Expiry, 0)}, nil
}

// Client talks to the API behind OLX's website, using the session from the user's
// own sign-in. OLX doesn't document this API, so it can change without notice.
type Client struct {
	// SiteURL is the site people browse, such as https://www.olx.pl.
	SiteURL  *url.URL
	ChatURL  *url.URL
	AuthURL  *url.URL
	SiteCode string
	ClientID string
	UserUUID string
	HTTP     *http.Client
	// OnTokens is called whenever a refresh replaces the tokens, so they can be saved.
	OnTokens func(Tokens)

	lock   sync.Mutex
	tokens Tokens

	namesLock sync.Mutex
	users     map[int64]*User
}

// NewClient makes a client for a site such as www.olx.pl, whose chat is at
// api.chat.olx.pl and sign-in at login.olx.pl. Any other address is used for both.
func NewClient(site *url.URL, clientID, userUUID string, tokens Tokens) *Client {
	chat, auth, code := site, site, "olxpl"
	if domain, ok := strings.CutPrefix(site.Host, "www.olx."); ok {
		chat = &url.URL{Scheme: "https", Host: "api.chat.olx." + domain}
		auth = &url.URL{Scheme: "https", Host: "login.olx." + domain}
		code = "olx" + domain
	}
	return &Client{
		SiteURL: site, ChatURL: chat, AuthURL: auth, SiteCode: code, ClientID: clientID, UserUUID: userUUID,
		HTTP:   &http.Client{Timeout: 60 * time.Second},
		tokens: tokens,
		users:  map[int64]*User{},
	}
}

func (c *Client) Tokens() Tokens {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.tokens
}

// accessToken returns a token that should work, refreshing first if it's about to
// expire, which website tokens do every 15 minutes. `rejected` is one the server
// has just refused.
func (c *Client) accessToken(ctx context.Context, rejected string) (string, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	current := c.tokens
	if current.AccessToken != "" && current.AccessToken != rejected && time.Now().Add(expiryMargin).Before(current.ExpiresAt) {
		return current.AccessToken, nil
	}
	if current.RefreshToken == "" {
		return "", ErrSignedOut
	}
	form := url.Values{
		"grant_type":    {"refresh_token"},
		"client_id":     {c.ClientID},
		"refresh_token": {current.RefreshToken},
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.AuthURL.JoinPath("oauth2/token").String(), strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := c.HTTP.Do(request)
	if err != nil {
		return "", redactError(err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusBadRequest || response.StatusCode == http.StatusUnauthorized {
		rejected := fmt.Errorf("%w: %w", ErrSignedOut, readError(response))
		if current.SignInCookie == "" {
			return "", rejected
		}
		// The refresh token lapses when unused for a while. The sign-in site's
		// cookie outlasts it, as it does for the website.
		fresh, err := c.signInAgain(ctx, current.SignInCookie)
		if err != nil {
			return "", err
		}
		c.tokens = fresh
		if c.OnTokens != nil {
			c.OnTokens(fresh)
		}
		return fresh.AccessToken, nil
	} else if response.StatusCode >= 300 {
		return "", readError(response)
	}
	var parsed tokenResponse
	if err = json.NewDecoder(response.Body).Decode(&parsed); err != nil || parsed.AccessToken == "" {
		return "", errors.New("couldn't read OLX's session refresh")
	}
	fresh := Tokens{
		AccessToken:  parsed.AccessToken,
		RefreshToken: parsed.RefreshToken,
		ExpiresAt:    time.Now().Add(time.Duration(parsed.ExpiresIn) * time.Second),
	}
	if fresh.RefreshToken == "" {
		fresh.RefreshToken = current.RefreshToken
	}
	fresh.SignInCookie = current.SignInCookie
	c.tokens = fresh
	if c.OnTokens != nil {
		c.OnTokens(fresh)
	}
	return fresh.AccessToken, nil
}

func (c *Client) authorized(ctx context.Context, build func() (*http.Request, error)) (*http.Response, error) {
	rejected := ""
	for attempt := 0; ; attempt++ {
		token, err := c.accessToken(ctx, rejected)
		if err != nil {
			return nil, err
		}
		request, err := build()
		if err != nil {
			return nil, err
		}
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("X-Site-Code", c.SiteCode)
		request.Header.Set("X-Client", "DESKTOP")
		response, err := c.HTTP.Do(request)
		if err != nil {
			return nil, redactError(err)
		}
		if response.StatusCode == http.StatusUnauthorized {
			apiErr := readError(response)
			response.Body.Close()
			if attempt > 0 {
				return nil, fmt.Errorf("%w: %w", ErrSignedOut, apiErr)
			}
			rejected = token
			continue
		}
		return response, nil
	}
}

func (c *Client) call(ctx context.Context, method, path string, query url.Values, input, output any) error {
	var body []byte
	if input != nil {
		var err error
		if body, err = json.Marshal(input); err != nil {
			return err
		}
	}
	target := c.ChatURL.JoinPath(path)
	target.RawQuery = query.Encode()
	response, err := c.authorized(ctx, func() (*http.Request, error) {
		request, err := http.NewRequestWithContext(ctx, method, target.String(), bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		request.Header.Set("Accept", "application/json")
		if method == http.MethodGet {
			request.Header.Set("X-Api-Version", "2")
		}
		if input != nil {
			request.Header.Set("Content-Type", "application/json")
		}
		return request, nil
	})
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return readError(response)
	}
	if output == nil {
		return nil
	}
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	return decode(data, output)
}

// The fields the bridge uses. Attachments and extras are loosely typed because
// their shapes vary by message type.
type chatConversation struct {
	ID string `json:"id"`
	Ad struct {
		ID    int64  `json:"id"`
		Title string `json:"title"`
	} `json:"ad"`
	// Loosely typed because its contents vary by advert category.
	Context    json.RawMessage `json:"context"`
	Messages   []chatMessage   `json:"messages"`
	Respondent struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	} `json:"respondent"`
	UnreadCount int `json:"unread_count"`
}

type chatMessage struct {
	ID          string           `json:"id"`
	CreatedAt   string           `json:"created_at"`
	Text        string           `json:"text"`
	Type        string           `json:"type"`
	UserUUID    string           `json:"user_uuid"`
	ReadAt      string           `json:"read_at"`
	Attachments []map[string]any `json:"attachments"`
	Extras      json.RawMessage  `json:"extras"`
}

func (c *Client) remember(conversation chatConversation) {
	c.namesLock.Lock()
	defer c.namesLock.Unlock()
	if conversation.Respondent.ID != 0 && conversation.Respondent.Name != "" {
		c.users[conversation.Respondent.ID] = &User{ID: conversation.Respondent.ID, Name: conversation.Respondent.Name}
	}
}

func (c *Client) toThread(conversation chatConversation) Thread {
	c.remember(conversation)
	thread := Thread{
		UUID:           conversation.ID,
		AdvertID:       conversation.Ad.ID,
		InterlocutorID: conversation.Respondent.ID,
		TotalCount:     len(conversation.Messages),
		UnreadCount:    conversation.UnreadCount,
		Listing:        c.listing(conversation),
	}
	messages := c.toMessages(conversation)
	if len(messages) > 0 {
		thread.LatestKey = messages[len(messages)-1].Key()
	}
	for _, message := range slices.Backward(messages) {
		if message.Type == MessageSent && !message.IsSystem {
			thread.LatestOwn = message.Key()
			break
		}
	}
	for _, message := range slices.Backward(messages) {
		if message.IsOffer && message.Type == MessageReceived {
			thread.LatestTheirOffer = &message
			break
		}
	}
	for _, message := range slices.Backward(messages) {
		if message.Type == MessageSent && !message.IsSystem && message.ReadAt != "" {
			thread.ReadByThem, thread.ReadByThemAt = message.Key(), message.ReadAt
			break
		}
	}
	return thread
}

func (c *Client) toMessages(conversation chatConversation) []Message {
	messages := make([]Message, 0, len(conversation.Messages))
	for _, source := range conversation.Messages {
		message := Message{
			UUID:       source.ID,
			ThreadUUID: conversation.ID,
			CreatedAt:  source.CreatedAt,
			Text:       source.Text,
			Type:       MessageReceived,
			IsSystem:   source.Type == "system",
			IsOffer:    strings.HasPrefix(source.Type, "custom:price_negotiation_widget_"),
			ReadAt:     source.ReadAt,
		}
		if source.UserUUID == c.UserUUID {
			message.Type = MessageSent
		}
		var extras struct {
			Text string `json:"text"`
		}
		_ = json.Unmarshal(source.Extras, &extras)
		// OLX's own lines open with a heading such as "• Automatyczna wiadomość
		// od OLX •", and keep the line without it in extras. One can be listed
		// for a moment before it's marked as OLX's, filed under whoever's
		// conversation it is, so the heading also marks it.
		if body, ok := withoutOLXHeading(message.Text); ok {
			message.IsSystem = true
			message.Text = body
		}
		if message.Text == "" || (message.IsSystem && extras.Text != "") {
			message.Text = extras.Text
		}
		for _, attachment := range source.Attachments {
			if file := attachmentFrom(attachment); file.URL != "" {
				message.Attachments = append(message.Attachments, file)
			}
		}
		messages = append(messages, message)
	}
	slices.SortStableFunc(messages, func(a, b Message) int {
		return ParseTime(a.CreatedAt, time.UTC).Compare(ParseTime(b.CreatedAt, time.UTC))
	})
	return messages
}

// withoutOLXHeading takes off the heading OLX puts on its own lines, a first line
// set between bullets ("• … •"), and reports whether there was one.
func withoutOLXHeading(text string) (string, bool) {
	first, rest, found := strings.Cut(text, "\n")
	first = strings.TrimSpace(first)
	rest = strings.TrimSpace(rest)
	if !found || rest == "" || len(first) < 3 || !strings.HasPrefix(first, "•") || !strings.HasSuffix(first, "•") {
		return text, false
	}
	return rest, true
}

func attachmentFrom(fields map[string]any) Attachment {
	text := func(keys ...string) string {
		for _, key := range keys {
			if value, ok := fields[key].(string); ok && value != "" {
				return value
			}
		}
		return ""
	}
	return Attachment{Name: text("filename", "file_name", "name"), URL: text("url", "file_url", "href")}
}

// listing reads the advert from a conversation. Anything missing is left empty, so
// a surprise in OLX's format costs a detail rather than the whole conversation.
func (c *Client) listing(conversation chatConversation) Listing {
	var context struct {
		Title    string `json:"title"`
		ImageURL string `json:"image_url"`
		Data     struct {
			Price *struct {
				MinorAmount *int64 `json:"minor_amount"`
				Currency    string `json:"currency_code"`
			} `json:"price"`
		} `json:"context_data"`
	}
	_ = json.Unmarshal(conversation.Context, &context)
	listing := Listing{AdvertID: conversation.Ad.ID, Title: conversation.Ad.Title, ImageURL: context.ImageURL}
	if listing.Title == "" {
		listing.Title = context.Title
	}
	if price := context.Data.Price; price != nil && price.MinorAmount != nil {
		listing.PriceMinor, listing.Currency = price.MinorAmount, price.Currency
	}
	if listing.AdvertID > 0 {
		listing.AdvertURL = c.AdvertURL(listing.AdvertID)
	}
	if conversation.ID != "" {
		listing.ConversationURL = c.ConversationURL(conversation.ID)
	}
	return listing
}

// AdvertURL is a link to an advert's page. OLX's links end in the advert ID in
// base 62, and the site redirects any words before it to the advert's own.
func (c *Client) AdvertURL(id int64) string {
	return c.SiteURL.JoinPath("d/oferta", "ogloszenie-ID"+base62(id)+".html").String()
}

// ConversationURL is a link to a conversation on OLX's site, where offers are answered.
func (c *Client) ConversationURL(id string) string {
	return c.SiteURL.JoinPath("myaccount/answer", id, "/").String()
}

func base62(n int64) string {
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	if n <= 0 {
		return "0"
	}
	var digits []byte
	for ; n > 0; n /= 62 {
		digits = append([]byte{alphabet[n%62]}, digits...)
	}
	return string(digits)
}

func (c *Client) ListThreads(ctx context.Context) ([]Thread, error) {
	var all []Thread
	seen := map[string]bool{}
	for offset := 0; ; offset += pageSize {
		query := url.Values{"offset": {strconv.Itoa(offset)}, "limit": {strconv.Itoa(pageSize)}}
		var page []chatConversation
		if err := c.call(ctx, http.MethodGet, "api/conversations", query, nil, &page); err != nil {
			return nil, err
		}
		added := false
		for _, conversation := range page {
			if !seen[conversation.ID] {
				seen[conversation.ID] = true
				added = true
				all = append(all, c.toThread(conversation))
			}
		}
		if len(page) < pageSize || !added {
			return all, nil
		}
	}
}

func (c *Client) conversation(ctx context.Context, threadKey string) (*chatConversation, error) {
	var conversation chatConversation
	if err := c.call(ctx, http.MethodGet, "api/conversations/"+url.PathEscape(threadKey), nil, nil, &conversation); err != nil {
		return nil, err
	}
	return &conversation, nil
}

func (c *Client) GetThread(ctx context.Context, threadKey string) (*Thread, error) {
	conversation, err := c.conversation(ctx, threadKey)
	if err != nil {
		return nil, err
	}
	thread := c.toThread(*conversation)
	return &thread, nil
}

// ListMessages returns a conversation's messages. Reading them doesn't mark it read.
func (c *Client) ListMessages(ctx context.Context, threadKey string) ([]Message, error) {
	conversation, err := c.conversation(ctx, threadKey)
	if err != nil {
		return nil, err
	}
	return c.toMessages(*conversation), nil
}

// SendMessage posts text with an ID of its own, as the website does. The response
// doesn't reliably identify the new message, so the caller finds it by its text.
// SendMessage sends text and returns the ID it gave the message. OLX's reply
// doesn't identify the message, but the ones the bridge has sent carry IDs of the
// form it gives, so OLX appears to keep it.
func (c *Client) SendMessage(ctx context.Context, threadKey, text string) (string, error) {
	id := newUUID()
	body := map[string]any{"message": map[string]any{"text": text, "message_id": id}}
	if err := c.call(ctx, http.MethodPost, "api/conversations/"+url.PathEscape(threadKey)+"/messages", nil, body, nil); err != nil {
		return "", err
	}
	return id, nil
}

func (c *Client) MarkThreadRead(ctx context.Context, threadKey string) error {
	return c.call(ctx, http.MethodPost, "api/conversations/"+url.PathEscape(threadKey)+"/read", nil,
		map[string]string{"event_uuid": newUUID()}, nil)
}

// GetUser answers from the conversations seen so far, the only place this API
// shows names.
func (c *Client) GetUser(ctx context.Context, id int64) (*User, error) {
	c.namesLock.Lock()
	defer c.namesLock.Unlock()
	if user, ok := c.users[id]; ok {
		return user, nil
	}
	return nil, &Error{Status: http.StatusNotFound}
}

// Download fetches an attachment. The session token only goes to the chat API.
func (c *Client) Download(ctx context.Context, rawURL string) ([]byte, error) {
	target, err := c.ChatURL.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	build := func() (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	}
	var response *http.Response
	if target.Scheme == c.ChatURL.Scheme && target.Host == c.ChatURL.Host {
		response, err = c.authorized(ctx, build)
	} else {
		var request *http.Request
		if request, err = build(); err == nil {
			response, err = c.HTTP.Do(request)
			err = redactError(err)
		}
	}
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readError(response)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxDownloadSize+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxDownloadSize {
		return nil, fmt.Errorf("the file is over %d bytes", maxDownloadSize)
	}
	return data, nil
}

func newUUID() string {
	raw := make([]byte, 16)
	_, _ = rand.Read(raw)
	raw[6] = raw[6]&0x0f | 0x40
	raw[8] = raw[8]&0x3f | 0x80
	text := hex.EncodeToString(raw)
	return text[:8] + "-" + text[8:12] + "-" + text[12:16] + "-" + text[16:20] + "-" + text[20:]
}

// decode reads a response body, which is normally under "data".
func decode(data []byte, output any) error {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	var wrapper struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(data, &wrapper); err == nil && len(wrapper.Data) > 0 {
		data = wrapper.Data
	}
	if err := json.Unmarshal(data, output); err != nil {
		return fmt.Errorf("couldn't read OLX's response: %w", err)
	}
	return nil
}

func readError(response *http.Response) *Error {
	data, _ := io.ReadAll(io.LimitReader(response.Body, 64*1024))
	apiErr := &Error{Status: response.StatusCode}
	if seconds, err := strconv.Atoi(response.Header.Get("Retry-After")); err == nil && seconds > 0 {
		apiErr.RetryAfter = time.Duration(seconds) * time.Second
	}
	// Both shapes appear: {"error":"code","error_description":"…"} on OAuth errors
	// and {"error":{"status":400,"title":"…","detail":"…"}} on the rest.
	var flat struct {
		Error            string `json:"error"`
		ErrorDescription string `json:"error_description"`
	}
	if json.Unmarshal(data, &flat) == nil && flat.Error != "" {
		apiErr.Code, apiErr.Description = flat.Error, flat.ErrorDescription
		return apiErr
	}
	var nested struct {
		Error struct {
			Title  string `json:"title"`
			Detail string `json:"detail"`
		} `json:"error"`
	}
	if json.Unmarshal(data, &nested) == nil {
		apiErr.Description = strings.TrimSpace(nested.Error.Title + " " + nested.Error.Detail)
	}
	return apiErr
}

// redactError drops the URL from transport errors. Keeping it could log a query string.
func redactError(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return urlErr.Err
	}
	return err
}
