package olx

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func fakeJWT(subject string, expires time.Time) string {
	payload, _ := json.Marshal(map[string]any{"sub": subject, "email": "a@example.test", "exp": expires.Unix()})
	return "h." + base64.RawURLEncoding.EncodeToString(payload) + ".s"
}

func newTestClient(t *testing.T, handler http.HandlerFunc, tokens Tokens) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	return NewClient(base, "client", "me-uuid", tokens)
}

const sampleConversation = `{"id":"c1","ad":{"id":5,"title":"Rower"},"context":{"image_url":"https://cdn.example/r.jpg"},"respondent":{"id":9,"name":"Anna","uuid":"her-uuid"},"unread_count":1,
"messages":[
 {"id":"m2","created_at":"2026-01-02T10:00:00+01:00","text":"Hej","type":null,"user_uuid":"her-uuid","attachments":[{"filename":"a.png","url":"https://cdn.example/a.png"}]},
 {"id":"m1","created_at":"2026-01-01T10:00:00+01:00","text":"","type":"custom:x","user_uuid":"me-uuid","attachments":[],"extras":{"text":"Propozycja"}}
]}`

func TestSiteHosts(t *testing.T) {
	site, _ := SiteURL("www.olx.ro")
	client := NewClient(site, "c", "u", Tokens{})
	if client.ChatURL.String() != "https://api.chat.olx.ro" || client.AuthURL.String() != "https://login.olx.ro" || client.SiteCode != "olxro" {
		t.Errorf("got %s, %s, %s", client.ChatURL, client.AuthURL, client.SiteCode)
	}
}

func TestParseSession(t *testing.T) {
	expires := time.Unix(2000000000, 0)
	claims, err := ParseSession(fakeJWT("uuid-1", expires))
	if err != nil || claims.UserUUID != "uuid-1" || claims.Email != "a@example.test" || !claims.ExpiresAt.Equal(expires) {
		t.Errorf("got %+v, %v", claims, err)
	}
	for _, bad := range []string{"", "a.b", "a.!!!.c", "h." + base64.RawURLEncoding.EncodeToString([]byte(`{"email":"x"}`)) + ".s"} {
		if _, err = ParseSession(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestConversationsBecomeThreadsAndMessages(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer good" || r.Header.Get("X-Api-Version") != "2" {
			t.Errorf("unexpected headers %v", r.Header)
		}
		if r.URL.Path == "/api/conversations" {
			fmt.Fprintf(w, `{"data":[%s],"metadata":{"total_elements":1}}`, sampleConversation)
			return
		}
		fmt.Fprintf(w, `{"data":%s}`, sampleConversation)
	}, goodTokens())

	threads, err := client.ListThreads(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	thread := threads[0]
	if thread.Key() != "c1" || thread.AdvertID != 5 || thread.InterlocutorID != 9 || thread.UnreadCount != 1 ||
		thread.TotalCount != 2 || thread.LatestKey != "m2" {
		t.Errorf("unexpected thread %+v", thread)
	}
	messages, err := client.ListMessages(context.Background(), "c1")
	if err != nil {
		t.Fatal(err)
	}
	if messages[0].Key() != "m1" || messages[0].Type != MessageSent || messages[0].Text != "Propozycja" {
		t.Errorf("unexpected first message %+v", messages[0])
	}
	if messages[1].Type != MessageReceived || messages[1].Attachments[0].Name != "a.png" || messages[1].Attachments[0].URL != "https://cdn.example/a.png" {
		t.Errorf("unexpected second message %+v", messages[1])
	}
	user, err := client.GetUser(context.Background(), 9)
	if err != nil || user.Name != "Anna" {
		t.Errorf("got %+v, %v", user, err)
	}
	if listing := thread.Listing; listing.Title != "Rower" || listing.AdvertID != 5 || listing.ImageURL != "https://cdn.example/r.jpg" {
		t.Errorf("unexpected listing %+v", listing)
	}
	if _, err = client.GetUser(context.Background(), 1); !IsNotFound(err) {
		t.Errorf("got %v", err)
	}
}

func TestSendAndRead(t *testing.T) {
	var sent map[string]map[string]string
	var read map[string]string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/conversations/c1/messages":
			_ = json.NewDecoder(r.Body).Decode(&sent)
		case "/api/conversations/c1/read":
			_ = json.NewDecoder(r.Body).Decode(&read)
		default:
			t.Errorf("unexpected %s", r.URL.Path)
		}
		fmt.Fprint(w, `{"data":{}}`)
	}, goodTokens())
	id, err := client.SendMessage(context.Background(), "c1", "Cześć")
	if err != nil {
		t.Fatal(err)
	}
	if sent["message"]["text"] != "Cześć" || len(sent["message"]["message_id"]) != 36 || sent["message"]["message_id"] != id {
		t.Errorf("sent %v, returned %q", sent, id)
	}
	if err := client.MarkThreadRead(context.Background(), "c1"); err != nil {
		t.Fatal(err)
	}
	if len(read["event_uuid"]) != 36 {
		t.Errorf("sent %v", read)
	}
}

func TestExpiredSessionRefreshesWithoutASecret(t *testing.T) {
	var saved Tokens
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth2/token" {
			_ = r.ParseForm()
			if r.Form.Get("grant_type") != "refresh_token" || r.Form.Get("client_id") != "client" ||
				r.Form.Get("refresh_token") != "keep" || r.Form.Has("client_secret") ||
				!strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
				t.Errorf("unexpected refresh %v", r.Form)
			}
			fmt.Fprint(w, `{"access_token":"fresh","expires_in":900}`)
			return
		}
		if r.Header.Get("Authorization") != "Bearer fresh" {
			t.Errorf("sent %q", r.Header.Get("Authorization"))
		}
		fmt.Fprint(w, `{"data":[]}`)
	}, Tokens{AccessToken: "old", RefreshToken: "keep", ExpiresAt: time.Now().Add(-time.Minute)})
	client.OnTokens = func(tokens Tokens) { saved = tokens }
	if _, err := client.ListThreads(context.Background()); err != nil {
		t.Fatal(err)
	}
	if saved.AccessToken != "fresh" || saved.RefreshToken != "keep" {
		t.Errorf("a response with no new refresh token should keep the old one: %+v", saved)
	}
}

func TestRevokedSessionSignsOut(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":"invalid_grant","error_description":"Invalid"}`)
	}, Tokens{AccessToken: "old", RefreshToken: "gone", ExpiresAt: time.Now().Add(-time.Minute)})
	if _, err := client.ListThreads(context.Background()); !errors.Is(err, ErrSignedOut) {
		t.Errorf("got %v", err)
	}
}

func TestChatDownloadOnlySendsTheTokenToTheChatAPI(t *testing.T) {
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Error("the token went to another host")
		}
		fmt.Fprint(w, "cdn")
	}))
	defer other.Close()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer good" {
			t.Errorf("sent %q", r.Header.Get("Authorization"))
		}
		fmt.Fprint(w, "api")
	}, goodTokens())
	if data, err := client.Download(context.Background(), other.URL+"/a.png"); err != nil || string(data) != "cdn" {
		t.Errorf("got %q, %v", data, err)
	}
	if data, err := client.Download(context.Background(), "/api/file"); err != nil || string(data) != "api" {
		t.Errorf("got %q, %v", data, err)
	}
}

func goodTokens() Tokens {
	return Tokens{AccessToken: "good", RefreshToken: "refresh", ExpiresAt: time.Now().Add(time.Hour)}
}

func TestRateLimitReportsRetryAfter(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprint(w, `{"error":{"status":429,"title":"Too many requests","detail":""}}`)
	}, goodTokens())
	_, err := client.ListThreads(context.Background())
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.Status != 429 || apiErr.RetryAfter != 2*time.Minute || apiErr.Description != "Too many requests" {
		t.Errorf("got %#v", err)
	}
}

func TestServerTroubleDoesntSignOut(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}, Tokens{AccessToken: "old", RefreshToken: "r", ExpiresAt: time.Now().Add(-time.Hour)})
	if _, err := client.ListThreads(context.Background()); err == nil || errors.Is(err, ErrSignedOut) {
		t.Errorf("got %v", err)
	}
}

func TestPaginationStopsIfTheServerIgnoresOffset(t *testing.T) {
	calls := 0
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		var items []string
		for i := range 100 {
			items = append(items, fmt.Sprintf(`{"id":"c%d"}`, i))
		}
		fmt.Fprintf(w, `{"data":[%s]}`, strings.Join(items, ","))
	}, goodTokens())
	threads, err := client.ListThreads(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(threads) != 100 || calls != 2 {
		t.Errorf("got %d threads in %d calls", len(threads), calls)
	}
}

func TestParseTime(t *testing.T) {
	warsaw, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2018, 1, 30, 7, 26, 36, 0, time.UTC)
	for _, value := range []string{"2018-01-30 08:26:36", "2018-01-30T08:26:36+01:00", "2018-01-30T07:26:36Z"} {
		if got := ParseTime(value, warsaw); !got.Equal(want) {
			t.Errorf("%s gave %s", value, got)
		}
	}
	if got := ParseTime("yesterday", warsaw); !got.IsZero() {
		t.Errorf("gave %s", got)
	}
}

func TestListingSurvivesUnexpectedContext(t *testing.T) {
	client := NewClient(&url.URL{Scheme: "https", Host: "example.test"}, "c", "me", Tokens{})
	conversation := chatConversation{Context: json.RawMessage(`{"title":["not","a","string"]}`)}
	conversation.Ad.Title = "Rower"
	if got := client.listing(conversation); got.Title != "Rower" || got.ImageURL != "" {
		t.Errorf("unexpected listing %+v", got)
	}
	if got := client.listing(chatConversation{}); got.Title != "" {
		t.Errorf("unexpected empty listing %+v", got)
	}
}

func TestLinks(t *testing.T) {
	site, _ := SiteURL("www.olx.pl")
	client := NewClient(site, "c", "me", Tokens{})
	// OLX's links end in the advert's ID in base 62.
	if got := client.AdvertURL(1000000000); got != "https://www.olx.pl/d/oferta/ogloszenie-ID15FTGg.html" {
		t.Errorf("got %s", got)
	}
	if got := client.ConversationURL("8f0c"); got != "https://www.olx.pl/myaccount/answer/8f0c/" {
		t.Errorf("got %s", got)
	}
	for n, want := range map[int64]string{0: "0", 9: "9", 10: "a", 36: "A", 61: "Z", 62: "10"} {
		if got := base62(n); got != want {
			t.Errorf("base62(%d) = %s", n, got)
		}
	}
	conversation := chatConversation{ID: "c1", Context: json.RawMessage(`{"context_data":{"price":{"minor_amount":150000,"currency_code":"PLN"}}}`)}
	conversation.Ad.ID = 61
	listing := client.listing(conversation)
	if listing.AdvertURL != "https://www.olx.pl/d/oferta/ogloszenie-IDZ.html" || listing.ConversationURL != "https://www.olx.pl/myaccount/answer/c1/" ||
		listing.PriceMinor == nil || *listing.PriceMinor != 150000 || listing.Currency != "PLN" {
		t.Errorf("unexpected listing %+v", listing)
	}
	if empty := client.listing(chatConversation{}); empty.AdvertURL != "" || empty.ConversationURL != "" || empty.PriceMinor != nil {
		t.Errorf("unexpected empty listing %+v", empty)
	}
}

func TestOffersAreRecognised(t *testing.T) {
	client := NewClient(&url.URL{Scheme: "https", Host: "example.test"}, "c", "me", Tokens{})
	messages := client.toMessages(chatConversation{ID: "c", Messages: []chatMessage{
		{ID: "a", Text: "Nowa propozycja: 1300.00 zł", Type: "custom:price_negotiation_widget_seller_proposed"},
		{ID: "b", Text: "Hej"},
	}})
	if !messages[0].IsOffer || messages[1].IsOffer {
		t.Errorf("unexpected offers %+v", messages)
	}
}

// OLX says when each message was read, so the thread knows the newest of yours
// the other person has read. Their own messages' reads are yours, and don't count.
func TestThreadKnowsWhatTheyveRead(t *testing.T) {
	conversation := `{"id":"c1","ad":{"id":5,"title":"Rower"},"respondent":{"id":9,"name":"Anna"},
"messages":[
 {"id":"m3","created_at":"2026-01-03T10:00:00Z","text":"Jeszcze jedno","user_uuid":"me-uuid","read_at":null},
 {"id":"m1","created_at":"2026-01-01T10:00:00Z","text":"Dzień dobry","user_uuid":"me-uuid","read_at":"2026-01-01T12:00:00.5Z"},
 {"id":"m2","created_at":"2026-01-02T10:00:00Z","text":"Hej","user_uuid":"her-uuid","read_at":"2026-01-02T11:00:00Z"}
]}`
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"data":[%s],"metadata":{"total_elements":1}}`, conversation)
	}, goodTokens())
	threads, err := client.ListThreads(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := threads[0]; got.ReadByThem != "m1" || got.ReadByThemAt != "2026-01-01T12:00:00.5Z" {
		t.Errorf("read by them = %q at %q, want m1", got.ReadByThem, got.ReadByThemAt)
	}
}
