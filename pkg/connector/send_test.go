package connector

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafalenden/mautrix-olx/pkg/olx"
)

// fakeThread serves one thread whose messages appear only after `lag` lists, as
// OLX's do a moment after a send.
func fakeThread(t *testing.T, lag int32, messages string) *Client {
	t.Helper()
	var lists atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if lists.Add(1) <= lag {
			fmt.Fprint(w, `{"data":{"id":"c1","messages":[]}}`)
			return
		}
		fmt.Fprintf(w, `{"data":{"id":"c1","messages":[%s]}}`, messages)
	}))
	t.Cleanup(server.Close)
	site, _ := url.Parse(server.URL)
	tokens := olx.Tokens{AccessToken: "good", RefreshToken: "refresh", ExpiresAt: time.Now().Add(time.Hour)}
	return &Client{api: olx.NewClient(site, "client", "me-uuid", tokens), location: time.UTC}
}

func quickLookups(t *testing.T) {
	saved := sentLookupDelays
	sentLookupDelays = []time.Duration{0, time.Millisecond, time.Millisecond}
	t.Cleanup(func() { sentLookupDelays = saved })
}

// OLX's list can lag a send, which used to leave the message under a stand-in
// ID and then bridge it a second time.
func TestASentMessageIsFoundOnceItShowsUp(t *testing.T) {
	quickLookups(t)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	client := fakeThread(t, 2, fmt.Sprintf(`{"id":"sent-id","created_at":%q,"text":"Hej","user_uuid":"me-uuid"}`, now))
	if got := client.findSent(context.Background(), "c1", "Hej", "sent-id", time.Now()); got.Key() != "sent-id" {
		t.Errorf("found %q, want the message once the list has it", got.Key())
	}
}

// An earlier message with the same words isn't the one just sent.
func TestAnOlderMessageWithTheSameWordsIsntTheSentOne(t *testing.T) {
	quickLookups(t)
	old := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	client := fakeThread(t, 0, fmt.Sprintf(`{"id":"old","created_at":%q,"text":"ok","user_uuid":"me-uuid"}`, old))
	if got := client.findSent(context.Background(), "c1", "ok", "new-id", time.Now()); got.Key() != "" {
		t.Errorf("took %q for the message just sent", got.Key())
	}
	client = fakeThread(t, 0, fmt.Sprintf(`{"id":"old","created_at":%q,"text":"ok","user_uuid":"me-uuid"},{"id":"other-id","created_at":%q,"text":"ok","user_uuid":"me-uuid"}`, old, now))
	if got := client.findSent(context.Background(), "c1", "ok", "new-id", time.Now()); got.Key() != "other-id" {
		t.Errorf("found %q, want the new one when OLX gave it its own ID", got.Key())
	}
}
