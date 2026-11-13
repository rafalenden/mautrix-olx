package connector

import (
	"errors"
	"strings"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"

	"github.com/rafalenden/mautrix-olx/pkg/olx"
)

func keys(messages []olx.Message) string {
	var out []string
	for _, message := range messages {
		out = append(out, message.Key())
	}
	return strings.Join(out, ",")
}

func TestSortMessagesIsOldestFirst(t *testing.T) {
	messages := []olx.Message{
		{UUID: "c", CreatedAt: "2026-01-02 10:00:00"},
		{UUID: "a", CreatedAt: "2026-01-01 10:00:00"},
		{UUID: "b", CreatedAt: "2026-01-01 10:00:00"},
	}
	sortMessages(messages, time.UTC)
	if got := keys(messages); got != "a,b,c" {
		t.Errorf("got %s", got)
	}
}

func TestNewSince(t *testing.T) {
	messages := []olx.Message{{UUID: "a"}, {UUID: "b"}, {UUID: "c"}}
	if fresh, ok := newSince(messages, "a"); !ok || keys(fresh) != "b,c" {
		t.Errorf("after a: %s, %v", keys(fresh), ok)
	}
	if fresh, ok := newSince(messages, "c"); !ok || len(fresh) != 0 {
		t.Errorf("after c: %s, %v", keys(fresh), ok)
	}
	if _, ok := newSince(messages, "gone"); ok {
		t.Error("found a message that isn't there")
	}
	if _, ok := newSince(messages, ""); ok {
		t.Error("treated no progress as progress")
	}
}

func TestSelectForBackfill(t *testing.T) {
	messages := []olx.Message{
		{UUID: "a", CreatedAt: "2026-01-01 10:00:00"},
		{UUID: "b", CreatedAt: "2026-01-02 10:00:00"},
		{UUID: "c", CreatedAt: "2026-01-03 10:00:00"},
	}
	if got := keys(selectForBackfill(messages, nil, true, time.UTC)); got != "a,b,c" {
		t.Errorf("no anchor: %s", got)
	}
	anchor := &database.Message{ID: "b"}
	if got := keys(selectForBackfill(messages, anchor, true, time.UTC)); got != "c" {
		t.Errorf("forward: %s", got)
	}
	if got := keys(selectForBackfill(messages, anchor, false, time.UTC)); got != "a" {
		t.Errorf("backward: %s", got)
	}
	// An anchor OLX no longer lists, such as a message sent before its ID was known.
	gone := &database.Message{ID: "local:x", Timestamp: time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)}
	if got := keys(selectForBackfill(messages, gone, true, time.UTC)); got != "c" {
		t.Errorf("forward by time: %s", got)
	}
	if got := keys(selectForBackfill(messages, gone, false, time.UTC)); got != "a,b" {
		t.Errorf("backward by time: %s", got)
	}
}

func TestSessionScriptReadsAllThreeFields(t *testing.T) {
	// bridgev2 says the script evaluates to a promise, so it starts as one.
	if !strings.HasPrefix(extractSessionScript, "new Promise(") {
		t.Errorf("the script isn't a promise expression:\n%s", extractSessionScript)
	}
	for _, want := range []string{fieldAccess, fieldRefresh, fieldClientID, "@@auth0spajs@@::"} {
		if !strings.Contains(extractSessionScript, want) {
			t.Errorf("script is missing %s", want)
		}
	}
}

func TestEverySiteHasAFlowAndATimeZone(t *testing.T) {
	flows := (&OLXConnector{}).GetLoginFlows()
	if len(flows) != len(sites) {
		t.Fatalf("got %d flows", len(flows))
	}
	for i, site := range sites {
		if flows[i].ID != site.domain || !strings.Contains(flows[i].Name, site.country) {
			t.Errorf("unexpected flow %+v", flows[i])
		}
		if siteLocation("www.olx."+site.domain) == time.UTC {
			t.Errorf("%s has no time zone", site.domain)
		}
	}
}

func TestRespondLinkIsInBothBodies(t *testing.T) {
	content := &event.MessageEventContent{MsgType: event.MsgText, Body: "Nowa propozycja: <1300.00 zł>"}
	addRespondLink(content, "https://www.olx.pl/myaccount/answer/c1/", textsFor("www.olx.pl").Respond)
	if content.Body != "Nowa propozycja: <1300.00 zł>" {
		t.Errorf("plain body %q", content.Body)
	}
	if content.Format != event.FormatHTML ||
		content.FormattedBody != `Nowa propozycja: &lt;1300.00 zł&gt;<br><a href="https://www.olx.pl/myaccount/answer/c1/">Odpowiedz na OLX</a>` {
		t.Errorf("formatted body %q", content.FormattedBody)
	}
}

// Only the other person's offer is one to answer; yours has nothing to answer.
func TestOnlyTheirOffersAreAnswerable(t *testing.T) {
	theirs := olx.Message{IsOffer: true, Type: olx.MessageReceived, ThreadUUID: "c1"}
	mine := olx.Message{IsOffer: true, Type: olx.MessageSent, ThreadUUID: "c1"}
	words := olx.Message{Type: olx.MessageReceived, ThreadUUID: "c1"}
	if !answerable(theirs) || answerable(mine) || answerable(words) {
		t.Errorf("answerable: theirs %v, mine %v, words %v", answerable(theirs), answerable(mine), answerable(words))
	}
	// Offers bridged earlier: all of them if links were never added, else yours.
	if !needsLinkFix(theirs, false) || !needsLinkFix(mine, false) || needsLinkFix(theirs, true) ||
		!needsLinkFix(mine, true) || needsLinkFix(words, false) {
		t.Error("the wrong offers would be edited")
	}
}

func TestListingTopic(t *testing.T) {
	price := int64(150000)
	odd := int64(129950)
	for want, listing := range map[string]olx.Listing{
		"Rower · 1500 zł\nhttps://x/a": {Title: "Rower", PriceMinor: &price, Currency: "PLN", AdvertURL: "https://x/a"},
		"Rower · 1299,50 EUR":          {Title: "Rower", PriceMinor: &odd, Currency: "EUR"},
		"Rower":                        {Title: "Rower"},
		"":                             {},
	} {
		if got := listingTopic(listing); got != strings.ReplaceAll(want, "EUR", "€") {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}

func TestSiteLocation(t *testing.T) {
	for site, want := range map[string]string{
		"www.olx.pl":          "Europe/Warsaw",
		"https://www.olx.ro/": "Europe/Bucharest",
		"www.olx.bg":          "Europe/Sofia",
		"www.olx.pt":          "Europe/Lisbon",
		"www.olx.ua":          "Europe/Kyiv",
		"www.olx.kz":          "Asia/Almaty",
		"example.test":        "UTC",
	} {
		if got := siteLocation(site).String(); got != want {
			t.Errorf("%s gave %s", site, got)
		}
	}
}

func TestSignedOutSendIsAVisibleFailure(t *testing.T) {
	status := bridgev2.WrapErrorInStatus(errSignedOutSend(olx.ErrSignedOut))
	if status.Status != event.MessageStatusRetriable || !status.SendNotice || !strings.Contains(status.Message, "signed out of OLX") {
		t.Errorf("unexpected status %+v", status)
	}
}

func TestRetryDelay(t *testing.T) {
	if got := retryDelay(errors.New("network"), 0); got != 15*time.Second {
		t.Errorf("first failure: %s", got)
	}
	if got := retryDelay(errors.New("network"), 8*time.Minute); got != 10*time.Minute {
		t.Errorf("capped: %s", got)
	}
	if got := retryDelay(&olx.Error{Status: 429}, 0); got != 2*time.Minute {
		t.Errorf("rate limited: %s", got)
	}
	if got := retryDelay(&olx.Error{Status: 429, RetryAfter: 5 * time.Minute}, 0); got != 5*time.Minute {
		t.Errorf("told when: %s", got)
	}
}

func TestMsgTypeFor(t *testing.T) {
	for mimeType, want := range map[string]event.MessageType{
		"image/png":                 event.MsgImage,
		"video/mp4":                 event.MsgVideo,
		"audio/mpeg":                event.MsgAudio,
		"application/pdf":           event.MsgFile,
		"text/plain; charset=utf-8": event.MsgFile,
	} {
		if got := msgTypeFor(mimeType); got != want {
			t.Errorf("%s gave %s", mimeType, got)
		}
	}
}

// A rejected offer is struck through and says so, and can't be answered any more.
// The plain body stays as OLX wrote it.
func TestSettledOffersSayWhatBecameOfThem(t *testing.T) {
	rejected := &event.MessageEventContent{Body: "Nowa propozycja: 100.00 zł"}
	addOfferState(rejected, olx.OfferRejected, textsFor("www.olx.pl").texts)
	if rejected.Body != "Nowa propozycja: 100.00 zł" || rejected.FormattedBody != "<del>Nowa propozycja: 100.00 zł</del><br><strong>Odrzucono</strong>" {
		t.Errorf("rejected = %q / %q", rejected.Body, rejected.FormattedBody)
	}
	accepted := &event.MessageEventContent{Body: "a <b"}
	addOfferState(accepted, olx.OfferAccepted, english)
	if accepted.FormattedBody != "a &lt;b<br><strong>Accepted</strong>" {
		t.Errorf("accepted = %q", accepted.FormattedBody)
	}
	theirs := olx.Message{IsOffer: true, Type: olx.MessageReceived, ThreadUUID: "t"}
	if !answerable(theirs) {
		t.Error("their waiting offer should be answerable")
	}
	theirs.OfferState = olx.OfferRejected
	if answerable(theirs) {
		t.Error("a rejected offer can't be answered")
	}
}

// Offers are asked about when the chat changes and now and then while one waits,
// but not once every one of them is settled.
func TestWhenOfferStatesAreAsked(t *testing.T) {
	client := &Client{}
	thread := olx.Thread{UUID: "t", Offers: []olx.Message{{UUID: "a", IsOffer: true, NegotiationID: "n"}}}
	if !client.offerStatesDue(thread, ThreadProgress{}, false) {
		t.Error("offers never asked about should be")
	}
	client.offersChecked = map[string]time.Time{"t": time.Now()}
	if client.offerStatesDue(thread, ThreadProgress{}, false) {
		t.Error("asked again straight away")
	}
	if !client.offerStatesDue(thread, ThreadProgress{}, true) {
		t.Error("a changed chat should ask again")
	}
	client.offersChecked["t"] = time.Now().Add(-offerStateInterval)
	if !client.offerStatesDue(thread, ThreadProgress{}, false) {
		t.Error("a waiting offer should be asked about again after a while")
	}
	settled := ThreadProgress{OfferStates: map[string]olx.OfferState{"a": olx.OfferRejected}}
	if client.offerStatesDue(thread, settled, true) {
		t.Error("settled offers were asked about")
	}
	messages := []olx.Message{{UUID: "a", IsOffer: true}, {UUID: "b"}}
	withStates(messages, map[string]olx.OfferState{"a": olx.OfferRejected, "b": olx.OfferAccepted})
	if messages[0].OfferState != olx.OfferRejected || messages[1].OfferState != "" {
		t.Errorf("unexpected states %+v", messages)
	}
}

// The bridge's own words are in the site's language, and every site has them all.
func TestEverySiteHasItsTexts(t *testing.T) {
	for _, site := range sites {
		words := textsFor("www.olx." + site.domain)
		if words.Language == "" || words.Language == english.Language {
			t.Errorf("olx.%s has no language of its own", site.domain)
		}
		if words.Respond == "" || words.Rejected == "" || words.Accepted == "" || words.NoContent == "" ||
			!strings.Contains(words.AttachmentFailed, "%s") {
			t.Errorf("olx.%s is missing words: %+v", site.domain, words)
		}
	}
	if got := textsFor("https://www.olx.pl/").Rejected; got != "Odrzucono" {
		t.Errorf("Polish rejected = %q", got)
	}
	if textsFor("127.0.0.1:8080").texts != english {
		t.Error("an unknown site should get English")
	}
	if got := textsFor("www.olx.ro").attachmentFailed(""); got != "Atașamentul nu a putut fi încărcat" {
		t.Errorf("unnamed attachment = %q", got)
	}
	if got := english.attachmentFailed("a.pdf"); got != "Couldn't load the attachment a.pdf" {
		t.Errorf("named attachment = %q", got)
	}
}

// Where a site is used in two languages, what goes under an offer follows the
// language of OLX's line, told by the letters only one of them has.
func TestTextsFollowTheOffersLine(t *testing.T) {
	ukraine, kazakhstan, poland := textsFor("www.olx.ua"), textsFor("www.olx.kz"), textsFor("www.olx.pl")
	for _, test := range []struct {
		site siteWords
		line string
		want string
	}{
		{ukraine, "Нова пропозиція: 100 грн", "uk"},
		{ukraine, "НОВА ПРОПОЗИЦІЯ: 100 грн", "uk"},
		{ukraine, "Новое предложение: 100 грн", "ru"},
		{ukraine, "100", "uk"},
		{ukraine, "", "uk"},
		{kazakhstan, "Жаңа ұсыныс: 100 ₸", "kk"},
		{kazakhstan, "Новое предложение: 100 ₸", "ru"},
		{kazakhstan, "100", "ru"},
		{poland, "Новое предложение: 100 zł", "pl"},
		{poland, "Nowa propozycja: 100 zł", "pl"},
	} {
		if got := test.site.under(test.line).Language; got != test.want {
			t.Errorf("under %q: %s, want %s", test.line, got, test.want)
		}
	}
	for _, words := range []texts{ukrainian, russian, kazakh} {
		if words.Respond == "" || words.Rejected == "" || words.Accepted == "" || words.NoContent == "" ||
			!strings.Contains(words.AttachmentFailed, "%s") {
			t.Errorf("%s is missing words: %+v", words.Language, words)
		}
	}
}
