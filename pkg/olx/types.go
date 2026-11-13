package olx

import (
	"strings"
	"time"
)

// Tokens are a user's session. Access tokens last 15 minutes, and the refresh
// token replaces them.
type Tokens struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	ExpiresAt    time.Time `json:"expires_at"`
	// SignInCookie is the Cookie header for the sign-in site (login.olx.pl). The
	// refresh token stops working after a while unused, such as while the Mac
	// sleeps; this sign-in outlasts it, and gets a new session as OLX's own
	// website does then. Empty for an account signed in without it.
	SignInCookie string `json:"sign_in_cookie,omitempty"`
}

type User struct {
	ID   int64
	Name string
}

// Thread is a conversation with one person about one advert.
type Thread struct {
	UUID           string
	AdvertID       int64
	InterlocutorID int64
	TotalCount     int
	UnreadCount    int
	// LatestKey is the newest message's key.
	LatestKey string
	// LatestOwn is the newest message you wrote yourself.
	LatestOwn string
	// LatestTheirOffer is the other person's newest offer, if they've made one.
	LatestTheirOffer *Message
	// Offers is every step of a price negotiation in the thread, oldest first.
	Offers []Message
	// ReadByThem is the newest of your messages the other person has read, and
	// ReadByThemAt when they read it. OLX says when each message was read, but
	// not when one reached the other side.
	ReadByThem   string
	ReadByThemAt string
	Listing      Listing
}

// Listing is the advert a thread is about.
type Listing struct {
	AdvertID   int64
	Title      string
	ImageURL   string
	PriceMinor *int64
	Currency   string
	// Where the user acts on OLX itself: the advert's page and this conversation.
	AdvertURL       string
	ConversationURL string
}

// Key is the thread's stable identifier.
func (t Thread) Key() string { return t.UUID }

type MessageType string

const (
	MessageSent     MessageType = "sent"
	MessageReceived MessageType = "received"
)

// OfferState is what became of an offer. OLX's chat doesn't say: its price
// negotiation service does, per negotiation.
type OfferState string

const (
	// OfferPending is an offer still waiting for an answer.
	OfferPending OfferState = "pending"
	// OfferRejected is one that was turned down, or that a newer offer replaced,
	// which OLX's site shows the same way.
	OfferRejected OfferState = "rejected"
	OfferAccepted OfferState = "accepted"
)

// Settled is whether the offer has been answered, so nothing more happens to it.
func (s OfferState) Settled() bool { return s == OfferRejected || s == OfferAccepted }

type Attachment struct {
	Name string
	URL  string
}

type Message struct {
	UUID        string
	ThreadUUID  string
	CreatedAt   string
	Type        MessageType
	Text        string
	Attachments []Attachment
	// IsSystem marks a line from OLX itself rather than a person.
	IsSystem bool
	// IsOffer marks a step in a price negotiation, which is answered on OLX.
	IsOffer bool
	// NegotiationID is the negotiation an offer belongs to, which is where OLX
	// keeps what became of it.
	NegotiationID string
	// OfferState is what became of an offer, if that's known.
	OfferState OfferState
	// ReadAt is when the person it was sent to read it, if they have.
	ReadAt string
}

// Key is the message's stable identifier.
func (m Message) Key() string { return m.UUID }

// ParseTime reads the API's timestamps. One with no zone is taken as the site's
// local time. It returns the zero time if the value can't be read.
func ParseTime(value string, location *time.Location) time.Time {
	value = strings.TrimSpace(value)
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05Z0700", "2006-01-02 15:04:05Z07:00"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed
		}
	}
	if location == nil {
		location = time.UTC
	}
	for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02T15:04:05"} {
		if parsed, err := time.ParseInLocation(layout, value, location); err == nil {
			return parsed
		}
	}
	return time.Time{}
}
