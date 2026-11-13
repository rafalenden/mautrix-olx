package connector

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"github.com/rafalenden/mautrix-olx/pkg/olx"
)

func quickRetries(t *testing.T) {
	saved := sendRetryDelays
	sendRetryDelays = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	t.Cleanup(func() { sendRetryDelays = saved })
}

var offline = &net.DNSError{Err: "no such host", Name: "example.test", IsNotFound: true}

// A send is tried again while OLX can't be reached, and goes through once it can.
func TestASendIsTriedAgainWhileOLXCantBeReached(t *testing.T) {
	quickRetries(t)
	var attempts []int
	err := whileUnreachable(context.Background(), func(attempt int) error {
		attempts = append(attempts, attempt)
		if attempt < 2 {
			return offline
		}
		return nil
	})
	if err != nil || len(attempts) != 3 || attempts[2] != 2 {
		t.Errorf("err %v after attempts %v, want success on the third", err, attempts)
	}
}

// It gives up after the last wait, with the failure a client can offer to retry.
func TestASendGivesUpAfterTheLastWait(t *testing.T) {
	quickRetries(t)
	count := 0
	err := whileUnreachable(context.Background(), func(int) error { count++; return offline })
	if !olx.IsUnreachable(err) || count != len(sendRetryDelays)+1 {
		t.Errorf("err %v after %d tries", err, count)
	}
	var status bridgev2.MessageStatus
	if !errors.As(errUnreachableSend(err), &status) || status.Status != event.MessageStatusRetriable || status.Message == "" {
		t.Errorf("status %+v, want a retriable failure with its reason", status)
	}
}

// What OLX itself refuses isn't tried again: it would be refused again.
func TestARefusalIsntTriedAgain(t *testing.T) {
	quickRetries(t)
	for _, refusal := range []error{&olx.Error{Status: 400}, olx.ErrSignedOut, &olx.Error{Status: 500}} {
		count := 0
		err := whileUnreachable(context.Background(), func(int) error { count++; return refusal })
		if count != 1 || !errors.Is(err, refusal) {
			t.Errorf("%v: tried %d times", refusal, count)
		}
	}
}

func TestWhatCountsAsUnreachable(t *testing.T) {
	timeout := &net.OpError{Op: "dial", Err: context.DeadlineExceeded}
	for _, err := range []error{offline, timeout, context.DeadlineExceeded} {
		if !olx.IsUnreachable(err) {
			t.Errorf("%v should count as unreachable", err)
		}
	}
	for _, err := range []error{nil, context.Canceled, &olx.Error{Status: 429}, errors.New("couldn't read OLX's response")} {
		if olx.IsUnreachable(err) {
			t.Errorf("%v shouldn't count as unreachable", err)
		}
	}
}
