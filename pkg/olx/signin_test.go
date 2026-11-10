package olx

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"
)

// fakeSignIn plays OLX's sign-in site: a refresh token that has lapsed, and a
// sign-in cookie that still gets a new session, as the website's does.
func fakeSignIn(t *testing.T, cookieWorks bool) (*Client, *Tokens) {
	t.Helper()
	var challenge, callback string
	var saved Tokens
	var client *Client
	client = newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oauth2/authorize":
			query := r.URL.Query()
			if query.Get("prompt") != "none" || query.Get("code_challenge_method") != "S256" ||
				query.Get("client_id") != "client" || query.Get("response_type") != "code" {
				t.Errorf("unexpected authorize %v", query)
			}
			challenge, callback = query.Get("code_challenge"), query.Get("redirect_uri")
			if callback != client.callbackURL() {
				t.Errorf("redirect_uri %q", callback)
			}
			cookie, err := r.Cookie("cognito")
			if !cookieWorks || err != nil || cookie.Value != "session" {
				// Cognito sends you to its sign-in form.
				http.Redirect(w, r, "/login?client_id=client", http.StatusFound)
				return
			}
			http.SetCookie(w, &http.Cookie{Name: "cognito", Value: "renewed", Path: "/"})
			http.SetCookie(w, &http.Cookie{Name: "csrf-state", Value: "", MaxAge: -1})
			http.Redirect(w, r, callback+"?code=the-code&state="+url.QueryEscape(query.Get("state")), http.StatusFound)
		case r.URL.Path == "/oauth2/token":
			_ = r.ParseForm()
			switch r.Form.Get("grant_type") {
			case "refresh_token":
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprint(w, `{"error":"invalid_grant","error_description":"Invalid \"refresh_token\"."}`)
			case "authorization_code":
				verified := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
				if r.Form.Get("code") != "the-code" || r.Form.Get("redirect_uri") != callback ||
					base64.RawURLEncoding.EncodeToString(verified[:]) != challenge || r.Form.Has("client_secret") {
					t.Errorf("unexpected exchange %v", r.Form)
				}
				fmt.Fprint(w, `{"access_token":"new-access","refresh_token":"new-refresh","expires_in":900}`)
			}
		default:
			if r.Header.Get("Authorization") != "Bearer new-access" {
				t.Errorf("sent %q", r.Header.Get("Authorization"))
			}
			fmt.Fprint(w, `{"data":[]}`)
		}
	}, Tokens{AccessToken: "old", RefreshToken: "lapsed", ExpiresAt: time.Now().Add(-time.Minute),
		SignInCookie: "cognito=session; csrf-state=x"})
	client.OnTokens = func(tokens Tokens) { saved = tokens }
	return client, &saved
}

func TestLapsedSessionSignsInAgainWithTheSignInCookie(t *testing.T) {
	client, saved := fakeSignIn(t, true)
	if _, err := client.ListThreads(context.Background()); err != nil {
		t.Fatal(err)
	}
	if saved.AccessToken != "new-access" || saved.RefreshToken != "new-refresh" {
		t.Errorf("new session not saved: %+v", saved)
	}
	if saved.SignInCookie != "cognito=renewed" {
		t.Errorf("sign-in cookie = %q, want the renewed one with the removed one gone", saved.SignInCookie)
	}
}

func TestEndedSignInSignsOut(t *testing.T) {
	client, _ := fakeSignIn(t, false)
	if _, err := client.ListThreads(context.Background()); !errors.Is(err, ErrSignedOut) {
		t.Errorf("got %v, want signed out", err)
	}
}

func TestMergeCookies(t *testing.T) {
	got := mergeCookies("a=1; b=2", []*http.Cookie{
		{Name: "b", Value: "3"}, {Name: "c", Value: "4"}, {Name: "a", MaxAge: -1},
	})
	if got != "b=3; c=4" {
		t.Errorf("got %q", got)
	}
}
