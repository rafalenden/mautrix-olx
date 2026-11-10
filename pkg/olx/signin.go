package olx

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// signInAgain gets a new session from the sign-in site's own cookie, without
// asking anything, as OLX's website does when its refresh token has stopped
// working: an authorization request with prompt=none and PKCE, whose redirect
// carries a code that's exchanged for tokens. It never follows the redirect, so
// the code doesn't reach the website. ErrSignedOut means the sign-in itself has
// ended; any other error may pass.
func (c *Client) signInAgain(ctx context.Context, cookie string) (Tokens, error) {
	verifier := randomString(32)
	challenge := sha256.Sum256([]byte(verifier))
	state := randomString(16)
	redirect := c.callbackURL()
	query := url.Values{
		"client_id":             {c.ClientID},
		"response_type":         {"code"},
		"scope":                 {"openid profile email offline_access"},
		"redirect_uri":          {redirect},
		"state":                 {state},
		"code_challenge":        {base64.RawURLEncoding.EncodeToString(challenge[:])},
		"code_challenge_method": {"S256"},
		"prompt":                {"none"},
	}
	authorize := c.AuthURL.JoinPath("oauth2/authorize")
	authorize.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, authorize.String(), nil)
	if err != nil {
		return Tokens{}, err
	}
	request.Header.Set("Cookie", cookie)
	noRedirects := *c.HTTP
	noRedirects.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := noRedirects.Do(request)
	if err != nil {
		return Tokens{}, redactError(err)
	}
	response.Body.Close()
	updated := mergeCookies(cookie, response.Cookies())

	code, err := codeFrom(response, redirect, state)
	if err != nil {
		return Tokens{}, err
	}
	form := url.Values{
		"grant_type":    {"authorization_code"},
		"client_id":     {c.ClientID},
		"code":          {code},
		"code_verifier": {verifier},
		"redirect_uri":  {redirect},
	}
	exchange, err := http.NewRequestWithContext(ctx, http.MethodPost, c.AuthURL.JoinPath("oauth2/token").String(), strings.NewReader(form.Encode()))
	if err != nil {
		return Tokens{}, err
	}
	exchange.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reply, err := c.HTTP.Do(exchange)
	if err != nil {
		return Tokens{}, redactError(err)
	}
	defer reply.Body.Close()
	if reply.StatusCode == http.StatusBadRequest || reply.StatusCode == http.StatusUnauthorized {
		return Tokens{}, fmt.Errorf("%w: %w", ErrSignedOut, readError(reply))
	} else if reply.StatusCode >= 300 {
		return Tokens{}, readError(reply)
	}
	var parsed tokenResponse
	if err = json.NewDecoder(reply.Body).Decode(&parsed); err != nil || parsed.AccessToken == "" || parsed.RefreshToken == "" {
		return Tokens{}, errors.New("couldn't read OLX's new session")
	}
	return Tokens{
		AccessToken:  parsed.AccessToken,
		RefreshToken: parsed.RefreshToken,
		ExpiresAt:    time.Now().Add(time.Duration(parsed.ExpiresIn) * time.Second),
		SignInCookie: updated,
	}, nil
}

// callbackURL is where the website has OLX send its sign-ins.
func (c *Client) callbackURL() string {
	return strings.TrimSuffix(c.SiteURL.String(), "/") + "/d/callback/"
}

// codeFrom reads the authorization code from the sign-in site's redirect. Being
// sent anywhere else, or shown the sign-in form, means the sign-in has ended.
func codeFrom(response *http.Response, redirect, state string) (string, error) {
	if response.StatusCode >= 500 || response.StatusCode == http.StatusTooManyRequests {
		return "", &Error{Status: response.StatusCode, Description: "OLX's sign-in is unavailable"}
	}
	location, err := response.Location()
	if response.StatusCode < 300 || response.StatusCode >= 400 || err != nil {
		return "", fmt.Errorf("%w: OLX asked to sign in again", ErrSignedOut)
	}
	if !strings.HasPrefix(location.String(), redirect) {
		return "", fmt.Errorf("%w: OLX asked to sign in again", ErrSignedOut)
	}
	values := location.Query()
	if reason := values.Get("error"); reason != "" {
		return "", fmt.Errorf("%w: OLX answered %s", ErrSignedOut, reason)
	}
	if values.Get("state") != state || values.Get("code") == "" {
		return "", errors.New("OLX's sign-in answered with something unexpected")
	}
	return values.Get("code"), nil
}

// mergeCookies applies cookies a response set to a Cookie header, so a sign-in
// that renews itself stays current.
func mergeCookies(header string, set []*http.Cookie) string {
	type pair struct{ name, value string }
	var pairs []pair
	for _, part := range strings.Split(header, ";") {
		name, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if ok && name != "" {
			pairs = append(pairs, pair{name, value})
		}
	}
	for _, cookie := range set {
		removed := cookie.MaxAge < 0 || (!cookie.Expires.IsZero() && cookie.Expires.Before(time.Now()))
		index := -1
		for i, existing := range pairs {
			if existing.name == cookie.Name {
				index = i
				break
			}
		}
		switch {
		case removed && index >= 0:
			pairs = append(pairs[:index], pairs[index+1:]...)
		case removed:
		case index >= 0:
			pairs[index].value = cookie.Value
		default:
			pairs = append(pairs, pair{cookie.Name, cookie.Value})
		}
	}
	parts := make([]string, len(pairs))
	for i, p := range pairs {
		parts[i] = p.name + "=" + p.value
	}
	return strings.Join(parts, "; ")
}

func randomString(size int) string {
	data := make([]byte, size)
	_, _ = rand.Read(data)
	return base64.RawURLEncoding.EncodeToString(data)
}
