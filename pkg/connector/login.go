package connector

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"

	"github.com/rafalenden/servuso/bridges/olx/pkg/olx"
)

const (
	stepIDWebsite  = "com.enden.servuso.olx.login.website"
	stepIDComplete = "com.enden.servuso.olx.login.complete"
	fieldAccess    = "com.enden.servuso.olx.login.access_token"
	fieldRefresh   = "com.enden.servuso.olx.login.refresh_token"
	fieldClientID  = "com.enden.servuso.olx.login.client_id"
	fieldSignIn    = "com.enden.servuso.olx.login.sign_in_cookie"
	// The site of accounts signed in before each kept its own.
	defaultSite = "www.olx.pl"
)

// The sign-in choices, one per site. The domain is also the flow's ID.
var sites = []struct{ domain, country string }{
	{"bg", "Bulgaria"},
	{"kz", "Kazakhstan"},
	{"pl", "Poland"},
	{"pt", "Portugal"},
	{"ro", "Romania"},
	{"ua", "Ukraine"},
}

func (c *OLXConnector) GetLoginFlows() []bridgev2.LoginFlow {
	flows := make([]bridgev2.LoginFlow, 0, len(sites))
	for _, site := range sites {
		flows = append(flows, bridgev2.LoginFlow{
			Name:        "OLX " + site.country,
			Description: "Sign in on olx." + site.domain,
			ID:          site.domain,
		})
	}
	return flows
}

func (c *OLXConnector) CreateLogin(ctx context.Context, user *bridgev2.User, flowID string) (bridgev2.LoginProcess, error) {
	for _, site := range sites {
		if site.domain == flowID {
			address := "www.olx." + site.domain
			if c.Config.Site != "" {
				address = c.Config.Site
			}
			return &websiteLogin{main: c, user: user, site: address}, nil
		}
	}
	return nil, bridgev2.ErrInvalidLoginFlowID
}

// websiteLogin signs in on OLX's own site and reuses the session the site stores,
// including the refresh token it renews itself with. See olx.ChatClient.
type websiteLogin struct {
	main *OLXConnector
	user *bridgev2.User
	site string
}

var _ bridgev2.LoginProcessCookies = (*websiteLogin)(nil)

func (l *websiteLogin) Cancel() {}

func (l *websiteLogin) Start(ctx context.Context) (*bridgev2.LoginStep, error) {
	site, err := olx.SiteURL(l.site)
	if err != nil {
		return nil, err
	}
	field := func(id string) bridgev2.LoginCookieField {
		return bridgev2.LoginCookieField{
			ID:       id,
			Required: true,
			Sources:  []bridgev2.LoginCookieFieldSource{{Type: bridgev2.LoginCookieTypeSpecial, Name: id}},
		}
	}
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeCookies,
		StepID:       stepIDWebsite,
		Instructions: "Sign in to OLX.",
		CookiesParams: &bridgev2.LoginCookiesParams{
			URL:       site.JoinPath("konto/").String(),
			ExtractJS: extractSessionScript,
			// The session is only in the site's storage under a name that varies,
			// so a client that can't run the script can't sign in.
			JSRequired: true,
			Fields: []bridgev2.LoginCookieField{
				field(fieldAccess), field(fieldRefresh), field(fieldClientID), signInCookieField(site),
			},
		},
	}, nil
}

// signInCookieField asks for the cookies of the sign-in site, which is where you
// sign in, so a session that lapses can be renewed as the website renews its
// own. A client that can't read them still signs in, but has to sign in again
// once the refresh token lapses.
func signInCookieField(site *url.URL) bridgev2.LoginCookieField {
	auth := olx.NewClient(site, "", "", olx.Tokens{}).AuthURL
	return bridgev2.LoginCookieField{
		ID: fieldSignIn,
		Sources: []bridgev2.LoginCookieFieldSource{{
			Type:            bridgev2.LoginCookieTypeRequestHeader,
			Name:            "Cookie",
			RequestURLRegex: "^" + regexp.QuoteMeta(auth.Scheme+"://"+auth.Host) + "/",
		}},
	}
}

// extractSessionScript reads the session OLX's site keeps in its local storage. As
// bridgev2 describes, it evaluates to a promise that resolves once the fields are
// there, which is after the user has signed in.
var extractSessionScript = fmt.Sprintf(`new Promise((resolve) => {
  const read = () => {
    const key = Object.keys(localStorage).find(k => k.startsWith("@@auth0spajs@@::") && k.includes("::default::"));
    if (!key) { return false; }
    const body = (JSON.parse(localStorage.getItem(key)) || {}).body;
    if (!body || !body.access_token || !body.refresh_token || !body.client_id) { return false; }
    resolve({%q: body.access_token, %q: body.refresh_token, %q: body.client_id});
    return true;
  };
  if (!read()) {
    const timer = setInterval(() => { if (read()) { clearInterval(timer); } }, 500);
  }
})`, fieldAccess, fieldRefresh, fieldClientID)

func (l *websiteLogin) SubmitCookies(ctx context.Context, cookies map[string]string) (*bridgev2.LoginStep, error) {
	access, refresh, clientID := cookies[fieldAccess], cookies[fieldRefresh], cookies[fieldClientID]
	if access == "" || refresh == "" || clientID == "" {
		return nil, errors.New("OLX's site didn't provide a complete session")
	}
	claims, err := olx.ParseSession(access)
	if err != nil {
		return nil, err
	}
	site, err := olx.SiteURL(l.site)
	if err != nil {
		return nil, err
	}
	tokens := olx.Tokens{
		AccessToken: access, RefreshToken: refresh, ExpiresAt: claims.ExpiresAt,
		SignInCookie: cookies[fieldSignIn],
	}
	// Fails here rather than later if the session doesn't work outside the browser.
	if _, err = olx.NewClient(site, clientID, claims.UserUUID, tokens).ListThreads(ctx); err != nil {
		return nil, fmt.Errorf("OLX didn't accept the session from the bridge: %w", err)
	}
	name := claims.Email
	if name == "" {
		name = "OLX account"
	}
	login, err := l.user.NewLogin(ctx, &database.UserLogin{
		ID:         networkid.UserLoginID(claims.UserUUID),
		RemoteName: name,
		RemoteProfile: status.RemoteProfile{
			Name:  name,
			Email: claims.Email,
		},
		Metadata: &UserLoginMetadata{
			Tokens:          tokens,
			Site:            l.site,
			WebsiteClientID: clientID,
			UserUUID:        claims.UserUUID,
		},
	}, &bridgev2.NewLoginParams{DeleteOnConflict: true})
	if err != nil {
		return nil, fmt.Errorf("couldn't save the login: %w", err)
	}
	login.Client.Connect(login.Log.WithContext(l.main.Bridge.BackgroundCtx))
	return &bridgev2.LoginStep{
		Type:           bridgev2.LoginStepTypeComplete,
		StepID:         stepIDComplete,
		Instructions:   "Signed in as " + name,
		CompleteParams: &bridgev2.LoginCompleteParams{UserLoginID: login.ID, UserLogin: login},
	}, nil
}
