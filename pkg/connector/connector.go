// Package connector connects OLX's messages to Matrix through the mautrix
// bridgev2 framework.
package connector

import (
	"context"
	_ "embed"
	"strings"
	"time"
	_ "time/tzdata"

	up "go.mau.fi/util/configupgrade"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
)

//go:embed example-config.yaml
var exampleConfig string

type Config struct {
	Site         string `yaml:"site"`
	PollInterval int    `yaml:"poll_interval"`
}

func upgradeConfig(helper up.Helper) {
	helper.Copy(up.Str, "site")
	helper.Copy(up.Int, "poll_interval")
}

type OLXConnector struct {
	Bridge *bridgev2.Bridge
	Config Config
}

var _ bridgev2.NetworkConnector = (*OLXConnector)(nil)

func (c *OLXConnector) Init(bridge *bridgev2.Bridge) {
	c.Bridge = bridge
}

func (c *OLXConnector) GetConfig() (string, any, up.Upgrader) {
	return exampleConfig, &c.Config, up.SimpleUpgrader(upgradeConfig)
}

func (c *OLXConnector) Start(ctx context.Context) error {
	return nil
}

func (c *OLXConnector) GetName() bridgev2.BridgeName {
	return bridgev2.BridgeName{
		DisplayName:      "OLX",
		NetworkURL:       "https://www.olx.pl",
		NetworkID:        "olx",
		BeeperBridgeType: "olx",
		DefaultPort:      29313,
	}
}

func (c *OLXConnector) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	return &bridgev2.NetworkGeneralCapabilities{}
}

func (c *OLXConnector) GetBridgeInfoVersion() (info, capabilities int) {
	return 1, 1
}

func (c *OLXConnector) GetDBMetaTypes() database.MetaTypes {
	return database.MetaTypes{
		UserLogin: func() any { return &UserLoginMetadata{} },
	}
}

func (c *OLXConnector) LoadUserLogin(ctx context.Context, login *bridgev2.UserLogin) error {
	client, err := newClient(c, login)
	if err != nil {
		return err
	}
	login.Client = client
	return nil
}

// siteLocation is the time zone of a site's timestamps, which often don't state one.
func siteLocation(site string) *time.Location {
	zones := map[string]string{
		".pl": "Europe/Warsaw",
		".ro": "Europe/Bucharest",
		".bg": "Europe/Sofia",
		".pt": "Europe/Lisbon",
		".ua": "Europe/Kyiv",
		".kz": "Asia/Almaty",
	}
	site = strings.TrimRight(strings.TrimSpace(site), "/")
	for suffix, zone := range zones {
		if strings.HasSuffix(site, suffix) {
			if location, err := time.LoadLocation(zone); err == nil {
				return location
			}
		}
	}
	return time.UTC
}
