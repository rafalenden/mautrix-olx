package main

import (
	"maunium.net/go/mautrix/bridgev2/matrix/mxmain"

	"github.com/rafalenden/mautrix-olx/pkg/connector"
)

// Filled in at build time with the -X linker flag.
var (
	Tag       = "unknown"
	Commit    = "unknown"
	BuildTime = "unknown"
)

var m = mxmain.BridgeMain{
	Name:        "mautrix-olx",
	URL:         "https://github.com/rafalenden/mautrix-olx",
	Description: "A Matrix-OLX puppeting bridge.",
	Version:     "0.1.0",
	Connector:   &connector.OLXConnector{},
}

func main() {
	m.InitVersion(Tag, Commit, BuildTime)
	m.Run()
}
