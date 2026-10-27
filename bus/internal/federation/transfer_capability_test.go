// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"crypto/tls"
	"encoding/base64"
	"testing"
)

func TestTransferALPNRetainsEveryLegacyFallback(t *testing.T) {
	secret := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	for _, older := range []string{WakeALPN, TraceALPN, RosterALPN, ""} {
		for _, oldSide := range []string{"client", "server"} {
			t.Run(oldSide+"/"+older, func(t *testing.T) {
				server, e := ServerTLS(map[string]string{"alpha": secret})
				if e != nil {
					t.Fatal(e)
				}
				client, e := ClientTLS("alpha", secret)
				if e != nil {
					t.Fatal(e)
				}
				var legacy []string
				if older != "" {
					legacy = []string{older}
				}
				if oldSide == "client" {
					client.NextProtos = legacy
				} else {
					server.NextProtos = legacy
					choose := server.GetConfigForClient
					server.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
						configured, err := choose(hello)
						if err != nil {
							return nil, err
						}
						copy := configured.Clone()
						copy.NextProtos = legacy
						return copy, nil
					}
				}
				cs, ss, ce, se := handshake(client, server)
				if ce != nil || se != nil || cs.NegotiatedProtocol != older || ss.NegotiatedProtocol != older {
					t.Fatal(cs.NegotiatedProtocol, ss.NegotiatedProtocol, ce, se)
				}
				if cs.NegotiatedProtocol == TransferALPN {
					t.Fatal("old parser gained transfer capability")
				}
			})
		}
	}
}

func TestTransferSupersetCannotSilentlyDisableOldCapabilities(t *testing.T) {
	for _, c := range []struct {
		p                   string
		wake, trace, roster bool
	}{
		{TransferALPN, true, true, true}, {WakeALPN, true, true, true}, {TraceALPN, false, true, true}, {RosterALPN, false, false, true}, {"", false, false, false},
	} {
		if supportsWakeProtocol(c.p) != c.wake || supportsTraceProtocol(c.p) != c.trace || supportsRosterProtocol(c.p) != c.roster {
			t.Fatal(c)
		}
	}
}
