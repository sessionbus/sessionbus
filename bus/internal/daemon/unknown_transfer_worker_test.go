// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"bufio"
	"encoding/json"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestUnknownTransferWorkerRequiresFlagTransportAndIssuedPrefix(t *testing.T) {
	for _, test := range []struct {
		name, token                 string
		transfer, unknown, admitted bool
	}{
		{"legacy worker new token", "sbt1:av1:fp1:ut1:token-fixture", true, false, true},
		{"new worker new token", "sbt1:av1:fp1:ut1:token-fixture", true, true, true},
		{"new flag old token", "sbt1:av1:fp1:token-fixture", true, true, false},
		{"new flag without transfer", "sbt1:av1:fp1:ut1:token-fixture", false, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			d, s, peer := reviewSession(t)
			start := &launch{token: test.token, product: "fixture", entry: &entry{}, claimed: make(chan struct{}, 1), describe: true}
			d.directory.tokens[test.token] = start
			fields := map[string]any{"protocol": 1, "product": "fixture", "launch_token": test.token, "supported_open_fields": []string{}, "extra_arguments": []any{}, "supports_availability": true, "supports_policy": true}
			if test.transfer {
				fields["supports_transfer"] = true
			}
			if test.unknown {
				fields["supports_unknown_transfer"] = true
			}
			params, err := json.Marshal(fields)
			must(t, err)
			s.firstHello(protocol.Frame{ID: 1, Request: true, Method: "session.hello", Params: params})
			must(t, peer.SetReadDeadline(time.Now().Add(time.Second)))
			raw, err := bufio.NewReader(peer).ReadBytes('\n')
			must(t, err)
			frame, err := protocol.DecodeFrame(raw)
			must(t, err)
			if !test.admitted {
				if frame.Error == nil || frame.Error.Code != protocol.InvalidHello || d.directory.tokens[test.token] != start {
					t.Fatalf("incompatible opt-in consumed token/admitted: %s", raw)
				}
				return
			}
			if frame.Error != nil || string(frame.Result) != "{}" || d.directory.tokens[test.token] != nil || !s.supportsTransfer || !s.supportsAvailability || !s.supportsPolicy || s.unknownTransferCode != test.unknown {
				t.Fatalf("issued token/old flags/admission changed: %s", raw)
			}
		})
	}
}
