// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"crypto/tls"
	"encoding/json"
	"strings"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestAvailabilityALPNRetainsAllOldLinkCombinations(t *testing.T) {
	for _, older := range []string{TransferALPN, WakeALPN, TraceALPN, RosterALPN, ""} {
		for _, side := range []string{"client", "server"} {
			t.Run(side+"/"+older, func(t *testing.T) {
				server, err := ServerTLS(map[string]string{"alpha": testSecret('a')})
				if err != nil {
					t.Fatal(err)
				}
				client, err := ClientTLS("alpha", testSecret('a'))
				if err != nil {
					t.Fatal(err)
				}
				var old []string
				if older != "" {
					old = []string{older}
				}
				if side == "client" {
					client.NextProtos = old
				} else {
					choose := server.GetConfigForClient
					server.GetConfigForClient = func(h *tls.ClientHelloInfo) (*tls.Config, error) {
						c, e := choose(h)
						if e != nil {
							return nil, e
						}
						copy := c.Clone()
						copy.NextProtos = old
						return copy, nil
					}
				}
				cs, ss, ce, se := handshake(client, server)
				if ce != nil || se != nil || cs.NegotiatedProtocol != older || ss.NegotiatedProtocol != older {
					t.Fatal(cs.NegotiatedProtocol, ss.NegotiatedProtocol, ce, se)
				}
			})
		}
	}
	if !supportsWakeProtocol(AvailabilityALPN) || !supportsTraceProtocol(AvailabilityALPN) || !supportsRosterProtocol(AvailabilityALPN) {
		t.Fatal("new token disabled an existing capability")
	}
}

func TestTraceOwnerRequestOptInDoesNotEscapeIntoLegacyReply(t *testing.T) {
	owner := testTraceCaller()
	owner.Availability = true
	input := Reply{Result: testTraceResult(t), Trace: []TraceRecipient{{Child: "child@beta", Target: "child@beta", Mode: "events", Version: "v1", Owner: owner}}}
	got, err := encodeTraceReply(input)
	if err != nil {
		t.Fatal(err)
	}
	legacy := input
	legacy.Trace = append([]TraceRecipient(nil), input.Trace...)
	legacy.Trace[0].Owner.Availability = false
	want, err := encodeTraceReply(legacy)
	if err != nil || string(got) != string(want) || !input.Trace[0].Owner.Availability || !owner.Availability {
		t.Fatalf("legacy identity projection mutated input or changed identity: %s/%s %v", got, want, err)
	}
	var envelope traceReplyEnvelope
	if err := json.Unmarshal(got, &envelope); err != nil || len(envelope.Trace) != 1 || strings.Contains(string(envelope.Trace[0]), `"availability"`) {
		t.Fatal("request-only key leaked to an old strict owner parser", string(got), err)
	}
}

func TestAvailabilityProjectionUsesCapturedOptInAndPreservesOldBytes(t *testing.T) {
	original := Reply{Result: []byte(`{"sessions":[],"hosts":[{"host":"beta","products":[],"availability":"not_responding"}]}`)}
	newReply := ProjectAvailability("session.list", original, true)
	oldReply := ProjectAvailability("session.list", original, false)
	if !strings.Contains(string(newReply.Result), `"availability"`) || strings.Contains(string(oldReply.Result), `"availability"`) {
		t.Fatal("old-hop projection failed", newReply, oldReply)
	}
	for _, capable := range []bool{false, true} {
		plain := Reply{Result: []byte(`{"hosts":[], "sessions":[]}`)}
		if got := ProjectAvailability("session.list", plain, capable); string(got.Result) != string(plain.Result) {
			t.Fatal("ordinary legacy bytes changed", got)
		}
	}
	for _, capable := range []bool{false, true} {
		lost := unavailableList("beta", capable)
		decoded, err := protocol.DecodeResult("session.list", lost.Result)
		if err != nil {
			t.Fatal(err)
		}
		listed := decoded.(*protocol.SessionListResult)
		if len(listed.Sessions) != 0 || (len(listed.Hosts) > 0) != capable {
			t.Fatal("partial-list legacy boundary", listed)
		}
	}
}
