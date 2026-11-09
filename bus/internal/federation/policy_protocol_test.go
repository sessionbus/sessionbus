// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestPolicyErrorRawAndTraceProjectionPreservesLegacyAndInputs(t *testing.T) {
	owner := testTraceCaller()
	owner.Availability = true
	owner.Policy = true
	original := errorReply(protocol.PolicyDenied, nil)
	original.Trace = []TraceRecipient{{Child: "child@beta", Target: "child@beta", Mode: "events", Version: "v1", Owner: owner}}
	for _, enabled := range []bool{false, true} {
		reply := ProjectPolicy("message.send", original, enabled)
		for _, internal := range []bool{false, true} {
			body, err := responseBytes(1, reply, internal)
			if err != nil {
				t.Fatal(err)
			}
			frame, err := protocol.DecodeFrame(body[:len(body)-1])
			if err != nil {
				t.Fatal(err)
			}
			actual := frame.Error
			if internal {
				var envelope traceReplyEnvelope
				if err := json.Unmarshal(frame.Result, &envelope); err != nil {
					t.Fatal(err)
				}
				actual, err = protocol.DecodeError(envelope.Error)
				if err != nil {
					t.Fatal(err)
				}
				if len(envelope.Trace) != 1 {
					t.Fatal("lost recipients")
				}
				var recipient map[string]json.RawMessage
				_ = json.Unmarshal(envelope.Trace[0], &recipient)
				var legacy legacyTransferCaller
				if err := protocol.DecodeJSON(recipient["owner"], &legacy); err != nil {
					t.Fatal("new trace-owner key escaped", err)
				}
			}
			want := protocol.UnknownSession
			if enabled {
				want = protocol.PolicyDenied
			}
			if actual == nil || actual.Code != want {
				t.Fatal("raw error overrode projection", string(body))
			}
		}
	}
	if original.Error.Code != protocol.PolicyDenied || !original.Trace[0].Owner.Policy || !owner.Policy {
		t.Fatal("shared original mutated")
	}
	legacy := errorReply(protocol.Busy, nil)
	if got := ProjectPolicy("turn.run", legacy, false); !reflect.DeepEqual(got, legacy) {
		t.Fatal("legacy bytes/fields changed")
	}
	input := protocol.MessageSendResult{MessageID: "m", Deliveries: []protocol.MessageSendDelivery{{Target: "x@beta", Disposition: "rejected", Reason: protocol.PolicyReason}, {Target: "y@beta", Disposition: "rejected", Reason: "custom product reason"}}}
	raw, err := protocol.EncodeResult("message.send", input)
	if err != nil {
		t.Fatal(err)
	}
	projected := ProjectPolicy("message.send", Reply{Result: raw}, false)
	decoded, err := protocol.DecodeResult("message.send", projected.Result)
	if err != nil {
		t.Fatal(err)
	}
	deliveries := decoded.(*protocol.MessageSendResult).Deliveries
	if deliveries[0].Reason != "unknown_session" || deliveries[1].Reason != "custom product reason" || input.Deliveries[0].Reason != protocol.PolicyReason {
		t.Fatal("reason projection narrowed arbitrary native reasons", deliveries)
	}
}

func TestPolicyALPNSupersetAndLegacyNegotiation(t *testing.T) {
	for _, older := range []string{AvailabilityALPN, TransferALPN, WakeALPN, TraceALPN, RosterALPN, ""} {
		for _, side := range []string{"client", "server"} {
			t.Run(side+older, func(t *testing.T) {
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
				cs, ss, ce, se := policyHandshake(client, server)
				if ce != nil || se != nil || cs.NegotiatedProtocol != older || ss.NegotiatedProtocol != older {
					t.Fatal(cs.NegotiatedProtocol, ss.NegotiatedProtocol, ce, se)
				}
			})
		}
	}
	if !supportsWakeProtocol(PolicyALPN) || !supportsTraceProtocol(PolicyALPN) || !supportsRosterProtocol(PolicyALPN) {
		t.Fatal("new token disabled old predicates")
	}
}

func TestPolicyCallerAndRepliesProjectedAtEveryOlderHop(t *testing.T) {
	for _, old := range []string{"", "alpha", "beta", "hub"} {
		t.Run(old, func(t *testing.T) {
			var hub *Hub
			var address string
			var secrets map[string]string
			if old != "hub" {
				hub, address, secrets = testHub(t)
			} else {
				// This listener negotiates the exact previous ALPN. Actual old executable
				// coverage is recorded separately; this control inspects each wire hop.
				hub, address, secrets = testPolicyLegacyHub(t)
			}
			_ = hub
			seen := make(chan IncomingCall, 2)
			connect := func(host string) *testDaemon {
				cfg, err := ClientTLS(host, secrets[host])
				if err != nil {
					t.Fatal(err)
				}
				if old == host {
					cfg.NextProtos = []string{AvailabilityALPN}
				}
				return connectTestDaemonTLS(t, address, host, cfg, func(call IncomingCall) (Wait, error) {
					seen <- call
					return immediate(errorReply(protocol.PolicyDenied, nil)), nil
				})
			}
			beta, alpha := connect("beta"), connect("alpha")
			_ = callHosts(t, beta)
			_ = callHosts(t, alpha)
			caller := policyTestCaller()
			caller.Policy = true
			reply := policyTestForward(t, alpha, caller, policyTestRequest(t, "turn.interrupt", protocol.SessionTarget{SessionID: "x@beta"}))
			got := policyTestSeen(t, seen)
			enabled := old == ""
			if got.From.Policy != enabled || reply.Error == nil || ((reply.Error.Code == protocol.PolicyDenied) != enabled) {
				t.Fatal("new flag/code crossed old hop", old, got.From, reply.Error)
			}
			if !caller.Policy || len(callHosts(t, alpha)) != 1 {
				t.Fatal("projection mutated caller or closed link")
			}
		})
	}
}

func testPolicyLegacyHub(t *testing.T) (*Hub, string, map[string]string) {
	t.Helper()
	secrets := map[string]string{"alpha": testSecret('a'), "beta": testSecret('b')}
	cfg, err := ServerTLS(secrets)
	if err != nil {
		t.Fatal(err)
	}
	choose := cfg.GetConfigForClient
	cfg.GetConfigForClient = func(h *tls.ClientHelloInfo) (*tls.Config, error) {
		c, e := choose(h)
		if e != nil {
			return nil, e
		}
		copy := c.Clone()
		copy.NextProtos = []string{AvailabilityALPN}
		return copy, nil
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hub := StartHub(listener, cfg, io.Discard)
	t.Cleanup(hub.Close)
	return hub, listener.Addr().String(), secrets
}

// Close the underlying pipe after both handshakes have joined. Serial TLS
// close_notify on an unread in-memory pipe spends five seconds per case; that
// is fixture shutdown, not a negotiation or production transport timeout.
func policyHandshake(clientConfig, serverConfig *tls.Config) (tls.ConnectionState, tls.ConnectionState, error, error) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	_ = left.SetDeadline(time.Now().Add(2 * time.Second))
	_ = right.SetDeadline(time.Now().Add(2 * time.Second))
	client, server := tls.Client(left, clientConfig), tls.Server(right, serverConfig)
	done := make(chan error, 1)
	go func() { done <- server.Handshake() }()
	ce := client.Handshake()
	se := <-done
	cs, ss := client.ConnectionState(), server.ConnectionState()
	_ = left.Close()
	_ = right.Close()
	_ = client.Close()
	_ = server.Close()
	return cs, ss, ce, se
}
