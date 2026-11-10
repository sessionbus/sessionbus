// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"testing"

	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestPolicyDirectedAndAggregateFinalRepliesUseAdmissionSnapshot(t *testing.T) {
	for _, aggregate := range []bool{false, true} {
		for _, initial := range []bool{false, true} {
			t.Run(map[bool]string{false: "directed", true: "aggregate"}[aggregate]+map[bool]string{false: "old-new", true: "new-old"}[initial], func(t *testing.T) {
				d := &Daemon{host: "alpha"}
				d.directory = newDirectory(d, nil)
				d.federation = &federationLink{inbox: make(chan any, 4), cancel: func() {}}
				s := pullReviewSender(d)
				defer close(s.identity.done)
				s.supportsPolicy = initial
				frame := protocol.Frame{ID: 1, Method: "turn.interrupt", Params: []byte(`{"session_id":"child@beta"}`), Request: true}
				if aggregate {
					frame.Method = "message.send"
					frame.Params = []byte(`{"targets":["child@beta","other@gamma"],"message":"test"}`)
					params, err := protocol.DecodeParams(frame.Method, frame.Params)
					if err != nil {
						t.Fatal(err)
					}
					if !s.sendFederated(frame, params.(*protocol.MessageSendRequest), "policy-test-message") {
						t.Fatal("aggregate not admitted")
					}
				} else {
					s.forward(frame, "child@beta")
				}
				s.supportsPolicy = !initial
				count := 1
				if aggregate {
					count = 2
				}
				for range count {
					call := (<-d.federation.inbox).(federation.OutgoingCall)
					if call.Value.From.Policy != initial {
						t.Fatal("leg used current hello")
					}
					raw := []byte(`{"code":-32017,"message":"federation_policy_denied"}`)
					value, err := protocol.DecodeError(raw)
					if err != nil {
						t.Fatal(err)
					}
					call.Reply <- federation.Reply{Error: value, ErrorRaw: raw}
				}
				s.consumeReply((<-s.inbox).(replyEvent))
				reply := <-s.forwarded
				if aggregate {
					decoded, err := protocol.DecodeResult("message.send", reply.Result)
					if err != nil {
						t.Fatal(err)
					}
					for _, delivery := range decoded.(*protocol.MessageSendResult).Deliveries {
						want := "unknown_session"
						if initial {
							want = protocol.PolicyReason
						}
						if delivery.Disposition != "rejected" || delivery.Reason != want {
							t.Fatal("collector lost policy or snapshot", delivery)
						}
					}
				} else {
					want := protocol.UnknownSession
					if initial {
						want = protocol.PolicyDenied
					}
					if reply.Error == nil || reply.Error.Code != want {
						t.Fatal("raw-backed final reply used current opt-in", reply)
					}
				}
				if s.owned != 0 || len(s.requests) != 0 {
					t.Fatal("snapshot test left admitted work")
				}
			})
		}
	}
}
