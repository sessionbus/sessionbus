// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"reflect"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestLegacyFinalListOmitsOnlyMarkedHostsWithoutMutatingInput(t *testing.T) {
	for _, raw := range []bool{false, true} {
		t.Run(map[bool]string{false: "typed", true: "raw"}[raw], func(t *testing.T) {
			d := &Daemon{host: "alpha"}
			d.directory = newDirectory(d, nil)
			s := pullReviewSender(d)
			input := &protocol.SessionListResult{Sessions: []protocol.SessionSummary{}, Hosts: []protocol.HostProducts{{Host: "alpha", Products: []string{}}, {Host: "beta", Products: []string{}, Availability: "not_responding"}, {Host: "gamma", Products: []string{"peer"}}}, SelfInfo: listSelfInfo(s.federationCaller())}
			before, err := protocol.EncodeResult("session.list", input)
			if err != nil {
				t.Fatal(err)
			}
			response := answer{value: input}
			if raw {
				response = remoteAnswer("session.list", federation.Reply{Result: before})
			}
			frame := protocol.Frame{ID: 1, Method: "session.list", Request: true}
			s.finishRequest(1, &requestState{frame: frame, availability: false}, response)
			got := <-s.forwarded
			decoded, err := protocol.DecodeResult("session.list", got.Result)
			if err != nil {
				t.Fatal(err)
			}
			listed := decoded.(*protocol.SessionListResult)
			want := []protocol.HostProducts{{Host: "alpha", Products: []string{}}, {Host: "gamma", Products: []string{"peer"}}}
			if !reflect.DeepEqual(listed.Hosts, want) || len(listed.Sessions) != 0 || !reflect.DeepEqual(listed.SelfInfo, input.SelfInfo) {
				t.Errorf("final legacy response retained silent stub or changed healthy rows: %#v", listed)
			}
			after, _ := protocol.EncodeResult("session.list", input)
			if string(before) != string(after) {
				t.Fatal("final response projection mutated shared input", string(after))
			}
		})
	}
}

func TestAvailabilityAggregateFinalResponseKeepsAdmissionSnapshot(t *testing.T) {
	for _, initial := range []bool{false, true} {
		t.Run(map[bool]string{false: "old-to-new", true: "new-to-old"}[initial], func(t *testing.T) {
			d := &Daemon{host: "alpha"}
			d.config.Products = []string{"peer"}
			d.directory = newDirectory(d, nil)
			d.federation = &federationLink{inbox: make(chan any, 2), cancel: func() {}}
			s := pullReviewSender(d)
			defer close(s.identity.done)
			s.supportsAvailability = initial
			frame := protocol.Frame{ID: 1, Method: "session.list", Params: []byte(`{}`), Request: true}
			if !s.listFederated(frame) {
				t.Fatal("aggregate was not admitted")
			}
			hosts := (<-d.federation.inbox).(federation.HostsCall)
			s.supportsAvailability = !initial
			hosts.Reply <- federation.Reply{Result: []byte(`{"hosts":["beta","gamma"]}`)}
			for range 2 {
				var call federation.OutgoingCall
				select {
				case event := <-d.federation.inbox:
					call = event.(federation.OutgoingCall)
				case <-time.After(time.Second):
					t.Fatal("aggregate remote leg missing")
				}
				if call.Value.From.Availability != initial {
					t.Fatal("aggregate leg lost admission snapshot")
				}
				params, err := protocol.DecodeParams("session.list", call.Value.Request.Params)
				if err != nil {
					t.Fatal(err)
				}
				if params.(*protocol.SessionListRequest).Host == "beta" {
					call.Reply <- federation.Reply{Result: []byte(`{"sessions":[],"hosts":[{"host":"beta","products":[],"availability":"not_responding"}]}`)}
				} else {
					call.Reply <- silenceListed("gamma")
				}
			}
			select {
			case event := <-s.inbox:
				s.consumeReply(event.(replyEvent))
			case <-time.After(time.Second):
				t.Fatal("aggregate did not settle")
			}
			got := <-s.forwarded
			decoded, err := protocol.DecodeResult("session.list", got.Result)
			if err != nil {
				t.Fatal(err)
			}
			listed := decoded.(*protocol.SessionListResult)
			want := []protocol.HostProducts{{Host: "alpha", Products: []string{"peer"}}, {Host: "gamma", Products: []string{"peer"}}}
			if initial {
				want = []protocol.HostProducts{{Host: "alpha", Products: []string{"peer"}}, {Host: "beta", Products: []string{}, Availability: "not_responding"}, {Host: "gamma", Products: []string{"peer"}}}
			}
			if !reflect.DeepEqual(listed.Hosts, want) || len(listed.Sessions) != 1 || listed.Sessions[0].SessionID != "peer@gamma" || listed.SelfInfo == nil || listed.SelfInfo.SessionID != "sender@alpha" || s.owned != 0 || len(s.requests) != 0 {
				t.Fatalf("final aggregate lost original opt-in, rows or settlement: %#v", listed)
			}
		})
	}
}
