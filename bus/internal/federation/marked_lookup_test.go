// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func lookupAnswer(t *testing.T, replies <-chan Reply) Reply {
	t.Helper()
	select {
	case reply := <-replies:
		return reply
	case <-time.After(time.Second):
		t.Fatal("lookup reply missing")
		return Reply{}
	}
}

func TestMarkedLookupPendingAndImmediateOnDestinationOwner(t *testing.T) {
	for _, optIn := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "opted-in"}[optIn], func(t *testing.T) {
			link, _, registry, frames, start := originReleaseFixture(t)
			link.fd = policyTransferFD(t, "alpha")
			go func() {
				event := <-registry
				if event.kind == registryClosed {
					event.ack <- true
				}
			}()
			start()
			caller := policyTestCaller()
			caller.Availability = optIn
			request := policyTestRequest(t, "session.list", protocol.SessionListRequest{SessionID: "lane@beta"})
			first := make(chan Reply, 1)
			link.inbox <- outgoingForward{value: Forward{From: caller, Request: request}, reply: first}
			if frame := managementFrame(t, frames); frame.Method != forwardMethod {
				t.Fatal(frame)
			}
			link.inbox <- livenessTick{at: time.Now().Add(silenceAfter + time.Second)}
			if reply := lookupAnswer(t, first); reply.Error == nil || reply.Error.Code != protocol.ForwardLost || len(reply.Error.Data) != 0 {
				t.Fatal("pending lookup became empty list", reply)
			}
			second := make(chan Reply, 1)
			link.inbox <- outgoingForward{value: Forward{From: caller, Request: request}, reply: second}
			if reply := lookupAnswer(t, second); reply.Error == nil || reply.Error.Code != protocol.ForwardLost {
				t.Fatal("new marked lookup became empty list", reply)
			}
			broad := make(chan Reply, 1)
			link.inbox <- outgoingForward{value: Forward{From: caller, Request: policyTestRequest(t, "session.list", protocol.SessionListRequest{Host: "beta"})}, reply: broad}
			value, err := protocol.DecodeResult("session.list", lookupAnswer(t, broad).Result)
			if err != nil || len(value.(*protocol.SessionListResult).Sessions) != 0 || (len(value.(*protocol.SessionListResult).Hosts) > 0) != optIn {
				t.Fatal("host-only projection changed", value, err)
			}
		})
	}
}

func TestMarkedLookupAtHubKeepsBroadProjectionAndSameCaller(t *testing.T) {
	for _, optIn := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "opted-in"}[optIn], func(t *testing.T) {
			origin, peer, registry, frames, start := originReleaseFixture(t)
			origin.fd = policyTransferFD(t, "alpha")
			destination, _, _, _, _ := originReleaseFixture(t)
			destination.host = "beta"
			destination.liveness.silent.Store(true)
			state := &hubState{links: map[string]*hostLink{"alpha": origin, "beta": destination}, log: io.Discard}
			go func() {
				for {
					event := <-registry
					state.handle(context.Background(), registry, event)
					if event.kind == registryClosed {
						return
					}
				}
			}()
			start()
			caller := policyTestCaller()
			caller.Availability = optIn
			for index, input := range []protocol.SessionListRequest{{SessionID: "lane@beta"}, {Host: "beta"}, {SessionID: "absent@missing"}} {
				body, err := requestBytes(int64(index+1), forwardMethod, Forward{From: caller, Request: policyTestRequest(t, "session.list", input)})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := peer.Write(body); err != nil {
					t.Fatal("same caller lost", err)
				}
				frame := managementFrame(t, frames)
				switch index {
				case 0:
					if frame.Error == nil || frame.Error.Code != protocol.ForwardLost || len(frame.Error.Data) != 0 {
						t.Fatal("hub lookup became successful list", frame)
					}
				case 1:
					value, err := protocol.DecodeResult("session.list", frame.Result)
					if err != nil || (len(value.(*protocol.SessionListResult).Hosts) > 0) != optIn {
						t.Fatal("hub broad projection", frame, err)
					}
				case 2:
					if frame.Error == nil || frame.Error.Code != protocol.UnknownHost {
						t.Fatal("valid absent host changed", frame)
					}
				}
			}
		})
	}
}

func TestMarkQueuedLookupAndLateCorrelationKeepLifetime(t *testing.T) {
	request := policyTestRequest(t, "session.list", protocol.SessionListRequest{SessionID: "lane@beta"})
	now := time.Now()
	live := linkLiveness{lastReply: now, waiting: true}
	reply := make(chan Reply, 1)
	p := &pendingCall{forwarded: true, lookup: exactSessionLookup(request), method: request.Method, target: "beta", reply: reply}
	queued := map[*pendingCall]bool{p: true}
	pending := map[int64]pendingCall{}
	if !live.tick(now.Add(silenceAfter), hostsMethod, nil, queued, pending) {
		t.Fatal("first mark missing")
	}
	if value := lookupAnswer(t, reply); value.Error == nil || value.Error.Code != protocol.ForwardLost {
		t.Fatal(value)
	}
	installCall(conn.Dispatched{ID: 1, Value: p}, queued, pending)
	var input protocol.SessionListRequest
	if json.Unmarshal(request.Params, &input) != nil || len(pending) != 1 || pending[1].reply != nil || !pending[1].lookup {
		t.Fatal("queued lookup metadata/correlation lost")
	}
	if live.tick(now.Add(silenceAfter+time.Second), hostsMethod, nil, queued, pending) {
		t.Fatal("duplicate mark")
	}
	if !live.replied() || live.replied() {
		t.Fatal("clear transition not once")
	}
}
