// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestDeliveryCrossingTurnEndWakesOnce(t *testing.T) {
	for _, readyFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "refusal-before-ready", true: "ready-before-refusal"}[readyFirst], func(t *testing.T) {
			s, reader, _ := policySession(t, true, 0)
			s.identity.row.Policy.Notify = false
			s.runID, s.runGeneration, s.runSequence = "g/1", "g", 1
			reply := make(chan answer, 1)
			original := protocol.DeliveryRequest{MessageID: "original", From: protocol.DeliverySource{SessionID: "peer@local", Product: "peer", Groups: []string{}}, Body: "work"}
			s.issue(routedRequest{destination: s.identity, method: "message.deliver", params: original, reply: reply, completion: true})
			first := policyFrame(t, reader)
			var input protocol.DeliveryRequest
			must(t, json.Unmarshal(first.Params, &input))
			if input.RunID != "" {
				t.Fatal("active delivery was seeded")
			}
			ready := func() {
				s.turnReady(protocol.Frame{ID: 77, Method: "turn.ready"}, &protocol.TurnReady{SessionID: s.identity.row.SessionID, RunID: "g/1", State: "done", Outcome: "completed"})
				if ack := policyFrame(t, reader); ack.ID != 77 || ack.Request {
					t.Fatalf("ready ack must precede new run: %+v", ack)
				}
			}
			if readyFirst {
				ready()
			}
			s.receiveResponse(protocol.Frame{ID: first.ID, Error: &protocol.RPCError{Code: protocol.NotRunning, Message: "not_running"}})
			if !readyFirst {
				if len(s.deferredDeliveries) != 1 {
					t.Fatal("crossing delivery not retained")
				}
				select {
				case got := <-reply:
					if got.code != 0 || got.value.(*protocol.DeliveryReceipt).Disposition != "queued_for_next_turn" {
						t.Fatalf("scheduled receipt %+v", got)
					}
				default:
					t.Fatal("active sender was blocked")
				}
				ready()
			}
			second := policyFrame(t, reader)
			must(t, json.Unmarshal(second.Params, &input))
			if input.RunID != "g/2" || input.MessageID != original.MessageID || input.Body != original.Body || input.From.SessionID != original.From.SessionID || !s.runFromCompletion {
				t.Fatalf("handoff changed delivery: %+v", input)
			}
			s.receiveResponse(protocol.Frame{ID: second.ID, Result: json.RawMessage(`{"disposition":"written"}`)})
			if readyFirst {
				if got := <-reply; got.code != 0 || got.value.(*protocol.DeliveryReceipt).Disposition != "written" {
					t.Fatalf("receipt %+v", got)
				}
			} else if len(reply) != 0 {
				t.Fatal("scheduled delivery answered twice")
			}
			if len(s.deferredDeliveries) != 0 || s.runSequence != 2 {
				t.Fatal("duplicate admission")
			}
		})
	}
}

// Each original run is blocked on its outgoing send. Neither may require the
// other's turn.ready before receiving a queue receipt.
func TestMutualActiveSendsDoNotWaitForEachOthersTurnEnd(t *testing.T) {
	for _, name := range []string{"A-to-B", "B-to-A"} {
		t.Run(name, func(t *testing.T) {
			s, reader, _ := policySession(t, true, 0)
			s.identity.row.Policy.Notify = false
			s.runID, s.runGeneration, s.runSequence = "g/1", "g", 1
			reply := make(chan answer, 1)
			s.issue(routedRequest{destination: s.identity, method: "message.deliver", params: protocol.DeliveryRequest{MessageID: name, From: protocol.DeliverySource{SessionID: "other@local", Product: "peer", Groups: []string{}}, Body: "reply automatically"}, reply: reply})
			deliver := policyFrame(t, reader)
			s.receiveResponse(protocol.Frame{ID: deliver.ID, Error: &protocol.RPCError{Code: protocol.NotRunning, Message: "not_running"}})
			select {
			case got := <-reply:
				if got.code != 0 || got.value.(*protocol.DeliveryReceipt).Disposition != "queued_for_next_turn" {
					t.Fatalf("queue admission: %+v", got)
				}
			default:
				t.Fatal("send waits for recipient turn.ready: mutually sending active runs deadlock")
			}
			s.turnReady(protocol.Frame{ID: 77, Method: "turn.ready"}, &protocol.TurnReady{SessionID: s.identity.row.SessionID, RunID: "g/1", State: "done", Outcome: "completed"})
			if ack := policyFrame(t, reader); ack.ID != 77 || ack.Request {
				t.Fatal("ready acknowledgement did not precede automatic run")
			}
			next := policyFrame(t, reader)
			var input protocol.DeliveryRequest
			must(t, json.Unmarshal(next.Params, &input))
			if input.RunID != "g/2" || input.MessageID != name {
				t.Fatalf("missing automatic follow-up: %+v", input)
			}
			s.receiveResponse(protocol.Frame{ID: next.ID, Result: json.RawMessage(`{"disposition":"injected"}`)})
			select {
			case <-reply:
				t.Fatal("sender received a second receipt")
			default:
			}
		})
	}
}

func TestCompletionSeedNeverEmitsAnotherAutomaticPointer(t *testing.T) {
	s, reader, _ := policySession(t, true, 0)
	s.identity.row.Policy.Notify = true
	s.identity.row.Policy.NotifyTarget = "third-party@local"
	s.runFromCompletion = true
	policyTerminal(t, s, reader)
	if s.owned != 0 || s.runFromCompletion {
		t.Fatal("completion-seeded run emitted a follow-on pointer or retained origin")
	}
	// An ordinary subsequent run does notify, including when it follows a
	// notification-seeded run on the same worker.
	policyTerminal(t, s, reader)
	if s.owned != 1 {
		t.Fatal("ordinary completion notification was suppressed")
	}
	s.handleEvent(<-s.inbox)
}

func TestCompletionNoticesWakeButStopAtOneHopAcrossFederation(t *testing.T) {
	_, owner := traceFederation(t)
	zero, yes := int64(0), true
	lanes := make([]string, 3)
	hosts := []string{"alpha", "beta", "gamma"}
	for index, host := range hosts {
		var lane protocol.LaneSpawnResult
		next := hosts[(index+1)%len(hosts)]
		must(t, owner.call("lane.spawn", protocol.LaneSpawnRequest{Name: "ring", Host: host, Product: "fixture-worker", Open: &protocol.OpenOptions{}, Persistent: &yes, AutoCloseMS: &zero, NotifyTarget: "parent/ring@" + next, ExtraGroups: []string{"team"}}, &lane))
		lanes[index] = lane.SessionID
	}
	var initial protocol.RunStatus
	must(t, owner.call("turn.run", protocol.TurnRunRequest{SessionID: lanes[0], Input: "start"}, &initial))
	var notified protocol.RunStatus
	deadline := time.Now().Add(3 * time.Second)
	for {
		err := owner.call("turn.status", protocol.ReadRequest{SessionID: lanes[1]}, &notified)
		if err == nil && notified.State == "done" {
			break
		}
		if err != nil && rpcCode(err) != protocol.UnknownSession {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("completion notice failed to wake recipient")
		}
		time.Sleep(time.Millisecond)
	}
	if notified.Result == nil || !strings.Contains(notified.Result.Result, initial.RunID) {
		t.Fatalf("notification-seeded output unavailable: %+v", notified)
	}
	if code := rpcCode(owner.call("turn.status", protocol.ReadRequest{SessionID: lanes[2]}, &protocol.RunStatus{})); code != protocol.UnknownSession {
		t.Fatalf("automatic notification propagated beyond one hop: %d", code)
	}
	for _, lane := range lanes {
		must(t, owner.call("session.close", protocol.SessionCloseRequest{SessionID: lane, Forget: true}, &struct{}{}))
	}
}

func TestDeferredDeliveriesStayBoundedAndUncertainWritesAreNotReseeded(t *testing.T) {
	s, reader, _ := policySession(t, true, 0)
	s.runID = "g/1"
	for range maxPendingCalls {
		s.deferredDeliveries = append(s.deferredDeliveries, routedRequest{reply: make(chan answer, 1)})
	}
	reply := make(chan answer, 1)
	s.issue(routedRequest{destination: s.identity, method: "message.deliver", reply: reply})
	if (<-reply).code != protocol.Busy {
		t.Fatal("deferred RPCs bypassed capacity")
	}
	s.settleDeferredDeliveries(protocol.NotConnected)
	s.issue(routedRequest{destination: s.identity, method: "message.deliver", params: protocol.DeliveryRequest{MessageID: "m", From: protocol.DeliverySource{SessionID: "peer@local", Product: "peer", Groups: []string{}}, Body: "work"}, reply: reply})
	frame := policyFrame(t, reader)
	s.receiveResponse(protocol.Frame{ID: frame.ID, Error: &protocol.RPCError{Code: protocol.Internal, Message: "internal"}})
	if (<-reply).code != protocol.Internal || len(s.deferredDeliveries) != 0 || s.runID != "g/1" {
		t.Fatal("uncertain native admission retried")
	}
}

func TestDeferredDeliverySettlesOnCloseAndDisconnect(t *testing.T) {
	for _, close := range []bool{false, true} {
		s, _, _ := policySession(t, true, 0)
		s.runID = "g/1"
		reply := make(chan answer, 1)
		s.deferredDeliveries = []routedRequest{{method: "message.deliver", reply: reply}}
		want := protocol.NotConnected
		if close {
			want = protocol.Busy
			s.beginClose(routedRequest{method: "session.close", params: &protocol.SessionCloseRequest{SessionID: s.identity.row.SessionID}, reply: make(chan answer, 1)})
		} else {
			s.settlePending(want)
		}
		if got := <-reply; got.code != want || len(s.deferredDeliveries) != 0 {
			t.Fatalf("unsettled delivery %+v", got)
		}
	}
}

func TestOrdinarySendWakesFreshAndResumedLane(t *testing.T) {
	products := t.TempDir()
	installFixture(t, products, "wake-worker")
	t.Setenv("PATH", products+string(os.PathListSeparator)+os.Getenv("PATH"))
	_, socket := startDaemon(t)
	owner := connectPeer(t, socket, "owner", "owner", "team")
	zero, no := int64(0), false
	var lane protocol.LaneSpawnResult
	must(t, owner.call("lane.spawn", protocol.LaneSpawnRequest{Name: "child", Product: "wake-worker", Open: &protocol.OpenOptions{}, AutoCloseMS: &zero, Notify: &no}, &lane))
	for generation := 0; generation < 2; generation++ {
		var sent protocol.MessageSendResult
		must(t, owner.call("message.send", protocol.MessageSendRequest{Target: lane.SessionID, Message: "answer without a second prompt"}, &sent))
		if len(sent.Deliveries) != 1 || sent.Deliveries[0].Disposition != "written" {
			t.Fatalf("native admission: %+v", sent)
		}
		var result protocol.RunStatus
		must(t, owner.call("turn.wait", protocol.WaitRequest{SessionID: lane.SessionID}, &result))
		if result.State != "done" || result.Result == nil || result.Result.Outcome != "completed" || result.Result.Result != "answer without a second prompt" {
			t.Fatalf("delivery failed to cause work: %+v", result)
		}
		must(t, owner.call("turn.ack", protocol.RunRef{SessionID: lane.SessionID, RunID: result.RunID}, &struct{}{}))
		must(t, owner.call("session.close", protocol.SessionCloseRequest{SessionID: lane.SessionID, Forget: generation == 1}, &struct{}{}))
		if generation == 0 {
			must(t, owner.call("lane.spawn", protocol.LaneSpawnRequest{ResumeSessionID: lane.SessionID, AutoCloseMS: &zero, Notify: &no}, &lane))
		}
	}
}

func TestStoredIdleMessagePolicyIsRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions")
	store, _, err := openTable(path)
	must(t, err)
	value := row{SessionID: "saved@local", Product: "wake-worker", Name: "saved@local", Groups: []string{"team", "session:saved@local"}, CreatedAt: time.Now(), Policy: &protocol.LanePolicy{Persistent: true}}
	must(t, store.write(value))
	_, _, err = openTable(path)
	must(t, err)
	file := filepath.Join(path, rowFile(value.SessionID))
	raw, err := os.ReadFile(file)
	must(t, err)
	var fields map[string]any
	must(t, json.Unmarshal(raw, &fields))
	fields["policy"].(map[string]any)["idle_message"] = "run"
	raw, err = json.Marshal(fields)
	must(t, err)
	must(t, os.WriteFile(file, raw, 0o600))
	if _, _, err := openTable(path); err == nil {
		t.Fatal("stored idle_message policy was accepted")
	}
}
