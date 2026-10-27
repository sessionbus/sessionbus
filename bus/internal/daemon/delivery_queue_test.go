// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/commslog"
	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestScheduledDeliveryReservesRecordCapacity(t *testing.T) {
	s, reader, _ := policySession(t, true, 0)
	s.identity.row.Policy.Notify = false
	s.runID, s.runGeneration, s.runSequence = "g/255", "g", 255
	send := func(id string) answer {
		reply := make(chan answer, 1)
		s.issue(routedRequest{destination: s.identity, method: "message.deliver", params: protocol.DeliveryRequest{MessageID: id, From: protocol.DeliverySource{SessionID: "sender@local", Product: "peer", Groups: []string{}}, Body: "work"}, reply: reply})
		frame := policyFrame(t, reader)
		s.receiveResponse(protocol.Frame{ID: frame.ID, Error: &protocol.RPCError{Code: protocol.NotRunning, Message: "not_running"}})
		return <-reply
	}
	if got := send("reserved"); got.code != 0 || got.value.(*protocol.DeliveryReceipt).Disposition != "queued_for_next_turn" {
		t.Fatalf("last available record not reserved: %+v", got)
	}
	if got := send("overflow"); got.code != protocol.Busy || len(s.deferredDeliveries) != 1 {
		t.Fatalf("acknowledged work without future capacity: %+v", got)
	}
	// Successful duplicate/foreign acknowledgments cannot manufacture capacity.
	ack := func(id string, code int) {
		reply := make(chan answer, 1)
		s.issue(routedRequest{destination: s.identity, method: "turn.ack", params: &protocol.RunRef{SessionID: s.identity.row.SessionID, RunID: id}, reply: reply})
		frame := policyFrame(t, reader)
		response := protocol.Frame{ID: frame.ID, Result: json.RawMessage(`{}`)}
		if code != 0 {
			response.Error = &protocol.RPCError{Code: code, Message: "busy"}
		}
		s.receiveResponse(response)
		<-reply
	}
	ack("other/1", 0)
	ack("g/256", 0)
	ack("g/1", protocol.Busy)
	if s.hasRunCapacity() {
		t.Fatal("foreign, future, or failed ack released capacity")
	}
	ack("g/1", 0)
	ack("g/1", 0)
	if s.acknowledgedSequence != 1 || !s.hasRunCapacity() {
		t.Fatal("successful ack not accounted once")
	}
	if got := send("second"); got.code != 0 {
		t.Fatalf("ack did not release one slot: %+v", got)
	}
	if got := send("overflow-again"); got.code != protocol.Busy {
		t.Fatal("duplicate ack released multiple slots")
	}
	// Retained messages become runs one at a time, in order, without more calls.
	for index, message := range []string{"reserved", "second"} {
		id := s.runID
		s.turnReady(protocol.Frame{ID: int64(77 + index), Method: "turn.ready"}, &protocol.TurnReady{SessionID: s.identity.row.SessionID, RunID: id, State: "done", Outcome: "completed"})
		if frame := policyFrame(t, reader); frame.Request {
			t.Fatal("scheduled run preceded ready ack")
		}
		frame := policyFrame(t, reader)
		var input protocol.DeliveryRequest
		must(t, json.Unmarshal(frame.Params, &input))
		if input.MessageID != message || input.RunID != fmt.Sprintf("g/%d", 256+index) || len(s.pending)+len(s.queued) != 1 {
			t.Fatalf("queue order or run reservation lost: %+v", input)
		}
		s.receiveResponse(protocol.Frame{ID: frame.ID, Result: json.RawMessage(`{"disposition":"injected"}`)})
	}
	if s.hasRunCapacity() || len(s.deferredDeliveries) != 0 {
		t.Fatal("completion freed a retained result without ack")
	}
}

func TestExplicitRunCannotConsumeScheduledReservation(t *testing.T) {
	s, _, _ := policySession(t, true, 0)
	s.runGeneration, s.runSequence = "g", 255
	s.deferredDeliveries = []routedRequest{{scheduled: true}}
	reply := make(chan answer, 1)
	s.issue(routedRequest{destination: s.identity, method: "turn.start", params: &protocol.TurnRunRequest{SessionID: s.identity.row.SessionID, Input: "other"}, reply: reply})
	if got := <-reply; got.code != protocol.Busy || s.runID != "" || s.runSequence != 255 {
		t.Fatalf("explicit run stole scheduled capacity: %+v", got)
	}
}

func TestScheduledDeliveryLogsNativeReceiptAndCloseLossWithoutAnotherReply(t *testing.T) {
	for _, close := range []bool{false, true} {
		t.Run(fmt.Sprint(close), func(t *testing.T) {
			s, reader, _ := policySession(t, true, 0)
			path := filepath.Join(t.TempDir(), "comms", "comms.jsonl")
			logger, err := commslog.Open(commslog.Options{Mode: commslog.Content, Path: path, Host: "local", Incarnation: "test", MaxFileBytes: 1 << 20, MaxFiles: 1, QueueBytes: 1 << 20})
			must(t, err)
			s.daemon.comms = logger
			t.Cleanup(func() { _ = logger.Close() })
			s.identity.row.Policy.Notify = false
			s.runID, s.runGeneration, s.runSequence = "g/1", "g", 1
			reply := make(chan answer, 1)
			s.issue(routedRequest{destination: s.identity, method: "message.deliver", params: protocol.DeliveryRequest{MessageID: "retained", From: protocol.DeliverySource{SessionID: "sender@local", Product: "peer", Groups: []string{}}, Body: "PRIVATE-BODY"}, reply: reply})
			frame := policyFrame(t, reader)
			s.receiveResponse(protocol.Frame{ID: frame.ID, Error: &protocol.RPCError{Code: protocol.NotRunning, Message: "not_running"}})
			if (<-reply).value.(*protocol.DeliveryReceipt).Disposition != "queued_for_next_turn" {
				t.Fatal("missing queue receipt")
			}
			if close {
				s.settlePending(protocol.NotConnected)
			} else {
				s.turnReady(protocol.Frame{ID: 77, Method: "turn.ready"}, &protocol.TurnReady{SessionID: s.identity.row.SessionID, RunID: "g/1", State: "done", Outcome: "completed"})
				policyFrame(t, reader)
				next := policyFrame(t, reader)
				s.receiveResponse(protocol.Frame{ID: next.ID, Result: json.RawMessage(`{"disposition":"injected"}`)})
			}
			if len(reply) != 0 || len(s.deferredDeliveries) != 0 {
				t.Fatal("second sender reply or queue leak")
			}
			must(t, logger.Close())
			raw, err := os.ReadFile(path)
			must(t, err)
			if strings.Contains(string(raw), "PRIVATE-BODY") || logger.Stats().LostInvalid != 0 {
				t.Fatal("invalid log or duplicate content")
			}
			want := `"disposition":"injected"`
			if close {
				want = `"outcome":"not_submitted"`
			}
			if !strings.Contains(string(raw), `"message_id":"retained"`) || !strings.Contains(string(raw), want) {
				t.Fatalf("later outcome absent: %s", raw)
			}
		})
	}
}

// Exercise two actual SDK workers and daemon-routed sends. Both native runs stay
// active until BOTH send receipts arrive, so a receipt waiting on turn.ready
// cannot be rescued by one run happening to finish first.
type mutualSendProduct struct {
	fixtureProduct
	worker *sessionkit.Worker
	id     string
}

func (p *mutualSendProduct) Open(ctx context.Context, request sessionkit.OpenRequest) (sessionkit.OpenResult, error) {
	result, err := p.fixtureProduct.Open(ctx, request)
	p.id = result.SessionID
	return result, err
}

func (p *mutualSendProduct) Deliver(context.Context, sessionkit.DeliveryRequest, *sessionkit.Run) (sessionkit.DeliveryReceipt, error) {
	return sessionkit.DeliveryReceipt{}, &sessionkit.ProtocolError{Code: protocol.NotRunning, Message: "not_running"}
}

func (p *mutualSendProduct) Run(ctx context.Context, run *sessionkit.Run, seed sessionkit.RunInput) (sessionkit.TurnResult, error) {
	if seed.Delivery != nil {
		if err := run.ReportDelivery(sessionkit.DeliveryReceipt{Disposition: "injected"}, nil); err != nil {
			return sessionkit.TurnResult{}, err
		}
		return sessionkit.TurnResult{Outcome: "completed", Result: seed.Delivery.Body}, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	barrier := func(phase string) error {
		dir := os.Getenv("MUTUAL_SEND_BARRIER")
		if err := os.WriteFile(filepath.Join(dir, phase+"-"+p.id), nil, 0600); err != nil {
			return err
		}
		for {
			matches, err := filepath.Glob(filepath.Join(dir, phase+"-*"))
			if err != nil {
				return err
			}
			if len(matches) == 2 {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Millisecond):
			}
		}
	}
	if err := barrier("running"); err != nil {
		return sessionkit.TurnResult{}, err
	}
	receipt, err := p.worker.Caller().Send(ctx, sessionkit.MessageSendRequest{Target: *seed.Text, Message: "from-" + p.id})
	if err != nil {
		return sessionkit.TurnResult{}, err
	}
	if len(receipt.Deliveries) != 1 || receipt.Deliveries[0].Disposition != "queued_for_next_turn" {
		return sessionkit.TurnResult{}, fmt.Errorf("expected scheduled admission: %+v", receipt)
	}
	if err := barrier("received"); err != nil {
		return sessionkit.TurnResult{}, err
	}
	return sessionkit.TurnResult{Outcome: "completed", Result: "send-receipt-before-terminal"}, nil
}

func TestSDKMutualActiveLaneSendsAutomaticallyContinue(t *testing.T) {
	products := t.TempDir()
	installFixture(t, products, "mutual-worker")
	t.Setenv("PATH", products+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("MUTUAL_SEND_BARRIER", t.TempDir())
	_, socket := startDaemon(t)
	owner := connectPeer(t, socket, "owner", "owner", "team")
	zero, no := int64(0), false
	var lanes [2]protocol.LaneSpawnResult
	for index := range lanes {
		must(t, owner.call("lane.spawn", protocol.LaneSpawnRequest{Name: fmt.Sprintf("mutual%d", index), Product: "mutual-worker", Open: &protocol.OpenOptions{}, ExtraGroups: []string{"team"}, AutoCloseMS: &zero, Notify: &no}, &lanes[index]))
	}
	var refs [2]protocol.RunRef
	for index := range lanes {
		must(t, owner.call("turn.start", protocol.TurnRunRequest{SessionID: lanes[index].SessionID, Input: lanes[1-index].SessionID}, &refs[index]))
	}
	for index, lane := range lanes {
		var result protocol.RunStatus
		must(t, owner.call("turn.wait", protocol.WaitRequest{SessionID: lane.SessionID, RunID: refs[index].RunID}, &result))
		if result.State != "done" || result.Result == nil || result.Result.Result != "send-receipt-before-terminal" {
			t.Fatalf("mutual send deadlocked: %+v", result)
		}
		must(t, owner.call("turn.ack", refs[index], &struct{}{}))
		must(t, owner.call("turn.wait", protocol.WaitRequest{SessionID: lane.SessionID}, &result))
		otherID, _, _ := strings.Cut(lanes[1-index].SessionID, "@")
		if result.State != "done" || result.Result == nil || result.Result.Result != "from-"+otherID || !strings.HasSuffix(result.RunID, "/2") {
			t.Fatalf("queued input did not autonomously run exactly once: %+v", result)
		}
		must(t, owner.call("turn.ack", protocol.RunRef{SessionID: lane.SessionID, RunID: result.RunID}, &struct{}{}))
		must(t, owner.call("session.close", protocol.SessionCloseRequest{SessionID: lane.SessionID, Forget: true}, &struct{}{}))
	}
}
