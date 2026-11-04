// SPDX-License-Identifier: MIT

package sessionkit

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/internal/rpc"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type workerRecord struct {
	status   RunStatus
	sequence uint64
}

func runSequence(id string) (string, uint64, bool) {
	prefix, tail, ok := strings.Cut(id, "/")
	sequence, err := strconv.ParseUint(tail, 10, 64)
	return prefix, sequence, ok && prefix != "" && sequence > 0 && err == nil && strconv.FormatUint(sequence, 10) == tail
}

func (w *Worker) execute(request *rpc.Request, delivery *DeliveryRequest, project ...func(context.Context, *DeliveryRequest) error) {
	w.mu.Lock()
	if !w.opened.Load() {
		w.mu.Unlock()
		go w.answer(request, nil, protocol.Busy)
		return
	}
	var ref RunRef
	var seed RunInput
	if delivery == nil {
		input := request.Params.(*protocol.ExecuteRequest)
		ref = RunRef{SessionID: input.SessionID, RunID: input.RunID}
		seed.Text = &input.Input
	} else {
		ref = RunRef{SessionID: w.sessionID, RunID: delivery.RunID}
		copy := *delivery
		copy.RunID = ""
		seed.Delivery = &copy
	}
	prefix, sequence, valid := runSequence(ref.RunID)
	code := 0
	switch {
	case !w.opened.Load() || w.run != nil || len(w.records) >= protocol.MaxOperations:
		code = protocol.Busy
	case ref.SessionID != w.sessionID || !valid || w.generation != "" && w.generation != prefix || sequence != w.lastSequence+1:
		code = protocol.InvalidFrame
	}
	if code != 0 {
		w.mu.Unlock()
		go w.answer(request, nil, code)
		return
	}
	w.generation, w.lastSequence = prefix, sequence
	runCtx, cancel := context.WithCancel(w.context)
	done, finish := context.WithCancel(context.Background())
	slot := &Run{context: runCtx, cancel: cancel, done: done.Done(), finish: finish, admitted: make(chan struct{})}
	if delivery != nil {
		slot.receipt = func(value DeliveryReceipt, err error) error {
			err = w.deliveryReply(request, value, err)
			w.reply(err)
			return err
		}
	}
	record := &workerRecord{status: RunStatus{SessionID: ref.SessionID, RunID: ref.RunID, State: "running"}, sequence: sequence}
	w.records = append(w.records, record)
	w.run = slot
	w.mu.Unlock()
	go func() {
		if delivery == nil {
			if err := w.conn.Result(request, ref); err != nil {
				w.reply(err)
				return
			}
		}
		var before error
		if delivery != nil && len(project) > 0 && project[0] != nil {
			before = project[0](slot.context, seed.Delivery)
		}
		w.runTurn(slot, record, seed, before)
	}()
}

func (w *Worker) runTurn(slot *Run, record *workerRecord, seed RunInput, before ...error) {
	var result TurnResult
	var err error
	if len(before) > 0 && before[0] != nil {
		err = errors.New("transfer notice projection unavailable")
	} else {
		result, err = w.product.Run(slot.context, slot, seed)
	}
	slot.cancel()
	status := record.status
	status.State, status.Result = "done", &result
	if err != nil {
		status.State, status.Result, status.Reason = "unavailable", nil, err.Error()
	}
	if err == nil {
		err = protocol.ValidateTurnResult(result)
	}
	if _, validation := protocol.ResultBytes(protocol.MaxRequestID, "turn.status", status); validation != nil || err != nil && status.State == "done" {
		status.State, status.Result, status.Reason = "unavailable", nil, "native result failed validation"
	}
	if seed.Delivery != nil {
		// Absence of a callback receipt is uncertainty, never native refusal.
		_ = slot.ReportDelivery(DeliveryReceipt{}, &ProtocolError{Code: protocol.Internal, Message: "internal", Data: []byte(`"native delivery receipt unavailable"`)})
	}
	ready := protocol.TurnReady{SessionID: status.SessionID, RunID: status.RunID, State: status.State, Reason: status.Reason}
	if status.Result != nil {
		ready.Outcome = status.Result.Outcome
	}
	// Publish on the ready acknowledgement before the reader dispatches a later
	// run/close frame. No worker lock is held across this duplex RPC.
	err = w.conn.CallObserved(w.context, "turn.ready", ready, &struct{}{}, func() error {
		w.mu.Lock()
		record.status = status
		if w.run == slot {
			w.run = nil
		}
		close(w.changed)
		w.changed = make(chan struct{})
		slot.finish()
		w.mu.Unlock()
		return nil
	})
	w.reply(err)
}

func (w *Worker) findRun(request ReadRequest) *workerRecord {
	for _, record := range w.records {
		if (request.RunID == "" || request.RunID == record.status.RunID) && request.SessionID == record.status.SessionID {
			return record
		}
	}
	return nil
}

func (w *Worker) readRun(request *rpc.Request) {
	var input ReadRequest
	var timeout *int64
	if request.Method == "turn.wait" {
		wait := request.Params.(*WaitRequest)
		input, timeout = ReadRequest{SessionID: wait.SessionID, RunID: wait.RunID}, wait.TimeoutMS
	} else {
		input = *request.Params.(*ReadRequest)
	}
	w.mu.Lock()
	if w.waiters >= protocol.MaxOperations || w.run != nil && w.run.done == nil {
		w.mu.Unlock()
		go w.answer(request, nil, protocol.Busy)
		return
	}
	w.waiters++
	w.mu.Unlock()
	go func() {
		defer func() { w.mu.Lock(); w.waiters--; close(w.changed); w.changed = make(chan struct{}); w.mu.Unlock() }()
		var deadline <-chan time.Time
		if timeout != nil {
			timer := time.NewTimer(time.Duration(*timeout) * time.Millisecond)
			defer timer.Stop()
			deadline = timer.C
		}
		for {
			w.mu.Lock()
			record, changed := w.findRun(input), w.changed
			if record == nil {
				w.mu.Unlock()
				w.answer(request, nil, protocol.UnknownSession)
				return
			}
			status := record.status
			w.mu.Unlock()
			if request.Method != "turn.wait" || status.State != "running" {
				w.answer(request, status, 0)
				return
			}
			select {
			case <-changed:
			case <-deadline:
				// Snapshot again so terminal completion wins a simultaneous timeout.
				w.mu.Lock()
				current := w.findRun(input)
				if current != nil {
					status = current.status
				}
				w.mu.Unlock()
				if current == nil {
					w.answer(request, nil, protocol.UnknownSession)
				} else {
					w.answer(request, status, 0)
				}
				return
			case <-w.context.Done():
				return
			}
		}
	}()
}

func (w *Worker) ackRun(request *rpc.Request) {
	input := request.Params.(*RunRef)
	prefix, sequence, valid := runSequence(input.RunID)
	w.mu.Lock()
	code := protocol.UnknownSession
	if valid && prefix == w.generation && input.SessionID == w.sessionID {
		switch {
		case sequence <= w.acknowledged:
			code = 0
		case len(w.records) > 0 && w.records[0].status.RunID == input.RunID && w.records[0].status.State != "running":
			w.acknowledged = sequence
			w.records[0] = nil
			w.records = w.records[1:]
			code = 0
		case len(w.records) > 0:
			code = protocol.Busy
		}
	}
	w.mu.Unlock()
	go w.answer(request, struct{}{}, code)
}
