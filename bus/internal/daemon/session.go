// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"encoding/json"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/antst/sessionbus/bus/internal/commslog"
	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/internal/structuredprocess"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

const (
	maxPendingCalls     = 256
	supersedeWriteBound = time.Second
)

type answer struct {
	after  func()
	trace  []federation.TraceRecipient
	value  any
	code   int
	data   any
	remote *federation.Reply
}

type routedRequest struct {
	scheduled     bool // Sender already received daemon queue admission.
	traceCopy     bool
	completion    bool
	traceLifetime string
	runID         string
	collect       bool
	destination   *entry
	method        string
	params        any
	reply         chan answer
}

type replyEvent struct {
	requestID int64
	leg       int
	answer    answer
}

type supersedeEvent struct{}

type session struct {
	transferKey                           atomic.Pointer[string]
	queued                                map[*routedRequest]bool
	inboundBulk                           map[int64]func()
	traceRequests                         map[int64]*traceRequest
	traceCollect                          bool
	traceCopy                             *federation.TraceDestination
	traceTargets                          []federation.TraceRecipient
	traceTargetsBytes                     int
	runFromTrace                          bool
	runFromCompletion                     bool
	completion                            bool
	commsRequests                         map[int64]commsRequest
	runID, runGeneration                  string
	runSequence                           uint64
	acknowledgedSequence                  uint64
	deadline, previousDeadline            time.Time
	transferHolds                         map[string]bool
	lastTransferTerminal, lastRunTerminal time.Time
	transferGracePending                  bool
	autoClose                             <-chan time.Time
	stopAuto                              func()

	supportsTransfer   bool
	daemon             *Daemon
	wire               *conn.Conn
	inbox              chan any
	identity           *entry
	lastIn             int64
	pending            map[int64]routedRequest
	deferredDeliveries []routedRequest
	requests           map[int64]*requestState
	owned              int
	stopping           bool
	closed             bool
	child              *structuredprocess.Process
	launch             *launch
	committed          bool
	openID             int64
	closeCall          *routedRequest
	closeAnswer        answer
	closeTimer         *time.Timer
	forget             bool
	launchResult       *answer
	processExited      bool
	stopSignal         syscall.Signal
	forwarded          chan federation.Reply
	caller             *federation.Caller
	omit               *entry
	messageID          string
}

func newSession(daemon *Daemon) *session {
	return &session{
		daemon:   daemon,
		inbox:    make(chan any, conn.OutboxSize),
		pending:  map[int64]routedRequest{},
		requests: map[int64]*requestState{},
		queued:   map[*routedRequest]bool{}, inboundBulk: map[int64]func(){},
	}
}

func (s *session) run() {
	shutdown := s.daemon.shutdown
	for {
		if s.closed && s.owned == 0 && (s.launch == nil || s.processExited) && s.drain() {
			if s.launch != nil {
				s.finishLane()
			}
			return
		}
		var closeTimer <-chan time.Time
		if s.closeTimer != nil {
			closeTimer = s.closeTimer.C
		}
		var spawnTimer <-chan time.Time
		if s.launch != nil && !s.committed {
			spawnTimer = s.launch.timer.C
		}
		select {
		case event := <-s.wire.Dispatched():
			s.dispatched(event)
		case event := <-s.wire.Bulk():
			s.handleEvent(event)
		case event := <-s.inbox:
			s.handleEvent(event)
		case <-s.autoClose:
			s.autoClose = nil
			s.policyClose()
		case <-closeTimer:
			s.closeTimer = nil
			s.hardStop()
		case <-spawnTimer:
			s.abortLaunch(answer{code: protocol.Timeout})
			s.hardStop()
		case <-shutdown:
			shutdown = nil
			if s.launch != nil && !s.committed {
				s.abortLaunch(answer{code: protocol.Internal, data: "daemon shutting down"})
			}
			s.orderlyStop()
		}
	}
}

func (s *session) handleEvent(event any) {
	switch value := event.(type) {
	case transferNoticeEvent:
		s.transferNotice(value)
	case transferNoticeInstalled:
		s.transferNoticeInstalled(value)
	case transferCallEvent:
		if !s.daemon.directory.transferCurrent(value.entry, s, value.key) || s.stopping {
			s.answerRouted(value.request, answer{code: protocol.NotConnected})
		} else {
			s.issue(value.request)
		}
	case transferHoldEvent:
		ok := s.transferHold(value)
		if value.reply != nil {
			value.reply <- ok
		}
	case ownerEndEvent:
		s.ownerEnded(value.owner)
	case conn.Frame:
		s.wire.DrainDispatched(s.dispatched)
		if value.Release != nil {
			s.inboundBulk[value.Value.ID] = value.Release
		}
		if !s.stopping {
			s.handleFrame(value)
		} else if value.Release != nil {
			value.Release()
		}
	case conn.Closed:
		s.wire.DrainDispatched(s.dispatched)
		s.connectionClosed()
		if s.launch != nil {
			if !s.committed && s.launchResult == nil {
				s.abortLaunch(answer{code: protocol.SpawnFailed, data: failure(s.child, "worker exited before open")})
			}
			s.orderlyStop()
		}
	case routedRequest:
		if s.stopping || value.destination != s.identity || s.launch != nil && !s.committed {
			s.answerRouted(value, answer{code: protocol.NotConnected})
		} else {
			s.issue(value)
		}
	case replyEvent:
		s.consumeReply(value)
	case supersedeEvent:
		s.supersede()
	case processEvent:
		s.handleProcess(value)
	case spawnTimeout:
		if value.launch == s.launch && !s.committed {
			s.abortLaunch(answer{code: protocol.Timeout})
			s.hardStop()
		}
	}
}

func (s *session) handleFrame(event conn.Frame) {
	frame := event.Value
	if event.Err == conn.ErrBulkCapacity {
		if s.identity == nil {
			s.reject(frame, protocol.InvalidHello)
			return
		}
		s.error(frame, protocol.Busy, nil)
		return
	}
	if event.Err != nil {
		code := protocol.InvalidFrame
		if s.identity == nil {
			code = protocol.InvalidHello
		}
		s.reject(frame, code)
		return
	}
	if s.identity == nil {
		s.firstHello(frame)
		return
	}
	if !frame.Request {
		if s.launch != nil && frame.ID == s.openID {
			s.finishOpen(frame)
			return
		}
		s.receiveResponse(frame)
		return
	}
	if !event.Ordered && frame.ID <= s.lastIn {
		s.reject(frame, protocol.InvalidFrame)
		return
	}
	s.lastIn = max(s.lastIn, frame.ID)
	if frame.Method == "session.hello" && s.launch != nil {
		s.reject(frame, protocol.InvalidHello)
		return
	}
	if s.launch != nil && !s.committed {
		s.error(frame, protocol.NotCommitted, nil)
		return
	}
	params, err := protocol.DecodeParams(frame.Method, frame.Params)
	if err != nil || !protocol.Allows(frame.Method, true) {
		code := protocol.InvalidFrame
		if frame.Method == "session.hello" {
			code = protocol.InvalidHello
		}
		s.reject(frame, code)
		return
	}
	if frame.Method == "session.hello" {
		hello, ok := params.(*protocol.PeerHello)
		if !ok {
			s.reject(frame, protocol.InvalidHello)
			return
		}
		s.peerHello(frame, hello)
		return
	}
	s.handleRequest(frame, params)
}

func (s *session) firstHello(frame protocol.Frame) {
	if !frame.Request || frame.ID <= s.lastIn || frame.Method != "session.hello" {
		s.reject(frame, protocol.InvalidHello)
		return
	}
	s.lastIn = frame.ID
	params, err := protocol.DecodeParams(frame.Method, frame.Params)
	if err != nil {
		s.reject(frame, protocol.InvalidHello)
		return
	}
	switch hello := params.(type) {
	case *protocol.PeerHello:
		s.peerHello(frame, hello)
	case *protocol.WorkerHello:
		start, ok := s.daemon.directory.claimWorker(s, hello.LaunchToken, hello.Product)
		if !ok {
			s.reject(frame, protocol.InvalidHello)
			return
		}
		start.claimed <- struct{}{}
		s.supportsTransfer = hello.SupportsTransfer && strings.HasPrefix(hello.LaunchToken, protocol.TransferTokenPrefix)
		s.startLane(start, frame, hello.HelloDescription)
	default:
		s.reject(frame, protocol.InvalidHello)
	}
}

func (s *session) issue(request routedRequest) {
	// Owner-local lifecycle calls also see requests already dispatched on wire,
	// rather than depending on a later outer-loop response to drain correlation.
	if s.wire != nil {
		s.wire.DrainDispatched(s.dispatched)
	}
	if s.closeCall != nil && !request.collect {
		s.answerRouted(request, answer{code: protocol.Busy})
		return
	}
	limit := maxPendingCalls
	bulk := request.method == "transfer.chunk"
	if bulk {
		limit = 4
	}
	if s.pendingCount(bulk) >= limit {
		s.answerRouted(request, answer{code: protocol.Busy})
		return
	}
	method := request.method
	if method == "turn.start" || method == "turn.run" {
		request.method, request.collect = "turn.execute", method == "turn.run"
	}
	if code := s.daemon.directory.admit(s.identity, s, request.method); code != 0 {
		s.answerRouted(request, answer{code: code})
		return
	}
	if method == "session.close" {
		s.beginClose(request)
		return
	}
	if request.method == "turn.execute" || method == "message.deliver" && !s.identity.peer && s.runID == "" {
		if !s.hasRunCapacity() {
			s.answerRouted(request, answer{code: protocol.Busy})
			return
		}
	}
	if request.method == "turn.execute" {
		input := request.params.(*protocol.TurnRunRequest)
		request.runID = s.reserveRun()
		request.params = &protocol.ExecuteRequest{SessionID: s.identity.row.SessionID, RunID: request.runID, Input: input.Input}
	} else if method == "message.deliver" && !s.identity.peer && s.runID == "" {
		if s.closeCall != nil {
			s.answerRouted(request, answer{code: protocol.Busy})
			return
		}
		input := request.params.(protocol.DeliveryRequest)
		request.runID = s.reserveRun()
		input.RunID = request.runID
		s.runFromTrace = request.traceCopy
		s.runFromCompletion = request.completion
		request.params = input
	}
	// The worker sees its canonical opened identity even when the public caller
	// selected the lane by an unqualified local ID.
	switch input := request.params.(type) {
	case *protocol.ReadRequest:
		copy := *input
		copy.SessionID = s.identity.row.SessionID
		request.params = &copy
	case *protocol.WaitRequest:
		copy := *input
		copy.SessionID = s.identity.row.SessionID
		request.params = &copy
	case *protocol.RunRef:
		copy := *input
		copy.SessionID = s.identity.row.SessionID
		request.params = &copy
	}

	if _, err := protocol.EncodeParams(request.method, request.params); err != nil {
		if request.runID != "" {
			s.refuseRun(request.runID)
		}
		s.answerRouted(request, answer{code: protocol.Internal})
		return
	}
	queued := &request
	if !s.wire.Request(bulk, queued, func(id int64) ([]byte, error) { return protocol.RequestBytes(id, request.method, request.params) }, protocol.TransferStreamID(request.params)) {
		if request.runID != "" {
			s.refuseRun(request.runID)
		}
		code := protocol.NotConnected
		if bulk {
			code = protocol.Busy
		}
		s.answerRouted(request, answer{code: code})
		return
	}
	s.queued[queued] = true
	if request.runID != "" {
		s.daemon.directory.admitted(s.identity, "turn.execute")
		s.logRun(commslog.Method(request.method), request.runID, commslog.Running, "")
	}

}

func (s *session) receiveResponse(frame protocol.Frame) {
	// Also enforce the dispatch-before-reply invariant for owner-local callers
	// (including direct response handling), not just the outer event loop.
	if s.wire != nil {
		s.wire.DrainDispatched(s.dispatched)
	}
	request, ok := s.pending[frame.ID]
	if !ok {
		s.stopping = true
		s.wire.Close()
		return
	}
	delete(s.pending, frame.ID)
	if request.method == "turn.execute" && request.collect {
		defer s.writeDeferredClose()
	}
	if request.destination != s.identity || !s.daemon.directory.current(request.destination, s) {
		s.answerRouted(request, answer{code: protocol.NotConnected})
		return
	}

	if request.method == "session.close" {
		s.finishCloseResponse(frame)
		return
	}
	if frame.Error != nil {
		// NotRunning on an ordinary worker delivery is a strict pre-submission
		// refusal: no native input was written or enqueued. Retain it for an
		// automatic run, answering now so mutually sending active runs cannot
		// deadlock waiting for each other's completion. Never replay a seeded
		// run or an uncertain native write.
		if frame.Error.Code == protocol.NotRunning && request.method == "message.deliver" && request.runID == "" && !s.identity.peer && s.closeCall == nil {
			if s.runID == "" {
				s.issue(request)
			} else if !s.hasRunCapacity() {
				s.answerRouted(request, answer{code: protocol.Busy})
			} else {
				s.answerRouted(request, answer{value: &protocol.DeliveryReceipt{Disposition: "queued_for_next_turn"}})
				request.scheduled, request.reply = true, nil
				s.deferredDeliveries = append(s.deferredDeliveries, request)
			}
			return
		}
		if request.runID != "" && (request.method == "turn.execute" || frame.Error.Code != protocol.Internal) {
			s.refuseRun(request.runID)
		}
		s.answerRouted(request, errorAnswer(frame.Error))
		s.startDeferredDelivery()
		return
	}
	value, err := protocol.DecodeResult(request.method, frame.Result)
	if err != nil {
		s.wire.Close()
		s.answerRouted(request, answer{code: protocol.NotConnected})
		return
	}
	if request.method == "turn.execute" {
		ref := value.(*protocol.RunRef)
		if ref.RunID != request.runID || ref.SessionID != s.identity.row.SessionID {
			s.wire.Close()
			s.answerRouted(request, answer{code: protocol.NotConnected})
			return
		}
		if request.collect {
			request.method, request.params, request.runID = "turn.wait", &protocol.WaitRequest{SessionID: ref.SessionID, RunID: ref.RunID}, ""
			s.issue(request)
			return
		}
	}
	if request.method == "turn.ack" {
		s.recordAcknowledgment(request.params.(*protocol.RunRef))
	}
	s.answerRouted(request, answer{value: value})
}

func errorAnswer(value *protocol.RPCError) answer {
	result := answer{code: value.Code}
	if len(value.Data) != 0 {
		_ = json.Unmarshal(value.Data, &result.data)
	}
	return result
}

func (s *session) connectionClosed() {
	if s.closed {
		return
	}
	s.logSession(commslog.Disconnected)
	s.stopping, s.closed = true, true
	s.stopAutoClose()
	s.wire.OwnerClosed()
	s.daemon.directory.detach(s.identity, s)
	s.settlePending(protocol.NotConnected)
	for id, release := range s.inboundBulk {
		release()
		delete(s.inboundBulk, id)
	}
	for id, state := range s.requests {
		s.logResult(state.frame, nil, protocol.NotConnected)
		s.finishTrace(state.frame, nil, protocol.NotConnected, nil)
		delete(s.requests, id)
	}
}

func (s *session) settlePending(code int) {
	s.settleDeferredDeliveries(code)
	for q := range s.queued {
		delete(s.queued, q)
		if s.closeCall == nil || q.reply != s.closeCall.reply {
			s.answerRouted(*q, answer{code: code})
		}
	}
	for id, request := range s.pending {
		delete(s.pending, id)
		if request.method == "turn.run" {
			s.daemon.directory.finishRun(s.identity, s)
		}
		if s.closeCall != nil && request.reply == s.closeCall.reply {
			continue
		}
		s.answerRouted(request, answer{code: code})
	}
}

func (s *session) settleDeferredDeliveries(code int) {
	for _, request := range s.deferredDeliveries {
		s.answerRouted(request, answer{code: code})
	}
	s.deferredDeliveries = nil
}

func (s *session) drain() bool {
	for {
		select {
		case event := <-s.inbox:
			s.handleEvent(event)
		default:
			return s.owned == 0
		}
	}
}

func (s *session) reject(frame protocol.Frame, code int) {
	s.logResult(frame, nil, code)
	s.finishTrace(frame, nil, code, nil)
	if s.forwarded != nil {
		s.forwarded <- s.traceReply(frame.Method, answer{code: code})
		return
	}
	s.stopping = true
	if frame.Request && frame.ID > 0 {
		body, _ := protocol.ErrorBytes(frame.ID, code, nil)
		if s.wire.Finish(body, supersedeWriteBound) {
			return
		}
	}
	s.wire.Close()
}

func (s *session) result(frame protocol.Frame, value any) {
	defer s.releaseBulk(frame.ID)
	s.logResult(frame, value, 0)
	s.finishTrace(frame, value, 0, nil)
	if s.forwarded != nil {
		s.forwarded <- s.traceReply(frame.Method, answer{value: value})
		return
	}
	body, err := protocol.ResultBytes(frame.ID, frame.Method, value)
	if err != nil || !s.wire.Send(body) {
		s.wire.Close()
	}
}

func (s *session) error(frame protocol.Frame, code int, data any) {
	defer s.releaseBulk(frame.ID)
	s.logResult(frame, nil, code)
	s.finishTrace(frame, nil, code, nil)
	if s.forwarded != nil {
		s.forwarded <- s.traceReply(frame.Method, answer{code: code, data: data})
		return
	}
	body, err := protocol.ErrorBytes(frame.ID, code, data)
	if err != nil || !s.wire.Send(body) {
		s.wire.Close()
	}
}

func (s *session) supersede() {
	if s.stopping {
		return
	}
	s.logSession(commslog.Superseded)
	s.stopping = true
	s.detachRequests(protocol.Superseded)
	if !s.wire.FinishRequest(func(id int64) ([]byte, error) { return protocol.RequestBytes(id, "session.superseded", struct{}{}) }, supersedeWriteBound) {
		s.wire.Close()
	}
}

func (s *session) await(requestID int64, leg int, reply <-chan answer, lifetime <-chan struct{}) {
	s.owned++
	go func() {
		value := answer{code: protocol.NotConnected}
		select {
		case value = <-reply:
		case <-lifetime:
		}
		s.inbox <- replyEvent{requestID: requestID, leg: leg, answer: value}
	}()
}

func (s *session) releaseBulk(id int64) {
	if release := s.inboundBulk[id]; release != nil {
		delete(s.inboundBulk, id)
		release()
	}
}

// Called only by the owner. Dispatch is queued before the corresponding socket
// write and drained before a reply can be correlated.
func (s *session) dispatched(event conn.Dispatched) {
	switch p := event.Value.(type) {
	case *routedRequest:
		if s.queued[p] {
			delete(s.queued, p)
			s.pending[event.ID] = *p
		}
	case openDispatch:
		s.openID = event.ID
	}
}

type openDispatch struct{}

func (s *session) pendingCount(bulk bool) int {
	n := 0
	if !bulk {
		n = len(s.deferredDeliveries)
	}
	for _, p := range s.pending {
		if (p.method == "transfer.chunk") == bulk {
			n++
		}
	}
	for p := range s.queued {
		if (p.method == "transfer.chunk") == bulk {
			n++
		}
	}
	return n
}
func (s *session) requestCount(bulk bool) int {
	n := 0
	for _, p := range s.requests {
		if (p.frame.Method == "transfer.chunk") == bulk {
			n++
		}
	}
	return n
}
