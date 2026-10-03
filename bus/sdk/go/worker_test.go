// SPDX-License-Identifier: MIT

package sessionkit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/internal/rpc"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type fakeProduct struct {
	deliveryResult          *DeliveryReceipt
	deliveryError           error
	worker                  *Worker
	started                 chan *Run
	release, interrupted    chan struct{}
	deliverStart            chan struct{}
	deliverRun              chan *Run
	deliverRelease          chan struct{}
	admit                   chan struct{}
	nativeEvents            chan string
	closeStart, closeEnd    chan struct{}
	closeContext            chan error
	closeRequest            chan SessionCloseRequest
	outbound, hangInterrupt bool
	closeErr, interruptErr  error
	interruptCall           func(context.Context, *Run) error
	calls                   [6]int32
}

func (p *fakeProduct) Hello(context.Context) (HelloDescription, error) {
	atomic.AddInt32(&p.calls[0], 1)
	if os.Getenv("SESSIONBUS_LAUNCH_TOKEN") != "" || os.Getenv("SESSIONBUS_LOCAL_KEY") != "" || os.Getenv("SESSIONBUS_SOCKET") != "" {
		panic("worker environment reached Hello")
	}
	return HelloDescription{Product: "example-peer", SupportedOpenFields: []string{}, ExtraArguments: []ExtraArgument{}}, nil
}
func (p *fakeProduct) Open(_ context.Context, request OpenRequest) (OpenResult, error) {
	atomic.AddInt32(&p.calls[1], 1)
	if request.Name != "parent/leaf@local" || len(request.Groups) != 1 || request.Groups[0] != "session:parent" || request.ResumeSessionID != "product-session" || request.Open.Cwd != "/tmp" || request.Open.PermissionMode != "ask" || request.Open.Model != "model" || request.Open.ReasoningEffort != "high" || len(request.Open.Arguments) != 1 || request.Open.Arguments[0] != "--flag" {
		return OpenResult{}, errors.New("open request changed")
	}
	return OpenResult{SessionID: "product-session"}, nil
}
func (p *fakeProduct) Run(ctx context.Context, run *Run, seed RunInput) (TurnResult, error) {
	input := *seed.Text
	atomic.AddInt32(&p.calls[2], 1)
	switch input {
	case "block":
		p.started <- run
		<-p.release
		if run.Interrupted() {
			return TurnResult{Outcome: "interrupted"}, nil
		}
		run.Native = "native"
		return TurnResult{Outcome: "completed"}, nil
	case "eof":
		p.started <- run
		<-ctx.Done()
		return TurnResult{}, ctx.Err()
	case "admit":
		p.started <- run
		p.nativeEvents <- "input"
		<-p.admit
		run.Admitted()
		run.Admitted()
		<-p.release
		return TurnResult{Outcome: "completed"}, nil
	case "fail":
		return TurnResult{}, p
	case "long":
		return TurnResult{Outcome: "completed", Result: strings.Repeat("x", protocol.MaxTextRunes+1)}, nil
	case "completed", "interrupted":
		return TurnResult{Outcome: input}, nil
	default:
		return TurnResult{Outcome: "completed", Result: input}, nil
	}
}
func (p *fakeProduct) Interrupt(ctx context.Context, run *Run) error {
	atomic.AddInt32(&p.calls[3], 1)
	if !run.Interrupted() {
		return errors.New("run was not marked interrupted")
	}
	if p.interruptCall != nil {
		return p.interruptCall(ctx, run)
	}
	if p.interrupted != nil {
		close(p.interrupted)
	}
	if p.hangInterrupt {
		<-ctx.Done()
	}
	return p.interruptErr
}
func (p *fakeProduct) Deliver(ctx context.Context, _ DeliveryRequest, run *Run) (DeliveryReceipt, error) {
	atomic.AddInt32(&p.calls[4], 1)
	if p.deliveryResult != nil {
		return *p.deliveryResult, p.deliveryError
	}
	if p.deliverRun != nil {
		p.deliverRun <- run
		if p.deliverRelease != nil {
			<-p.deliverRelease
		}
		if run == nil {
			return DeliveryReceipt{}, &ProtocolError{Code: protocol.NotRunning, Message: "not_running"}
		}
		select {
		case <-run.Done():
			return DeliveryReceipt{}, &ProtocolError{Code: protocol.NotRunning, Message: "not_running"}
		default:
		}
		select {
		case <-run.Done():
			return DeliveryReceipt{}, &ProtocolError{Code: protocol.NotRunning, Message: "not_running"}
		case <-run.AdmittedDone():
		}
		select {
		case <-run.Done():
			return DeliveryReceipt{}, &ProtocolError{Code: protocol.NotRunning, Message: "not_running"}
		default:
		}
		if p.nativeEvents != nil {
			p.nativeEvents <- "steer"
		}
		return DeliveryReceipt{Disposition: "injected"}, nil
	}
	if p.deliverStart != nil {
		close(p.deliverStart)
		<-ctx.Done()
		return DeliveryReceipt{}, ctx.Err()
	}
	if p.outbound {
		_, err := p.worker.Caller().List(ctx, protocol.SessionListRequest{})
		return DeliveryReceipt{Disposition: "injected"}, err
	}
	return DeliveryReceipt{Disposition: "injected"}, nil
}

func TestWorkerIdleDeliveryRequiresSeed(t *testing.T) {
	p := &fakeProduct{deliverRun: make(chan *Run)}
	h := startHarness(t, p, true, true)
	var receipt DeliveryReceipt
	wantCode(t, h.Call(context.Background(), "message.deliver", delivery, &receipt), protocol.NotRunning)
	check(t, atomic.LoadInt32(&p.calls[4]) == 0, "idle delivery reached product")
}

func TestWorkerDeliveryKeepsAdmissionRun(t *testing.T) {
	p := &fakeProduct{started: make(chan *Run), release: make(chan struct{}), deliverRun: make(chan *Run), deliverRelease: make(chan struct{})}
	h := startHarness(t, p, true, true)
	running := async(h, "turn.run", protocol.TurnRunRequest{SessionID: target.SessionID, Input: "block"}, &TurnResult{})
	run := <-p.started
	var receipt DeliveryReceipt
	delivered := async(h, "message.deliver", delivery, &receipt)
	check(t, <-p.deliverRun == run, "delivery did not receive the admitted run")
	close(p.release)
	check(t, <-running == nil, "run failed")
	<-run.Done()
	select {
	case <-run.AdmittedDone():
		t.Fatal("kit marked a failed native admission successful")
	default:
	}
	close(p.deliverRelease)
	wantCode(t, <-delivered, protocol.NotRunning)
}

func TestRunAdmissionOrdersDelivery(t *testing.T) {
	p := &fakeProduct{started: make(chan *Run), release: make(chan struct{}), admit: make(chan struct{}), nativeEvents: make(chan string, 2), deliverRun: make(chan *Run)}
	h := startHarness(t, p, true, true)
	running := async(h, "turn.run", protocol.TurnRunRequest{SessionID: target.SessionID, Input: "admit"}, &TurnResult{})
	run := <-p.started
	check(t, <-p.nativeEvents == "input", "native input was not first")
	var receipt DeliveryReceipt
	delivered := async(h, "message.deliver", delivery, &receipt)
	check(t, <-p.deliverRun == run, "active delivery received the wrong run")
	select {
	case event := <-p.nativeEvents:
		t.Fatalf("delivery crossed native admission: %s", event)
	default:
	}
	close(p.admit)
	check(t, <-p.nativeEvents == "steer", "native steer was not second")
	check(t, <-delivered == nil && receipt.Disposition == "injected", "admitted delivery receipt = %#v", receipt)
	close(p.release)
	check(t, <-running == nil, "run failed")
}
func (p *fakeProduct) Close(ctx context.Context, request SessionCloseRequest) error {
	atomic.AddInt32(&p.calls[5], 1)
	if p.closeContext != nil {
		p.closeContext <- ctx.Err()
	}
	if p.closeRequest != nil {
		p.closeRequest <- request
	}
	if p.closeStart != nil {
		close(p.closeStart)
		<-p.closeEnd
	}
	return p.closeErr
}
func (p *fakeProduct) Error() string {
	if p.deliverStart != nil {
		close(p.deliverStart)
		<-p.release
	}
	return "failed exactly"
}
func startHarness(t *testing.T, p *fakeProduct, acknowledge, openNow bool) *workerHarness {
	return startHarnessContext(t, context.Background(), p, acknowledge, openNow)
}

func startHarnessContext(t *testing.T, ctx context.Context, p *fakeProduct, acknowledge, openNow bool) *workerHarness {
	setEnvironment(t, "token", "")
	worker, daemon := net.Pipe()
	p.worker = NewWorker(p)
	caller := p.worker.Caller()
	check(t, caller == p.worker.Caller(), "worker did not retain one pre-serve caller")
	p.worker.dial = func(_ context.Context, network, address string) (net.Conn, error) {
		check(t, atomic.LoadInt32(&p.calls[0]) == 1 && network == "unix" && address == "/fixture/socket", "dial before hello or wrong endpoint")
		return worker, nil
	}
	hello := make(chan struct{})
	var h *rpc.Conn
	transport := &workerTestTransport{Conn: daemon, waitWritten: make(chan struct{}, 1)}
	h = rpc.New(transport, false, func(_ context.Context, request *rpc.Request) {
		switch request.Method {
		case "session.hello":
			if acknowledge {
				check(t, h.Result(request, struct{}{}) == nil, "hello response failed")
			} else {
				_ = h.Close()
			}
			close(hello)
		case "turn.ready":
			go func() { _ = h.Result(request, struct{}{}) }()
		case "session.list":
			if p.worker.opened.Load() {
				check(t, h.Result(request, protocol.SessionListResult{Sessions: []protocol.SessionSummary{}}) == nil, "list response failed")
			} else {
				check(t, h.Error(request, protocol.NotCommitted, nil) == nil, "list error response failed")
			}
		}
	})
	go p.worker.Serve(ctx)
	<-hello
	t.Cleanup(func() { p.worker.Shutdown(); <-p.worker.Closed() })
	harness := &workerHarness{Conn: h, waitWritten: transport.waitWritten}
	if openNow {
		open(t, harness)
	}
	return harness
}

// workerHarness performs the daemon's execute-then-wait translation on the
// real wire for the shared native callback lifecycle rows.
type workerHarness struct {
	*rpc.Conn
	mu          sync.Mutex
	sequence    uint64
	waitWritten <-chan struct{}
}

type workerTestTransport struct {
	net.Conn
	waitWritten chan struct{}
}

func (c *workerTestTransport) Write(body []byte) (int, error) {
	n, err := c.Conn.Write(body)
	if bytes.Contains(body, []byte(`"method":"turn.wait"`)) {
		c.waitWritten <- struct{}{}
	}
	return n, err
}
func (h *workerHarness) Call(ctx context.Context, method string, params, result any) error {
	if method != "turn.run" {
		if method == "session.close" {
			h.mu.Lock()
			h.mu.Unlock() //nolint:staticcheck // Barrier: the earlier blocking run has written its read frame.
		}
		return h.Conn.Call(ctx, method, params, result)
	}
	input := params.(protocol.TurnRunRequest)
	h.mu.Lock()
	h.sequence++
	id := "fixture/" + strconv.FormatUint(h.sequence, 10)
	var ref RunRef
	err := h.Conn.Call(ctx, "turn.execute", protocol.ExecuteRequest{SessionID: input.SessionID, RunID: id, Input: input.Input}, &ref)
	if err != nil {
		h.sequence--
	}
	if err != nil {
		h.mu.Unlock()
		return err
	}
	var status RunStatus
	waiting := make(chan error, 1)
	go func() {
		waiting <- h.Conn.Call(ctx, "turn.wait", WaitRequest{SessionID: ref.SessionID, RunID: ref.RunID}, &status)
	}()
	select {
	case <-h.waitWritten:
	case <-h.Conn.Done():
	}
	h.mu.Unlock()
	err = <-waiting
	if err != nil {
		return err
	}
	if value, ok := result.(*RunStatus); ok {
		*value = status
		return nil
	}
	if status.Result == nil {
		return errors.New(status.Reason)
	}
	*result.(*TurnResult) = *status.Result
	return nil
}

func async(h interface {
	Call(context.Context, string, any, any) error
}, method string, params, result any) <-chan error {
	done := make(chan error, 1)
	go func() { done <- h.Call(context.Background(), method, params, result) }()
	return done
}
func open(t *testing.T, h *workerHarness) {
	var result OpenResult
	check(t, h.Call(context.Background(), "session.open", OpenRequest{Name: "parent/leaf@local", Groups: []string{"session:parent"}, ResumeSessionID: "product-session", Open: OpenOptions{Cwd: "/tmp", PermissionMode: "ask", Model: "model", ReasoningEffort: "high", Arguments: []string{"--flag"}}}, &result) == nil && result.SessionID == "product-session", "open result = %#v", result)
}

func TestWorkerLifecycleTable(t *testing.T) {
	stderrReader, stderrWriter, err := os.Pipe()
	check(t, err == nil, "stderr pipe: %v", err)
	previousStderr := os.Stderr
	os.Stderr = stderrWriter
	defer func() { os.Stderr = previousStderr; _ = stderrReader.Close(); _ = stderrWriter.Close() }()
	var table []struct {
		Name  string   `json:"name"`
		Calls [6]int32 `json:"calls"`
	}
	raw, err := os.ReadFile("protocol/session-lifecycle.fixtures.json")
	check(t, err == nil && json.Unmarshal(raw, &table) == nil, "read lifecycle table: %v", err)
	for _, row := range table {
		t.Run(row.Name, func(t *testing.T) {
			check(t, runCase(t, row.Name) == row.Calls, "callback calls did not match %v", row.Calls)
		})
	}
	os.Stderr = previousStderr
	check(t, stderrWriter.Close() == nil, "close stderr writer")
	raw, err = io.ReadAll(stderrReader)
	check(t, err == nil, "read stderr: %v", err)
	counts := map[string]int{}
	for _, line := range strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n") {
		counts[line]++
	}
	interrupt := `sessionbus: product interrupt: "first failure\nsecond failure"`
	closeLine := `sessionbus: product close: "first failure\nsecond failure"`
	check(t, len(counts) == 2 && counts[interrupt] == 2 && counts[closeLine] == 2, "callback stderr = %q", raw)
}

func TestWorkerInterruptRetry(t *testing.T) {
	for _, failed := range []bool{false, true} {
		name := "success"
		if failed {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			p := &fakeProduct{started: make(chan *Run), release: make(chan struct{})}
			h := startHarness(t, p, true, true)
			running := async(h, "turn.run", protocol.TurnRunRequest{SessionID: target.SessionID, Input: "block"}, &RunStatus{})
			run := <-p.started
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce, finishOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			defer finishOnce.Do(func() { close(p.release) })
			p.interruptCall = func(ctx context.Context, current *Run) error {
				if current != run {
					t.Error("interrupt changed Run identity")
				}
				if atomic.LoadInt32(&p.calls[3]) == 1 {
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
					if failed {
						return errors.New("first interrupt failed")
					}
				}
				return nil
			}
			first := async(h, "turn.interrupt", target, &struct{}{})
			<-entered
			check(t, h.Call(context.Background(), "turn.interrupt", target, &struct{}{}) == nil, "in-flight duplicate failed")
			check(t, atomic.LoadInt32(&p.calls[3]) == 1 && run.Interrupted(), "in-flight duplicate called product or lost intent")
			releaseOnce.Do(func() { close(release) })
			err := <-first
			if failed {
				var failure *protocol.RPCError
				var detail string
				if !errors.As(err, &failure) || failure.Code != protocol.Internal || json.Unmarshal(failure.Data, &detail) != nil || detail != "product interrupt failed" {
					t.Errorf("first interrupt error = %#v, want Internal/string", err)
				}
				if run.Interrupted() {
					t.Error("failed interrupt kept the attempt mark")
				}
			} else {
				check(t, err == nil && run.Interrupted(), "successful interrupt failed or lost its mark: %v", err)
			}
			check(t, h.Call(context.Background(), "turn.interrupt", target, &struct{}{}) == nil, "later explicit interrupt failed")
			check(t, h.Call(context.Background(), "turn.interrupt", target, &struct{}{}) == nil, "successful duplicate failed")
			want := int32(1)
			if failed {
				want = 2
			}
			calls := atomic.LoadInt32(&p.calls[3])
			if calls != want {
				t.Errorf("product interrupt calls = %d, want %d", calls, want)
			}
			finishOnce.Do(func() { close(p.release) })
			check(t, <-running == nil, "run result failed")
			<-run.Done()
			wantCode(t, h.Call(context.Background(), "turn.interrupt", target, &struct{}{}), protocol.NotRunning)
			check(t, atomic.LoadInt32(&p.calls[3]) == calls, "ended Run invoked product interrupt")
		})
	}
}

func runCase(t *testing.T, name string) [6]int32 {
	p := &fakeProduct{}
	switch name {
	case "ready-open-commit":
		h := startHarness(t, p, true, false)
		wantCode(t, p.worker.Call(context.Background(), "session.list", protocol.SessionListRequest{}, &protocol.SessionListResult{}), protocol.NotCommitted)
		open(t, h)
		check(t, p.worker.Call(context.Background(), "session.list", protocol.SessionListRequest{}, &protocol.SessionListResult{}) == nil, "worker list failed")
	case "describe-eof":
		startHarness(t, p, false, false)
	case "terminal-results":
		h := startHarness(t, p, true, true)
		for _, input := range []string{"ok", "interrupted", "fail", "completed", "long"} {
			var result RunStatus
			check(t, h.Call(context.Background(), "turn.run", protocol.TurnRunRequest{SessionID: target.SessionID, Input: input}, &result) == nil, "read failed")
			if input == "fail" || input == "long" {
				check(t, result.State == "unavailable" && result.Result == nil, "invalid native result %#v", result)
				continue
			}
			want := TurnResult{Outcome: "completed"}
			if input == "interrupted" {
				want.Outcome = input
			}
			if input == "ok" {
				want.Result = input
			}
			check(t, result.Result != nil && *result.Result == want, "terminal %#v", result)
		}
	case "one-run", "one-interrupt", "interrupt-error", "full-duplex", "close-during-run", "callback-originated-method", "run-done":
		blockingCase(t, name, p)
	case "eof-during-run", "run-done-write-failure":
		if name == "eof-during-run" {
			p.closeErr = errors.New("first failure\nsecond failure")
			p.closeContext = make(chan error, 1)
			p.closeRequest = make(chan SessionCloseRequest, 1)
		}
		h := startHarness(t, p, true, true)
		p.started = make(chan *Run)
		run := async(h, "turn.run", protocol.TurnRunRequest{SessionID: "product-session@local", Input: "eof"}, &TurnResult{})
		runToken := <-p.started
		_ = h.Close()
		<-p.worker.Closed()
		check(t, <-run != nil, "expected error")
		<-runToken.Done()
		if name == "eof-during-run" {
			check(t, <-p.closeContext != nil, "EOF left the product Close context live")
			check(t, <-p.closeRequest == (SessionCloseRequest{}), "pure EOF carried a close request")
		}
	case "peer-lifetime":
		h := startHarness(t, p, true, false)
		check(t, h.Call(context.Background(), "session.superseded", struct{}{}, &struct{}{}) == nil, "superseded call failed")
		<-p.worker.Closed()
		check(t, p.worker.Call(context.Background(), "session.list", protocol.SessionListRequest{}, &protocol.SessionListResult{}) != nil, "expected error")
	case "wrong-direction-request":
		h := startHarness(t, p, true, false)
		check(t, h.Call(context.Background(), "session.hello", protocol.WorkerHello{Protocol: 1, LaunchToken: "again", HelloDescription: HelloDescription{Product: "worker", SupportedOpenFields: []string{}, ExtraArguments: []ExtraArgument{}}}, &struct{}{}) != nil, "expected error")
	case "terminal-before-interrupt":
		p.release, p.deliverStart = make(chan struct{}), make(chan struct{})
		h := startHarness(t, p, true, true)
		terminal := async(h, "turn.run", protocol.TurnRunRequest{SessionID: "product-session@local", Input: "fail"}, &RunStatus{})
		<-p.deliverStart
		wantCode(t, h.Call(context.Background(), "turn.interrupt", target, &struct{}{}), protocol.NotRunning)
		close(p.release)
		check(t, <-terminal == nil, "terminal response failed")
	case "idle-close-deliver":
		h := startHarness(t, p, true, true)
		p.closeStart, p.closeEnd = make(chan struct{}), make(chan struct{})
		wantCode(t, h.Call(context.Background(), "message.deliver", delivery, &DeliveryReceipt{}), protocol.NotRunning)
		closed := async(h, "session.close", target, &struct{}{})
		<-p.closeStart
		checkDelivery(t, h, "rejected")
		close(p.closeEnd)
		check(t, <-closed == nil, "close response failed")
	case "environment":
		setEnvironment(t, "token", "key")
		err := NewWorker(p).Serve(context.Background())
		check(t, err != nil && err.Error() == "local key transport not implemented in this build" && os.Getenv("SESSIONBUS_LAUNCH_TOKEN") == "" && os.Getenv("SESSIONBUS_LOCAL_KEY") == "" && os.Getenv("SESSIONBUS_SOCKET") == "", "key error or uncleared environment: %v", err)
	case "shutdown":
		startHarness(t, p, true, false)
		p.worker.Shutdown()
	case "close-error":
		p.closeErr = errors.New("first failure\nsecond failure")
		p.closeContext = make(chan error, 1)
		p.closeRequest = make(chan SessionCloseRequest, 1)
		h := startHarness(t, p, true, true)
		check(t, h.Call(context.Background(), "session.close", target, &struct{}{}) == nil, "close error reached the wire")
		check(t, <-p.closeContext == nil, "session.close cancelled the product Close context")
		check(t, <-p.closeRequest == (SessionCloseRequest{SessionID: target.SessionID}), "ordinary close request changed")
	case "close-forget":
		p.started, p.release, p.interrupted = make(chan *Run), make(chan struct{}), make(chan struct{})
		p.closeContext, p.closeRequest = make(chan error, 1), make(chan SessionCloseRequest, 1)
		h := startHarness(t, p, true, true)
		running := async(h, "turn.run", protocol.TurnRunRequest{SessionID: target.SessionID, Input: "block"}, &TurnResult{})
		<-p.started
		request := SessionCloseRequest{SessionID: target.SessionID, Forget: true}
		closing := async(h, "session.close", request, &struct{}{})
		<-p.interrupted
		_ = h.Close()
		check(t, <-p.closeContext != nil, "EOF left crossed product Close context live")
		check(t, <-p.closeRequest == request, "product Close request changed")
		close(p.release)
		<-p.worker.Closed()
		check(t, <-running != nil && <-closing != nil, "crossed calls survived EOF")
	default:
		t.Fatalf("unknown lifecycle case %q", name)
	}
	return p.calls
}

func blockingCase(t *testing.T, name string, p *fakeProduct) {
	p.started, p.release = make(chan *Run), make(chan struct{})
	h := startHarness(t, p, true, true)
	run := async(h, "turn.run", protocol.TurnRunRequest{SessionID: "product-session@local", Input: "block"}, &TurnResult{})
	runToken := <-p.started
	switch name {
	case "one-run":
		wantCode(t, h.Call(context.Background(), "turn.run", protocol.TurnRunRequest{SessionID: "product-session@local", Input: "again"}, &TurnResult{}), protocol.Busy)
	case "one-interrupt":
		p.interrupted = make(chan struct{})
		first, second := async(h, "turn.interrupt", target, &struct{}{}), async(h, "turn.interrupt", target, &struct{}{})
		<-p.interrupted
		check(t, errors.Join(<-first, <-second) == nil, "interrupt response failed")
	case "interrupt-error":
		p.interruptErr = errors.New("first failure\nsecond failure")
		wantCode(t, h.Call(context.Background(), "turn.interrupt", target, &struct{}{}), protocol.Internal)
	case "full-duplex", "callback-originated-method":
		p.outbound = true
		checkDelivery(t, h, "injected")
	case "close-during-run":
		p.interrupted, p.closeStart, p.closeEnd, p.hangInterrupt = make(chan struct{}), make(chan struct{}), make(chan struct{}), true
		p.interruptErr = errors.New("first failure\nsecond failure")
		closed := async(h, "session.close", target, &struct{}{})
		<-p.interrupted
		check(t, h.Call(context.Background(), "turn.interrupt", target, &struct{}{}) == nil, "coalesced interrupt failed")
		close(p.release)
		check(t, <-run == nil, "run response failed")
		<-p.closeStart
		close(p.closeEnd)
		check(t, <-closed == nil, "close response failed")
		return
	case "run-done":
		select {
		case <-runToken.Done():
			t.Fatal("Run.Done closed before the terminal response")
		default:
		}
	}
	close(p.release)
	check(t, <-run == nil, "run response failed")
	<-runToken.Done()
	if name == "one-interrupt" {
		check(t, runToken.Native == nil, "native work started after pre-handoff interrupt")
	}
}

func checkDelivery(t *testing.T, h *workerHarness, disposition string) {
	var receipt DeliveryReceipt
	check(t, h.Call(context.Background(), "message.deliver", delivery, &receipt) == nil && receipt.Disposition == disposition, "receipt = %#v", receipt)
}

func setEnvironment(t *testing.T, token, key string) {
	for name, value := range map[string]string{"SESSIONBUS_LAUNCH_TOKEN": token, "SESSIONBUS_LOCAL_KEY": key, "SESSIONBUS_SOCKET": "/fixture/socket"} {
		t.Setenv(name, value)
	}
}

func wantCode(t *testing.T, err error, code int) {
	var rpcErr *protocol.RPCError
	check(t, errors.As(err, &rpcErr) && rpcErr.Code == code, "error = %v, want code %d", err, code)
}
func check(t *testing.T, condition bool, format string, args ...any) {
	if !condition {
		t.Fatalf(format, args...)
	}
}

var delivery = DeliveryRequest{MessageID: "m", From: DeliverySource{SessionID: "peer@local", Name: "peer@local", Product: "peer", Groups: []string{}}, Body: "body"}
var target = protocol.SessionTarget{SessionID: "product-session@local"}

func TestWorkerWrittenAndUncertainSubmission(t *testing.T) {
	for _, test := range []struct {
		name    string
		receipt DeliveryReceipt
		err     error
	}{
		{"complete write", DeliveryReceipt{Disposition: "written"}, nil},
		{"native EOF after complete write", DeliveryReceipt{Disposition: "written"}, nil},
		{"pre-write failure", DeliveryReceipt{}, errors.New("pre-write failure")},
		{"observed native refusal", DeliveryReceipt{Disposition: "rejected", Reason: "observed native refusal"}, nil},
		{"partial write", DeliveryReceipt{}, &ProtocolError{Code: protocol.Internal, Message: "internal", Data: json.RawMessage(`"partial write; submission uncertain"`)}},
		{"post-submission transport loss", DeliveryReceipt{}, &ProtocolError{Code: protocol.Internal, Message: "internal", Data: json.RawMessage(`"transport lost after submission; consumption unknown"`)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			product := &fakeProduct{deliveryResult: &test.receipt, deliveryError: test.err, started: make(chan *Run), release: make(chan struct{})}
			h := startHarness(t, product, true, true)
			running := async(h, "turn.run", protocol.TurnRunRequest{SessionID: target.SessionID, Input: "block"}, &TurnResult{})
			<-product.started
			t.Cleanup(func() { close(product.release); <-running })
			var receipt DeliveryReceipt
			err := h.Call(context.Background(), "message.deliver", delivery, &receipt)
			var failure *ProtocolError
			if errors.As(test.err, &failure) {
				var got *ProtocolError
				if !errors.As(err, &got) || got.Code != protocol.Internal || string(got.Data) != string(failure.Data) || receipt.Disposition != "" {
					t.Fatalf("uncertain submission: %#v, %v", receipt, err)
				}
			} else {
				mustRPC(t, err)
				want := test.receipt
				if test.err != nil {
					want = DeliveryReceipt{Disposition: "rejected", Reason: test.err.Error()}
				}
				if receipt != want {
					t.Fatalf("receipt = %#v, want %#v", receipt, want)
				}
			}
		})
	}
}
