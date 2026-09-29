// SPDX-License-Identifier: MIT

// Package sessionkit is the product-agnostic Go implementation of Sessionbus.
package sessionkit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/antst/sessionbus/bus/sdk/go/internal/rpc"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type WorkerCallbacks interface {
	Hello(context.Context) (HelloDescription, error)
	Open(context.Context, OpenRequest) (OpenResult, error)
	Run(context.Context, *Run, RunInput) (TurnResult, error)
	Interrupt(context.Context, *Run) error
	Deliver(context.Context, DeliveryRequest, *Run) (DeliveryReceipt, error)
	Close(context.Context, SessionCloseRequest) error
}

// RunInput is exactly one caller input or an idle delivery seed.
type RunInput struct {
	Text     *string
	Delivery *DeliveryRequest
}

type Run struct {
	receipt     func(DeliveryReceipt, error) error
	receiptOnce sync.Once

	Native      any
	context     context.Context
	cancel      context.CancelFunc
	done        <-chan struct{}
	finish      context.CancelFunc
	admitted    chan struct{}
	admit       sync.Once
	interrupted atomic.Bool
}

func (r *Run) Interrupted() bool             { return r.interrupted.Load() }
func (r *Run) Done() <-chan struct{}         { return r.done }
func (r *Run) Admitted()                     { r.admit.Do(func() { close(r.admitted) }) }
func (r *Run) AdmittedDone() <-chan struct{} { return r.admitted }

// ReportDelivery answers the original delivery RPC once. It may block on the
// transport write; adapters must not call it while holding their native reader lock.
func (r *Run) ReportDelivery(value DeliveryReceipt, failure error) error {
	if r.receipt == nil {
		return errors.New("run has no delivery seed")
	}
	called := false
	var err error
	r.receiptOnce.Do(func() { called = true; err = r.receipt(value, failure) })
	if !called {
		return errors.New("delivery receipt already reported")
	}
	return err
}

type Worker struct {
	supportsWake bool
	sessionID    string
	records      []*workerRecord
	changed      chan struct{}
	generation   string
	lastSequence uint64
	acknowledged uint64
	waiters      int

	product      WorkerCallbacks
	caller       *Caller
	dial         func(context.Context, string, string) (net.Conn, error)
	mu           sync.Mutex
	conn         *rpc.Conn
	serveStarted bool
	shuttingDown bool
	serveCancel  context.CancelFunc
	context      context.Context
	cancel       context.CancelFunc
	run          *Run
	closeRequest SessionCloseRequest
	opened       atomic.Bool
	openDone     chan struct{}
	once         sync.Once
	closed       chan struct{}
}

func NewWorker(product WorkerCallbacks) *Worker {
	worker := &Worker{product: product, dial: (&net.Dialer{}).DialContext, closed: make(chan struct{}), changed: make(chan struct{})}
	worker.caller = NewCaller(func(ctx context.Context, method string, params any) (result json.RawMessage, err error) {
		err = worker.Call(ctx, method, params, &result)
		return
	})
	return worker
}

func (w *Worker) Closed() <-chan struct{} { return w.closed }
func (w *Worker) Caller() *Caller         { return w.caller }

func (w *Worker) Serve(ctx context.Context) error {
	if ctx == nil {
		return errors.New("worker context is nil")
	}
	w.mu.Lock()
	if w.serveStarted {
		w.mu.Unlock()
		return errors.New("worker Serve is single-use")
	}
	w.serveStarted = true
	ctx, cancel := context.WithCancel(ctx)
	w.serveCancel = cancel
	if w.shuttingDown {
		cancel()
	}
	w.mu.Unlock()
	defer close(w.closed)
	defer cancel()
	endpoint, token, err := sessionEnvironment(true)
	if err != nil {
		return err
	}
	hello, err := w.product.Hello(ctx)
	if err != nil {
		return err
	}
	w.supportsWake = hello.SupportsMessageRun
	fd, err := w.dial(ctx, "unix", endpoint)
	if err != nil {
		return err
	}
	// The reader cannot dispatch a frame before the connection and product
	// lifetime are published. Shutdown owns cancellation even before dialing.
	assigned := make(chan struct{})
	conn := rpc.New(fd, true, func(ctx context.Context, request *rpc.Request) {
		<-assigned
		w.handle(ctx, request)
	})
	w.mu.Lock()
	w.conn = conn
	w.context, w.cancel = context.WithCancel(conn.Context())
	w.mu.Unlock()
	close(assigned)
	stopDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = conn.Close(); close(stopDone) })
	defer func() {
		if !stop() {
			<-stopDone
		}
	}()
	request := protocol.WorkerHello{Protocol: 1, LaunchToken: token, HelloDescription: hello}
	if err = w.conn.Call(ctx, "session.hello", request, &struct{}{}); err == nil {
		<-w.conn.Done()
		err = rpc.ErrClosed
	}
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	_ = w.conn.Close()
	w.mu.Lock()
	if w.run != nil && w.run.finish != nil {
		w.run.finish()
	}
	w.mu.Unlock()
	w.closeProduct(w.conn.Context())
	return err
}

func (w *Worker) Call(ctx context.Context, method string, params, result any) error {
	w.mu.Lock()
	conn := w.conn
	w.mu.Unlock()
	if conn == nil {
		return errNotConnected
	}
	return conn.Call(ctx, method, params, result)
}

// Shutdown cancels startup or the active transport. Serve owns product cleanup;
// callers that started Serve can wait for Closed to observe its completion.
func (w *Worker) Shutdown() {
	w.mu.Lock()
	w.shuttingDown = true
	cancel, conn := w.serveCancel, w.conn
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if conn != nil {
		_ = conn.Close()
	}
}

func (w *Worker) handle(ctx context.Context, request *rpc.Request) {
	switch request.Method {
	case "session.superseded":
		go func() { w.reply(w.conn.Result(request, struct{}{})); _ = w.conn.Close() }()
	case "session.open":
		// Register adoption on the reader before Open is scheduled. EOF cleanup
		// must join this callback even if native success arrives after EOF.
		w.mu.Lock()
		if w.openDone != nil || ctx.Err() != nil || w.run != nil && w.run.done == nil {
			w.mu.Unlock()
			go w.answer(request, nil, protocol.InvalidFrame)
			return
		}
		done := make(chan struct{})
		w.openDone = done
		w.mu.Unlock()
		go func() { defer close(done); w.open(w.context, request) }()
	case "turn.execute":
		w.execute(request, nil)
	case "turn.status", "turn.wait":
		w.readRun(request)
	case "turn.ack":
		w.ackRun(request)
	case "turn.interrupt":
		w.mu.Lock()
		if w.run == nil {
			w.mu.Unlock()
			go w.answer(request, nil, protocol.NotRunning)
			return
		}
		slot := w.run
		if slot.context != nil && slot.context.Err() != nil {
			w.mu.Unlock()
			go w.answer(request, nil, protocol.NotRunning)
			return
		}
		call := slot.context != nil && slot.interrupted.CompareAndSwap(false, true)
		w.mu.Unlock()
		go w.interrupt(request, slot, call)
	case "message.deliver":
		delivery := request.Params.(*DeliveryRequest)
		if delivery.RunID != "" {
			w.execute(request, delivery)
			return
		}
		w.mu.Lock()
		slot := w.run
		closing := slot != nil && slot.done == nil
		w.mu.Unlock()
		if closing {
			go w.answer(request, DeliveryReceipt{Disposition: "rejected", Reason: "closing"}, 0)
			return
		}
		go w.deliver(w.context, request, slot)
	case "session.close":
		w.mu.Lock()
		slot := w.run
		if slot != nil && slot.done == nil {
			w.mu.Unlock()
			_ = w.conn.Close()
			return
		}
		w.run = &Run{}
		w.run.interrupted.Store(true)
		w.closeRequest = *request.Params.(*SessionCloseRequest)
		interrupt := slot != nil && slot.context.Err() == nil && slot.interrupted.CompareAndSwap(false, true)
		w.mu.Unlock()
		go w.close(ctx, request, slot, interrupt)
	}
}

func (w *Worker) open(ctx context.Context, request *rpc.Request) {
	if request.Params.(*OpenRequest).Policy != nil && !w.supportsWake {
		w.reply(w.conn.Error(request, protocol.UnsupportedOpen, nil))
		return
	}
	result, err := w.product.Open(ctx, *request.Params.(*OpenRequest))
	if err != nil {
		w.reply(w.conn.Error(request, protocol.SpawnFailed, map[string]any{"stderr_tail": []string{err.Error()}}))
		return
	}
	w.mu.Lock()
	w.sessionID = result.SessionID
	name := request.Params.(*OpenRequest).Name
	if at := strings.LastIndexByte(name, '@'); at >= 0 {
		w.sessionID += name[at:]
	}
	w.opened.Store(true)
	w.mu.Unlock()
	w.reply(w.conn.Result(request, result))
}

func (w *Worker) interrupt(request *rpc.Request, run *Run, call bool) {
	if call {
		callbackError("interrupt", w.nativeInterrupt(run))
	}
	w.reply(w.conn.Result(request, struct{}{}))
}

func (w *Worker) deliver(ctx context.Context, request *rpc.Request, run *Run) {
	// The native Run can finish before this goroutine gets scheduled, while
	// turn.ready is still in flight. No native submission has occurred here.
	if run == nil || run.context.Err() != nil {
		w.reply(w.conn.Error(request, protocol.NotRunning, nil))
		return
	}
	receipt, err := w.product.Deliver(ctx, *request.Params.(*DeliveryRequest), run)
	w.reply(w.deliveryReply(request, receipt, err))
}

func (w *Worker) deliveryReply(request *rpc.Request, receipt DeliveryReceipt, err error) error {
	var failure *ProtocolError
	if errors.As(err, &failure) {
		if failure.Code == protocol.NotRunning {
			return w.conn.Error(request, failure.Code, nil)
		}
		if failure.Code == protocol.Internal {
			return w.conn.Error(request, failure.Code, failure.Data)
		}
	}
	if err != nil {
		receipt = DeliveryReceipt{Disposition: "rejected", Reason: err.Error()}
	}
	return w.conn.Result(request, receipt)
}

func (w *Worker) close(ctx context.Context, request *rpc.Request, slot *Run, interrupt bool) {
	if interrupt {
		go func() {
			callbackError("interrupt", w.nativeInterrupt(slot))
		}()
	}
	if slot != nil {
		select {
		case <-slot.done:
		case <-w.conn.Context().Done():
			slot.finish()
			return
		}
	}
	for {
		w.mu.Lock()
		count, changed := w.waiters, w.changed
		w.mu.Unlock()
		if count == 0 {
			break
		}
		select {
		case <-changed:
		case <-w.conn.Context().Done():
			w.cancel()
			return
		}
	}
	w.cancel()
	w.closeProduct(ctx)
	w.reply(w.conn.Result(request, struct{}{}))
	_ = w.conn.Close()
}

func (w *Worker) nativeInterrupt(run *Run) error {
	if run.context.Err() != nil {
		return nil //nolint:nilerr // An already-cancelled run has nothing left to interrupt.
	}
	return w.product.Interrupt(run.context, run)
}

func (w *Worker) answer(request *rpc.Request, value any, code int) {
	if code != 0 {
		w.reply(w.conn.Error(request, code, nil))
	} else {
		w.reply(w.conn.Result(request, value))
	}
}

func (w *Worker) closeProduct(ctx context.Context) {
	w.mu.Lock()
	opening := w.openDone
	w.mu.Unlock()
	if opening != nil {
		<-opening
	}
	w.once.Do(func() {
		w.mu.Lock()
		request := w.closeRequest
		w.mu.Unlock()
		if w.opened.Load() {
			callbackError("close", w.product.Close(ctx, request))
		}
	})
}

func callbackError(callback string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "sessionbus: product %s: %q\n", callback, err.Error())
	}
}

func (w *Worker) reply(err error) {
	if err != nil {
		_ = w.conn.Close()
	}
}

func sessionEnvironment(worker bool) (string, string, error) {
	token, ok := os.LookupEnv("SESSIONBUS_LAUNCH_TOKEN")
	key, endpoint := os.Getenv("SESSIONBUS_LOCAL_KEY"), Socket()
	for _, name := range []string{"SESSIONBUS_LAUNCH_TOKEN", "SESSIONBUS_LOCAL_KEY", "SESSIONBUS_SOCKET"} {
		_ = os.Unsetenv(name)
	}
	if worker && (!ok || token == "") {
		return "", "", errors.New("launch token is required")
	}
	if endpoint == "" {
		return "", "", errors.New("sessionbus socket is required")
	}
	if key != "" {
		return "", "", errors.New("local key transport not implemented in this build")
	}
	return endpoint, token, nil
}
