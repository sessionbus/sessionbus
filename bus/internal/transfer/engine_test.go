// SPDX-License-Identifier: GPL-3.0-only
package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type testEndpoint struct {
	mu                                    sync.Mutex
	ep                                    Endpoint
	done                                  chan struct{}
	calls                                 []string
	held                                  map[string]bool
	size                                  int
	data                                  []byte
	published                             bool
	refuseHold, loseFinish, refuseCleanup bool
	notices                               []protocol.TransferNotice
}

func endpointForTest(key string) *testEndpoint {
	x := &testEndpoint{done: make(chan struct{}), held: map[string]bool{}, size: 4}
	x.ep = Endpoint{Key: key, Identity: protocol.DeliverySource{SessionID: key + "@host", Product: "fixture", Groups: []string{"test"}}, Done: x.done}
	x.ep.Call = func(ctx context.Context, method string, params, result any) error {
		x.mu.Lock()
		defer x.mu.Unlock()
		x.calls = append(x.calls, method)
		raw, err := protocol.EncodeParams(method, params)
		if err != nil {
			return err
		}
		var value any
		switch method {
		case "transfer.prepare":
			var p protocol.TransferPrepare
			_ = json.Unmarshal(raw, &p)
			if p.Role == "source" {
				n := protocol.Integer(x.size)
				value = protocol.TransferPrepared{ID: p.ID, Size: &n}
			} else {
				value = protocol.TransferPrepared{ID: p.ID, Ready: true}
			}
		case "transfer.start":
			var p protocol.TransferStart
			_ = json.Unmarshal(raw, &p)
			value = protocol.TransferStarted{ID: p.ID, Ready: p.Role == "destination", Started: p.Role == "source"}
		case "transfer.chunk":
			p := params.(protocol.TransferChunk)
			data, _ := base64.StdEncoding.DecodeString(p.Data)
			x.data = append(x.data, data...)
			value = protocol.TransferCredit{Offset: protocol.Integer(len(x.data))}
		case "transfer.finish":
			p := params.(protocol.TransferFinish)
			x.published = true
			if x.loseFinish {
				return errors.New("lost result AFTER publication")
			}
			value = protocol.TransferTerminal{ID: p.ID, Bytes: p.Bytes, SHA256: p.SHA256, Outcome: "completed"}
		}
		raw, err = protocol.EncodeResult(method, value)
		if err != nil {
			return err
		}
		return json.Unmarshal(raw, result)
	}
	x.ep.Hold = func(ctx context.Context, id string, terminal bool) bool {
		x.mu.Lock()
		defer x.mu.Unlock()
		if terminal {
			delete(x.held, id)
			return true
		}
		if x.refuseHold {
			return false
		}
		x.held[id] = true
		return true
	}
	x.ep.Notice = func(ctx context.Context, n protocol.TransferNotice) error {
		x.mu.Lock()
		defer x.mu.Unlock()
		if _, err := protocol.EncodeParams("transfer.notice", n); err != nil {
			return err
		}
		x.notices = append(x.notices, n)
		if n.Kind == "terminal" && x.refuseCleanup {
			return errors.New("cleanup unknown")
		}
		return nil
	}
	return x
}
func engineTest(t *testing.T) (*Engine, *testEndpoint, *testEndpoint) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return NewEngine(ctx, HostBudget()), endpointForTest("source"), endpointForTest("destination")
}
func eventual(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.After(time.Second)
	for !f() {
		select {
		case <-deadline:
			t.Fatal("condition not reached")
		case <-time.After(time.Millisecond):
		}
	}
}
func TestOfferAndRequestSamePreparationStreamingEngine(t *testing.T) {
	for _, offer := range []bool{true, false} {
		t.Run(map[bool]string{true: "offer", false: "request"}[offer], func(t *testing.T) {
			e, source, dest := engineTest(t)
			creator, other := source, dest
			if !offer {
				creator, other = dest, source
			}
			d, err := e.Create(creator.ep, other.ep, offer, "payload", "the requested file")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = protocol.EncodeResult("transfer.offer", d); err != nil {
				t.Fatal(err)
			}
			source.mu.Lock()
			if len(source.calls) != 0 || len(source.held) != 0 {
				t.Fatal("pending preparation/hold")
			}
			source.mu.Unlock()
			e.Announce(d.ID)
			if _, err = e.Accept(other.ep.Key, protocol.TransferAccept{ID: d.ID}); err != nil {
				t.Fatal(err)
			}
			if duplicate, duplicateErr := e.Accept(other.ep.Key, protocol.TransferAccept{ID: d.ID}); duplicateErr != nil || duplicate.ID != d.ID || duplicate.State != "streaming" {
				t.Fatal("live duplicate accept did not return its current descriptor", duplicate, duplicateErr)
			}
			credit, err := e.Chunk(source.ep.Key, protocol.TransferChunk{ID: d.ID, Data: base64.StdEncoding.EncodeToString([]byte("data"))})
			if err != nil || credit.Offset != 4 {
				t.Fatal(credit, err)
			}
			sum := sha256.Sum256([]byte("data"))
			terminal, err := e.Finish(source.ep.Key, protocol.TransferFinish{ID: d.ID, Bytes: 4, SHA256: hex.EncodeToString(sum[:])})
			if err != nil || terminal.Outcome != "completed" {
				t.Fatal(terminal, err)
			}
			eventual(t, func() bool { return e.budget.Usage().Active == 0 })
			if _, err = e.Status(source.ep.Key, d.ID); !errors.Is(err, ErrUnknown) {
				t.Fatal("terminal table retained", err)
			}
			source.mu.Lock()
			defer source.mu.Unlock()
			dest.mu.Lock()
			defer dest.mu.Unlock()
			if len(source.calls) != 2 || len(dest.calls) != 4 || len(source.held) != 0 || len(dest.held) != 0 || !dest.published {
				t.Fatal(source.calls, dest.calls, source.held, dest.held)
			}
		})
	}
}
func TestUnconfirmedCommitKeepsReceiverFactAndRemoteUnknown(t *testing.T) {
	e, source, dest := engineTest(t)
	dest.loseFinish = true
	d, _ := e.Create(source.ep, dest.ep, true, "payload", "")
	e.Announce(d.ID)
	_, err := e.Accept(dest.ep.Key, protocol.TransferAccept{ID: d.ID})
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.Chunk(source.ep.Key, protocol.TransferChunk{ID: d.ID, Data: "ZGF0YQ=="})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("data"))
	terminal, err := e.Finish(source.ep.Key, protocol.TransferFinish{ID: d.ID, Bytes: 4, SHA256: hex.EncodeToString(sum[:])})
	if err != nil || terminal.Outcome != "unknown" {
		t.Fatal(terminal, err)
	}
	eventual(t, func() bool { return e.budget.Usage().Active == 0 })
	dest.mu.Lock()
	defer dest.mu.Unlock()
	if !dest.published {
		t.Fatal("publication retracted")
	}
}

func TestSourceFailureIsRoleAndGenerationBoundBeforePublication(t *testing.T) {
	e, source, dest := engineTest(t)
	d, err := e.Create(source.ep, dest.ep, true, "payload", "")
	if err != nil {
		t.Fatal(err)
	}
	e.Announce(d.ID)
	if _, err = e.Accept(dest.ep.Key, protocol.TransferAccept{ID: d.ID}); err != nil {
		t.Fatal(err)
	}
	p := protocol.TransferFailure{ID: d.ID, Reason: "source_changed"}
	if _, err = e.Fail(dest.ep.Key, p); !errors.Is(err, ErrRole) {
		t.Fatal("receiver gained source failure authority", err)
	}
	if _, err = e.Fail(source.ep.Key+"-replacement", p); !errors.Is(err, ErrUnknown) {
		t.Fatal("replacement adopted previous transfer", err)
	}
	if _, err = e.Fail(source.ep.Key, protocol.TransferFailure{ID: d.ID, Reason: "stream_lost"}); err == nil {
		t.Fatal("generic transport reason admitted")
	}
	result, err := e.Fail(source.ep.Key, p)
	if err != nil || result.Outcome != "failed" || result.Reason != "source_changed" {
		t.Fatal(result, err)
	}
	eventual(t, func() bool { return e.budget.Usage().Active == 0 })
	dest.mu.Lock()
	defer dest.mu.Unlock()
	if dest.published || len(dest.data) != 0 {
		t.Fatal("source failure wrote or published")
	}
}

func TestSourceFailureRacingFinishCannotDowngradePublication(t *testing.T) {
	e, source, dest := engineTest(t)
	// Keep uncertain cleanup charged so the concurrent caller can still observe
	// the in-flight record. This does not introduce a terminal history table.
	source.refuseCleanup = true
	dest.refuseCleanup = true
	entered, release := make(chan struct{}), make(chan struct{})
	call := dest.ep.Call
	dest.ep.Call = func(ctx context.Context, method string, params, result any) error {
		if method == "transfer.finish" {
			close(entered)
			<-release
		}
		return call(ctx, method, params, result)
	}
	d, err := e.Create(source.ep, dest.ep, true, "payload", "")
	if err != nil {
		t.Fatal(err)
	}
	e.Announce(d.ID)
	if _, err = e.Accept(dest.ep.Key, protocol.TransferAccept{ID: d.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err = e.Chunk(source.ep.Key, protocol.TransferChunk{ID: d.ID, Data: "ZGF0YQ=="}); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("data"))
	finished := make(chan protocol.TransferTerminal, 1)
	go func() {
		v, err := e.Finish(source.ep.Key, protocol.TransferFinish{ID: d.ID, Bytes: 4, SHA256: hex.EncodeToString(sum[:])})
		if err != nil {
			t.Error(err)
		}
		finished <- v
	}()
	<-entered
	failed := make(chan protocol.TransferTerminal, 1)
	go func() {
		v, err := e.Fail(source.ep.Key, protocol.TransferFailure{ID: d.ID, Reason: "source_read_failed"})
		if err != nil {
			t.Error(err)
		}
		failed <- v
	}()
	close(release)
	if result := <-finished; result.Outcome != "completed" {
		t.Fatal(result)
	}
	if result := <-failed; result.Outcome != "completed" {
		t.Fatal("late source failure downgraded commit", result)
	}
	if _, err = e.Status(source.ep.Key, d.ID); !errors.Is(err, ErrUnknown) {
		t.Fatal("retiring record masquerades as streaming", err)
	}
	dest.mu.Lock()
	published := dest.published
	dest.mu.Unlock()
	if !published {
		t.Fatal("publication retracted")
	}
	close(source.done)
	close(dest.done)
	eventual(t, func() bool { return e.budget.Usage().Active == 0 })
}

func TestCancellationRetainsReservationUntilAllQueuedPayloadLeasesRetire(t *testing.T) {
	e, source, dest := engineTest(t)
	d, err := e.Create(source.ep, dest.ep, true, "payload", "")
	if err != nil {
		t.Fatal(err)
	}
	e.Announce(d.ID)
	if _, err = e.Accept(dest.ep.Key, protocol.TransferAccept{ID: d.ID}); err != nil {
		t.Fatal(err)
	}
	r, err := e.get(source.ep.Key, d.ID)
	if err != nil {
		t.Fatal(err)
	}
	r.io.Lock()
	done := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func() {
			_, err := e.Chunk(source.ep.Key, protocol.TransferChunk{ID: d.ID, Data: "ZGF0YQ=="})
			done <- err
		}()
	}
	eventual(t, func() bool { return r.stage.Usage().Ingress == 4 })
	e.EndEndpoint(source.ep.Key)
	if usage := e.budget.Usage(); usage.Active != 1 {
		r.io.Unlock()
		t.Fatal("queued backing buffers released at logical cancellation", usage)
	}
	if _, err = e.Chunk(source.ep.Key, protocol.TransferChunk{ID: d.ID, Data: "ZGF0YQ=="}); !errors.Is(err, ErrState) {
		r.io.Unlock()
		t.Fatal("ended generation admitted more payload", err)
	}
	r.io.Unlock()
	for i := 0; i < 4; i++ {
		if err := <-done; err == nil {
			t.Fatal("cancelled queued payload was written")
		}
	}
	eventual(t, func() bool { return e.budget.Usage().Active == 0 })
	if usage := r.stage.Usage(); usage.Bytes != 0 || usage.Ingress != 0 {
		t.Fatal("payload lease retained", usage)
	}
	dest.mu.Lock()
	defer dest.mu.Unlock()
	if len(dest.data) != 0 || dest.published {
		t.Fatal("cancelled queue reached native filesystem")
	}
}
func TestActivationRollbackAndUnknownCleanupRetainsCharge(t *testing.T) {
	e, source, dest := engineTest(t)
	dest.refuseHold = true
	dest.refuseCleanup = true
	d, _ := e.Create(source.ep, dest.ep, true, "payload", "")
	e.Announce(d.ID)
	if _, err := e.Accept(dest.ep.Key, protocol.TransferAccept{ID: d.ID}); err == nil {
		t.Fatal("accepted closing participant")
	}
	if e.budget.Usage().Active != 1 {
		t.Fatal("unknown resources released")
	}
	close(dest.done)
	eventual(t, func() bool { return e.budget.Usage().Active == 0 })
	source.mu.Lock()
	defer source.mu.Unlock()
	if len(source.held) != 0 {
		t.Fatal("partial hold leaked")
	}
}
func TestGapOrWrongRoleNeverWritesDestination(t *testing.T) {
	e, source, dest := engineTest(t)
	d, _ := e.Create(source.ep, dest.ep, true, "payload", "")
	e.Announce(d.ID)
	_, _ = e.Accept(dest.ep.Key, protocol.TransferAccept{ID: d.ID})
	if _, err := e.Chunk(dest.ep.Key, protocol.TransferChunk{ID: d.ID, Data: "YQ=="}); !errors.Is(err, ErrRole) {
		t.Fatal(err)
	}
	if _, err := e.Chunk(source.ep.Key, protocol.TransferChunk{ID: d.ID, Offset: 1, Data: "YQ=="}); err == nil {
		t.Fatal("gap accepted")
	}
	eventual(t, func() bool { return e.budget.Usage().Active == 0 })
	dest.mu.Lock()
	defer dest.mu.Unlock()
	if len(dest.data) != 0 {
		t.Fatal("bad bytes written")
	}
}
func TestPendingExpiryAndGenerationEndNeverActivate(t *testing.T) {
	e, source, dest := engineTest(t)
	e.pendingTTL = 5 * time.Millisecond
	d, _ := e.Create(source.ep, dest.ep, true, "payload", "")
	e.Announce(d.ID)
	eventual(t, func() bool { return e.budget.Usage().Pending == 0 })
	if _, err := e.Accept(dest.ep.Key, protocol.TransferAccept{ID: d.ID}); !errors.Is(err, ErrUnknown) {
		t.Fatal(err)
	}
	e.pendingTTL = time.Minute
	d, _ = e.Create(source.ep, dest.ep, true, "payload", "")
	e.Announce(d.ID)
	close(source.done)
	eventual(t, func() bool { return e.budget.Usage().Pending == 0 })
	if _, err := e.Accept(dest.ep.Key, protocol.TransferAccept{ID: d.ID}); !errors.Is(err, ErrUnknown) {
		t.Fatal(err)
	}
}

func TestCreationTerminalRaceWaitsForObservedReplyBoundary(t *testing.T) {
	e, source, dest := engineTest(t)
	d, err := e.Create(source.ep, dest.ep, true, "payload", "")
	if err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	record := e.live[d.ID]
	e.mu.Unlock()
	close(dest.done)
	<-record.ctx.Done()
	source.mu.Lock()
	n := len(source.notices)
	source.mu.Unlock()
	if n != 0 {
		t.Fatal("terminal overtook creator result binding")
	}
	if e.budget.Usage().Pending != 1 {
		t.Fatal("uncertain creation binding was released")
	}
	e.Announce(d.ID)
	eventual(t, func() bool { return e.budget.Usage().Pending == 0 })
	source.mu.Lock()
	defer source.mu.Unlock()
	if len(source.notices) != 1 || source.notices[0].Kind != "terminal" || len(source.calls) != 0 {
		t.Fatal(source.notices, source.calls)
	}
}

func TestUncertainReceiverCleanupDoesNotHideOtherTerminal(t *testing.T) {
	e, source, dest := engineTest(t)
	dest.loseFinish = true
	dest.refuseCleanup = true
	d, err := e.Create(source.ep, dest.ep, true, "payload", "")
	if err != nil {
		t.Fatal(err)
	}
	e.Announce(d.ID)
	if _, err = e.Accept(dest.ep.Key, protocol.TransferAccept{ID: d.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err = e.Chunk(source.ep.Key, protocol.TransferChunk{ID: d.ID, Data: "ZGF0YQ=="}); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("data"))
	terminal, err := e.Finish(source.ep.Key, protocol.TransferFinish{ID: d.ID, Bytes: 4, SHA256: hex.EncodeToString(sum[:])})
	if err != nil || terminal.Outcome != "unknown" {
		t.Fatal(terminal, err)
	}
	eventual(t, func() bool {
		source.mu.Lock()
		defer source.mu.Unlock()
		for _, n := range source.notices {
			if n.Kind == "terminal" && n.Outcome == "unknown" {
				return len(source.held) == 0
			}
		}
		return false
	})
	if e.budget.Usage().Active != 1 {
		t.Fatal("receiver uncertainty released its charge")
	}
	dest.mu.Lock()
	held := len(dest.held)
	published := dest.published
	dest.mu.Unlock()
	if held != 1 || !published {
		t.Fatal("receiver hold/fact lost")
	}
	close(dest.done)
	eventual(t, func() bool { return e.budget.Usage().Active == 0 })
}
