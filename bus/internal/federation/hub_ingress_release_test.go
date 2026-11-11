// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"bufio"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestRunDroppedIngressReturnsRealBulkCredits(t *testing.T) {
	for _, mode := range []string{"already-failed", "unadopted-invalid", "origin-cap"} {
		t.Run(mode, func(t *testing.T) {
			link, send, _ := bulkCreditFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			link.ctx, link.log, link.done = ctx, io.Discard, make(chan struct{})
			// Closure can take the existing context-cancelled registry path.
			link.registry = make(chan registryEvent, 1)
			if mode != "already-failed" {
				link.fd = policyTransferFD(t, "alpha")
				link.transfers.Store(newHubTransfers(ctx))
			}
			wantReserved := 0
			if mode == "origin-cap" {
				link.originAdmitted = protocol.MaxOperations
				wantReserved = protocol.MaxOperations
			}
			var frames []conn.Frame
			var originals []func()
			for range 4 {
				frame := send()
				if frame.Err != nil || frame.Release == nil {
					t.Fatal("missing real reader credit", frame.Err)
				}
				originals = append(originals, frame.Release)
				if mode == "unadopted-invalid" {
					frame.Value.Params = []byte(`{}`)
				}
				frames = append(frames, frame)
			}
			entered, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			// The observer pauses AFTER the original callback has returned its
			// real credit. It never implements or substitutes credit accounting.
			frames[3].Release = func() {
				originals[3]()
				once.Do(func() { close(entered); <-resume })
			}
			var release sync.Once
			t.Cleanup(func() {
				release.Do(func() { close(resume) })
				cancel()
				link.wire.Close()
				select {
				case <-link.done:
				case <-time.After(time.Second):
					t.Error("run did not join")
				}
			})
			for _, frame := range frames {
				link.inbox <- frame
			}
			go link.run(mode != "already-failed")
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("run did not release the dropped real ingress callbacks")
			}
			if link.originAdmitted != wantReserved || len(link.originReplies) != 0 {
				t.Fatal("dropped frame acquired origin reservation")
			}
			var replies *bufio.Reader
			if mode == "origin-cap" {
				replies = bufio.NewReader(link.raw)
				if err := link.raw.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				for id := int64(1); id < 4; id++ {
					assertIngressBusy(t, replies, id)
				}
			}
			var replacements []conn.Frame
			for range 4 {
				frame := send()
				if frame.Err != nil || frame.Release == nil {
					t.Fatal("dropped ingress failed to return all four credits", frame.Err)
				}
				replacements = append(replacements, frame)
			}
			// Prove all four credits were returned by run BEFORE invoking any
			// callback from the test. Only now try duplicate old releases.
			for _, original := range originals {
				original() // Real sync.Once must prevent extra credits.
			}
			blocked := send()
			if blocked.Err != conn.ErrBulkCapacity || len(blocked.Value.Params) != 0 || blocked.Release != nil {
				t.Fatal("duplicate callback created a fifth credit", blocked)
			}
			for _, frame := range replacements {
				frame.Release()
			}
			release.Do(func() { close(resume) })
			if mode == "origin-cap" {
				assertIngressBusy(t, replies, 4)
			}
		})
	}
}

func assertIngressBusy(t *testing.T, reader *bufio.Reader, id int64) {
	t.Helper()
	body, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	frame, err := protocol.DecodeFrame(body)
	if err != nil || frame.ID != id || frame.Error == nil || frame.Error.Code != protocol.Busy || len(frame.Error.Data) != 0 {
		t.Fatal("nonadmitted real chunk did not get strict Busy", string(body), err)
	}
}

func TestRunFatalIngressReleasesRealCallback(t *testing.T) {
	link, send, _ := bulkCreditFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	link.ctx, link.log, link.done = ctx, io.Discard, make(chan struct{})
	registry := make(chan registryEvent, 1)
	link.registry = registry
	frame := send()
	if frame.Release == nil {
		t.Fatal("missing real reader callback")
	}
	// No transfer ALPN: this real credited frame is fatal before adoption.
	released := make(chan struct{})
	original := frame.Release
	frame.Release = func() { original(); close(released) }
	link.inbox <- frame
	go link.run(true)
	t.Cleanup(func() { cancel(); link.wire.Close(); <-link.done })
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("fatal run path dropped a real callback")
	}
	select {
	case event := <-registry:
		if event.kind != registryClosed || event.link != link {
			t.Fatal("wrong closed generation")
		}
		event.ack <- true
	case <-time.After(time.Second):
		t.Fatal("fatal reader did not reach Closed")
	}
	select {
	case <-link.done:
	case <-time.After(time.Second):
		t.Fatal("fatal owner did not join")
	}
	original() // Existing real callback remains idempotent after owner end.
	if link.originAdmitted != 0 {
		t.Fatal("fatal unadopted frame reserved capacity")
	}
}
