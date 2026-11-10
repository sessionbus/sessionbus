// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

// This fixture uses only pre-F2 constructor/frame/reply APIs, so the identical
// regression also runs against the old production source.
func originReleaseFixture(t *testing.T) (*hostLink, net.Conn, chan registryEvent, <-chan protocol.Frame, func()) {
	t.Helper()
	left, right := net.Pipe()
	registry := make(chan registryEvent, conn.OutboxSize+8)
	link := newHostLink(context.Background(), "alpha", left, left, registry, io.Discard)
	frames := make(chan protocol.Frame, conn.OutboxSize+8)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		reader := bufio.NewReader(right)
		for {
			raw, err := reader.ReadBytes('\n')
			if err != nil {
				return
			}
			frame, err := protocol.DecodeFrame(raw)
			if err != nil {
				t.Error("strict origin response", err, string(raw))
				return
			}
			frames <- frame
		}
	}()
	started := false
	start := func() {
		if started {
			t.Fatal("owner started twice")
		}
		started = true
		go link.run(true)
	}
	t.Cleanup(func() {
		link.wire.Close()
		_ = right.Close()
		if started {
			select {
			case <-link.done:
			case <-time.After(time.Second):
				t.Error("origin owner did not join")
			}
		} else {
			deadline := time.After(time.Second)
		drain:
			for {
				select {
				case event := <-link.inbox:
					if _, ok := event.(conn.Closed); ok {
						break drain
					}
				case <-deadline:
					t.Error("wire close was not retained")
					return
				}
			}
			link.group.Wait()
		}
		select {
		case <-readDone:
		case <-time.After(time.Second):
			t.Error("strict response reader did not join")
		}
	})
	return link, right, registry, frames, start
}

func TestOriginMarkReleaseAnswersDoNotOverflowReaderInbox(t *testing.T) {
	link, peer, registry, frames, start := originReleaseFixture(t)
	raw, err := json.Marshal(Forward{From: policyTestCaller(), Request: policyTestRequest(t, "turn.wait", protocol.WaitRequest{SessionID: "lane@beta"})})
	if err != nil {
		t.Fatal(err)
	}
	last := int64(0)
	for id := int64(1); id <= protocol.MaxOperations; id++ {
		if err := link.frame(conn.Frame{Value: protocol.Frame{ID: id, Request: true, Method: forwardMethod, Params: raw}}, map[int64]pendingCall{}, &last); err != nil {
			t.Fatal(err)
		}
		if event := <-registry; event.kind != registryForward {
			t.Fatal("forward admission changed", event.kind)
		}
	}
	// Model the independent reader producing a full inbox before the registry's
	// first-release burst. No link owner consumes replies until after the burst.
	for range conn.OutboxSize {
		link.inbox <- struct{}{}
	}
	state := &hubState{links: map[string]*hostLink{"alpha": link}, log: io.Discard}
	for id := int64(1); id <= protocol.MaxOperations; id++ {
		state.reply(originReply{origin: link, id: id, method: "turn.wait", value: errorReply(protocol.ForwardLost, nil)})
	}
	if state.links["alpha"] != link {
		t.Fatal("first-release response burst removed the live origin")
	}
	select {
	case <-link.wire.Done():
		t.Fatal("first-release burst closed origin")
	default:
	}
	// The registry is again owned by one goroutine while the real link owner
	// consumes the dedicated replies and sends strict frames on the same wire.
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for event := range registry {
			state.handle(context.Background(), registry, event)
			if event.kind == registryClosed {
				return
			}
		}
	}()
	t.Cleanup(func() {
		link.wire.Close()
		_ = peer.Close()
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("registry fixture did not join")
		}
	})
	start()
	seen := map[int64]bool{}
	for range protocol.MaxOperations {
		select {
		case frame := <-frames:
			if frame.ID < 1 || frame.ID > protocol.MaxOperations || seen[frame.ID] || frame.Error == nil || frame.Error.Code != protocol.ForwardLost {
				t.Fatal("wrong first-release answer", frame)
			}
			seen[frame.ID] = true
		case <-time.After(3 * time.Second):
			t.Fatal("not all first-release answers delivered", len(seen))
		}
	}
	hosts, _ := requestBytes(257, hostsMethod, struct{}{})
	if _, err := peer.Write(hosts); err != nil {
		t.Fatal("same origin disconnected", err)
	}
	select {
	case frame := <-frames:
		if frame.ID != 257 || frame.Error != nil {
			t.Fatal("same origin did not continue", frame)
		}
		if _, err := DecodeHosts(Reply{Result: frame.Result}); err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("same origin hosts response missing")
	}
}
