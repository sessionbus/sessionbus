// SPDX-License-Identifier: GPL-3.0-only
package transfer

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestDEV1BotExplicitCloseDoesNotWaitForOwnerHoldReply(t *testing.T) {
	e, source, dest := engineTest(t)
	e.prepareTTL = time.Second
	entered, release := make(chan struct{}), make(chan struct{})
	source.ep.Hold = func(ctx context.Context, _ string, terminal bool) bool {
		if terminal {
			return true
		}
		close(entered)
		select {
		case <-release:
			return true
		case <-ctx.Done():
			return false
		}
	}
	d, err := e.Create(source.ep, dest.ep, true, "file", "")
	if err != nil {
		t.Fatal(err)
	}
	e.Announce(d.ID)
	accepted := make(chan struct{})
	go func() { defer close(accepted); _, _ = e.Accept(dest.ep.Key, protocol.TransferAccept{ID: d.ID}) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("prepare did not reach hold")
	}
	closed := make(chan struct{})
	go func() { defer close(closed); e.EndEndpoint(source.ep.Key) }()
	stalled := false
	select {
	case <-closed:
	case <-time.After(25 * time.Millisecond):
		stalled = true
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close did not retire after control release")
	}
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("accept did not retire")
	}
	if stalled {
		t.Error("explicit close waited for a Hold reply from that same owner loop")
	}
}

func TestActivationJoinsLateOrUnconfirmedHoldBeforeRetiring(t *testing.T) {
	for _, acknowledged := range []bool{true, false} {
		t.Run(fmt.Sprint(acknowledged), func(t *testing.T) {
			e, source, dest := engineTest(t)
			entered, release := make(chan struct{}), make(chan struct{})
			var releases atomic.Int32
			source.ep.Hold = func(_ context.Context, id string, terminal bool) bool {
				if terminal {
					source.mu.Lock()
					delete(source.held, id)
					source.mu.Unlock()
					releases.Add(1)
					return true
				}
				close(entered)
				<-release
				source.mu.Lock()
				source.held[id] = true
				source.mu.Unlock()
				return acknowledged
			}
			d, err := e.Create(source.ep, dest.ep, true, "file", "")
			if err != nil {
				t.Fatal(err)
			}
			e.Announce(d.ID)
			accepted := make(chan struct{})
			go func() { defer close(accepted); _, _ = e.Accept(dest.ep.Key, protocol.TransferAccept{ID: d.ID}) }()
			<-entered
			closed := make(chan struct{})
			go func() { e.EndEndpoint(source.ep.Key); close(closed) }()
			select {
			case <-closed:
			case <-time.After(time.Second):
				close(release)
				t.Fatal("close waited for owner callback")
			}
			if e.budget.Usage().Active != 1 {
				t.Error("released resource charge before callback join")
			}
			close(release)
			<-accepted
			eventual(t, func() bool { return e.budget.Usage().Active == 0 })
			source.mu.Lock()
			held := len(source.held)
			source.mu.Unlock()
			dest.mu.Lock()
			calls := len(dest.calls)
			dest.mu.Unlock()
			if held != 0 || releases.Load() != 1 || calls != 1 {
				t.Fatal("late/unconfirmed hold cleanup or successor start", held, releases.Load(), calls)
			}
		})
	}
}
