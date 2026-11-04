// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/transfer"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func bridgeTerminalFixture(t *testing.T, phase string) (*bridgeBinding, <-chan protocol.TransferNotice) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	budget := transfer.HostBudget()
	r, err := budget.PendingFor("generation")
	if err != nil {
		t.Fatal(err)
	}
	if phase != "pending" {
		if err = r.Prepare(); err != nil {
			t.Fatal(err)
		}
	}
	notices := make(chan protocol.TransferNotice, 4)
	b := &transferBridge{daemon: &Daemon{traceContext: ctx}, ctx: ctx, tokens: map[string]*bridgeBinding{}, ids: map[string]*bridgeBinding{}}
	x := &bridgeBinding{bridge: b, token: "token", id: "id", role: "destination", phase: phase, reservation: r, ctx: ctx, cancel: func() {}, retired: make(chan struct{}), timer: time.NewTimer(time.Hour)}
	t.Cleanup(func() { x.timer.Stop() })
	x.endpoint = transfer.Endpoint{Key: "generation", Done: make(chan struct{}), Notice: func(_ context.Context, n protocol.TransferNotice) error { notices <- n; return nil }}
	b.tokens[x.token], b.ids[x.id] = x, x
	t.Cleanup(func() {
		select {
		case <-x.retired:
		case <-time.After(time.Second):
			t.Error("binding retirement missing")
		}
		if usage := budget.Usage(); usage.Pending != 0 || usage.Active != 0 {
			t.Error("binding accounting survived positive cleanup", usage)
		}
	})
	return x, notices
}

func TestRemoteExplicitCloseUsesCurrentCommitKnowledge(t *testing.T) {
	for _, phase := range []string{"pending", "preparing", "streaming", "committing", "confirmed"} {
		t.Run(phase, func(t *testing.T) {
			actualPhase := phase
			if phase == "confirmed" {
				actualPhase = "committing"
			}
			x, notices := bridgeTerminalFixture(t, actualPhase)
			if phase == "confirmed" {
				digest := strings.Repeat("a", 64)
				x.observeCommit(protocol.TransferFinish{ID: x.id, Bytes: 4, SHA256: digest}, protocol.TransferTerminal{ID: x.id, Bytes: 4, SHA256: digest, Outcome: "completed"})
			}
			x.bridge.endEndpoint(x.endpoint.Key)
			select {
			case n := <-notices:
				want := "cancelled"
				if phase == "committing" {
					want = "unknown"
				}
				if phase == "confirmed" {
					want = "completed"
				}
				if n.Outcome != want {
					t.Fatal("close lost commit knowledge", n, want)
				}
			case <-time.After(time.Second):
				t.Fatal("no close notice")
			}
		})
	}
}

func TestRemoteDeadlineClassifiesActualPhaseAndTransportLossRemainsSeparate(t *testing.T) {
	for _, item := range []struct{ phase, outcome, reason string }{
		{"pending", "expired", "expired"},
		{"preparing", "failed", "prepare_timeout"},
		{"streaming", "failed", "progress_timeout"},
		{"committing", "unknown", "commit_unconfirmed"},
	} {
		t.Run(item.phase, func(t *testing.T) {
			x, notices := bridgeTerminalFixture(t, item.phase)
			if err := x.terminal(protocol.TransferNotice{Kind: "terminal", Reason: "deadline_elapsed"}); err != nil {
				t.Fatal(err)
			}
			n := <-notices
			if n.Outcome != item.outcome || n.Reason != item.reason {
				t.Fatal(n, item)
			}
		})
	}
	t.Run("actual-link-loss", func(t *testing.T) {
		x, notices := bridgeTerminalFixture(t, "streaming")
		if err := x.terminal(protocol.TransferNotice{Kind: "terminal", Outcome: "unknown", Reason: "hub_generation_ended"}); err != nil {
			t.Fatal(err)
		}
		if n := <-notices; n.Outcome != "unknown" || n.Reason != "hub_generation_ended" {
			t.Fatal("link loss inferred nonpublication", n)
		}
	})
}

func TestRemoteCommitObservationRequiresExactFinishBinding(t *testing.T) {
	x, notices := bridgeTerminalFixture(t, "committing")
	finish := protocol.TransferFinish{ID: x.id, Bytes: 4, SHA256: strings.Repeat("a", 64)}
	for _, wrong := range []protocol.TransferTerminal{
		{ID: "other", Bytes: 4, SHA256: finish.SHA256, Outcome: "completed"},
		{ID: x.id, Bytes: 5, SHA256: finish.SHA256, Outcome: "completed"},
		{ID: x.id, Bytes: 4, SHA256: strings.Repeat("b", 64), Outcome: "completed"},
	} {
		x.observeCommit(finish, wrong)
	}
	if err := x.terminal(protocol.TransferNotice{Kind: "terminal", Reason: "endpoint_closing"}); err != nil {
		t.Fatal(err)
	}
	if n := <-notices; n.Outcome != "unknown" {
		t.Fatal("wrong finish promoted completion", n)
	}
}
