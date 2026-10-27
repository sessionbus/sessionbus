// SPDX-License-Identifier: GPL-3.0-only
package transfer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestConcurrentDuplicateAcceptCoalescesPreparationAndKeepsRoleSizeAndTerminalChecks(t *testing.T) {
	for _, offer := range []bool{true, false} {
		t.Run(map[bool]string{true: "offer", false: "request"}[offer], func(t *testing.T) {
			e, source, dest := engineTest(t)
			original := source.ep.Call
			entered, release := make(chan struct{}), make(chan struct{})
			source.ep.Call = func(ctx context.Context, method string, params, result any) error {
				if method == "transfer.prepare" {
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return original(ctx, method, params, result)
			}
			creator, accepter := source, dest
			if !offer {
				creator, accepter = dest, source
			}
			d, err := e.Create(creator.ep, accepter.ep, offer, "file", "file")
			if err != nil {
				t.Fatal(err)
			}
			e.Announce(d.ID)
			results := make(chan protocol.TransferDescriptor, 9)
			errorsSeen := make(chan error, 9)
			var wg sync.WaitGroup
			accept := func() {
				defer wg.Done()
				result, err := e.Accept(accepter.ep.Key, protocol.TransferAccept{ID: d.ID})
				results <- result
				errorsSeen <- err
			}
			wg.Add(1)
			go accept()
			<-entered
			for range 8 {
				wg.Add(1)
				go accept()
			}
			close(release)
			wg.Wait()
			for range 9 {
				if result, err := <-results, <-errorsSeen; err != nil || result.ID != d.ID || result.State != "streaming" {
					t.Fatal("duplicate acceptance did not coalesce", result, err)
				}
			}
			for _, ep := range []*testEndpoint{source, dest} {
				ep.mu.Lock()
				if len(ep.calls) != 2 || ep.calls[0] != "transfer.prepare" || ep.calls[1] != "transfer.start" || len(ep.held) != 1 {
					t.Fatal("duplicate repeated prepare/start/hold", ep.calls, ep.held)
				}
				ep.mu.Unlock()
			}
			if usage := e.budget.Usage(); usage.Active != 1 || usage.Pending != 0 {
				t.Fatal("duplicate recharged the transfer", usage)
			}
			if _, err = e.Accept(creator.ep.Key, protocol.TransferAccept{ID: d.ID}); !errors.Is(err, ErrRole) {
				t.Fatal("creator accepted own offer/request", err)
			}
			badSize := protocol.Integer(5)
			if _, err = e.Accept(accepter.ep.Key, protocol.TransferAccept{ID: d.ID, Size: &badSize}); !errors.Is(err, ErrState) {
				t.Fatal("contradictory size accepted", err)
			}
			if _, err = e.Cancel(accepter.ep.Key, d.ID); err != nil {
				t.Fatal(err)
			}
			if _, err = e.Accept(accepter.ep.Key, protocol.TransferAccept{ID: d.ID}); !errors.Is(err, ErrUnknown) {
				t.Fatal("terminal accept was replayed", err)
			}
			eventual(t, func() bool { return e.budget.Usage().Active == 0 })
		})
	}
}

func TestPendingExpiryUsesExpiredForBothWorkflowsWithoutPreparingOrHolding(t *testing.T) {
	for _, offer := range []bool{true, false} {
		t.Run(map[bool]string{true: "offer", false: "request"}[offer], func(t *testing.T) {
			e, source, dest := engineTest(t)
			e.pendingTTL = 20 * time.Millisecond
			d, err := e.Create(source.ep, dest.ep, offer, "file", "file")
			if err != nil {
				t.Fatal(err)
			}
			e.Announce(d.ID)
			eventual(t, func() bool { return e.budget.Usage().Pending == 0 })
			for _, ep := range []*testEndpoint{source, dest} {
				ep.mu.Lock()
				terminals := 0
				for _, notice := range ep.notices {
					if notice.Kind == "terminal" {
						terminals++
						if notice.Outcome != "expired" || notice.Reason != "expired" {
							t.Fatal("pending expiry mislabeled", notice)
						}
					}
				}
				if terminals != 1 || len(ep.calls) != 0 || len(ep.held) != 0 {
					t.Fatal("expiry opened/held or lost terminal", ep.notices, ep.calls, ep.held)
				}
				ep.mu.Unlock()
			}
			if _, err = e.Status(source.ep.Key, d.ID); !errors.Is(err, ErrUnknown) {
				t.Fatal("expired ID persisted", err)
			}
		})
	}
}

func TestResetDeadlineUsesCurrentPhaseAndCannotClaimNonpublicationInCommit(t *testing.T) {
	for _, item := range []struct{ phase, outcome, reason string }{
		{"preparing", "failed", "prepare_timeout"},
		{"streaming", "failed", "progress_timeout"},
		{"committing", "unknown", "commit_unconfirmed"},
	} {
		t.Run(item.phase, func(t *testing.T) {
			e, source, dest := engineTest(t)
			d, err := e.Create(source.ep, dest.ep, true, "file", "")
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
			// Controlled phase fixture drives the exact resettable timer callback;
			// it does not claim a native commit or a physical-clock measurement.
			r.mu.Lock()
			r.descriptor.State = item.phase
			r.mu.Unlock()
			r.terminate("expired", "expired", false)
			r.mu.Lock()
			result, ended := r.terminal, r.ended
			r.mu.Unlock()
			if !ended || result.Outcome != item.outcome || result.Reason != item.reason {
				t.Fatal("reset timer lost phase/commit uncertainty", result, ended)
			}
			eventual(t, func() bool { return e.budget.Usage().Active == 0 })
		})
	}
}
