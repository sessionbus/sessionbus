// SPDX-License-Identifier: GPL-3.0-only
package transfer

import (
	"context"
	"errors"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestAcceptedSourceTerminalCannotRetroactivelyRejectAcceptance(t *testing.T) {
	for _, offer := range []bool{true, false} {
		for _, reason := range []string{"source_changed", "source_read_failed"} {
			t.Run(map[bool]string{true: "offer", false: "request"}[offer]+"/"+reason, func(t *testing.T) {
				e, source, dest := engineTest(t)
				original := source.ep.Call
				starts := 0
				source.ep.Call = func(ctx context.Context, method string, params, result any) error {
					if method == "transfer.start" {
						starts++
						p := params.(protocol.TransferStart)
						terminal, err := e.Fail(source.ep.Key, protocol.TransferFailure{ID: p.ID, Reason: reason})
						if err != nil || terminal.Outcome != "failed" || terminal.Reason != reason {
							t.Fatal(terminal, err)
						}
						<-ctx.Done()
						return ctx.Err()
					}
					return original(ctx, method, params, result)
				}
				creator, accepter := source, dest
				if !offer {
					creator, accepter = dest, source
				}
				d, err := e.Create(creator.ep, accepter.ep, offer, "payload", "the requested file")
				if err != nil {
					t.Fatal(err)
				}
				e.Announce(d.ID)
				accepted, err := e.Accept(accepter.ep.Key, protocol.TransferAccept{ID: d.ID})
				if err != nil || accepted.ID != d.ID || accepted.State != "streaming" || starts != 1 {
					t.Fatal("activated acceptance was rejected or restarted", accepted, err, starts)
				}
				assertTerminalResources(t, e, source, dest, d.ID, "failed", reason)
			})
		}
	}
}

func TestUnconfirmedSourceStartStillRejectsAcceptWithoutReplay(t *testing.T) {
	e, source, dest := engineTest(t)
	original := source.ep.Call
	starts := 0
	source.ep.Call = func(ctx context.Context, method string, params, result any) error {
		if method == "transfer.start" {
			starts++
			return errors.New("source start response lost")
		}
		return original(ctx, method, params, result)
	}
	d, err := e.Create(source.ep, dest.ep, true, "payload", "")
	if err != nil {
		t.Fatal(err)
	}
	e.Announce(d.ID)
	if _, err = e.Accept(dest.ep.Key, protocol.TransferAccept{ID: d.ID}); !errors.Is(err, ErrState) || starts != 1 {
		t.Fatal("unconfirmed start became successful or replayed", err, starts)
	}
	assertTerminalResources(t, e, source, dest, d.ID, "unknown", "source_start_unconfirmed")
}

func TestPreActivationFailureStillRejectsAccept(t *testing.T) {
	e, source, dest := engineTest(t)
	dest.refuseHold = true
	d, err := e.Create(source.ep, dest.ep, true, "payload", "")
	if err != nil {
		t.Fatal(err)
	}
	e.Announce(d.ID)
	if _, err = e.Accept(dest.ep.Key, protocol.TransferAccept{ID: d.ID}); !errors.Is(err, ErrState) {
		t.Fatal("pre-activation failure became successful", err)
	}
	assertTerminalResources(t, e, source, dest, d.ID, "failed", "participant_closing")
	for _, endpoint := range []*testEndpoint{source, dest} {
		endpoint.mu.Lock()
		for _, method := range endpoint.calls {
			if method == "transfer.start" {
				t.Error("failed activation started a participant")
			}
		}
		endpoint.mu.Unlock()
	}
}

func assertTerminalResources(t *testing.T, e *Engine, source, dest *testEndpoint, id, outcome, reason string) {
	t.Helper()
	eventual(t, func() bool { return e.budget.Usage().Active == 0 })
	for _, endpoint := range []*testEndpoint{source, dest} {
		endpoint.mu.Lock()
		matches := 0
		for _, notice := range endpoint.notices {
			if notice.Kind == "terminal" && notice.ID == id {
				matches++
				if notice.Outcome != outcome || notice.Reason != reason {
					t.Error("terminal was relabelled", notice)
				}
			}
		}
		if matches != 1 || len(endpoint.held) != 0 || endpoint.published {
			t.Error("terminal/resource retirement mismatch", matches, endpoint.held, endpoint.published)
		}
		endpoint.mu.Unlock()
	}
}
