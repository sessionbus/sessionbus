// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/transfer"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestEndedGenerationCreateUsesUnavailableBeforeAllocation(t *testing.T) {
	for _, reserved := range []bool{false, true} {
		for _, ended := range []string{"creator", "other"} {
			name := ended + "-Create"
			if reserved {
				name += "Reserved"
			}
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				budget := transfer.HubBudget()
				engine := transfer.NewEngine(ctx, budget)
				var calls atomic.Int32
				endpoint := func(key string, done <-chan struct{}) transfer.Endpoint {
					return transfer.Endpoint{Key: key, Done: done,
						Identity: protocol.DeliverySource{SessionID: key + "@host", Product: "fixture", Groups: []string{"team"}},
						Call:     func(context.Context, string, any, any) error { calls.Add(1); return nil },
						Hold:     func(context.Context, string, bool) bool { calls.Add(1); return true },
						Notice:   func(context.Context, protocol.TransferNotice) error { calls.Add(1); return nil }}
				}
				create := func(first, second transfer.Endpoint) (protocol.TransferDescriptor, error) {
					if !reserved {
						return engine.Create(first, second, true, "file", "")
					}
					reservation, err := budget.PendingFor(first.Key)
					if err != nil {
						t.Fatal(err)
					}
					descriptor, err := engine.CreateReserved(first, second, true, "file", "", reservation)
					if err != nil {
						reservation.Release() // Existing caller's unadopted responsibility.
					}
					return descriptor, err
				}
				firstDone, secondDone := make(chan struct{}), make(chan struct{})
				first, second := endpoint("creator", firstDone), endpoint("other", secondDone)
				if ended == "creator" {
					close(firstDone)
				} else {
					close(secondDone)
				}
				descriptor, err := create(first, second)
				if descriptor.ID != "" || calls.Load() != 0 || budget.Usage().Pending != 0 || budget.Usage().Active != 0 {
					t.Fatal("pre-ID effect or unadopted reservation leak", descriptor, calls.Load(), budget.Usage())
				}
				if !errors.Is(err, context.Canceled) {
					t.Errorf("ended generation classified as capability refusal: %v", err)
				}
				wire, encodeErr := ResponseBytes(17, transferReply(nil, err))
				frame, decodeErr := protocol.DecodeFrame(wire)
				if encodeErr != nil || decodeErr != nil || frame.Error == nil || frame.Error.Code != protocol.Internal || string(frame.Error.Data) != `"transfer_unavailable"` {
					t.Errorf("actual mapped pre-ID error=%s encode=%v decode=%v", wire, encodeErr, decodeErr)
				}
				first.Call = nil
				_, err = create(first, second)
				code, data := TransferError(err)
				if !errors.Is(err, transfer.ErrUnsupported) || code != protocol.UnsupportedTransfer || data != nil || budget.Usage().Pending != 0 {
					t.Fatal("genuine missing capability changed", err, code, data, budget.Usage())
				}
				first, second = endpoint("fresh-creator", make(chan struct{})), endpoint("fresh-other", make(chan struct{}))
				descriptor, err = create(first, second)
				if err != nil || descriptor.ID == "" || descriptor.State != "offered" || budget.Usage().Pending != 1 {
					t.Fatal("healthy same engine continuity failed", descriptor, err, budget.Usage())
				}
				cancel()
				until := time.Now().Add(time.Second)
				for budget.Usage().Pending != 0 && time.Now().Before(until) {
					time.Sleep(time.Millisecond)
				}
				if usage := budget.Usage(); usage.Pending != 0 || usage.Active != 0 {
					t.Fatal("healthy creation cleanup leaked", usage)
				}
			})
		}
	}
}
