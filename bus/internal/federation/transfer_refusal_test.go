// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/transfer"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestTransferRefusalAndUncertaintyRemainDistinct(t *testing.T) {
	for _, test := range []struct {
		err  error
		code int
	}{
		{transfer.ErrCapacity, protocol.Busy}, {transfer.ErrUnsupported, protocol.UnsupportedTransfer},
		{transfer.ErrUnknown, protocol.TransferFailed}, {transfer.ErrRole, protocol.TransferFailed}, {transfer.ErrState, protocol.TransferFailed},
		{context.Canceled, protocol.Internal}, {context.DeadlineExceeded, protocol.Internal}, {errors.New("transport lost"), protocol.Internal},
		{&protocol.RPCError{Code: protocol.Busy}, protocol.Busy}, {&protocol.RPCError{Code: protocol.UnsupportedTransfer}, protocol.UnsupportedTransfer},
		{&protocol.RPCError{Code: protocol.TransferFailed}, protocol.TransferFailed}, {&protocol.RPCError{Code: protocol.ForwardLost}, protocol.Internal},
	} {
		t.Run(test.err.Error(), func(t *testing.T) {
			body, err := ResponseBytes(7, transferReply(nil, fmt.Errorf("cause: %w", test.err)))
			if err != nil {
				t.Fatal(err)
			}
			frame, err := protocol.DecodeFrame(body)
			if err != nil || frame.Error == nil || frame.Error.Code != test.code {
				t.Fatal(string(body), err)
			}
			if test.code == protocol.Internal {
				if string(frame.Error.Data) != `"transfer_unavailable"` {
					t.Fatal(frame.Error)
				}
			} else if len(frame.Error.Data) != 0 {
				t.Fatal("semantic refusal data", frame.Error)
			}
		})
	}
	if reply := transferReply(make(chan int), nil); reply.Error == nil || reply.Error.Code != protocol.Internal {
		t.Fatal("serialization failure became semantic refusal", reply)
	}
	dead := make(chan struct{})
	close(dead)
	link := &hostLink{fd: policyTransferFD(t, "alpha"), inbox: make(chan any), transferBulk: make(chan TransferCall), done: dead}
	for _, method := range []string{"transfer.status", "transfer.chunk"} {
		err := link.transferCall(context.Background(), method, protocol.TransferTarget{ID: "id"}, nil)
		if !errors.Is(err, context.Canceled) {
			t.Fatal("ended generation called unsupported", err)
		}
		code, _ := TransferError(err)
		if code != protocol.Internal {
			t.Fatal("ended generation invented refusal")
		}
	}
}

// This is the real hub action+legacy encoder boundary, independently of the
// local connection's optional status-only error translation.
func TestHubStatusKeepsLegacyRefusalForAbsentForeignAndRetainedEnded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := newHubTransfers(ctx)
	origin, other, foreign := &hostLink{}, &hostLink{}, &hostLink{}
	gate := make(chan struct{})
	entered := make(chan protocol.TransferNotice, 2)
	defer func() {
		close(gate)
		cancel()
		until := time.Now().Add(time.Second)
		for h.budget.Usage().Pending != 0 && time.Now().Before(until) {
			time.Sleep(time.Millisecond)
		}
		if usage := h.budget.Usage(); usage.Pending != 0 || usage.Active != 0 {
			t.Error("hub status fixture did not retire its held reservation", usage)
		}
	}()
	endpoint := func(key, id string) transfer.Endpoint {
		return transfer.Endpoint{Key: key, Identity: protocol.DeliverySource{SessionID: id, Product: "fixture", Groups: []string{"shared"}}, Done: ctx.Done(),
			Call: func(context.Context, string, any, any) error {
				t.Error("status fixture attempted endpoint I/O")
				return errors.New("unexpected endpoint I/O")
			},
			Hold: func(context.Context, string, bool) bool {
				t.Error("pending status fixture attempted an active hold")
				return false
			},
			Notice: func(ctx context.Context, n protocol.TransferNotice) error {
				if n.Kind == "terminal" {
					entered <- n
					select {
					case <-gate:
					case <-ctx.Done():
					}
				}
				return nil
			}}
	}
	source, destination := endpoint("source-generation", "source@alpha"), endpoint("destination-generation", "destination@beta")
	descriptor, err := h.engine.Create(source, destination, true, "file", "")
	if err != nil {
		t.Fatal(err)
	}
	h.engine.Announce(descriptor.ID)
	h.routes[descriptor.ID] = &hubTransferRoute{id: descriptor.ID, creator: &hubTransferBinding{link: origin, key: source.Key}, other: &hubTransferBinding{link: other, key: destination.Key}}
	value, err := h.action(origin, "transfer.status", &protocol.TransferTarget{ID: descriptor.ID})
	if err != nil || value.(protocol.TransferDescriptor).ID != descriptor.ID {
		t.Fatal("actual live hub status", value, err)
	}
	terminal, err := h.engine.Cancel(source.Key, descriptor.ID)
	if err != nil || terminal.Outcome != "cancelled" {
		t.Fatal("actual coordinator end", terminal, err)
	}
	for range 2 {
		select {
		case n := <-entered:
			if n.ID != descriptor.ID || n.Outcome != "cancelled" {
				t.Fatal(n)
			}
		case <-time.After(time.Second):
			t.Fatal("real terminal cleanup not held")
		}
	}
	if usage := h.budget.Usage(); usage.Pending != 1 {
		t.Fatal("ended record reservation was not retained under held cleanup", usage)
	}
	for _, test := range []struct {
		name, id string
		link     *hostLink
	}{{"absent", "absent", origin}, {"retained-ended", descriptor.ID, origin}, {"foreign", descriptor.ID, foreign}} {
		t.Run(test.name, func(t *testing.T) {
			value, err := h.action(test.link, "transfer.status", &protocol.TransferTarget{ID: test.id})
			if err == nil {
				t.Fatal("hub accepted inaccessible/non-live status", value)
			}
			raw, encodeErr := ResponseBytes(9, transferReply(value, err))
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			frame, decodeErr := protocol.DecodeFrame(raw)
			if decodeErr != nil || frame.Error == nil || frame.Error.Code != -32019 || frame.Error.Message != "transfer_failed" || len(frame.Error.Data) != 0 {
				t.Fatal("federation vocabulary changed", string(raw), decodeErr)
			}
		})
	}
}
