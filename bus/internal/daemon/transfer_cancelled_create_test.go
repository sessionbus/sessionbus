// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/antst/sessionbus/bus/internal/transfer"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestPreIDTransferCancellationRefusesWithoutLosingCaller(t *testing.T) {
	d, socket := startDaemon(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	budget := transfer.HostBudget()
	d.transfers = transfer.NewEngine(ctx, budget)
	source := connectPeer(t, socket, "source", "source", "shared")
	_ = connectPeer(t, socket, "destination", "destination", "shared")
	for _, method := range []string{"transfer.offer", "transfer.request"} {
		var input any = protocol.TransferOffer{Target: "destination@local", Name: "file"}
		if method == "transfer.request" {
			input = protocol.TransferRequest{Target: "destination@local", What: "file"}
		}
		err := source.call(method, input, nil)
		var refusal *protocol.RPCError
		if !errors.As(err, &refusal) || refusal.Code != protocol.Internal || string(refusal.Data) != `"transfer_unavailable"` {
			t.Fatal("pre-ID cancellation invented a failed transfer", err)
		}
		usage := budget.Usage()
		if usage.Pending != 0 || usage.Active != 0 {
			t.Fatal("pre-ID request retained a reservation", usage)
		}
		var rows protocol.SessionListResult
		if err := source.call("session.list", protocol.SessionListRequest{}, &rows); err != nil {
			t.Fatal("caller disconnected", err)
		}
	}
	err := source.call("transfer.status", protocol.TransferTarget{ID: "unknown"}, nil)
	// TestUnknownTransferStatusLocalAndForwardedEnded retains non-opted legacy status coverage.
	if rpcCode(err) != protocol.UnknownTransfer {
		t.Fatal("ordinary unknown transfer changed", err)
	}
}
