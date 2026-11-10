// SPDX-License-Identifier: MIT
package sessionkit

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/internal/rpc"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestTransferDisabledCallbackRefusalIsSent(t *testing.T) {
	for _, state := range []string{"missing", "disabled", "cancelled", "different-wire"} {
		t.Run(state, func(t *testing.T) {
			local, remote := net.Pipe()
			defer local.Close()
			defer remote.Close()
			tx := newTransfers(nil)
			var client *rpc.Conn
			client = rpc.New(local, true, func(_ context.Context, r *rpc.Request) {
				if !tx.handle(client, r) {
					t.Error("callback unhandled")
				}
			})
			defer client.Close()
			var server *rpc.Conn
			server = rpc.New(remote, false, func(_ context.Context, r *rpc.Request) {
				go func() { _ = server.Result(r, protocol.SessionListResult{Sessions: []protocol.SessionSummary{}}) }()
			})
			defer server.Close()
			life, cancel := context.WithCancel(context.Background())
			defer cancel()
			if state != "missing" {
				tx.current = &transferGeneration{ctx: life, wire: client, enabled: state != "disabled"}
				if state == "cancelled" {
					cancel()
				}
				if state == "different-wire" {
					tx.current.wire = &rpc.Conn{}
				}
			}
			ctx, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			err := server.Call(ctx, "transfer.prepare", protocol.TransferPrepare{ID: "x", Role: "source"}, &protocol.TransferPrepared{})
			var refusal *protocol.RPCError
			if !errors.As(err, &refusal) || refusal.Code != -32018 || refusal.Message != "unsupported_transfer" || len(refusal.Data) != 0 {
				t.Fatalf("refusal not sent: %v", err)
			}
			var listed protocol.SessionListResult
			if err := client.Call(ctx, "session.list", protocol.SessionListRequest{}, &listed); err != nil {
				t.Fatal("ordinary connection lost", err)
			}
		})
	}
}
