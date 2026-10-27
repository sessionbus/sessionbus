// SPDX-License-Identifier: MIT
package sessionkit

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/internal/rpc"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestLateAcceptSnapshotCannotRecreateRetiredBinding(t *testing.T) {
	local, remote := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	transfers := newTransfers(nil)
	assigned := make(chan struct{})
	var wire *rpc.Conn
	wire = rpc.New(local, true, func(_ context.Context, request *rpc.Request) {
		<-assigned
		if !transfers.handle(wire, request) {
			t.Error("unexpected machine callback")
		}
	})
	close(assigned)
	defer wire.Close()
	var accepts atomic.Int32
	var server *rpc.Conn
	server = rpc.New(remote, false, func(_ context.Context, request *rpc.Request) {
		go func() {
			if request.Method != "transfer.accept" {
				t.Error("unexpected request", request.Method)
				return
			}
			accepts.Add(1)
			zero := protocol.Integer(0)
			var prepared protocol.TransferPrepared
			if err := server.Call(ctx, "transfer.prepare", protocol.TransferPrepare{ID: "one", Role: "destination", Size: &zero}, &prepared); err != nil {
				t.Error(err)
				return
			}
			var started protocol.TransferStarted
			if err := server.Call(ctx, "transfer.start", protocol.TransferStart{ID: "one", Role: "destination", Size: zero}, &started); err != nil {
				t.Error(err)
				return
			}
			var empty struct{}
			if err := server.Call(ctx, "transfer.notice", protocol.TransferNotice{Kind: "terminal", ID: "one", Outcome: "failed", Bytes: &zero, Reason: "source_changed"}, &empty); err != nil {
				t.Error(err)
				return
			}
			// This admitted snapshot arrives after the terminal machine ACK.
			if err := server.Result(request, protocol.TransferDescriptor{ID: "one", Sender: "source@local", Receiver: "destination@local", CreatedBy: "source@local", Name: "file", State: "streaming", Size: &zero}); err != nil {
				t.Error(err)
			}
		}()
	})
	defer server.Close()
	transfers.attach(wire, ctx, true)
	defer transfers.wait(wire)
	from := protocol.DeliverySource{SessionID: "source@local", Product: "fixture", Groups: []string{"test"}}
	var empty struct{}
	if err := server.Call(ctx, "transfer.notice", protocol.TransferNotice{Kind: "offer", ID: "one", From: &from, Name: "file"}, &empty); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "destination")
	accepted, err := transfers.Accept(ctx, "one", destination, false)
	if err != nil || accepted.ID != "one" || accepted.State != "streaming" {
		t.Fatal("late admission snapshot lost", accepted, err)
	}
	g, err := transfers.generation()
	if err != nil || g.binding("one") != nil {
		t.Fatal("retired binding recreated", err)
	}
	transfers.mu.Lock()
	pending, active := transfers.pending, transfers.active
	transfers.mu.Unlock()
	if pending != 0 || active != 0 {
		t.Fatal("late response recharged a retired slot", pending, active)
	}
	if _, err = transfers.Accept(ctx, "one", destination, false); !errors.Is(err, ErrTransferBinding) || accepts.Load() != 1 {
		t.Fatal("retired transfer was accepted again", err, accepts.Load())
	}
	if _, err = os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed source published", err)
	}
	entries, err := os.ReadDir(filepath.Dir(destination))
	if err != nil || len(entries) != 0 {
		t.Fatal("prepared temporary file survived", entries, err)
	}
}
