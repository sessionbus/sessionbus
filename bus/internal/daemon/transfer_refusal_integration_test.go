// SPDX-License-Identifier: GPL-3.0-only
package daemon_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/daemon"
	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

func TestTransferU6UnsupportedTargetPreservesOwnerAndLane(t *testing.T) {
	for _, target := range []string{"old@local", "old@remote"} {
		t.Run(target, func(t *testing.T) { transferU6Refusal(t, target) })
	}
}

func transferU6Refusal(t *testing.T, target string) {
	dir := testsocket.Directory(t)
	name := "fixture-worker-refusal"
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(executable, filepath.Join(dir, name)); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	socket := filepath.Join(dir, "bus.sock")
	d, err := daemon.Start(daemon.Config{SocketPath: socket, TablePath: filepath.Join(dir, "rows"), Products: []string{name}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	source, _ := transferPeer(t, socket, "source", &fixtureTransferExecutor{source: "/private/unused"})
	// A raw old hello has no transfer bit. Current SDK Peer defaults to a
	// filesystem executor and therefore cannot stand in for an old endpoint.
	old, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	_ = old.SetDeadline(time.Now().Add(time.Second))
	hello, err := protocol.RequestBytes(1, "session.hello", protocol.PeerHello{Protocol: 1, Product: "old-fixture", SessionID: "old", Groups: []string{"shared"}, Info: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = old.Write(hello); err != nil {
		t.Fatal(err)
	}
	raw, err := bufio.NewReader(old).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	frame, err := protocol.DecodeFrame(raw)
	if err != nil || frame.Error != nil {
		t.Fatal("legacy target hello", string(raw), err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	zero := int64(0)
	lane, err := source.Caller.Spawn(ctx, sessionkit.LaneSpawnRequest{Product: name, Name: "child", Open: &sessionkit.OpenOptions{}, AutoCloseMS: &zero})
	if err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{"transfer.offer", "transfer.request"} {
		var params any = protocol.TransferOffer{Target: target, Name: "file"}
		if method == "transfer.request" {
			params = protocol.TransferRequest{Target: target, What: "file"}
		}
		_, err = source.Call(ctx, method, params)
		var refusal *protocol.RPCError
		if !errors.As(err, &refusal) || refusal.Code != -32018 || refusal.Message != "unsupported_transfer" || len(refusal.Data) != 0 {
			t.Fatalf("%s refusal disconnected sender: %v", method, err)
		}
		if _, err = source.Caller.List(ctx, sessionkit.SessionListRequest{}); err != nil {
			t.Fatal("ordinary list lost", err)
		}
	}
	for _, method := range []string{"transfer.status", "transfer.cancel", "transfer.accept"} {
		_, err = source.Call(ctx, method, protocol.TransferTarget{ID: "unknown"})
		var refusal *protocol.RPCError
		if !errors.As(err, &refusal) || refusal.Code != -32019 || len(refusal.Data) != 0 {
			t.Fatalf("%s unknown transfer refusal = %v", method, err)
		}
		if _, err = source.Caller.List(ctx, sessionkit.SessionListRequest{}); err != nil {
			t.Fatal("unknown transfer lost caller", err)
		}
	}
	result, err := source.Caller.Run(ctx, sessionkit.TurnRunRequest{SessionID: lane.SessionID, Input: "useful after refusal"})
	if err != nil || result.State != "done" || result.Result == nil || result.Result.Outcome != "completed" {
		t.Fatal("nonpersistent lane was lost", result, err)
	}
	t.Logf("used original owner's lane result before ACK: %#v", result.Result)
	if err = source.Caller.Ack(ctx, sessionkit.RunRef{SessionID: lane.SessionID, RunID: result.RunID}); err != nil {
		t.Fatal(err)
	}
	if err = source.Caller.Close(ctx, sessionkit.SessionCloseRequest{SessionID: lane.SessionID, Forget: true}); err != nil {
		t.Fatal(err)
	}
}

func TestTransferCapacityRefusalPreservesConnection(t *testing.T) {
	dir := testsocket.Directory(t)
	socket := filepath.Join(dir, "bus.sock")
	d, err := daemon.Start(daemon.Config{SocketPath: socket, TablePath: filepath.Join(dir, "rows")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	source, _ := transferPeer(t, socket, "source", &fixtureTransferExecutor{})
	_, _ = transferPeer(t, socket, "destination", &fixtureTransferExecutor{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var ids []string
	for range protocol.MaxPendingTransfersPerConnection {
		raw, err := source.Call(ctx, "transfer.offer", protocol.TransferOffer{Target: "destination@local", Name: "file"})
		if err != nil {
			t.Fatal("pending creation", err)
		}
		var value protocol.TransferDescriptor
		if err := json.Unmarshal(raw, &value); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, value.ID)
	}
	_, err = source.Call(ctx, "transfer.offer", protocol.TransferOffer{Target: "destination@local", Name: "file"})
	var refusal *protocol.RPCError
	if !errors.As(err, &refusal) || refusal.Code != protocol.Busy || len(refusal.Data) != 0 {
		t.Fatal("capacity refusal lost connection", err)
	}
	if _, err = source.Caller.List(ctx, sessionkit.SessionListRequest{}); err != nil {
		t.Fatal("list after capacity", err)
	}
	for _, id := range ids {
		if _, err = source.Call(ctx, "transfer.cancel", protocol.TransferTarget{ID: id}); err != nil {
			t.Fatal("pending cleanup after capacity", err)
		}
	}
}
