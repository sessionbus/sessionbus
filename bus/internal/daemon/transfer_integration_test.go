// SPDX-License-Identifier: GPL-3.0-only
package daemon_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/daemon"
	"github.com/antst/sessionbus/bus/internal/federation"
	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

// This fixture grants only two exact test-owned paths. It demonstrates real
// file/stream/publication behavior, NOT a production native policy executor.
type fixtureTransferExecutor struct {
	source, destination string
	mu                  sync.Mutex
	opens               int
	deny                bool
	verifyError         error
	readError           error
}

func (x *fixtureTransferExecutor) OpenTransferSource(ctx context.Context, path string) (sessionkit.TransferSource, error) {
	if x.deny || path != x.source {
		return nil, sessionkit.ErrTransferScope
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	stat, e := f.Stat()
	if e != nil || !stat.Mode().IsRegular() {
		_ = f.Close()
		return nil, errors.New("not a file")
	}
	x.mu.Lock()
	x.opens++
	x.mu.Unlock()
	return &fixtureTransferSource{File: f, stat: stat, verifyError: x.verifyError, readError: x.readError}, nil
}
func (x *fixtureTransferExecutor) OpenTransferDestination(ctx context.Context, path string, overwrite bool) (sessionkit.TransferDestination, error) {
	if x.deny || path != x.destination || overwrite {
		return nil, sessionkit.ErrTransferScope
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".transfer-")
	if e != nil {
		return nil, e
	}
	x.mu.Lock()
	x.opens++
	x.mu.Unlock()
	return &fixtureTransferDestination{File: f, target: path}, nil
}

type fixtureTransferSource struct {
	*os.File
	stat        os.FileInfo
	verifyError error
	readError   error
}

func (x *fixtureTransferSource) Read(raw []byte) (int, error) {
	if x.readError != nil {
		return 0, x.readError
	}
	return x.File.Read(raw)
}

func (x *fixtureTransferSource) Size() int64 { return x.stat.Size() }
func (x *fixtureTransferSource) Verify() error {
	if x.verifyError != nil {
		return x.verifyError
	}
	s, e := x.Stat()
	if e != nil {
		return e
	}
	if !os.SameFile(x.stat, s) || s.Size() != x.stat.Size() {
		return errors.New("source changed")
	}
	return nil
}

type fixtureTransferDestination struct {
	*os.File
	target    string
	published bool
}

func (x *fixtureTransferDestination) Commit() (bool, error) {
	if e := x.Close(); e != nil {
		return false, e
	}
	if e := os.Link(x.Name(), x.target); e != nil {
		return false, e
	}
	x.published = true
	return true, os.Remove(x.Name())
}
func (x *fixtureTransferDestination) Abort() error {
	_ = x.Close()
	if x.published {
		return nil
	}
	e := os.Remove(x.Name())
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	return e
}
func transferPeer(t *testing.T, socket, id string, executor sessionkit.TransferExecutor) (*sessionkit.Peer, <-chan protocol.TransferNotice) {
	t.Helper()
	t.Setenv("SESSIONBUS_SOCKET", socket)
	notices := make(chan protocol.TransferNotice, 16)
	p, e := sessionkit.ConnectPeerWithTransfersContext(context.Background(), sessionkit.PeerIdentity{Product: "fixture-client", SessionID: id, Name: id, Groups: []string{"shared"}, Info: map[string]any{}}, func(ctx context.Context, _ sessionkit.PeerIdentity, d sessionkit.DeliveryRequest) (sessionkit.DeliveryReceipt, error) {
		var n protocol.TransferNotice
		if json.Unmarshal([]byte(d.Body), &n) == nil && n.Kind != "" {
			if fixture, ok := executor.(*fixtureTransferExecutor); ok && n.Kind == "terminal" {
				var local map[string]json.RawMessage
				_ = json.Unmarshal([]byte(d.Body), &local)
				if fixture.source != "" && (strings.Contains(d.Body, fixture.source) || local["destination"] != nil) {
					t.Error("source/sender notice leaked local path")
				}
				if fixture.destination != "" {
					var destination string
					_ = json.Unmarshal(local["destination"], &destination)
					if destination != fixture.destination {
						t.Error("receiver destination projection missing", destination)
					}
				}
			}
			select {
			case notices <- n:
			case <-ctx.Done():
			}
		}
		return sessionkit.DeliveryReceipt{Disposition: "written"}, nil
	}, executor)
	if e != nil {
		t.Fatal(e)
	}
	select {
	case <-p.Ready():
	case <-time.After(time.Second):
		t.Fatal("not ready")
	}
	t.Cleanup(func() { p.Shutdown(); <-p.Closed() })
	return p, notices
}
func awaitTransferNotice(t *testing.T, notices <-chan protocol.TransferNotice, id, kind string) protocol.TransferNotice {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case n := <-notices:
			if n.ID == id && n.Kind == kind {
				return n
			}
		case <-timer.C:
			t.Fatalf("missing %s for %s", kind, id)
		}
	}
}
func TestTransferSDKRealDaemonOfferRequestAndOrdinaryContinuity(t *testing.T) {
	for _, offer := range []bool{true, false} {
		t.Run(map[bool]string{true: "offer", false: "request"}[offer], func(t *testing.T) {
			dir := testsocket.Directory(t)
			socket := filepath.Join(dir, "sessionbus.sock")
			d, e := daemon.Start(daemon.Config{SocketPath: socket, TablePath: filepath.Join(dir, "sessions")})
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() { _ = d.Close() })
			sourcePath, destPath := filepath.Join(dir, "private-source"), filepath.Join(dir, "private-destination")
			data := make([]byte, 3*protocol.TransferChunkBytes+17)
			for i := range data {
				data[i] = byte(i % 251)
			}
			if e = os.WriteFile(sourcePath, data, 0600); e != nil {
				t.Fatal(e)
			}
			sourceExec := &fixtureTransferExecutor{source: sourcePath}
			destExec := &fixtureTransferExecutor{destination: destPath}
			source, sourceNotices := transferPeer(t, socket, "source", sourceExec)
			dest, destNotices := transferPeer(t, socket, "destination", destExec)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var descriptor protocol.TransferDescriptor
			if offer {
				descriptor, e = source.Transfers.Offer(ctx, "destination@local", sourcePath, "payload")
			} else {
				descriptor, e = dest.Transfers.Request(ctx, "source@local", "the fixture data", destPath, "payload", false)
			}
			if e != nil {
				t.Fatal(e)
			}
			sourceExec.mu.Lock()
			opens := sourceExec.opens
			sourceExec.mu.Unlock()
			if opens != 0 {
				t.Fatal("pending opened source")
			}
			if _, e = os.Stat(destPath); !errors.Is(e, os.ErrNotExist) {
				t.Fatal("pending published destination")
			}
			if offer {
				awaitTransferNotice(t, destNotices, descriptor.ID, "offer")
				_, e = dest.Transfers.Accept(ctx, descriptor.ID, destPath, false)
			} else {
				awaitTransferNotice(t, sourceNotices, descriptor.ID, "request")
				_, e = source.Transfers.Accept(ctx, descriptor.ID, sourcePath, false)
			}
			if e != nil {
				t.Fatal(e)
			}
			terminal := awaitTransferNotice(t, destNotices, descriptor.ID, "terminal")
			if terminal.Outcome != "completed" {
				t.Fatal(terminal)
			}
			awaitTransferNotice(t, sourceNotices, descriptor.ID, "terminal")
			got, e := os.ReadFile(destPath)
			if e != nil || string(got) != string(data) {
				t.Fatal("published bytes mismatch", e, len(got))
			}
			sourceExec.mu.Lock()
			opens = sourceExec.opens
			sourceExec.mu.Unlock()
			if opens != 1 {
				t.Fatal("not one source handle", opens)
			}
			listed, e := source.Caller.List(ctx, sessionkit.SessionListRequest{})
			if e != nil || len(listed.Sessions) != 2 {
				t.Fatal("ordinary list lost", listed, e)
			}
			result, e := source.Caller.Send(ctx, sessionkit.MessageSendRequest{Target: "destination@local", Message: "ordinary after transfer"})
			if e != nil || len(result.Deliveries) != 1 || result.Deliveries[0].Disposition != "written" {
				t.Fatal("ordinary send lost", result, e)
			}
		})
	}
}
func TestTransferSDKDeniedPreparationAndNoOverwriteKeepConnection(t *testing.T) {
	dir := testsocket.Directory(t)
	socket := filepath.Join(dir, "sessionbus.sock")
	d, e := daemon.Start(daemon.Config{SocketPath: socket, TablePath: filepath.Join(dir, "sessions")})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = d.Close() })
	sourcePath, destPath := filepath.Join(dir, "source"), filepath.Join(dir, "destination")
	_ = os.WriteFile(sourcePath, []byte("data"), 0600)
	_ = os.WriteFile(destPath, []byte("existing"), 0600)
	source, _ := transferPeer(t, socket, "source", &fixtureTransferExecutor{source: sourcePath})
	dest, notices := transferPeer(t, socket, "destination", &fixtureTransferExecutor{destination: destPath})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	descriptor, e := source.Transfers.Offer(ctx, "destination@local", sourcePath, "payload")
	if e != nil {
		t.Fatal(e)
	}
	awaitTransferNotice(t, notices, descriptor.ID, "offer")
	_, _ = dest.Transfers.Accept(ctx, descriptor.ID, destPath, false)
	terminal := awaitTransferNotice(t, notices, descriptor.ID, "terminal")
	if terminal.Outcome != "failed" {
		t.Fatal(terminal)
	}
	got, _ := os.ReadFile(destPath)
	if string(got) != "existing" {
		t.Fatal("overwrote destination")
	}
	_, e = dest.Caller.List(ctx, sessionkit.SessionListRequest{})
	if e != nil {
		t.Fatal("healthy connection lost", e)
	}
	temps, _ := filepath.Glob(filepath.Join(dir, ".transfer-*"))
	if len(temps) != 0 {
		t.Fatal("temp retained", temps)
	}
}

var _ io.Reader = (*fixtureTransferSource)(nil)

func TestTransferSDKSourceFailuresDoNotPublishOrBreakOrdinaryWork(t *testing.T) {
	for _, reason := range []string{"source_changed", "source_read_failed"} {
		t.Run(reason, func(t *testing.T) {
			dir := testsocket.Directory(t)
			socket := filepath.Join(dir, "bus.sock")
			d, err := daemon.Start(daemon.Config{SocketPath: socket, TablePath: filepath.Join(dir, "sessions")})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = d.Close() })
			sourcePath, destinationPath := filepath.Join(dir, "private-source"), filepath.Join(dir, "private-destination")
			if err = os.WriteFile(sourcePath, []byte("verified data"), 0600); err != nil {
				t.Fatal(err)
			}
			executor := &fixtureTransferExecutor{source: sourcePath}
			if reason == "source_changed" {
				executor.verifyError = errors.New("local identity changed")
			} else {
				executor.readError = io.ErrUnexpectedEOF
			}
			source, _ := transferPeer(t, socket, "source", executor)
			dest, notices := transferPeer(t, socket, "destination", &fixtureTransferExecutor{destination: destinationPath})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			descriptor, err := source.Transfers.Offer(ctx, "destination@local", sourcePath, "payload")
			if err != nil {
				t.Fatal(err)
			}
			awaitTransferNotice(t, notices, descriptor.ID, "offer")
			if _, err = dest.Transfers.Accept(ctx, descriptor.ID, destinationPath, false); err != nil {
				t.Fatal(err)
			}
			terminal := awaitTransferNotice(t, notices, descriptor.ID, "terminal")
			if terminal.Outcome != "failed" || terminal.Reason != reason {
				t.Fatal(terminal)
			}
			if _, err = os.Stat(destinationPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("failed source published", err)
			}
			if _, err = source.Caller.Send(ctx, sessionkit.MessageSendRequest{Target: "destination@local", Message: "ordinary after source failure"}); err != nil {
				t.Fatal("ordinary connection lost", err)
			}
		})
	}
}

func TestTransferSDKPrivateHubOfferRequestAndOrdinaryContinuity(t *testing.T) {
	for _, offer := range []bool{true, false} {
		t.Run(map[bool]string{true: "offer", false: "request"}[offer], func(t *testing.T) {
			secrets := map[string]string{}
			for _, host := range []string{"alpha", "beta"} {
				secret, err := federation.NewSecret()
				if err != nil {
					t.Fatal(err)
				}
				secrets[host] = secret
			}
			tlsConfig, err := federation.ServerTLS(secrets)
			if err != nil {
				t.Fatal(err)
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			hub := federation.StartHub(listener, tlsConfig, io.Discard)
			t.Cleanup(hub.Close)
			start := func(host string) string {
				dir := testsocket.Directory(t)
				socket := filepath.Join(dir, "sessionbus.sock")
				d, err := daemon.Start(daemon.Config{SocketPath: socket, TablePath: filepath.Join(dir, "sessions"), Host: host, HubAddress: listener.Addr().String(), HubSecret: secrets[host]})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = d.Close() })
				return socket
			}
			alpha, beta := start("alpha"), start("beta")
			dir := t.TempDir()
			sourcePath, destPath := filepath.Join(dir, "private-source"), filepath.Join(dir, "private-destination")
			data := make([]byte, 4*protocol.TransferChunkBytes+13)
			for i := range data {
				data[i] = byte(i % 239)
			}
			if err = os.WriteFile(sourcePath, data, 0600); err != nil {
				t.Fatal(err)
			}
			source, sourceNotices := transferPeer(t, alpha, "source", &fixtureTransferExecutor{source: sourcePath})
			dest, destNotices := transferPeer(t, beta, "destination", &fixtureTransferExecutor{destination: destPath})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			// Native-free fixture admission only: no transfer stimulus is retried.
			for {
				_, err = source.Caller.List(ctx, sessionkit.SessionListRequest{Host: "beta"})
				if err == nil {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("federation admission", err)
				case <-time.After(time.Millisecond):
				}
			}
			var descriptor protocol.TransferDescriptor
			if offer {
				descriptor, err = source.Transfers.Offer(ctx, "destination@beta", sourcePath, "payload")
			} else {
				descriptor, err = dest.Transfers.Request(ctx, "source@alpha", "fixture data", destPath, "payload", false)
			}
			if err != nil {
				t.Fatal("creation", err)
			}
			if offer {
				awaitTransferNotice(t, destNotices, descriptor.ID, "offer")
				_, err = dest.Transfers.Accept(ctx, descriptor.ID, destPath, false)
			} else {
				awaitTransferNotice(t, sourceNotices, descriptor.ID, "request")
				_, err = source.Transfers.Accept(ctx, descriptor.ID, sourcePath, false)
			}
			if err != nil {
				t.Fatal("accept", err)
			}
			terminal := awaitTransferNotice(t, destNotices, descriptor.ID, "terminal")
			if terminal.Outcome != "completed" {
				t.Fatal("terminal", terminal)
			}
			awaitTransferNotice(t, sourceNotices, descriptor.ID, "terminal")
			actual, err := os.ReadFile(destPath)
			if err != nil || string(actual) != string(data) {
				t.Fatal("receiver bytes", len(actual), err)
			}
			sent, err := source.Caller.Send(ctx, sessionkit.MessageSendRequest{Target: "destination@beta", Message: "ordinary after transfer"})
			if err != nil || len(sent.Deliveries) != 1 || sent.Deliveries[0].Disposition != "written" {
				t.Fatal("federated ordinary continuity", sent, err)
			}
		})
	}
}
