// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/internal/federation"
	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

func TestSemanticHostRefusalsKeepSamePeer(t *testing.T) {
	secrets := reconnectSecrets()
	_, address := reconnectHub(t, "127.0.0.1:0", secrets)
	_, socket := reconnectDaemon(t, "alpha", address, secrets["alpha"])
	for index, host := range []string{"dev2-nonexistent-fed-host-20261019", "BAD", "-invalid", "gamma", strings.Repeat("z", 32)} {
		for _, method := range []string{"lane.describe", "lane.spawn"} {
			t.Run(method+"/"+host, func(t *testing.T) {
				id := fmt.Sprintf("host-check-%d-%s", index, strings.TrimPrefix(method, "lane."))
				peer := connectPeer(t, socket, id, id, "shared")
				var params any = &protocol.LaneDescribeRequest{Product: "fixture-worker", Host: host}
				if method == "lane.spawn" {
					params = &protocol.LaneSpawnRequest{Product: "fixture-worker", Name: "no-dispatch", Host: host, Open: &protocol.OpenOptions{}}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_, err := peer.peer.Call(ctx, method, params)
				if rpcCode(err) != protocol.UnknownHost {
					t.Fatalf("semantic host refusal=%v, want unknown_host", err)
				}
				var rows protocol.SessionListResult
				if err = peer.call("session.list", protocol.SessionListRequest{SessionID: id}, &rows); err != nil || len(rows.Sessions) != 1 || rows.Sessions[0].SessionID != id+"@alpha" {
					t.Fatalf("same caller lost after refusal: %#v %v", rows, err)
				}
			})
		}
	}
}

// Exercise the real daemon response methods over one private framed SDK wire.
// This is codec/SDK transport evidence, not native product or live-host evidence.
func errorCodecFixture(t *testing.T, mode string) (string, <-chan error) {
	t.Helper()
	socket := filepath.Join(testsocket.Directory(t), "error.sock")
	listener, err := net.Listen("unix", socket)
	must(t, err)
	done := make(chan error, 1)
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		fd, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		_ = fd.SetDeadline(time.Now().Add(5 * time.Second))
		d := &Daemon{}
		d.directory = newDirectory(d, nil)
		s := newSession(d)
		s.wire = conn.Start(fd, s.inbox, &d.group)
		defer func() { s.wire.Close(); s.wire.OwnerClosed(); d.group.Wait() }()
		var ids []int64
		for event := range s.inbox {
			switch event := event.(type) {
			case conn.Frame:
				frame := event.Value
				ids = append(ids, frame.ID)
				if !frame.Request || frame.Method != "session.list" || len(ids) > 2 {
					done <- fmt.Errorf("unexpected request %#v", frame)
					return
				}
				if len(ids) == 1 {
					switch mode {
					case "local":
						s.error(frame, protocol.InvalidFrame, "forbidden")
					case "forwarded-typed":
						s.remoteResponse(frame, federation.Reply{Error: &protocol.RPCError{Code: protocol.InvalidFrame, Message: "invalid_frame", Data: json.RawMessage(`"forbidden"`)}})
					case "forwarded-raw":
						s.remoteResponse(frame, federation.Reply{Error: &protocol.RPCError{Code: protocol.Busy, Message: "busy"}, ErrorRaw: json.RawMessage(`{"code":-32600,"message":"invalid_frame","data":"forbidden"}`)})
					default:
						done <- fmt.Errorf("unknown mode %s", mode)
						return
					}
				} else {
					s.result(frame, protocol.SessionListResult{Sessions: []protocol.SessionSummary{}})
				}
			case conn.Closed:
				if !reflect.DeepEqual(ids, []int64{1, 2}) {
					done <- fmt.Errorf("requests=%v, want same wire IDs1,2", ids)
				} else {
					done <- nil
				}
				return
			}
		}
	}()
	return socket, done
}

func TestStrictGoAndJSClientsKeepErrorCodecConnection(t *testing.T) {
	for _, language := range []string{"go", "js"} {
		for _, mode := range []string{"local", "forwarded-typed", "forwarded-raw"} {
			t.Run(language+"/"+mode, func(t *testing.T) {
				socket, done := errorCodecFixture(t, mode)
				if language == "go" {
					client, err := sessionkit.Dial(socket)
					must(t, err)
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					defer cancel()
					_, err = client.Call(ctx, "session.list", protocol.SessionListRequest{})
					var failure *sessionkit.ProtocolError
					if !errors.As(err, &failure) || failure.Code != protocol.Internal || failure.Message != "internal" || string(failure.Data) != `"internal"` {
						_ = client.Close()
						t.Fatalf("strict Go fallback=%v", err)
					}
					raw, err := client.Call(ctx, "session.list", protocol.SessionListRequest{})
					_ = client.Close()
					if err != nil {
						t.Fatal("same Go wire lost", err)
					}
					var result protocol.SessionListResult
					if err = json.Unmarshal(raw, &result); err != nil || len(result.Sessions) != 0 {
						t.Fatalf("continuation=%s %v", raw, err)
					}
				} else {
					node, err := exec.LookPath("node")
					if err != nil {
						t.Skip("existing JavaScript SDK runtime unavailable")
					}
					fixture, err := filepath.Abs("testdata/error-client.cjs")
					must(t, err)
					source, err := filepath.Abs("../../sdk/js/connection.js")
					must(t, err)
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					output, err := exec.CommandContext(ctx, node, fixture, socket, source).CombinedOutput()
					if err != nil {
						t.Fatalf("strict JS codec/continuation: %s %v", output, err)
					}
				}
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("private SDK wire did not finish")
				}
			})
		}
	}
}
