// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/internal/roster"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

func readRoster(t *testing.T, socket string, local bool) roster.Report {
	t.Helper()
	fd, err := net.Dial("unix", roster.Socket(socket))
	must(t, err)
	defer fd.Close()
	must(t, fd.SetDeadline(time.Now().Add(12*time.Second)))
	must(t, json.NewEncoder(fd).Encode(roster.Request{Local: local}))
	body, err := bufio.NewReaderSize(fd, protocol.MaxFrameBytes).ReadSlice('\n')
	must(t, err)
	var result roster.Report
	must(t, json.Unmarshal(body, &result))
	return result
}

func TestOperatorRosterCrossGroupsRedactionAndClose(t *testing.T) {
	dir := testsocket.Directory(t)
	socket := filepath.Join(dir, "presence.sock")
	table := filepath.Join(t.TempDir(), "rows")
	stored := &tableForRoster{path: table}
	stored.write(t)
	d, err := Start(Config{SocketPath: socket, TablePath: table})
	must(t, err)
	defer d.Close()
	first := connectPeer(t, socket, "first", "First", "one")
	_ = connectPeer(t, socket, "second", "Second", "two")
	d.directory.mu.Lock()
	d.directory.entries["second@local"].info = map[string]any{"credential": "do-not-export", "cwd": "/private/native"}
	d.directory.mu.Unlock()
	var list protocol.SessionListResult
	must(t, first.call("session.list", protocol.SessionListRequest{}, &list))
	if len(list.Sessions) != 1 {
		t.Fatalf("ordinary visibility changed: %#v", list.Sessions)
	}
	result := readRoster(t, socket, false)
	if !result.Complete || result.Schema != roster.Schema || len(result.Local.Sessions) != 3 {
		t.Fatalf("roster=%#v", result)
	}
	data, _ := json.Marshal(result)
	for _, secret := range []string{"do-not-export", "/private/native", "hidden-system-prompt", "owner-token-secret", "secret-notify"} {
		if strings.Contains(string(data), secret) {
			t.Fatalf("roster leaked %s", secret)
		}
	}
	lane := result.Local.Sessions[1]
	if lane.SessionID != "retained@local" || lane.Connected || !lane.Persistent || lane.Owner != "" {
		t.Fatalf("retained lane=%#v", lane)
	}
	info, err := os.Stat(roster.Socket(socket))
	must(t, err)
	if info.Mode().Perm() != 0600 {
		t.Fatal("operator mode", info.Mode())
	}
	// Neither registered peers nor public pre-hello calls gain the operator method.
	if err = first.call("federation.roster", map[string]any{}, &list); err == nil {
		t.Fatal("peer invoked private method")
	}
	stalled, err := net.Dial("unix", roster.Socket(socket))
	must(t, err)
	defer stalled.Close()
	done := make(chan error, 1)
	go func() { done <- d.Close() }()
	select {
	case err = <-done:
		must(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("operator connection blocked daemon Close")
	}
	if _, err = os.Lstat(roster.Socket(socket)); !os.IsNotExist(err) {
		t.Fatalf("operator socket remained: %v", err)
	}
}

type tableForRoster struct{ path string }

func (s *tableForRoster) write(t *testing.T) {
	t.Helper()
	store := &table{path: s.path}
	must(t, store.write(row{SessionID: "retained@local", Name: "Retained@local", Product: "fixture", Groups: []string{"third", "session:retained@local"}, CreatedAt: time.Now(), Open: protocol.OpenOptions{Arguments: []string{"hidden-system-prompt"}}, Policy: &protocol.LanePolicy{Persistent: true, AutoCloseMS: 60000, Notify: true, NotifyTarget: "secret-notify@local"}}))
}

func TestOperatorRosterRejectsExistingFileAndHandlesStaleSocket(t *testing.T) {
	socket := filepath.Join(testsocket.Directory(t), "presence.sock")
	path := roster.Socket(socket)
	must(t, os.WriteFile(path, []byte("keep"), 0600))
	_, err := Start(Config{SocketPath: socket, TablePath: filepath.Join(t.TempDir(), "rows")})
	if err == nil {
		t.Fatal("overwrote file")
	}
	raw, err := os.ReadFile(path)
	must(t, err)
	if string(raw) != "keep" {
		t.Fatal("changed file")
	}
	must(t, os.Remove(path))
	leaveStaleSocket(t, path)
	d, err := Start(Config{SocketPath: socket, TablePath: filepath.Join(t.TempDir(), "rows")})
	must(t, err)
	must(t, d.Close())
}

func TestFederatedOperatorRosterAndLegacyCapability(t *testing.T) {
	for _, variant := range []string{"current", "old-hub", "old-host"} {
		t.Run(variant, func(t *testing.T) {
			secret := func(b byte) string { v := make([]byte, 32); v[0] = b; return base64.StdEncoding.EncodeToString(v) }
			secrets := map[string]string{"alpha": secret(1), "beta": secret(2)}
			config, err := federation.ServerTLS(secrets)
			must(t, err)
			if variant == "old-hub" {
				original := config.GetConfigForClient
				config.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
					c, e := original(hello)
					if e != nil {
						return nil, e
					}
					c = c.Clone()
					c.NextProtos = nil
					return c, nil
				}
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			must(t, err)
			hub := federation.StartHub(listener, config, io.Discard)
			defer hub.Close()
			start := func(host string) (*Daemon, string) {
				socket := filepath.Join(testsocket.Directory(t), "presence.sock")
				cfg := Config{SocketPath: socket, TablePath: filepath.Join(t.TempDir(), "rows"), Host: host, Products: []string{"fixture"}}
				if variant != "old-host" || host != "beta" {
					cfg.HubAddress = listener.Addr().String()
					cfg.HubSecret = secrets[host]
				}
				d, e := Start(cfg)
				must(t, e)
				if variant == "old-host" && host == "beta" {
					client, e := federation.ClientTLS(host, secrets[host])
					must(t, e)
					client.NextProtos = nil
					fd, e := tls.Dial("tcp", listener.Addr().String(), client)
					must(t, e)
					must(t, d.StartFederation(context.Background(), fd, io.Discard))
				}
				return d, socket
			}
			alpha, a := start("alpha")
			defer alpha.Close()
			beta, b := start("beta")
			defer beta.Close()
			caller := connectPeer(t, a, "caller", "Caller", "one")
			_ = connectPeer(t, b, "hidden", "Hidden", "two")
			var ordinary protocol.SessionListResult
			must(t, caller.call("session.list", protocol.SessionListRequest{}, &ordinary))
			if len(ordinary.Sessions) != 1 {
				t.Fatalf("cross-group public leak: %#v", ordinary.Sessions)
			}
			var result roster.Report
			deadline := time.Now().Add(5 * time.Second)
			for {
				result = readRoster(t, a, false)
				if len(result.Remote) == 1 && result.Remote[0].Host == "beta" {
					break
				}
				if time.Now().After(deadline) {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			if len(result.Remote) != 1 || result.Remote[0].Host != "beta" {
				t.Fatalf("roster=%#v", result)
			}
			if variant == "current" {
				if !result.Complete || len(result.Remote[0].Sessions) != 1 || result.Remote[0].Sessions[0].SessionID != "hidden@beta" {
					t.Fatalf("roster=%#v", result)
				}
			} else if result.Complete || result.Remote[0].Error == "" {
				t.Fatalf("legacy capability not reported: %#v", result)
			}
			// A legacy roster request must not retire or damage either federation link.
			must(t, caller.call("session.list", protocol.SessionListRequest{Host: "beta"}, &ordinary))
			if len(ordinary.Sessions) != 0 {
				t.Fatal("legacy path changed visibility")
			}
			local := readRoster(t, a, true)
			if !local.Complete || len(local.Remote) != 0 {
				t.Fatal("local roster should work independently")
			}
		})
	}
}

func TestPublicSocketRejectsOperatorRPCBeforeAndAfterHello(t *testing.T) {
	socket := filepath.Join(testsocket.Directory(t), "presence.sock")
	d, err := Start(Config{SocketPath: socket, TablePath: filepath.Join(t.TempDir(), "rows")})
	must(t, err)
	defer d.Close()
	for _, hello := range []bool{false, true} {
		fd, err := net.Dial("unix", socket)
		must(t, err)
		must(t, fd.SetDeadline(time.Now().Add(time.Second)))
		reader := bufio.NewReader(fd)
		if hello {
			must(t, rawCall(fd, reader, 1, "session.hello", protocol.PeerHello{Protocol: 1, Product: "fixture", SessionID: "public-test", Groups: []string{}, Info: map[string]any{}}, &struct{}{}))
		}
		// Deliberately bypass the SDK schema check to exercise daemon admission.
		_, err = io.WriteString(fd, "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"federation.roster\",\"params\":{}}\n")
		must(t, err)
		frame, err := readRawFrame(reader)
		must(t, err)
		if frame.Error == nil || len(frame.Result) != 0 {
			t.Fatalf("public operator RPC succeeded: %#v", frame)
		}
		_ = fd.Close()
	}
}

// An authenticated hub must negotiate the private operator capability before
// the daemon admits a direct roster control frame (not a public forwarded RPC).
func TestDaemonRosterAdmissionRequiresNegotiatedCapability(t *testing.T) {
	for _, capable := range []bool{false, true} {
		label := "no-alpn"
		if capable {
			label = "roster-alpn"
		}
		t.Run(label, func(t *testing.T) {
			secret, err := federation.NewSecret()
			must(t, err)
			serverConfig, err := federation.ServerTLS(map[string]string{"alpha": secret})
			must(t, err)
			if !capable {
				original := serverConfig.GetConfigForClient
				serverConfig.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
					config, err := original(hello)
					if err != nil {
						return nil, err
					}
					config = config.Clone()
					config.NextProtos = nil
					return config, nil
				}
			}
			listener, err := tls.Listen("tcp", "127.0.0.1:0", serverConfig)
			must(t, err)
			defer listener.Close()
			accepted := make(chan net.Conn, 1)
			acceptErrors := make(chan error, 1)
			go func() {
				fd, err := listener.Accept()
				if err == nil {
					err = fd.(*tls.Conn).Handshake()
				}
				if err != nil {
					if fd != nil {
						_ = fd.Close()
					}
					acceptErrors <- err
					return
				}
				accepted <- fd
			}()
			clientConfig, err := federation.ClientTLS("alpha", secret)
			must(t, err)
			client, err := tls.Dial("tcp", listener.Addr().String(), clientConfig)
			must(t, err)
			defer client.Close()
			var hub net.Conn
			select {
			case hub = <-accepted:
			case err = <-acceptErrors:
				t.Fatal(err)
			case <-time.After(5 * time.Second):
				t.Fatal("TLS accept stalled")
			}
			defer hub.Close()
			if federation.SupportsRoster(client) != capable {
				t.Fatal("wrong negotiated capability")
			}
			socket := filepath.Join(testsocket.Directory(t), "presence.sock")
			d, err := Start(Config{SocketPath: socket, TablePath: filepath.Join(t.TempDir(), "rows"), Host: "alpha"})
			must(t, err)
			defer d.Close()
			_ = connectPeer(t, socket, "private", "Private", "private-group")
			must(t, d.StartFederation(context.Background(), client, io.Discard))
			must(t, hub.SetDeadline(time.Now().Add(5*time.Second)))
			_, err = io.WriteString(hub, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"federation.roster\",\"params\":{}}\n")
			must(t, err)
			frame, err := readRawFrame(bufio.NewReader(hub))
			if !capable {
				if err != io.EOF {
					t.Fatalf("unnegotiated roster was not rejected: frame=%#v err=%v", frame, err)
				}
				return
			}
			must(t, err)
			result, err := roster.DecodeRemote(frame.Result)
			must(t, err)
			if len(result.Hosts) != 1 || len(result.Hosts[0].Sessions) != 1 || result.Hosts[0].Sessions[0].SessionID != "private@alpha" {
				t.Fatalf("negotiated roster=%#v", result)
			}
		})
	}
}

func TestCombinedRosterOverflowPreservesLocalProjection(t *testing.T) {
	rows := func(host string, count int) []roster.Row {
		result := make([]roster.Row, count)
		for i := range result {
			result[i] = roster.Row{SessionID: fmt.Sprintf("peer-%d@%s", i, host), Name: strings.Repeat("n", 80), Kind: "peer", Product: "fixture", Groups: []string{strings.Repeat("g", 80)}, Connected: true}
		}
		return result
	}
	value := roster.Report{Schema: roster.Schema, Federation: "connected", Complete: true, Local: roster.Host{Host: "local", Products: []string{}, Sessions: rows("local", 1300)}, Remote: []roster.Host{{Host: "beta", Products: []string{}, Sessions: rows("beta", 2500)}}}
	local, err := roster.Encode(value.Local)
	must(t, err)
	if len(local) >= roster.MaxBytes/2 {
		t.Fatal("local fixture exceeded its own bound")
	}
	_, err = roster.Encode(roster.Remote{Hosts: value.Remote})
	must(t, err)
	if _, err = roster.Encode(value); err == nil {
		t.Fatal("fixture did not exceed combined bound")
	}
	body := encodeOperatorRoster(value)
	if len(body) > roster.MaxBytes {
		t.Fatal("fallback exceeded wire bound")
	}
	var actual roster.Report
	must(t, json.Unmarshal(body, &actual))
	if actual.Complete || actual.Error != "roster_too_large" || !reflect.DeepEqual(actual.Local, value.Local) || actual.Federation != "connected" {
		t.Fatalf("lost local projection or incomplete authority: complete=%v error=%s", actual.Complete, actual.Error)
	}
	if len(actual.Remote) != 1 || actual.Remote[0].Host != "beta" || actual.Remote[0].Error != "roster_too_large" || len(actual.Remote[0].Sessions) != 0 {
		t.Fatal("missing explicit remote overflow")
	}
}
