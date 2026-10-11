// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/antst/sessionbus/bus/internal/roster"
	"github.com/antst/sessionbus/bus/sdk/go/socketpath"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

func TestStartCleansOwnSocketAndPreservesLaneSockets(t *testing.T) {
	directory := testsocket.Directory(t)
	socket := filepath.Join(directory, "sessionbus.sock")
	lanes := filepath.Join(directory, "lanes")
	if err := os.Mkdir(lanes, 0o700); err != nil {
		t.Fatal(err)
	}
	leaveStaleSocket(t, socket)
	staleLane := filepath.Join(lanes, "stale.sock")
	leaveStaleSocket(t, staleLane)
	staleInfo, err := os.Lstat(staleLane)
	if err != nil {
		t.Fatal(err)
	}
	liveLane, err := net.Listen("unix", filepath.Join(lanes, "live.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer liveLane.Close()
	daemon, err := Start(Config{SocketPath: socket, TablePath: filepath.Join(directory, "sessions")})
	if err != nil {
		t.Fatal(err)
	}
	defer daemon.Close()
	if info, statErr := os.Lstat(staleLane); statErr != nil || !os.SameFile(staleInfo, info) {
		t.Fatalf("product-owned socket entry changed: %v", statErr)
	}
	if fd, dialErr := net.Dial("unix", liveLane.Addr().String()); dialErr != nil {
		t.Fatalf("live lane removed: %v", dialErr)
	} else {
		_ = fd.Close()
	}
}

func TestSweepLeavesOwnLiveSocketAndUnexpectedDialError(t *testing.T) {
	t.Run("live", func(t *testing.T) {
		path := filepath.Join(testsocket.Directory(t), "live.sock")
		listener, err := net.Listen("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		before, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		sweepSockets(path)
		if after, statErr := os.Lstat(path); statErr != nil || !os.SameFile(before, after) {
			t.Fatalf("live daemon socket changed: %v", statErr)
		}
	})
	t.Run("unexpected dial error", func(t *testing.T) {
		path := filepath.Join(testsocket.Directory(t), strings.Repeat("x", 120))
		if err := os.WriteFile(path, []byte("unchanged"), 0o600); err != nil {
			t.Fatal(err)
		}
		fd, err := net.Dial("unix", path)
		if fd != nil {
			_ = fd.Close()
		}
		if err == nil || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, os.ErrNotExist) {
			t.Fatalf("fixture needs an unhandled dial error: %v", err)
		}
		sweepSockets(path)
		if contents, readErr := os.ReadFile(path); readErr != nil || string(contents) != "unchanged" {
			t.Fatalf("unexpected dial error removed the entry: %v", readErr)
		}
	})
}

func TestStartRejectsPartialFederationConfiguration(t *testing.T) {
	for _, values := range []struct{ host, address, secret string }{
		{host: "alpha", address: "hub:9443"},
		{host: "alpha", secret: "secret"},
		{host: "local", address: "hub:9443", secret: "secret"},
	} {
		directory := t.TempDir()
		_, err := Start(Config{SocketPath: filepath.Join(directory, "sessionbus.sock"), TablePath: filepath.Join(directory, "sessions"), Host: values.host, HubAddress: values.address, HubSecret: values.secret})
		if err == nil {
			t.Fatalf("accepted partial federation config %#v", values)
		}
	}
}

func TestStartAllowsRuntimeRootWithoutLaneSocketRoom(t *testing.T) {
	limit := 107
	if runtime.GOOS == "darwin" {
		limit = 103
	}
	parent := testsocket.Directory(t)
	rootLength := limit - 1 - len(filepath.Base(roster.Socket("presence.sock")))
	root := filepath.Join(parent, strings.Repeat("x", rootLength-len(parent)-1))
	socket := filepath.Join(root, "presence.sock")
	table := filepath.Join(t.TempDir(), "sessions")
	lane := socketpath.Lane(socket, "")
	if len(lane) <= limit || len(roster.Socket(socket)) != limit {
		t.Fatalf("invalid boundary fixture: lane %q, operator %q", lane, roster.Socket(socket))
	}
	daemon, err := Start(Config{SocketPath: socket, TablePath: table})
	if err != nil {
		t.Fatal(err)
	}
	defer daemon.Close()
}

func TestStartRejectsUnusableDaemonSocketPathsBeforeCreation(t *testing.T) {
	limit := 107
	if runtime.GOOS == "darwin" {
		limit = 103
	}
	for _, endpoint := range []string{"public", "operator"} {
		t.Run(endpoint, func(t *testing.T) {
			parent := testsocket.Directory(t)
			root := filepath.Join(parent, "runtime")
			socket := filepath.Join(root, strings.Repeat("s", limit-len(root)))
			unusable := socket
			if endpoint == "operator" {
				rootLength := limit - len(filepath.Base(roster.Socket("presence.sock")))
				root = filepath.Join(parent, strings.Repeat("x", rootLength-len(parent)-1))
				socket = filepath.Join(root, "presence.sock")
				unusable = roster.Socket(socket)
			}
			if len(unusable) != limit+1 {
				t.Fatalf("boundary fixture length = %d", len(unusable))
			}
			_, err := Start(Config{SocketPath: socket, TablePath: filepath.Join(t.TempDir(), "sessions")})
			if err == nil || err.Error() != "sessionbus Unix socket path limit "+fmt.Sprint(limit)+" exceeded by \""+unusable+"\"" {
				t.Fatalf("start error = %v", err)
			}
			if _, statErr := os.Stat(root); !os.IsNotExist(statErr) {
				t.Fatalf("runtime root was created before rejection: %v", statErr)
			}
		})
	}
}

func leaveStaleSocket(t *testing.T, path string) {
	t.Helper()
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(false)
	if err = listener.Close(); err != nil {
		t.Fatal(err)
	}
}
