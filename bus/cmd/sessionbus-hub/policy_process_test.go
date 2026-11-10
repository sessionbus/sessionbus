// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/internal/roster"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestHubPolicyPrivateProcess(t *testing.T) {
	encoded := os.Getenv("SESSIONBUS_TEST_POLICY_CHILD")
	if encoded == "" {
		return
	}
	var arguments []string
	if err := json.Unmarshal([]byte(encoded), &arguments); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := runWithIO(arguments, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

func TestHubPolicySIGHUPReloadRetainsLinksAndInvalidSnapshot(t *testing.T) {
	dir := t.TempDir()
	config, policy := filepath.Join(dir, "hub.json"), filepath.Join(dir, "policy.json")
	secrets := map[string]string{}
	for i, host := range []string{"alpha", "beta"} {
		key := make([]byte, federation.SecretBytes)
		key[0] = byte(i + 1)
		secrets[host] = base64.StdEncoding.EncodeToString(key)
	}
	raw, _ := json.Marshal(secrets)
	if err := os.WriteFile(config, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	write := func(body string) {
		t.Helper()
		if err := os.WriteFile(policy, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"hosts":{"alpha":{"groups":["team"]}}}`)
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := reservation.Addr().String()
	_ = reservation.Close()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	arguments, _ := json.Marshal([]string{"--listen", address, "--config", config, "--policy", policy})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^TestHubPolicyPrivateProcess$")
	command.Env = append(os.Environ(), "SESSIONBUS_TEST_POLICY_CHILD="+string(arguments))
	logPath := filepath.Join(dir, "hub.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	command.Stdout, command.Stderr = log, log
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	ended := make(chan error, 1)
	go func() { ended <- command.Wait() }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-ended:
		case <-time.After(2 * time.Second):
			t.Error("private CLI process did not join")
		}
	})
	seen := make(chan federation.Caller, 4)
	type endpoint struct{ inbox chan any }
	connect := func(host string) endpoint {
		t.Helper()
		configuration, err := federation.ClientTLS(host, secrets[host])
		if err != nil {
			t.Fatal(err)
		}
		var fd net.Conn
		for deadline := time.Now().Add(3 * time.Second); ; {
			fd, err = net.DialTimeout("tcp", address, 50*time.Millisecond)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("private CLI did not listen", err)
			}
			time.Sleep(time.Millisecond)
		}
		secure := tls.Client(fd, configuration)
		if err := secure.HandshakeContext(ctx); err != nil {
			t.Fatal(err)
		}
		inbox, closed := make(chan any, 16), make(chan struct{})
		linkContext, stop := context.WithCancel(ctx)
		go func() {
			defer close(closed)
			_ = federation.ServeDaemon(linkContext, host, secure, inbox, func(call federation.IncomingCall) (federation.Wait, error) {
				reply := federation.Reply{Result: []byte(`{}`)}
				if federation.IsRoster(call) {
					reply = federation.RosterReply(roster.Host{Host: host, Products: []string{}, Sessions: []roster.Row{}})
				} else if call.Request.Method == "session.list" {
					reply.Result = []byte(`{"sessions":[{"session_id":"row@beta","kind":"peer","product":"fixture","groups":["team","other"],"connected":true,"running":false}]}`)
				} else {
					seen <- call.From
				}
				return func(<-chan struct{}) (federation.Reply, bool) { return reply, true }, nil
			}, io.Discard)
		}()
		t.Cleanup(func() {
			stop()
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Error("private fixture link did not join")
			}
		})
		answer := make(chan federation.Reply, 1)
		inbox <- federation.HostsCall{Reply: answer}
		select {
		case value := <-answer:
			if _, err := federation.DecodeHosts(value); err != nil {
				t.Fatal("authenticated registration did not complete", err)
			}
		case <-time.After(time.Second):
			t.Fatal("registration did not settle")
		}
		return endpoint{inbox: inbox}
	}
	_ = connect("beta")
	alpha := connect("alpha")
	caller := federation.Caller{SessionID: "parent@alpha", Product: "fixture", PrivateGroup: "session:parent@alpha", Groups: []string{"team", "other", "session:parent@alpha"}}
	call := func(from federation.Caller, method string, params any) federation.Reply {
		t.Helper()
		raw, err := protocol.EncodeParams(method, params)
		if err != nil {
			t.Fatal(err)
		}
		answer := make(chan federation.Reply, 1)
		alpha.inbox <- federation.OutgoingCall{Value: federation.Forward{From: from, Request: federation.PublicRequest{Method: method, Params: raw}}, Reply: answer}
		select {
		case value := <-answer:
			return value
		case <-time.After(time.Second):
			t.Fatal("private CLI call did not settle")
			return federation.Reply{}
		}
	}
	control := func(allowed, removed string) {
		t.Helper()
		if reply := call(caller, "turn.interrupt", protocol.SessionTarget{SessionID: "row@beta"}); reply.Error != nil {
			t.Fatal(reply.Error)
		}
		from := <-seen
		if !slices.Contains(from.Groups, allowed) || slices.Contains(from.Groups, removed) {
			t.Fatal("private CLI did not apply the active policy snapshot", from.Groups)
		}
	}
	control("team", "other")
	write(`{"hosts":{"alpha":{"groups":null}}}`)
	if err := command.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(time.Second); ; {
		logged, _ := os.ReadFile(logPath)
		if strings.Contains(string(logged), "policy rule must be an array") {
			if strings.Count(string(logged), "policy rule must be an array") != 1 {
				t.Fatal("one invalid reload logged more than once", string(logged))
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("invalid SIGHUP was not processed", string(logged))
		}
		time.Sleep(time.Millisecond)
	}
	control("team", "other")
	write(`{"hosts":{"alpha":{"groups":["other"]}}}`)
	if err := command.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	probe := caller
	probe.Groups = []string{"team", probe.PrivateGroup}
	// Read-only lists synchronize with asynchronous signal handling. No work
	// or uncertain message is replayed while waiting for the new snapshot.
	for deadline := time.Now().Add(time.Second); ; {
		reply := call(probe, "session.list", protocol.SessionListRequest{Host: "beta"})
		value, err := protocol.DecodeResult("session.list", reply.Result)
		if reply.Error != nil || err != nil {
			t.Fatal("reload broke an admitted link", reply.Error, err)
		}
		if len(value.(*protocol.SessionListResult).Sessions) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("valid SIGHUP did not replace the policy")
		}
		time.Sleep(time.Millisecond)
	}
	control("other", "team")
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-ended:
		if err != nil {
			t.Fatal("private CLI did not stop normally", err)
		}
		// Keep the single Wait result available for the cleanup owner.
		ended <- err
	case <-time.After(3 * time.Second):
		t.Fatal("private CLI SIGTERM did not join")
	}
}
