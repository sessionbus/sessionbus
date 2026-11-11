// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/internal/roster"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestPolicyUnknownNamesFailBeforeListener(t *testing.T) {
	dir := t.TempDir()
	config, policy := filepath.Join(dir, "secrets.json"), filepath.Join(dir, "policy.json")
	secret := base64.StdEncoding.EncodeToString(make([]byte, federation.SecretBytes))
	if err := os.WriteFile(config, []byte(`{"alpha":"`+secret+`"}`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{`{"hosts":{"ghost":{}}}`, `{"hosts":{"alpha":{"lanes_on":["ghost"]}}}`, `{"hosts":{"alpha":{"lanes_to":["ghost"]}}}`} {
		if err := os.WriteFile(policy, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		err := runWithIO([]string{"--listen", "invalid-address", "--config", config, "--policy", policy}, io.Discard, io.Discard)
		if err == nil || !strings.Contains(err.Error(), `unknown federation policy host "ghost"`) || strings.Contains(err.Error(), secret) {
			t.Fatal("unknown host failed open or reached listener", err)
		}
	}
}

func TestPolicyUnknownStartupHasNonzeroProcessExit(t *testing.T) {
	dir := t.TempDir()
	config, policy := filepath.Join(dir, "hub.json"), filepath.Join(dir, "policy.json")
	secret := base64.StdEncoding.EncodeToString(make([]byte, federation.SecretBytes))
	if err := os.WriteFile(config, []byte(`{"alpha":"`+secret+`"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(policy, []byte(`{"hosts":{"alpha":{"lanes_to":["ghost"]}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal([]string{"--listen", "invalid-address", "--config", config, "--policy", policy})
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^TestHubPolicyPrivateProcess$")
	command.Env = append(os.Environ(), "SESSIONBUS_TEST_POLICY_CHILD="+string(encoded))
	out, err := command.CombinedOutput()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() == 0 || !strings.Contains(string(out), `unknown federation policy host "ghost"`) || strings.Contains(string(out), secret) {
		t.Fatalf("startup exit=%v output=%s", err, out)
	}
}

func TestPolicyCLIUnknownReloadAndUnconfiguredHUP(t *testing.T) {
	for _, configured := range []bool{false, true} {
		t.Run(map[bool]string{false: "no-policy", true: "policy"}[configured], func(t *testing.T) {
			dir := t.TempDir()
			config, policy, logPath := filepath.Join(dir, "hub.json"), filepath.Join(dir, "policy.json"), filepath.Join(dir, "log")
			secrets := map[string]string{}
			for index, host := range []string{"alpha", "beta"} {
				key := make([]byte, federation.SecretBytes)
				key[0] = byte(index + 1)
				secrets[host] = base64.StdEncoding.EncodeToString(key)
			}
			raw, _ := json.Marshal(secrets)
			if err := os.WriteFile(config, raw, 0600); err != nil {
				t.Fatal(err)
			}
			write := func(body string) {
				t.Helper()
				if err := os.WriteFile(policy, []byte(body), 0600); err != nil {
					t.Fatal(err)
				}
			}
			write(`{"hosts":{"alpha":{"groups":[]}}}`)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			address := listener.Addr().String()
			_ = listener.Close()
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			arguments := []string{"--listen", address, "--config", config}
			if configured {
				arguments = append(arguments, "--policy", policy)
			}
			encoded, _ := json.Marshal(arguments)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, executable, "-test.run=^TestHubPolicyPrivateProcess$")
			command.Env = append(os.Environ(), "SESSIONBUS_TEST_POLICY_CHILD="+string(encoded))
			log, err := os.Create(logPath)
			if err != nil {
				t.Fatal(err)
			}
			defer log.Close()
			command.Stdout, command.Stderr = log, log
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			ended := make(chan struct{})
			var exit error
			go func() { exit = command.Wait(); close(ended) }()
			t.Cleanup(func() {
				cancel()
				select {
				case <-ended:
				case <-time.After(2 * time.Second):
					t.Error("child did not reap")
				}
			})
			connect := func(host string) chan any {
				t.Helper()
				var raw net.Conn
				for deadline := time.Now().Add(2 * time.Second); ; {
					raw, err = net.DialTimeout("tcp", address, 50*time.Millisecond)
					if err == nil {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal(err)
					}
					time.Sleep(time.Millisecond)
				}
				configuration, err := federation.ClientTLS(host, secrets[host])
				if err != nil {
					t.Fatal(err)
				}
				secure := tls.Client(raw, configuration)
				if err := secure.HandshakeContext(ctx); err != nil {
					t.Fatal(err)
				}
				inbox := make(chan any, 16)
				linkCtx, stop := context.WithCancel(ctx)
				joined := make(chan struct{})
				go func() {
					defer close(joined)
					_ = federation.ServeDaemon(linkCtx, host, secure, inbox, func(call federation.IncomingCall) (federation.Wait, error) {
						value := federation.Reply{Result: []byte(`{}`)}
						if federation.IsRoster(call) {
							value = federation.RosterReply(roster.Host{Host: host, Products: []string{}, Sessions: []roster.Row{}})
						}
						return func(<-chan struct{}) (federation.Reply, bool) { return value, true }, nil
					}, io.Discard)
				}()
				t.Cleanup(func() {
					stop()
					select {
					case <-joined:
					case <-time.After(time.Second):
						t.Error("own daemon did not join")
					}
				})
				answer := make(chan federation.Reply, 1)
				inbox <- federation.HostsCall{Reply: answer}
				select {
				case reply := <-answer:
					if _, err := federation.DecodeHosts(reply); err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("registration missing")
				}
				return inbox
			}
			_ = connect("beta")
			alpha := connect("alpha")
			check := func(blocked bool) {
				t.Helper()
				params, _ := protocol.EncodeParams("turn.interrupt", protocol.SessionTarget{SessionID: "row@beta"})
				reply := make(chan federation.Reply, 1)
				alpha <- federation.OutgoingCall{Value: federation.Forward{From: federation.Caller{SessionID: "parent@alpha", Product: "fixture", PrivateGroup: "session:parent@alpha", Groups: []string{"team", "session:parent@alpha"}, Policy: true}, Request: federation.PublicRequest{Method: "turn.interrupt", Params: params}}, Reply: reply}
				select {
				case value := <-reply:
					if blocked {
						if value.Error == nil || value.Error.Code != protocol.PolicyDenied {
							t.Fatal("invalid reload changed old refusal", value)
						}
					} else if value.Error != nil {
						t.Fatal("same caller stopped", value.Error)
					}
				case <-time.After(time.Second):
					t.Fatal("same caller response missing")
				}
			}
			waitLog := func(token string) {
				t.Helper()
				for deadline := time.Now().Add(time.Second); ; {
					body, _ := os.ReadFile(logPath)
					if strings.Count(string(body), token) == 1 {
						return
					}
					if time.Now().After(deadline) {
						t.Fatal("expected one log", token, string(body))
					}
					time.Sleep(time.Millisecond)
				}
			}
			check(configured)
			if configured {
				write(`{"hosts":{"ghost":{}}}`)
			}
			if err := command.Process.Signal(syscall.SIGHUP); err != nil {
				t.Fatal(err)
			}
			if configured {
				waitLog(`unknown federation policy host "ghost"`)
				check(true)
				write(`{"hosts":{}}`)
				if err := command.Process.Signal(syscall.SIGHUP); err != nil {
					t.Fatal(err)
				}
				waitLog("federation policy reloaded")
				check(false)
			} else {
				waitLog("SIGHUP ignored")
				check(false)
			}
			if err := command.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			select {
			case <-ended:
				if exit != nil {
					t.Fatal("normal stop", exit)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("child stop missing")
			}
		})
	}
}
