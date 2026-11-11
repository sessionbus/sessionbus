// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestConfiguredFederationEmitsMarkAndClear(t *testing.T) {
	if os.Getenv("SESSIONBUS_TEST_CONFIGURED_LOG_CHILD") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestConfiguredFederationEmitsMarkAndClear$", "-test.v")
		cmd.Env = append(os.Environ(), "SESSIONBUS_TEST_CONFIGURED_LOG_CHILD=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("configured daemon logging: %v\n%s", err, out)
		}
		return
	}
	// Isolate the process-global existing stderr sink. The ordinary configured
	// reconnect supervisor, not StartFederation(custom writer), owns this link.
	capture, err := os.CreateTemp(t.TempDir(), "stderr")
	must(t, err)
	previous := os.Stderr
	os.Stderr = capture
	t.Cleanup(func() { os.Stderr = previous; _ = capture.Close() })
	secrets := reconnectSecrets()
	configuration, err := federation.ServerTLS(secrets)
	must(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan net.Conn, 1)
	go func() { fd, _ := listener.Accept(); accepted <- fd }()
	d, _ := reconnectDaemon(t, "alpha", listener.Addr().String(), secrets["alpha"])
	var raw net.Conn
	select {
	case raw = <-accepted:
		if raw == nil {
			t.Fatal("configured connection missing")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("configured supervisor did not dial")
	}
	t.Cleanup(func() { _ = raw.Close() })
	fd := tls.Server(raw, configuration)
	must(t, fd.SetDeadline(time.Now().Add(30*time.Second)))
	must(t, fd.Handshake())
	body, err := bufio.NewReader(fd).ReadBytes('\n')
	must(t, err)
	probe, err := protocol.DecodeFrame(body)
	must(t, err)
	if !probe.Request || probe.Method != "federation.hosts" {
		t.Fatal("not the genuine configured hosts probe", string(body))
	}
	await := func(state string) string {
		t.Helper()
		until := time.Now().Add(20 * time.Second)
		for time.Now().Before(until) {
			data, readErr := os.ReadFile(capture.Name())
			must(t, readErr)
			if strings.Contains(string(data), "state="+state+" ") {
				return string(data)
			}
			time.Sleep(10 * time.Millisecond)
		}
		data, _ := os.ReadFile(capture.Name())
		t.Fatalf("configured stderr missing %s: %s", state, data)
		return ""
	}
	_ = await("not_responding")
	// Answer the SAME retained probe after its existing mark, without reconnect
	// or a new liveness request. This is valid ordinary hub response metadata.
	result, err := json.Marshal(map[string]any{"hosts": []string{"alpha"}})
	must(t, err)
	reply, err := federation.ResponseBytes(probe.ID, federation.Reply{Result: result})
	must(t, err)
	_, err = fd.Write(reply)
	must(t, err)
	output := await("responding")
	for _, state := range []string{"not_responding", "responding"} {
		prefix := `federation host="alpha" peer=hub state=` + state + ` time=`
		if strings.Count(output, prefix) != 1 {
			t.Fatal("transition missing or duplicated", output)
		}
		for _, line := range strings.Split(output, "\n") {
			if strings.HasPrefix(line, prefix) {
				_, err := time.Parse(time.RFC3339Nano, strings.TrimPrefix(line, prefix))
				must(t, err)
			}
		}
	}
	select {
	case <-d.Done():
		t.Fatal("transition logging changed daemon lifetime")
	default:
	}
	t.Log(output)
}
