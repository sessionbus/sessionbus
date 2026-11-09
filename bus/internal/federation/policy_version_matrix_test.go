// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"encoding/json"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/roster"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

// This helper is compiled unchanged at both pinned revisions. It uses only
// their preexisting APIs; test-process stdin/stdout is not a federation frame.
func TestVersionedPolicyFederationProcess(t *testing.T) {
	mode := os.Getenv("FEDERATION_MATRIX_MODE")
	if mode == "" {
		t.Skip("external pinned-version helper")
	}
	encode := json.NewEncoder(os.Stdout)
	decode := json.NewDecoder(os.Stdin)
	secrets := map[string]string{"alpha": testSecret('a'), "beta": testSecret('b')}
	if mode == "hub" {
		configuration, err := ServerTLS(secrets)
		if err != nil {
			t.Fatal(err)
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		hub := StartHub(listener, configuration, io.Discard)
		defer hub.Close()
		_ = encode.Encode(map[string]string{"ready": listener.Addr().String()})
		var command map[string]string
		_ = decode.Decode(&command)
		return
	}
	host := os.Getenv("FEDERATION_MATRIX_HOST")
	daemon := connectTestDaemon(t, os.Getenv("FEDERATION_MATRIX_ADDRESS"), host, secrets[host], func(call IncomingCall) (Wait, error) {
		if IsRoster(call) {
			return immediate(RosterReply(roster.Host{Host: host, Products: []string{}, Sessions: []roster.Row{}})), nil
		}
		if call.Request.Method == "turn.status" {
			if body, err := protocol.ErrorBytes(1, -32017, nil); err == nil {
				frame, _ := protocol.DecodeFrame(body[:len(body)-1])
				return immediate(Reply{Error: frame.Error, ErrorRaw: frame.ErrorRaw}), nil
			}
			return immediate(errorReply(protocol.Busy, nil)), nil
		}
		if call.Request.Method == "session.list" {
			from, _ := json.Marshal(call.From)
			var fields map[string]any
			_ = json.Unmarshal(from, &fields)
			raw, err := protocol.EncodeResult("session.list", protocol.SessionListResult{Sessions: []protocol.SessionSummary{{SessionID: "peer@" + host, Kind: "peer", Product: "fixture", Groups: []string{"team"}, Connected: true, Info: map[string]any{"saw_policy": fields["policy"] == true}}}})
			if err != nil {
				return nil, err
			}
			return immediate(Reply{Result: raw}), nil
		}
		return immediate(Reply{Result: []byte(`{}`)}), nil
	})
	_ = callHosts(t, daemon)
	_ = encode.Encode(map[string]string{"ready": host})
	for {
		var command map[string]string
		if decode.Decode(&command) != nil || command["op"] == "quit" {
			return
		}
		if command["op"] == "hosts" {
			_ = encode.Encode(map[string]any{"hosts": callHosts(t, daemon)})
			continue
		}
		caller := Caller{SessionID: "sender@" + host, Product: "peer", PrivateGroup: "session:sender@" + host, Groups: []string{"team", "session:sender@" + host}}
		_ = json.Unmarshal([]byte(`{"policy":true}`), &caller)
		method := command["op"]
		var params []byte
		if method == "session.list" {
			params, _ = protocol.EncodeParams(method, protocol.SessionListRequest{Host: "beta"})
		} else {
			params, _ = protocol.EncodeParams(method, protocol.SessionTarget{SessionID: "lane@beta"})
		}
		reply := make(chan Reply, 1)
		daemon.inbox <- OutgoingCall{Value: Forward{From: caller, Request: PublicRequest{Method: method, Params: params}}, Reply: reply}
		select {
		case value := <-reply:
			_ = encode.Encode(map[string]any{"result": value.Result, "error": value.Error})
		case <-time.After(time.Second):
			t.Fatal("mixed-version forwarding did not settle")
		}
	}
}
