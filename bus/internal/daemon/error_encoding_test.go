// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"bufio"
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

func TestOwnErrorEncodingFailureKeepsConnection(t *testing.T) {
	for _, test := range []struct {
		name string
		code int
		data any
	}{
		{"unsupported-data", protocol.UnsupportedOpen, "unsupported_transfer"},
		{"busy-data", protocol.Busy, "transfer_failed"},
		{"unknown-data", protocol.UnknownSession, "transfer_failed"},
		{"internal-missing", protocol.Internal, nil},
		{"internal-empty", protocol.Internal, ""},
		{"unencodable", protocol.Internal, make(chan int)},
		{"busy-unencodable", protocol.Busy, make(chan int)},
		{"oversized", protocol.Internal, strings.Repeat("x", protocol.MaxFrameBytes)},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, s, peer := reviewSession(t)
			_ = peer.SetReadDeadline(time.Now().Add(time.Second))
			reader := bufio.NewReader(peer)
			s.error(protocol.Frame{ID: 7, Method: "session.list"}, test.code, test.data)
			raw, err := reader.ReadBytes('\n')
			if err != nil {
				t.Fatalf("own error encoding lost caller: %v", err)
			}
			frame, err := protocol.DecodeFrame(raw)
			if err != nil || frame.ID != 7 || frame.Error == nil || frame.Error.Code != protocol.Internal || frame.Error.Message != "internal" || string(frame.Error.Data) != `"internal"` {
				t.Fatalf("fallback = %s, %v", raw, err)
			}
			s.result(protocol.Frame{ID: 8, Method: "session.list"}, protocol.SessionListResult{Sessions: []protocol.SessionSummary{}})
			raw, err = reader.ReadBytes('\n')
			if err != nil {
				t.Fatal("subsequent result lost", err)
			}
			frame, err = protocol.DecodeFrame(raw)
			if err != nil || frame.ID != 8 || frame.Error != nil {
				t.Fatal("subsequent result invalid", string(raw), err)
			}
		})
	}
}

func TestOwnForwardedErrorEncodingFailureDoesNotPanic(t *testing.T) {
	for _, bad := range []answer{{code: protocol.UnsupportedOpen, data: "unsupported_transfer"}, {code: protocol.Busy, data: "transfer_failed"}, {code: protocol.Internal}} {
		func() {
			defer func() {
				if value := recover(); value != nil {
					t.Errorf("own forwarded error panicked: %v", value)
				}
			}()
			reply := rpcReply("transfer.offer", bad)
			value, err := protocol.DecodeError(reply.ErrorRaw)
			if err != nil || value.Code != protocol.Internal || string(value.Data) != `"internal"` {
				t.Errorf("forwarded fallback = %#v, %v", reply, err)
			}
		}()
	}
}

func TestReconstructedErrorsKeepRequiredData(t *testing.T) {
	raw := json.RawMessage(`"original_error"`)
	reply := federation.Reply{Error: &protocol.RPCError{Code: protocol.Internal, Message: "internal", Data: raw}}
	s := newSession(&Daemon{})
	channel := make(chan federation.Reply, 1)
	channel <- reply
	got := s.collectFederatedList(federation.Caller{}, nil, channel, make(chan struct{}))
	if got.code != protocol.Internal || got.data != "original_error" {
		t.Errorf("hosts error lost data: %#v", got)
	}
	channel <- reply
	got = collectMessageSend("m", []messageLeg{{reply: channel}}, nil, make(chan struct{}), protocol.NotConnected)
	if got.code != protocol.Internal || got.data != "original_error" {
		t.Errorf("aggregate error lost data: %#v", got)
	}
}

func TestNonTransferSenderRefusalKeepsConnection(t *testing.T) {
	_, socket := startDaemon(t)
	fd, err := net.Dial("unix", socket)
	must(t, err)
	defer fd.Close()
	_ = fd.SetDeadline(time.Now().Add(time.Second))
	reader := bufio.NewReader(fd)
	must(t, rawCall(fd, reader, 1, "session.hello", protocol.PeerHello{Protocol: 1, Product: "legacy", SessionID: "ordinary", Groups: []string{"shared"}, Info: map[string]any{}}, &protocol.HelloResult{}))
	err = rawCall(fd, reader, 2, "transfer.offer", protocol.TransferOffer{Target: "missing@local", Name: "file"}, nil)
	if rpcCode(err) != protocol.UnsupportedOpen {
		t.Fatal("non-transfer sender lost old-decodable refusal", err)
	}
	var rows protocol.SessionListResult
	if err := rawCall(fd, reader, 3, "session.list", protocol.SessionListRequest{}, &rows); err != nil {
		t.Fatal("sender disconnected", err)
	}
}

func TestTransferUnknownRefusalKeepsConnection(t *testing.T) {
	for _, method := range []string{"transfer.status", "transfer.cancel", "transfer.accept"} {
		t.Run(method, func(t *testing.T) {
			_, socket := startDaemon(t)
			peer := connectPeer(t, socket, "sender", "sender", "shared")
			err := peer.call(method, protocol.TransferTarget{ID: "unknown"}, nil)
			if rpcCode(err) != -32019 {
				t.Fatal("unknown transfer lost valid refusal", err)
			}
			var rows protocol.SessionListResult
			if err := peer.call("session.list", protocol.SessionListRequest{}, &rows); err != nil {
				t.Fatal("unknown transfer disconnected caller", err)
			}
		})
	}
}

func TestInvalidRoutedParamsHaveValidInternalError(t *testing.T) {
	d, s, _ := reviewSession(t)
	item := &entry{row: row{SessionID: "worker@local"}, attachment: s}
	d.directory.entries[item.row.SessionID], s.identity = item, item
	reply := make(chan answer, 1)
	s.issue(routedRequest{method: "turn.status", params: map[string]any{"session_id": ""}, reply: reply})
	value := <-reply
	if value.code != protocol.Internal || value.data != "internal" {
		t.Fatalf("invalid own request = %#v", value)
	}
}

func TestOwnErrorEncodingLogsOnce(t *testing.T) {
	if os.Getenv("SESSIONBUS_TEST_ERROR_LOG_CHILD") == "1" {
		for range 3 {
			func() {
				defer func() { _ = recover() }()
				_ = rpcReply("session.list", answer{code: protocol.Internal})
			}()
		}
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestOwnErrorEncodingLogsOnce$")
	cmd.Env = append(os.Environ(), "SESSIONBUS_TEST_ERROR_LOG_CHILD=1")
	raw, err := cmd.CombinedOutput()
	if err != nil || strings.Count(string(raw), "cannot encode daemon RPC error") != 1 || !strings.Contains(string(raw), "code=-32603;") {
		t.Fatalf("bounded real encoding diagnostic = %s, %v", raw, err)
	}
}
