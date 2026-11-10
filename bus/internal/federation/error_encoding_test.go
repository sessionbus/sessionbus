// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestOutgoingErrorSchemaAndRawPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reply Reply
	}{
		{"typed-forbidden-data", Reply{Error: &protocol.RPCError{Code: protocol.InvalidFrame, Message: "invalid_frame", Data: json.RawMessage(`"forbidden"`)}}},
		{"raw-forbidden-data", Reply{Error: &protocol.RPCError{Code: protocol.Busy, Message: "busy"}, ErrorRaw: json.RawMessage(`{"code":-32600,"message":"invalid_frame","data":"forbidden"}`)}},
		{"raw-only", Reply{ErrorRaw: json.RawMessage(`{"code":-32003,"message":"busy","data":false}`)}},
		{"raw-malformed", Reply{Error: &protocol.RPCError{Code: protocol.Busy, Message: "busy"}, ErrorRaw: json.RawMessage(`{`)}},
		{"raw-missing-data", Reply{Error: &protocol.RPCError{Code: protocol.Internal, Message: "internal"}, ErrorRaw: json.RawMessage(`{"code":-32603,"message":"internal"}`)}},
		{"typed-mismatched-message", Reply{Error: &protocol.RPCError{Code: protocol.Busy, Message: "internal"}}},
		{"typed-invalid-json", Reply{Error: &protocol.RPCError{Code: protocol.Internal, Message: "internal", Data: json.RawMessage(`{`)}}},
		{"oversized-valid-error", Reply{Error: &protocol.RPCError{Code: protocol.Internal, Message: "internal", Data: json.RawMessage(`"` + strings.Repeat("x", protocol.MaxFrameBytes) + `"`)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := tc.reply
			before.ErrorRaw = bytes.Clone(tc.reply.ErrorRaw)
			if tc.reply.Error != nil {
				v := *tc.reply.Error
				v.Data = bytes.Clone(v.Data)
				before.Error = &v
			}
			body, err := ResponseBytes(47, tc.reply)
			if err != nil {
				t.Fatal("own error failed to serialize", err)
			}
			frame, err := protocol.DecodeFrame(body)
			if err != nil || frame.ID != 47 || frame.Error == nil || frame.Error.Code != protocol.Internal || frame.Error.Message != "internal" || string(frame.Error.Data) != `"internal"` {
				t.Fatalf("fallback=%s error=%v", body, err)
			}
			if !reflect.DeepEqual(before, tc.reply) {
				t.Fatal("input mutated")
			}
			if tc.name != "oversized-valid-error" {
				withTrace := tc.reply
				withTrace.Trace = []TraceRecipient{}
				encoded, err := encodeTraceReply(withTrace)
				if err != nil {
					t.Fatal(err)
				}
				var envelope traceReplyEnvelope
				if err = json.Unmarshal(encoded, &envelope); err != nil {
					t.Fatal(err)
				}
				failure, err := protocol.DecodeError(envelope.Error)
				if err != nil || failure.Code != protocol.Internal || string(failure.Data) != `"internal"` {
					t.Fatalf("nested fallback=%s %v", encoded, err)
				}
			}
		})
	}
	raw := json.RawMessage(`{ "message":"internal", "data":"arbitrary permitted reason", "code":-32603 }`)
	original := bytes.Clone(raw)
	reply := Reply{Error: &protocol.RPCError{Code: protocol.Busy, Message: "busy"}, ErrorRaw: raw}
	got, err := ResponseBytes(9, reply)
	want := append(append([]byte(`{"jsonrpc":"2.0","id":9,"error":`), raw...), '}', '\n')
	if err != nil || !bytes.Equal(got, want) || !bytes.Equal(raw, original) {
		t.Fatalf("valid raw precedence/bytes changed: %s %v", got, err)
	}
	for _, value := range []Reply{errorReply(protocol.SpawnFailed, "native diagnostic"), errorReply(protocol.ForwardLost, nil), errorReply(protocol.PolicyDenied, nil)} {
		body, err := ResponseBytes(8, value)
		if err != nil {
			t.Fatal(err)
		}
		frame, err := protocol.DecodeFrame(body)
		if err != nil || frame.Error.Code != value.Error.Code || !bytes.Equal(frame.Error.Data, value.Error.Data) {
			t.Fatalf("valid error changed: %s %v", body, err)
		}
	}
}

func TestOutgoingErrorLogOnceAcrossSerializerEntries(t *testing.T) {
	if os.Getenv("SESSIONBUS_TEST_SHARED_ERROR_LOG_CHILD") == "1" {
		_ = errorReply(protocol.Internal, nil)
		_, _ = ResponseBytes(2, Reply{Error: &protocol.RPCError{Code: protocol.InvalidFrame, Message: "invalid_frame", Data: json.RawMessage(`"forbidden"`)}})
		_, _ = encodeTraceReply(Reply{ErrorRaw: json.RawMessage(`{"code":-32003,"message":"busy","data":false}`), Trace: []TraceRecipient{}})
		_ = errorReply(protocol.Busy, make(chan int))
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestOutgoingErrorLogOnceAcrossSerializerEntries$")
	cmd.Env = append(os.Environ(), "SESSIONBUS_TEST_SHARED_ERROR_LOG_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err != nil || strings.Count(string(out), "cannot encode daemon RPC error") != 1 {
		t.Fatalf("once log=%s error=%v", out, err)
	}
}
