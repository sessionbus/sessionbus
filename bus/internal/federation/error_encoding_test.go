// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
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
	for _, test := range []struct {
		code int
		data any
	}{
		{protocol.SpawnFailed, struct {
			StderrTail []string `json:"stderr_tail"`
		}{[]string{"native diagnostic"}}},
		{protocol.ForwardLost, nil}, {protocol.PolicyDenied, nil},
	} {
		body, err := ResponseBytes(8, errorReply(test.code, test.data))
		if err != nil {
			t.Fatal(err)
		}
		frame, err := protocol.DecodeFrame(body)
		var expected []byte
		if test.data != nil {
			expected, _ = json.Marshal(test.data)
		}
		if err != nil || frame.Error == nil || frame.Error.Code != test.code || !bytes.Equal(frame.Error.Data, expected) {
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
	if err != nil || strings.Count(string(out), "cannot encode daemon RPC error") != 3 {
		t.Fatalf("once log=%s error=%v", out, err)
	}
}

func TestOutgoingErrorLoggingHasFiniteCodeDomain(t *testing.T) {
	if os.Getenv("SESSIONBUS_TEST_FINITE_ERROR_LOG_CHILD") == "1" {
		var schema map[string]any
		if err := json.Unmarshal(protocol.SessionSchema, &schema); err != nil {
			t.Fatal(err)
		}
		values := schema["$defs"].(map[string]any)["RPCError"].(map[string]any)["properties"].(map[string]any)["code"].(map[string]any)["enum"].([]any)
		codes := make([]int, len(values))
		for index, value := range values {
			codes[index] = int(value.(float64))
		}

		if len(codes) == 0 {
			t.Fatal("missing schema code authority")
		}
		// The selected raw code must win over the conflicting typed Busy. The first
		// arbitrary value is retained exactly in the ONE shared unknown log bucket.
		_, _ = ResponseBytes(1, Reply{Error: &protocol.RPCError{Code: protocol.Busy}, ErrorRaw: []byte(`{"code":987654321987654321987654321,"message":"bad","data":"SECRET_PAYLOAD"}`)})
		var group sync.WaitGroup
		for _, code := range codes {
			for range 8 {
				group.Add(1)
				go func(code int) {
					defer group.Done()
					body := ErrorBytes(2, code, make(chan int))
					frame, err := protocol.DecodeFrame(body)
					if err != nil || frame.Error == nil || frame.Error.Code != protocol.Internal || string(frame.Error.Data) != `"internal"` {
						t.Error("fallback changed")
					}
				}(code)
			}
		}
		for code := 0; code < 256; code++ {
			_, _ = ResponseBytes(3, Reply{ErrorRaw: json.RawMessage(fmt.Sprintf(`{"code":%d,"message":"invalid","data":"SECRET_PAYLOAD"}`, code))})
		}
		for _, raw := range []string{`{"code":1.5}`, `{"code":-1e99}`, `{"code":null}`, `{"code":"SECRET_PAYLOAD"}`, `{}`} {
			_, _ = ResponseBytes(4, Reply{ErrorRaw: []byte(raw)})
		}
		group.Wait()
		count := 0
		errorEncodingLog.Range(func(key, value any) bool {
			count++
			numeric, ok := key.(int)
			if !ok {
				t.Error("non-integer log key")
			}
			if numeric != 0 {
				found := false
				for _, code := range codes {
					found = found || numeric == code
				}
				if !found {
					t.Error("arbitrary key", numeric)
				}
			}
			return true
		})
		if count != len(codes)+1 {
			t.Fatal("unbounded or missing log domain", count, len(codes))
		}
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestOutgoingErrorLoggingHasFiniteCodeDomain$")
	cmd.Env = append(os.Environ(), "SESSIONBUS_TEST_FINITE_ERROR_LOG_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err != nil || strings.Count(string(out), "cannot encode daemon RPC error") != 22 || !strings.Contains(string(out), "code=987654321987654321987654321;") || strings.Contains(string(out), "SECRET_PAYLOAD") {
		t.Fatalf("finite logging=%s error=%v", out, err)
	}
}
