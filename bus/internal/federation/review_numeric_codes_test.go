// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestFallbackLoggingUsesProtocolNumericCodeSemantics(t *testing.T) {
	if os.Getenv("SESSIONBUS_TEST_NUMERIC_FALLBACK_CHILD") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestFallbackLoggingUsesProtocolNumericCodeSemantics$", "-test.v")
		cmd.Env = append(os.Environ(), "SESSIONBUS_TEST_NUMERIC_FALLBACK_CHILD=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("actual numeric protocol/serializer control: %v\n%s", err, out)
		}
		return
	}
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(previous)
	var schema map[string]any
	if err := json.Unmarshal(protocol.SessionSchema, &schema); err != nil {
		t.Fatal(err)
	}
	values := schema["$defs"].(map[string]any)["RPCError"].(map[string]any)["properties"].(map[string]any)["code"].(map[string]any)["enum"].([]any)
	codes := map[int]bool{}
	for _, value := range values {
		code := int(value.(float64))
		codes[code] = true
		var data any
		if code == protocol.Internal {
			data = "internal"
		} else if code == protocol.SpawnFailed {
			data = struct {
				StderrTail []string `json:"stderr_tail"`
			}{[]string{}}
		}
		body, err := protocol.ErrorBytes(1, code, data)
		if err != nil {
			t.Fatal(err)
		}
		frame, err := protocol.DecodeFrame(body)
		if err != nil {
			t.Fatal(err)
		}
		for _, numeric := range []string{strconv.Itoa(code) + ".0", fmt.Sprintf("%de0", code), strconv.Itoa(code), fmt.Sprintf("%e", float64(code))} {
			raw := fmt.Sprintf(`{"code":%s,"message":%q`, numeric, frame.Error.Message)
			valid := raw
			if data != nil {
				valid += `,"data":` + string(frame.Error.Data)
			}
			valid += `}`
			parsed, err := protocol.DecodeError([]byte(valid))
			if err != nil || parsed.Code != code {
				t.Fatalf("protocol numeric authority %s: %#v %v", valid, parsed, err)
			}
			selected := Reply{Error: &protocol.RPCError{Code: protocol.Busy}, ErrorRaw: []byte(valid)}
			wire, err := ResponseBytes(7, selected)
			want := []byte(`{"jsonrpc":"2.0","id":7,"error":` + valid + "}\n")
			if err != nil || !bytes.Equal(wire, want) {
				t.Fatal("valid selected raw representation changed", string(wire), err)
			}
			selected.ErrorRaw = []byte(raw + `,"data":false}`)
			wire, err = ResponseBytes(7, selected)
			fallback, decodeErr := protocol.DecodeFrame(wire)
			if err != nil || decodeErr != nil || fallback.Error == nil || fallback.Error.Code != protocol.Internal || string(fallback.Error.Data) != `"internal"` {
				t.Fatal("fallback bytes/data changed", string(wire), err, decodeErr)
			}
			if _, ok := errorEncodingLog.Load(code); !ok {
				t.Errorf("supported numeric %s used the wrong logging bucket", numeric)
			}
		}
		if !strings.Contains(logs.String(), "code="+strconv.Itoa(code)+".0;") {
			t.Errorf("first selected representation lost for %d: %s", code, logs.String())
		}
	}
	if strings.Count(logs.String(), "cannot encode daemon RPC error") != len(codes) {
		t.Error("equivalent or distinct supported logging count", logs.String())
	}
	if _, found := errorEncodingLog.Load(0); found {
		t.Error("supported representations polluted the unknown bucket")
	}
	// Compare rounding boundaries with the actual protocol's enum-first path.
	for _, numeric := range []string{"-32003.000000000000001", "-32003.0000001", "-3.2003e4", "-320030e-1"} {
		valid := []byte(fmt.Sprintf(`{"code":%s,"message":"busy"}`, numeric))
		parsed, decodeErr := protocol.DecodeError(valid)
		key := 0
		if decodeErr == nil {
			key = parsed.Code
		}
		// A fresh per-case state makes an accidental alias visible, without
		// depending on prior supported-code entries.
		errorEncodingLog.Clear()
		_, err := ResponseBytes(9, Reply{ErrorRaw: []byte(fmt.Sprintf(`{"code":%s,"message":"busy","data":false}`, numeric))})
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		errorEncodingLog.Range(func(actual, _ any) bool {
			count++
			if actual != key {
				t.Errorf("numeric rounding %s: key %v, protocol requires %d", numeric, actual, key)
			}
			return true
		})
		if count != 1 {
			t.Fatal("rounding bucket count", count)
		}
	}
	errorEncodingLog.Clear()
	logs.Reset()
	unknowns := []string{"987654321987654321987654321", "1.5", "-1e99", "1e99", "1e999", "-1e999", "9223372036854775808", "-9223372036854775809", "2147483648", "-2147483649", "null", "false", `"SECRET_PAYLOAD"`}
	for _, numeric := range unknowns {
		_, _ = ResponseBytes(11, Reply{Error: &protocol.RPCError{Code: protocol.Busy}, ErrorRaw: []byte(fmt.Sprintf(`{"code":%s,"message":"invalid","data":"SECRET_PAYLOAD"}`, numeric))})
	}
	count := 0
	errorEncodingLog.Range(func(key, _ any) bool {
		count++
		if key != 0 {
			t.Error("extreme/invalid code escaped unknown bucket", key)
		}
		return true
	})
	if count != 1 || strings.Count(logs.String(), "cannot encode daemon RPC error") != 1 || !strings.Contains(logs.String(), "code="+unknowns[0]+";") || strings.Contains(logs.String(), "SECRET_PAYLOAD") {
		t.Fatal("unknown domain/first representation/payload changed", count, logs.String())
	}
	if len(codes) != len(values) {
		t.Fatal("duplicate schema codes")
	}
}
