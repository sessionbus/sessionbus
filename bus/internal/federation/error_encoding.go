// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"bytes"
	"encoding/json"
	"log"
	"strconv"
	"sync"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

// Supported codes have one key each. All invalid/arbitrary codes share zero,
// so the fallback path cannot grow a map from attacker-controlled integers.
var errorEncodingLog sync.Map

const fallbackError = `{"code":-32603,"message":"internal","data":"internal"}`

func fallbackErrorRaw(code string) json.RawMessage {
	key := 0
	if numeric, err := strconv.Atoi(code); err == nil {
		var data any
		switch numeric {
		case protocol.Internal:
			data = "internal"
		case protocol.SpawnFailed:
			data = struct {
				StderrTail []string `json:"stderr_tail"`
			}{StderrTail: []string{}}
		}
		// The low-level serializer's schema is the finite supported-code
		// authority; it does not call this federation fallback.
		if _, err := protocol.ErrorBytes(1, numeric, data); err == nil {
			key = numeric
		}
	}
	if _, loaded := errorEncodingLog.LoadOrStore(key, struct{}{}); !loaded {
		// Never log the validator error: it can contain error-data payloads.
		log.Printf("cannot encode daemon RPC error: code=%s; using internal fallback", code)
	}
	return json.RawMessage(fallbackError)
}

func replyErrorCode(reply Reply) string {
	if len(reply.ErrorRaw) == 0 {
		if reply.Error != nil {
			return strconv.Itoa(reply.Error.Code)
		}
		return "unknown"
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(reply.ErrorRaw, &fields) == nil {
		decoder := json.NewDecoder(bytes.NewReader(fields["code"]))
		decoder.UseNumber()
		var value any
		if decoder.Decode(&value) == nil {
			if number, ok := value.(json.Number); ok {
				return string(number)
			}
		}
	}
	return "unknown"
}

// ErrorBytes is the shared typed-error serializer for daemon and hub replies.
// An error made by us must not invalidate the caller's connection or ownership.
func ErrorBytes(id int64, code int, data any) []byte {
	body, err := protocol.ErrorBytes(id, code, data)
	if err == nil {
		return body
	}
	raw := fallbackErrorRaw(strconv.Itoa(code))
	body = strconv.AppendInt([]byte(`{"jsonrpc":"2.0","id":`), id, 10)
	body = append(body, `,"error":`...)
	body = append(body, raw...)
	return append(body, '}', '\n')
}

func hasReplyError(reply Reply) bool { return reply.Error != nil || len(reply.ErrorRaw) != 0 }

// Raw bytes take precedence. Validate the actual bytes without changing valid
// representations or mutating the caller's original Reply.
func replyErrorRaw(reply Reply) json.RawMessage {
	raw := reply.ErrorRaw
	var err error
	if len(raw) == 0 {
		raw, err = json.Marshal(reply.Error)
	}
	if err == nil {
		_, err = protocol.DecodeError(raw)
	}
	if err != nil {
		return fallbackErrorRaw(replyErrorCode(reply))
	}
	return raw
}

func errorResponseBytes(id int64, reply Reply) []byte {
	body, err := rawResponseBytes(id, `,"error":`, replyErrorRaw(reply))
	if err == nil {
		return body
	}
	body, _ = rawResponseBytes(id, `,"error":`, fallbackErrorRaw(replyErrorCode(reply)))
	return body
}
