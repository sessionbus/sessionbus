// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"encoding/json"
	"log"
	"strconv"
	"sync"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

var errorEncodingLog sync.Once

const fallbackError = `{"code":-32603,"message":"internal","data":"internal"}`

func fallbackErrorRaw(err error) json.RawMessage {
	errorEncodingLog.Do(func() { log.Printf("cannot encode daemon RPC error: %v", err) })
	return json.RawMessage(fallbackError)
}

// ErrorBytes is the shared typed-error serializer for daemon and hub replies.
// An error made by us must not invalidate the caller's connection or ownership.
func ErrorBytes(id int64, code int, data any) []byte {
	body, err := protocol.ErrorBytes(id, code, data)
	if err == nil {
		return body
	}
	raw := fallbackErrorRaw(err)
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
		return fallbackErrorRaw(err)
	}
	return raw
}

func errorResponseBytes(id int64, reply Reply) []byte {
	body, err := rawResponseBytes(id, `,"error":`, replyErrorRaw(reply))
	if err == nil {
		return body
	}
	body, _ = rawResponseBytes(id, `,"error":`, fallbackErrorRaw(err))
	return body
}
