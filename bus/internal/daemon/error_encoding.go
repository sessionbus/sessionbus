// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"log"
	"strconv"
	"sync"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

var errorEncodingLog sync.Once

// A malformed error made by the daemon must not end the caller's ownership.
// The static fallback is independent of the value that failed to encode.
func errorBytes(id int64, code int, data any) []byte {
	body, err := protocol.ErrorBytes(id, code, data)
	if err == nil {
		return body
	}
	errorEncodingLog.Do(func() { log.Printf("cannot encode daemon RPC error (code %d): %v", code, err) })
	body = strconv.AppendInt([]byte(`{"jsonrpc":"2.0","id":`), id, 10)
	return append(body, []byte(",\"error\":{\"code\":-32603,\"message\":\"internal\",\"data\":\"internal\"}}\n")...)
}
