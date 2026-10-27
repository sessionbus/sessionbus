// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"encoding/json"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

// Opt-in asserts handler capability only. It grants neither product identity
// nor permission; authenticated attachment/generation remains authoritative.
func transferOptIn(info map[string]any) bool {
	switch v := info[protocol.TransferInfoKey].(type) {
	case float64:
		return v == 1
	case int:
		return v == 1
	case json.Number:
		return v == "1"
	}
	return false
}
