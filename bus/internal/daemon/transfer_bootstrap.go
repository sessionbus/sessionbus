// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"encoding/json"
	"maps"

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

// Transport negotiation is not product metadata in the public directory. Keep
// the original hello intact for its conditional capability response.
func peerPublicInfo(info map[string]any) map[string]any {
	public := maps.Clone(info)
	delete(public, protocol.TransferInfoKey)
	return public
}
