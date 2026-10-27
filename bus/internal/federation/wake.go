// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"crypto/tls"
	"encoding/json"
	"net"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

// WakeALPN includes roster and trace, and preserves completion provenance so
// automatic notifications cannot start an unbounded chain of notifying runs.
const WakeALPN = "sessionbus-wake/1"

func SupportsWake(fd net.Conn) bool {
	connection, ok := fd.(*tls.Conn)
	return ok && supportsWakeProtocol(connection.ConnectionState().NegotiatedProtocol)
}

func validateCompletionRequest(request PublicRequest, fields map[string]json.RawMessage) error {
	raw, present := fields["completion"]
	if !present {
		// encoding/json also matches differently capitalized struct fields.
		// Such a decoded marker must not bypass the checks below.
		if request.Completion {
			return errFrame
		}
		return nil
	}
	var enabled bool
	if json.Unmarshal(raw, &enabled) != nil || !enabled || !request.Completion || request.Method != "message.send" || request.TraceCopy != nil {
		return errFrame
	}
	params, err := protocol.DecodeParams(request.Method, request.Params)
	if err != nil {
		return errFrame
	}
	message := params.(*protocol.MessageSendRequest)
	if message.Group != "" || (message.Target == "" && len(message.Targets) != 1) {
		return errFrame
	}
	return nil
}
