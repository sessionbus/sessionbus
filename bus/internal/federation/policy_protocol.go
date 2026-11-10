// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"crypto/tls"
	"net"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

// PolicyALPN is a strict superset of preceding daemon-link capabilities.
const PolicyALPN = "sessionbus-policy/1"

func SupportsPolicy(fd net.Conn) bool {
	c, ok := fd.(*tls.Conn)
	return ok && c.ConnectionState().NegotiatedProtocol == PolicyALPN
}

// ProjectPolicy uses the captured declaration AND the hop's negotiated schema.
// ErrorRaw must never override a downgraded typed error.
func ProjectPolicy(method string, reply Reply, enabled bool) Reply {
	if enabled {
		return reply
	}
	if reply.Error != nil {
		if reply.Error.Code == protocol.PolicyDenied {
			projected := errorReply(protocol.UnknownSession, nil)
			reply.Error, reply.ErrorRaw = projected.Error, projected.ErrorRaw
		}
		return reply
	}
	if method != "message.send" {
		return reply
	}
	decoded, err := protocol.DecodeResult(method, reply.Result)
	if err != nil {
		return reply
	}
	result := decoded.(*protocol.MessageSendResult)
	if deliveries := LegacyPolicyDeliveries(result.Deliveries); deliveries != nil {
		result.Deliveries = deliveries
		reply.Result, _ = protocol.EncodeResult(method, result)
	}
	return reply
}

// Nil means no projection is necessary. Arbitrary native rejection reasons
// remain valid; only the newly introduced policy token is projected.
func LegacyPolicyDeliveries(input []protocol.MessageSendDelivery) []protocol.MessageSendDelivery {
	var output []protocol.MessageSendDelivery
	for i, delivery := range input {
		if delivery.Reason == protocol.PolicyReason {
			if output == nil {
				output = append([]protocol.MessageSendDelivery{}, input...)
			}
			output[i].Reason = "unknown_session"
		}
	}
	return output
}
