// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"crypto/tls"
	"net"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

const AvailabilityALPN = "sessionbus-availability/1"

func SupportsAvailability(fd net.Conn) bool {
	connection, ok := fd.(*tls.Conn)
	return ok && connection.ConnectionState().NegotiatedProtocol == AvailabilityALPN
}

// ProjectAvailability is a per-hop projection, using the captured request's
// opt-in. New-kit knowledge alone cannot enable an old intermediary's parser.
func ProjectAvailability(method string, reply Reply, enabled bool) Reply {
	if enabled || method != "session.list" || reply.Error != nil {
		return reply
	}
	decoded, err := protocol.DecodeResult(method, reply.Result)
	if err != nil {
		return reply // Ordinary frame validation remains authoritative.
	}
	listed := decoded.(*protocol.SessionListResult)
	changed := false
	for i := range listed.Hosts {
		changed = changed || listed.Hosts[i].Availability != ""
		listed.Hosts[i].Availability = ""
	}
	if changed {
		reply.Result, _ = protocol.EncodeResult(method, listed)
	}
	return reply
}

func unavailableList(host string, enabled bool) Reply {
	listed := protocol.SessionListResult{Sessions: []protocol.SessionSummary{}}
	if enabled {
		listed.Hosts = []protocol.HostProducts{{Host: host, Products: []string{}, Availability: "not_responding"}}
	}
	raw, _ := protocol.EncodeResult("session.list", listed)
	return Reply{Result: raw, Unavailable: true}
}
