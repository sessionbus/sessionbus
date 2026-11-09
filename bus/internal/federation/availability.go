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
	return ok && (connection.ConnectionState().NegotiatedProtocol == AvailabilityALPN || connection.ConnectionState().NegotiatedProtocol == PolicyALPN)
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
	hosts := LegacyHostProducts(listed.Hosts)
	if len(hosts) != len(listed.Hosts) {
		listed.Hosts = hosts
		reply.Result, _ = protocol.EncodeResult(method, listed)
	}
	return reply
}

// LegacyHostProducts omits silent-leg stubs without changing confirmed hosts
// or the input slice shared with another recipient.
func LegacyHostProducts(hosts []protocol.HostProducts) []protocol.HostProducts {
	for i, host := range hosts {
		if host.Availability == "not_responding" {
			projected := make([]protocol.HostProducts, 0, len(hosts)-1)
			projected = append(projected, hosts[:i]...)
			for _, remaining := range hosts[i+1:] {
				if remaining.Availability != "not_responding" {
					projected = append(projected, remaining)
				}
			}
			return projected
		}
	}
	return hosts
}

func unavailableList(host string, enabled bool) Reply {
	listed := protocol.SessionListResult{Sessions: []protocol.SessionSummary{}}
	if enabled {
		listed.Hosts = []protocol.HostProducts{{Host: host, Products: []string{}, Availability: "not_responding"}}
	}
	raw, _ := protocol.EncodeResult("session.list", listed)
	return Reply{Result: raw, Unavailable: true}
}
