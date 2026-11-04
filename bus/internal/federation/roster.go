// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"context"
	"crypto/tls"
	"net"
	"sort"
	"time"

	"github.com/antst/sessionbus/bus/internal/roster"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

const RosterALPN = "sessionbus-roster/1"
const rosterMethod = "federation.roster"

type RosterCall struct{ Reply chan Reply }
type outgoingRoster struct{ reply chan Reply }

// An old hub/host negotiates no ALPN and must never receive the new method.
func SupportsRoster(fd net.Conn) bool {
	connection, ok := fd.(*tls.Conn)
	if !ok {
		return false
	}
	protocol := connection.ConnectionState().NegotiatedProtocol
	return supportsRosterProtocol(protocol)
}

func SupportsTrace(fd net.Conn) bool {
	connection, ok := fd.(*tls.Conn)
	return ok && supportsTraceProtocol(connection.ConnectionState().NegotiatedProtocol)
}

func IsRoster(call IncomingCall) bool { return call.Request.Method == rosterMethod }

func RosterReply(host roster.Host) Reply {
	raw, err := roster.Encode(roster.Remote{Hosts: []roster.Host{host}})
	if err != nil {
		return errorReply(protocol.ForwardLost, nil)
	}
	return Reply{Result: raw}
}

func decodeRosterReply(raw []byte) (roster.Remote, error) { return roster.DecodeRemote(raw) }

// The registry captures attachment identities once. Replies go back only to
// that origin attachment; ordinary group-filtered forwards are unchanged.
func (s *hubState) roster(ctx context.Context, inbox chan<- registryEvent, origin *hostLink, id int64) {
	if s.links[origin.host] != origin {
		return
	}
	select {
	case origin.rosterSlots <- struct{}{}:
	default:
		s.reply(originReply{origin: origin, id: id, value: errorReply(protocol.ForwardLost, nil)})
		return
	}
	names := []string{}
	for host := range s.links {
		if host != origin.host {
			names = append(names, host)
		}
	}
	sort.Strings(names)
	if len(names) > roster.MaxRows {
		<-origin.rosterSlots
		s.reply(originReply{origin: origin, id: id, value: errorReply(protocol.ForwardLost, nil)})
		return
	}
	type leg struct {
		host        string
		destination *hostLink
	}
	legs := make([]leg, 0, len(names))
	for _, host := range names {
		legs = append(legs, leg{host, s.links[host]})
	}
	s.group.Add(1)
	go func() {
		defer s.group.Done()
		defer func() { <-origin.rosterSlots }()
		deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		result := roster.Remote{Hosts: []roster.Host{}}
		retainedBytes := 0
		for _, leg := range legs {
			host := roster.Host{Host: leg.host, Products: []string{}, Sessions: []roster.Row{}, Error: "host_roster_unavailable"}
			answer := make(chan Reply, 1)
			if deadline.Err() != nil {
				answer <- errorReply(protocol.ForwardLost, nil)
			} else if !SupportsRoster(leg.destination.fd) {
				answer <- RosterReply(roster.Host{Host: leg.host, Products: []string{}, Sessions: []roster.Row{}, Error: "upgrade_host_for_roster"})
			} else if !leg.destination.wire.Post(outgoingRoster{answer}) {
				answer <- errorReply(protocol.ForwardLost, nil)
			}
			select {
			case value := <-answer:
				if value.Error == nil {
					decoded, err := decodeRosterReply(value.Result)
					if err == nil && len(decoded.Hosts) == 1 && decoded.Hosts[0].Host == leg.host {
						host = decoded.Hosts[0]
					}
				}
			case <-leg.destination.done:
			case <-deadline.Done():
			case <-origin.done:
				return
			}
			encoded, err := roster.Encode(host)
			retainedBytes += len(encoded) + 1
			if err != nil || retainedBytes > roster.MaxBytes-1024 {
				postRegistry(ctx, inbox, registryEvent{kind: registryReply, reply: &originReply{origin: origin, id: id, value: errorReply(protocol.ForwardLost, "roster_too_large")}})
				return
			}
			result.Hosts = append(result.Hosts, host)
		}
		raw, err := roster.Encode(result)
		value := Reply{Result: raw}
		if err != nil {
			value = errorReply(protocol.ForwardLost, nil)
		}
		postRegistry(ctx, inbox, registryEvent{kind: registryReply, reply: &originReply{origin: origin, id: id, value: value}})
	}()
}
