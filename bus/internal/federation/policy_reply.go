// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"github.com/antst/sessionbus/bus/internal/roster"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type policyReplyScope struct {
	policy         *Policy
	source, target string
}

func (p *policyReplyScope) project(method string, reply Reply) Reply {
	if p == nil || method != "session.list" || reply.Error != nil {
		return reply
	}
	decoded, err := protocol.DecodeResult(method, reply.Result)
	if err != nil {
		return reply // Existing frame validation remains authoritative.
	}
	listed := decoded.(*protocol.SessionListResult)
	changed := false
	for i := range listed.Sessions {
		groups := p.policy.returnedGroups(p.source, p.target, listed.Sessions[i].Groups)
		changed = changed || len(groups) != len(listed.Sessions[i].Groups)
		listed.Sessions[i].Groups = groups
	}
	if changed {
		reply.Result, _ = protocol.EncodeResult(method, listed)
	}
	return reply
}

func (p *Policy) projectRoster(source, destination string, host roster.Host) roster.Host {
	if !p.groupsRestricted(source, destination) {
		return host
	}
	rows := make([]roster.Row, 0, len(host.Sessions))
	for _, row := range host.Sessions {
		if len(p.namedGroups(source, destination, row.Groups)) == 0 {
			continue
		}
		row.Groups = p.returnedGroups(source, destination, row.Groups)
		rows = append(rows, row)
	}
	host.Sessions = rows
	return host
}
