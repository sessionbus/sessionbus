// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"context"
	"strings"
	"time"

	"github.com/antst/sessionbus/bus/internal/roster"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

const policyRelationsPerPair = protocol.MaxOperations

// These maps and every mutation of an admission belong to the registry. Link
// owners carry immutable admission pointers and report authoritative results.
type policyRelations struct {
	entries map[string]*policyRelation
	pending map[*policyAdmission]bool
	charges map[policyPair]int
}

type policyPair struct{ source, destination string }

type policyRelation struct {
	lane, creator, private, parentPrivate string
	recorded                              time.Time
	oneWay                                bool
}

type policyAdmissionKind uint8

const (
	policyFresh policyAdmissionKind = iota + 1
	policyResume
	policyResumeAccess
	policyForget
)

type policyAdmission struct {
	ctx                        context.Context
	registry                   chan<- registryEvent
	kind                       policyAdmissionKind
	destination                *hostLink
	creator, private, name, id string
	entry                      *policyRelation
	reply                      chan Reply
}

type policyCompletion struct {
	admission *policyAdmission
	reply     Reply
	deliver   bool
}

func (a *policyAdmission) report(reply Reply, deliver bool) {
	postRegistry(a.ctx, a.registry, registryEvent{kind: registryPolicyComplete, completion: &policyCompletion{admission: a, reply: reply, deliver: deliver}})
}

func newPolicyRelations() *policyRelations {
	return &policyRelations{entries: map[string]*policyRelation{}, pending: map[*policyAdmission]bool{}, charges: map[policyPair]int{}}
}

func (r *policyRelations) admit(a *policyAdmission) bool {
	if a.kind == policyFresh || a.kind == policyResumeAccess {
		source, err := suffix(a.creator)
		if err != nil || a.destination == nil || !validHost(a.destination.host) {
			return false
		}
		pair := policyPair{source, a.destination.host}
		if r.charges[pair] >= policyRelationsPerPair {
			return false
		}
		r.charges[policyPair{policySourceHost(a.creator), a.destination.host}]++
	}
	r.pending[a] = true
	return true
}

func (r *policyRelations) complete(a *policyAdmission, reply Reply, current bool) (Reply, bool) {
	if !r.pending[a] {
		return Reply{}, false
	}
	delete(r.pending, a)
	if a.kind == policyFresh || a.kind == policyResumeAccess {
		r.charges[policyPair{policySourceHost(a.creator), a.destination.host}]--
	}
	if reply.Error != nil {
		return reply, true
	}
	if !current {
		return errorReply(protocol.ForwardLost, nil), true
	}
	switch a.kind {
	case policyFresh, policyResume, policyResumeAccess:
		decoded, err := protocol.DecodeResult("lane.spawn", reply.Result)
		if err != nil {
			return errorReply(protocol.ForwardLost, nil), true
		}
		id := decoded.(*protocol.LaneSpawnResult).SessionID
		if !ownedPart(id, a.destination.host, false) || a.kind != policyFresh && id != a.id {
			return errorReply(protocol.ForwardLost, nil), true
		}
		if a.kind == policyFresh || a.kind == policyResumeAccess {
			if r.entries[id] != nil {
				return errorReply(protocol.ForwardLost, nil), true
			}
			entry := &policyRelation{lane: id, creator: a.creator, parentPrivate: a.private, recorded: time.Now(), oneWay: a.kind == policyResumeAccess}
			if !entry.oneWay {
				entry.private = a.private + "/" + a.name
			}
			r.entries[id] = entry
			r.charges[policyPair{policySourceHost(a.creator), a.destination.host}]++
		}
	case policyForget:
		if a.entry != nil && r.entries[a.id] == a.entry {
			r.remove(a.entry)
		}
	}
	return reply, true
}

func (r *policyRelations) remove(entry *policyRelation) {
	if r.entries[entry.lane] == entry {
		delete(r.entries, entry.lane)
		host, _ := suffix(entry.lane)
		r.charges[policyPair{policySourceHost(entry.creator), host}]--
	}
}

func (r *policyRelations) counterpart(first, second string) bool {
	if r == nil {
		return false
	}
	return r.entries[first] != nil && !r.entries[first].oneWay && r.entries[first].creator == second || r.entries[second] != nil && !r.entries[second].oneWay && r.entries[second].creator == first
}

// targeted includes reconstructed access only in its proven direction. It is
// deliberately separate from symmetric counterparts used for transfers.
func (r *policyRelations) targeted(caller, target string) bool {
	return r.counterpart(caller, target) || r.child(caller, target)
}

func (r *policyRelations) child(parent, child string) bool {
	return r != nil && r.entries[child] != nil && r.entries[child].creator == parent
}

func (r *policyRelations) verified(caller Caller) (string, []string, bool) {
	index := strings.LastIndexByte(caller.SessionID, '@')
	if index > 0 && !strings.ContainsAny(caller.SessionID[:index], "/@") && caller.PrivateGroup == "session:"+caller.SessionID {
		return caller.PrivateGroup, []string{caller.PrivateGroup}, true
	}
	if r != nil {
		if entry := r.entries[caller.SessionID]; entry != nil && !entry.oneWay && caller.PrivateGroup == entry.private {
			return entry.private, []string{entry.parentPrivate, entry.private}, true
		}
	}
	return "", nil, false
}

func (r *policyRelations) traceParent(sourceHost, target, private string) bool {
	if r == nil {
		return false
	}
	for _, entry := range r.entries {
		host, _ := suffix(entry.lane)
		if !entry.oneWay && host == sourceHost && entry.creator == target && entry.parentPrivate == private {
			return true
		}
	}
	return false
}

func (r *policyRelations) prune(host string, selected time.Time, result roster.Host) {
	if r == nil || selected.IsZero() || result.Host != host || result.Error != "" || result.Sessions == nil {
		return
	}
	present := make(map[string]bool, len(result.Sessions))
	for _, row := range result.Sessions {
		present[row.SessionID] = true
	}
	for _, entry := range r.entries {
		entryHost, _ := suffix(entry.lane)
		if entryHost == host && selected.After(entry.recorded) && !present[entry.lane] {
			r.remove(entry)
		}
	}
}

// Called only after existing transfer pending admission. Retain relevant
// immutable references, not a deep table/group copy or a destination-row cache.
func (r *policyRelations) transferSnapshot(source, destinationHost string) []*policyRelation {
	if r == nil {
		return nil
	}
	var result []*policyRelation
	for _, entry := range r.entries {
		if entry.oneWay {
			continue
		}
		other := ""
		if entry.creator == source {
			other = entry.lane
		} else if entry.lane == source {
			other = entry.creator
		}
		if host, err := suffix(other); err == nil && host == destinationHost {
			result = append(result, entry)
		}
	}
	return result
}

func snapshotCounterpart(entries []*policyRelation, first, second string) bool {
	for _, entry := range entries {
		if !entry.oneWay && (entry.lane == first && entry.creator == second || entry.lane == second && entry.creator == first) {
			return true
		}
	}
	return false
}

func policySourceHost(id string) string { host, _ := suffix(id); return host }

func (r *policyRelations) chargedDestination(host string) bool {
	if r != nil {
		for pair, count := range r.charges {
			if pair.destination == host && count != 0 {
				return true
			}
		}
	}
	return false
}
