// SPDX-License-Identifier: GPL-3.0-only

package federation

import "slices"

type policyTransfer struct {
	policy                           *Policy
	sourceHost, targetHost, sourceID string
	relations                        []*policyRelation
}

func (p *policyTransfer) allows(source, target TransferBound) bool {
	if p == nil {
		return true
	}
	if source.Identity.SessionID != p.sourceID || !ownedPart(source.Identity.SessionID, p.sourceHost, false) || !ownedPart(target.Identity.SessionID, p.targetHost, false) {
		return false
	}
	for _, group := range source.Identity.Groups {
		if p.policy.groupAllowed(p.sourceHost, p.targetHost, group) && slices.Contains(target.Identity.Groups, group) {
			return true
		}
	}
	return snapshotCounterpart(p.relations, source.Identity.SessionID, target.Identity.SessionID)
}
