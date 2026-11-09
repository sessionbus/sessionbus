// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"crypto/rand"
	"encoding/hex"
)

// policyCaller applies only a groups restriction. Lane-only and unused rules
// return the original envelope without allocation or identity normalization.
func (s *hubState) policyCaller(policy *Policy, source, destination string, from Caller, relation, traceCopy bool) (Caller, bool, error) {
	if !policy.groupsRestricted(source, destination) {
		return from, true, nil
	}
	named := policy.namedGroups(source, destination, from.Groups)
	if relation {
		private, groups, verified := s.relations.verified(from)
		if traceCopy {
			private, groups, verified = from.PrivateGroup, []string{from.PrivateGroup}, true
		}
		if !verified {
			return Caller{}, false, nil
		}
		from.PrivateGroup = private
		from.Groups = appendPolicyGroups(named, groups)
		return from, true, nil
	}
	if len(named) == 0 {
		return Caller{}, false, nil
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return Caller{}, false, err
	}
	from.PrivateGroup = "session:" + from.SessionID + "#" + hex.EncodeToString(nonce[:])
	from.Groups = append(named, from.PrivateGroup)
	return from, true, nil
}

func appendPolicyGroups(named, private []string) []string {
	for _, group := range private {
		present := false
		for _, old := range named {
			present = present || old == group
		}
		if !present {
			named = append(named, group)
		}
	}
	return named
}
