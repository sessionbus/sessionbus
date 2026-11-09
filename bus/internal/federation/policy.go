// SPDX-License-Identifier: GPL-3.0-only

package federation

import "strings"

// Policy is immutable after loading. Nil dimensions mean unrestricted; a
// present empty map permits nothing for that dimension.
type Policy struct{ hosts map[string]hostPolicy }

type hostPolicy struct {
	groups, lanesOn, lanesTo map[string]bool
}

func (p *Policy) tracks(source, destination string) bool {
	if p == nil {
		return false
	}
	_, sourcePresent := p.hosts[source]
	_, destinationPresent := p.hosts[destination]
	return sourcePresent || destinationPresent
}

func (p *Policy) groupsRestricted(source, destination string) bool {
	return p != nil && (p.hosts[source].groups != nil || p.hosts[destination].groups != nil)
}

func permits(values map[string]bool, value string) bool { return values == nil || values[value] }

func (p *Policy) laneAllowed(source, destination string) bool {
	return p == nil || permits(p.hosts[source].lanesTo, destination) && permits(p.hosts[destination].lanesOn, source)
}

func (p *Policy) groupAllowed(source, destination, group string) bool {
	return !strings.HasPrefix(group, "session:") && (p == nil || permits(p.hosts[source].groups, group) && permits(p.hosts[destination].groups, group))
}

func (p *Policy) namedGroups(source, destination string, groups []string) []string {
	var result []string
	for _, group := range groups {
		if p.groupAllowed(source, destination, group) {
			result = append(result, group)
		}
	}
	return result
}

// returnedGroups preserves private identity groups but clips named groups.
// Whole-row operator visibility is a separate named-group test.
func (p *Policy) returnedGroups(source, destination string, groups []string) []string {
	if !p.groupsRestricted(source, destination) {
		return groups
	}
	result := make([]string, 0, len(groups))
	for _, group := range groups {
		if strings.HasPrefix(group, "session:") || p.groupAllowed(source, destination, group) {
			result = append(result, group)
		}
	}
	return result
}
