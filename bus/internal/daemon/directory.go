// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"maps"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type entry struct {
	traceMode      string
	traceVersion   string
	lifetime       *ownership
	parent         *ownership
	row            row
	peer           bool
	info           map[string]any
	declaredGroups []string
	claimed        bool
	running        bool
	attachment     *session
	done           chan struct{}
}

type directory struct {
	remoteOwners map[string]*ownership
	daemon       *Daemon
	mu           sync.Mutex
	closing      bool
	entries      map[string]*entry
	connected    map[string]*entry
	tokens       map[string]*launch
}

type selected struct {
	label     string
	item      *entry
	summary   protocol.SessionSummary
	code      int
	ambiguous bool
}

func newDirectory(daemon *Daemon, rows []row) *directory {
	d := &directory{daemon: daemon, entries: map[string]*entry{}, connected: map[string]*entry{}, tokens: map[string]*launch{}, remoteOwners: map[string]*ownership{}}
	for _, value := range rows {
		item := &entry{row: cloneRow(value), done: closedChannel()}
		d.entries[value.SessionID] = item
	}
	return d
}

func (d *directory) installPeer(owner *session, hello *protocol.PeerHello, host string) (current *entry, displaced *session, ended *entry, ok bool) {
	id, name := qualify(hello.SessionID, host), qualify(hello.Name, host)
	d.mu.Lock()
	defer d.mu.Unlock()
	old := owner.identity
	if old != nil && old.attachment != owner {
		return nil, nil, nil, false
	}
	if old != nil && old.row.SessionID == id {
		if old.row.Product != hello.Product || !slices.Equal(old.declaredGroups, hello.Groups) {
			return nil, nil, nil, false
		}
		old.row.Name, old.info = name, maps.Clone(hello.Info)
		return old, nil, nil, true
	}
	if found := d.entries[id]; found != nil && !found.peer {
		return nil, nil, nil, false
	}
	item := &entry{lifetime: &ownership{id: id, token: randomID("owner"), destinations: map[string]bool{}}, peer: true, declaredGroups: append([]string(nil), hello.Groups...), attachment: owner, done: make(chan struct{})}
	item.row = row{SessionID: id, Product: hello.Product, Name: name}
	item.row.Groups = orderedPeerGroups(hello.Groups, privateGroup(item))
	item.info = maps.Clone(hello.Info)
	if old != nil {
		delete(d.entries, old.row.SessionID)
		d.end(old)
		ended = old
	}
	if found := d.entries[id]; found != nil {
		displaced = found.attachment
		item.lifetime = found.lifetime
		found.lifetime = nil
		d.end(found)
	}
	d.entries[id] = item
	d.addConnected(item)
	return item, displaced, ended, true
}

func (d *directory) current(item *entry, owner *session) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return item != nil && d.entries[item.row.SessionID] == item && item.attachment == owner
}

func (d *directory) finishRun(item *entry, owner *session) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if item != nil && item.attachment == owner {
		item.running = false
	}
}

func (d *directory) admit(item *entry, owner *session, method string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	if item.attachment != owner {
		return protocol.NotConnected
	}
	if item.claimed && method != "message.deliver" {
		return protocol.Busy
	}
	if method == "turn.execute" && item.running {
		return protocol.Busy
	}
	if method == "turn.interrupt" && !item.running {
		return protocol.NotRunning
	}
	return 0
}

func (d *directory) admitted(item *entry, method string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if method == "turn.execute" {
		item.running = true
	}
	if method == "session.close" {
		item.claimed = true
	}
}

func (d *directory) detach(item *entry, owner *session) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if item == nil || item.attachment != owner {
		return
	}
	d.end(item)
	if item.peer {
		delete(d.entries, item.row.SessionID)
	} else {
		item.claimed = true
	}
}

func (d *directory) offline(item *entry, owner *session, forget bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if item.attachment == owner {
		d.removeConnected(item)
		item.attachment, item.running = nil, false
	}
	item.claimed = false
	if forget && d.entries[item.row.SessionID] == item {
		delete(d.entries, item.row.SessionID)
	}
}

func (d *directory) reserveFresh(value row, start *launch) (*entry, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if start.parent == nil || start.parent.ended {
		return nil, protocol.NotConnected
	}
	if code := d.addLaunch(start); code != 0 {
		return nil, code
	}
	item := &entry{row: cloneRow(value), parent: start.parent, claimed: true, done: make(chan struct{})}
	start.entry = item
	return item, 0
}

func (d *directory) reserveResume(id string, groups []string, start *launch) (*entry, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if start.parent == nil || start.parent.ended {
		return nil, protocol.NotConnected
	}
	previous := d.entries[id]
	if previous == nil || previous.peer || !shares(groups, previous.row.Groups) {
		return nil, protocol.UnknownSession
	}
	if previous.claimed {
		return nil, protocol.Busy
	}
	if previous.attachment != nil {
		return nil, protocol.AlreadyConnected
	}
	policy, err := normalizePolicy(start.input, previous.row.Policy, start.parent.id)
	if err != nil {
		return nil, protocol.UnsupportedOpen
	}
	if code := d.addLaunch(start); code != 0 {
		return nil, code
	}
	item := &entry{row: cloneRow(previous.row), parent: start.parent, claimed: true, done: make(chan struct{})}
	item.row.Policy = policy
	start.previous = previous
	d.entries[id], start.entry, start.product = item, item, item.row.Product
	return item, 0
}

func (d *directory) reserveDescribe(start *launch) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.addLaunch(start)
}

func (d *directory) addLaunch(start *launch) int {
	if d.closing {
		return protocol.Internal
	}
	d.tokens[start.token] = start
	d.daemon.group.Add(1)
	return 0
}

func (d *directory) claimWorker(owner *session, token, product string) (*launch, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	start := d.tokens[token]
	if start == nil || start.product != product || start.owner != nil {
		return nil, false
	}
	delete(d.tokens, token)
	start.owner = owner
	return start, true
}

func (d *directory) reserveID(item *entry, id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !item.claimed || d.entries[id] != nil {
		return false
	}
	item.row.SessionID = id
	d.entries[id] = item
	return true
}

func (d *directory) publish(start *launch, owner *session, createdAt time.Time) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	item := start.entry
	if d.closing || d.entries[item.row.SessionID] != item || !item.claimed || item.attachment != nil || item.parent != nil && item.parent.ended && !item.row.Policy.Persistent {
		return false
	}
	item.claimed = false
	item.attachment = owner
	d.addConnected(item)
	item.lifetime = &ownership{id: item.row.SessionID, token: randomID("owner"), destinations: map[string]bool{}}
	if start.input != nil && item.parent != nil && !item.parent.ended {
		item.traceMode = start.input.Trace
		item.traceVersion = randomID("policy")
	}
	if !createdAt.IsZero() {
		item.row.CreatedAt = createdAt
	}
	return true
}

func (d *directory) revokeLaunch(start *launch) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if start.owner != nil || d.tokens[start.token] != start {
		return false
	}
	delete(d.tokens, start.token)
	return true
}

// Project committed live trace state into this parent's response only. The
// stored policy, worker-open policy and roster projections remain unchanged.
func (d *directory) spawnResult(start *launch, owner *session) *protocol.LaneSpawnResult {
	d.mu.Lock()
	defer d.mu.Unlock()
	item := start.entry
	result := &protocol.LaneSpawnResult{SessionID: item.row.SessionID, Policy: cloneRow(item.row).Policy}
	parent := start.parent
	if result.Policy == nil || d.closing || d.entries[item.row.SessionID] != item || item.attachment != owner || item.claimed || parent == nil || item.parent != parent || parent.ended {
		return result
	}
	if parent.host == "" {
		current := d.entries[parent.id]
		if current == nil || current.lifetime != parent || current.attachment == nil {
			return result
		}
	}
	result.Policy.Trace = item.traceMode
	if result.Policy.Trace == "" {
		result.Policy.Trace = "off"
	}
	return result
}

func (d *directory) releaseLaunch(start *launch) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.tokens[start.token] == start {
		delete(d.tokens, start.token)
	}
	item := start.entry
	if item == nil || !item.claimed {
		return
	}
	item.claimed = false
	d.end(item)
	if start.previous != nil {
		d.entries[item.row.SessionID] = start.previous
	}
	if !item.peer && item.row.CreatedAt.IsZero() {
		if d.entries[item.row.SessionID] == item {
			delete(d.entries, item.row.SessionID)
		}
	}
}

// Target commands use only connected entries. Identity-filtered list and
// forget are record-side; resume and trace have their existing ID-only paths.
func (d *directory) candidatesLocked(method string, params any) map[string]*entry {
	switch method {
	case "session.list":
		if params.(*protocol.SessionListRequest).SessionID != "" {
			return d.entries
		}
	case "session.close":
		if params.(*protocol.SessionCloseRequest).Forget {
			return d.entries
		}
	case "message.send", "turn.run", "turn.start", "turn.status", "turn.wait", "turn.ack", "turn.interrupt":
		// Active-target commands, including idle connected workers.
	}
	return d.connected
}

func matchingEntries(entries map[string]*entry, canonical string, groups []string) []*entry {
	if item := entries[canonical]; visibleTo(item, groups) {
		return []*entry{item}
	}
	var found []*entry
	for _, item := range entries {
		if item.row.Name != "" && item.row.Name == canonical && visibleTo(item, groups) {
			found = append(found, item)
		}
	}
	return found
}

func visibleTo(item *entry, groups []string) bool {
	return item != nil && (item.peer || !item.row.CreatedAt.IsZero()) && shares(groups, item.row.Groups)
}

func (d *directory) selectEntries(groups, labels []string, group, method string, params any, omit *entry, request *routedRequest) ([]selected, int) {
	d.mu.Lock()
	var offlineForget *entry
	entries := d.candidatesLocked(method, params)
	result := make([]selected, 0, len(entries))
	if labels == nil {
		for _, item := range entries {
			if !visibleTo(item, groups) || group != "" && (item == omit || !slices.Contains(item.row.Groups, group)) {
				continue
			}
			result = append(result, selected{label: item.row.SessionID, item: item, summary: summarize(item)})
		}
		d.mu.Unlock()
		sort.Slice(result, func(left, right int) bool { return result[left].summary.SessionID < result[right].summary.SessionID })
		return result, 0
	}
	valid := validIDPart
	if method == "message.send" {
		valid = validNamePart
	}
	for _, label := range labels {
		canonical, code := canonical(label, d.daemon.host, valid)
		if code == protocol.InvalidFrame {
			d.mu.Unlock()
			return nil, code
		}
		matches := matchingEntries(entries, canonical, groups)
		if method == "session.list" && len(matches) != 0 {
			for _, item := range matches {
				result = append(result, selected{label: label, item: item, code: code, summary: summarize(item)})
			}
			continue
		}
		value := selected{label: label, code: code, ambiguous: len(matches) > 1}
		if len(matches) == 1 {
			value.item, value.summary = matches[0], summarize(matches[0])
		}
		result = append(result, value)
	}
	if request != nil && result[0].code == 0 && result[0].item != nil && !result[0].ambiguous {
		item := result[0].item
		if request.method == "session.close" && !item.peer && item.attachment == nil && !item.claimed {
			// Only record-side forget can select an offline lane. Claim its
			// exact row before dropping the mutex so resume or another cleanup
			// cannot race the row deletion below.
			item.claimed = true
			offlineForget = item
		} else {
			result[0].code = d.routeLocked(item, request.method, *request)
		}
	}
	d.mu.Unlock()
	if offlineForget != nil {
		d.finishOfflineForget(offlineForget, *request)
	}
	if method == "session.list" {
		sort.Slice(result, func(left, right int) bool { return result[left].summary.SessionID < result[right].summary.SessionID })
	}
	return result, 0
}

func (d *directory) finishOfflineForget(item *entry, request routedRequest) {
	err := d.daemon.table.delete(item.row.SessionID)

	completed := false
	d.mu.Lock()
	if d.entries[item.row.SessionID] == item && item.attachment == nil && item.claimed {
		completed = true
		item.claimed = false
		if err == nil {
			delete(d.entries, item.row.SessionID)
		}
	}
	d.mu.Unlock()

	if !completed {
		request.reply <- answer{code: protocol.Internal, data: "offline close claim lost"}
		return
	}
	if err != nil {
		request.reply <- answer{code: protocol.Internal, data: err.Error()}
		return
	}
	request.reply <- answer{value: struct{}{}}
}

func summarize(item *entry) protocol.SessionSummary {
	kind := "lane"
	if item.peer {
		kind = "peer"
	}
	return protocol.SessionSummary{SessionID: item.row.SessionID, Kind: kind, Product: item.row.Product, Name: item.row.Name,
		Groups: append([]string(nil), item.row.Groups...), Connected: item.attachment != nil, Running: item.running, Info: maps.Clone(item.info), Policy: cloneRow(item.row).Policy}
}

func (d *directory) route(item *entry, method string, request routedRequest) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.routeLocked(item, method, request)
}

func (d *directory) routeLocked(item *entry, method string, request routedRequest) int {
	if item == nil || d.entries[item.row.SessionID] != item {
		return protocol.NotConnected
	}
	if item.claimed && method != "message.deliver" {
		return protocol.Busy
	}
	if request.traceCopy && (item.lifetime == nil || item.lifetime.ended || item.lifetime.token != request.traceLifetime) {
		return protocol.NotConnected
	}
	if item.attachment == nil {
		return protocol.NotConnected
	}
	if item.peer && method != "message.deliver" {
		return protocol.UnknownSession
	}
	request.destination = item
	if item.attachment.wire.Post(request) {
		return 0
	}
	select {
	case <-item.attachment.wire.Done():
		return protocol.NotConnected
	default:
		return protocol.Busy
	}
}

func (d *directory) end(item *entry) {
	d.removeConnected(item)
	item.traceMode = ""
	item.traceVersion = ""
	d.endOwner(item.lifetime)
	item.attachment, item.running = nil, false
	close(item.done)
}

// Called only at attachment transitions while d.mu is held. A stale owner
// ending cannot remove a replacement entry with the same ID.
func (d *directory) addConnected(item *entry) {
	d.connected[item.row.SessionID] = item
}

func (d *directory) removeConnected(item *entry) {
	if d.connected[item.row.SessionID] == item {
		delete(d.connected, item.row.SessionID)
	}
}

func closedChannel() chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
