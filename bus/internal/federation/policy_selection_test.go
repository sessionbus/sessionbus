// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"bufio"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/internal/roster"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type policyWriteBarrier struct {
	net.Conn
	entered chan struct{}
	once    sync.Once
}

func (c *policyWriteBarrier) Write(body []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	return c.Conn.Write(body)
}

func TestPolicyProbeSelectionStampPublishesBeforeReplyAndPruning(t *testing.T) {
	for _, mode := range []string{"observed", "queued-before-tracking", "selected-before-entry", "untracked", "incomplete"} {
		t.Run(mode, func(t *testing.T) {
			left, right := net.Pipe()
			barrier := &policyWriteBarrier{Conn: left, entered: make(chan struct{})}
			inbox := make(chan any, 8)
			var workers sync.WaitGroup
			wire := conn.Start(barrier, inbox, &workers)
			t.Cleanup(func() { wire.Close(); _ = right.Close(); workers.Wait() })
			registry := make(chan registryEvent, 1)
			link := &hostLink{host: "beta", ctx: context.Background(), wire: wire, registry: registry}
			if mode == "observed" || mode == "selected-before-entry" || mode == "incomplete" {
				link.liveness.observeRoster.Store(true)
			}
			body, _ := ResponseBytes(100, emptyReply())
			if !wire.Send(body) {
				t.Fatal("blocking control refused")
			}
			select {
			case <-barrier.entered:
			case <-time.After(time.Second):
				t.Fatal("writer did not reach blocking write")
			}
			queued, pending := map[*pendingCall]bool{}, map[int64]pendingCall{}
			if !queueCall(wire, queued, pendingCall{method: rosterMethod, probe: &link.liveness}, false, func(id int64) ([]byte, error) { return requestBytes(id, rosterMethod, struct{}{}) }) {
				t.Fatal("reserved probe refused")
			}
			for p := range queued {
				if !p.selectedAt.IsZero() {
					t.Fatal("probe timestamped at enqueue instead of writer selection")
				}
			}
			recorded := time.Now()
			if mode == "queued-before-tracking" {
				link.liveness.observeRoster.Store(true)
			}
			_ = right.SetDeadline(time.Now().Add(time.Second))
			reader := bufio.NewReader(right)
			if _, err := reader.ReadBytes('\n'); err != nil {
				t.Fatal(err)
			}
			request, err := reader.ReadBytes('\n')
			if err != nil {
				t.Fatal(err)
			}
			frame, err := protocol.DecodeFrame(request[:len(request)-1])
			if err != nil || frame.Method != rosterMethod || frame.ID != 1 {
				t.Fatal("invalid serialized own probe", frame, err)
			}
			select {
			case event := <-wire.Dispatched():
				installCall(event, queued, pending)
			case <-time.After(time.Second):
				t.Fatal("writer did not publish selection")
			}
			if mode == "selected-before-entry" {
				recorded = time.Now()
			}
			state := hubState{relations: newPolicyRelations(), links: map[string]*hostLink{"beta": link}}
			entry := &policyRelation{lane: "child@beta", creator: "parent@alpha", recorded: recorded}
			if mode != "untracked" {
				state.relations.entries[entry.lane], state.relations.charges[policyPair{"alpha", "beta"}] = entry, 1
			}
			host := roster.Host{Host: "beta", Products: []string{}, Sessions: []roster.Row{}}
			if mode == "incomplete" {
				host.Error = "host_roster_unavailable"
			}
			response := RosterReply(host)
			if _, err := decodeRosterReply(response.Result); response.Error != nil || err != nil {
				t.Fatal("invalid roster fixture", err)
			}
			body, _ = ResponseBytes(frame.ID, response)
			if _, err := right.Write(body); err != nil {
				t.Fatal(err)
			}
			select {
			case raw := <-inbox:
				value, ok := raw.(conn.Frame)
				if !ok {
					t.Fatal("unexpected reader event", raw)
				}
				last := int64(0)
				if err := link.frame(value, pending, &last); err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("actual reader did not publish reply")
			}
			if mode == "untracked" {
				if len(registry) != 0 {
					t.Fatal("unused policy added roster observation work")
				}
				return
			}
			event := <-registry
			if (mode == "observed" || mode == "incomplete") && !event.observation.selected.After(recorded) {
				t.Fatal("selection stamp was not published through Dispatched")
			}
			state.handle(context.Background(), registry, event)
			if mode == "incomplete" && (state.relations.entries[entry.lane] != entry || state.relations.charges[policyPair{"alpha", "beta"}] != 1 || !link.liveness.observeRoster.Load()) {
				t.Fatal("fresh incomplete observation changed the relation, charge or tracking")
			}
			if removed := state.relations.entries[entry.lane] == nil; removed != (mode == "observed") {
				t.Fatal("stale/unknown selection pruned, or fresh complete absence did not prune", mode, event.observation.selected, recorded)
			}
		})
	}
}
