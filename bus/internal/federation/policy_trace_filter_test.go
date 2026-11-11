// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"reflect"
	"sync"
	"testing"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestPolicyTraceReplyPreservesCreatorAndFiltersOtherRecipients(t *testing.T) {
	for _, name := range []string{"creator", "one-way", "allowed", "denied", "stale", "local", "unrestricted"} {
		t.Run(name, func(t *testing.T) {
			local, remote := net.Pipe()
			inbox := make(chan any, 8)
			var group sync.WaitGroup
			wire := conn.Start(local, inbox, &group)
			defer func() {
				wire.Close()
				_ = remote.Close()
				for {
					if _, ok := (<-inbox).(conn.Closed); ok {
						break
					}
				}
				group.Wait()
			}()
			origin := &hostLink{host: "gamma", wire: wire, fd: local, originReplies: make(chan originReply, conn.OutboxSize+8)}
			destination := &hostLink{host: "beta"}
			owner := testTraceCaller()
			owner.Groups = append(owner.Groups, "denied", "session:unrelated@alpha")
			policy := &Policy{hosts: map[string]hostPolicy{"alpha": {groups: map[string]bool{"team": true}}}}
			state := &hubState{policy: policy, links: map[string]*hostLink{"gamma": origin, "beta": destination, "alpha": {host: "alpha", attachment: owner.SourceAttachment}}, relations: newPolicyRelations(), log: io.Discard}
			expected := []string{}
			switch name {
			case "creator":
				state.relations.entries["child@beta"] = &policyRelation{lane: "child@beta", creator: owner.SessionID}
				expected = []string{owner.PrivateGroup}
			case "one-way":
				state.relations.entries["child@beta"] = &policyRelation{lane: "child@beta", creator: owner.SessionID, oneWay: true}
			case "allowed":
				owner.Groups = append(owner.Groups, "team")
				expected = []string{"team", owner.PrivateGroup}
			case "stale":
				owner.SourceAttachment = "old-attachment"
			case "local":
				owner.SessionID = "parent@beta"
				owner.Name = "Parent@beta"
				owner.PrivateGroup = "session:parent@beta"
				owner.Groups = []string{owner.PrivateGroup, "denied"}
				owner.SourceAttachment = ""
				expected = append([]string(nil), owner.Groups...)
			case "unrestricted":
				owner.SessionID = "parent@gamma"
				owner.Name = "Parent@gamma"
				owner.PrivateGroup = "session:parent@gamma"
				owner.Groups = []string{owner.PrivateGroup, "denied"}
				state.links["gamma"].attachment = owner.SourceAttachment
				expected = append([]string(nil), owner.Groups...)
			}
			ref := TraceRecipient{Child: "child@beta", Target: "child@beta", Mode: "events", Version: "v1", Owner: owner}
			before, _ := json.Marshal(ref)
			request := policyTestRequest(t, "message.send", protocol.MessageSendRequest{Target: "child@beta", Message: "hello"})
			request.Trace = true
			prepared := state.preparePolicyForward(context.Background(), nil, origin, destination, &Forward{From: Caller{SessionID: "caller@gamma", PrivateGroup: "session:caller@gamma", Groups: []string{"session:caller@gamma"}}, Request: request}, make(chan Reply, 1))
			// Even an unrestricted gamma->beta call can return a restricted alpha recipient.
			state.policy = nil
			state.reply(originReply{origin: origin, destination: destination, id: 7, method: request.Method, policy: prepared.reply, collect: true, value: Reply{Result: testTraceResult(t), Trace: []TraceRecipient{ref}}})
			reply := (<-origin.originReplies).value
			if len(expected) == 0 {
				if len(reply.Trace) != 0 {
					t.Fatal("ineligible recipient survived", reply.Trace)
				}
				return
			}
			if len(reply.Trace) != 1 || !reflect.DeepEqual(reply.Trace[0].Owner.Groups, expected) || reply.Trace[0].Owner.OwnerLifetime != owner.OwnerLifetime {
				t.Fatal("recipient authority/groups changed", reply.Trace)
			}
			raw, err := encodeTraceReply(reply)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeTraceReply("message.send", raw, "beta"); err != nil {
				t.Fatal("filtered recipient fails strict validation", err, string(raw))
			}
			after, _ := json.Marshal(ref)
			if string(before) != string(after) {
				t.Fatal("shared recipient mutated")
			}
		})
	}
}
