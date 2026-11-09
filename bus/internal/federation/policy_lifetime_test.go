// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/roster"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestPolicyActualLateSpawnAfterSilenceRecordsWithoutSecondAnswer(t *testing.T) {
	hub, address, secrets := testHub(t)
	if !hub.ReloadPolicy(testPolicy(t, `{"hosts":{"alpha":{"groups":[]}}}`)) {
		t.Fatal("reload failed")
	}
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	admitted, probes := make(chan struct{}, 1), make(chan struct{}, 4)
	beta := connectTestDaemon(t, address, "beta", secrets["beta"], func(call IncomingCall) (Wait, error) {
		if call.Request.Method == "lane.spawn" || IsRoster(call) {
			if IsRoster(call) {
				probes <- struct{}{}
			} else {
				admitted <- struct{}{}
			}
			return func(done <-chan struct{}) (Reply, bool) {
				select {
				case <-release:
					if IsRoster(call) {
						return RosterReply(roster.Host{Host: "beta", Products: []string{}, Sessions: []roster.Row{{SessionID: "late@beta", Kind: "lane", Groups: []string{"session:parent@alpha", "session:parent@alpha/late"}}}}), true
					}
					return relationResult("late@beta"), true
				case <-done:
					return Reply{}, false
				}
			}, nil
		}
		return immediate(emptyReply()), nil
	})
	_ = callHosts(t, beta)
	alpha := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(call IncomingCall) (Wait, error) {
		if IsRoster(call) {
			return immediate(RosterReply(roster.Host{Host: "alpha", Products: []string{}, Sessions: []roster.Row{}})), nil
		}
		return immediate(emptyReply()), nil
	})
	_ = callHosts(t, alpha)
	caller := policyTestCaller()
	request := policyTestRequest(t, "lane.spawn", protocol.LaneSpawnRequest{Host: "beta", Name: "late", Product: "fixture-worker", Open: &protocol.OpenOptions{}})
	reply := make(chan Reply, 1)
	alpha.inbox <- OutgoingCall{Value: Forward{From: caller, Request: request}, Reply: reply}
	select {
	case <-admitted:
	case <-time.After(time.Second):
		t.Fatal("spawn did not reach destination")
	}
	select {
	case result := <-reply:
		if result.Error == nil || result.Error.Code != protocol.ForwardLost {
			t.Fatal("silence fabricated spawn success or nonexecution", result)
		}
	case <-time.After(21 * time.Second):
		t.Fatal("silence left the original waiter unbounded")
	}
	once.Do(func() { close(release) })
	for range 2 {
		select {
		case <-probes:
		case <-time.After(6 * time.Second):
			t.Fatal("retained probe did not recover the same attachment")
		}
	}
	control := policyTestRequest(t, "turn.interrupt", protocol.SessionTarget{SessionID: "late@beta"})
	if result := policyTestForward(t, alpha, caller, control); result.Error != nil {
		t.Fatal("authoritative late spawn was not recorded for creator control", result.Error)
	}
	select {
	case result := <-reply:
		t.Fatal("late spawn produced a second public answer", result)
	case <-admitted:
		t.Fatal("uncertain spawn was replayed")
	default:
	}
	if hosts := callHosts(t, alpha); len(hosts) != 1 || hosts[0] != "beta" {
		t.Fatal("silence/table bookkeeping closed the host link", hosts)
	}
}

func TestPolicyActualRelationCapCountsLiveAndInFlightAndForgetReleases(t *testing.T) {
	hub, address, secrets := testHub(t)
	if !hub.ReloadPolicy(testPolicy(t, `{"hosts":{"alpha":{"groups":[]}}}`)) {
		t.Fatal("reload failed")
	}
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	seen := make(chan string, protocol.MaxOperations+4)
	beta := connectTestDaemon(t, address, "beta", secrets["beta"], func(call IncomingCall) (Wait, error) {
		if IsRoster(call) {
			return immediate(errorReply(protocol.ForwardLost, nil)), nil // Not complete absence.
		}
		if call.Request.Method != "lane.spawn" {
			return immediate(emptyReply()), nil
		}
		params, _ := protocol.DecodeParams("lane.spawn", call.Request.Params)
		name := params.(*protocol.LaneSpawnRequest).Name
		seen <- name
		result := relationResult(name + "@beta")
		if name != "held0" && name != "held1" {
			return immediate(result), nil
		}
		return func(done <-chan struct{}) (Reply, bool) {
			select {
			case <-release:
				return result, true
			case <-done:
				return Reply{}, false
			}
		}, nil
	})
	_ = callHosts(t, beta)
	alpha := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(IncomingCall) (Wait, error) { return immediate(emptyReply()), nil })
	_ = callHosts(t, alpha)
	caller := policyTestCaller()
	request := func(name string) PublicRequest {
		return policyTestRequest(t, "lane.spawn", protocol.LaneSpawnRequest{Host: "beta", Name: name, Product: "fixture-worker", Open: &protocol.OpenOptions{}})
	}
	for i := 0; i < policyRelationsPerHost-2; i++ {
		name := fmt.Sprintf("child%d", i)
		if reply := policyTestForward(t, alpha, caller, request(name)); reply.Error != nil {
			t.Fatal("live grants refused below cap", i, reply.Error)
		}
		if got := <-seen; got != name {
			t.Fatal(got, name)
		}
	}
	var held [2]chan Reply
	for i := range held {
		held[i] = make(chan Reply, 1)
		name := fmt.Sprintf("held%d", i)
		alpha.inbox <- OutgoingCall{Value: Forward{From: caller, Request: request(name)}, Reply: held[i]}
		select {
		case got := <-seen:
			if got != name {
				t.Fatal(got, name)
			}
		case <-time.After(time.Second):
			t.Fatal("inflight reservation did not reach destination")
		}
	}
	if reply := policyTestForward(t, alpha, caller, request("excess")); reply.Error == nil || reply.Error.Code == protocol.ForwardLost {
		t.Fatal("live plus inflight cap was bypassed or mistaken for transport loss", reply)
	}
	select {
	case name := <-seen:
		t.Fatal("over-cap spawn reached destination", name)
	default:
	}
	forget := policyTestRequest(t, "session.close", protocol.SessionCloseRequest{SessionID: "child0@beta", Forget: true})
	if reply := policyTestForward(t, alpha, caller, forget); reply.Error != nil {
		t.Fatal("exact forget at cap failed", reply.Error)
	}
	if reply := policyTestForward(t, alpha, caller, request("replacement")); reply.Error != nil {
		t.Fatal("authoritative forget did not release its grant before answer", reply.Error)
	}
	if got := <-seen; got != "replacement" {
		t.Fatal(got)
	}
	once.Do(func() { close(release) })
	for _, answer := range held {
		select {
		case reply := <-answer:
			if reply.Error != nil {
				t.Fatal("admitted inflight spawn failed after cap refusal", reply.Error)
			}
		case <-time.After(time.Second):
			t.Fatal("admitted spawn did not complete")
		}
	}
	if len(callHosts(t, alpha)) != 1 {
		t.Fatal("cap refusal closed destination")
	}
}
