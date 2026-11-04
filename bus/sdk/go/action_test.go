// SPDX-License-Identifier: MIT

package sessionkit

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestCallerWireActions(t *testing.T) {
	tests := []struct {
		action, method, args, response string
		want                           any
	}{
		{"list", "session.list", `{}`, `{"sessions":[]}`, SessionListRequest{}},
		{"send", "message.send", `{"target":"lane","message":"hi"}`, `{"message_id":"m","deliveries":[]}`, MessageSendRequest{Target: "lane", Message: "hi"}},
		{"spawn", "lane.spawn", `{"name":"child","product":"example-peer","open":{},"trace":"events"}`, `{"session_id":"child@local"}`, LaneSpawnRequest{Name: "child", Product: "example-peer", Open: &OpenOptions{}, Trace: "events"}},
		{"trace", "trace.configure", `{"session_id":"child@local","mode":"content"}`, `{"session_id":"child@local","mode":"content"}`, TraceConfigureRequest{SessionID: "child@local", Mode: "content"}},
		{"describe", "lane.describe", `{"product":"example-peer"}`, `{"product":"example-peer","supported_open_fields":[],"extra_arguments":[]}`, LaneDescribeRequest{Product: "example-peer"}},
		{"run", "turn.run", `{"session_id":"child@local","input":"hi"}`, `{"session_id":"child@local","run_id":"g/1","state":"done","result":{"outcome":"completed","result":"hi"}}`, TurnRunRequest{SessionID: "child@local", Input: "hi"}},
		{"interrupt", "turn.interrupt", `{"session_id":"child@local"}`, `{}`, SessionTarget{SessionID: "child@local"}},
		{"close", "session.close", `{"session_id":"child@local"}`, `{}`, SessionCloseRequest{SessionID: "child@local"}},
		{"forget", "session.close", `{"session_id":"child@local"}`, `{}`, SessionCloseRequest{SessionID: "child@local", Forget: true}},
	}
	for _, test := range tests {
		t.Run(test.action, func(t *testing.T) {
			var method string
			var params any
			caller := newCaller(func(_ context.Context, gotMethod string, gotParams, result any) error {
				method, params = gotMethod, gotParams
				return json.Unmarshal([]byte(test.response), result)
			})
			got, err := caller.Action(context.Background(), test.action, json.RawMessage(test.args))
			if err != nil || method != test.method || string(got) != test.response {
				t.Fatalf("method %q params %#v result %s, err %v", method, params, got, err)
			}
			assertJSON(t, params, test.want)
		})
	}
}

func TestCallerSharedWireFixtures(t *testing.T) {
	raw, err := os.ReadFile("protocol/caller-sugar.fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures struct {
		Operations []struct {
			Action, Method  string
			Request, Result json.RawMessage
		}
	}
	if err = json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, row := range fixtures.Operations {
		caller := NewCaller(func(_ context.Context, method string, params any) (json.RawMessage, error) {
			if method != row.Method {
				t.Fatalf("method %s want %s", method, row.Method)
			}
			got, _ := json.Marshal(params)
			var a, b any
			_ = json.Unmarshal(got, &a)
			_ = json.Unmarshal(row.Request, &b)
			if !reflect.DeepEqual(a, b) {
				t.Fatalf("params %s want %s", got, row.Request)
			}
			return row.Result, nil
		})
		got, err := caller.Action(context.Background(), row.Action, row.Request)
		if err != nil {
			t.Fatal(err)
		}
		var a, b any
		_ = json.Unmarshal(got, &a)
		_ = json.Unmarshal(row.Result, &b)
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("result %s want %s", got, row.Result)
		}
	}
}

func TestCallerActionRejectsInvalidInput(t *testing.T) {
	caller := newCaller(func(context.Context, string, any, any) error { return nil })
	for _, test := range []struct {
		name, action, args string
	}{
		{"unknown action", "unknown", `{}`},
		{"wire shape", "list", `{"extra":true}`},
		{"trace mode", "trace", `{"session_id":"lane@local","mode":"all"}`},
		{"local shape", "status", `{"turn_id":"t-1","extra":true}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := caller.Action(context.Background(), test.action, json.RawMessage(test.args)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	if !reflect.DeepEqual(Actions, []string{"list", "send", "spawn", "describe", "trace", "run", "start", "wait", "status", "interrupt", "close", "forget", "ack", "transfer.offer", "transfer.request", "transfer.accept", "transfer.cancel", "transfer.status"}) {
		t.Fatalf("actions = %v", Actions)
	}
}

func TestNewCallerRejectsInvalidResult(t *testing.T) {
	for _, test := range []struct {
		name, result string
	}{
		{"missing field", `{}`},
		{"extra field", `{"sessions":[],"extra":true}`},
		{"wrong field type", `{"sessions":"wrong"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			caller := NewCaller(func(context.Context, string, any) (json.RawMessage, error) {
				return json.RawMessage(test.result), nil
			})
			listed, err := caller.List(context.Background(), SessionListRequest{})
			if err == nil || listed.Sessions != nil || listed.Hosts != nil {
				t.Fatalf("typed result = %#v, err %v", listed, err)
			}
			raw, err := caller.Action(context.Background(), "list", json.RawMessage(`{}`))
			if err == nil || raw != nil {
				t.Fatalf("action result = %s, err %v", raw, err)
			}
		})
	}
}
