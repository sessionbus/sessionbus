// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"encoding/json"
	"maps"
	"reflect"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestTransferNegotiationRemainsPrivateAndDoesNotMutateHello(t *testing.T) {
	for _, test := range []struct {
		name    string
		input   map[string]any
		want    map[string]any
		capable bool
	}{
		{name: "nil"},
		{name: "empty", input: map[string]any{}, want: map[string]any{}},
		{name: "only capability", input: map[string]any{protocol.TransferInfoKey: 1}, want: map[string]any{}, capable: true},
		{name: "product metadata", input: map[string]any{"ready": true, "nested": map[string]any{"cwd": "/work"}, protocol.TransferInfoKey: float64(1)}, want: map[string]any{"ready": true, "nested": map[string]any{"cwd": "/work"}}, capable: true},
		{name: "JSON number", input: map[string]any{protocol.TransferInfoKey: json.Number("1"), "revision": 2}, want: map[string]any{"revision": 2}, capable: true},
		{name: "not opted in", input: map[string]any{protocol.TransferInfoKey: "1", "ready": false}, want: map[string]any{"ready": false}},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := maps.Clone(test.input)
			public := peerPublicInfo(test.input)
			if !reflect.DeepEqual(public, test.want) {
				t.Fatalf("public = %#v, want %#v", public, test.want)
			}
			if !reflect.DeepEqual(test.input, before) || transferOptIn(test.input) != test.capable {
				t.Fatalf("hello/negotiation changed: %#v", test.input)
			}
			if public != nil {
				public["local mutation"] = true
				if _, ok := test.input["local mutation"]; ok {
					t.Fatal("directory aliases hello map")
				}
			}
		})
	}
}
