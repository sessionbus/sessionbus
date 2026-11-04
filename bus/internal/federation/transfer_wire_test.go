// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"

	"github.com/antst/sessionbus/bus/internal/transfer"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestTransferInternalControlsAreClosedAndPathFree(t *testing.T) {
	for _, test := range []struct {
		method, raw string
		valid       bool
	}{
		{TransferBindReadyMethod, `{"binding":"opaque"}`, true},
		{TransferBindReadyMethod, `{"binding":"opaque","path":"/private/source"}`, false},
		{TransferAnnounceMethod, `{"id":"opaque"}`, true},
		{TransferHoldMethod, `{"binding":"opaque","id":"transfer","terminal":false}`, true},
		{TransferHoldMethod, `{"binding":"opaque","id":"transfer","terminal":false,"owner":"spoof"}`, false},
		{"transfer.fail", `{"id":"opaque","reason":"source_read_failed"}`, true},
		{"transfer.fail", `{"id":"opaque","reason":"stream_lost"}`, false},
		{"transfer.chunk", `{"id":"opaque","offset":0,"data":"YQ==","groups":["spoof"]}`, false},
		{"transfer.exec", `{"method":"open","path":"/private/source"}`, false},
		{TransferPrepareMethod, `{"binding":"opaque","prepare":{"id":"transfer","role":"source"}}`, true},
		{TransferPrepareMethod, `{"binding":"opaque","prepare":{"id":"transfer","role":"source","path":"/private/source"}}`, false},
	} {
		t.Run(test.method+test.raw, func(t *testing.T) {
			_, err := decodeTransferParams(test.method, json.RawMessage(test.raw))
			if (err == nil) != test.valid {
				t.Fatal("strict internal boundary", err)
			}
		})
	}
	for _, method := range []string{TransferDropMethod, TransferBindReadyMethod, TransferAnnounceMethod} {
		if !validateTransferResult(method, json.RawMessage(`{}`)) || validateTransferResult(method, json.RawMessage(`{"path":"/private/result"}`)) {
			t.Fatal("internal result widened", method)
		}
	}
}

func TestTransferCreationRequiresBothNegotiatedLegsBeforeBinding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := newHubTransfers(ctx)
	one, two := net.Pipe()
	defer one.Close()
	defer two.Close()
	// Each legacy link is a real ordinary stream with no transfer ALPN. These
	// refusals precede binding, endpoint preparation, budget mutation or any send.
	old := &hostLink{fd: one, attachment: "old"}
	other := &hostLink{fd: two, attachment: "other"}
	for _, pair := range [][2]*hostLink{{old, nil}, {old, other}, {other, old}, {old, old}} {
		if _, err := h.create(pair[0], pair[1], TransferCreate{}); !errors.Is(err, transfer.ErrUnsupported) {
			t.Fatal(err)
		}
	}
	if usage := h.budget.Usage(); usage.Pending != 0 || usage.Active != 0 || usage.ControlledBytes != 0 {
		t.Fatal("unsupported leg consumed admission", usage)
	}
	if len(h.bindings) != 0 || len(h.routes) != 0 {
		t.Fatal("unsupported leg created endpoint state")
	}
	if endpointTransferControl("transfer.fail") || !transferMethod("transfer.fail") || !protocol.Allows("transfer.fail", true) {
		t.Fatal("source failure became a destination callback")
	}
}
