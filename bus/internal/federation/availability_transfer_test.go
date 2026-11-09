// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"bufio"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

// This is the pre-availability Caller schema. Strict nested decoding must work
// on both the origin-to-hub create and hub-to-endpoint bind frames.
type legacyTransferCaller struct {
	OwnerLifetime    string   `json:"owner_lifetime,omitempty"`
	SourceAttachment string   `json:"source_attachment,omitempty"`
	SessionID        string   `json:"session_id"`
	Name             string   `json:"name,omitempty"`
	Product          string   `json:"product"`
	PrivateGroup     string   `json:"private_group"`
	Groups           []string `json:"groups"`
}

func TestTransferCallerAvailabilityDoesNotEscapeSerialization(t *testing.T) {
	for _, method := range []string{TransferCreateMethod, TransferBindMethod} {
		for _, form := range []string{"value", "pointer", "raw", "raw-present", "raw-alias-duplicate"} {
			for _, optIn := range []bool{false, true} {
				t.Run(method+"/"+form+map[bool]string{false: "/legacy", true: "/opted-in"}[optIn], func(t *testing.T) {
					from := Caller{Availability: optIn, SessionID: "sender@alpha", Name: "Sender@alpha", Product: "peer", OwnerLifetime: "owner-generation", PrivateGroup: "session:sender@alpha", Groups: []string{"team", "session:sender@alpha"}}
					var input, legacy any
					if method == TransferCreateMethod {
						value := TransferCreate{From: from, Generation: "native-generation", Target: "target@beta", Kind: "offer", Name: "file.txt"}
						input = value
						if form == "pointer" {
							input = &value
						}
						value.From.Availability = false
						legacy = value
						if form == "pointer" { // Retain an opted-in input distinct from the expected copy.
							original := input.(*TransferCreate)
							original.From.Availability = optIn
						}
					} else {
						from.SourceAttachment = "hub-attachment"
						value := TransferBind{From: from, Generation: "native-generation", Target: "target@beta"}
						input = value
						if form == "pointer" {
							input = &value
						}
						value.From.Availability = false
						legacy = value
						if form == "pointer" {
							input.(*TransferBind).From.Availability = optIn
						}
					}
					if strings.HasPrefix(form, "raw") {
						raw, err := json.MarshalIndent(input, "", " ")
						if err != nil {
							t.Fatal(err)
						}
						if form == "raw-present" && !optIn {
							raw = []byte(strings.Replace(string(raw), `"from": {`, `"from": {"availability":false,`, 1))
						}
						if form == "raw-alias-duplicate" {
							// Decoding merges the later From object into the earlier one.
							// Presence must survive even if the later object has no key.
							raw = append([]byte(`{"From":{"Availability":null},`), raw[1:]...)
						}
						input = json.RawMessage(raw)
					}
					before, err := json.Marshal(input)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := decodeTransferParams(method, before); err != nil {
						t.Fatal("invalid transfer fixture", err)
					}
					left, right := net.Pipe()
					var group sync.WaitGroup
					wire := conn.Start(left, make(chan any, 16), &group)
					t.Cleanup(func() { wire.Close(); _ = right.Close(); group.Wait() })
					queued, pending := map[*pendingCall]bool{}, map[int64]pendingCall{}
					if !queueTransferCall(wire, queued, pending, TransferCall{Method: method, Params: input, Reply: make(chan Reply, 1)}) {
						t.Fatal("valid transfer refused")
					}
					if err := right.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
						t.Fatal(err)
					}
					line, err := bufio.NewReader(right).ReadBytes('\n')
					if err != nil {
						t.Fatal(err)
					}
					frame, err := protocol.DecodeFrame(line[:len(line)-1])
					if err != nil || frame.Method != method || frame.ID != 1 {
						t.Fatal("invalid serialized request", frame, err)
					}
					var old struct {
						From       legacyTransferCaller `json:"from"`
						Generation string               `json:"generation"`
						Target     string               `json:"target"`
						Kind       string               `json:"kind,omitempty"`
						Name       string               `json:"name,omitempty"`
					}
					if err := protocol.DecodeJSON(frame.Params, &old); err != nil {
						t.Error("old transfer decoder rejected nested request-only flag", string(frame.Params), err)
					}
					want, _ := json.Marshal(legacy)
					if string(frame.Params) != string(want) {
						t.Errorf("transfer identity or keys changed: %s want %s", frame.Params, want)
					}
					after, _ := json.Marshal(input)
					if string(before) != string(after) {
						t.Error("serialization mutated the original caller", string(after))
					}
					if queueTransferCall(wire, queued, pending, TransferCall{Method: method, Params: json.RawMessage(`{"from":{}}`)}) || queueTransferCall(wire, queued, pending, TransferCall{Method: "transfer.unknown", Params: input}) {
						t.Error("malformed or unsupported transfer was admitted")
					}
					if !queueTransferCall(wire, queued, pending, TransferCall{Method: TransferDropMethod, Params: TransferBindingTarget{Binding: "binding"}, Reply: make(chan Reply, 1)}) {
						t.Fatal("later valid cleanup refused")
					}
					line, err = bufio.NewReader(right).ReadBytes('\n')
					if err != nil {
						t.Fatal(err)
					}
					frame, err = protocol.DecodeFrame(line[:len(line)-1])
					if err != nil || frame.Method != TransferDropMethod || frame.ID != 2 || string(frame.Params) != `{"binding":"binding"}` {
						t.Fatal("later cleanup identity or serialized ID changed", frame, err)
					}
				})
			}
		}
	}
}
