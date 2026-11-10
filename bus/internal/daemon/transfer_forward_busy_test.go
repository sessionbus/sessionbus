// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/internal/transfer"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestForwardTransferChunkPreservesReceivedBusy(t *testing.T) {
	for _, test := range []struct {
		name, method string
		remoteCode   int
		wantCode     int
	}{
		{"chunk-busy", "transfer.chunk", protocol.Busy, protocol.Busy},
		{"chunk-success", "transfer.chunk", 0, 0},
		{"chunk-internal", "transfer.chunk", protocol.Internal, protocol.Internal},
		{"chunk-forward-lost", "transfer.chunk", protocol.ForwardLost, protocol.Internal},
		{"status-busy", "transfer.status", protocol.Busy, protocol.Internal},
		{"finish-busy", "transfer.finish", protocol.Busy, protocol.Internal},
		{"create-busy", "transfer.offer", protocol.Busy, protocol.Internal},
		{"chunk-cancelled", "transfer.chunk", -1, protocol.Internal},
	} {
		t.Run(test.name, func(t *testing.T) {
			d, s, peer := reviewSession(t)
			d.host = "local"
			item, _, _, ok := d.directory.installPeer(s, &protocol.PeerHello{Protocol: 1, Product: "fixture", SessionID: "source", Groups: []string{"shared"}, Info: map[string]any{}}, d.host)
			if !ok {
				t.Fatal("fixture peer identity rejected")
			}
			s.identity = item
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			link := &federationLink{transfer: true, transferBulk: make(chan federation.TransferCall), inbox: make(chan any)}
			bridge := &transferBridge{daemon: d, link: link, ctx: ctx, ids: map[string]*bridgeBinding{}}
			link.bridge, d.federation = bridge, link
			origin := transfer.Endpoint{Key: "generation"}
			binding := &bridgeBinding{bridge: bridge, id: "id", endpoint: origin, phase: "streaming", timer: time.NewTimer(time.Hour)}
			t.Cleanup(func() { binding.timer.Stop() })
			bridge.ids[binding.id] = binding
			var params any = &protocol.TransferChunk{ID: "id", Data: "YQ=="}
			switch test.method {
			case "transfer.status":
				params = &protocol.TransferTarget{ID: "id"}
			case "transfer.finish":
				params = &protocol.TransferFinish{ID: "id", Bytes: 1, SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
			case "transfer.offer":
				params = &protocol.TransferOffer{Target: "destination@remote", Name: "file"}
			}
			raw, err := protocol.RequestBytes(7, test.method, params)
			if err != nil {
				t.Fatal(err)
			}
			request, err := protocol.DecodeFrame(raw)
			if err != nil {
				t.Fatal(err)
			}
			if test.remoteCode == -1 {
				cancel()
			}
			if !s.forwardTransfer(request, params, origin) {
				t.Fatal("fixture did not reach ordinary transfer forwarding")
			}
			if test.remoteCode != -1 {
				var call federation.TransferCall
				if test.method == "transfer.chunk" {
					select {
					case call = <-link.transferBulk:
					case <-time.After(time.Second):
						t.Fatal("bulk bridge call missing")
					}
				} else {
					select {
					case event := <-link.inbox:
						call = event.(federation.TransferCall)
					case <-time.After(time.Second):
						t.Fatal("ordinary bridge call missing")
					}
				}
				if call.Method != test.method && !(test.method == "transfer.offer" && call.Method == federation.TransferCreateMethod) {
					t.Fatal("wrong bridge method", call.Method)
				}
				var reply []byte
				if test.remoteCode == 0 {
					reply, err = protocol.ResultBytes(9, test.method, protocol.TransferCredit{Offset: 1})
				} else {
					var data any
					if test.remoteCode == protocol.Internal {
						data = "upstream_failure"
					}
					reply, err = protocol.ErrorBytes(9, test.remoteCode, data)
				}
				if err != nil {
					t.Fatal(err)
				}
				decoded, err := protocol.DecodeFrame(reply)
				if err != nil {
					t.Fatal("injected bridge reply is not schema valid", err)
				}
				call.Reply <- federation.Reply{Error: decoded.Error, ErrorRaw: decoded.ErrorRaw, Result: decoded.Result}
			}
			select {
			case event := <-s.inbox:
				if _, ok := event.(replyEvent); !ok {
					t.Fatalf("expected actual session reply event, got %T", event)
				}
				s.handleEvent(event)
			case <-time.After(time.Second):
				t.Fatal("session forwarding reply missing")
			}
			_ = peer.SetReadDeadline(time.Now().Add(time.Second))
			reader := bufio.NewReader(peer)
			response, err := reader.ReadBytes('\n')
			if err != nil {
				t.Fatal("forwarding lost caller", err)
			}
			frame, err := protocol.DecodeFrame(response)
			if err != nil || frame.ID != request.ID {
				t.Fatal("invalid original correlated response", string(response), err)
			}
			if test.wantCode == 0 {
				var credit protocol.TransferCredit
				if frame.Error != nil || json.Unmarshal(frame.Result, &credit) != nil || credit.Offset != 1 {
					t.Fatal("successful chunk credit changed", string(response))
				}
			} else if frame.Error == nil || frame.Error.Code != test.wantCode {
				t.Errorf("forwarded %s = %s, want code %d", test.name, response, test.wantCode)
			} else if test.wantCode == protocol.Busy && (frame.Error.Message != "busy" || len(frame.Error.Data) != 0) {
				t.Errorf("Busy carried changed message/data: %s", response)
			} else if test.wantCode == protocol.Internal && string(frame.Error.Data) != `"transfer_unavailable"` {
				t.Errorf("uncertain error mapping changed: %s", response)
			}
			if len(s.requests) != 0 || s.owned != 0 || s.closed || s.stopping || bridge.ids["id"] != binding {
				t.Fatal("reply changed ownership, connection, or binding")
			}
			// A real ordinary request on the same wire must follow the single
			// original reply; a duplicate original reply also fails this check.
			raw, err = protocol.RequestBytes(11, "session.list", protocol.SessionListRequest{SessionID: item.row.SessionID})
			if err != nil {
				t.Fatal(err)
			}
			ordinary, err := protocol.DecodeFrame(raw)
			if err != nil {
				t.Fatal(err)
			}
			s.handleFrame(conn.Frame{Value: ordinary, Ordered: true})
			response, err = reader.ReadBytes('\n')
			if err != nil {
				t.Fatal("subsequent ordinary request lost", err)
			}
			frame, err = protocol.DecodeFrame(response)
			if err != nil || frame.ID != 11 || frame.Error != nil {
				t.Fatal("duplicate/error instead of ordinary response", string(response), err)
			}
			var listed protocol.SessionListResult
			if json.Unmarshal(frame.Result, &listed) != nil || len(listed.Sessions) != 1 || listed.Sessions[0].SessionID != item.row.SessionID {
				t.Fatal("ordinary current identity not preserved", string(response))
			}
		})
	}
}
