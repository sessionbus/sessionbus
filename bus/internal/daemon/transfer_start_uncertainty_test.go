// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/internal/transfer"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestAcceptStartUncertaintyKeepsLocalAndBridgedWireTruthful(t *testing.T) {
	for _, bridged := range []bool{false, true} {
		for _, offer := range []bool{true, false} {
			for _, mode := range []string{"unconfirmed", "genuine-state", "authoritative-winner"} {
				name := mode + "/" + map[bool]string{false: "local", true: "bridged"}[bridged] + "/" + map[bool]string{true: "offer", false: "request"}[offer]
				t.Run(name, func(t *testing.T) {
					d, s, peer := reviewSession(t)
					d.host, s.supportsTransfer = "local", true
					item, _, _, ok := d.directory.installPeer(s, &protocol.PeerHello{Protocol: 1, Product: "fixture", SessionID: "accepter", Groups: []string{"shared"}, Info: map[string]any{protocol.TransferInfoKey: 1}}, d.host)
					if !ok {
						t.Fatal("peer installation")
					}
					s.identity = item
					actor, err := d.directory.transferEndpoint(item)
					if err != nil {
						t.Fatal(err)
					}
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					budget := transfer.HostBudget()
					engine := transfer.NewEngine(ctx, budget)
					d.transfers = engine
					var mu sync.Mutex
					starts, finishes := 0, 0
					held := map[string]bool{}
					terminals := map[string][]protocol.TransferNotice{}
					sourceKey, destinationKey := "source-generation", actor.Key
					if !offer {
						sourceKey, destinationKey = actor.Key, "destination-generation"
					}
					endpoint := func(key, id string) transfer.Endpoint {
						ep := transfer.Endpoint{Key: key, Identity: protocol.DeliverySource{SessionID: id, Product: "fixture", Groups: []string{"shared"}}, Done: make(chan struct{})}
						ep.Call = func(callCtx context.Context, method string, params, result any) error {
							if _, err := protocol.EncodeParams(method, params); err != nil {
								return err
							}
							var value any
							switch method {
							case "transfer.prepare":
								p := params.(protocol.TransferPrepare)
								size := protocol.Integer(4)
								value = protocol.TransferPrepared{ID: p.ID, Ready: p.Role == "destination"}
								if p.Role == "source" {
									value = protocol.TransferPrepared{ID: p.ID, Size: &size}
								}
							case "transfer.start":
								p := params.(protocol.TransferStart)
								if p.Role == "source" {
									mu.Lock()
									starts++
									mu.Unlock()
									if mode == "authoritative-winner" {
										if _, err := engine.Fail(sourceKey, protocol.TransferFailure{ID: p.ID, Reason: "source_read_failed"}); err != nil {
											return err
										}
										<-callCtx.Done()
										return callCtx.Err()
									}
									return errors.New("fixture source-start response lost")
								}
								value = protocol.TransferStarted{ID: p.ID, Ready: true}
							default:
								mu.Lock()
								finishes++
								mu.Unlock()
								return errors.New("unexpected byte/publication call")
							}
							raw, err := protocol.EncodeResult(method, value)
							if err != nil {
								return err
							}
							return json.Unmarshal(raw, result)
						}
						ep.Hold = func(_ context.Context, id string, terminal bool) bool {
							mu.Lock()
							defer mu.Unlock()
							if terminal {
								delete(held, key+":"+id)
							} else {
								held[key+":"+id] = true
							}
							return true
						}
						ep.Notice = func(_ context.Context, notice protocol.TransferNotice) error {
							if _, err := protocol.EncodeParams("transfer.notice", notice); err != nil {
								return err
							}
							mu.Lock()
							defer mu.Unlock()
							if notice.Kind == "terminal" {
								terminals[key] = append(terminals[key], notice)
							}
							return nil
						}
						return ep
					}
					source, destination := endpoint(sourceKey, "source@local"), endpoint(destinationKey, "destination@local")
					creator, accepter := source, destination
					if !offer {
						creator, accepter = destination, source
					}
					descriptor, err := engine.Create(creator, accepter, offer, "file", "requested file")
					if err != nil {
						t.Fatal(err)
					}
					engine.Announce(descriptor.ID)
					input := &protocol.TransferAccept{ID: descriptor.ID}
					if mode == "genuine-state" {
						badSize := protocol.Integer(5)
						input.Size = &badSize
					}
					var link *federationLink
					if bridged {
						link = &federationLink{transfer: true, inbox: make(chan any, 1)}
						binding := &bridgeBinding{id: descriptor.ID, endpoint: actor, phase: "pending", timer: time.NewTimer(time.Hour)}
						defer binding.timer.Stop()
						bridge := &transferBridge{daemon: d, link: link, ctx: ctx, ids: map[string]*bridgeBinding{descriptor.ID: binding}}
						binding.bridge, link.bridge, d.federation = bridge, bridge, link
					}
					raw, err := protocol.RequestBytes(7, "transfer.accept", input)
					must(t, err)
					request, err := protocol.DecodeFrame(raw)
					must(t, err)
					s.transferRequest(request, input)
					if bridged {
						select {
						case event := <-link.inbox:
							call := event.(federation.TransferCall)
							if call.Method != "transfer.accept" {
								t.Fatal(call.Method)
							}
							params, err := json.Marshal(call.Params)
							must(t, err)
							decoded, err := protocol.DecodeParams(call.Method, params)
							must(t, err)
							value, engineErr := engine.Accept(actor.Key, *decoded.(*protocol.TransferAccept))
							reply := federation.Reply{}
							if engineErr != nil {
								code, data := federation.TransferError(engineErr)
								body := federation.ErrorBytes(9, code, data)
								frame, err := protocol.DecodeFrame(body)
								must(t, err)
								reply.Error, reply.ErrorRaw = frame.Error, frame.ErrorRaw
							} else {
								reply.Result, err = protocol.EncodeResult(call.Method, value)
								must(t, err)
							}
							call.Reply <- reply
						case <-time.After(time.Second):
							t.Fatal("actual bridged Accept missing")
						}
					}
					select {
					case event := <-s.inbox:
						if _, ok := event.(replyEvent); !ok {
							t.Fatalf("unexpected event %T", event)
						}
						s.handleEvent(event)
					case <-time.After(time.Second):
						t.Fatal("actual Accept response missing")
					}
					must(t, peer.SetReadDeadline(time.Now().Add(time.Second)))
					reader := bufio.NewReader(peer)
					response, err := reader.ReadBytes('\n')
					must(t, err)
					frame, err := protocol.DecodeFrame(response)
					must(t, err)
					if frame.ID != 7 {
						t.Fatal("wrong original response", string(response))
					}
					wantOutcome, wantReason := "unknown", "source_start_unconfirmed"
					if mode == "authoritative-winner" {
						wantOutcome, wantReason = "failed", "source_read_failed"
						var accepted protocol.TransferDescriptor
						if frame.Error != nil || json.Unmarshal(frame.Result, &accepted) != nil || accepted.ID != descriptor.ID || accepted.State != "streaming" {
							t.Error("authoritative terminal retroactively rejected admission", string(response))
						}
					} else if mode == "genuine-state" {
						wantOutcome, wantReason = "failed", "source_size_invalid"
						if frame.Error == nil || frame.Error.Code != protocol.TransferFailed || len(frame.Error.Data) != 0 {
							t.Error("genuine state refusal changed", string(response))
						}
					} else if frame.Error == nil || frame.Error.Code != protocol.Internal || string(frame.Error.Data) != `"transfer_unavailable"` {
						t.Error("allocated source-start uncertainty became semantic refusal", string(response))
					}
					until := time.Now().Add(time.Second)
					for budget.Usage().Active != 0 && time.Now().Before(until) {
						time.Sleep(time.Millisecond)
					}
					mu.Lock()
					wantStarts := 1
					if mode == "genuine-state" {
						wantStarts = 0
					}
					if starts != wantStarts || finishes != 0 || len(held) != 0 || len(terminals[sourceKey]) != 1 || len(terminals[destinationKey]) != 1 {
						t.Error("start replay/publication/resource/terminal count", starts, finishes, held, terminals)
					}
					for _, notices := range terminals {
						for _, notice := range notices {
							if notice.ID != descriptor.ID || notice.Outcome != wantOutcome || notice.Reason != wantReason {
								t.Error("terminal knowledge changed", notice)
							}
						}
					}
					mu.Unlock()
					if usage := budget.Usage(); usage.Active != 0 || usage.Pending != 0 {
						t.Error("terminal budget leaked", usage)
					}
					// The next ordinary operation on this SAME wire also detects an
					// accidental second original reply, without replaying Accept.
					raw, err = protocol.RequestBytes(11, "session.list", protocol.SessionListRequest{SessionID: item.row.SessionID})
					must(t, err)
					ordinary, err := protocol.DecodeFrame(raw)
					must(t, err)
					s.handleFrame(conn.Frame{Value: ordinary, Ordered: true})
					response, err = reader.ReadBytes('\n')
					must(t, err)
					frame, err = protocol.DecodeFrame(response)
					if err != nil || frame.ID != 11 || frame.Error != nil {
						t.Error("same caller lost or duplicate original response", string(response), err)
					}
				})
			}
		}
	}
}
