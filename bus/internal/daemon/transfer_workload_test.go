// SPDX-License-Identifier: GPL-3.0-only
package daemon_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/daemon"
	"github.com/antst/sessionbus/bus/internal/federation"
	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

type pacedTransferExecutor struct{ fixtureTransferExecutor }

func (x *pacedTransferExecutor) OpenTransferSource(ctx context.Context, path string) (sessionkit.TransferSource, error) {
	source, err := x.fixtureTransferExecutor.OpenTransferSource(ctx, path)
	if err != nil {
		return nil, err
	}
	return &pacedTransferSource{TransferSource: source, ctx: ctx, start: time.Now()}, nil
}

type pacedTransferSource struct {
	sessionkit.TransferSource
	ctx   context.Context
	start time.Time
	bytes int64
}

func (s *pacedTransferSource) Read(raw []byte) (int, error) {
	// Limit source production to 32MiB/s so the measured transfer interval has
	// enough 10/s observations. This is not a socket timeout or link-stall fix.
	delay := s.start.Add(time.Duration(float64(s.bytes) / float64(32<<20) * float64(time.Second))).Sub(time.Now())
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-s.ctx.Done():
			return 0, s.ctx.Err()
		}
	}
	n, err := s.TransferSource.Read(raw)
	s.bytes += int64(n)
	return n, err
}

type workloadEndpoint struct {
	peer       *sessionkit.Peer
	remote     string
	notices    chan protocol.TransferNotice
	mu         sync.Mutex
	replies    map[string]chan time.Time
	replyQueue chan string
	replyError chan error
}

func newWorkloadEndpoint(t *testing.T, socket, id, host, remote string, executor sessionkit.TransferExecutor) *workloadEndpoint {
	t.Helper()
	t.Setenv("SESSIONBUS_SOCKET", socket)
	endpoint := &workloadEndpoint{remote: remote, notices: make(chan protocol.TransferNotice, 8), replies: map[string]chan time.Time{}, replyQueue: make(chan string, 4), replyError: make(chan error, 1)}
	peer, err := sessionkit.ConnectPeerWithTransfersContext(context.Background(), sessionkit.PeerIdentity{Product: "workload-fixture", SessionID: id, Groups: []string{"private-workload"}, Info: map[string]any{}}, func(ctx context.Context, _ sessionkit.PeerIdentity, d sessionkit.DeliveryRequest) (sessionkit.DeliveryReceipt, error) {
		if d.From.SessionID != remote && !(strings.HasPrefix(d.MessageID, "transfer-notice-") && d.From.Product == "sessionbus" && d.From.SessionID == "transfer@"+host) {
			return sessionkit.DeliveryReceipt{Disposition: "rejected", Reason: "unexpected fixture identity"}, nil
		}
		if strings.HasPrefix(d.Body, "request:") {
			key, _, _ := strings.Cut(strings.TrimPrefix(d.Body, "request:"), "\n")
			select {
			case endpoint.replyQueue <- key:
			default:
				return sessionkit.DeliveryReceipt{Disposition: "rejected", Reason: "fixture reply queue full"}, nil
			}
		} else if strings.HasPrefix(d.Body, "reply:") {
			key, _, _ := strings.Cut(strings.TrimPrefix(d.Body, "reply:"), "\n")
			endpoint.mu.Lock()
			waiter := endpoint.replies[key]
			endpoint.mu.Unlock()
			if waiter != nil {
				select {
				case waiter <- time.Now():
				default:
				}
			}
		} else {
			var n protocol.TransferNotice
			if json.Unmarshal([]byte(d.Body), &n) == nil && n.Kind != "" {
				select {
				case endpoint.notices <- n:
				case <-ctx.Done():
				}
			}
		}
		return sessionkit.DeliveryReceipt{Disposition: "written"}, nil
	}, executor)
	if err != nil {
		t.Fatal(err)
	}
	endpoint.peer = peer
	replyctx, stopReplies := context.WithCancel(context.Background())
	repliesDone := make(chan struct{})
	go func() {
		defer close(repliesDone)
		for {
			select {
			case <-replyctx.Done():
				return
			case key := <-endpoint.replyQueue:
				callctx, cancel := context.WithTimeout(replyctx, 3*time.Second)
				_, err := peer.Caller.Send(callctx, sessionkit.MessageSendRequest{Target: remote, Message: "reply:" + key + "\n" + strings.Repeat("r", 1024-len("reply:")-len(key)-1)})
				cancel()
				if err != nil {
					select {
					case endpoint.replyError <- err:
					default:
					}
					return
				}
			}
		}
	}()
	select {
	case <-peer.Ready():
	case <-time.After(3 * time.Second):
		t.Fatal("workload peer readiness")
	}
	t.Cleanup(func() { stopReplies(); peer.Shutdown(); <-peer.Closed(); <-repliesDone })
	return endpoint
}
func workloadMessages(ctx context.Context, endpoints []*workloadEndpoint, duration time.Duration) ([][]float64, error) {
	samples := make([][]float64, len(endpoints))
	failures := make(chan error, len(endpoints))
	var workers sync.WaitGroup
	for index, endpoint := range endpoints {
		workers.Add(1)
		go func(index int, endpoint *workloadEndpoint) {
			defer workers.Done()
			tick := time.NewTicker(100 * time.Millisecond)
			defer tick.Stop()
			deadline := time.NewTimer(duration)
			defer deadline.Stop()
			sequence := 0
			for {
				select {
				case err := <-endpoint.replyError:
					failures <- err
					return
				case <-ctx.Done():
					failures <- ctx.Err()
					return
				case <-deadline.C:
					return
				case <-tick.C:
				}
				sequence++
				key := fmt.Sprintf("%d-%d", index, sequence)
				reply := make(chan time.Time, 1)
				endpoint.mu.Lock()
				endpoint.replies[key] = reply
				endpoint.mu.Unlock()
				start := time.Now()
				sendctx, cancel := context.WithTimeout(ctx, 3*time.Second)
				_, err := endpoint.peer.Caller.Send(sendctx, sessionkit.MessageSendRequest{Target: endpoint.remote, Message: "request:" + key + "\n" + strings.Repeat("q", 1024-len("request:")-len(key)-1)})
				if err == nil {
					select {
					case at := <-reply:
						samples[index] = append(samples[index], float64(at.Sub(start))/float64(time.Millisecond))
					case <-sendctx.Done():
						err = sendctx.Err()
					}
				}
				cancel()
				endpoint.mu.Lock()
				delete(endpoint.replies, key)
				endpoint.mu.Unlock()
				if err != nil {
					failures <- err
					return
				}
			}
		}(index, endpoint)
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		return samples, err
	}
	return samples, nil
}
func workloadQuantile(values []float64, q float64) float64 {
	copy := append([]float64(nil), values...)
	sort.Float64s(copy)
	return copy[int(float64(len(copy)-1)*q)]
}
func TestTransferOneGiBResponsiveness(t *testing.T) {
	if os.Getenv("SESSIONBUS_TRANSFER_WORKLOAD") != "1" {
		t.Skip("explicit 1GiB private-daemon/hub workload")
	}
	for _, remote := range []bool{false, true} {
		t.Run(map[bool]string{false: "same_host", true: "private_tls_hub"}[remote], func(t *testing.T) {
			sourceDir, destDir := testsocket.Directory(t), testsocket.Directory(t)
			sourceSocket, destSocket := filepath.Join(sourceDir, "bus.sock"), filepath.Join(destDir, "bus.sock")
			sourceHost, destHost := "local", "local"
			var address string
			secrets := map[string]string{}
			if remote {
				sourceHost, destHost = "alpha", "beta"
				for _, host := range []string{sourceHost, destHost} {
					value, err := federation.NewSecret()
					if err != nil {
						t.Fatal(err)
					}
					secrets[host] = value
				}
				tls, err := federation.ServerTLS(secrets)
				if err != nil {
					t.Fatal(err)
				}
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				hub := federation.StartHub(listener, tls, io.Discard)
				t.Cleanup(hub.Close)
				address = listener.Addr().String()
			} else {
				destSocket = sourceSocket
			}
			start := func(socket, host string) {
				d, err := daemon.Start(daemon.Config{SocketPath: socket, TablePath: socket + ".table", Host: host, HubAddress: address, HubSecret: secrets[host]})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = d.Close() })
			}
			start(sourceSocket, sourceHost)
			if remote {
				start(destSocket, destHost)
			}
			sourcePath, destinationPath := filepath.Join(sourceDir, "source-1gib"), filepath.Join(destDir, "destination-1gib")
			sourceFile, err := os.Create(sourcePath)
			if err != nil {
				t.Fatal(err)
			}
			if err = sourceFile.Truncate(1 << 30); err != nil {
				t.Fatal(err)
			}
			_ = sourceFile.Close()
			sourceExecutor := &pacedTransferExecutor{fixtureTransferExecutor: fixtureTransferExecutor{source: sourcePath}}
			destinationExecutor := &fixtureTransferExecutor{destination: destinationPath}
			source := newWorkloadEndpoint(t, sourceSocket, "source", sourceHost, "destination@"+destHost, sourceExecutor)
			dest := newWorkloadEndpoint(t, destSocket, "destination", destHost, "source@"+sourceHost, destinationExecutor)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
			defer cancel()
			if remote {
				for {
					_, err = source.peer.Caller.List(ctx, sessionkit.SessionListRequest{Host: destHost})
					if err == nil {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal(err)
					case <-time.After(time.Millisecond):
					}
				}
			}
			endpoints := []*workloadEndpoint{source, dest}
			baseline, err := workloadMessages(ctx, endpoints, 12*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			var peakHeap atomic.Uint64
			memoryDone := make(chan struct{})
			defer close(memoryDone)
			go func() {
				ticker := time.NewTicker(20 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-memoryDone:
						return
					case <-ticker.C:
						var stats runtime.MemStats
						runtime.ReadMemStats(&stats)
						for {
							old := peakHeap.Load()
							if stats.HeapAlloc <= old || peakHeap.CompareAndSwap(old, stats.HeapAlloc) {
								break
							}
						}
					}
				}
			}()
			descriptor, err := source.peer.Transfers.Offer(ctx, "destination@"+destHost, sourcePath, "one-gib")
			if err != nil {
				t.Fatal(err)
			}
			awaitTransferNotice(t, dest.notices, descriptor.ID, "offer")
			startTime := time.Now()
			if _, err = dest.peer.Transfers.Accept(ctx, descriptor.ID, destinationPath, false); err != nil {
				t.Fatal(err)
			}
			type batch struct {
				values [][]float64
				err    error
			}
			batches := make(chan batch, 1)
			sampleCtx, stopSamples := context.WithCancel(ctx)
			defer stopSamples()
			go func() {
				values, err := workloadMessages(sampleCtx, endpoints, 14*time.Minute)
				batches <- batch{values, err}
			}()
			var terminal protocol.TransferNotice
			select {
			case terminal = <-dest.notices:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			elapsed := time.Since(startTime)
			stopSamples()
			samples := <-batches
			if samples.err != nil && samples.err != context.Canceled {
				t.Fatal(samples.err)
			}
			if terminal.Kind != "terminal" || terminal.Outcome != "completed" || terminal.Bytes == nil || int64(*terminal.Bytes) != (1<<30) {
				t.Fatal(terminal)
			}
			awaitTransferNotice(t, source.notices, descriptor.ID, "terminal")
			file, err := os.Open(destinationPath)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.New()
			count, err := io.Copy(digest, file)
			_ = file.Close()
			if err != nil || count != 1<<30 || hex.EncodeToString(digest.Sum(nil)) != terminal.SHA256 {
				t.Fatal("published file/hash mismatch", count, err)
			}
			directions := make([]map[string]any, 2)
			for i := range endpoints {
				if len(baseline[i]) < 100 || len(samples.values[i]) < 100 {
					t.Fatal("insufficient 10/s observations", len(baseline[i]), len(samples.values[i]))
				}
				bp95, bp99 := workloadQuantile(baseline[i], .95), workloadQuantile(baseline[i], .99)
				tp95, tp99 := workloadQuantile(samples.values[i], .95), workloadQuantile(samples.values[i], .99)
				directions[i] = map[string]any{"baseline_samples": len(baseline[i]), "transfer_samples": len(samples.values[i]), "baseline_p95_ms": bp95, "baseline_p99_ms": bp99, "transfer_p95_ms": tp95, "transfer_p99_ms": tp99, "additional_p95_ms": tp95 - bp95, "additional_p99_ms": tp99 - bp99}
				if bp99 > 100 || tp95-bp95 > 100 || tp99-bp99 > 250 {
					t.Error("responsiveness target missed", directions[i])
				}
			}
			result := map[string]any{"route": t.Name(), "bytes": count, "seconds": elapsed.Seconds(), "payload_mib_s": 1024 / elapsed.Seconds(), "raw_chunk_bytes": 65536, "credit_bytes": 262144, "message_bytes": 1024, "requested_each_direction_hz": 10, "source_production_cap_mib_s": 32, "directions": directions, "baseline_raw_ms": baseline, "transfer_raw_ms": samples.values, "process_peak_sampled_heap_bytes": peakHeap.Load(), "memory_scope": "whole private fixture process heap; not controlled-payload usage or production RSS", "host_admission_controlled_data_cap_bytes": protocol.TransferHostBufferBytes, "hub_admission_controlled_data_cap_bytes": protocol.TransferHubBufferBytes, "native_permission_acceptance": false}
			raw, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			if output := os.Getenv("SESSIONBUS_TRANSFER_WORKLOAD_OUTPUT"); output != "" {
				if err = os.MkdirAll(output, 0700); err != nil {
					t.Fatal(err)
				}
				name := map[bool]string{false: "same-host.json", true: "private-hub.json"}[remote]
				if err = os.WriteFile(filepath.Join(output, name), append(raw, '\n'), 0600); err != nil {
					t.Fatal(err)
				}
			}
			t.Logf("1GiB in %.3fs; authenticated 1KiB request/replies: %+v; sampled whole-process peak heap %d", elapsed.Seconds(), directions, peakHeap.Load())
		})
	}
}
