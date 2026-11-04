// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func awaitCleanupCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("terminal resources did not retire while connected")
		}
		time.Sleep(time.Millisecond)
	}
}
func TestJavaScriptMidStreamParentRenameFailsAndRetiresHostQuotaWhileConnected(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("existing JS SDK runtime unavailable")
	}
	d, socket := startDaemon(t)
	source := connectPeer(t, socket, "go", "go", "shared")
	dir := t.TempDir()
	parent := filepath.Join(dir, "parent")
	must(t, os.Mkdir(parent, 0o700))
	sourcePath := filepath.Join(dir, "source")
	destination := filepath.Join(parent, "destination")
	data := bytes.Repeat([]byte("actual streamed default filesystem data\n"), 6000)
	must(t, os.WriteFile(sourcePath, data, 0o600))
	fixture, err := filepath.Abs("../../sdk/js/transfer-fixture.cjs")
	must(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, node, fixture, socket, "destination", destination, "parent-rename")
	stdout, err := command.StdoutPipe()
	must(t, err)
	stdin, err := command.StdinPipe()
	must(t, err)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	must(t, command.Start())
	t.Cleanup(func() { _ = stdin.Close() })
	var notice protocol.TransferNotice
	select {
	case delivery := <-source.deliveries:
		must(t, json.Unmarshal([]byte(delivery.Body), &notice))
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if notice.Kind != "request" {
		t.Fatal(notice)
	}
	_, err = source.peer.Transfers.Accept(ctx, notice.ID, sourcePath, false)
	must(t, err)
	var report struct {
		Outcome, Reason                string
		Active, Pending, ClosedHandles int
		OrdinaryMessageID              string
	}
	if err = json.NewDecoder(stdout).Decode(&report); err != nil {
		_ = stdin.Close()
		waitErr := command.Wait()
		t.Fatalf("fixture report: %v; process: %v; stderr: %s", err, waitErr, stderr.String())
	}
	if report.Outcome != "failed" || !strings.Contains(report.Reason, "destination_parent_changed") || !strings.Contains(report.Reason, "leftover_temp=") || report.Active != 0 || report.Pending != 0 || report.ClosedHandles != 2 {
		t.Fatal(report)
	}
	awaitCleanupCondition(t, func() bool {
		u := d.transferBudget.Usage()
		return u.Active == 0 && u.Pending == 0 && u.ControlledBytes == 0
	})
	d.directory.mu.Lock()
	item := d.directory.entries["js@local"]
	connected := item != nil && item.attachment != nil
	d.directory.mu.Unlock()
	if !connected {
		t.Fatal("quota released only after disconnect")
	}
	select {
	case delivery := <-source.deliveries:
		// Match the independent ordinary continuation, allowing terminal notices.
		for delivery.Body != "ordinary authenticated JavaScript continuation" {
			select {
			case delivery = <-source.deliveries:
			case <-ctx.Done():
				t.Fatal("missing ordinary continuation", ctx.Err())
			}
		}
		if delivery.From.SessionID != "js@local" || delivery.From.Product != "controlled-js-fixture" || delivery.MessageID != report.OrdinaryMessageID {
			t.Fatal("continuation sender/result mismatch", delivery)
		}
	case <-ctx.Done():
		t.Fatal("missing ordinary continuation", ctx.Err())
	}
	// The original directory inode contains only its staged file. Neither named
	// destination, the replacement parent nor another path gained publication.
	entries, err := os.ReadDir(parent + "-moved")
	must(t, err)
	if len(entries) != 1 || !strings.HasPrefix(entries[0].Name(), ".sessionbus-transfer-") || !strings.Contains(report.Reason, entries[0].Name()) {
		t.Fatal(entries, report)
	}
	raw, err := os.ReadFile(filepath.Join(parent+"-moved", entries[0].Name()))
	must(t, err)
	if !bytes.Equal(raw, data) {
		t.Fatal("incomplete real stream")
	}
	entries, err = os.ReadDir(parent)
	must(t, err)
	if len(entries) != 0 {
		t.Fatal("publication redirected into replacement parent", entries)
	}
	if _, err = os.Stat(filepath.Join(parent+"-moved", "destination")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unexpected publication", err)
	}
	_, err = stdin.Write([]byte("release\n"))
	must(t, err)
	must(t, stdin.Close())
	if err = command.Wait(); err != nil {
		t.Fatal(err, stderr.String())
	}
}

type cleanupWorkerExecutor struct{ mode string }

func (cleanupWorkerExecutor) OpenTransferSource(context.Context, string) (sessionkit.TransferSource, error) {
	return nil, errors.New("unused fixture source")
}
func (x cleanupWorkerExecutor) OpenTransferDestination(context.Context, string, bool) (sessionkit.TransferDestination, error) {
	return cleanupWorkerDestination{mode: x.mode}, nil
}

type cleanupWorkerDestination struct{ mode string }

func (cleanupWorkerDestination) Write(raw []byte) (int, error) { return len(raw), nil }
func (x cleanupWorkerDestination) Commit() (bool, error) {
	if strings.HasSuffix(x.mode, "unknown") {
		return false, sessionkit.ErrTransferCommitUnknown
	}
	if strings.HasSuffix(x.mode, "completed") {
		return true, errors.New("closed-handle cleanup residue")
	}
	return false, errors.New("confirmed publication refusal")
}
func (cleanupWorkerDestination) Abort() error { return errors.New("closed-handle fixture residue") }

func TestTerminalCleanupAcknowledgmentReleasesLaneHoldAndHostQuota(t *testing.T) {
	for _, outcome := range []string{"unknown", "failed", "completed"} {
		t.Run(outcome, func(t *testing.T) {
			products := t.TempDir()
			product := "cleanup-worker-" + outcome
			installFixture(t, products, product)
			t.Setenv("PATH", products+string(os.PathListSeparator)+os.Getenv("PATH"))
			d, socket := startDaemon(t)
			owner := connectPeer(t, socket, "owner", "owner", "shared")
			persistent := true
			grace := int64(1000)
			var lane protocol.LaneSpawnResult
			must(t, owner.call("lane.spawn", protocol.LaneSpawnRequest{Name: "cleanup", Product: product, ExtraGroups: []string{"shared"}, Persistent: &persistent, AutoCloseMS: &grace, Open: &protocol.OpenOptions{}}, &lane))
			sourcePath := filepath.Join(t.TempDir(), "source")
			must(t, os.WriteFile(sourcePath, []byte("data"), 0o600))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			offer, err := owner.peer.Transfers.Offer(ctx, lane.SessionID, sourcePath, "file")
			must(t, err)
			var terminal protocol.TransferNotice
			for terminal.Kind != "terminal" || terminal.ID != offer.ID {
				select {
				case delivery := <-owner.deliveries:
					var n protocol.TransferNotice
					if json.Unmarshal([]byte(delivery.Body), &n) == nil {
						terminal = n
					}
				case <-ctx.Done():
					t.Fatal("missing native-free terminal", ctx.Err())
				}
			}
			if terminal.Outcome != outcome || terminal.Reason == "" {
				t.Fatal(terminal)
			}
			awaitCleanupCondition(t, func() bool {
				u := d.transferBudget.Usage()
				return u.Active == 0 && u.Pending == 0 && u.ControlledBytes == 0
			})
			var row protocol.SessionListResult
			must(t, owner.call("session.list", protocol.SessionListRequest{SessionID: lane.SessionID}, &row))
			if len(row.Sessions) != 1 || !row.Sessions[0].Connected {
				t.Fatal("resources retired only after lane exit", row)
			}
			awaitCleanupCondition(t, func() bool {
				if owner.call("session.list", protocol.SessionListRequest{SessionID: lane.SessionID}, &row) != nil {
					return false
				}
				return len(row.Sessions) == 1 && !row.Sessions[0].Connected
			})
		})
	}
}

type cleanupWorkerProduct struct {
	fixtureProduct
	worker *sessionkit.Worker
}

func (p *cleanupWorkerProduct) Run(ctx context.Context, run *sessionkit.Run, seed sessionkit.RunInput) (sessionkit.TurnResult, error) {
	// An idle lane receives the ordinary offer as its Run seed, not Deliver.
	if seed.Delivery != nil {
		var notice protocol.TransferNotice
		if json.Unmarshal([]byte(seed.Delivery.Body), &notice) == nil && notice.Kind == "offer" {
			if _, err := p.worker.Transfers().Accept(ctx, notice.ID, "/private/fixture-destination", false); err != nil {
				return sessionkit.TurnResult{}, err
			}
			if err := run.ReportDelivery(sessionkit.DeliveryReceipt{Disposition: "injected"}, nil); err != nil {
				return sessionkit.TurnResult{}, err
			}
			return sessionkit.TurnResult{Outcome: "completed", Result: "accepted fixture transfer"}, nil
		}
	}
	return p.fixtureProduct.Run(ctx, run, seed)
}

func (p *cleanupWorkerProduct) Deliver(ctx context.Context, delivery sessionkit.DeliveryRequest, _ *sessionkit.Run) (sessionkit.DeliveryReceipt, error) {
	var n protocol.TransferNotice
	if json.Unmarshal([]byte(delivery.Body), &n) == nil && n.Kind == "offer" {
		if _, err := p.worker.Transfers().Accept(ctx, n.ID, "/private/fixture-destination", false); err != nil {
			return sessionkit.DeliveryReceipt{}, err
		}
	}
	return sessionkit.DeliveryReceipt{Disposition: "injected"}, nil
}
