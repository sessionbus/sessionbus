// SPDX-License-Identifier: MIT

package sessionkit

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type heldCleanupSource struct {
	entered, release chan struct{}
	closed           atomic.Int32
}

func (*heldCleanupSource) Read([]byte) (int, error) { return 0, io.EOF }
func (*heldCleanupSource) Size() int64              { return 0 }
func (*heldCleanupSource) Verify() error            { return nil }
func (s *heldCleanupSource) Close() error {
	s.closed.Add(1)
	close(s.entered)
	<-s.release
	return errors.New("closed /private/source: private cleanup diagnostic")
}

func TestSourceCloseFailureProjectsLocallyAfterRetirement(t *testing.T) {
	for _, outcome := range []string{"cancelled", "completed", "unknown"} {
		t.Run(outcome, func(t *testing.T) {
			g, b, _ := localTransferBinding(t, nil)
			source := &heldCleanupSource{entered: make(chan struct{}), release: make(chan struct{})}
			b.source = source
			g.owner.current = g
			reason := strings.Repeat("界", 680)
			zero := protocol.Integer(0)
			terminal := protocol.TransferNotice{ID: b.id, Kind: "terminal", Outcome: outcome, Bytes: &zero, Reason: reason}
			raw, err := protocol.EncodeParams("transfer.notice", terminal)
			if err != nil {
				t.Fatal(err)
			}
			d := protocol.DeliveryRequest{MessageID: "transfer-notice-owned", Body: string(raw)}
			project := g.owner.ProjectDelivery(nil, &d)
			if project == nil {
				close(source.release)
				t.Fatal("source cleanup projector missing")
			}
			projected := make(chan error, 1)
			go func() { projected <- project(context.Background(), &d) }()
			ended := make(chan error, 1)
			go func() { _, err := g.notice(terminal); ended <- err }()
			<-source.entered
			select {
			case <-projected:
				t.Error("projection overtook owned source Close")
			case <-time.After(time.Millisecond):
			}
			close(source.release)
			if err := <-ended; err != nil {
				t.Fatal(err)
			}
			if err := <-projected; err != nil {
				t.Fatal(err)
			}
			var body map[string]any
			if err := json.Unmarshal([]byte(d.Body), &body); err != nil {
				t.Fatal(err)
			}
			if body["outcome"] != outcome || terminal.Reason != reason {
				t.Fatal("cleanup changed authoritative terminal", d.Body)
			}
			if _, ok := body["destination"]; ok || strings.Contains(d.Body, "/private/") || strings.Contains(d.Body, "diagnostic") {
				t.Fatal("source projection leaked a path or private diagnostic", d.Body)
			}
			localReason, ok := body["reason"].(string)
			if !ok || !strings.HasSuffix(localReason, "; cleanup_failed") || len(localReason) > 2048 || !utf8.ValidString(localReason) {
				t.Fatal("lost or unbounded local source cleanup reason", d.Body)
			}
			if source.closed.Load() != 1 || g.owner.pending != 0 || g.owner.active != 0 || g.binding(b.id) != nil {
				t.Fatal("source terminal did not join/release exactly once")
			}
			g.end(b)
			if source.closed.Load() != 1 || g.owner.ProjectDelivery(nil, &d) != nil {
				t.Fatal("duplicate terminal resurrected/reclosed source")
			}
		})
	}
}
