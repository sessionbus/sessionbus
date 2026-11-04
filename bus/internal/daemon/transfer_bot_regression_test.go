// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestDEV1BotDisabledCapabilityCanBeReenabled(t *testing.T) {
	for _, sequence := range [][]bool{{false, true}, {true, false, true}} {
		t.Run("transition", func(t *testing.T) {
			defer func() {
				if got := recover(); got != nil {
					t.Errorf("capability rehello panicked: %v", got)
				}
			}()
			d := &directory{}
			item := &entry{}
			for _, enabled := range sequence {
				d.setTransferCapability(item, enabled)
			}
		})
	}
}

func TestBridgeHoldDoesNotBlockReaderGateAndRetiresLateGrant(t *testing.T) {
	x, _ := bridgeTerminalFixture(t, "preparing")
	entered, release := make(chan struct{}), make(chan struct{})
	var released atomic.Int32
	x.endpoint.Hold = func(_ context.Context, _ string, terminal bool) bool {
		if terminal {
			released.Add(1)
			return true
		}
		close(entered)
		<-release
		return true
	}
	raw, _ := json.Marshal(federation.TransferHold{Binding: x.token, ID: x.id})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = x.bridge.server(context.Background(), federation.TransferHoldMethod, raw)
	}()
	<-entered
	gate := make(chan bool, 1)
	go func() { gate <- x.bridge.canChunk("", x.id, "destination") }()
	stalled := false
	select {
	case v := <-gate:
		if v {
			t.Error("preparing admitted a chunk")
		}
	case <-time.After(25 * time.Millisecond):
		stalled = true
	}
	terminal := make(chan struct{})
	go func() {
		defer close(terminal)
		_ = x.terminal(protocol.TransferNotice{Kind: "terminal", Reason: "endpoint_closing"})
	}()
	close(release)
	<-done
	<-terminal
	if stalled {
		<-gate
		t.Error("shared-reader chunk gate waited for owner Hold")
	}
	if released.Load() != 1 {
		t.Errorf("late hold release count %d", released.Load())
	}
}
