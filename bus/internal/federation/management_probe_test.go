// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestReservedProbeAtFullDispatchedManagementCap(t *testing.T) {
	inbox, frames, remote := heldManagementLink(t)
	for range protocol.MaxOperations {
		inbox <- HostsCall{Reply: make(chan Reply, 1)}
		_ = managementFrame(t, frames)
	}
	inbox <- livenessTick{at: time.Now()}
	probe := managementFrame(t, frames)
	if probe.ID != protocol.MaxOperations+1 || probe.Method != hostsMethod {
		t.Fatal("full pending cap hid reserved probe", probe)
	}
	inbox <- livenessTick{at: time.Now()}
	select {
	case duplicate := <-frames:
		t.Fatal("probe reservation grew while one was dispatched", duplicate)
	case <-time.After(25 * time.Millisecond):
	}
	body, _ := ResponseBytes(probe.ID, Reply{Result: []byte(`{"hosts":[]}`)})
	if _, err := remote.Write(body); err != nil {
		t.Fatal("reserved response ended link", err)
	}
	// This ordinary call remains refused: a probe reply frees only its own
	// reservation, never one of the 256 still-pending ordinary correlations.
	refused := make(chan Reply, 1)
	inbox <- HostsCall{Reply: refused}
	if r := <-refused; r.Error == nil || r.Error.Code != protocol.ForwardLost {
		t.Fatal("probe freed ordinary charge", r)
	}
}
