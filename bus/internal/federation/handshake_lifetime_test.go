// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/roster"
)

func TestHubExpiresSilentTLSClientWithoutStoppingHealthyHost(t *testing.T) {
	_, address, secrets := testHub(t)
	silent, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	alpha := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(call IncomingCall) (Wait, error) {
		if IsRoster(call) {
			return immediate(RosterReply(roster.Host{Host: "alpha", Products: []string{}, Sessions: []roster.Row{}})), nil
		}
		return immediate(Reply{}), nil
	})
	if hosts := callHosts(t, alpha); len(hosts) != 0 {
		t.Fatalf("unexpected other hosts: %v", hosts)
	}
	// An incomplete handshake must expire without requiring hub shutdown.
	_ = silent.SetReadDeadline(time.Now().Add(7 * time.Second))
	if _, err = silent.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("silent TLS client was not closed by the hub: %v", err)
	}
	if hosts := callHosts(t, alpha); len(hosts) != 0 {
		t.Fatalf("healthy authenticated link changed: %v", hosts)
	}
}
