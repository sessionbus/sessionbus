// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"bytes"
	"errors"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"
)

type failedRawControl struct{ calls *int }

func (r failedRawControl) Control(func(uintptr)) error {
	*r.calls++
	return errors.New("injected control failure")
}
func (failedRawControl) Read(func(uintptr) bool) error  { return nil }
func (failedRawControl) Write(func(uintptr) bool) error { return nil }

func TestTCPOptionFailuresDoNotSkipTheOtherSetter(t *testing.T) {
	var log bytes.Buffer
	calls := 0
	configureTCP(func(c net.KeepAliveConfig) error {
		if !c.Enable || c.Idle != 60*time.Second || c.Interval != 10*time.Second || c.Count != 9 {
			t.Fatalf("keepalive budget: %#v", c)
		}
		return errors.New("injected keepalive failure")
	}, func() (syscall.RawConn, error) { return failedRawControl{&calls}, nil }, &log)
	if calls != 1 || strings.Count(log.String(), "federation TCP ") != 2 {
		t.Fatalf("option attempts/logs: %d %q", calls, log.String())
	}
}
