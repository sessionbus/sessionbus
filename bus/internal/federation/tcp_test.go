// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"bytes"
	"errors"
	"net"
	"runtime"
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
	wantCalls := 0
	if runtime.GOOS == "linux" || runtime.GOOS == "darwin" {
		wantCalls = 1
	} else if !strings.Contains(log.String(), "unsupported on this platform") {
		t.Fatalf("fallback did not report unsupported drop-time option: %q", log.String())
	}
	if calls != wantCalls || strings.Count(log.String(), "federation TCP ") != 2 {
		t.Fatalf("option attempts/logs: %d %q", calls, log.String())
	}
}
