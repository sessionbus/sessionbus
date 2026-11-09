// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"context"
	"encoding/base64"
	"io"
	"net"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/antst/sessionbus/bus/internal/federation"
)

func TestFederationDialedTCPUsesThe150SecondBudget(t *testing.T) {
	secret := base64.StdEncoding.EncodeToString(make([]byte, federation.SecretBytes))
	server, err := federation.ServerTLS(map[string]string{"alpha": secret})
	must(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	hub := federation.StartHub(listener, server, io.Discard)
	t.Cleanup(hub.Close)
	client, err := federation.ClientTLS("alpha", secret)
	must(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fd, err := dialFederation(ctx, listener.Addr().String(), client, io.Discard)
	must(t, err)
	t.Cleanup(func() { _ = fd.Close() })
	raw, err := fd.NetConn().(*net.TCPConn).SyscallConn()
	must(t, err)
	values := map[int]int{}
	var readErr error
	err = raw.Control(func(fd uintptr) {
		for _, option := range []int{unix.TCP_KEEPIDLE, unix.TCP_KEEPINTVL, unix.TCP_KEEPCNT, unix.TCP_USER_TIMEOUT} {
			values[option], readErr = unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, option)
			if readErr != nil {
				return
			}
		}
	})
	must(t, err)
	must(t, readErr)
	if values[unix.TCP_KEEPIDLE] != 60 || values[unix.TCP_KEEPINTVL] != 10 || values[unix.TCP_KEEPCNT] != 9 || values[unix.TCP_USER_TIMEOUT] != 150000 {
		t.Fatal("dial path did not apply explicit no-progress budget", values)
	}
}
