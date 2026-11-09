// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"crypto/tls"
	"io"
	"net"
	"testing"

	"golang.org/x/sys/unix"
)

type observedTCPListener struct {
	net.Listener
	accepted chan *net.TCPConn
}

func (l observedTCPListener) Accept() (net.Conn, error) {
	fd, err := l.Listener.Accept()
	if err == nil {
		l.accepted <- fd.(*net.TCPConn)
	}
	return fd, err
}

// This test uses the existing public constructor and fails on main's previous
// default 15-second keepalive; it does not call the new option helper itself.
func TestHubAcceptedTCPUsesThe150SecondBudget(t *testing.T) {
	secret := testSecret('a')
	server, err := ServerTLS(map[string]string{"alpha": secret})
	if err != nil {
		t.Fatal(err)
	}
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := observedTCPListener{Listener: base, accepted: make(chan *net.TCPConn, 1)}
	hub := StartHub(listener, server, io.Discard)
	t.Cleanup(hub.Close)
	client, err := ClientTLS("alpha", secret)
	if err != nil {
		t.Fatal(err)
	}
	fd, err := tls.Dial("tcp", base.Addr().String(), client)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fd.Close() })
	tcp := <-listener.accepted
	raw, err := tcp.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var readErr error
	values := map[int]int{}
	err = raw.Control(func(fd uintptr) {
		for _, option := range []int{unix.TCP_KEEPIDLE, unix.TCP_KEEPINTVL, unix.TCP_KEEPCNT, unix.TCP_USER_TIMEOUT} {
			var e error
			values[option], e = unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, option)
			if e != nil {
				readErr = e
			}
		}
	})
	if err != nil || readErr != nil {
		t.Fatalf("read options: %v/%v", err, readErr)
	}
	if values[unix.TCP_KEEPIDLE] != 60 || values[unix.TCP_KEEPINTVL] != 10 || values[unix.TCP_KEEPCNT] != 9 || values[unix.TCP_USER_TIMEOUT] != 150000 {
		t.Fatalf("effective accepted TCP options: %#v", values)
	}
}
