// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"fmt"
	"io"
	"net"
	"syscall"
	"time"
)

// ConfigureTCP requests the same nominal no-progress budget in both directions.
// It runs before TLS. A socket-option failure is observable, never a reason to
// destroy ownership or skip the remaining option setters.
func ConfigureTCP(fd net.Conn, log io.Writer) {
	tcp, ok := fd.(*net.TCPConn)
	if !ok {
		return // Local/in-memory test transports have no TCP options.
	}
	configureTCP(tcp.SetKeepAliveConfig, tcp.SyscallConn, log)
}

func configureTCP(keepAlive func(net.KeepAliveConfig) error, raw func() (syscall.RawConn, error), log io.Writer) {
	if err := keepAlive(net.KeepAliveConfig{Enable: true, Idle: 60 * time.Second, Interval: 10 * time.Second, Count: 9}); err != nil {
		fmt.Fprintf(log, "federation TCP keepalive: %v\n", err)
	}
	connection, err := raw()
	if err == nil {
		err = setTCPDropTime(connection)
	}
	if err != nil {
		fmt.Fprintf(log, "federation TCP no-progress option: %v\n", err)
	}
}
