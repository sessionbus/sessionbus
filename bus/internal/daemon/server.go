// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/antst/sessionbus/bus/internal/commslog"
	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/internal/roster"
	"github.com/antst/sessionbus/bus/internal/transfer"
	"github.com/antst/sessionbus/bus/sdk/go/socketpath"
)

type Config struct {
	CommsLog    commslog.Options
	now         func() time.Time
	policyTimer func(time.Duration) (<-chan time.Time, func())
	SocketPath  string
	TablePath   string
	Host        string
	Products    []string
	HubAddress  string
	HubSecret   string
}

type Daemon struct {
	transferBudget   *transfer.Budget
	transfers        *transfer.Engine
	traceContext     context.Context
	traceCancel      context.CancelFunc
	traceSource      string
	traceBytes       int
	traceCount       int
	traceDropped     uint64
	comms            *commslog.Logger
	config           Config
	host             string
	table            *table
	directory        *directory
	listener         net.Listener
	operator         *operatorServer
	shutdown         chan struct{}
	done             chan struct{}
	acceptDone       chan struct{}
	group            sync.WaitGroup
	federation       *federationLink
	federationCancel context.CancelFunc
}

func Start(config Config) (*Daemon, error) {
	if config.now == nil {
		config.now = time.Now
	}
	if config.policyTimer == nil {
		config.policyTimer = func(delay time.Duration) (<-chan time.Time, func()) {
			timer := time.NewTimer(delay)
			return timer.C, func() { timer.Stop() }
		}
	}
	if config.Host == "" {
		config.Host = "local"
	}
	if !validHost(config.Host) {
		return nil, errors.New("invalid sessionbus host")
	}
	if (config.HubAddress == "") != (config.HubSecret == "") || config.HubAddress != "" && config.Host == "local" {
		return nil, errors.New("hub address, non-local host, and hub secret must be configured together")
	}
	seen := map[string]bool{}
	for _, product := range config.Products {
		if !validHost(product) {
			return nil, errors.New("invalid advertised product")
		}
		if seen[product] {
			return nil, errors.New("duplicate advertised product")
		}
		seen[product] = true
	}
	if len(config.Products) == 0 {
		config.Products = nil
	}
	config.CommsLog.Host = config.Host
	config.CommsLog.Incarnation = randomID("daemon")
	var logger *commslog.Logger
	if config.CommsLog.Mode != "" && config.CommsLog.Mode != commslog.Off {
		var err error
		logger, err = commslog.Open(config.CommsLog)
		if err != nil {
			return nil, err
		}
	}
	started := false
	defer func() {
		if !started && logger != nil {
			_ = logger.Close()
		}
	}()
	store, rows, err := openTable(config.TablePath)
	if err != nil {
		return nil, err
	}
	if err = socketpath.PrepareSockets(config.SocketPath, roster.Socket(config.SocketPath)); err != nil {
		return nil, err
	}
	sweepSockets(config.SocketPath)
	listener, err := net.Listen("unix", config.SocketPath)
	if err != nil {
		return nil, err
	}
	if err = os.Chmod(config.SocketPath, 0o600); err != nil {
		_ = listener.Close()
		return nil, err
	}
	d := &Daemon{comms: logger, config: config, host: config.Host, table: store, listener: listener,
		shutdown: make(chan struct{}), done: make(chan struct{}), acceptDone: make(chan struct{})}
	d.traceContext, d.traceCancel = context.WithCancel(context.Background())
	d.transferBudget = transfer.HostBudget()
	d.transfers = transfer.NewEngine(d.traceContext, d.transferBudget)
	d.traceSource = randomID("trace") + "@" + d.host
	d.directory = newDirectory(d, rows)
	if err = d.startOperator(); err != nil {
		_ = listener.Close()
		return nil, err
	}
	if config.HubAddress != "" {
		if err = d.connectFederation(); err != nil {
			close(d.shutdown)
			d.operator.close()
			_ = listener.Close()
			_ = os.Remove(config.SocketPath)
			return nil, err
		}
	}
	started = true
	d.logEvent(commslog.Event{Type: commslog.Lifecycle, Method: "daemon.start"})
	go d.accept()
	return d, nil
}

func sweepSockets(socket string) {
	fd, err := net.Dial("unix", socket)
	if err == nil {
		_ = fd.Close()
	} else if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, os.ErrNotExist) {
		_ = os.Remove(socket)
	}
}

func (d *Daemon) Done() <-chan struct{} { return d.done }

func (d *Daemon) accept() {
	defer close(d.acceptDone)
	for {
		fd, err := d.listener.Accept()
		if err != nil {
			return
		}
		d.group.Add(1)
		s := newSession(d)
		s.wire = conn.Start(fd, s.inbox, &d.group)
		s.wire.SetBulkGate(s.bulkAdmitted)
		go func() { defer d.group.Done(); s.run() }()
	}
}

func (d *Daemon) Close() error {
	d.directory.mu.Lock()
	if d.directory.closing {
		d.directory.mu.Unlock()
		<-d.done
		return nil
	}
	d.directory.closing = true
	if d.traceCancel != nil {
		d.traceCancel()
	}
	if d.federationCancel != nil {
		d.federationCancel()
	}
	if d.federation != nil {
		d.federation.cancel()
	}
	d.directory.mu.Unlock()
	_ = d.listener.Close()
	close(d.shutdown)
	if d.operator != nil {
		d.operator.close()
	}
	<-d.acceptDone
	d.group.Wait()
	_ = os.Remove(d.config.SocketPath)
	d.logEvent(commslog.Event{Type: commslog.Lifecycle, Method: "daemon.stop"})
	var logErr error
	if d.comms != nil {
		logErr = d.comms.Close()
	}
	close(d.done)
	return logErr
}
