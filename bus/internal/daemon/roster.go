// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/internal/roster"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type operatorServer struct {
	listener    net.Listener
	done        chan struct{}
	mu          sync.Mutex
	connections map[net.Conn]bool
	group       sync.WaitGroup
	slots       chan struct{}
}

func (d *Daemon) startOperator() error {
	path := roster.Socket(d.config.SocketPath)
	// Only remove a stale socket, never an arbitrary file or symlink.
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return errors.New("operator endpoint is not a socket")
		}
		fd, dialErr := net.DialTimeout("unix", path, time.Second)
		if dialErr == nil {
			_ = fd.Close()
			return errors.New("operator endpoint is already active")
		}
		if !errors.Is(dialErr, syscall.ECONNREFUSED) {
			return dialErr
		}
		if err = os.Remove(path); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	if err = os.Chmod(path, 0600); err != nil {
		_ = listener.Close()
		return err
	}
	d.operator = &operatorServer{listener: listener, done: make(chan struct{}), connections: map[net.Conn]bool{}, slots: make(chan struct{}, 8)}
	go d.acceptOperator()
	return nil
}

func (d *Daemon) acceptOperator() {
	o := d.operator
	defer close(o.done)
	for {
		fd, err := o.listener.Accept()
		if err != nil {
			return
		}
		select {
		case o.slots <- struct{}{}:
		default:
			_ = fd.Close()
			continue
		}
		o.mu.Lock()
		o.connections[fd] = true
		o.mu.Unlock()
		o.group.Add(1)
		go func() {
			defer o.group.Done()
			defer func() { _ = fd.Close(); o.mu.Lock(); delete(o.connections, fd); o.mu.Unlock(); <-o.slots }()
			d.serveOperator(fd)
		}()
	}
}

func (o *operatorServer) close() {
	_ = o.listener.Close()
	<-o.done // no further admissions or WaitGroup.Add
	o.mu.Lock()
	for fd := range o.connections {
		_ = fd.Close()
	}
	o.mu.Unlock()
	o.group.Wait()
}

func (d *Daemon) serveOperator(fd net.Conn) {
	_ = fd.SetDeadline(time.Now().Add(12 * time.Second))
	raw, err := bufio.NewReaderSize(fd, 1024).ReadSlice('\n')
	if err != nil {
		return
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return
	}
	_, action := fields["action"]
	_, target := fields["session_id"]
	if action || target {
		d.serveArchive(fd, raw)
		return
	}
	var request roster.Request
	if protocol.DecodeJSON(raw, &request) != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	finished := make(chan struct{})
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-d.shutdown:
			cancel()
		case <-finished:
		}
	}()
	value := d.operatorRoster(ctx, request.Local)
	close(finished)
	<-joined
	body := encodeOperatorRoster(value)
	body = append(body, '\n')
	for len(body) > 0 {
		n, e := fd.Write(body)
		if e != nil || n == 0 {
			return
		}
		body = body[n:]
	}
}

// Preserve useful local results when the combined federation exceeds the wire
// bound. Explicit errors distinguish omitted remote data from an empty roster.
func encodeOperatorRoster(value roster.Report) []byte {
	body, err := roster.Encode(value)
	if err == nil {
		return body
	}
	value.Complete, value.Error = false, "roster_too_large"
	stubs := make([]roster.Host, 0, len(value.Remote))
	for _, host := range value.Remote {
		stubs = append(stubs, roster.Host{Host: host.Host, Products: []string{}, Sessions: []roster.Row{}, Error: "roster_too_large"})
	}
	value.Remote = stubs
	if body, err = roster.Encode(value); err == nil {
		return body
	}
	// Host names themselves may exhaust the remaining budget.
	value.Remote = []roster.Host{}
	if body, err = roster.Encode(value); err == nil {
		return body
	}
	// Defensive fallback if even the local projection/configuration is invalid.
	body, _ = json.Marshal(roster.Report{Schema: roster.Schema, Complete: false, Error: "roster_too_large"})
	return body
}

func (d *Daemon) localRoster() roster.Host {
	value := roster.Host{Host: d.host, Products: append([]string{}, d.config.Products...), Sessions: []roster.Row{}}
	d.directory.mu.Lock()
	bytes := 0
	for _, item := range d.directory.entries {
		if !item.peer && item.row.CreatedAt.IsZero() {
			continue
		}
		if len(value.Sessions) >= roster.MaxRows {
			value.Error = "roster_too_large"
			value.Sessions = []roster.Row{}
			break
		}
		kind := "lane"
		if item.peer {
			kind = "peer"
		}
		row := roster.Row{PermissionMode: item.row.Open.PermissionMode, SessionID: item.row.SessionID, Name: item.row.Name, Kind: kind, Product: item.row.Product, Groups: append([]string{}, item.row.Groups...), Connected: item.attachment != nil, Running: item.running}
		if item.row.Policy != nil {
			row.Owner = item.row.Policy.OwnerSessionID
			row.Persistent = item.row.Policy.Persistent
		}
		raw, _ := json.Marshal(row)
		bytes += len(raw) + 1
		if bytes > roster.MaxBytes/2 {
			value.Error = "roster_too_large"
			value.Sessions = []roster.Row{}
			break
		}
		value.Sessions = append(value.Sessions, row)
	}
	d.directory.mu.Unlock()
	sort.Slice(value.Sessions, func(i, j int) bool { return value.Sessions[i].SessionID < value.Sessions[j].SessionID })
	return value
}

func (d *Daemon) operatorRoster(ctx context.Context, local bool) roster.Report {
	result := roster.Report{Schema: roster.Schema, Complete: true, Federation: "not_configured", Local: d.localRoster(), Remote: []roster.Host{}}
	result.Complete = result.Local.Error == ""
	d.directory.mu.Lock()
	link := d.federation
	d.directory.mu.Unlock()
	if link == nil {
		if d.config.HubAddress != "" {
			result.Federation = "disconnected"
			if !local {
				result.Complete = false
			}
		}
		return result
	}
	result.Federation = "connected"
	if local {
		return result
	}
	reply := make(chan federation.Reply, 1)
	var call any = federation.RosterCall{Reply: reply}
	if !link.roster {
		call = federation.HostsCall{Reply: reply}
	}
	// Pin admission to the exact current link without altering ordinary visibility.
	d.directory.mu.Lock()
	if d.federation != link {
		d.directory.mu.Unlock()
		result.Complete = false
		result.Error = "federation_changed"
		return result
	}
	select {
	case link.inbox <- call:
	default:
		d.directory.mu.Unlock()
		result.Complete = false
		result.Error = "federation_busy"
		return result
	}
	d.directory.mu.Unlock()
	select {
	case answer := <-reply:
		if answer.Error != nil {
			result.Complete = false
			result.Error = "federation_roster_unavailable"
			return result
		}
		if !link.roster {
			hosts, err := federation.DecodeHosts(answer)
			if err != nil {
				result.Complete = false
				result.Error = "federation_roster_unavailable"
				return result
			}
			for _, host := range hosts {
				result.Remote = append(result.Remote, roster.Host{Host: host, Products: []string{}, Sessions: []roster.Row{}, Error: "upgrade_hub_for_roster"})
			}
		} else {
			remote, err := roster.DecodeRemote(answer.Result)
			if err != nil {
				result.Complete = false
				result.Error = "invalid_federation_roster"
				return result
			}
			result.Remote = remote.Hosts
		}
		for _, host := range result.Remote {
			if host.Error != "" {
				result.Complete = false
			}
		}
	case <-ctx.Done():
		result.Complete = false
		result.Error = "federation_roster_timeout"
	}
	return result
}

func (d *Daemon) admitFederationWithRoster(call federation.IncomingCall) (federation.Wait, error) {
	if federation.IsRoster(call) {
		value := federation.RosterReply(d.localRoster())
		return func(<-chan struct{}) (federation.Reply, bool) { return value, true }, nil
	}
	return d.directory.admitFederation(call)
}
