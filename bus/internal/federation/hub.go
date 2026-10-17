// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type Hub struct {
	listener net.Listener
	tls      *tls.Config
	registry *Registry
	accepted chan struct{}
}

type Registry struct {
	ctx    context.Context
	cancel context.CancelFunc
	inbox  chan registryEvent
	done   chan struct{}
	log    io.Writer
}

type registryEventKind uint8

const (
	registryAccept registryEventKind = iota + 1
	registryRetire
	registryReady
	registryClosed
	registryHosts
	registryForward
	registryReply
	registryOwnerEnd
	registryRoster
)

type registryEvent struct {
	ownerEnd *incomingOwnerEnd
	kind     registryEventKind
	link     *hostLink
	fd       net.Conn
	tls      *tls.Config
	forward  *incomingForward
	reply    *originReply
	id       int64
	ack      chan bool
}

type hostLink struct {
	attachment  string
	rosterSlots chan struct{}
	ctx         context.Context
	host        string
	fd, raw     net.Conn
	wire        *conn.Conn
	inbox       chan any
	registry    chan<- registryEvent
	done        chan struct{}
	group       sync.WaitGroup
	log         io.Writer
	probeID     int64
	probeSent   time.Time
}

type hubState struct {
	links   map[string]*hostLink
	sockets map[net.Conn]bool
	group   sync.WaitGroup
	log     io.Writer
}

type outgoingForward struct {
	value Forward
	reply chan Reply
}

type incomingForward struct {
	id    int64
	value Forward
	host  string
}

type originReply struct {
	origin      *hostLink
	destination *hostLink
	id          int64
	value       Reply
	collect     bool
}

func StartHub(listener net.Listener, configuration *tls.Config, stderr io.Writer) *Hub {
	h := &Hub{listener: listener, tls: configuration, registry: NewRegistry(stderr), accepted: make(chan struct{})}
	go h.accept()
	return h
}

func (h *Hub) accept() {
	defer close(h.accepted)
	for {
		fd, err := h.listener.Accept()
		if err != nil || !h.registry.Accept(fd, h.tls) {
			return
		}
	}
}

func (h *Hub) Close() {
	_ = h.listener.Close()
	<-h.accepted
	h.registry.Close()
}

func NewRegistry(stderr io.Writer) *Registry {
	ctx, cancel := context.WithCancel(context.Background())
	r := &Registry{ctx: ctx, cancel: cancel, inbox: make(chan registryEvent, conn.OutboxSize), done: make(chan struct{}), log: stderr}
	go r.run()
	return r
}

func (r *Registry) Accept(fd net.Conn, configuration *tls.Config) bool {
	ack := make(chan bool, 1)
	if !postRegistry(r.ctx, r.inbox, registryEvent{kind: registryAccept, fd: fd, tls: configuration, ack: ack}) {
		_ = fd.Close()
		return false
	}
	select {
	case <-ack:
		return true
	case <-r.ctx.Done():
		_ = fd.Close()
		return false
	}
}

func (r *Registry) Close() {
	r.cancel()
	<-r.done
}

func (r *Registry) run() {
	state := hubState{links: map[string]*hostLink{}, sockets: map[net.Conn]bool{}, log: r.log}
	defer close(r.done)
	for {
		select {
		case event := <-r.inbox:
			state.handle(r.ctx, r.inbox, event)
		case <-r.ctx.Done():
			for fd := range state.sockets {
				_ = fd.Close()
			}
			for _, link := range state.links {
				link.wire.Close()
			}
			state.group.Wait()
			return
		}
	}
}

func (s *hubState) handle(ctx context.Context, inbox chan<- registryEvent, event registryEvent) {
	switch event.kind {
	case registryAccept:
		s.sockets[event.fd] = true
		s.group.Add(1)
		go func() { defer s.group.Done(); authenticate(ctx, event.fd, event.tls, inbox, s.log) }()
		event.ack <- true
	case registryRetire:
		delete(s.sockets, event.fd)
	case registryReady:
		delete(s.sockets, event.link.raw)
		accepted := s.links[event.link.host] == nil
		if accepted {
			s.links[event.link.host] = event.link
		} else {
			fmt.Fprintf(s.log, "duplicate federation host %q\n", event.link.host)
			event.link.wire.Close()
		}
		event.ack <- accepted
	case registryClosed:
		if s.links[event.link.host] == event.link {
			s.remove(event.link)
		}
		event.ack <- true
	case registryHosts:
		if s.links[event.link.host] == event.link {
			hosts := make([]string, 0, len(s.links)-1)
			for host := range s.links {
				if host != event.link.host {
					hosts = append(hosts, host)
				}
			}
			sort.Strings(hosts)
			raw, _ := json.Marshal(hostsResult{Hosts: hosts})
			s.reply(originReply{origin: event.link, id: event.id, value: Reply{Result: raw}})
		}
	case registryRoster:
		s.roster(ctx, inbox, event.link, event.id)
	case registryForward:
		s.forward(ctx, inbox, event.link, *event.forward)
	case registryOwnerEnd:
		s.ownerEnd(ctx, inbox, event.link, *event.ownerEnd)
	case registryReply:
		s.reply(*event.reply)
	}
}

const hubHandshakeTimeout = 5 * time.Second

// A roster-capable host link is checked only while a short forwarded call
// waits on it: after linkStallAfter of inbound silence the hub sends one
// federation.roster probe, and closes the link if nothing arrives for
// linkProbeTimeout after the later of that probe and the last inbound frame.
// Variables only so tests can shrink them; each link copies them at start.
var (
	linkStallAfter    = 10 * time.Second
	linkProbeTimeout  = 20 * time.Second
	linkCheckInterval = time.Second
)

func authenticate(ctx context.Context, raw net.Conn, configuration *tls.Config, registry chan<- registryEvent, log io.Writer) {
	fd := tls.Server(raw, configuration)
	handshakeCtx, cancelHandshake := context.WithTimeout(ctx, hubHandshakeTimeout)
	err := fd.HandshakeContext(handshakeCtx)
	cancelHandshake()
	if err != nil {
		fmt.Fprintln(log, err)
		_ = raw.Close()
		postRegistry(ctx, registry, registryEvent{kind: registryRetire, fd: raw})
		return
	}
	link := newHostLink(ctx, fd.ConnectionState().ServerName, fd, raw, registry, log)
	ack := make(chan bool, 1)
	accepted := false
	if postRegistry(ctx, registry, registryEvent{kind: registryReady, link: link, ack: ack}) {
		select {
		case accepted = <-ack:
		case <-ctx.Done():
		}
	}
	if !accepted {
		link.wire.Close()
	}
	link.run(accepted)
}

func newHostLink(ctx context.Context, host string, fd, raw net.Conn, registry chan<- registryEvent, log io.Writer) *hostLink {
	identity := make([]byte, 16)
	if _, err := rand.Read(identity); err != nil {
		panic(err)
	}
	link := &hostLink{rosterSlots: make(chan struct{}, 8), attachment: hex.EncodeToString(identity), ctx: ctx, host: host, fd: fd, raw: raw, inbox: make(chan any, conn.OutboxSize), registry: registry, done: make(chan struct{}), log: log}
	link.wire = conn.Start(fd, link.inbox, &link.group)
	return link
}

func postRegistry(ctx context.Context, target chan<- registryEvent, event registryEvent) bool {
	select {
	case target <- event:
		return true
	case <-ctx.Done():
		return false
	}
}

func (l *hostLink) run(accepted bool) {
	defer func() {
		l.wire.Close()
		l.group.Wait()
		close(l.done)
	}()
	failed, nextID, lastIn := !accepted, int64(0), int64(0)
	stopping := l.ctx.Done()
	pending := map[int64]pendingCall{}
	stallAfter, probeTimeout, checkEvery := linkStallAfter, linkProbeTimeout, linkCheckInterval
	var check <-chan time.Time
	if accepted && SupportsRoster(l.fd) {
		ticker := time.NewTicker(checkEvery)
		defer ticker.Stop()
		check = ticker.C
	}
	for {
		select {
		case <-stopping:
			l.wire.Close()
			failed, stopping = true, nil
		case <-check:
			if failed {
				continue
			}
			now, last := time.Now(), l.wire.LastRead()
			if l.probeID == 0 && now.Sub(last) >= stallAfter && stalledShortCall(pending, now, stallAfter) {
				nextID++
				body, err := requestBytes(nextID, rosterMethod, struct{}{})
				if err != nil || !l.wire.Send(body) {
					failed = true
					l.wire.Close()
					continue
				}
				l.probeID, l.probeSent = nextID, now
			} else if l.probeID != 0 && now.Sub(later(l.probeSent, last)) >= probeTimeout {
				fmt.Fprintf(l.log, "federation host %q silent for %s; closing link\n", l.host, now.Sub(last).Round(time.Millisecond))
				failed = true
				l.wire.Close()
			}
		case raw := <-l.inbox:
			switch event := raw.(type) {
			case outgoingRoster:
				if failed || len(pending) >= maxPendingForwardedPerHost {
					event.reply <- errorReply(protocol.ForwardLost, nil)
					continue
				}
				nextID++
				body, err := requestBytes(nextID, rosterMethod, struct{}{})
				if err != nil || !l.wire.Send(body) {
					event.reply <- errorReply(protocol.ForwardLost, nil)
					continue
				}
				pending[nextID] = pendingCall{method: rosterMethod, reply: event.reply, since: time.Now()}
			case lifetimeCall:
				if failed || len(pending) >= maxPendingForwardedPerHost {
					event.Reply <- errorReply(protocol.ForwardLost, nil)
					l.wire.Close()
					failed = true
					continue
				}
				nextID++
				body, err := requestBytes(nextID, event.Method, event.Value)
				if err != nil || !l.wire.Send(body) {
					event.Reply <- errorReply(protocol.ForwardLost, nil)
					l.wire.Close()
					failed = true
					continue
				}
				pending[nextID] = pendingCall{method: event.Method, reply: event.Reply, since: time.Now()}
			case outgoingForward:
				request := event.value.Request
				if request.Completion && !SupportsWake(l.fd) {
					event.reply <- errorReply(protocol.ForwardLost, nil)
					continue
				}
				if requestRequiresTrace(request) && !SupportsTrace(l.fd) {
					event.reply <- traceUnsupported()
					continue
				}
				if request.Trace && !SupportsTrace(l.fd) {
					request.Trace = false
					event.value.Request = request
				}
				if failed || len(pending) >= maxPendingForwardedPerHost {
					event.reply <- errorReply(protocol.ForwardLost, nil)
					if len(pending) >= maxPendingForwardedPerHost {
						l.wire.Close()
						failed = true
					}
					continue
				}
				nextID++
				body, forwarded, err := forwardRequestBytes(nextID, event.value)
				event.value, request = forwarded, forwarded.Request
				target, targetErr := targetHost(request)
				if targetErr != nil || err != nil {
					event.reply <- errorReply(protocol.InvalidFrame, "forwarded request exceeds the frame limit")
					continue
				}
				if !l.wire.Send(body) {
					event.reply <- errorReply(protocol.ForwardLost, nil)
					failed = true
					continue
				}
				pending[nextID] = pendingCall{method: request.Method, reply: event.reply, trace: request.Trace, target: target, since: time.Now()}
			case originReply:
				body, err := responseBytes(event.id, event.value, true)
				if !failed && (err != nil || !l.wire.Send(body)) {
					failed = true
					l.wire.Close()
				}
			case conn.Frame:
				if !failed {
					if err := l.frame(event, pending, &lastIn); err != nil {
						fmt.Fprintln(l.log, err)
						failed = true
						l.wire.Close()
					}
				}
			case conn.Closed:
				ack := make(chan bool, 1)
				if postRegistry(l.ctx, l.registry, registryEvent{kind: registryClosed, link: l, ack: ack}) {
					select {
					case <-ack:
					case <-l.ctx.Done():
					}
				}
				settlePending(pending)
				l.settleQueued()
				return
			}
		}
	}
}

// Long-holding forwards may legitimately wait; every other call is short.
func stalledShortCall(pending map[int64]pendingCall, now time.Time, stallAfter time.Duration) bool {
	for _, call := range pending {
		switch call.method {
		case "turn.run", "turn.wait", "lane.spawn", "session.close":
			continue
		}
		if now.Sub(call.since) >= stallAfter {
			return true
		}
	}
	return false
}

func later(first, second time.Time) time.Time {
	if first.After(second) {
		return first
	}
	return second
}

func (l *hostLink) settleQueued() {
	for {
		select {
		case value := <-l.inbox:
			switch call := value.(type) {
			case outgoingRoster:
				call.reply <- errorReply(protocol.ForwardLost, nil)
			case outgoingForward:
				call.reply <- errorReply(protocol.ForwardLost, nil)
			case lifetimeCall:
				call.Reply <- errorReply(protocol.ForwardLost, nil)
			}
		default:
			return
		}
	}
}

func (l *hostLink) frame(event conn.Frame, pending map[int64]pendingCall, lastIn *int64) error {
	frame := event.Value
	if event.Err != nil {
		return event.Err
	}
	if !frame.Request {
		// The liveness probe is correlated outside pending, so it never uses
		// forwarded-call capacity; only its validated reply clears it.
		if l.probeID != 0 && frame.ID == l.probeID {
			if _, valid := decodePendingReply(pendingCall{method: rosterMethod}, frame); !valid {
				return errFrame
			}
			l.probeID = 0
			return nil
		}
		call, ok := pending[frame.ID]
		if !ok {
			return errFrame
		}
		reply, valid := decodePendingReply(call, frame)
		if !valid {
			return errFrame
		}
		delete(pending, frame.ID)
		call.reply <- reply
		return nil
	}
	if frame.ID <= *lastIn {
		return errFrame
	}
	*lastIn = frame.ID
	if frame.Method == rosterMethod {
		var params map[string]json.RawMessage
		if !SupportsRoster(l.fd) || protocol.DecodeJSON(frame.Params, &params) != nil || params == nil || len(params) != 0 {
			return errFrame
		}
		postRegistry(l.ctx, l.registry, registryEvent{kind: registryRoster, link: l, id: frame.ID})
		return nil
	}
	if frame.Method == hostsMethod {
		var params map[string]json.RawMessage
		if protocol.DecodeJSON(frame.Params, &params) != nil || params == nil || len(params) != 0 {
			return errFrame
		}
		postRegistry(l.ctx, l.registry, registryEvent{kind: registryHosts, link: l, id: frame.ID})
		return nil
	}
	if frame.Method == ownerEndMethod {
		var value OwnerEnd
		if protocol.DecodeJSON(frame.Params, &value) != nil || !validHost(value.Host) || !ownedPart(value.SessionID, l.host, false) || value.Lifetime == "" || value.Source != "" || value.Attachment != "" {
			return errFrame
		}
		postRegistry(l.ctx, l.registry, registryEvent{kind: registryOwnerEnd, link: l, ownerEnd: &incomingOwnerEnd{frame.ID, value}})
		return nil
	}
	if frame.Method != forwardMethod {
		return errFrame
	}
	value, host, err := decodeForward(frame.Params, l.host)
	if err == nil && value.From.SourceAttachment != "" {
		return errFrame
	}
	if err == nil {
		postRegistry(l.ctx, l.registry, registryEvent{kind: registryForward, link: l, forward: &incomingForward{id: frame.ID, value: value, host: host}})
	}
	return err
}

func settlePending(pending map[int64]pendingCall) {
	for id, call := range pending {
		delete(pending, id)
		call.reply <- errorReply(protocol.ForwardLost, nil)
	}
}

func decodePendingReply(call pendingCall, frame protocol.Frame) (Reply, bool) {
	if frame.Error != nil {
		return Reply{Error: frame.Error, ErrorRaw: append(json.RawMessage(nil), frame.ErrorRaw...)}, true
	}
	if call.trace {
		value, err := decodeTraceReply(call.method, frame.Result, call.target)
		if err == nil {
			return value, true
		}
		if _, plainErr := protocol.DecodeResult(call.method, frame.Result); plainErr == nil {
			return Reply{Result: append(json.RawMessage(nil), frame.Result...), Trace: []TraceRecipient{}}, true
		}
		return Reply{}, false
	}
	if call.method == ownerEndMethod || call.method == hostEndMethod {
		return Reply{Result: append(json.RawMessage(nil), frame.Result...)}, validControlReply(frame)
	}
	if call.method == rosterMethod {
		_, err := decodeRosterReply(frame.Result)
		return Reply{Result: append(json.RawMessage(nil), frame.Result...)}, err == nil
	}
	if call.method == hostsMethod {
		_, err := DecodeHosts(Reply{Result: frame.Result})
		return Reply{Result: append(json.RawMessage(nil), frame.Result...)}, err == nil
	}
	_, err := protocol.DecodeResult(call.method, frame.Result)
	return Reply{Result: append(json.RawMessage(nil), frame.Result...)}, err == nil
}

func (s *hubState) forward(ctx context.Context, inbox chan<- registryEvent, origin *hostLink, call incomingForward) {
	if s.links[origin.host] != origin {
		return
	}
	if call.value.Request.Completion && !SupportsWake(origin.fd) {
		s.reply(originReply{origin: origin, id: call.id, value: errorReply(protocol.ForwardLost, nil)})
		return
	}
	if requestRequiresTrace(call.value.Request) && !SupportsTrace(origin.fd) {
		s.reply(originReply{origin: origin, id: call.id, value: traceUnsupported()})
		return
	}
	if call.value.Request.Trace && !SupportsTrace(origin.fd) {
		call.value.Request.Trace = false
	}
	destination := s.links[call.host]
	if destination == nil {
		s.reply(originReply{origin: origin, id: call.id, value: errorReply(protocol.UnknownHost, nil)})
		return
	}
	if call.value.Request.Completion && !SupportsWake(destination.fd) {
		s.reply(originReply{origin: origin, id: call.id, value: errorReply(protocol.ForwardLost, nil)})
		return
	}
	if requestRequiresTrace(call.value.Request) && !SupportsTrace(destination.fd) {
		s.reply(originReply{origin: origin, id: call.id, value: traceUnsupported()})
		return
	}
	collect := call.value.Request.Trace && SupportsTrace(origin.fd)
	if collect && !SupportsTrace(destination.fd) {
		call.value.Request.Trace = false
	}
	call.value.From.SourceAttachment = origin.attachment
	reply := make(chan Reply, 1)
	if !destination.wire.Post(outgoingForward{call.value, reply}) {
		reply <- errorReply(protocol.ForwardLost, nil)
		s.remove(destination)
	}
	s.group.Add(1)
	go func() {
		defer s.group.Done()
		select {
		case value := <-reply:
			postRegistry(ctx, inbox, registryEvent{kind: registryReply, reply: &originReply{origin: origin, destination: destination, id: call.id, value: value, collect: collect}})
		case <-origin.done:
		case <-ctx.Done():
		}
	}()
}

func (s *hubState) reply(event originReply) {
	if event.collect {
		if event.value.Trace == nil {
			event.value.Trace = []TraceRecipient{}
		} else {
			event.value.Trace = s.liveTraceRecipients(event.destination, event.value.Trace)
		}
	}
	if s.links[event.origin.host] != event.origin || !event.origin.wire.Post(event) {
		s.remove(event.origin)
	}
}

func (s *hubState) liveTraceRecipients(destination *hostLink, values []TraceRecipient) []TraceRecipient {
	if destination == nil || s.links[destination.host] != destination {
		return []TraceRecipient{}
	}
	result := make([]TraceRecipient, 0, len(values))
	for _, value := range values {
		ownerHost, err := suffix(value.Owner.SessionID)
		if err != nil {
			continue
		}
		if ownerHost != destination.host {
			owner := s.links[ownerHost]
			if owner == nil || owner.attachment != value.Owner.SourceAttachment {
				continue
			}
		}
		value.Owner.Groups = append([]string(nil), value.Owner.Groups...)
		result = append(result, value)
	}
	return result
}

func (s *hubState) remove(link *hostLink) {
	if link != nil && s.links[link.host] == link {
		delete(s.links, link.host)
		link.wire.Close()
		for _, other := range s.links {
			if !other.wire.Post(lifetimeCall{Method: hostEndMethod, Value: HostEnd{Host: link.host, Attachment: link.attachment}, Reply: make(chan Reply, 1)}) {
				other.wire.Close()
			}
		}
	}
}

func (s *hubState) ownerEnd(ctx context.Context, inbox chan<- registryEvent, origin *hostLink, call incomingOwnerEnd) {
	if s.links[origin.host] != origin {
		return
	}
	destination := s.links[call.Value.Host]
	if destination == nil {
		s.reply(originReply{origin: origin, id: call.ID, value: errorReply(protocol.UnknownHost, nil)})
		return
	}
	call.Value.Source, call.Value.Attachment = origin.host, origin.attachment
	reply := make(chan Reply, 1)
	if !destination.wire.Post(lifetimeCall{ownerEndMethod, call.Value, reply}) {
		s.remove(destination)
		s.reply(originReply{origin: origin, id: call.ID, value: errorReply(protocol.ForwardLost, nil)})
		return
	}
	s.group.Add(1)
	go func() {
		defer s.group.Done()
		select {
		case value := <-reply:
			postRegistry(ctx, inbox, registryEvent{kind: registryReply, reply: &originReply{origin: origin, destination: destination, id: call.ID, value: value}})
		case <-origin.done:
		case <-ctx.Done():
		}
	}()
}
