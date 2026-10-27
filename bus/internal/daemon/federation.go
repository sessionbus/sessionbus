// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"slices"
	"sort"
	"time"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type federationLink struct {
	transfer     bool
	transferBulk chan federation.TransferCall
	bridge       *transferBridge
	roster       bool
	inbox        chan any
	cancel       context.CancelFunc
	done         chan struct{}
}

func (d *Daemon) StartFederation(ctx context.Context, fd net.Conn, stderr io.Writer) error {
	_, err := d.startFederation(ctx, fd, stderr)
	return err
}

func (d *Daemon) startFederation(ctx context.Context, fd net.Conn, stderr io.Writer) (*federationLink, error) {
	linkCtx, cancel := context.WithCancel(ctx)
	link := &federationLink{transfer: federation.SupportsTransfer(fd), transferBulk: make(chan federation.TransferCall, 4), inbox: make(chan any, conn.OutboxSize), cancel: cancel, roster: federation.SupportsRoster(fd), done: make(chan struct{})}
	d.directory.mu.Lock()
	if d.federation != nil || d.directory.closing || linkCtx.Err() != nil {
		d.directory.mu.Unlock()
		cancel()
		_ = fd.Close()
		return nil, errors.New("federation link already connected or closing")
	}
	link.bridge = newTransferBridge(d, link, linkCtx)
	d.federation = link
	d.group.Add(1)
	d.directory.mu.Unlock()
	admit := d.directory.admitFederation
	if link.roster {
		admit = d.admitFederationWithRoster
	}
	go func() {
		defer d.group.Done()
		defer close(link.done)
		_ = federation.ServeDaemonWithTransfers(linkCtx, d.host, fd, link.inbox, admit, stderr, federation.TransferServices{Server: link.bridge.server, Bulk: link.transferBulk, Gate: link.bridge.destinationGate}, d.directory.remoteEnded)
		cancel()
		d.directory.remoteEnded(federation.LifetimeEvent{})
		d.directory.mu.Lock()
		if d.federation == link {
			d.federation = nil
			settleFederationQueue(link.inbox)
		}
		d.directory.mu.Unlock()
	}()
	return link, nil
}

const (
	federationDialTimeout = 5 * time.Second
	federationRetryMin    = 250 * time.Millisecond
	federationRetryMax    = 30 * time.Second
)

func (d *Daemon) connectFederation() error {
	_, port, err := net.SplitHostPort(d.config.HubAddress)
	if err != nil {
		return fmt.Errorf("invalid federation hub address: %w", err)
	}
	if _, err = net.LookupPort("tcp", port); err != nil || port == "" {
		return errors.New("invalid federation hub port")
	}
	configuration, err := federation.ClientTLS(d.host, d.config.HubSecret)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	d.directory.mu.Lock()
	if d.directory.closing || d.federationCancel != nil {
		d.directory.mu.Unlock()
		cancel()
		return errors.New("federation connection owner already started or closing")
	}
	d.federationCancel = cancel
	d.group.Add(1)
	d.directory.mu.Unlock()
	go d.reconnectFederation(ctx, configuration)
	return nil
}

// Each connection has its own queue and lifetime. Lost operations are settled
// by StartFederation before another connection is admitted; none are replayed.
func (d *Daemon) reconnectFederation(ctx context.Context, configuration *tls.Config) {
	defer d.group.Done()
	delay := federationRetryMin
	dialer := tls.Dialer{NetDialer: &net.Dialer{Timeout: federationDialTimeout}, Config: configuration}
	for ctx.Err() == nil {
		attemptCtx, cancel := context.WithTimeout(ctx, federationDialTimeout)
		fd, err := dialer.DialContext(attemptCtx, "tcp", d.config.HubAddress)
		cancel()
		if err == nil {
			connected := time.Now()
			link, linkErr := d.startFederation(ctx, fd, io.Discard)
			if linkErr == nil {
				// The link inherits ctx, so shutdown cancels and joins its transport,
				// handlers and remote ownership before the supervisor returns.
				<-link.done
				if time.Since(connected) >= federationRetryMax {
					delay = federationRetryMin
				}
			}
		}
		// Jitter avoids synchronized redials after a shared hub outage. Repeated
		// TLS rejection or short-lived connections retain the increasing delay.
		pause := delay/2 + time.Duration(rand.Int64N(int64(delay/2)))
		timer := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		delay = min(delay*2, federationRetryMax)
	}
}

func settleFederationQueue(inbox chan any) {
	for {
		select {
		case value := <-inbox:
			lost := federation.Reply{Error: &protocol.RPCError{Code: protocol.ForwardLost, Message: "forward_lost"}}
			switch call := value.(type) {
			case federation.TransferCall:
				call.Reply <- lost
			case federation.OutgoingCall:
				call.Reply <- lost
			case federation.OwnerEndCall:
				call.Reply <- lost
			case federation.HostsCall:
				call.Reply <- lost
			case federation.RosterCall:
				call.Reply <- lost
			}
		default:
			return
		}
	}
}

func (d *directory) postFederation(value any) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.daemon.federation == nil {
		return protocol.UnknownHost
	}
	select {
	case d.daemon.federation.inbox <- value:
		return 0
	default:
		d.daemon.federation.cancel()
		return protocol.ForwardLost
	}
}

func (s *session) postAggregate(frame protocol.Frame, value any) int {
	code := s.daemon.directory.postFederation(value)
	if code != 0 && code != protocol.UnknownHost {
		s.error(frame, code, nil)
	}
	return code
}

func (s *session) federationCaller() federation.Caller {
	if s.caller != nil {
		value := *s.caller
		value.Groups = append([]string(nil), value.Groups...)
		return value
	}
	s.daemon.directory.mu.Lock()
	defer s.daemon.directory.mu.Unlock()
	token := ""
	if s.identity.lifetime != nil {
		token = s.identity.lifetime.token
	}
	return federation.Caller{OwnerLifetime: token, SessionID: s.identity.row.SessionID, Name: s.identity.row.Name,
		Product: s.identity.row.Product, PrivateGroup: privateGroup(s.identity), Groups: append([]string(nil), s.identity.row.Groups...)}
}

func (s *session) deliveryOmit() *entry {
	if s.omit != nil {
		return s.omit
	}
	return s.identity
}

func listSelfInfo(caller federation.Caller) *protocol.SessionSelfInfo {
	return &protocol.SessionSelfInfo{SessionID: caller.SessionID, Name: caller.Name,
		Product: caller.Product, Groups: append([]string{}, caller.Groups...)}
}

func (s *session) forward(frame protocol.Frame, targetID string) {
	reply := make(chan federation.Reply, 1)
	caller := s.federationCaller()
	call := federation.OutgoingCall{Value: federation.Forward{From: caller,
		Request: federation.PublicRequest{Method: frame.Method, Params: append(json.RawMessage(nil), frame.Params...)}}, Reply: reply}
	if code := s.daemon.directory.postCallerForward(s, call, targetID); code != 0 {
		s.error(frame, code, nil)
		return
	}
	s.requests[frame.ID] = &requestState{frame: frame, targetID: targetID}
	if frame.Method == "session.list" {
		s.requests[frame.ID].selfInfo = listSelfInfo(caller)
	}
	s.awaitRemote(frame.ID, 0, frame.Method, reply, s.identity.done)
}

func (s *session) awaitRemote(requestID int64, leg int, method string, reply <-chan federation.Reply, lifetime <-chan struct{}) {
	s.owned++
	go func() {
		value := answer{code: protocol.NotConnected}
		select {
		case remote := <-reply:
			value = remoteAnswer(method, remote)
		case <-lifetime:
		}
		s.inbox <- replyEvent{requestID: requestID, leg: leg, answer: value}
	}()
}

func remoteAnswer(method string, reply federation.Reply) answer {
	result := answer{remote: &reply, trace: reply.Trace}
	if reply.Error != nil {
		result.code = reply.Error.Code
		if len(reply.Error.Data) != 0 {
			_ = json.Unmarshal(reply.Error.Data, &result.data)
		}
		return result
	}
	value, err := protocol.DecodeResult(method, reply.Result)
	if err != nil {
		return answer{code: protocol.ForwardLost}
	}
	result.value = value
	return result
}

func (s *session) remoteResponse(frame protocol.Frame, reply federation.Reply) {
	if s.forwarded != nil {
		s.forwarded <- reply
		return
	}
	body, err := federation.ResponseBytes(frame.ID, reply)
	if err != nil || !s.wire.Send(body) {
		s.wire.Close()
	}
}

func (s *session) listFederated(frame protocol.Frame) bool {
	hosts := make(chan federation.Reply, 1)
	if code := s.postAggregate(frame, federation.HostsCall{Reply: hosts}); code != 0 {
		return code != protocol.UnknownHost
	}
	caller, lifetime := s.federationCaller(), s.identity.done
	local := s.daemon.directory.forwardLocal(caller, s.identity, "session.list", &protocol.SessionListRequest{Host: s.daemon.host}, "")
	s.requests[frame.ID] = &requestState{frame: frame}
	s.owned++
	go func() {
		value := s.collectFederatedList(caller, local, hosts, lifetime)
		s.inbox <- replyEvent{requestID: frame.ID, answer: value}
	}()
	return true
}

func (s *session) collectFederatedList(caller federation.Caller, local federation.Wait, hosts <-chan federation.Reply, lifetime <-chan struct{}) answer {
	var hostReply federation.Reply
	select {
	case hostReply = <-hosts:
	case <-lifetime:
		return answer{code: protocol.NotConnected}
	}
	names, err := federation.DecodeHosts(hostReply)
	if err != nil {
		if hostReply.Error != nil {
			return answer{code: hostReply.Error.Code}
		}
		return answer{code: protocol.ForwardLost}
	}
	replies := make([]chan federation.Reply, len(names))
	for index, host := range names {
		params, _ := protocol.EncodeParams("session.list", protocol.SessionListRequest{Host: host})
		replies[index] = make(chan federation.Reply, 1)
		call := federation.OutgoingCall{Value: federation.Forward{From: caller,
			Request: federation.PublicRequest{Method: "session.list", Params: params}}, Reply: replies[index]}
		if code := s.daemon.directory.postFederation(call); code != 0 {
			return answer{code: protocol.ForwardLost}
		}
	}
	localReply, ok := local(lifetime)
	if !ok {
		return answer{code: protocol.NotConnected}
	}
	value := remoteAnswer("session.list", localReply)
	if value.code != 0 {
		return value
	}
	result := value.value.(*protocol.SessionListResult)
	result.SelfInfo = listSelfInfo(caller)
	for _, reply := range replies {
		var remote federation.Reply
		select {
		case remote = <-reply:
		case <-lifetime:
			return answer{code: protocol.NotConnected}
		}
		value := remoteAnswer("session.list", remote)
		if value.code != 0 {
			return answer{code: protocol.ForwardLost}
		}
		listed := value.value.(*protocol.SessionListResult)
		result.Sessions = append(result.Sessions, listed.Sessions...)
		result.Hosts = append(result.Hosts, listed.Hosts...)
	}
	sort.Slice(result.Sessions, func(left, right int) bool { return result.Sessions[left].SessionID < result.Sessions[right].SessionID })
	sort.Slice(result.Hosts, func(left, right int) bool { return result.Hosts[left].Host < result.Hosts[right].Host })
	return answer{value: result}
}

type messageLeg struct {
	labels []string
	wait   federation.Wait
	reply  <-chan federation.Reply
	code   int
}

// Trace collection is optional for ordinary sends; the federation transport
// downgrades it on legacy links without altering ordinary delivery.
func (s *session) collectTraceLeg() bool {
	return s.traceCopy == nil && (s.forwarded == nil || s.traceCollect)
}

func (s *session) newMessageLeg(caller federation.Caller, messageID, host string, input *protocol.MessageSendRequest, labels []string) messageLeg {
	leg := messageLeg{labels: labels}
	if host == s.daemon.host {
		leg.wait = s.daemon.directory.forwardLocalTrace(caller, s.identity, "message.send", input, messageID, s.collectTraceLeg(), s.traceCopy, s.completion)
	} else if host == "local" {
		leg.code = protocol.UnknownHost
	} else {
		if s.traceCopy != nil {
			input = &protocol.MessageSendRequest{Target: s.traceCopy.SessionID, Message: input.Message}
		}
		params, _ := protocol.EncodeParams("message.send", input)
		reply := make(chan federation.Reply, 1)
		leg.reply, leg.code = reply, s.daemon.directory.postFederation(federation.OutgoingCall{Value: federation.Forward{From: caller,
			Request: federation.PublicRequest{Method: "message.send", Params: params, MessageID: messageID, Trace: s.collectTraceLeg(), TraceCopy: s.traceCopy, Completion: s.completion}}, Reply: reply})
	}
	return leg
}

func (s *session) sendFederated(frame protocol.Frame, input *protocol.MessageSendRequest, messageID string) bool {
	labels := input.Targets
	if input.Target != "" {
		labels = []string{input.Target}
	}
	caller := s.federationCaller()
	if input.Group != "" {
		if input.Host != "" {
			if input.Host == s.daemon.host {
				return false
			}
			leg := s.newMessageLeg(caller, messageID, input.Host, input, nil)
			s.startWork(frame, func(done <-chan struct{}) answer {
				return collectMessageSend(messageID, []messageLeg{leg}, nil, done, protocol.ForwardLost)
			})
			return true
		}
		hosts := make(chan federation.Reply, 1)
		if code := s.postAggregate(frame, federation.HostsCall{Reply: hosts}); code != 0 {
			return code != protocol.UnknownHost
		}
		localInput := *input
		localInput.Host = s.daemon.host
		local := s.daemon.directory.forwardLocalTrace(caller, s.identity, frame.Method, &localInput, messageID, s.collectTraceLeg(), s.traceCopy, s.completion)
		s.startWork(frame, func(done <-chan struct{}) answer {
			return s.collectGroupSend(caller, input, messageID, local, hosts, done)
		})
		return true
	}
	type hostLabels struct {
		host   string
		labels []string
	}
	partitions := make([]hostLabels, 0, len(labels))
	byHost := map[string]int{}
	local := true
	for _, label := range labels {
		_, host, code := canonicalTarget(label, s.daemon.host, validNamePart)
		if code != 0 {
			s.reject(frame, protocol.InvalidFrame)
			return true
		}
		local = local && host == s.daemon.host
		index, ok := byHost[host]
		if !ok {
			index = len(partitions)
			byHost[host] = index
			partitions = append(partitions, hostLabels{host: host})
		}
		partitions[index].labels = append(partitions[index].labels, label)
	}
	if local {
		return false
	}
	legs := make([]messageLeg, 0, len(partitions))
	for _, partition := range partitions {
		request := &protocol.MessageSendRequest{Targets: partition.labels, Message: input.Message}
		legs = append(legs, s.newMessageLeg(caller, messageID, partition.host, request, partition.labels))
	}
	s.startWork(frame, func(done <-chan struct{}) answer {
		return collectMessageSend(messageID, legs, labels, done, protocol.NotConnected)
	})
	return true
}

func (s *session) startWork(frame protocol.Frame, work func(<-chan struct{}) answer) {
	lifetime := s.identity.done
	s.requests[frame.ID] = &requestState{frame: frame, aggregate: true}
	s.owned++
	go func() { s.inbox <- replyEvent{requestID: frame.ID, answer: work(lifetime)} }()
}

func collectMessageSend(messageID string, legs []messageLeg, order []string, done <-chan struct{}, failure int) answer {
	traceRefs := []federation.TraceRecipient{}
	result := &protocol.MessageSendResult{MessageID: messageID, Deliveries: []protocol.MessageSendDelivery{}}
	for _, leg := range legs {
		if leg.code != 0 {
			if len(leg.labels) == 0 {
				return answer{code: leg.code, trace: traceRefs}
			}
			for _, label := range leg.labels {
				result.Deliveries = append(result.Deliveries, protocol.MessageSendDelivery{Target: label, Disposition: "rejected", Reason: reason(leg.code, "no_receipt")})
			}
			continue
		}
		var reply federation.Reply
		var ok bool
		if leg.wait != nil {
			reply, ok = leg.wait(done)
		} else {
			select {
			case reply = <-leg.reply:
				ok = true
			case <-done:
			}
		}
		if !ok {
			return answer{code: failure, trace: traceRefs}
		}
		value := remoteAnswer("message.send", reply)
		remaining := maxTraceRecipients - len(traceRefs)
		traceRefs = append(traceRefs, value.trace[:min(remaining, len(value.trace))]...)
		if value.code != 0 {
			if len(leg.labels) == 0 {
				return answer{code: value.code, trace: traceRefs}
			}
			for _, label := range leg.labels {
				result.Deliveries = append(result.Deliveries, protocol.MessageSendDelivery{Target: label, Disposition: "rejected", Reason: reason(value.code, "no_receipt")})
			}
			continue
		}
		result.Deliveries = append(result.Deliveries, value.value.(*protocol.MessageSendResult).Deliveries...)
	}
	if order != nil {
		sort.SliceStable(result.Deliveries, func(left, right int) bool {
			return slices.Index(order, result.Deliveries[left].Target) < slices.Index(order, result.Deliveries[right].Target)
		})
	} else {
		sort.Slice(result.Deliveries, func(left, right int) bool {
			return result.Deliveries[left].SessionID < result.Deliveries[right].SessionID
		})
	}
	return answer{value: result, trace: traceRefs}
}

func (s *session) collectGroupSend(caller federation.Caller, input *protocol.MessageSendRequest, messageID string, local federation.Wait, hosts <-chan federation.Reply, done <-chan struct{}) answer {
	var roster federation.Reply
	select {
	case roster = <-hosts:
	case <-done:
		return answer{code: protocol.NotConnected}
	}
	names, err := federation.DecodeHosts(roster)
	if err != nil {
		return answer{code: protocol.ForwardLost}
	}
	legs := []messageLeg{{wait: local}}
	for _, host := range names {
		request := &protocol.MessageSendRequest{Group: input.Group, Host: host, Message: input.Message}
		legs = append(legs, s.newMessageLeg(caller, messageID, host, request, nil))
	}
	return collectMessageSend(messageID, legs, nil, done, protocol.ForwardLost)
}

func (d *directory) admitFederation(call federation.IncomingCall) (federation.Wait, error) {
	params, err := protocol.DecodeParams(call.Request.Method, call.Request.Params)
	if err != nil {
		return nil, err
	}
	return d.forwardLocalTrace(call.From, nil, call.Request.Method, params, call.Request.MessageID, call.Request.Trace, call.Request.TraceCopy, call.Request.Completion), nil
}

func (d *directory) forwardLocal(from federation.Caller, omit *entry, method string, params any, messageID string) federation.Wait {
	return d.forwardLocalTrace(from, omit, method, params, messageID, true, nil, false)
}

func (d *directory) forwardLocalTrace(from federation.Caller, omit *entry, method string, params any, messageID string, collect bool, copy *federation.TraceDestination, completion bool) federation.Wait {
	caller := from
	caller.Groups = append([]string(nil), from.Groups...)
	owned := make(chan struct{})
	reply := make(chan federation.Reply, 1)
	s := newSession(d.daemon)
	s.identity = &entry{row: row{SessionID: caller.SessionID, Name: caller.Name, Product: caller.Product,
		Groups: append([]string(nil), caller.Groups...)}, peer: true, done: owned}
	s.caller, s.omit, s.forwarded, s.messageID = &caller, omit, reply, messageID
	s.traceCollect, s.traceCopy, s.completion = collect, copy, completion
	s.dispatchRequest(protocol.Frame{ID: 1, Method: method, Request: true}, params)
	return func(done <-chan struct{}) (federation.Reply, bool) {
		defer s.releaseTraceTargets()
		for {
			select {
			case value := <-reply:
				return value, true
			case event := <-s.inbox:
				s.handleEvent(event)
			case <-done:
				close(owned)
				for s.owned != 0 {
					s.handleEvent(<-s.inbox)
				}
				return federation.Reply{}, false
			}
		}
	}
}

func rpcReply(method string, value answer) federation.Reply {
	if value.code != 0 {
		body, _ := protocol.ErrorBytes(1, value.code, value.data)
		frame, _ := protocol.DecodeFrame(body[:len(body)-1])
		return federation.Reply{Error: frame.Error, ErrorRaw: frame.ErrorRaw}
	}
	raw, err := protocol.EncodeResult(method, value.value)
	if err != nil {
		return rpcReply(method, answer{code: protocol.Internal, data: err.Error()})
	}
	return federation.Reply{Result: json.RawMessage(raw)}
}

func (d *directory) postCallerForward(s *session, call federation.OutgoingCall, target string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.daemon.federation == nil {
		return protocol.UnknownHost
	}
	if (call.Value.Request.Method == "lane.spawn" || call.Value.Request.Method == "trace.configure") && s.caller == nil {
		owner := s.identity.lifetime
		if owner == nil || owner.ended || s.identity.attachment != s {
			return protocol.NotConnected
		}
		_, host, _ := canonicalTarget(target, d.daemon.host, validIDPart)
		owner.destinations[host] = true
	}
	select {
	case d.daemon.federation.inbox <- call:
		return 0
	default:
		d.daemon.federation.cancel()
		return protocol.ForwardLost
	}
}
