// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/antst/sessionbus/bus/internal/transfer"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type transferNoticeEvent struct {
	entry  *entry
	key    string
	notice protocol.TransferNotice
	reply  chan answer
}

type transferCallEvent struct {
	entry   *entry
	key     string
	request routedRequest
}

func (d *directory) transferCurrent(item *entry, owner *session, key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return item != nil && d.entries[item.row.SessionID] == item && item.attachment == owner && item.transferKey == key && item.transferCapable
}
func (d *directory) setTransferCapability(item *entry, enabled bool) {
	if item.transferCapable == enabled && item.transferDone != nil {
		return
	}
	if item.transferDone != nil {
		close(item.transferDone)
	}
	item.transferCapable = enabled
	item.transferKey = randomID("transfer-owner")
	item.transferDone = make(chan struct{})
	if item.attachment != nil {
		key := item.transferKey
		if !enabled {
			key = ""
		}
		item.attachment.transferKey.Store(&key)
	}
	if !enabled {
		close(item.transferDone)
	}
}
func (d *directory) transferEndpoint(item *entry) (transfer.Endpoint, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if item == nil || d.entries[item.row.SessionID] != item || item.attachment == nil || !item.transferCapable || item.claimed {
		return transfer.Endpoint{}, transfer.ErrUnsupported
	}
	owner, key, done := item.attachment, item.transferKey, item.transferDone
	identity := protocol.DeliverySource{SessionID: item.row.SessionID, Product: item.row.Product, Name: item.row.Name, Groups: append([]string(nil), item.row.Groups...)}
	endpoint := transfer.Endpoint{Key: key, Identity: identity, Done: done}
	endpoint.Call = func(ctx context.Context, method string, params, result any) error {
		reply := make(chan answer, 1)
		request := routedRequest{destination: item, method: method, params: params, reply: reply}
		select {
		case owner.inbox <- transferCallEvent{entry: item, key: key, request: request}:
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return transfer.ErrUnsupported
		}
		select {
		case response := <-reply:
			if response.code != 0 {
				return &protocol.RPCError{Code: response.code, Message: "transfer_endpoint_unavailable"}
			}
			raw, err := json.Marshal(response.value)
			if err != nil {
				return err
			}
			return json.Unmarshal(raw, result)
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return transfer.ErrUnsupported
		}
	}
	endpoint.Hold = func(ctx context.Context, id string, terminal bool) bool {
		reply := make(chan bool, 1)
		event := transferHoldEvent{entry: item, key: key, id: id, terminal: terminal, reply: reply}
		select {
		case owner.inbox <- event:
		case <-ctx.Done():
			return false
		case <-done:
			return false
		}
		select {
		case ok := <-reply:
			return ok
		case <-ctx.Done():
			return false
		case <-done:
			return false
		}
	}

	endpoint.Notice = func(ctx context.Context, n protocol.TransferNotice) error {
		reply := make(chan answer, 1)
		select {
		case owner.inbox <- transferNoticeEvent{entry: item, key: key, notice: n, reply: reply}:
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return transfer.ErrUnsupported
		}
		select {
		case response := <-reply:
			if response.code != 0 {
				return transfer.ErrState
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return transfer.ErrUnsupported
		}
	}

	return endpoint, nil
}
func (s *session) transferRequest(frame protocol.Frame, params any) {
	origin, err := s.daemon.directory.transferEndpoint(s.identity)
	if err != nil {
		s.error(frame, protocol.UnsupportedOpen, "unsupported_transfer")
		return
	}
	if s.forwardTransfer(frame, params, origin) {
		return
	}
	var other transfer.Endpoint
	switch p := params.(type) {
	case *protocol.TransferOffer:
		other, err = s.transferTarget(p.Target)
	case *protocol.TransferRequest:
		other, err = s.transferTarget(p.Target)
	}
	if err != nil {
		s.error(frame, protocol.UnsupportedOpen, "unsupported_transfer")
		return
	}
	reply := make(chan answer, 1)
	s.requests[frame.ID] = &requestState{frame: frame, pending: 1}
	s.await(frame.ID, 0, reply, s.identity.done)
	go func() {
		engine := s.daemon.transfers
		var value any
		var err error
		created := ""
		switch p := params.(type) {
		case *protocol.TransferOffer:
			var d protocol.TransferDescriptor
			d, err = engine.Create(origin, other, true, p.Name, "")
			value = d
			if err == nil {
				created = d.ID
			}
		case *protocol.TransferRequest:
			var d protocol.TransferDescriptor
			d, err = engine.Create(origin, other, false, p.Name, p.What)
			value = d
			if err == nil {
				created = d.ID
			}
		case *protocol.TransferAccept:
			value, err = engine.Accept(origin.Key, *p)
		case *protocol.TransferChunk:
			value, err = engine.Chunk(origin.Key, *p)
		case *protocol.TransferFinish:
			value, err = engine.Finish(origin.Key, *p)
		case *protocol.TransferFailure:
			value, err = engine.Fail(origin.Key, *p)
		case *protocol.TransferTarget:
			if frame.Method == "transfer.status" {
				value, err = engine.Status(origin.Key, p.ID)
			} else {
				value, err = engine.Cancel(origin.Key, p.ID)
			}
		default:
			err = transfer.ErrUnsupported
		}
		response := answer{value: value}
		if err != nil {
			response.code = protocol.Internal
			response.data = "transfer_failed"
			if errors.Is(err, transfer.ErrCapacity) {
				response.code = protocol.Busy
			}
			if errors.Is(err, transfer.ErrUnknown) {
				response.code = protocol.UnknownSession
			}
		}
		if created != "" {
			response.after = func() { engine.Announce(created) }
		}
		reply <- response
	}()
}
func (s *session) transferTarget(target string) (transfer.Endpoint, error) {
	id, host, code := canonicalTarget(target, s.daemon.host, validIDPart)
	if code != 0 || host != s.daemon.host {
		return transfer.Endpoint{}, transfer.ErrUnsupported
	}
	d := s.daemon.directory
	d.mu.Lock()
	items := matchingEntries(d.connected, id, s.identity.row.Groups)
	var other *entry
	if len(items) == 1 {
		other = items[0]
	}
	d.mu.Unlock()
	return d.transferEndpoint(other)
}

func (s *session) bulkAdmitted(frame protocol.Frame) bool {
	key := s.transferKey.Load()
	if key == nil || *key == "" {
		return false
	}
	var p struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(frame.Params, &p) != nil {
		return false
	}
	if s.daemon.transfers.CanChunk(*key, p.ID) {
		return true
	}
	s.daemon.directory.mu.Lock()
	link := s.daemon.federation
	s.daemon.directory.mu.Unlock()
	return link != nil && link.transfer && link.bridge.canChunk(*key, p.ID, "source")
}

func (s *session) transferNotice(event transferNoticeEvent) {
	if !s.daemon.directory.transferCurrent(event.entry, s, event.key) || s.stopping {
		event.reply <- answer{code: protocol.NotConnected}
		return
	}
	n := event.notice
	machine := routedRequest{destination: event.entry, method: "transfer.notice", params: n, reply: event.reply}
	ordinary := func() {
		body, err := json.Marshal(n)
		if err != nil {
			return
		}
		from := protocol.DeliverySource{SessionID: "transfer@" + s.daemon.host, Product: "sessionbus", Name: "Sessionbus transfer", Groups: append([]string(nil), event.entry.row.Groups...)}
		if n.From != nil {
			from = *n.From
		}
		// The existing worker message.deliver path decides seed/steer/queue policy.
		// No direct SDK model callback, forced wake or new native input path is used.
		s.issue(routedRequest{destination: event.entry, method: "message.deliver", params: protocol.DeliveryRequest{MessageID: randomID("transfer-notice"), From: from, Body: string(body)}, reply: make(chan answer, 1)})
	}
	if n.Kind == "terminal" {
		// Wire-reader order lets only the destination SDK decorate this ordinary
		// notice while its local binding still exists. No destination enters wire.
		ordinary()
		s.issue(machine)
	} else {
		// Initial notice installs the SDK binding before the ordinary delivery.
		reply := make(chan answer, 1)
		machine.reply = reply
		s.issue(machine)
		done := event.entry.transferDone
		s.owned++
		go func() {
			response := answer{code: protocol.NotConnected}
			select {
			case response = <-reply:
			case <-done:
			}
			s.inbox <- transferNoticeInstalled{event: event, response: response}
		}()
	}
}

type transferNoticeInstalled struct {
	event    transferNoticeEvent
	response answer
}

func (s *session) transferNoticeInstalled(e transferNoticeInstalled) {
	if s.owned > 0 {
		s.owned--
	}
	if e.response.code == 0 && s.daemon.directory.transferCurrent(e.event.entry, s, e.event.key) && !s.stopping {
		n := e.event.notice
		body, err := json.Marshal(n)
		if err == nil {
			from := *n.From
			s.issue(routedRequest{destination: e.event.entry, method: "message.deliver", params: protocol.DeliveryRequest{MessageID: randomID("transfer-notice"), From: from, Body: string(body)}, reply: make(chan answer, 1)})
		}
	}
	e.event.reply <- e.response
}
