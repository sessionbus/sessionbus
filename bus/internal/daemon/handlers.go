// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"slices"

	"github.com/antst/sessionbus/bus/internal/commslog"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type requestState struct {
	frame      protocol.Frame
	target     *entry
	targetID   string
	held       *answer
	pending    int
	deliveries []protocol.MessageSendDelivery
	messageID  string
	aggregate  bool
	selfInfo   *protocol.SessionSelfInfo
}

func (s *session) handleRequest(frame protocol.Frame, params any) {
	if !s.daemon.directory.current(s.identity, s) {
		s.supersede()
		return
	}
	s.dispatchRequest(frame, params)
}

func (s *session) dispatchRequest(frame protocol.Frame, params any) {
	if len(s.requests) >= protocol.MaxOperations && frame.Method != "turn.ready" {
		messageID := ""
		if frame.Method == "message.send" {
			messageID = s.messageID
			if messageID == "" {
				messageID = randomID("message")
			}
		}
		s.beginTrace(frame, params, messageID)
		s.logRequest(frame, params, messageID)
		s.error(frame, protocol.Busy, nil)
		return
	}
	if frame.Method != "message.send" {
		s.logRequest(frame, params, "")
	}
	switch frame.Method {
	case "session.list":
		s.list(frame, params.(*protocol.SessionListRequest))
	case "message.send":
		s.send(frame, params.(*protocol.MessageSendRequest))
	case "lane.describe":
		s.describe(frame, params.(*protocol.LaneDescribeRequest))
	case "trace.configure":
		s.configureTrace(frame, params.(*protocol.TraceConfigureRequest))
	case "lane.spawn":
		s.spawn(frame, params.(*protocol.LaneSpawnRequest))
	case "turn.run", "turn.start":
		s.route(frame, params.(*protocol.TurnRunRequest).SessionID, params)
	case "turn.status":
		s.route(frame, params.(*protocol.ReadRequest).SessionID, params)
	case "turn.wait":
		s.route(frame, params.(*protocol.WaitRequest).SessionID, params)
	case "turn.ack":
		s.route(frame, params.(*protocol.RunRef).SessionID, params)
	case "turn.ready":
		s.turnReady(frame, params.(*protocol.TurnReady))
	case "turn.interrupt":
		s.route(frame, params.(*protocol.SessionTarget).SessionID, params)
	case "session.close":
		s.route(frame, params.(*protocol.SessionCloseRequest).SessionID, params)
	default:
		s.reject(frame, protocol.InvalidFrame)
	}
}

func (s *session) peerHello(frame protocol.Frame, hello *protocol.PeerHello) {
	if s.identity != nil && !s.identity.peer || !validIDPart(hello.SessionID) || hello.Name != "" && !validNamePart(hello.Name) || !validHost(hello.Product) {
		s.reject(frame, protocol.InvalidHello)
		return
	}
	item, displaced, ended, ok := s.daemon.directory.installPeer(s, hello, s.daemon.host)
	if !ok {
		s.reject(frame, protocol.InvalidHello)
		return
	}
	if ended != nil {
		s.settlePending(protocol.NotConnected)
		s.detachRequests(protocol.Superseded)
	}
	s.identity = item
	s.logSession(commslog.Connected)
	if displaced != nil && !displaced.wire.Post(supersedeEvent{}) {
		displaced.wire.Close()
	}
	s.result(frame, struct{}{})
}

func (s *session) list(frame protocol.Frame, input *protocol.SessionListRequest) {
	caller := s.federationCaller()
	if input.SessionID != "" {
		canonical, host, code := canonicalTarget(input.SessionID, s.daemon.host, validNamePart)
		if code == protocol.InvalidFrame {
			s.reject(frame, code)
			return
		}
		if host != s.daemon.host {
			s.forward(frame, canonical)
			return
		}
	}
	if input.Host != "" && input.Host != s.daemon.host {
		if input.Host == "local" {
			s.error(frame, protocol.UnknownHost, nil)
			return
		}
		s.forward(frame, "list@"+input.Host)
		return
	}
	if input.SessionID == "" && input.Host == "" && s.listFederated(frame) {
		return
	}
	var labels []string
	if input.SessionID != "" {
		labels = []string{input.SessionID}
	}
	items, code := s.daemon.directory.selectEntries(caller.Groups, labels, "", frame.Method, input, nil, nil)
	if code == protocol.InvalidFrame {
		s.reject(frame, code)
		return
	}
	if labels != nil && (len(items) == 0 || items[0].code != 0 || items[0].item == nil || items[0].ambiguous) {
		code = protocol.UnknownSession
		if len(items) != 0 && items[0].code != 0 {
			code = items[0].code
		}
		s.error(frame, code, nil)
		return
	}
	result := protocol.SessionListResult{SelfInfo: listSelfInfo(caller), Sessions: make([]protocol.SessionSummary, len(items))}
	for index := range items {
		result.Sessions[index] = items[index].summary
	}
	if s.daemon.config.Products != nil {
		result.Hosts = []protocol.HostProducts{{Host: s.daemon.host, Products: append([]string(nil), s.daemon.config.Products...)}}
	}
	s.result(frame, result)
}

func (s *session) route(frame protocol.Frame, label string, params any) {
	caller := s.federationCaller()
	canonical, host, code := canonicalTarget(label, s.daemon.host, validIDPart)
	if code == protocol.InvalidFrame {
		s.reject(frame, code)
		return
	}
	if host != s.daemon.host {
		s.forward(frame, canonical)
		return
	}
	reply := make(chan answer, 1)
	request := routedRequest{method: frame.Method, params: params, reply: reply}
	items, code := s.daemon.directory.selectEntries(caller.Groups, []string{label}, "", frame.Method, params, nil, &request)
	if code == protocol.InvalidFrame {
		s.reject(frame, code)
		return
	}
	item := items[0]
	if item.item == nil || item.ambiguous {
		code = item.code
		if code == 0 {
			code = protocol.UnknownSession
		}
		s.error(frame, code, nil)
		return
	}
	if item.code != 0 {
		s.error(frame, item.code, nil)
		return
	}
	s.requests[frame.ID] = &requestState{frame: frame, target: item.item, pending: 1}
	s.await(frame.ID, 0, reply, s.identity.done)
}

func (s *session) describe(frame protocol.Frame, input *protocol.LaneDescribeRequest) {
	if input.Host != "" && input.Host != s.daemon.host {
		if input.Host != "local" {
			s.forward(frame, "describe@"+input.Host)
			return
		}
		s.error(frame, protocol.UnknownHost, nil)
		return
	}
	if !validHost(input.Product) {
		s.error(frame, protocol.UnknownProduct, nil)
		return
	}
	start := newLaunch(input.Product, true, false)
	start.entry = &entry{row: row{Product: input.Product}, claimed: true, done: make(chan struct{})}
	if code := s.daemon.directory.reserveDescribe(start); code != 0 {
		start.timer.Stop()
		s.error(frame, code, internalData(code))
		return
	}
	s.launchRequest(frame, start)
}

func (s *session) spawn(frame protocol.Frame, input *protocol.LaneSpawnRequest) {
	parent := s.daemon.directory.callerOwner(s)
	if parent == nil {
		s.error(frame, protocol.NotConnected, nil)
		return
	}
	caller := s.federationCaller()
	resumeID := ""
	if input.Host != "" && input.Host != s.daemon.host {
		if input.Host != "local" {
			s.forward(frame, "spawn@"+input.Host)
			return
		}
		s.error(frame, protocol.UnknownHost, nil)
		return
	}
	if input.ResumeSessionID != "" {
		id, host, code := canonicalTarget(input.ResumeSessionID, s.daemon.host, validIDPart)
		if code != 0 {
			s.reject(frame, code)
			return
		}
		if host != s.daemon.host {
			s.forward(frame, id)
			return
		}
		resumeID = id
	}
	if resumeID != "" {
		start := newLaunch("", false, false)
		start.parent, start.input = parent, input
		_, code := s.daemon.directory.reserveResume(resumeID, caller.Groups, start)
		if code != 0 {
			start.timer.Stop()
			s.error(frame, code, internalData(code))
			return
		}
		s.launchRequest(frame, start)
		return
	}
	if !validHost(input.Product) {
		s.error(frame, protocol.UnknownProduct, nil)
		return
	}
	parentName := unqualify(caller.Name)
	composedName := parentName + "/" + input.Name
	if !validNamePart(input.Name) || !validNamePart(composedName) || input.Open == nil {
		s.reject(frame, protocol.InvalidFrame)
		return
	}
	parentGroup := caller.PrivateGroup
	private := parentGroup + "/" + input.Name
	groups := unique(append([]string{parentGroup, private}, input.ExtraGroups...))
	policy, err := normalizePolicy(input, nil, caller.SessionID)
	if err != nil {
		s.error(frame, protocol.UnsupportedOpen, nil)
		return
	}
	value := row{Policy: policy, Product: input.Product, Name: qualify(composedName, s.daemon.host), Groups: groups, Open: *input.Open}
	start := newLaunch(input.Product, false, true)
	start.parent, start.input = parent, input
	_, code := s.daemon.directory.reserveFresh(value, start)
	if code != 0 {
		start.timer.Stop()
		s.error(frame, code, internalData(code))
		return
	}
	s.launchRequest(frame, start)
}

func (s *session) launchRequest(frame protocol.Frame, start *launch) {
	s.requests[frame.ID] = &requestState{frame: frame, pending: 1}
	s.await(frame.ID, 0, start.reply, s.identity.done)
	s.daemon.startProduct(start)
}

func (s *session) send(frame protocol.Frame, input *protocol.MessageSendRequest) {
	caller := s.federationCaller()
	if !s.validTraceCopy(input) {
		s.error(frame, protocol.NotConnected, nil)
		return
	}
	messageID := s.messageID
	if messageID == "" {
		messageID = randomID("message")
	}
	s.beginTrace(frame, input, messageID)
	s.logRequest(frame, input, messageID)
	if s.sendFederated(frame, input, messageID) {
		return
	}
	labels := input.Targets
	if input.Target != "" {
		labels = []string{input.Target}
	}
	if input.Group != "" {
		labels = nil
	}
	state := &requestState{frame: frame, messageID: messageID, deliveries: []protocol.MessageSendDelivery{}}
	items, code := s.daemon.directory.selectEntries(caller.Groups, labels, input.Group, frame.Method, input, s.deliveryOmit(), nil)
	if code == protocol.InvalidFrame {
		s.reject(frame, code)
		return
	}
	targets := make([]*entry, 0, len(labels))
	seen := map[*entry]bool{}
	for _, selected := range items {
		delivery := protocol.MessageSendDelivery{Target: selected.label}
		if selected.ambiguous {
			delivery.Disposition, delivery.Reason = "rejected", "ambiguous"
		} else if selected.code != 0 || selected.item == nil {
			delivery.Disposition, delivery.Reason = "rejected", reason(selected.code, "unknown_session")
		} else if seen[selected.item] {
			continue
		} else {
			seen[selected.item] = true
		}
		state.deliveries = append(state.deliveries, delivery)
		targets = append(targets, selected.item)
	}
	deliveryRequest := protocol.DeliveryRequest{MessageID: state.messageID,
		From: protocol.DeliverySource{SessionID: caller.SessionID, Name: caller.Name,
			Product: caller.Product, Groups: append([]string(nil), caller.Groups...)}, Body: input.Message}
	for index, item := range targets {
		if item == nil {
			continue
		}
		s.traceTarget(frame.ID, item, state.deliveries[index].Target)
		reply := make(chan answer, 1)
		request := routedRequest{traceCopy: s.traceCopy != nil, completion: s.completion, method: "message.deliver", params: deliveryRequest, reply: reply}
		if s.traceCopy != nil {
			request.traceLifetime = s.traceCopy.Lifetime
		}
		code := s.daemon.directory.route(item, request.method, request)
		if code != 0 {
			state.deliveries[index].Disposition = "rejected"
			state.deliveries[index].Reason = reason(code, "no_receipt")
			if code == protocol.NotConnected {
				// route did not enqueue this delivery. This proof does not apply
				// to NotConnected replies after dispatch or transport loss.
				state.deliveries[index].Reason = "not_submitted"
			}
			continue
		}
		state.deliveries[index].SessionID = item.row.SessionID
		state.deliveries[index].DeliveryID = randomID("delivery")
		s.logDispatch(state.messageID, state.deliveries[index].DeliveryID, state.deliveries[index].Target, item.row.SessionID)
		state.pending++
		s.await(frame.ID, index, reply, s.identity.done)
	}
	if state.pending == 0 {
		s.result(frame, protocol.MessageSendResult{MessageID: state.messageID, Deliveries: state.deliveries})
		return
	}
	s.requests[frame.ID] = state
}

func (s *session) consumeReply(event replyEvent) {
	if s.owned > 0 {
		s.owned--
	}
	state := s.requests[event.requestID]
	if state == nil {
		return
	}
	if state.frame.Method == "message.send" && !state.aggregate {
		delivery := &state.deliveries[event.leg]
		if event.answer.code != 0 {
			delivery.Disposition, delivery.Reason = "rejected", reason(event.answer.code, "no_receipt")
		} else if receipt, ok := event.answer.value.(*protocol.DeliveryReceipt); ok {
			delivery.Disposition, delivery.Reason = receipt.Disposition, receipt.Reason
		} else {
			delivery.Disposition, delivery.Reason = "rejected", "no_receipt"
		}
		state.pending--
		if state.pending != 0 {
			return
		}
		delete(s.requests, event.requestID)
		s.result(state.frame, protocol.MessageSendResult{MessageID: state.messageID, Deliveries: state.deliveries})
		return
	}
	if state.frame.Method == "session.close" && event.answer.code == 0 && s.runPending(state.target, state.targetID) {
		value := event.answer
		state.held = &value
		return
	}
	s.finishRequest(event.requestID, state, event.answer)
}

func (s *session) finishRequest(id int64, state *requestState, result answer) {
	delete(s.requests, id)
	s.logResult(state.frame, result.value, result.code)
	s.finishTrace(state.frame, result.value, result.code, result.trace)
	if result.code == 0 && state.selfInfo != nil {
		// A directed remote list must identify our captured caller, even when
		// an older destination omits self_info or reports a different identity.
		listed := result.value.(*protocol.SessionListResult)
		listed.SelfInfo = state.selfInfo
		result.remote = nil
	}
	if result.code != 0 {
		if result.remote != nil {
			s.remoteResponse(state.frame, *result.remote)
		} else {
			s.error(state.frame, result.code, result.data)
		}
	} else if result.remote != nil {
		s.remoteResponse(state.frame, *result.remote)
	} else {
		s.result(state.frame, result.value)
	}
	if state.frame.Method != "turn.run" || s.runPending(state.target, state.targetID) {
		return
	}
	for closeID, closeState := range s.requests {
		if closeState.frame.Method == "session.close" && (closeState.target != nil && closeState.target == state.target || closeState.targetID != "" && closeState.targetID == state.targetID) && closeState.held != nil {
			s.finishRequest(closeID, closeState, *closeState.held)
			return
		}
	}
}

func (s *session) runPending(target *entry, targetID string) bool {
	for _, state := range s.requests {
		if state.frame.Method == "turn.run" && (target != nil && state.target == target || targetID != "" && state.targetID == targetID) {
			return true
		}
	}
	return false
}

func (s *session) detachRequests(code int) {
	for id, state := range s.requests {
		delete(s.requests, id)
		s.error(state.frame, code, nil)
	}
}

func reason(code int, fallback string) string {
	switch code {
	case protocol.UnknownHost:
		return "unknown_host"
	case protocol.Busy:
		return "busy"
	case protocol.NotConnected:
		return "no_receipt"
	case protocol.UnknownSession:
		return "unknown_session"
	default:
		return fallback
	}
}

func internalData(code int) any {
	if code == protocol.Internal {
		return "daemon shutting down"
	}
	return nil
}

func unique(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if !slices.Contains(result, value) {
			result = append(result, value)
		}
	}
	return result
}
