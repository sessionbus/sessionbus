// SPDX-License-Identifier: MIT

package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"
	"unicode/utf8"
)

var errInvalid = errors.New("invalid session value")

type methodCodec struct {
	params, result       string
	newParams, newResult func() any
	client, daemon       bool
}

func newValue[T any]() any { return new(T) }

var methodCodecs = map[string]methodCodec{
	"transfer.offer":     {"TransferOffer", "TransferDescriptor", newValue[TransferOffer], newValue[TransferDescriptor], true, false},
	"transfer.request":   {"TransferRequest", "TransferDescriptor", newValue[TransferRequest], newValue[TransferDescriptor], true, false},
	"transfer.accept":    {"TransferAccept", "TransferDescriptor", newValue[TransferAccept], newValue[TransferDescriptor], true, false},
	"transfer.status":    {"TransferTarget", "TransferDescriptor", newValue[TransferTarget], newValue[TransferDescriptor], true, false},
	"transfer.cancel":    {"TransferTarget", "TransferTerminal", newValue[TransferTarget], newValue[TransferTerminal], true, false},
	"transfer.fail":      {"TransferFailure", "TransferTerminal", newValue[TransferFailure], newValue[TransferTerminal], true, false},
	"transfer.prepare":   {"TransferPrepare", "TransferPrepared", newValue[TransferPrepare], newValue[TransferPrepared], false, true},
	"transfer.start":     {"TransferStart", "TransferStarted", newValue[TransferStart], newValue[TransferStarted], false, true},
	"transfer.chunk":     {"TransferChunk", "TransferCredit", newValue[TransferChunk], newValue[TransferCredit], true, true},
	"transfer.finish":    {"TransferFinish", "TransferTerminal", newValue[TransferFinish], newValue[TransferTerminal], true, true},
	"transfer.notice":    {"TransferNotice", "SessionCloseResult", newValue[TransferNotice], newValue[struct{}], false, true},
	"session.hello":      {"SessionHelloRequest", "SessionHelloResult", nil, newValue[HelloResult], true, false},
	"session.superseded": {"SessionSupersededRequest", "SessionSupersededResult", newValue[struct{}], newValue[struct{}], false, true},
	"session.list":       {"SessionListRequest", "SessionListResult", newValue[SessionListRequest], newValue[SessionListResult], true, false},
	"message.send":       {"MessageSendRequest", "MessageSendResult", newValue[MessageSendRequest], newValue[MessageSendResult], true, false},
	"message.deliver":    {"MessageDeliverRequest", "MessageDeliverResult", newValue[DeliveryRequest], newValue[DeliveryReceipt], false, true},
	"lane.describe":      {"LaneDescribeRequest", "LaneDescribeResult", newValue[LaneDescribeRequest], newValue[LaneDescribeResult], true, false},
	"lane.spawn":         {"LaneSpawnRequest", "LaneSpawnResult", newValue[LaneSpawnRequest], newValue[LaneSpawnResult], true, false},
	"trace.configure":    {"TraceConfigureRequest", "TraceConfigureResult", newValue[TraceConfigureRequest], newValue[TraceConfigureResult], true, false},
	"session.open":       {"SessionOpenRequest", "SessionOpenResult", newValue[OpenRequest], newValue[OpenResult], false, true},
	"turn.run":           {"TurnRunRequest", "RunStatus", newValue[TurnRunRequest], newValue[RunStatus], true, false},
	"turn.start":         {"TurnRunRequest", "RunRef", newValue[TurnRunRequest], newValue[RunRef], true, false},
	"turn.execute":       {"ExecuteRequest", "RunRef", newValue[ExecuteRequest], newValue[RunRef], false, true},
	"turn.status":        {"ReadRequest", "RunStatus", newValue[ReadRequest], newValue[RunStatus], true, true},
	"turn.wait":          {"WaitRequest", "RunStatus", newValue[WaitRequest], newValue[RunStatus], true, true},
	"turn.ack":           {"RunRef", "SessionCloseResult", newValue[RunRef], newValue[struct{}], true, true},
	"turn.ready":         {"TurnReady", "SessionCloseResult", newValue[TurnReady], newValue[struct{}], true, false},
	"turn.interrupt":     {"TurnInterruptRequest", "TurnInterruptResult", newValue[SessionTarget], newValue[struct{}], true, true},
	"session.close":      {"SessionCloseRequest", "SessionCloseResult", newValue[SessionCloseRequest], newValue[struct{}], true, true},
}

func Allows(method string, client bool) bool {
	spec, ok := methodCodecs[method]
	return ok && (client && spec.client || !client && spec.daemon)
}

func DecodeParams(method string, raw []byte) (any, error) {
	spec, ok := methodCodecs[method]
	if !ok {
		return nil, errInvalid
	}
	if err := validateDefinition(spec.params, raw); err != nil {
		return nil, err
	}
	target := spec.newParams
	if method == "session.hello" {
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(raw, &fields)
		if _, worker := fields["launch_token"]; worker {
			target = newValue[WorkerHello]
		} else {
			target = newValue[PeerHello]
		}
	}
	value, err := decode(raw, target)
	if err == nil {
		err = ValidateTransferParams(method, value)
	}
	return value, err
}

func DecodeResult(method string, raw []byte) (any, error) {
	spec, ok := methodCodecs[method]
	if !ok {
		return nil, errInvalid
	}
	if err := validateDefinition(spec.result, raw); err != nil {
		return nil, err
	}
	value, err := decode(raw, spec.newResult)
	if err == nil {
		err = ValidateTransferResult(method, value)
	}
	return value, err
}

func decode(raw []byte, newTarget func() any) (any, error) {
	target := newTarget()
	if json.Unmarshal(raw, target) != nil {
		return nil, errInvalid
	}
	return target, nil
}

func EncodeParams(method string, value any) ([]byte, error) {
	return encodeDefinition(method, value, true)
}
func EncodeResult(method string, value any) ([]byte, error) {
	return encodeDefinition(method, value, false)
}
func encodeDefinition(method string, value any, params bool) ([]byte, error) {
	spec, ok := methodCodecs[method]
	if !ok {
		return nil, errInvalid
	}
	name := spec.result
	if params {
		name = spec.params
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%s: $ cannot be encoded as JSON", name)
	}
	if err = validateDefinition(name, raw); err != nil {
		return nil, err
	}
	if params {
		if err = ValidateTransferParams(method, value); err != nil {
			return nil, err
		}
	} else if err = ValidateTransferResult(method, value); err != nil {
		return nil, err
	}
	return raw, nil
}
func UnmarshalResult(method string, raw []byte, target any) error {
	spec, ok := methodCodecs[method]
	if !ok || target == nil {
		return errInvalid
	}
	if err := validateDefinition(spec.result, raw); err != nil {
		return err
	}
	if err := ValidateTransferResult(method, json.RawMessage(raw)); err != nil {
		return err
	}
	if json.Unmarshal(raw, target) != nil {
		return errInvalid
	}
	return nil
}
func EncodeError(value *RPCError) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("RPCError: $ cannot be encoded as JSON")
	}
	if err = validateDefinition("RPCError", raw); err != nil {
		return nil, err
	}
	return raw, nil
}
func DecodeError(raw []byte) (*RPCError, error) {
	if err := validateDefinition("RPCError", raw); err != nil {
		return nil, err
	}
	var value RPCError
	var generic any
	_ = json.Unmarshal(raw, &generic)
	normalized, _ := json.Marshal(generic)
	if json.Unmarshal(normalized, &value) != nil {
		return nil, errInvalid
	}
	return &value, nil
}

var errorMessages = map[int]string{InvalidFrame: "invalid_frame", InvalidHello: "invalid_hello", Internal: "internal", UnknownSession: "unknown_session", NotConnected: "not_connected", Busy: "busy", NotRunning: "not_running", AlreadyConnected: "already_connected", UnknownProduct: "unknown_product", UnsupportedOpen: "unsupported_open_field", SpawnFailed: "spawn_failed", Timeout: "timeout", NotCommitted: "not_committed", Superseded: "superseded", NameTaken: "name_taken", UnknownHost: "unknown_host", ForwardLost: "forward_lost", UnsupportedTrace: "unsupported_trace"}

type schemaNode map[string]any

var schemaDefinitions = loadSchema()
var schemaKeywords = map[string]bool{"$ref": true, "const": true, "enum": true, "type": true, "required": true, "additionalProperties": true, "minProperties": true, "properties": true, "items": true, "uniqueItems": true, "minLength": true, "maxLength": true, "pattern": true, "minimum": true, "maximum": true, "exclusiveMinimum": true, "allOf": true, "if": true, "then": true, "else": true, "not": true}

func loadSchema() map[string]any {
	var root map[string]any
	if json.Unmarshal(SessionSchema, &root) != nil || len(root) != 3 {
		panic("invalid session schema root")
	}
	for key := range root {
		if key != "$schema" && key != "$id" && key != "$defs" {
			panic("unsupported session schema root keyword " + key)
		}
	}
	defs, ok := root["$defs"].(map[string]any)
	if !ok || len(defs) == 0 {
		panic("invalid session schema definitions")
	}
	for name, value := range defs {
		if checkSchemaNode(defs, node(value)) != nil {
			panic("invalid session schema definition " + name)
		}
	}
	return defs
}

func checkSchemaNode(defs map[string]any, value schemaNode) error {
	if value == nil {
		return errInvalid
	}
	for key, raw := range value {
		if !schemaKeywords[key] {
			return errInvalid
		}
		if key == "$ref" && (!strings.HasPrefix(raw.(string), "#/$defs/") || defs[strings.TrimPrefix(raw.(string), "#/$defs/")] == nil) {
			return errInvalid
		}
		if key == "pattern" {
			if _, err := regexp.Compile(raw.(string)); err != nil {
				return errInvalid
			}
		}
		if key == "properties" {
			for _, child := range raw.(map[string]any) {
				if checkSchemaNode(defs, node(child)) != nil {
					return errInvalid
				}
			}
		}
		if key == "items" || key == "if" || key == "then" || key == "else" || key == "not" {
			if checkSchemaNode(defs, node(raw)) != nil {
				return errInvalid
			}
		}
		if key == "allOf" {
			for _, child := range raw.([]any) {
				if checkSchemaNode(defs, node(child)) != nil {
					return errInvalid
				}
			}
		}
	}
	return nil
}

func validateDefinition(name string, raw []byte) error {
	definition, ok := schemaDefinitions[name]
	if !ok {
		return errInvalid
	}
	var value any
	if !utf8.Valid(raw) || json.Unmarshal(raw, &value) != nil {
		return fmt.Errorf("%s: $ is not valid JSON", name)
	}
	if issue := schemaIssue(node(definition), value, "", ""); issue != "" {
		return fmt.Errorf("%s: %s", name, issue)
	}
	return nil
}

func validSchemaNode(rule schemaNode, value any) bool {
	return schemaIssue(rule, value, "", "") == ""
}

func schemaIssue(rule schemaNode, value any, path, with string) string {
	if ref, ok := rule["$ref"].(string); ok {
		return schemaIssue(node(schemaDefinitions[strings.TrimPrefix(ref, "#/$defs/")]), value, path, with)
	}
	if kind, ok := rule["type"].(string); ok && !validType(kind, value) {
		return fmt.Sprintf("%s must be %s", location(path), kind)
	}
	if expected, ok := rule["const"]; ok && !reflect.DeepEqual(value, expected) {
		return fmt.Sprintf("%s must equal %v", location(path), expected)
	}
	if values, ok := rule["enum"].([]any); ok && !contains(values, value) {
		return fmt.Sprintf("%s must be one of the allowed values", location(path))
	}
	if text, ok := value.(string); ok {
		length := float64(utf8.RuneCountInString(text))
		if !utf8.ValidString(text) {
			return fmt.Sprintf("%s must be valid UTF-8", location(path))
		}
		if minimum := number(rule, "minLength"); length < minimum {
			return fmt.Sprintf("%s must contain at least %g character%s", location(path), minimum, plural(minimum))
		}
		if maximum := number(rule, "maxLength"); has(rule, "maxLength") && length > maximum {
			return fmt.Sprintf("%s must contain at most %g character%s", location(path), maximum, plural(maximum))
		}
		if pattern, ok := rule["pattern"].(string); ok {
			var matched bool
			if pattern == "^[A-Za-z0-9+/]+={0,2}$" {
				_, matched = transferBase64Syntax(text)
			} else {
				matched, _ = regexp.MatchString(pattern, text)
			}
			if !matched {
				return fmt.Sprintf("%s must match %q", location(path), pattern)
			}
		}
	}
	if value, ok := value.(float64); ok {
		if minimum := number(rule, "minimum"); has(rule, "minimum") && value < minimum {
			return fmt.Sprintf("%s must be at least %g", location(path), minimum)
		}
		if maximum := number(rule, "maximum"); has(rule, "maximum") && value > maximum {
			return fmt.Sprintf("%s must be at most %g", location(path), maximum)
		}
		if minimum := number(rule, "exclusiveMinimum"); has(rule, "exclusiveMinimum") && value <= minimum {
			return fmt.Sprintf("%s must be greater than %g", location(path), minimum)
		}
	}
	if object, ok := value.(map[string]any); ok {
		for _, name := range stringsOf(rule["required"]) {
			if _, ok := object[name]; !ok {
				return fmt.Sprintf("%s is required", location(childPath(path, name)))
			}
		}
		if minimum := number(rule, "minProperties"); float64(len(object)) < minimum {
			return fmt.Sprintf("%s must contain at least %g properties", location(path), minimum)
		}
		properties, _ := rule["properties"].(map[string]any)
		for name, child := range object {
			schema, known := properties[name]
			if !known && rule["additionalProperties"] == false {
				return fmt.Sprintf("%s is not allowed", location(childPath(path, name)))
			}
			if known {
				if issue := schemaIssue(node(schema), child, childPath(path, name), with); issue != "" {
					return issue
				}
			}
		}
	}
	if list, ok := value.([]any); ok {
		if item, ok := rule["items"]; ok {
			for index, child := range list {
				if issue := schemaIssue(node(item), child, fmt.Sprintf("%s[%d]", path, index), with); issue != "" {
					return issue
				}
			}
		}
		if rule["uniqueItems"] == true {
			for index := range list {
				if contains(list[:index], list[index]) {
					return fmt.Sprintf("%s must not duplicate an earlier item", location(fmt.Sprintf("%s[%d]", path, index)))
				}
			}
		}
	}
	if children, ok := rule["allOf"].([]any); ok {
		for _, child := range children {
			if issue := schemaIssue(node(child), value, path, with); issue != "" {
				return issue
			}
		}
	}
	if child, ok := rule["not"]; ok && schemaIssue(node(child), value, path, "") == "" {
		required := stringsOf(node(child)["required"])
		if len(required) == 1 {
			if with != "" {
				return fmt.Sprintf("%s is not allowed with %q", location(childPath(path, required[0])), with)
			}
			return fmt.Sprintf("%s is not allowed", location(childPath(path, required[0])))
		}
		return fmt.Sprintf("%s violates an excluded constraint", location(path))
	}
	if condition, ok := rule["if"]; ok {
		branch := "else"
		relation := with
		if schemaIssue(node(condition), value, path, "") == "" {
			branch = "then"
			if required := stringsOf(node(condition)["required"]); len(required) == 1 {
				relation = required[0]
			}
		}
		if child, ok := rule[branch]; ok {
			return schemaIssue(node(child), value, path, relation)
		}
	}
	return ""
}

func childPath(path, name string) string {
	if path == "" {
		return name
	}
	return path + "." + name
}
func location(path string) string {
	if path == "" {
		return "$"
	}
	return fmt.Sprintf("%q", path)
}
func plural(number float64) string {
	if number == 1 {
		return ""
	}
	return "s"
}
func validType(kind string, value any) bool {
	switch kind {
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	case "string":
		_, ok := value.(string)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "integer":
		n, ok := value.(float64)
		return ok && math.Trunc(n) == n
	}
	return false
}
func node(value any) schemaNode                   { object, _ := value.(map[string]any); return object }
func number(rule schemaNode, name string) float64 { value, _ := rule[name].(float64); return value }
func has(rule schemaNode, name string) bool       { _, ok := rule[name]; return ok }
func contains(values []any, target any) bool {
	for _, value := range values {
		if reflect.DeepEqual(value, target) {
			return true
		}
	}
	return false
}
func stringsOf(value any) []string {
	raw, _ := value.([]any)
	values := make([]string, len(raw))
	for i := range raw {
		values[i], _ = raw[i].(string)
	}
	return values
}

// ValidateTurnResult validates a native terminal independently of caller methods.
func ValidateTurnResult(value TurnResult) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return validateDefinition("TurnRunResult", raw)
}
