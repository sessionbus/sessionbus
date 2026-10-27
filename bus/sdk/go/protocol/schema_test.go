// SPDX-License-Identifier: MIT

package protocol

import (
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v5"
)

type fixtureFile struct {
	Cases []struct {
		Name, Definition, Raw string
		Valid                 bool
		Value                 json.RawMessage
		Repeat                *struct {
			Path  []string
			Text  string
			Count int
		}
	} `json:"cases"`
}

func TestSchemaFixtures(t *testing.T) {
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("schema.json", strings.NewReader(string(SessionSchema))); err != nil {
		t.Fatal(err)
	}
	var root struct {
		Definitions map[string]json.RawMessage `json:"$defs"`
	}
	if err := json.Unmarshal(SessionSchema, &root); err != nil {
		t.Fatal(err)
	}
	definitions := map[string]*jsonschema.Schema{}
	for name := range root.Definitions {
		compiled, err := compiler.Compile("schema.json#/$defs/" + name)
		if err != nil {
			t.Fatal(err)
		}
		definitions[name] = compiled
	}
	var fixtures fixtureFile
	raw, err := os.ReadFile("session.fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	coverage := map[string][2]bool{}
	for _, fixture := range fixtures.Cases {
		fixture := fixture
		t.Run(fixture.Name, func(t *testing.T) {
			raw := fixture.Value
			if fixture.Raw != "" {
				raw = []byte(fixture.Raw)
			}
			if fixture.Repeat != nil {
				raw = repeatFixture(t, raw, fixture.Repeat.Path, fixture.Repeat.Text, fixture.Repeat.Count)
			}
			var value any
			if err := json.Unmarshal(raw, &value); err != nil {
				t.Fatal(err)
			}
			valid := definitions[fixture.Definition].Validate(value) == nil
			if valid != fixture.Valid {
				t.Fatalf("valid = %t, want %t", valid, fixture.Valid)
			}
			seen := coverage[fixture.Definition]
			seen[0], seen[1] = seen[0] || fixture.Valid, seen[1] || !fixture.Valid
			coverage[fixture.Definition] = seen
		})
	}
	for definition := range definitions {
		if coverage[definition] != [2]bool{true, true} {
			t.Errorf("definition %s lacks valid and invalid fixtures", definition)
		}
	}
}

func TestClosedTypesMatchSchemaFixtures(t *testing.T) {
	var fixtures fixtureFile
	raw, err := os.ReadFile("session.fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range fixtures.Cases {
		fixture := fixture
		t.Run(fixture.Name, func(t *testing.T) {
			raw := fixture.Value
			if fixture.Raw != "" {
				raw = []byte(fixture.Raw)
			}
			if fixture.Repeat != nil {
				raw = repeatFixture(t, raw, fixture.Repeat.Path, fixture.Repeat.Text, fixture.Repeat.Count)
			}
			err := validateClosedFixture(fixture.Definition, raw)
			if (err == nil) != fixture.Valid {
				t.Fatalf("closed type valid = %t, want %t: %v", err == nil, fixture.Valid, err)
			}
		})
	}
}

func validateClosedFixture(definition string, raw []byte) error {
	params := map[string]string{
		"TransferOffer":   "transfer.offer",
		"TransferRequest": "transfer.request",
		"TransferTarget":  "transfer.status",
		"TransferAccept":  "transfer.accept",
		"TransferPrepare": "transfer.prepare",
		"TransferStart":   "transfer.start",
		"TransferChunk":   "transfer.chunk",
		"TransferFinish":  "transfer.finish",
		"TransferFailure": "transfer.fail",
		"TransferNotice":  "transfer.notice",

		"SessionHelloRequest": "session.hello", "SessionSupersededRequest": "session.superseded", "SessionListRequest": "session.list",
		"MessageSendRequest": "message.send", "MessageDeliverRequest": "message.deliver", "LaneDescribeRequest": "lane.describe",
		"LaneSpawnRequest": "lane.spawn", "TraceConfigureRequest": "trace.configure", "SessionOpenRequest": "session.open", "TurnRunRequest": "turn.run",
		"TurnInterruptRequest": "turn.interrupt", "SessionCloseRequest": "session.close",
		"RunRef": "turn.ack", "ReadRequest": "turn.status", "WaitRequest": "turn.wait", "ExecuteRequest": "turn.execute", "TurnReady": "turn.ready",
	}
	results := map[string]string{
		"TransferDescriptor": "transfer.offer",
		"TransferPrepared":   "transfer.prepare",
		"TransferStarted":    "transfer.start",
		"TransferCredit":     "transfer.chunk",
		"TransferTerminal":   "transfer.finish",

		"SessionHelloResult": "session.hello", "SessionSupersededResult": "session.superseded", "SessionListResult": "session.list",
		"MessageSendResult": "message.send", "MessageDeliverResult": "message.deliver", "DeliveryReceipt": "message.deliver",
		"LaneDescribeResult": "lane.describe", "LaneSpawnResult": "lane.spawn", "TraceConfigureResult": "trace.configure", "SessionOpenResult": "session.open",
		"RunStatus": "turn.run", "TurnInterruptResult": "turn.interrupt", "SessionCloseResult": "session.close",
	}
	if method := params[definition]; method != "" {
		_, err := DecodeParams(method, raw)
		return err
	}
	if method := results[definition]; method != "" {
		_, err := DecodeResult(method, raw)
		return err
	}
	switch definition {
	case "TurnRunResult":
		var value TurnResult
		if err := validateDefinition("TurnRunResult", raw); err != nil {
			return err
		}
		return DecodeJSON(raw, &value)
	case "LanePolicy":
		_, err := DecodeResult("lane.spawn", wrap(`{"session_id":"id@local","policy":`, raw, `}`))
		return err
	case "HostProducts":
		_, err := DecodeResult("session.list", wrap(`{"sessions":[],"hosts":[`, raw, `]}`))
		return err
	case "SessionSummary":
		_, err := DecodeResult("session.list", wrap(`{"sessions":[`, raw, `]}`))
		return err
	case "SessionSelfInfo":
		_, err := DecodeResult("session.list", wrap(`{"sessions":[],"self_info":`, raw, `}`))
		return err
	case "MessageSendDelivery":
		_, err := DecodeResult("message.send", wrap(`{"message_id":"m","deliveries":[`, raw, `]}`))
		return err
	case "DeliverySource":
		_, err := DecodeParams("message.deliver", wrap(`{"message_id":"m","from":`, raw, `,"body":""}`))
		return err
	case "ExtraArgument":
		_, err := DecodeResult("lane.describe", wrap(`{"product":"p","supported_open_fields":[],"extra_arguments":[`, raw, `]}`))
		return err
	case "SessionOpenOptions":
		_, err := DecodeParams("session.open", wrap(`{"name":"n","groups":[],"open":`, raw, `}`))
		return err
	case "RPCError":
		_, err := DecodeError(raw)
		return err
	case "SpawnFailedData":
		_, err := DecodeError(wrap(`{"code":-32009,"message":"spawn_failed","data":`, raw, `}`))
		return err
	case "RPCErrorResponse":
		_, err := DecodeFrame(raw)
		return err
	default:
		return errors.New("unmapped fixture definition " + definition)
	}
}

func wrap(before string, raw []byte, after string) []byte {
	return append(append([]byte(before), raw...), after...)
}

func TestSchemaDefinitions(t *testing.T) {
	var root struct {
		Definitions map[string]json.RawMessage `json:"$defs"`
	}
	if err := json.Unmarshal(SessionSchema, &root); err != nil {
		t.Fatal(err)
	}
	want := []string{"DeliveryReceipt", "DeliverySource", "ExtraArgument", "HostProducts", "LaneDescribeRequest", "LaneDescribeResult", "LaneSpawnRequest", "LaneSpawnResult", "MessageDeliverRequest", "MessageDeliverResult", "MessageSendDelivery", "MessageSendRequest", "MessageSendResult", "RPCError", "RPCErrorResponse", "SessionCloseRequest", "SessionCloseResult", "SessionHelloRequest", "SessionHelloResult", "SessionListRequest", "SessionListResult", "SessionOpenOptions", "SessionOpenRequest", "SessionOpenResult", "SessionSummary", "SessionSupersededRequest", "SessionSupersededResult", "SpawnFailedData", "TurnInterruptRequest", "TurnInterruptResult", "TurnRunRequest", "TurnRunResult"}
	want = append(want, "ExecuteRequest", "LanePolicy", "ReadRequest", "RunRef", "RunStatus", "TurnReady", "WaitRequest", "SessionSelfInfo", "TraceConfigureRequest", "TraceConfigureResult")
	want = append(want, "TransferOffer", "TransferRequest", "TransferTarget", "TransferAccept", "TransferDescriptor", "TransferPrepare", "TransferPrepared", "TransferStart", "TransferStarted", "TransferChunk", "TransferCredit", "TransferFinish", "TransferFailure", "TransferTerminal", "TransferNotice")
	slicesSort(want)
	got := make([]string, 0, len(root.Definitions))
	for name := range root.Definitions {
		got = append(got, name)
	}
	slicesSort(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("definitions = %v", got)
	}
}

func TestMinimalInterpreterRejectsUnknownKeywords(t *testing.T) {
	if checkSchemaNode(schemaDefinitions, schemaNode{"title": "x"}) == nil {
		t.Fatal("unknown keyword accepted")
	}
	if checkSchemaNode(schemaDefinitions, schemaNode{"pattern": "["}) == nil {
		t.Fatal("invalid pattern accepted")
	}
	if checkSchemaNode(schemaDefinitions, schemaNode{"$ref": "#/$defs/Missing"}) == nil {
		t.Fatal("unknown reference accepted")
	}
	if !validSchemaNode(schemaNode{"type": "object", "minProperties": float64(1)}, map[string]any{"x": true}) || validSchemaNode(schemaNode{"type": "object", "minProperties": float64(1)}, map[string]any{}) {
		t.Fatal("minProperties not enforced")
	}
	if !validSchemaNode(schemaNode{"type": "integer", "exclusiveMinimum": float64(1)}, float64(2)) || validSchemaNode(schemaNode{"type": "integer", "exclusiveMinimum": float64(1)}, float64(1)) {
		t.Fatal("exclusiveMinimum not enforced")
	}
}

func repeatFixture(t *testing.T, raw []byte, path []string, text string, count int) []byte {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	current := value.(map[string]any)
	for _, key := range path[:len(path)-1] {
		current = current[key].(map[string]any)
	}
	current[path[len(path)-1]] = strings.Repeat(text, count)
	result, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func slicesSort(values []string) {
	for i := range values {
		for j := i + 1; j < len(values); j++ {
			if values[j] < values[i] {
				values[i], values[j] = values[j], values[i]
			}
		}
	}
}
