// SPDX-License-Identifier: MIT

package protocol

import (
	"bytes"
	"encoding/base64"
	"regexp"
	"strings"
	"testing"
)

func TestTransferProtocolDoesNotCarryPathsOrSpoofedIdentity(t *testing.T) {
	for _, raw := range []string{
		`{"target":"b@host","name":"x","source":"/private/source"}`,
		`{"target":"b@host","name":"x","sender":"another@host"}`,
	} {
		if _, err := DecodeParams("transfer.offer", []byte(raw)); err == nil {
			t.Fatalf("accepted private/identity field: %s", raw)
		}
	}
	if _, err := EncodeParams("transfer.offer", (*TransferOffer)(nil)); err == nil {
		t.Fatal("accepted nil offer")
	}
	if _, err := EncodeParams("transfer.offer", map[string]any{"target": "b@host", "name": strings.Repeat("é", 200)}); err == nil {
		t.Fatal("rune limit bypassed UTF8 byte limit")
	}
}

func TestTransferChunkCanonicalAndBounded(t *testing.T) {
	data := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{42}, TransferChunkBytes))
	if len(data) != 87384 {
		t.Fatal(len(data))
	}
	raw, err := EncodeParams("transfer.chunk", TransferChunk{ID: "abc", Data: data})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = DecodeParams("transfer.chunk", raw); err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"", "YQ", "YR==", "YWJ=", "A===", "YQ==x", base64.StdEncoding.EncodeToString(make([]byte, TransferChunkBytes+1))} {
		_, err = EncodeParams("transfer.chunk", TransferChunk{ID: "abc", Data: s})
		if err == nil {
			t.Fatalf("accepted bad chunk %q", s[:min(20, len(s))])
		}
	}
	if _, err = EncodeParams("transfer.chunk", TransferChunk{ID: "abc", Offset: Integer(MaxTransferBytes + 1), Data: "YQ=="}); err == nil {
		t.Fatal("accepted oversized offset")
	}
}

func TestTransferRolePreparationAndStartAreExplicit(t *testing.T) {
	cases := []struct {
		method, raw string
		valid       bool
	}{
		{"transfer.prepare", `{"id":"x","role":"source"}`, true},
		{"transfer.prepare", `{"id":"x","role":"destination","size":0}`, true},
		{"transfer.prepare", `{"id":"x","role":"destination"}`, false},
		{"transfer.prepare", `{"id":"x","role":"source","size":4}`, false},
		{"transfer.start", `{"id":"x","role":"source","size":4,"credit":262144}`, true},
		{"transfer.start", `{"id":"x","role":"source","size":4}`, false},
		{"transfer.start", `{"id":"x","role":"destination","size":4,"credit":262144}`, false},
		{"transfer.notice", `{"kind":"start","id":"x"}`, false},
	}
	for _, c := range cases {
		if _, err := DecodeParams(c.method, []byte(c.raw)); (err == nil) != c.valid {
			t.Fatalf("%s %s: %v", c.method, c.raw, err)
		}
	}
}

func TestTransferHelloRetainsLegacyShapeAndConditionalResult(t *testing.T) {
	for _, raw := range []string{`{}`, `{"capabilities":["transfer/1"]}`} {
		if _, err := DecodeResult("session.hello", []byte(raw)); err != nil {
			t.Fatal(raw, err)
		}
	}
	if _, err := DecodeResult("session.hello", []byte(`{"capabilities":["unregistered"]}`)); err == nil {
		t.Fatal("unknown capability accepted")
	}
	legacy := []byte(`{"protocol":1,"product":"test","launch_token":"token-old","supported_open_fields":[],"extra_arguments":[]}`)
	if _, err := DecodeParams("session.hello", legacy); err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeParams("session.hello", []byte(`{"protocol":1,"product":"test","session_id":"test","groups":[],"info":{},"supports_transfer":true}`)); err == nil {
		t.Fatal("peer acquired worker-only opt-in")
	}
}

func BenchmarkTransferChunkValidation(b *testing.B) {
	p := TransferChunk{ID: "bounded-machine-stream", Data: base64.StdEncoding.EncodeToString(make([]byte, TransferChunkBytes))}
	b.SetBytes(TransferChunkBytes)
	b.ReportAllocs()
	for b.Loop() {
		raw, err := RequestBytes(1, "transfer.chunk", p)
		if err != nil {
			b.Fatal(err)
		}
		frame, err := DecodeFrame(raw[:len(raw)-1])
		if err != nil {
			b.Fatal(err)
		}
		if _, err = DecodeParams(frame.Method, frame.Params); err != nil {
			b.Fatal(err)
		}
	}
}

func TestFastTransferAlphabetMatchesDeclaredSchemaLanguage(t *testing.T) {
	reference := regexp.MustCompile("^[A-Za-z0-9+/]+={0,2}$")
	alphabet := []byte{'A', 'z', '0', '+', '/', '=', '!', '\r', '\n', 0, 255}
	var check func(string, int)
	check = func(s string, remaining int) {
		_, actual := transferBase64Syntax(s)
		if actual != reference.MatchString(s) {
			t.Fatalf("schema language changed for %q", s)
		}
		if remaining > 0 {
			for _, char := range alphabet {
				check(s+string([]byte{char}), remaining-1)
			}
		}
	}
	check("", 4)
	for size := 0; size <= TransferChunkBytes+1; size += 113 {
		raw := make([]byte, size)
		for i := range raw {
			raw[i] = byte(i % 251)
		}
		s := base64.StdEncoding.EncodeToString(raw)
		err := canonicalChunk(s)
		if (err == nil) != (size > 0 && size <= TransferChunkBytes) {
			t.Fatal("decoded size/canonical rule", size, err)
		}
	}
}
