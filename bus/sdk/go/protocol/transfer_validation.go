// SPDX-License-Identifier: MIT

package protocol

import (
	"encoding/json"
	"errors"
	"strings"
)

// ValidateTransferParams adds byte and canonical-base64 limits to the common
// JSON schema. It deliberately does not decode/allocate a chunk before the
// receiving transfer stage has reserved its buffer capacity.
func ValidateTransferParams(method string, value any) error {
	return validateTransferValue(method, value, method == "transfer.chunk")
}

func ValidateTransferResult(method string, value any) error {
	return validateTransferValue(method, value, false)
}

func validateTransferValue(method string, value any, chunk bool) error {
	if !strings.HasPrefix(method, "transfer.") {
		return nil
	}
	// Typed chunks have already passed the closed schema at the wire boundary.
	// The opaque ID and canonical alphabet need no JSON escaping; avoid making
	// another full JSON/backing-string copy just to recheck the byte bound.
	if chunk {
		var p *TransferChunk
		switch v := value.(type) {
		case TransferChunk:
			p = &v
		case *TransferChunk:
			p = v
		}
		if p != nil {
			if len(p.ID)+len(p.Data)+128 > MaxTransferFrameBytes {
				return errors.New("transfer frame exceeds its byte bound")
			}
			return canonicalChunk(p.Data)
		}
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	bound := MaxTransferMetadataBytes - 128
	if chunk {
		bound = MaxTransferFrameBytes - 128
	}
	if len(raw) > bound {
		return errors.New("transfer frame exceeds its byte bound")
	}
	var fields struct {
		Name string `json:"name"`
		What string `json:"what"`
		Data string `json:"data"`
	}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if err := transferText(fields.Name, 255); err != nil {
		return err
	}
	if err := transferText(fields.What, 2048); err != nil {
		return err
	}
	if chunk {
		return canonicalChunk(fields.Data)
	}
	return nil
}

func transferText(s string, max int) error {
	if len(s) > max {
		return errors.New("transfer metadata exceeds its UTF-8 byte bound")
	}
	return nil
}

func canonicalChunk(s string) error {
	if len(s) == 0 || len(s)%4 != 0 {
		return errors.New("invalid transfer base64")
	}
	padding, syntax := transferBase64Syntax(s)
	if !syntax || len(s)/4*3-padding > TransferChunkBytes {
		return errors.New("invalid transfer chunk length")
	}
	last := transferBase64Digit[s[len(s)-padding-1]]
	if padding == 1 && last&3 != 0 || padding == 2 && last&15 != 0 {
		return errors.New("noncanonical transfer base64")
	}
	return nil
}

var transferBase64Digit = func() (table [256]byte) {
	for i := range table {
		table[i] = 255
	}
	for value, char := range []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/") {
		table[char] = byte(value)
	}
	return
}()

// Exactly the shared schema's ^[A-Za-z0-9+/]+={0,2}$ language. Canonical pad
// bits and decoded length are the additional machine-protocol checks above.
func transferBase64Syntax(s string) (padding int, valid bool) {
	for i := len(s) - 1; i >= 0 && s[i] == '='; i-- {
		padding++
	}
	if padding > 2 || len(s) == padding {
		return padding, false
	}
	for i := 0; i < len(s)-padding; i++ {
		if transferBase64Digit[s[i]] == 255 {
			return padding, false
		}
	}
	return padding, true
}
