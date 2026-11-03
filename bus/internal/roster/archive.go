// SPDX-License-Identifier: GPL-3.0-only

package roster

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

const ArchiveAction = "lane.archive"
const ArchiveSchema = "sessionbus.lane-archive.v1"

type ArchiveRequest struct {
	Action    string `json:"action"`
	SessionID string `json:"session_id"`
}

// ArchiveResult describes a local settlement snapshot, not a ban on later resume.
// Unknown means the caller must not infer whether an admitted close finished.
type ArchiveResult struct {
	Schema    string `json:"schema"`
	SessionID string `json:"session_id,omitempty"`
	State     string `json:"state,omitempty"`
	Changed   bool   `json:"changed"`
	Error     string `json:"error,omitempty"`
}

// DecodeArchive requires exactly two nonnull string fields. In particular,
// local:false must not disappear into a typed zero value and grant mutation.
func DecodeArchive(raw []byte) (ArchiveRequest, error) {
	var value ArchiveRequest
	invalid := errors.New("invalid archive request")
	d := json.NewDecoder(bytes.NewReader(raw))
	first, err := d.Token()
	if err != nil || first != json.Delim('{') {
		return value, invalid
	}
	seen := make(map[string]bool, 2)
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return value, invalid
		}
		name, ok := key.(string)
		if !ok || seen[name] || name != "action" && name != "session_id" {
			return value, invalid
		}
		seen[name] = true
		var text *string
		if d.Decode(&text) != nil || text == nil || *text == "" {
			return value, invalid
		}
		if name == "action" {
			value.Action = *text
		} else {
			value.SessionID = *text
		}
	}
	last, err := d.Token()
	if err != nil || last != json.Delim('}') || len(seen) != 2 || value.Action != ArchiveAction || d.Decode(new(json.RawMessage)) != io.EOF {
		return value, invalid
	}
	return value, nil
}
