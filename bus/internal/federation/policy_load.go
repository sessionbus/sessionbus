// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

// LoadPolicy validates the complete file before it can replace a live policy.
// An omitted policy is represented by nil at the caller, never by a load error.
func LoadPolicy(path string) (*Policy, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return nil, errors.New("federation policy must be a mode-0600 regular file")
	}
	raw, err := io.ReadAll(io.LimitReader(file, protocol.MaxFrameBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > protocol.MaxFrameBytes {
		return nil, errors.New("federation policy exceeds the file size limit")
	}
	return decodePolicy(raw)
}

func decodePolicy(raw []byte) (*Policy, error) {
	policy := &Policy{hosts: map[string]hostPolicy{}}
	err := policyObject(raw, func(key string, value json.RawMessage) error {
		if key != "hosts" {
			return fmt.Errorf("unknown federation policy field %q", key)
		}
		return policyObject(value, func(host string, rules json.RawMessage) error {
			if !validHost(host) {
				return fmt.Errorf("invalid federation policy host %q", host)
			}
			var entry hostPolicy
			err := policyObject(rules, func(field string, raw json.RawMessage) error {
				if field != "groups" && field != "lanes_on" && field != "lanes_to" {
					return fmt.Errorf("unknown federation policy rule %q", field)
				}
				values, err := policyNames(raw, field == "groups")
				if err != nil {
					return fmt.Errorf("host %q rule %q: %w", host, field, err)
				}
				switch field {
				case "groups":
					entry.groups = values
				case "lanes_on":
					entry.lanesOn = values
				case "lanes_to":
					entry.lanesTo = values
				}
				return nil
			})
			if err == nil {
				policy.hosts[host] = entry
			}
			return err
		})
	})
	if err != nil {
		return nil, err
	}
	return policy, nil
}

func policyObject(raw []byte, visit func(string, json.RawMessage) error) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return errors.New("federation policy requires a JSON object")
	}
	seen := map[string]bool{}
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok || seen[key] {
			return errors.New("duplicate or invalid federation policy field")
		}
		seen[key] = true
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return err
		}
		if err := visit(key, value); err != nil {
			return err
		}
	}
	if _, err := decoder.Token(); err != nil {
		return err
	}
	if decoder.Decode(new(json.RawMessage)) != io.EOF {
		return errors.New("trailing federation policy value")
	}
	return nil
}

func policyNames(raw []byte, groups bool) (map[string]bool, error) {
	raw = bytes.TrimSpace(raw)
	var values []string
	if len(raw) == 0 || raw[0] != '[' || json.Unmarshal(raw, &values) != nil {
		return nil, errors.New("policy rule must be an array of exact names")
	}
	result := make(map[string]bool, len(values))
	for _, value := range values {
		if value == "" || result[value] || groups && strings.HasPrefix(value, "session:") || !groups && !validHost(value) {
			return nil, errors.New("invalid or duplicate policy name")
		}
		result[value] = true
	}
	return result, nil
}

// ValidateHosts binds all policy dimensions to the loaded secret-map names.
// Offline but configured hosts remain valid; no secret contents are reported.
func (p *Policy) ValidateHosts(secrets map[string]string) error {
	if p == nil {
		return nil
	}
	check := func(host string) error {
		if _, ok := secrets[host]; !ok {
			return fmt.Errorf("unknown federation policy host %q", host)
		}
		return nil
	}
	for host, rules := range p.hosts {
		if err := check(host); err != nil {
			return err
		}
		for name := range rules.lanesOn {
			if err := check(name); err != nil {
				return err
			}
		}
		for name := range rules.lanesTo {
			if err := check(name); err != nil {
				return err
			}
		}
	}
	return nil
}
