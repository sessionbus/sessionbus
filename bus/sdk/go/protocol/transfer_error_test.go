// SPDX-License-Identifier: MIT
package protocol

import "testing"

func TestTransferRefusalErrorsAreStrictAndDecodable(t *testing.T) {
	for code, message := range map[int]string{-32018: "unsupported_transfer", -32019: "transfer_failed"} {
		raw, err := ErrorBytes(17, code, nil)
		if err != nil {
			t.Errorf("%s cannot encode: %v", message, err)
			continue
		}
		frame, err := DecodeFrame(raw)
		if err != nil || frame.ID != 17 || frame.Error == nil || frame.Error.Message != message || len(frame.Error.Data) != 0 {
			t.Errorf("refusal = %s, %v", raw, err)
		}
		if _, err := ErrorBytes(17, code, "reason"); err == nil {
			t.Error("transfer code accepted forbidden data")
		}
	}
}

func TestErrorBytesDoesNotDropUnencodableData(t *testing.T) {
	if raw, err := ErrorBytes(17, Busy, make(chan int)); err == nil {
		t.Fatalf("invalid caller data silently disappeared: %s", raw)
	}
}
