// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/antst/sessionbus/bus/internal/transfer"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestTransferRefusalAndUncertaintyRemainDistinct(t *testing.T) {
	for _, test := range []struct {
		err  error
		code int
	}{
		{transfer.ErrCapacity, protocol.Busy}, {transfer.ErrUnsupported, protocol.UnsupportedTransfer},
		{transfer.ErrUnknown, protocol.TransferFailed}, {transfer.ErrRole, protocol.TransferFailed}, {transfer.ErrState, protocol.TransferFailed},
		{context.Canceled, protocol.Internal}, {context.DeadlineExceeded, protocol.Internal}, {errors.New("transport lost"), protocol.Internal},
		{&protocol.RPCError{Code: protocol.Busy}, protocol.Busy}, {&protocol.RPCError{Code: protocol.UnsupportedTransfer}, protocol.UnsupportedTransfer},
		{&protocol.RPCError{Code: protocol.TransferFailed}, protocol.TransferFailed}, {&protocol.RPCError{Code: protocol.ForwardLost}, protocol.Internal},
	} {
		t.Run(test.err.Error(), func(t *testing.T) {
			body, err := ResponseBytes(7, transferReply(nil, fmt.Errorf("cause: %w", test.err)))
			if err != nil {
				t.Fatal(err)
			}
			frame, err := protocol.DecodeFrame(body)
			if err != nil || frame.Error == nil || frame.Error.Code != test.code {
				t.Fatal(string(body), err)
			}
			if test.code == protocol.Internal {
				if string(frame.Error.Data) != `"transfer_unavailable"` {
					t.Fatal(frame.Error)
				}
			} else if len(frame.Error.Data) != 0 {
				t.Fatal("semantic refusal data", frame.Error)
			}
		})
	}
	if reply := transferReply(make(chan int), nil); reply.Error == nil || reply.Error.Code != protocol.Internal {
		t.Fatal("serialization failure became semantic refusal", reply)
	}
	dead := make(chan struct{})
	close(dead)
	link := &hostLink{fd: policyTransferFD(t, "alpha"), inbox: make(chan any), transferBulk: make(chan TransferCall), done: dead}
	for _, method := range []string{"transfer.status", "transfer.chunk"} {
		err := link.transferCall(context.Background(), method, protocol.TransferTarget{ID: "id"}, nil)
		if !errors.Is(err, context.Canceled) {
			t.Fatal("ended generation called unsupported", err)
		}
		code, _ := TransferError(err)
		if code != protocol.Internal {
			t.Fatal("ended generation invented refusal")
		}
	}
}
