// SPDX-License-Identifier: GPL-3.0-only

package transfer

import (
	"sync"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestPendingAndPreparingChargesCannotCreateHiddenCapacity(t *testing.T) {
	b := HostBudget()
	var rs []*Reservation
	for i := 0; i < 64; i++ {
		r, e := b.Pending()
		if e != nil {
			t.Fatal(e)
		}
		rs = append(rs, r)
	}
	if _, e := b.Pending(); e != ErrCapacity {
		t.Fatal("65th pending", e)
	}
	if u := b.Usage(); u.ControlledBytes != 3<<20 {
		t.Fatal(u)
	}
	for i := 0; i < 4; i++ {
		if e := rs[i].Prepare(); e != nil {
			t.Fatal(e)
		}
	}
	if e := rs[4].Prepare(); e != ErrCapacity {
		t.Fatal("fifth preparing", e)
	}
	for i := 0; i < 4; i++ {
		if e := rs[i].Prepare(); e != nil {
			t.Fatal("duplicate preparation", e)
		}
	}
	// Preparing moved, rather than duplicated, the original pending slots.
	if u := b.Usage(); u.Pending != 60 || u.Active != 4 {
		t.Fatal(u)
	}
	for i := 0; i < 4; i++ {
		r, e := b.Pending()
		if e != nil {
			t.Fatal(e)
		}
		rs = append(rs, r)
	}
	if u := b.Usage(); u.ControlledBytes != protocol.TransferHostBufferBytes {
		t.Fatal(u)
	}
	var wg sync.WaitGroup
	for _, r := range rs {
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(r *Reservation) { defer wg.Done(); r.Release() }(r)
		}
	}
	wg.Wait()
	if u := b.Usage(); u != (Usage{}) {
		t.Fatal(u)
	}
	if e := rs[0].Prepare(); e == nil {
		t.Fatal("revived terminal reservation")
	}
	// Later valid admission is unaffected by full/duplicate/terminal events.
	r, e := b.Pending()
	if e != nil {
		t.Fatal(e)
	}
	if e = r.Prepare(); e != nil {
		t.Fatal(e)
	}
	r.Release()
}

func TestHubBoundAndEndpointStagesAreSubsets(t *testing.T) {
	b := HubBudget()
	var rs []*Reservation
	for i := 0; i < 16; i++ {
		r, e := b.Pending()
		if e != nil {
			t.Fatal(e)
		}
		if e = r.Prepare(); e != nil {
			t.Fatal(e)
		}
		rs = append(rs, r)
	}
	for i := 0; i < 256; i++ {
		r, e := b.Pending()
		if e != nil {
			t.Fatal(e)
		}
		rs = append(rs, r)
	}
	if u := b.Usage(); u.ControlledBytes != protocol.TransferHubBufferBytes {
		t.Fatal(u)
	}
	for _, r := range rs {
		r.Release()
	}
}

func TestStageRefusesCopiesAndQueueOverflowWithoutClosingControl(t *testing.T) {
	s := &Stage{}
	var ls []*BufferLease
	for i := 0; i < 4; i++ {
		l, e := s.Reserve(protocol.MaxTransferFrameBytes, Ingress)
		if e != nil {
			t.Fatal(e)
		}
		ls = append(ls, l)
	}
	if _, e := s.Reserve(1, Ingress); e != ErrCapacity {
		t.Fatal("fifth ingress", e)
	}
	for i := 0; i < 4; i++ {
		l, e := s.Reserve(protocol.MaxTransferFrameBytes, Egress)
		if e != nil {
			t.Fatal(e)
		}
		ls = append(ls, l)
	}
	rest := protocol.TransferStageBufferBytes - s.Usage().Bytes
	l, e := s.Reserve(rest, Scratch)
	if e != nil {
		t.Fatal(e)
	}
	ls = append(ls, l)
	if _, e = s.Reserve(1, Scratch); e != ErrCapacity {
		t.Fatal("unaccounted byte", e)
	}
	for _, l := range ls {
		l.Release()
		l.Release()
	}
	if u := s.Usage(); u != (StageUsage{}) {
		t.Fatal(u)
	}
	if l, e = s.Reserve(protocol.TransferChunkBytes, Scratch); e != nil {
		t.Fatal(e)
	} else {
		l.Release()
	}
}

func TestSharedHostAdmissionIncludesFederationAndPreIDReservation(t *testing.T) {
	budget := HostBudget()
	var reservations []*Reservation
	for range protocol.MaxPendingTransfersPerConnection {
		r, e := budget.PendingFor("same-native-generation")
		if e != nil {
			t.Fatal(e)
		}
		reservations = append(reservations, r)
	}
	if _, e := budget.PendingFor("same-native-generation", "other"); e == nil {
		t.Fatal("second local/federated registry bypassed endpoint cap")
	}
	if e := reservations[0].BindPendingEndpoints("other"); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		if e := reservations[i].Prepare(); e != nil {
			t.Fatal(e)
		}
	}
	if e := reservations[2].Prepare(); e == nil {
		t.Fatal("accepted endpoint cap exceeded")
	}
	for _, r := range reservations {
		r.Release()
	}
	if u := budget.Usage(); u.Pending != 0 || u.Active != 0 || u.ControlledBytes != 0 {
		t.Fatal(u)
	}
}
