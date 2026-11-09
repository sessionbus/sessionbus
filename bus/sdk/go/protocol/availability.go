// SPDX-License-Identifier: MIT

package protocol

// Availability is negotiated independently from transfer support. The worker
// token remains inside the existing transfer-token namespace for old workers.
const (
	AvailabilityCapability  = "availability/1"
	AvailabilityInfoKey     = "sessionbus_availability"
	AvailabilityTokenPrefix = TransferTokenPrefix + "av1:"
)
