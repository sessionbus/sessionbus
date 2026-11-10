// SPDX-License-Identifier: MIT

package protocol

// Policy refusals require their own opt-in; availability-only kits predate
// the code. Keep both older opaque Worker bootstrap prefixes intact.
const (
	PolicyCapability  = "policy/1"
	PolicyInfoKey     = "sessionbus_policy"
	PolicyTokenPrefix = AvailabilityTokenPrefix + "fp1:"
	PolicyReason      = "federation_policy_denied"
)
