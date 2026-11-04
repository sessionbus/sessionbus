// SPDX-License-Identifier: MIT

package protocol

// Transfer bounds apply to machine data, independently of the ordinary RPC
// frame and pending-operation limits. Paths are deliberately absent here.
const (
	TransferCapability                     = "transfer/1"
	TransferInfoKey                        = "sessionbus_transfer"
	TransferTokenPrefix                    = "sbt1:"
	MaxTransferBytes                 int64 = 1 << 30
	TransferChunkBytes                     = 64 << 10
	TransferCreditBytes                    = 256 << 10
	MaxTransferFrameBytes                  = 88 << 10
	MaxTransferMetadataBytes               = 8 << 10
	MaxPendingTransfersPerHost             = 64
	MaxActiveTransfersPerHost              = 4
	MaxPendingTransfersPerConnection       = 8
	MaxActiveTransfersPerConnection        = 2
	MaxPendingTransfersPerHub              = 256
	MaxActiveTransfersPerHub               = 16
	TransferStageBufferBytes               = 1536 << 10
	TransferPendingCopyBytes               = 16 << 10
	TransferHostBufferBytes                = 21 << 20
	TransferHubBufferBytes                 = 28 << 20
)

// TransferStreamID identifies only a validated opaque transfer, never a path.
func TransferStreamID(params any) string {
	switch p := params.(type) {
	case TransferChunk:
		return p.ID
	case *TransferChunk:
		return p.ID
	}
	return ""
}
