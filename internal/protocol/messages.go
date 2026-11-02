package protocol

import (
	"fmt"

	"github.com/mirror-sync/mirror/internal/model"
)

const (
	// ProtocolVersion1 is the wire protocol version implemented by this package.
	ProtocolVersion1 uint16 = 1

	// DefaultMaxFrameSize caps a single wire frame at 16 MiB to prevent memory exhaustion.
	DefaultMaxFrameSize uint32 = 16 * 1024 * 1024

	// FrameHeaderSize is 1 byte MsgType + 4 bytes uint32 big-endian payload length.
	FrameHeaderSize = 5
)

// MsgType identifies the type of a framed wire protocol message.
type MsgType uint8

const (
	MsgAuthChallenge MsgType = 1
	MsgAuthResponse  MsgType = 2
	MsgAuthResult    MsgType = 3
	MsgIndexExchange MsgType = 4
	MsgIndexUpdate   MsgType = 5
	MsgBlockRequest  MsgType = 6
	MsgBlockResponse MsgType = 7
	MsgPing          MsgType = 8
	MsgPong          MsgType = 9
)

func (m MsgType) String() string {
	switch m {
	case MsgAuthChallenge:
		return "AuthChallenge"
	case MsgAuthResponse:
		return "AuthResponse"
	case MsgAuthResult:
		return "AuthResult"
	case MsgIndexExchange:
		return "IndexExchange"
	case MsgIndexUpdate:
		return "IndexUpdate"
	case MsgBlockRequest:
		return "BlockRequest"
	case MsgBlockResponse:
		return "BlockResponse"
	case MsgPing:
		return "Ping"
	case MsgPong:
		return "Pong"
	default:
		return fmt.Sprintf("MsgType(%d)", uint8(m))
	}
}

// HelloChallenge is exchanged first during the mutual authentication handshake.
type HelloChallenge struct {
	ProtocolVersion uint16
	NodeID          string
	ListenPort      int
	Nonce           [32]byte
}

// AuthProof carries the sender's HMAC-SHA256 proof over the handshake transcript and TLS channel binding.
type AuthProof struct {
	HMAC [32]byte
}

// AuthAck informs the peer whether its authentication proof was accepted.
type AuthAck struct {
	OK    bool
	Error string
}

// IndexExchange carries the full list of tracked FileInfo entries immediately after handshake.
type IndexExchange struct {
	Files []model.FileInfo
}

// IndexUpdate carries incremental FileInfo changes pushed in real time as files change.
type IndexUpdate struct {
	Files []model.FileInfo
}

// BlockRequest requests a specific content-defined chunk from a peer by BLAKE3 hash.
type BlockRequest struct {
	RequestID uint64
	Hash      [32]byte
	Size      int32
}

// BlockResponse returns the raw chunk data (or an error string) for a prior BlockRequest.
type BlockResponse struct {
	RequestID uint64
	Hash      [32]byte
	Error     string
	Data      []byte
}

// Ping measures peer liveness and round-trip latency.
type Ping struct {
	TimestampNs int64
}

// Pong replies to a Ping message echoing the original timestamp.
type Pong struct {
	TimestampNs int64
}
