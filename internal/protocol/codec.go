package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/mirror-sync/mirror/internal/model"
)

var (
	// ErrFrameTooLarge is returned when a frame header or payload exceeds maxFrameSize.
	ErrFrameTooLarge = errors.New("protocol: frame exceeds maximum allowed size")
	// ErrUnknownMessageType is returned when an unrecognized MsgType byte is encountered.
	ErrUnknownMessageType = errors.New("protocol: unknown message type")
	// ErrInvalidPayload is returned when a message payload is malformed or of the wrong Go type.
	ErrInvalidPayload = errors.New("protocol: invalid message payload")
)

// Conn is a thread-safe framed duplex wrapper around a net.Conn.
// Multiple goroutines may call Send / WriteFrame concurrently, while a reader loop
// calls Receive / ReadFrame to decode incoming messages.
type Conn struct {
	conn         net.Conn
	writeMu      sync.Mutex
	readMu       sync.Mutex
	maxFrameSize uint32
}

// NewConn wraps an existing net.Conn (typically a *tls.Conn) with framed duplex messaging.
func NewConn(conn net.Conn) *Conn {
	return &Conn{
		conn:         conn,
		maxFrameSize: DefaultMaxFrameSize,
	}
}

// WithMaxFrameSize sets a custom maximum frame payload size on the connection.
func (c *Conn) WithMaxFrameSize(maxBytes uint32) *Conn {
	if maxBytes > 0 {
		c.maxFrameSize = maxBytes
	}
	return c
}

// NetConn returns the underlying net.Conn.
func (c *Conn) NetConn() net.Conn {
	return c.conn
}

// Close closes the underlying connection.
func (c *Conn) Close() error {
	return c.conn.Close()
}

// RemoteAddr returns the remote network address.
func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// LocalAddr returns the local network address.
func (c *Conn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// SetDeadline sets both read and write deadlines on the underlying connection.
func (c *Conn) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

// SetReadDeadline sets the read deadline on the underlying connection.
func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline on the underlying connection.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

// WriteFrame writes a single length-prefixed frame `[1B MsgType][4B uint32 length][payload]`
// atomically under the write mutex.
func (c *Conn) WriteFrame(msgType MsgType, payload []byte) error {
	if uint64(len(payload)) > uint64(c.maxFrameSize) {
		return fmt.Errorf("%w: %d > %d", ErrFrameTooLarge, len(payload), c.maxFrameSize)
	}

	frame := make([]byte, FrameHeaderSize+len(payload))
	frame[0] = byte(msgType)
	binary.BigEndian.PutUint32(frame[1:FrameHeaderSize], uint32(len(payload)))
	copy(frame[FrameHeaderSize:], payload)

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	return writeFull(c.conn, frame)
}

// ReadFrame reads a single length-prefixed frame from the connection.
func (c *Conn) ReadFrame() (MsgType, []byte, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	var header [FrameHeaderSize]byte
	if _, err := io.ReadFull(c.conn, header[:]); err != nil {
		return 0, nil, err
	}

	msgType := MsgType(header[0])
	length := binary.BigEndian.Uint32(header[1:FrameHeaderSize])
	if length > c.maxFrameSize {
		return msgType, nil, fmt.Errorf("%w: %d > %d", ErrFrameTooLarge, length, c.maxFrameSize)
	}

	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(c.conn, payload); err != nil {
			return msgType, nil, err
		}
	}
	return msgType, payload, nil
}

// Send encodes payload for msgType and sends it as a framed message.
func (c *Conn) Send(msgType MsgType, payload any) error {
	data, err := EncodeMessage(msgType, payload)
	if err != nil {
		return err
	}
	return c.WriteFrame(msgType, data)
}

// SendMessage infers the MsgType from the concrete message value and sends it.
func (c *Conn) SendMessage(msg any) error {
	switch m := msg.(type) {
	case HelloChallenge:
		return c.Send(MsgAuthChallenge, m)
	case *HelloChallenge:
		return c.Send(MsgAuthChallenge, *m)
	case AuthProof:
		return c.Send(MsgAuthResponse, m)
	case *AuthProof:
		return c.Send(MsgAuthResponse, *m)
	case AuthAck:
		return c.Send(MsgAuthResult, m)
	case *AuthAck:
		return c.Send(MsgAuthResult, *m)
	case IndexExchange:
		return c.Send(MsgIndexExchange, m)
	case *IndexExchange:
		return c.Send(MsgIndexExchange, *m)
	case IndexUpdate:
		return c.Send(MsgIndexUpdate, m)
	case *IndexUpdate:
		return c.Send(MsgIndexUpdate, *m)
	case BlockRequest:
		return c.Send(MsgBlockRequest, m)
	case *BlockRequest:
		return c.Send(MsgBlockRequest, *m)
	case BlockResponse:
		return c.Send(MsgBlockResponse, m)
	case *BlockResponse:
		return c.Send(MsgBlockResponse, *m)
	case Ping:
		return c.Send(MsgPing, m)
	case *Ping:
		return c.Send(MsgPing, *m)
	case Pong:
		return c.Send(MsgPong, m)
	case *Pong:
		return c.Send(MsgPong, *m)
	default:
		return fmt.Errorf("%w: unsupported type %T", ErrInvalidPayload, msg)
	}
}

// Receive reads the next frame and decodes it into its corresponding message struct.
func (c *Conn) Receive() (MsgType, any, error) {
	msgType, payload, err := c.ReadFrame()
	if err != nil {
		return 0, nil, err
	}
	msg, err := DecodeMessage(msgType, payload)
	if err != nil {
		return msgType, nil, err
	}
	return msgType, msg, nil
}

// SendIndexExchange sends a full FileInfo index exchange message.
func (c *Conn) SendIndexExchange(files []model.FileInfo) error {
	return c.Send(MsgIndexExchange, IndexExchange{Files: files})
}

// SendIndexUpdate sends an incremental FileInfo update message.
func (c *Conn) SendIndexUpdate(files []model.FileInfo) error {
	return c.Send(MsgIndexUpdate, IndexUpdate{Files: files})
}

// SendBlockRequest sends a chunk request message.
func (c *Conn) SendBlockRequest(req BlockRequest) error {
	return c.Send(MsgBlockRequest, req)
}

// SendBlockResponse sends a chunk response message.
func (c *Conn) SendBlockResponse(resp BlockResponse) error {
	return c.Send(MsgBlockResponse, resp)
}

// SendPing sends a Ping message with the given nanosecond timestamp.
func (c *Conn) SendPing(timestampNs int64) error {
	return c.Send(MsgPing, Ping{TimestampNs: timestampNs})
}

// SendPong sends a Pong message echoing the given nanosecond timestamp.
func (c *Conn) SendPong(timestampNs int64) error {
	return c.Send(MsgPong, Pong{TimestampNs: timestampNs})
}

// EncodeMessage serializes a protocol message payload for the given MsgType.
func EncodeMessage(msgType MsgType, payload any) ([]byte, error) {
	switch msgType {
	case MsgAuthChallenge:
		switch v := payload.(type) {
		case HelloChallenge:
			return gobEncode(v)
		case *HelloChallenge:
			if v == nil {
				return nil, ErrInvalidPayload
			}
			return gobEncode(*v)
		default:
			return nil, fmt.Errorf("%w: expected HelloChallenge, got %T", ErrInvalidPayload, payload)
		}

	case MsgAuthResponse:
		switch v := payload.(type) {
		case AuthProof:
			return v.HMAC[:], nil
		case *AuthProof:
			if v == nil {
				return nil, ErrInvalidPayload
			}
			return v.HMAC[:], nil
		default:
			return nil, fmt.Errorf("%w: expected AuthProof, got %T", ErrInvalidPayload, payload)
		}

	case MsgAuthResult:
		switch v := payload.(type) {
		case AuthAck:
			return gobEncode(v)
		case *AuthAck:
			if v == nil {
				return nil, ErrInvalidPayload
			}
			return gobEncode(*v)
		default:
			return nil, fmt.Errorf("%w: expected AuthAck, got %T", ErrInvalidPayload, payload)
		}

	case MsgIndexExchange:
		switch v := payload.(type) {
		case IndexExchange:
			return gobEncode(v)
		case *IndexExchange:
			if v == nil {
				return nil, ErrInvalidPayload
			}
			return gobEncode(*v)
		case []model.FileInfo:
			return gobEncode(IndexExchange{Files: v})
		default:
			return nil, fmt.Errorf("%w: expected IndexExchange, got %T", ErrInvalidPayload, payload)
		}

	case MsgIndexUpdate:
		switch v := payload.(type) {
		case IndexUpdate:
			return gobEncode(v)
		case *IndexUpdate:
			if v == nil {
				return nil, ErrInvalidPayload
			}
			return gobEncode(*v)
		case []model.FileInfo:
			return gobEncode(IndexUpdate{Files: v})
		default:
			return nil, fmt.Errorf("%w: expected IndexUpdate, got %T", ErrInvalidPayload, payload)
		}

	case MsgBlockRequest:
		var req BlockRequest
		switch v := payload.(type) {
		case BlockRequest:
			req = v
		case *BlockRequest:
			if v == nil {
				return nil, ErrInvalidPayload
			}
			req = *v
		default:
			return nil, fmt.Errorf("%w: expected BlockRequest, got %T", ErrInvalidPayload, payload)
		}
		buf := make([]byte, 8+32+4)
		binary.BigEndian.PutUint64(buf[0:8], req.RequestID)
		copy(buf[8:40], req.Hash[:])
		binary.BigEndian.PutUint32(buf[40:44], uint32(req.Size))
		return buf, nil

	case MsgBlockResponse:
		var resp BlockResponse
		switch v := payload.(type) {
		case BlockResponse:
			resp = v
		case *BlockResponse:
			if v == nil {
				return nil, ErrInvalidPayload
			}
			resp = *v
		default:
			return nil, fmt.Errorf("%w: expected BlockResponse, got %T", ErrInvalidPayload, payload)
		}
		errBytes := []byte(resp.Error)
		buf := make([]byte, 8+32+4+len(errBytes)+4+len(resp.Data))
		binary.BigEndian.PutUint64(buf[0:8], resp.RequestID)
		copy(buf[8:40], resp.Hash[:])
		binary.BigEndian.PutUint32(buf[40:44], uint32(len(errBytes)))
		copy(buf[44:44+len(errBytes)], errBytes)
		dataOffset := 44 + len(errBytes)
		binary.BigEndian.PutUint32(buf[dataOffset:dataOffset+4], uint32(len(resp.Data)))
		copy(buf[dataOffset+4:], resp.Data)
		return buf, nil

	case MsgPing:
		var p Ping
		switch v := payload.(type) {
		case Ping:
			p = v
		case *Ping:
			if v == nil {
				return nil, ErrInvalidPayload
			}
			p = *v
		default:
			return nil, fmt.Errorf("%w: expected Ping, got %T", ErrInvalidPayload, payload)
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(p.TimestampNs))
		return buf[:], nil

	case MsgPong:
		var p Pong
		switch v := payload.(type) {
		case Pong:
			p = v
		case *Pong:
			if v == nil {
				return nil, ErrInvalidPayload
			}
			p = *v
		default:
			return nil, fmt.Errorf("%w: expected Pong, got %T", ErrInvalidPayload, payload)
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(p.TimestampNs))
		return buf[:], nil

	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownMessageType, msgType)
	}
}

// DecodeMessage deserializes raw frame bytes into the typed struct for msgType.
func DecodeMessage(msgType MsgType, data []byte) (any, error) {
	switch msgType {
	case MsgAuthChallenge:
		var v HelloChallenge
		if err := gobDecode(data, &v); err != nil {
			return nil, fmt.Errorf("%w: decode HelloChallenge: %v", ErrInvalidPayload, err)
		}
		return v, nil

	case MsgAuthResponse:
		if len(data) != 32 {
			return nil, fmt.Errorf("%w: AuthProof must be 32 bytes, got %d", ErrInvalidPayload, len(data))
		}
		var v AuthProof
		copy(v.HMAC[:], data)
		return v, nil

	case MsgAuthResult:
		var v AuthAck
		if err := gobDecode(data, &v); err != nil {
			return nil, fmt.Errorf("%w: decode AuthAck: %v", ErrInvalidPayload, err)
		}
		return v, nil

	case MsgIndexExchange:
		var v IndexExchange
		if err := gobDecode(data, &v); err != nil {
			return nil, fmt.Errorf("%w: decode IndexExchange: %v", ErrInvalidPayload, err)
		}
		return v, nil

	case MsgIndexUpdate:
		var v IndexUpdate
		if err := gobDecode(data, &v); err != nil {
			return nil, fmt.Errorf("%w: decode IndexUpdate: %v", ErrInvalidPayload, err)
		}
		return v, nil

	case MsgBlockRequest:
		if len(data) != 44 {
			return nil, fmt.Errorf("%w: BlockRequest must be 44 bytes, got %d", ErrInvalidPayload, len(data))
		}
		var req BlockRequest
		req.RequestID = binary.BigEndian.Uint64(data[0:8])
		copy(req.Hash[:], data[8:40])
		req.Size = int32(binary.BigEndian.Uint32(data[40:44]))
		return req, nil

	case MsgBlockResponse:
		if len(data) < 48 {
			return nil, fmt.Errorf("%w: BlockResponse too short (%d bytes)", ErrInvalidPayload, len(data))
		}
		var resp BlockResponse
		resp.RequestID = binary.BigEndian.Uint64(data[0:8])
		copy(resp.Hash[:], data[8:40])
		errLen := binary.BigEndian.Uint32(data[40:44])
		if uint64(44)+uint64(errLen)+4 > uint64(len(data)) {
			return nil, fmt.Errorf("%w: BlockResponse error length overflow", ErrInvalidPayload)
		}
		if errLen > 0 {
			resp.Error = string(data[44 : 44+errLen])
		}
		dataOffset := 44 + int(errLen)
		dataLen := binary.BigEndian.Uint32(data[dataOffset : dataOffset+4])
		if uint64(dataOffset+4)+uint64(dataLen) != uint64(len(data)) {
			return nil, fmt.Errorf("%w: BlockResponse data length mismatch", ErrInvalidPayload)
		}
		if dataLen > 0 {
			resp.Data = make([]byte, dataLen)
			copy(resp.Data, data[dataOffset+4:])
		}
		return resp, nil

	case MsgPing:
		if len(data) != 8 {
			return nil, fmt.Errorf("%w: Ping must be 8 bytes, got %d", ErrInvalidPayload, len(data))
		}
		return Ping{TimestampNs: int64(binary.BigEndian.Uint64(data))}, nil

	case MsgPong:
		if len(data) != 8 {
			return nil, fmt.Errorf("%w: Pong must be 8 bytes, got %d", ErrInvalidPayload, len(data))
		}
		return Pong{TimestampNs: int64(binary.BigEndian.Uint64(data))}, nil

	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownMessageType, msgType)
	}
}

func gobEncode(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gobDecode(data []byte, dst any) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(dst)
}

func writeFull(w io.Writer, buf []byte) error {
	for len(buf) > 0 {
		n, err := w.Write(buf)
		buf = buf[n:]
		if err != nil {
			return err
		}
	}
	return nil
}
