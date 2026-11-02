package protocol

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mirror-sync/mirror/internal/model"
)

// newTLSPair creates an in-memory TLS 1.3 client/server connection pair over a local TCP listener.
func newTLSPair(t *testing.T) (*Conn, *Conn, func()) {
	t.Helper()

	clientTLS, err := GenerateTLSConfig()
	if err != nil {
		t.Fatalf("GenerateTLSConfig client: %v", err)
	}
	serverTLS, err := GenerateTLSConfig()
	if err != nil {
		t.Fatalf("GenerateTLSConfig server: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}

	type acceptRes struct {
		conn net.Conn
		err  error
	}
	acceptCh := make(chan acceptRes, 1)
	go func() {
		c, err := ln.Accept()
		acceptCh <- acceptRes{conn: c, err: err}
	}()

	clientTCP, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		_ = ln.Close()
		t.Fatalf("net.Dial: %v", err)
	}

	ar := <-acceptCh
	if ar.err != nil {
		_ = clientTCP.Close()
		_ = ln.Close()
		t.Fatalf("ln.Accept: %v", ar.err)
	}

	clientRaw := tls.Client(clientTCP, clientTLS)
	serverRaw := tls.Server(ar.conn, serverTLS)

	clientConn := NewConn(clientRaw)
	serverConn := NewConn(serverRaw)
	cleanup := func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
		_ = ln.Close()
	}
	return clientConn, serverConn, cleanup
}

func runHandshakePair(
	t *testing.T,
	c1, c2 *Conn,
	opts1, opts2 HandshakeOptions,
) (*PeerInfo, *PeerInfo, error, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		p1, p2     *PeerInfo
		err1, err2 error
		wg         sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		p1, err1 = Handshake(ctx, c1, opts1)
	}()
	go func() {
		defer wg.Done()
		p2, err2 = Handshake(ctx, c2, opts2)
	}()
	wg.Wait()
	return p1, p2, err1, err2
}

func TestHandshake_MatchingNonEmptySecret(t *testing.T) {
	c1, c2, cleanup := newTLSPair(t)
	defer cleanup()

	p1, p2, err1, err2 := runHandshakePair(
		t,
		c1, c2,
		HandshakeOptions{NodeID: "node-alpha", Secret: "super-secret", ListenPort: 22001},
		HandshakeOptions{NodeID: "node-beta", Secret: "super-secret", ListenPort: 22003},
	)
	if err1 != nil || err2 != nil {
		t.Fatalf("expected handshake success, got err1=%v, err2=%v", err1, err2)
	}
	if p1.NodeID != "node-beta" || p1.ListenPort != 22003 {
		t.Fatalf("unexpected peer1 info: %+v", p1)
	}
	if p2.NodeID != "node-alpha" || p2.ListenPort != 22001 {
		t.Fatalf("unexpected peer2 info: %+v", p2)
	}

	// Verify TLS 1.3 was negotiated.
	tlsConn := c1.NetConn().(*tls.Conn)
	if v := tlsConn.ConnectionState().Version; v != tls.VersionTLS13 {
		t.Fatalf("expected TLS 1.3 (0x%04x), got 0x%04x", tls.VersionTLS13, v)
	}
}

func TestHandshake_MatchingEmptySecret(t *testing.T) {
	c1, c2, cleanup := newTLSPair(t)
	defer cleanup()

	p1, p2, err1, err2 := runHandshakePair(
		t,
		c1, c2,
		HandshakeOptions{NodeID: "node-1", Secret: "", ListenPort: 22001},
		HandshakeOptions{NodeID: "node-2", Secret: "", ListenPort: 22001},
	)
	if err1 != nil || err2 != nil {
		t.Fatalf("expected handshake with matching empty secrets to succeed, got err1=%v, err2=%v", err1, err2)
	}
	if p1.NodeID != "node-2" || p2.NodeID != "node-1" {
		t.Fatalf("unexpected peer IDs: p1=%+v, p2=%+v", p1, p2)
	}
}

func TestHandshake_MismatchedSecrets(t *testing.T) {
	cases := []struct {
		name    string
		secret1 string
		secret2 string
	}{
		{
			name:    "empty_vs_non_empty",
			secret1: "",
			secret2: "secret",
		},
		{
			name:    "non_empty_vs_empty",
			secret1: "secret",
			secret2: "",
		},
		{
			name:    "different_non_empty_secrets",
			secret1: "secret1",
			secret2: "secret2",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c1, c2, cleanup := newTLSPair(t)
			defer cleanup()

			_, _, err1, err2 := runHandshakePair(
				t,
				c1, c2,
				HandshakeOptions{NodeID: "node-a", Secret: tc.secret1, ListenPort: 22001},
				HandshakeOptions{NodeID: "node-b", Secret: tc.secret2, ListenPort: 22001},
			)
			if !errors.Is(err1, ErrSecretMismatch) {
				t.Fatalf("expected peer 1 ErrSecretMismatch, got: %v", err1)
			}
			if !errors.Is(err2, ErrSecretMismatch) {
				t.Fatalf("expected peer 2 ErrSecretMismatch, got: %v", err2)
			}
		})
	}
}

func TestHandshake_SelfConnectionRejected(t *testing.T) {
	c1, c2, cleanup := newTLSPair(t)
	defer cleanup()

	_, _, err1, err2 := runHandshakePair(
		t,
		c1, c2,
		HandshakeOptions{NodeID: "same-node-id", Secret: "shared", ListenPort: 22001},
		HandshakeOptions{NodeID: "same-node-id", Secret: "shared", ListenPort: 22001},
	)
	if !errors.Is(err1, ErrSelfConnection) {
		t.Fatalf("expected peer 1 ErrSelfConnection, got: %v", err1)
	}
	if !errors.Is(err2, ErrSelfConnection) {
		t.Fatalf("expected peer 2 ErrSelfConnection, got: %v", err2)
	}
}

func TestCodec_FullDuplexFramingAndConcurrentBlockTransfer(t *testing.T) {
	c1, c2, cleanup := newTLSPair(t)
	defer cleanup()

	_, _, err1, err2 := runHandshakePair(
		t,
		c1, c2,
		HandshakeOptions{NodeID: "node-client", Secret: "s3cr3t", ListenPort: 22001},
		HandshakeOptions{NodeID: "node-server", Secret: "s3cr3t", ListenPort: 22001},
	)
	if err1 != nil || err2 != nil {
		t.Fatalf("handshake failed: %v / %v", err1, err2)
	}

	var fileHash [32]byte
	var blkHash [32]byte
	for i := range 32 {
		fileHash[i] = byte(i + 1)
		blkHash[i] = byte(100 + i)
	}

	sampleFiles := []model.FileInfo{
		{
			Path:       "docs/readme.md",
			Type:       model.FileTypeRegular,
			Size:       4096,
			Mode:       0o644,
			Uid:        1000,
			Gid:        1000,
			ModTimeNs:  1700000000123456789,
			Version:    model.VersionVector{"node-client": 3, "node-server": 1},
			ModifiedBy: "node-client",
			FileHash:   fileHash,
			Blocks: []model.BlockInfo{
				{Index: 0, Offset: 0, Size: 4096, Hash: blkHash},
			},
		},
		{
			Path:          "bin/current",
			Type:          model.FileTypeSymlink,
			Mode:          0o777,
			ModTimeNs:     1700000000999999999,
			SymlinkTarget: "../releases/v1",
			Version:       model.VersionVector{"node-client": 1},
			ModifiedBy:    "node-client",
		},
	}

	// 1. Test IndexExchange & IndexUpdate.
	if err := c1.SendIndexExchange(sampleFiles); err != nil {
		t.Fatalf("SendIndexExchange: %v", err)
	}
	msgType, raw, err := c2.Receive()
	if err != nil {
		t.Fatalf("c2.Receive IndexExchange: %v", err)
	}
	if msgType != MsgIndexExchange {
		t.Fatalf("expected MsgIndexExchange, got %s", msgType)
	}
	idxEx := raw.(IndexExchange)
	if len(idxEx.Files) != 2 || idxEx.Files[0].Path != "docs/readme.md" || idxEx.Files[1].SymlinkTarget != "../releases/v1" {
		t.Fatalf("unexpected IndexExchange payload: %+v", idxEx)
	}

	if err := c2.SendIndexUpdate(sampleFiles[:1]); err != nil {
		t.Fatalf("SendIndexUpdate: %v", err)
	}
	msgType, raw, err = c1.Receive()
	if err != nil {
		t.Fatalf("c1.Receive IndexUpdate: %v", err)
	}
	if msgType != MsgIndexUpdate {
		t.Fatalf("expected MsgIndexUpdate, got %s", msgType)
	}
	idxUp := raw.(IndexUpdate)
	if len(idxUp.Files) != 1 || idxUp.Files[0].FileHash != fileHash {
		t.Fatalf("unexpected IndexUpdate payload: %+v", idxUp)
	}

	// 2. Test concurrent full-duplex BlockRequest / BlockResponse + Ping / Pong.
	const numRequests = 25
	serverDone := make(chan error, 1)

	// Server loop on c2 reading requests/pings and sending responses/pongs concurrently.
	go func() {
		var wg sync.WaitGroup
		for range numRequests + 1 {
			mType, msg, err := c2.Receive()
			if err != nil {
				serverDone <- fmt.Errorf("c2.Receive: %w", err)
				return
			}
			switch mType {
			case MsgBlockRequest:
				req := msg.(BlockRequest)
				wg.Add(1)
				go func(r BlockRequest) {
					defer wg.Done()
					payload := bytes.Repeat([]byte{byte(r.RequestID)}, int(r.Size))
					_ = c2.SendBlockResponse(BlockResponse{
						RequestID: r.RequestID,
						Hash:      r.Hash,
						Data:      payload,
					})
				}(req)
			case MsgPing:
				p := msg.(Ping)
				wg.Add(1)
				go func(ts int64) {
					defer wg.Done()
					_ = c2.SendPong(ts)
				}(p.TimestampNs)
			default:
				serverDone <- fmt.Errorf("unexpected message on server: %s", mType)
				return
			}
		}
		wg.Wait()
		serverDone <- nil
	}()

	// Client sends concurrent BlockRequests + 1 Ping from multiple goroutines.
	var sendWG sync.WaitGroup
	for i := 1; i <= numRequests; i++ {
		sendWG.Add(1)
		go func(id uint64) {
			defer sendWG.Done()
			var h [32]byte
			h[0] = byte(id)
			if err := c1.SendBlockRequest(BlockRequest{
				RequestID: id,
				Hash:      h,
				Size:      1024,
			}); err != nil {
				t.Errorf("SendBlockRequest(%d): %v", id, err)
			}
		}(uint64(i))
	}
	const pingTS int64 = 1234567890123
	sendWG.Add(1)
	go func() {
		defer sendWG.Done()
		if err := c1.SendPing(pingTS); err != nil {
			t.Errorf("SendPing: %v", err)
		}
	}()
	sendWG.Wait()

	receivedBlocks := make(map[uint64]bool)
	gotPong := false
	for range numRequests + 1 {
		mType, msg, err := c1.Receive()
		if err != nil {
			t.Fatalf("c1.Receive: %v", err)
		}
		switch mType {
		case MsgBlockResponse:
			resp := msg.(BlockResponse)
			if len(resp.Data) != 1024 || resp.Data[0] != byte(resp.RequestID) {
				t.Fatalf("corrupted BlockResponse data for id=%d", resp.RequestID)
			}
			receivedBlocks[resp.RequestID] = true
		case MsgPong:
			pong := msg.(Pong)
			if pong.TimestampNs != pingTS {
				t.Fatalf("expected pong timestamp %d, got %d", pingTS, pong.TimestampNs)
			}
			gotPong = true
		default:
			t.Fatalf("unexpected message on client: %s", mType)
		}
	}

	if err := <-serverDone; err != nil {
		t.Fatalf("server loop failed: %v", err)
	}
	if len(receivedBlocks) != numRequests {
		t.Fatalf("expected %d block responses, got %d", numRequests, len(receivedBlocks))
	}
	if !gotPong {
		t.Fatalf("expected Pong response")
	}
}

func TestCodec_MaxFrameSizeEnforced(t *testing.T) {
	p1, p2 := net.Pipe()
	defer p1.Close()
	defer p2.Close()

	c1 := NewConn(p1).WithMaxFrameSize(128)
	err := c1.WriteFrame(MsgPing, make([]byte, 256))
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("expected ErrFrameTooLarge, got: %v", err)
	}
}
