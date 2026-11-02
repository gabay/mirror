package protocol

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"time"
)

const (
	// DefaultHandshakeTimeout is the default deadline applied to the TLS + mutual secret handshake.
	DefaultHandshakeTimeout = 10 * time.Second

	// tlsExporterLabel binds the mutual HMAC proof to the underlying TLS 1.3 session.
	tlsExporterLabel = "EXPORTER-mirror-v1"

	// authDomainSeparator prefixes the mutual challenge-response HMAC input.
	authDomainSeparator = "mirror-v1-auth"
)

var (
	// ErrSelfConnection is returned when a node connects to itself (remoteNodeID == localNodeID).
	ErrSelfConnection = errors.New("protocol: self connection detected")
	// ErrSecretMismatch is returned when the peer's MIRROR_SECRET does not match the local secret.
	ErrSecretMismatch = errors.New("protocol: secret authentication failed")
	// ErrProtocolVersion is returned when the peer's wire protocol version is incompatible.
	ErrProtocolVersion = errors.New("protocol: unsupported protocol version")
	// ErrUnexpectedMessage is returned when a non-handshake message is received during handshake.
	ErrUnexpectedMessage = errors.New("protocol: unexpected handshake message")
)

// GenerateTLSConfig generates an in-memory ephemeral ECDSA P-256 self-signed certificate
// and returns a *tls.Config enforcing TLS 1.3 for peer-to-peer transport encryption.
func GenerateTLSConfig() (*tls.Config, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("protocol: generate ECDSA P-256 key: %w", err)
	}

	var serialBytes [16]byte
	if _, err := rand.Read(serialBytes[:]); err != nil {
		return nil, fmt.Errorf("protocol: generate certificate serial: %w", err)
	}
	serial := new(big.Int).SetBytes(serialBytes[:])

	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			Organization: []string{"mirror-p2p"},
			CommonName:   "mirror-ephemeral",
		},
		NotBefore:             now.Add(-1 * time.Hour),
		NotAfter:              now.Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		return nil, fmt.Errorf("protocol: create self-signed x509 certificate: %w", err)
	}

	cert := tls.Certificate{
		Certificate: [][]byte{derBytes},
		PrivateKey:  priv,
		Leaf:        tmpl,
	}

	return &tls.Config{
		Certificates:       []tls.Certificate{cert},
		MinVersion:         tls.VersionTLS13,
		MaxVersion:         tls.VersionTLS13,
		InsecureSkipVerify: true, // Peer authenticity is verified via TLS-bound mutual HMAC handshake.
		ClientAuth:         tls.NoClientCert,
	}, nil
}

// HandshakeOptions configures the mutual challenge-response secret handshake.
type HandshakeOptions struct {
	NodeID     string
	Secret     string
	ListenPort int
	Timeout    time.Duration
	Logger     *slog.Logger
}

// PeerInfo describes a remote peer that has completed the mutual handshake.
type PeerInfo struct {
	NodeID          string
	ListenPort      int
	ProtocolVersion uint16
	RemoteAddr      string
}

// ComputeAuthHMAC computes the directional mutual authentication proof:
//
//	HMAC-SHA256(key = []byte(secret), data = "mirror-v1-auth" || senderNodeID || receiverNodeID || senderNonce || receiverNonce || tlsChannelBinding)
//
// Note that even when secret is empty (""), HMAC-SHA256 with an empty key produces a deterministic
// 32-byte tag that matches if and only if the peer also uses an empty secret.
func ComputeAuthHMAC(
	secret string,
	senderNodeID string,
	receiverNodeID string,
	senderNonce [32]byte,
	receiverNonce [32]byte,
	tlsChannelBinding []byte,
) [32]byte {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(authDomainSeparator))

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(senderNodeID)))
	mac.Write(lenBuf[:])
	mac.Write([]byte(senderNodeID))

	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(receiverNodeID)))
	mac.Write(lenBuf[:])
	mac.Write([]byte(receiverNodeID))

	mac.Write(senderNonce[:])
	mac.Write(receiverNonce[:])

	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(tlsChannelBinding)))
	mac.Write(lenBuf[:])
	mac.Write(tlsChannelBinding)

	var out [32]byte
	copy(out[:], mac.Sum(nil))
	return out
}

// Handshake executes the mutual challenge-response secret authentication handshake over conn.
// Both sides must have the exact same Secret (including both empty "") and distinct NodeIDs.
func Handshake(ctx context.Context, conn *Conn, opts HandshakeOptions) (*PeerInfo, error) {
	if opts.NodeID == "" {
		return nil, errors.New("protocol: local NodeID must not be empty")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = DefaultHandshakeTimeout
	}

	deadline := time.Now().Add(timeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, fmt.Errorf("protocol: set handshake deadline: %w", err)
	}
	defer func() {
		_ = conn.SetDeadline(time.Time{})
	}()

	// If the underlying connection is TLS, complete the TLS 1.3 handshake and extract channel binding.
	var tlsBinding []byte
	if tlsConn, ok := conn.NetConn().(*tls.Conn); ok {
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			return nil, fmt.Errorf("protocol: tls handshake: %w", err)
		}
		cs := tlsConn.ConnectionState()
		if cs.Version != tls.VersionTLS13 {
			return nil, fmt.Errorf("protocol: TLS 1.3 required, got version 0x%04x", cs.Version)
		}
		ekm, err := cs.ExportKeyingMaterial(tlsExporterLabel, nil, 32)
		if err != nil {
			return nil, fmt.Errorf("protocol: export TLS keying material: %w", err)
		}
		tlsBinding = ekm
	}

	// Step 1: Generate 32-byte random nonce and exchange HelloChallenge concurrently
	// (concurrent send + receive works on both buffered sockets and unbuffered net.Pipe).
	var localNonce [32]byte
	if _, err := rand.Read(localNonce[:]); err != nil {
		return nil, fmt.Errorf("protocol: generate nonce: %w", err)
	}

	localChallenge := HelloChallenge{
		ProtocolVersion: ProtocolVersion1,
		NodeID:          opts.NodeID,
		ListenPort:      opts.ListenPort,
		Nonce:           localNonce,
	}

	sendErrCh := make(chan error, 1)
	go func() {
		sendErrCh <- conn.Send(MsgAuthChallenge, localChallenge)
	}()

	msgType, rawMsg, err := conn.Receive()
	sendErr := <-sendErrCh
	if err != nil {
		return nil, fmt.Errorf("protocol: read HelloChallenge: %w", err)
	}
	if sendErr != nil {
		return nil, fmt.Errorf("protocol: send HelloChallenge: %w", sendErr)
	}
	if msgType != MsgAuthChallenge {
		return nil, fmt.Errorf("%w: expected %s, got %s", ErrUnexpectedMessage, MsgAuthChallenge, msgType)
	}
	remoteChallenge, ok := rawMsg.(HelloChallenge)
	if !ok {
		return nil, fmt.Errorf("%w: invalid HelloChallenge payload", ErrUnexpectedMessage)
	}

	// Step 2: Check protocol version and reject self-connection immediately.
	if remoteChallenge.ProtocolVersion != ProtocolVersion1 {
		return nil, fmt.Errorf("%w: peer version %d != %d", ErrProtocolVersion, remoteChallenge.ProtocolVersion, ProtocolVersion1)
	}
	if remoteChallenge.NodeID == opts.NodeID {
		return nil, ErrSelfConnection
	}

	// Step 3: Compute our proof and the expected proof from the remote peer.
	localProof := ComputeAuthHMAC(
		opts.Secret,
		opts.NodeID,
		remoteChallenge.NodeID,
		localNonce,
		remoteChallenge.Nonce,
		tlsBinding,
	)
	expectedRemoteProof := ComputeAuthHMAC(
		opts.Secret,
		remoteChallenge.NodeID,
		opts.NodeID,
		remoteChallenge.Nonce,
		localNonce,
		tlsBinding,
	)

	// Step 4: Exchange AuthProof messages.
	go func() {
		sendErrCh <- conn.Send(MsgAuthResponse, AuthProof{HMAC: localProof})
	}()

	msgType, rawMsg, err = conn.Receive()
	sendErr = <-sendErrCh
	if err != nil {
		return nil, fmt.Errorf("protocol: read AuthProof: %w", err)
	}
	if sendErr != nil {
		return nil, fmt.Errorf("protocol: send AuthProof: %w", sendErr)
	}
	if msgType != MsgAuthResponse {
		return nil, fmt.Errorf("%w: expected %s, got %s", ErrUnexpectedMessage, MsgAuthResponse, msgType)
	}
	remoteProof, ok := rawMsg.(AuthProof)
	if !ok {
		return nil, fmt.Errorf("%w: invalid AuthProof payload", ErrUnexpectedMessage)
	}

	// Verify peer's HMAC in constant time.
	matched := hmac.Equal(remoteProof.HMAC[:], expectedRemoteProof[:])

	// Step 5: Exchange AuthAck so the remote peer also knows immediately if it was rejected.
	ack := AuthAck{OK: matched}
	if !matched {
		ack.Error = "secret mismatch"
	}
	go func() {
		sendErrCh <- conn.Send(MsgAuthResult, ack)
	}()

	msgType, rawMsg, recvErr := conn.Receive()
	_ = <-sendErrCh

	remoteAddr := ""
	if conn.RemoteAddr() != nil {
		remoteAddr = conn.RemoteAddr().String()
	}

	if !matched {
		logger.Warn("peer rejected due to MIRROR_SECRET mismatch",
			"remote_node_id", remoteChallenge.NodeID,
			"remote_addr", remoteAddr,
		)
		return nil, ErrSecretMismatch
	}

	if recvErr != nil {
		logger.Warn("peer closed connection during auth ack",
			"remote_node_id", remoteChallenge.NodeID,
			"remote_addr", remoteAddr,
			"err", recvErr,
		)
		return nil, ErrSecretMismatch
	}
	if msgType != MsgAuthResult {
		return nil, fmt.Errorf("%w: expected %s, got %s", ErrUnexpectedMessage, MsgAuthResult, msgType)
	}
	remoteAck, ok := rawMsg.(AuthAck)
	if !ok || !remoteAck.OK {
		logger.Warn("peer rejected our authentication proof",
			"remote_node_id", remoteChallenge.NodeID,
			"remote_addr", remoteAddr,
			"peer_error", remoteAck.Error,
		)
		return nil, ErrSecretMismatch
	}

	return &PeerInfo{
		NodeID:          remoteChallenge.NodeID,
		ListenPort:      remoteChallenge.ListenPort,
		ProtocolVersion: remoteChallenge.ProtocolVersion,
		RemoteAddr:      remoteAddr,
	}, nil
}
