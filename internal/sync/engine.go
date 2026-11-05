package sync

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mirror-sync/mirror/internal/chunker"
	"github.com/mirror-sync/mirror/internal/config"
	"github.com/mirror-sync/mirror/internal/discovery"
	"github.com/mirror-sync/mirror/internal/fs"
	"github.com/mirror-sync/mirror/internal/ignore"
	"github.com/mirror-sync/mirror/internal/model"
	"github.com/mirror-sync/mirror/internal/protocol"
	"github.com/mirror-sync/mirror/internal/store"
)

const (
	defaultDialTimeout         = 5 * time.Second
	defaultBlockRequestTimeout = 30 * time.Second
	defaultDiscoveryPoll       = 5 * time.Second
	maxConcurrentBlockServes   = 64
)

// PeerSession represents an active authenticated TLS 1.3 session with a remote peer.
type PeerSession struct {
	engine         *Engine
	conn           *protocol.Conn
	peerInfo       *protocol.PeerInfo
	isOutbound     bool
	isCanonical    bool
	dialedAddr     string
	peerListenAddr string

	nextReqID   atomic.Uint64
	reqMu       sync.Mutex
	pendingReqs map[uint64]chan protocol.BlockResponse

	closed    atomic.Bool
	closeOnce sync.Once
	doneCh    chan struct{}
}

func newPeerSession(
	engine *Engine,
	conn *protocol.Conn,
	peerInfo *protocol.PeerInfo,
	isOutbound bool,
	isCanonical bool,
	dialedAddr string,
	peerListenAddr string,
) *PeerSession {
	return &PeerSession{
		engine:         engine,
		conn:           conn,
		peerInfo:       peerInfo,
		isOutbound:     isOutbound,
		isCanonical:    isCanonical,
		dialedAddr:     dialedAddr,
		peerListenAddr: peerListenAddr,
		pendingReqs:    make(map[uint64]chan protocol.BlockResponse),
		doneCh:         make(chan struct{}),
	}
}

// NodeID returns the remote peer's NodeID.
func (s *PeerSession) NodeID() string {
	if s.peerInfo == nil {
		return ""
	}
	return s.peerInfo.NodeID
}

// RemoteAddr returns the remote peer's network address.
func (s *PeerSession) RemoteAddr() string {
	if s.peerInfo == nil {
		return ""
	}
	return s.peerInfo.RemoteAddr
}

func (s *PeerSession) isClosed() bool {
	return s.closed.Load()
}

// Close terminates the peer session and unblocks any pending block requests.
func (s *PeerSession) Close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		close(s.doneCh)
		closeErr = s.conn.Close()

		s.reqMu.Lock()
		for id, ch := range s.pendingReqs {
			close(ch)
			delete(s.pendingReqs, id)
		}
		s.reqMu.Unlock()
	})
	return closeErr
}

// FetchBlock requests a single content block by its 32-byte BLAKE3 hash and size from this peer.
func (s *PeerSession) FetchBlock(ctx context.Context, hash [32]byte, size int32) ([]byte, error) {
	if s.isClosed() {
		return nil, errors.New("sync: peer session is closed")
	}

	reqID := s.nextReqID.Add(1)
	respCh := make(chan protocol.BlockResponse, 1)

	s.reqMu.Lock()
	if s.isClosed() {
		s.reqMu.Unlock()
		return nil, errors.New("sync: peer session is closed")
	}
	s.pendingReqs[reqID] = respCh
	s.reqMu.Unlock()

	defer func() {
		s.reqMu.Lock()
		delete(s.pendingReqs, reqID)
		s.reqMu.Unlock()
	}()

	err := s.conn.SendBlockRequest(protocol.BlockRequest{
		RequestID: reqID,
		Hash:      hash,
		Size:      size,
	})
	if err != nil {
		return nil, fmt.Errorf("sync: send block request to %s: %w", s.NodeID(), err)
	}

	timer := time.NewTimer(defaultBlockRequestTimeout)
	defer timer.Stop()

	select {
	case resp, ok := <-respCh:
		if !ok {
			return nil, fmt.Errorf("sync: peer session %s closed while awaiting block %x", s.NodeID(), hash[:4])
		}
		if resp.Error != "" {
			return nil, fmt.Errorf("sync: peer %s block %x error: %s", s.NodeID(), hash[:4], resp.Error)
		}
		if int32(len(resp.Data)) != size {
			return nil, fmt.Errorf("sync: peer %s block %x size mismatch: got %d, want %d", s.NodeID(), hash[:4], len(resp.Data), size)
		}
		if chunker.HashBytes(resp.Data) != hash {
			return nil, fmt.Errorf("sync: peer %s block %x hash mismatch", s.NodeID(), hash[:4])
		}
		return resp.Data, nil

	case <-s.doneCh:
		return nil, fmt.Errorf("sync: peer session %s closed", s.NodeID())

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-timer.C:
		return nil, fmt.Errorf("sync: timeout fetching block %x from peer %s", hash[:4], s.NodeID())
	}
}

func (s *PeerSession) readLoop() {
	defer func() {
		_ = s.Close()
		s.engine.removeSession(s)
	}()

	for {
		msgType, rawMsg, err := s.conn.Receive()
		if err != nil {
			if !s.isClosed() && s.engine.ctx.Err() == nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				s.engine.logger.Debug("peer session read ended",
					"remote_node_id", s.NodeID(),
					"remote_addr", s.RemoteAddr(),
					"err", err,
				)
			}
			return
		}

		switch msgType {
		case protocol.MsgIndexExchange:
			idx, ok := rawMsg.(protocol.IndexExchange)
			if !ok {
				continue
			}
			s.engine.logger.Debug("received IndexExchange from peer",
				"remote_node_id", s.NodeID(),
				"entries", len(idx.Files),
			)
			s.engine.enqueueReconcile(s, idx.Files)

		case protocol.MsgIndexUpdate:
			upd, ok := rawMsg.(protocol.IndexUpdate)
			if !ok {
				continue
			}
			s.engine.logger.Debug("received IndexUpdate from peer",
				"remote_node_id", s.NodeID(),
				"entries", len(upd.Files),
			)
			s.engine.enqueueReconcile(s, upd.Files)

		case protocol.MsgBlockRequest:
			req, ok := rawMsg.(protocol.BlockRequest)
			if !ok {
				continue
			}
			s.engine.dispatchBlockRequest(s, req)

		case protocol.MsgBlockResponse:
			resp, ok := rawMsg.(protocol.BlockResponse)
			if !ok {
				continue
			}
			s.reqMu.Lock()
			ch, exists := s.pendingReqs[resp.RequestID]
			if exists {
				select {
				case ch <- resp:
				default:
				}
			}
			s.reqMu.Unlock()

		case protocol.MsgPing:
			ping, ok := rawMsg.(protocol.Ping)
			if !ok {
				continue
			}
			_ = s.conn.SendPong(ping.TimestampNs)

		case protocol.MsgPong:
			// Liveness confirmed.
		}
	}
}

type reconcileTask struct {
	session *PeerSession
	files   []model.FileInfo
}

// Engine coordinates local scanning, filesystem watching, peer discovery, TLS 1.3 sessions,
// delta index exchange, conflict resolution, and block transfers.
type Engine struct {
	cfg       config.Config
	store     *store.Store
	matcher   *ignore.Matcher
	scanner   *fs.Scanner
	watcher   *fs.Watcher
	writer    *fs.Writer
	discovery *discovery.Manager
	tlsConfig *tls.Config
	listener  net.Listener
	logger    *slog.Logger

	listenAddrMu sync.RWMutex
	listenAddr   string
	listenPort   int

	peersMu      sync.RWMutex
	peers        map[string]*PeerSession
	addrToNodeID map[string]string
	dialing      map[string]bool
	selfAddrs    map[string]bool

	opMu      sync.Mutex
	pathLocks sync.Map

	reconcileQueueMu sync.Mutex
	reconcilePending []reconcileTask
	reconcileNotify  chan struct{}

	blockServeSem chan struct{}

	resolver          discovery.Resolver
	discoveryInterval time.Duration
	lanTargets        []string

	ctx       context.Context
	cancel    context.CancelFunc
	started   atomic.Bool
	closed    atomic.Bool
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// NewEngine constructs a new distributed sync Engine.
func NewEngine(cfg config.Config, st *store.Store, logger *slog.Logger) (*Engine, error) {
	if st == nil {
		return nil, errors.New("sync: store must not be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}

	if strings.TrimSpace(cfg.Path) == "" {
		cfg.Path = config.DefaultPath
	}
	if strings.TrimSpace(cfg.ListenAddr) == "" {
		cfg.ListenAddr = config.DefaultListenAddr
	}
	if strings.TrimSpace(cfg.ConflictStrategy) == "" {
		cfg.ConflictStrategy = config.DefaultConflictStrategy
	}
	if cfg.RescanInterval <= 0 {
		cfg.RescanInterval = config.DefaultRescanInterval
	}
	if cfg.DebounceDelay <= 0 {
		cfg.DebounceDelay = config.DefaultDebounceDelay
	}
	if cfg.DiscoveryPort <= 0 {
		cfg.DiscoveryPort = config.DefaultDiscoveryPort
	}
	if cfg.NodeID == "" {
		if cfg.StateDir != "" {
			if err := cfg.EnsureNodeID(); err != nil {
				return nil, fmt.Errorf("sync: ensure node_id: %w", err)
			}
		} else {
			return nil, errors.New("sync: NodeID must not be empty")
		}
	}

	if err := os.MkdirAll(cfg.Path, 0o755); err != nil {
		return nil, fmt.Errorf("sync: mkdir path %q: %w", cfg.Path, err)
	}

	tlsCfg, err := protocol.GenerateTLSConfig()
	if err != nil {
		return nil, fmt.Errorf("sync: generate TLS config: %w", err)
	}

	matcher := ignore.NewMatcher(cfg.Exclude)
	scanner := fs.NewScanner(cfg.Path, cfg.NodeID, st, matcher, logger)
	writer := fs.NewWriter(cfg.Path, st, logger)

	ctx, cancel := context.WithCancel(context.Background())

	return &Engine{
		cfg:               cfg,
		store:             st,
		matcher:           matcher,
		scanner:           scanner,
		writer:            writer,
		tlsConfig:         tlsCfg,
		logger:            logger,
		listenAddr:        cfg.ListenAddr,
		listenPort:        discovery.ExtractListenPort(cfg.ListenAddr, 22001),
		peers:             make(map[string]*PeerSession),
		addrToNodeID:      make(map[string]string),
		dialing:           make(map[string]bool),
		selfAddrs:         make(map[string]bool),
		reconcileNotify:   make(chan struct{}, 1),
		blockServeSem:     make(chan struct{}, maxConcurrentBlockServes),
		discoveryInterval: defaultDiscoveryPoll,
		ctx:               ctx,
		cancel:            cancel,
	}, nil
}

// WithResolver configures a custom DNS Resolver on the Engine's discovery manager.
func (e *Engine) WithResolver(r discovery.Resolver) *Engine {
	if r != nil {
		e.resolver = r
	}
	return e
}

// WithDiscoveryInterval configures the polling interval for peer discovery.
func (e *Engine) WithDiscoveryInterval(d time.Duration) *Engine {
	if d > 0 {
		e.discoveryInterval = d
	}
	return e
}

// WithLANBroadcastTargets overrides the UDP broadcast targets for LAN discovery.
func (e *Engine) WithLANBroadcastTargets(targets []string) *Engine {
	e.lanTargets = targets
	return e
}

// WithChunker configures a custom Chunker on the Engine's Scanner.
func (e *Engine) WithChunker(c *chunker.Chunker) *Engine {
	if c != nil {
		e.scanner.WithChunker(c)
	}
	return e
}

// ListenAddr returns the bound TCP address (host:port) of the Engine's TLS listener.
func (e *Engine) ListenAddr() string {
	e.listenAddrMu.RLock()
	defer e.listenAddrMu.RUnlock()
	return e.listenAddr
}

// NodeID returns the local NodeID of the Engine.
func (e *Engine) NodeID() string {
	return e.cfg.NodeID
}

// Store returns the Engine's underlying state Store.
func (e *Engine) Store() *store.Store {
	return e.store
}

// Scanner returns the Engine's filesystem Scanner.
func (e *Engine) Scanner() *fs.Scanner {
	return e.scanner
}

// ConnectedPeers returns the sorted list of remote NodeIDs with active sessions.
func (e *Engine) ConnectedPeers() []string {
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()

	out := make([]string, 0, len(e.peers))
	for id, sess := range e.peers {
		if !sess.isClosed() {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// PeerCount returns the number of currently active peer sessions.
func (e *Engine) PeerCount() int {
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()

	count := 0
	for _, sess := range e.peers {
		if !sess.isClosed() {
			count++
		}
	}
	return count
}

// Start performs the initial local scan, starts the filesystem watcher, binds the TLS listener,
// and launches the background accept, reconciliation, periodic rescan, and discovery loops.
func (e *Engine) Start(ctx context.Context) error {
	if e.closed.Load() {
		return errors.New("sync: engine is closed")
	}
	if !e.started.CompareAndSwap(false, true) {
		return nil
	}

	// 1. Perform initial ScanAll before accepting or making peer connections so any pre-populated
	// local files are indexed in Store before IndexExchange occurs.
	e.opMu.Lock()
	initialChanged, err := e.scanner.ScanAll()
	e.opMu.Unlock()
	if err != nil {
		return fmt.Errorf("sync: initial scan failed: %w", err)
	}
	e.logger.Info("initial filesystem scan completed",
		"node_id", e.cfg.NodeID,
		"path", e.cfg.Path,
		"changed_entries", len(initialChanged),
	)

	// 2. Start recursive inotify watcher with debounce.
	watcher, err := fs.NewWatcher(e.cfg.Path, e.matcher, e.cfg.DebounceDelay, e.onLocalChanges, e.logger)
	if err != nil {
		return fmt.Errorf("sync: start watcher: %w", err)
	}
	e.watcher = watcher
	e.watcher.Start(e.ctx)

	// 3. Start TLS 1.3 listener on cfg.ListenAddr.
	ln, err := tls.Listen("tcp", e.cfg.ListenAddr, e.tlsConfig)
	if err != nil {
		_ = e.watcher.Close()
		return fmt.Errorf("sync: listen TLS on %q: %w", e.cfg.ListenAddr, err)
	}
	e.listener = ln

	boundAddr, boundPort := resolveBoundAddr(ln.Addr(), e.cfg.ListenAddr)
	e.listenAddrMu.Lock()
	e.listenAddr = boundAddr
	e.listenPort = boundPort
	e.cfg.ListenAddr = boundAddr
	e.listenAddrMu.Unlock()

	e.peersMu.Lock()
	e.selfAddrs[boundAddr] = true
	e.selfAddrs[ln.Addr().String()] = true
	e.peersMu.Unlock()

	// 4. Configure discovery manager with the bound listen address/port.
	discCfg := e.cfg
	discCfg.ListenAddr = boundAddr
	e.discovery = discovery.NewManager(&discCfg, e.onPeerDiscovered, e.logger).
		WithResolver(e.resolver).
		WithPollInterval(e.discoveryInterval)
	if len(e.lanTargets) > 0 {
		e.discovery.WithLANBroadcastTargets(e.lanTargets)
	}

	// 5. Launch background workers.
	e.wg.Add(4)
	go e.reconcileLoop()
	go e.acceptLoop()
	go e.rescanLoop()
	go func() {
		defer e.wg.Done()
		_ = e.discovery.Run(e.ctx)
	}()

	// Bind Engine lifecycle to caller ctx if provided.
	if ctx != nil {
		go func() {
			select {
			case <-ctx.Done():
				_ = e.Close()
			case <-e.ctx.Done():
			}
		}()
	}

	e.logger.Info("sync engine started",
		"node_id", e.cfg.NodeID,
		"listen_addr", boundAddr,
		"socket_listen_addr", ln.Addr().String(),
		"path", e.cfg.Path,
		"state_dir", e.cfg.StateDir,
		"tracked_files", len(e.store.ListFiles()),
		"discovery_lan", e.cfg.DiscoveryLAN,
		"discovery_port", e.cfg.DiscoveryPort,
		"discovery_dns", e.cfg.DiscoveryDNS,
		"static_peers", e.cfg.Peers,
		"exclude", e.cfg.Exclude,
		"conflict_strategy", e.cfg.ConflictStrategy,
		"debounce_delay", e.cfg.DebounceDelay.String(),
		"rescan_interval", e.cfg.RescanInterval.String(),
		"has_secret", e.cfg.Secret != "",
	)
	return nil
}

// Run starts the Engine and blocks until ctx is cancelled or Close is called.
func (e *Engine) Run(ctx context.Context) error {
	if err := e.Start(ctx); err != nil {
		return err
	}
	<-e.ctx.Done()
	return e.Close()
}

// Close gracefully shuts down the Engine, closing the listener, watcher, discovery, and all peer sessions.
func (e *Engine) Close() error {
	var firstErr error
	e.closeOnce.Do(func() {
		e.closed.Store(true)
		e.cancel()

		if e.listener != nil {
			if err := e.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) && firstErr == nil {
				firstErr = err
			}
		}

		if e.watcher != nil {
			if err := e.watcher.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}

		e.peersMu.Lock()
		sessions := make([]*PeerSession, 0, len(e.peers))
		for _, s := range e.peers {
			sessions = append(sessions, s)
		}
		clear(e.peers)
		clear(e.addrToNodeID)
		clear(e.dialing)
		e.peersMu.Unlock()

		for _, s := range sessions {
			_ = s.Close()
		}

		e.wg.Wait()
	})
	return firstErr
}

// TriggerScan runs an immediate full filesystem scan and broadcasts any detected changes to connected peers.
func (e *Engine) TriggerScan() ([]model.FileInfo, error) {
	if e.closed.Load() {
		return nil, errors.New("sync: engine is closed")
	}
	e.opMu.Lock()
	changed, err := e.scanner.ScanAll()
	e.opMu.Unlock()
	if err != nil {
		return nil, err
	}
	if len(changed) > 0 {
		e.broadcastIndexUpdate(changed, "")
	}
	return changed, nil
}

func (e *Engine) onLocalChanges(paths []string) {
	if e.closed.Load() || e.ctx.Err() != nil || len(paths) == 0 {
		return
	}

	e.opMu.Lock()
	if e.closed.Load() || e.ctx.Err() != nil {
		e.opMu.Unlock()
		return
	}
	changed, err := e.scanner.ScanPaths(paths)
	e.opMu.Unlock()

	if err != nil {
		e.logger.Warn("incremental scan failed", "paths", paths, "err", err)
		return
	}
	if len(changed) == 0 {
		e.logger.Debug("filesystem event scanned; no content or metadata changes to sync", "paths", paths)
		return
	}
	for _, fi := range changed {
		e.logger.Info("local file change indexed",
			"path", fi.Path,
			"type", fi.Type.String(),
			"size", fi.Size,
			"deleted", fi.Deleted,
			"blocks", len(fi.Blocks),
			"version", fi.Version,
		)
	}
	e.logger.Debug("broadcasting local filesystem updates", "count", len(changed))
	e.broadcastIndexUpdate(changed, "")
}

func (e *Engine) rescanLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.cfg.RescanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.opMu.Lock()
			if e.ctx.Err() != nil {
				e.opMu.Unlock()
				return
			}
			changed, err := e.scanner.ScanAll()
			e.opMu.Unlock()

			if err != nil {
				e.logger.Warn("periodic rescan failed", "err", err)
				continue
			}
			if len(changed) > 0 {
				for _, fi := range changed {
					e.logger.Info("periodic rescan detected local change",
						"path", fi.Path,
						"type", fi.Type.String(),
						"size", fi.Size,
						"deleted", fi.Deleted,
						"version", fi.Version,
					)
				}
				e.logger.Debug("periodic rescan detected changes", "count", len(changed))
				e.broadcastIndexUpdate(changed, "")
			} else {
				e.logger.Debug("periodic rescan completed with no changes",
					"connected_peers", e.PeerCount(),
					"tracked_files", len(e.store.ListFiles()),
				)
			}
		}
	}
}

func (e *Engine) acceptLoop() {
	defer e.wg.Done()

	for {
		conn, err := e.listener.Accept()
		if err != nil {
			if e.ctx.Err() != nil || e.closed.Load() || errors.Is(err, net.ErrClosed) {
				return
			}
			e.logger.Debug("accept error", "err", err)
			continue
		}

		e.wg.Add(1)
		go func(c net.Conn) {
			defer e.wg.Done()
			e.handleConn(c, false, "")
		}(conn)
	}
}

func (e *Engine) onPeerDiscovered(addr string, source string) {
	addr = strings.TrimSpace(addr)
	if addr == "" || e.closed.Load() || e.ctx.Err() != nil {
		return
	}

	if !e.beginDial(addr) {
		return
	}

	e.logger.Debug("dialing discovered peer", "addr", addr, "source", source)

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer e.endDial(addr)

		dialer := &net.Dialer{Timeout: defaultDialTimeout}
		tlsConn, err := tls.DialWithDialer(dialer, "tcp", addr, e.tlsConfig)
		if err != nil {
			e.logger.Debug("failed to dial peer", "addr", addr, "source", source, "err", err)
			return
		}
		e.handleConn(tlsConn, true, addr)
	}()
}

// DialPeer dials and handshakes with a remote peer address synchronously.
func (e *Engine) DialPeer(ctx context.Context, addr string) error {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return errors.New("sync: peer address must not be empty")
	}
	if !e.beginDial(addr) {
		return nil
	}
	defer e.endDial(addr)

	dialer := &net.Dialer{Timeout: defaultDialTimeout}
	rawConn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	tlsConn := tls.Client(rawConn, e.tlsConfig)

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.handleConn(tlsConn, true, addr)
	}()
	return nil
}

func (e *Engine) beginDial(addr string) bool {
	e.peersMu.Lock()
	defer e.peersMu.Unlock()

	if e.closed.Load() || e.ctx.Err() != nil {
		return false
	}
	if e.selfAddrs[addr] {
		return false
	}
	if e.dialing[addr] {
		return false
	}
	if nodeID, ok := e.addrToNodeID[addr]; ok {
		if sess, active := e.peers[nodeID]; active && !sess.isClosed() {
			return false
		}
	}
	e.dialing[addr] = true
	return true
}

func (e *Engine) endDial(addr string) {
	e.peersMu.Lock()
	delete(e.dialing, addr)
	e.peersMu.Unlock()
}

func (e *Engine) handleConn(rawConn net.Conn, isOutbound bool, dialedAddr string) {
	if e.closed.Load() || e.ctx.Err() != nil {
		_ = rawConn.Close()
		return
	}

	pconn := protocol.NewConn(rawConn)

	e.listenAddrMu.RLock()
	listenPort := e.listenPort
	e.listenAddrMu.RUnlock()

	peerInfo, err := protocol.Handshake(e.ctx, pconn, protocol.HandshakeOptions{
		NodeID:     e.cfg.NodeID,
		Secret:     e.cfg.Secret,
		ListenPort: listenPort,
		Timeout:    protocol.DefaultHandshakeTimeout,
		Logger:     e.logger,
	})
	if err != nil {
		_ = pconn.Close()
		if errors.Is(err, protocol.ErrSelfConnection) && dialedAddr != "" {
			e.peersMu.Lock()
			e.selfAddrs[dialedAddr] = true
			e.peersMu.Unlock()
		} else {
			remoteStr := ""
			if rawConn.RemoteAddr() != nil {
				remoteStr = rawConn.RemoteAddr().String()
			}
			e.logger.Debug("peer handshake failed",
				"remote_addr", remoteStr,
				"dialed_addr", dialedAddr,
				"is_outbound", isOutbound,
				"err", err,
			)
		}
		return
	}

	remoteNodeID := peerInfo.NodeID
	var peerListenAddr string
	if host, _, splitErr := net.SplitHostPort(peerInfo.RemoteAddr); splitErr == nil && peerInfo.ListenPort > 0 {
		peerListenAddr = net.JoinHostPort(host, strconv.Itoa(peerInfo.ListenPort))
	}

	// Deterministic tie-breaker when two peers dial each other simultaneously:
	// The canonical connection is the one initiated by the node with the smaller NodeID:
	// dialerNodeID < acceptorNodeID  <=>  isOutbound == (localNodeID < remoteNodeID)
	isCanonical := isOutbound == (e.cfg.NodeID < remoteNodeID)
	session := newPeerSession(e, pconn, peerInfo, isOutbound, isCanonical, dialedAddr, peerListenAddr)

	var replaced *PeerSession

	e.peersMu.Lock()
	if e.closed.Load() || e.ctx.Err() != nil {
		e.peersMu.Unlock()
		_ = session.Close()
		return
	}

	if existing, ok := e.peers[remoteNodeID]; ok && !existing.isClosed() {
		if !session.isCanonical && existing.isCanonical {
			// Existing session is canonical; drop the new duplicate connection.
			if dialedAddr != "" {
				e.addrToNodeID[dialedAddr] = remoteNodeID
			}
			if peerListenAddr != "" {
				e.addrToNodeID[peerListenAddr] = remoteNodeID
			}
			e.peersMu.Unlock()
			e.logger.Debug("closing non-canonical duplicate peer connection",
				"remote_node_id", remoteNodeID,
				"is_outbound", isOutbound,
			)
			_ = session.Close()
			return
		}
		// New session is canonical (or same direction reconnect); replace existing.
		replaced = existing
	}

	e.peers[remoteNodeID] = session
	if dialedAddr != "" {
		e.addrToNodeID[dialedAddr] = remoteNodeID
	}
	if peerListenAddr != "" {
		e.addrToNodeID[peerListenAddr] = remoteNodeID
	}
	e.peersMu.Unlock()

	if replaced != nil {
		e.logger.Debug("replacing previous peer session with canonical connection",
			"remote_node_id", remoteNodeID,
			"is_outbound", isOutbound,
		)
		_ = replaced.Close()
	}

	e.logger.Info("peer session established",
		"remote_node_id", remoteNodeID,
		"remote_addr", peerInfo.RemoteAddr,
		"is_outbound", isOutbound,
	)

	// Send initial full IndexExchange (excluding any locally ignored paths).
	localFiles := e.localIndexForPeer()
	e.logger.Debug("sending initial IndexExchange to peer",
		"remote_node_id", remoteNodeID,
		"entries", len(localFiles),
	)
	if err := session.conn.SendIndexExchange(localFiles); err != nil {
		e.logger.Debug("failed to send initial IndexExchange",
			"remote_node_id", remoteNodeID,
			"err", err,
		)
		_ = session.Close()
		e.removeSession(session)
		return
	}

	session.readLoop()
}

func (e *Engine) removeSession(s *PeerSession) {
	nodeID := s.NodeID()
	if nodeID == "" {
		return
	}

	e.peersMu.Lock()
	defer e.peersMu.Unlock()

	if cur, ok := e.peers[nodeID]; ok && cur == s {
		delete(e.peers, nodeID)
		for addr, id := range e.addrToNodeID {
			if id == nodeID {
				delete(e.addrToNodeID, addr)
			}
		}
	}
}

func (e *Engine) localIndexForPeer() []model.FileInfo {
	all := e.store.ListFiles()
	if e.matcher == nil {
		return all
	}
	out := make([]model.FileInfo, 0, len(all))
	for _, fi := range all {
		if e.matcher.Match(fi.Path, fi.Type == model.FileTypeDir) {
			continue
		}
		out = append(out, fi)
	}
	return out
}

func (e *Engine) getSession(nodeID string) *PeerSession {
	if nodeID == "" {
		return nil
	}
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()
	sess := e.peers[nodeID]
	if sess != nil && !sess.isClosed() {
		return sess
	}
	return nil
}

func (e *Engine) broadcastIndexUpdate(files []model.FileInfo, excludeNodeID string) {
	if len(files) == 0 {
		return
	}

	filtered := make([]model.FileInfo, 0, len(files))
	for _, fi := range files {
		if e.matcher != nil && e.matcher.Match(fi.Path, fi.Type == model.FileTypeDir) {
			continue
		}
		filtered = append(filtered, fi)
	}
	if len(filtered) == 0 {
		return
	}

	e.peersMu.RLock()
	sessions := make([]*PeerSession, 0, len(e.peers))
	for id, s := range e.peers {
		if id != excludeNodeID && !s.isClosed() {
			sessions = append(sessions, s)
		}
	}
	totalPeers := len(e.peers)
	e.peersMu.RUnlock()

	if len(sessions) == 0 && excludeNodeID == "" && totalPeers == 0 {
		e.logger.Warn("local file change indexed while 0 peers are connected; will sync automatically when a peer connects",
			"changed_files", len(filtered),
		)
		return
	}

	for _, s := range sessions {
		e.logger.Debug("sending IndexUpdate to peer",
			"remote_node_id", s.NodeID(),
			"entries", len(filtered),
		)
		if err := s.conn.SendIndexUpdate(filtered); err != nil {
			e.logger.Debug("failed to send IndexUpdate to peer",
				"remote_node_id", s.NodeID(),
				"err", err,
			)
		}
	}
}

func (e *Engine) enqueueReconcile(session *PeerSession, files []model.FileInfo) {
	if len(files) == 0 || e.closed.Load() || e.ctx.Err() != nil {
		return
	}

	cloned := make([]model.FileInfo, len(files))
	for i, fi := range files {
		cloned[i] = fi.Clone()
	}

	e.reconcileQueueMu.Lock()
	e.reconcilePending = append(e.reconcilePending, reconcileTask{
		session: session,
		files:   cloned,
	})
	e.reconcileQueueMu.Unlock()

	select {
	case e.reconcileNotify <- struct{}{}:
	default:
	}
}

func (e *Engine) reconcileLoop() {
	defer e.wg.Done()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-e.reconcileNotify:
			for {
				if e.ctx.Err() != nil {
					return
				}
				e.reconcileQueueMu.Lock()
				if len(e.reconcilePending) == 0 {
					e.reconcileQueueMu.Unlock()
					break
				}
				tasks := e.reconcilePending
				e.reconcilePending = nil
				e.reconcileQueueMu.Unlock()

				for _, task := range tasks {
					if e.ctx.Err() != nil {
						return
					}
					e.reconcileBatch(task.session, task.files)
				}
			}
		}
	}
}

func (e *Engine) reconcileBatch(session *PeerSession, incoming []model.FileInfo) {
	filtered := make([]model.FileInfo, 0, len(incoming))
	for _, fi := range incoming {
		if strings.TrimSpace(fi.Path) == "" {
			continue
		}
		if e.matcher != nil && e.matcher.Match(fi.Path, fi.Type == model.FileTypeDir) {
			continue
		}
		filtered = append(filtered, fi)
	}
	if len(filtered) == 0 {
		return
	}

	// Order entries safely:
	// 0: Non-deleted directories (parent-first, ascending depth)
	// 1: Non-deleted regular files & symlinks (ascending depth)
	// 2: Deleted regular files & symlinks (child-first, descending depth)
	// 3: Deleted directories (child-first, descending depth)
	sort.SliceStable(filtered, func(i, j int) bool {
		a, b := filtered[i], filtered[j]
		catA, catB := reconcileCategory(a), reconcileCategory(b)
		if catA != catB {
			return catA < catB
		}
		depthA := strings.Count(a.Path, "/")
		depthB := strings.Count(b.Path, "/")
		if catA <= 1 {
			if depthA != depthB {
				return depthA < depthB
			}
			return a.Path < b.Path
		}
		if depthA != depthB {
			return depthA > depthB
		}
		return a.Path > b.Path
	})

	var broadcastOther []model.FileInfo
	var replySender []model.FileInfo

	for _, remoteFI := range filtered {
		if e.ctx.Err() != nil {
			return
		}

		pathMu := e.getPathLock(remoteFI.Path)
		pathMu.Lock()
		e.opMu.Lock()

		otherUpdates, senderUpdates, err := e.reconcileSingleFile(e.ctx, remoteFI, session)

		e.opMu.Unlock()
		pathMu.Unlock()

		if err != nil {
			if e.ctx.Err() == nil {
				e.logger.Warn("failed to reconcile remote file",
					"path", remoteFI.Path,
					"from_peer", session.NodeID(),
					"err", err,
				)
			}
			continue
		}

		if len(otherUpdates) > 0 {
			broadcastOther = append(broadcastOther, otherUpdates...)
		}
		if len(senderUpdates) > 0 {
			replySender = append(replySender, senderUpdates...)
		}
	}

	var senderNodeID string
	if session != nil {
		senderNodeID = session.NodeID()
	}

	if len(broadcastOther) > 0 {
		e.broadcastIndexUpdate(broadcastOther, senderNodeID)
	}
	if len(replySender) > 0 && senderNodeID != "" {
		if activeSess := e.getSession(senderNodeID); activeSess != nil {
			_ = activeSess.conn.SendIndexUpdate(replySender)
		}
	}
}

func reconcileCategory(fi model.FileInfo) int {
	if !fi.Deleted {
		if fi.Type == model.FileTypeDir {
			return 0
		}
		return 1
	}
	if fi.Type != model.FileTypeDir {
		return 2
	}
	return 3
}

func (e *Engine) getPathLock(relPath string) *sync.Mutex {
	val, _ := e.pathLocks.LoadOrStore(relPath, &sync.Mutex{})
	return val.(*sync.Mutex)
}

func (e *Engine) reconcileSingleFile(
	ctx context.Context,
	remoteFI model.FileInfo,
	session *PeerSession,
) (toOtherPeers []model.FileInfo, toSender []model.FileInfo, err error) {
	if e.matcher != nil && e.matcher.Match(remoteFI.Path, remoteFI.Type == model.FileTypeDir) {
		return nil, nil, nil
	}

	// Ensure any unindexed local change on disk for this path is reflected in Store first.
	_, _, _ = e.scanner.ScanPath(remoteFI.Path)

	localFI, exists := e.store.GetFile(remoteFI.Path)

	// Case A: File has never been seen on this node.
	if !exists {
		if remoteFI.Deleted {
			if err := e.store.PutFile(remoteFI); err != nil {
				return nil, nil, err
			}
			return []model.FileInfo{remoteFI}, nil, nil
		}

		if err := e.writer.ApplyRemoteFile(ctx, remoteFI, e.fetchBlockWithFallback(session)); err != nil {
			return nil, nil, err
		}
		e.logger.Info("synced new entry from peer",
			"path", remoteFI.Path,
			"type", remoteFI.Type.String(),
			"size", remoteFI.Size,
			"from_peer", session.NodeID(),
			"version", remoteFI.Version,
		)
		return []model.FileInfo{remoteFI}, nil, nil
	}

	// Case B: File exists in local Store.
	cmp := remoteFI.Version.Compare(localFI.Version)
	switch cmp {
	case model.CmpEqual:
		// Already converged.
		return nil, nil, nil

	case model.CmpDominated:
		// Local version is strictly newer than remote (local > remote).
		// Inform the sender about our newer version.
		return nil, []model.FileInfo{localFI}, nil

	case model.CmpDominates:
		// Remote version is strictly newer than local (remote > local).
		if remoteFI.Deleted && remoteFI.Type == model.FileTypeDir && e.hasActiveChildren(remoteFI.Path) {
			if err := e.store.PutFile(remoteFI); err != nil {
				return nil, nil, err
			}
			return []model.FileInfo{remoteFI}, nil, nil
		}
		if err := e.writer.ApplyRemoteFile(ctx, remoteFI, e.fetchBlockWithFallback(session)); err != nil {
			return nil, nil, err
		}
		e.logger.Info("synced updated entry from peer",
			"path", remoteFI.Path,
			"type", remoteFI.Type.String(),
			"size", remoteFI.Size,
			"deleted", remoteFI.Deleted,
			"from_peer", session.NodeID(),
			"version", remoteFI.Version,
		)
		return []model.FileInfo{remoteFI}, nil, nil

	case model.CmpConcurrent:
		winner, loser, isConflict := model.ResolveConflict(localFI, remoteFI)
		if isConflict {
			e.logger.Warn("conflict detected, resolved via LWW",
				"path", remoteFI.Path,
				"winner_node", winner.ModifiedBy,
				"loser_node", loser.ModifiedBy,
				"winner_mtime_ns", winner.ModTimeNs,
				"loser_mtime_ns", loser.ModTimeNs,
				"strategy", e.cfg.ConflictStrategy,
			)
		}

		remoteWon := !sameFileAttributes(winner, localFI)

		if isConflict && remoteWon && !localFI.Deleted && e.cfg.ConflictStrategy == config.ConflictStrategyKeepCopy {
			conflictRel, copyErr := e.writer.SaveConflictCopy(localFI.Path, localFI.ModifiedBy, localFI.ModTimeNs)
			if copyErr != nil {
				e.logger.Warn("failed to save conflict copy",
					"path", localFI.Path,
					"err", copyErr,
				)
			} else if conflictRel != "" {
				if conflictFI, updated, scanErr := e.scanner.ScanPath(conflictRel); scanErr == nil && updated && conflictFI != nil {
					toOtherPeers = append(toOtherPeers, *conflictFI)
					toSender = append(toSender, *conflictFI)
				}
			}
		}

		if remoteWon {
			if winner.Deleted && localFI.Deleted {
				if err := e.store.PutFile(winner); err != nil {
					return nil, nil, err
				}
			} else if winner.Deleted && winner.Type == model.FileTypeDir && e.hasActiveChildren(winner.Path) {
				if err := e.store.PutFile(winner); err != nil {
					return nil, nil, err
				}
			} else {
				if err := e.writer.ApplyRemoteFile(ctx, winner, e.fetchBlockWithFallback(session)); err != nil {
					return nil, nil, err
				}
			}
		} else {
			// Local version won (or attributes are already identical on disk): persist merged VersionVector.
			if err := e.store.PutFile(winner); err != nil {
				return nil, nil, err
			}
			if isConflict {
				// Send unmerged localFI before merged winner so any peer that has not yet seen
				// our winning branch also detects the concurrent conflict and saves its conflict copy.
				toOtherPeers = append(toOtherPeers, localFI)
				toSender = append(toSender, localFI)
			}
		}

		toOtherPeers = append(toOtherPeers, winner)
		toSender = append(toSender, winner)
		return toOtherPeers, toSender, nil

	default:
		return nil, nil, nil
	}
}

func (e *Engine) hasActiveChildren(dirRelPath string) bool {
	prefix := strings.Trim(dirRelPath, "/") + "/"
	for _, fi := range e.store.ListFiles() {
		if !fi.Deleted && strings.HasPrefix(fi.Path, prefix) {
			return true
		}
	}
	return false
}

func sameFileAttributes(a, b model.FileInfo) bool {
	return a.Deleted == b.Deleted &&
		a.Type == b.Type &&
		a.Size == b.Size &&
		a.Mode == b.Mode &&
		a.Uid == b.Uid &&
		a.Gid == b.Gid &&
		a.ModTimeNs == b.ModTimeNs &&
		a.SymlinkTarget == b.SymlinkTarget &&
		a.ModifiedBy == b.ModifiedBy &&
		a.FileHash == b.FileHash
}

func (e *Engine) fetchBlockWithFallback(preferred *PeerSession) fs.FetchBlockFunc {
	return func(ctx context.Context, hash [32]byte, size int32) ([]byte, error) {
		var lastErr error

		// 1. Try the preferred session that announced the file.
		if preferred != nil && !preferred.isClosed() {
			data, err := preferred.FetchBlock(ctx, hash, size)
			if err == nil {
				return data, nil
			}
			lastErr = err
		}

		// 2. If preferred session was replaced by a canonical session for the same NodeID, try that next.
		if preferred != nil && preferred.NodeID() != "" {
			e.peersMu.RLock()
			replacement := e.peers[preferred.NodeID()]
			e.peersMu.RUnlock()
			if replacement != nil && replacement != preferred && !replacement.isClosed() {
				data, err := replacement.FetchBlock(ctx, hash, size)
				if err == nil {
					return data, nil
				}
				lastErr = err
			}
		}

		// 3. Fallback to any other active peer session.
		e.peersMu.RLock()
		candidates := make([]*PeerSession, 0, len(e.peers))
		for _, s := range e.peers {
			if s != preferred && !s.isClosed() {
				candidates = append(candidates, s)
			}
		}
		e.peersMu.RUnlock()

		for _, candidate := range candidates {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			data, err := candidate.FetchBlock(ctx, hash, size)
			if err == nil {
				return data, nil
			}
			lastErr = err
		}

		if lastErr != nil {
			return nil, lastErr
		}
		return nil, errors.New("sync: no active peers available to fetch block")
	}
}

func (e *Engine) dispatchBlockRequest(session *PeerSession, req protocol.BlockRequest) {
	select {
	case e.blockServeSem <- struct{}{}:
	case <-session.doneCh:
		return
	case <-e.ctx.Done():
		return
	}

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer func() { <-e.blockServeSem }()
		e.handleBlockRequest(session, req)
	}()
}

func (e *Engine) handleBlockRequest(session *PeerSession, req protocol.BlockRequest) {
	if session.isClosed() || e.ctx.Err() != nil {
		return
	}

	if req.Size < 0 || uint32(req.Size) > protocol.DefaultMaxFrameSize {
		_ = session.conn.SendBlockResponse(protocol.BlockResponse{
			RequestID: req.RequestID,
			Hash:      req.Hash,
			Error:     "invalid block size",
		})
		return
	}

	data, ok := e.readBlockFromDisk(req.Hash, req.Size)
	if !ok {
		_ = session.conn.SendBlockResponse(protocol.BlockResponse{
			RequestID: req.RequestID,
			Hash:      req.Hash,
			Error:     "block not found",
		})
		return
	}

	_ = session.conn.SendBlockResponse(protocol.BlockResponse{
		RequestID: req.RequestID,
		Hash:      req.Hash,
		Data:      data,
	})
}

func (e *Engine) readBlockFromDisk(hash [32]byte, size int32) ([]byte, bool) {
	locations, ok := e.store.LookupBlock(hash)
	if !ok {
		return nil, false
	}

	for _, loc := range locations {
		if loc.Size != size {
			continue
		}
		absPath := filepath.Join(e.cfg.Path, filepath.FromSlash(loc.Path))
		f, err := os.Open(absPath)
		if err != nil {
			continue
		}
		buf := make([]byte, size)
		n, readErr := f.ReadAt(buf, loc.Offset)
		_ = f.Close()
		if int32(n) != size || (readErr != nil && !errors.Is(readErr, io.EOF)) {
			continue
		}
		if chunker.HashBytes(buf) == hash {
			return buf, true
		}
	}
	return nil, false
}

func resolveBoundAddr(addr net.Addr, configuredAddr string) (string, int) {
	if tcpAddr, ok := addr.(*net.TCPAddr); ok {
		port := tcpAddr.Port
		host := ""
		if h, _, err := net.SplitHostPort(strings.TrimSpace(configuredAddr)); err == nil {
			host = strings.TrimSpace(h)
		}
		if host == "" || host == "0.0.0.0" || host == "::" || tcpAddr.IP.IsUnspecified() {
			return net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), port
		}
		return net.JoinHostPort(tcpAddr.IP.String(), strconv.Itoa(port)), port
	}
	s := addr.String()
	port := discovery.ExtractListenPort(s, 22001)
	return s, port
}
