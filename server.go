package iperf3

import (
	"bytes"
	"context"
	"encoding/binary"

	"errors"
	"io"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ServerConfig configures a server started by Listen.
type ServerConfig struct {
	// Addr is the listen address, such as ":5201" or "127.0.0.1:0".
	// Empty means all interfaces on DefaultPort. TCP and UDP share the port.
	Addr string
	// Control, if set, is applied to the TCP and UDP listening sockets,
	// for example to bind them to an interface or exclude them from a VPN.
	// An error on the TCP socket makes Listen fail; an error on the UDP
	// socket leaves the server TCP-only.
	Control func(network, address string, c syscall.RawConn) error
	// OnUpdate receives the progress and result of every test. It is called
	// synchronously and should return quickly.
	OnUpdate func(State)
}

// Server is a running iPerf3 server. Like official iperf3 it runs one test
// at a time and answers other clients with the standard busy reply.
type Server struct {
	listener net.Listener
	udp      *net.UDPConn
	ctx      context.Context
	cancel   context.CancelFunc
	notify   func(State)
	active   atomic.Pointer[serverSession]
	wg       sync.WaitGroup

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	serial int
	closed bool
}

// Listen starts an iPerf3 server. It returns once the sockets are open;
// tests are served in the background until Close is called.
func Listen(cfg ServerConfig) (*Server, error) {
	addr := cfg.Addr
	if addr == "" {
		addr = ":" + strconv.Itoa(DefaultPort)
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	listener, err := listenConfig(cfg.Control).Listen(context.Background(), "tcp", addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	server := &Server{
		listener: listener,
		ctx:      ctx,
		cancel:   cancel,
		notify:   cfg.OnUpdate,
		conns:    map[net.Conn]struct{}{},
	}
	actual := listener.Addr().(*net.TCPAddr).Port
	if pc, err := udpListenConfig(cfg.Control).ListenPacket(ctx, "udp", net.JoinHostPort(host, strconv.Itoa(actual))); err == nil {
		if udp, ok := pc.(*net.UDPConn); ok {
			_ = udp.SetReadBuffer(4 << 20)
			_ = udp.SetWriteBuffer(4 << 20)
			server.udp = udp
		} else {
			_ = pc.Close()
		}
	}
	server.wg.Add(1)
	go server.acceptLoop()
	if server.udp != nil {
		server.wg.Add(1)
		go server.udpLoop()
	}
	return server, nil
}

// Port returns the TCP port the server listens on, which is useful when Addr used port 0.
func (s *Server) Port() int {
	return s.listener.Addr().(*net.TCPAddr).Port
}

// UDP reports whether UDP tests are available on the server.
func (s *Server) UDP() bool {
	return s.udp != nil
}

// Close stops the server and any running test, and waits until all of its
// goroutines have exited. It is safe to call more than once.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	conns := make([]net.Conn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	s.mu.Unlock()
	s.cancel()
	_ = s.listener.Close()
	if s.udp != nil {
		_ = s.udp.Close()
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
	s.wg.Wait()
	return nil
}

func (s *Server) track(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.conns[conn] = struct{}{}
	return true
}

func (s *Server) drop(conn net.Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
	_ = conn.Close()
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if !s.track(conn) {
			_ = conn.Close()
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(conn)
		}()
	}
}

func (s *Server) handleConn(conn net.Conn) {
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	cookie := make([]byte, cookieSize)
	if _, err := io.ReadFull(conn, cookie); err != nil {
		s.drop(conn)
		return
	}
	_ = conn.SetReadDeadline(time.Time{})
	s.mu.Lock()
	session := s.active.Load()
	if session == nil && !s.closed {
		s.serial++
		session = newServerSession(s, conn, cookie, s.serial)
		s.active.Store(session)
		s.mu.Unlock()
		session.run()
		s.active.CompareAndSwap(session, nil)
		s.drop(conn)
		return
	}
	s.mu.Unlock()
	if session != nil && bytes.Equal(cookie, session.cookie) && session.acceptTCP(conn) {
		return
	}
	_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
	_ = writeState(conn, stateAccessDenied)
	s.drop(conn)
}

func (s *Server) udpLoop() {
	defer s.wg.Done()
	reader := newUDPReader(s.udp, maxUDPBlockSize+1)
	var (
		lastSession *serverSession
		lastPeers   *[]udpPeer
		lastAddr    netip.AddrPort
		lastStream  *testStream
	)
	handle := func(packet []byte, addr netip.AddrPort, now int64) {
		session := s.active.Load()
		if session == nil {
			return
		}
		if len(packet) == 4 {
			session.handleConnect(addr)
			lastPeers = nil
			return
		}
		peers := session.peers.Load()
		if session != lastSession || peers != lastPeers || addr != lastAddr {
			lastSession, lastPeers, lastAddr = session, peers, addr
			lastStream = findPeer(peers, addr)
		}
		if lastStream != nil && !lastStream.sender {
			lastStream.receiveUDP(packet, now)
		}
	}
	for {
		if err := reader.read(handle); err != nil {
			if s.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
		}
	}
}

type udpPeer struct {
	addr   netip.AddrPort
	stream *testStream
}

func findPeer(peers *[]udpPeer, addr netip.AddrPort) *testStream {
	if peers == nil {
		return nil
	}
	for _, peer := range *peers {
		if peer.addr == addr {
			return peer.stream
		}
	}
	return nil
}

type serverSession struct {
	server *Server
	ctrl   net.Conn
	cookie []byte
	serial int
	params testParams
	size   int

	mu        sync.Mutex
	accepting bool
	expected  int
	receivers int
	streams   []*testStream
	peers     atomic.Pointer[[]udpPeer]
	ready     chan struct{}
	state     State
}

func newServerSession(server *Server, ctrl net.Conn, cookie []byte, serial int) *serverSession {
	return &serverSession{
		server: server,
		ctrl:   ctrl,
		cookie: cookie,
		serial: serial,
		ready:  make(chan struct{}),
		state: State{
			Role:    RoleServer,
			Session: serial,
			Phase:   PhaseConnecting,
			Peer:    hostOf(ctrl.RemoteAddr()),
		},
	}
}

func (x *serverSession) update(fn func(state *State)) {
	x.mu.Lock()
	fn(&x.state)
	state := x.state
	x.mu.Unlock()
	if x.server.notify != nil {
		x.server.notify(state)
	}
}

func (x *serverSession) fail(code string) {
	x.update(func(state *State) {
		state.Phase = PhaseDone
		state.Error = code
		state.Upload = 0
		state.Download = 0
	})
}

func (x *serverSession) validate() int32 {
	p := &x.params
	switch {
	case p.SCTP:
		return codeNoSCTP
	case p.UDP && x.server.udp == nil:
		return codeStreamListen
	case p.Reverse && p.Bidirectional:
		return codeReverseBidir
	case p.Parallel > MaxStreams:
		return codeNumStreams
	case p.Time < 0 || p.Time > MaxDuration:
		return codeDuration
	case p.Omit < 0 || p.Omit > 600:
		return codeOmit
	}
	if p.Parallel < 1 {
		p.Parallel = 1
	}
	if !p.UDP {
		p.TCP = true
	}
	x.size = int(p.Len)
	if p.UDP {
		header := 12
		if p.UDPCounters64 != 0 {
			header = 16
		}
		if x.size == 0 {
			x.size = udpBlockSize
		}
		if x.size < header || x.size > maxUDPBlockSize {
			return codeUDPBlockSize
		}
		return 0
	}
	if x.size == 0 {
		x.size = tcpBlockSize
	}
	if x.size > maxTCPBlockSize {
		return codeBlockSize
	}
	return 0
}

func (x *serverSession) prepare() {
	p := x.params
	switch {
	case p.Bidirectional:
		x.receivers = p.Parallel
		x.expected = p.Parallel * 2
	case p.Reverse:
		x.expected = p.Parallel
	default:
		x.receivers = p.Parallel
		x.expected = p.Parallel
	}
}

func (x *serverSession) newStream(prepare func(stream *testStream)) *testStream {
	index := len(x.streams)
	stream := newTestStream(streamID(index), index >= x.receivers, x.params.UDP, x.params.UDPCounters64 != 0)
	prepare(stream)
	x.streams = append(x.streams, stream)
	if len(x.streams) == x.expected {
		x.accepting = false
		close(x.ready)
	}
	return stream
}

func (x *serverSession) acceptTCP(conn net.Conn) bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	if !x.accepting || x.params.UDP {
		return false
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(x.params.NoDelay)
		if window := int(x.params.Window); window > 0 {
			_ = tcp.SetReadBuffer(window)
			_ = tcp.SetWriteBuffer(window)
		}
	}
	x.newStream(func(stream *testStream) {
		stream.conn = conn
		stream.write = conn.Write
	})
	return true
}

func connectReply() []byte {
	reply := make([]byte, 4)
	binary.LittleEndian.PutUint32(reply, udpConnectReply)
	return reply
}

// handleConnect registers a UDP stream when a client sends its 4-byte hello
// to the shared server port, and answers it. Repeated hellos are answered again
// so a lost reply does not break the setup.
func (x *serverSession) handleConnect(addr netip.AddrPort) {
	x.mu.Lock()
	stream := findPeer(x.peers.Load(), addr)
	if stream == nil && x.accepting && x.params.UDP {
		stream = x.newStream(func(stream *testStream) {
			if stream.sender {
				if sender, err := dialUDPSender(x.server.Port(), addr); err == nil {
					stream.conn = sender
					stream.writer = newUDPWriter(sender, netip.AddrPort{})
					go answerConnect(sender)
					return
				}
			}
			stream.writer = newUDPWriter(x.server.udp, addr)
		})
		var peers []udpPeer
		if current := x.peers.Load(); current != nil {
			peers = append(peers, *current...)
		}
		peers = append(peers, udpPeer{addr: addr, stream: stream})
		x.peers.Store(&peers)
	}
	x.mu.Unlock()
	if stream != nil {
		_, _ = x.server.udp.WriteToUDPAddrPort(connectReply(), addr)
	}
}

func answerConnect(conn *net.UDPConn) {
	buf := make([]byte, 64)
	reply := connectReply()
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		if n == 4 {
			_, _ = conn.Write(reply)
		}
	}
}

func (x *serverSession) run() {
	ctx, cancel := context.WithCancel(x.server.ctx)
	defer cancel()
	go func() {
		<-ctx.Done()
		_ = x.ctrl.SetDeadline(time.Now())
	}()
	defer x.cleanup()

	_ = x.ctrl.SetDeadline(time.Now().Add(10 * time.Second))
	if err := writeState(x.ctrl, stateParamExchange); err != nil {
		x.fail(ErrorInterrupted)
		return
	}
	if err := readJSON(x.ctrl, &x.params); err != nil {
		x.fail(ErrorProtocol)
		return
	}
	if code := x.validate(); code != 0 {
		writeServerError(x.ctrl, code)
		x.fail((&remoteError{code: code}).Error())
		return
	}
	x.prepare()
	x.update(func(state *State) {
		state.Protocol = x.params.protocol()
		state.Direction = x.params.direction()
		state.Parallel = x.params.Parallel
		state.Duration = x.params.Omit + x.params.Time
	})
	x.mu.Lock()
	x.accepting = true
	x.mu.Unlock()
	if err := writeState(x.ctrl, stateCreateStreams); err != nil {
		x.fail(ErrorInterrupted)
		return
	}
	timer := time.NewTimer(10 * time.Second)
	select {
	case <-x.ready:
		timer.Stop()
	case <-timer.C:
		writeServerError(x.ctrl, codeStreamAccept)
		x.fail(ErrorTimeout)
		return
	case <-ctx.Done():
		timer.Stop()
		x.fail(ErrorCancelled)
		return
	}
	if writeState(x.ctrl, stateTestStart) != nil || writeState(x.ctrl, stateTestRunning) != nil {
		x.fail(ErrorInterrupted)
		return
	}

	start := time.Now()
	runCtx, stop := context.WithCancel(ctx)
	var workers sync.WaitGroup
	budget := newByteBudget(x.params)
	rate := x.params.Bandwidth
	for _, stream := range x.streams {
		stream := stream
		switch {
		case stream.sender:
			workers.Add(1)
			go func() {
				defer workers.Done()
				stream.sendLoop(runCtx, x.size, rate, budget)
			}()
		case !stream.udp:
			workers.Add(1)
			go func() {
				defer workers.Done()
				stream.receiveLoop(x.size)
			}()
		}
	}
	omitAt := start
	if x.params.Omit > 0 {
		omitAt = start.Add(time.Duration(x.params.Omit * float64(time.Second)))
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		x.sample(runCtx, start, omitAt)
	}()

	limit := x.params.Omit + x.params.Time
	if limit <= 0 {
		limit = 3600
	}
	_ = x.ctrl.SetDeadline(time.Now().Add(time.Duration((limit + 60) * float64(time.Second))))
	ended := false
	for !ended {
		state, err := readState(x.ctrl)
		if err != nil {
			stop()
			x.closeStreams()
			workers.Wait()
			if ctx.Err() != nil {
				x.fail(ErrorCancelled)
			} else {
				x.fail(ErrorInterrupted)
			}
			return
		}
		switch state {
		case stateTestEnd:
			ended = true
		case stateClientTerminate:
			stop()
			x.closeStreams()
			workers.Wait()
			x.fail(ErrorInterrupted)
			return
		}
	}
	end := time.Now()
	for _, stream := range x.streams {
		stream.counting.Store(false)
		stream.measureTCP()
	}
	stop()
	x.closeStreams()
	workers.Wait()

	elapsed := end.Sub(omitAt).Seconds()
	if elapsed <= 0 {
		elapsed = end.Sub(start).Seconds()
	}
	_ = x.ctrl.SetDeadline(time.Now().Add(10 * time.Second))
	if err := writeState(x.ctrl, stateExchangeResults); err != nil {
		x.fail(ErrorInterrupted)
		return
	}
	var peer testResults
	if err := readJSON(x.ctrl, &peer); err != nil {
		x.fail(ErrorProtocol)
		return
	}
	results := resultsOf(x.streams, elapsed, x.params.UDP)
	if x.params.GetServerOutput != 0 {
		results.ServerOutputText = serverOutput(x.streams, elapsed)
	}
	if err := writeJSON(x.ctrl, results); err != nil {
		x.fail(ErrorInterrupted)
		return
	}
	_ = writeState(x.ctrl, stateDisplayResults)
	_ = x.ctrl.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _ = readState(x.ctrl)

	remote := peerResults(peer)
	peerRetrans := peer.SenderHasRetransmits == 1
	x.update(func(state *State) {
		state.Phase = PhaseDone
		state.Elapsed = end.Sub(start).Seconds()
		state.Upload = 0
		state.Download = 0
		state.UploadResult = buildFlow(x.streams, false, remote, peerRetrans, elapsed)
		state.DownloadResult = buildFlow(x.streams, true, remote, peerRetrans, elapsed)
	})
}

func (x *serverSession) sample(ctx context.Context, start, omitAt time.Time) {
	ticker := time.NewTicker(sampleInterval)
	defer ticker.Stop()
	omitted := !omitAt.After(start)
	lastAt := start
	lastRx, lastTx := int64(0), int64(0)
	x.update(func(state *State) {
		state.Phase = PhaseRunning
	})
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if !omitted && !now.Before(omitAt) {
				omitted = true
				for _, stream := range x.streams {
					stream.markOmit()
				}
			}
			for _, stream := range x.streams {
				stream.measureTCP()
			}
			rx := totalBytes(x.streams, false)
			tx := totalBytes(x.streams, true)
			seconds := now.Sub(lastAt).Seconds()
			if seconds <= 0 {
				continue
			}
			upload := float64(rx-lastRx) * 8 / seconds
			download := float64(tx-lastTx) * 8 / seconds
			lastAt, lastRx, lastTx = now, rx, tx
			x.update(func(state *State) {
				state.Elapsed = now.Sub(start).Seconds()
				state.Upload = upload
				state.Download = download
			})
		}
	}
}

func (x *serverSession) closeStreams() {
	x.mu.Lock()
	streams := append([]*testStream(nil), x.streams...)
	x.peers.Store(nil)
	x.mu.Unlock()
	for _, stream := range streams {
		if stream.conn != nil {
			x.server.drop(stream.conn)
		}
	}
}

func (x *serverSession) cleanup() {
	x.mu.Lock()
	x.accepting = false
	x.mu.Unlock()
	x.closeStreams()
}
