package iperf3

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DialFunc opens a connection, like net.Dialer.DialContext.
type DialFunc func(ctx context.Context, network, address string) (net.Conn, error)

// ClientConfig configures a test run by RunClient. Zero values select the defaults.
type ClientConfig struct {
	// Hosts lists the server addresses, as IPs or host names. They are all
	// tried at once and the first one to complete the handshake is used.
	Hosts []string
	// Port is the server port. Zero means DefaultPort.
	Port int
	// Protocol defaults to TCP.
	Protocol Protocol
	// Direction defaults to Upload.
	Direction Direction
	// Parallel is the number of streams per direction, from 1 to MaxStreams.
	Parallel int
	// Duration is the test length. It is rounded up to whole seconds and
	// capped at MaxDuration. Zero means DefaultDuration.
	Duration time.Duration
	// Bandwidth is the target bitrate per direction in bits per second,
	// split evenly across the streams. Zero means unlimited, for UDP too.
	Bandwidth float64
	// Dial opens the control connection and the data streams.
	// Nil means a plain net.Dialer.
	Dial DialFunc
	// OnUpdate receives every state change, and about every 250 ms while
	// the test is running. It is called synchronously and should return quickly.
	OnUpdate func(State)
}

// RunClient runs one test against an iPerf3 server and returns the final state.
// It blocks until the test finishes, fails or ctx is cancelled. Failures are
// reported in State.Error rather than as a Go error.
func RunClient(ctx context.Context, cfg ClientConfig) State {
	c := newClientSession(cfg)
	c.run(ctx)
	return c.snapshot()
}

type clientSession struct {
	params  ClientConfig
	seconds int
	dial    DialFunc
	notify  func(State)

	mu      sync.Mutex
	state   State
	writeMu sync.Mutex
	ctrl    net.Conn
	host    string
	cookie  []byte
	streams []*testStream
	size    int
}

func newClientSession(params ClientConfig) *clientSession {
	if params.Port <= 0 || params.Port > 65535 {
		params.Port = DefaultPort
	}
	if params.Protocol != UDP {
		params.Protocol = TCP
	}
	switch params.Direction {
	case Download, Bidir:
	default:
		params.Direction = Upload
	}
	if params.Parallel < 1 {
		params.Parallel = 1
	}
	if params.Parallel > MaxStreams {
		params.Parallel = MaxStreams
	}
	if params.Duration <= 0 {
		params.Duration = DefaultDuration
	}
	seconds := int((params.Duration + time.Second - 1) / time.Second)
	if seconds > MaxDuration {
		seconds = MaxDuration
	}
	hosts := make([]string, 0, len(params.Hosts))
	seen := map[string]bool{}
	for _, host := range params.Hosts {
		host = strings.Trim(strings.TrimSpace(host), "[]")
		if host != "" && !seen[host] {
			seen[host] = true
			hosts = append(hosts, host)
		}
	}
	params.Hosts = hosts
	dial := params.Dial
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	return &clientSession{
		params:  params,
		seconds: seconds,
		dial:    dial,
		notify:  params.OnUpdate,
		state: State{
			Role:      RoleClient,
			Phase:     PhaseConnecting,
			Protocol:  params.Protocol,
			Direction: params.Direction,
			Parallel:  params.Parallel,
			Duration:  float64(seconds),
		},
	}
}

func (c *clientSession) update(fn func(state *State)) {
	c.mu.Lock()
	fn(&c.state)
	state := c.state
	c.mu.Unlock()
	if c.notify != nil {
		c.notify(state)
	}
}

func (c *clientSession) snapshot() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

func (c *clientSession) fail(ctx context.Context, err error) {
	code := classify(err)
	if ctx.Err() != nil {
		code = ErrorCancelled
	}
	c.update(func(state *State) {
		state.Phase = PhaseDone
		state.Error = code
		state.Upload = 0
		state.Download = 0
	})
}

func classify(err error) string {
	var remote *remoteError
	var netErr net.Error
	switch {
	case err == nil:
		return ""
	case errors.As(err, &remote):
		return remote.Error()
	case errors.Is(err, errBusy):
		return ErrorBusy
	case errors.Is(err, errUDP):
		return ErrorUDP
	case errors.Is(err, errProtocol):
		return ErrorProtocol
	case errors.Is(err, errInterrupted), errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return ErrorInterrupted
	case isConnRefused(err):
		return ErrorRefused
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &netErr) && netErr.Timeout():
		return ErrorTimeout
	}
	return ErrorUnreachable
}

func (c *clientSession) signal(state int8) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return writeState(c.ctrl, state)
}

type handshakeResult struct {
	conn   net.Conn
	host   string
	cookie []byte
	state  int8
	err    error
}

func (c *clientSession) handshake(ctx context.Context, host string) handshakeResult {
	attempt := handshakeResult{host: host, cookie: newCookie()}
	conn, err := c.dial(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(c.params.Port)))
	if err != nil {
		attempt.err = err
		return attempt
	}
	deadline := time.Now().Add(5 * time.Second)
	if value, ok := ctx.Deadline(); ok && value.Before(deadline) {
		deadline = value
	}
	_ = conn.SetDeadline(deadline)
	if _, err = conn.Write(attempt.cookie); err == nil {
		attempt.state, err = readState(conn)
	}
	switch {
	case err != nil:
		attempt.err = err
	case attempt.state == stateAccessDenied:
		attempt.err = errBusy
	case attempt.state == stateServerError:
		var buf [8]byte
		if _, readErr := io.ReadFull(conn, buf[:]); readErr != nil {
			attempt.err = errProtocol
		} else {
			attempt.err = &remoteError{code: int32(binary.BigEndian.Uint32(buf[:4]))}
		}
	case attempt.state != stateParamExchange:
		attempt.err = errProtocol
	}
	if attempt.err != nil {
		_ = conn.Close()
		return attempt
	}
	attempt.conn = conn
	return attempt
}

func errorRank(err error) int {
	var remote *remoteError
	switch {
	case errors.Is(err, errBusy), errors.As(err, &remote):
		return 3
	case errors.Is(err, errProtocol):
		return 2
	case isConnRefused(err):
		return 1
	}
	return 0
}

// connect races the handshake against every host and keeps the first success.
// If all fail, it returns the most informative error, preferring answers from
// a real server (busy, rejected) over a refused or unreachable address.
func (c *clientSession) connect(ctx context.Context) (handshakeResult, error) {
	if len(c.params.Hosts) == 0 {
		return handshakeResult{}, errors.New(ErrorUnreachable)
	}
	dialCtx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	results := make(chan handshakeResult, len(c.params.Hosts))
	for _, host := range c.params.Hosts {
		host := host
		go func() {
			results <- c.handshake(dialCtx, host)
		}()
	}
	var failure error
	for i := range c.params.Hosts {
		result := <-results
		if result.err == nil {
			rest := len(c.params.Hosts) - i - 1
			go func() {
				for j := 0; j < rest; j++ {
					if late := <-results; late.conn != nil {
						_ = late.conn.Close()
					}
				}
			}()
			return result, nil
		}
		if failure == nil || errorRank(result.err) > errorRank(failure) {
			failure = result.err
		}
	}
	return handshakeResult{}, failure
}

func (c *clientSession) buildParams() testParams {
	p := c.params
	params := testParams{
		Time:          float64(c.seconds),
		Parallel:      p.Parallel,
		Reverse:       p.Direction == Download,
		Bidirectional: p.Direction == Bidir,
		Len:           float64(c.size),
		PacingTimer:   1000,
		ClientVersion: protocolVersion,
	}
	if p.Protocol == UDP {
		params.UDP = true
	} else {
		params.TCP = true
	}
	if p.Bandwidth > 0 {
		params.Bandwidth = float64(int64(p.Bandwidth / float64(p.Parallel)))
	}
	return params
}

func (c *clientSession) createStream(ctx context.Context, index int, sender bool) error {
	udp := c.params.Protocol == UDP
	address := net.JoinHostPort(c.host, strconv.Itoa(c.params.Port))
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	network := "tcp"
	if udp {
		network = "udp"
	}
	conn, err := c.dial(dialCtx, network, address)
	if err != nil {
		return err
	}
	stream := newTestStream(streamID(index), sender, udp, false)
	stream.conn = conn
	stream.write = conn.Write
	if udp {
		stream.writer = newUDPWriter(conn, netip.AddrPort{})
	}
	c.mu.Lock()
	c.streams = append(c.streams, stream)
	c.mu.Unlock()
	if !udp {
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, err = conn.Write(c.cookie)
		_ = conn.SetWriteDeadline(time.Time{})
		return err
	}
	if raw, ok := conn.(*net.UDPConn); ok {
		_ = raw.SetReadBuffer(4 << 20)
		_ = raw.SetWriteBuffer(4 << 20)
	}
	hello := make([]byte, 4)
	binary.LittleEndian.PutUint32(hello, udpConnectMsg)
	reply := make([]byte, 64)
	for attempt := 0; attempt < 3; attempt++ {
		if _, err := conn.Write(hello); err != nil {
			return errUDP
		}
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		n, err := conn.Read(reply)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}
		if n == 4 {
			le := binary.LittleEndian.Uint32(reply)
			be := binary.BigEndian.Uint32(reply)
			if le == udpConnectReply || be == udpConnectReply || le == udpLegacyReply || be == udpLegacyReply {
				_ = conn.SetReadDeadline(time.Time{})
				return nil
			}
		}
	}
	return errUDP
}

func (c *clientSession) closeStreams() {
	c.mu.Lock()
	streams := append([]*testStream(nil), c.streams...)
	c.mu.Unlock()
	for _, stream := range streams {
		if stream.conn != nil {
			_ = stream.conn.Close()
		}
	}
}

func (c *clientSession) run(ctx context.Context) {
	attempt, err := c.connect(ctx)
	if err != nil {
		c.fail(ctx, err)
		return
	}
	ctrl, host := attempt.conn, attempt.host
	c.ctrl = ctrl
	c.host = host
	c.cookie = attempt.cookie
	defer ctrl.Close()
	defer c.closeStreams()
	c.size = tcpBlockSize
	if c.params.Protocol == UDP {
		c.size = udpSize(ctrl)
	}
	c.update(func(state *State) {
		state.Peer = host
	})

	finished := make(chan struct{})
	defer close(finished)
	go func() {
		select {
		case <-ctx.Done():
			_ = ctrl.SetWriteDeadline(time.Now().Add(time.Second))
			_ = c.signal(stateClientTerminate)
			_ = ctrl.SetDeadline(time.Now())
			c.closeStreams()
		case <-finished:
		}
	}()

	_ = ctrl.SetDeadline(time.Now().Add(10 * time.Second))

	var (
		runCtx  context.Context
		stop    context.CancelFunc = func() {}
		workers sync.WaitGroup
		start   time.Time
		end     time.Time
		peer    testResults
	)
	defer func() {
		stop()
		c.closeStreams()
		workers.Wait()
	}()
	pending := true
	for {
		state := attempt.state
		if !pending {
			state, err = readState(ctrl)
			if err != nil {
				c.fail(ctx, err)
				return
			}
		}
		pending = false
		switch state {
		case stateParamExchange:
			c.writeMu.Lock()
			err = writeJSON(ctrl, c.buildParams())
			c.writeMu.Unlock()
		case stateCreateStreams:
			err = c.createStreams(ctx)
		case stateTestStart:
		case stateTestRunning:
			start = time.Now()
			runCtx, stop = context.WithCancel(ctx)
			c.startWorkers(runCtx, &workers, start)
			duration := time.Duration(c.seconds) * time.Second
			_ = ctrl.SetDeadline(start.Add(duration + 30*time.Second))
			endTimer := time.AfterFunc(duration, func() {
				for _, stream := range c.streams {
					stream.counting.Store(false)
					stream.measureTCP()
				}
				c.mu.Lock()
				end = time.Now()
				c.mu.Unlock()
				stop()
				_ = c.signal(stateTestEnd)
			})
			defer endTimer.Stop()
		case stateExchangeResults:
			_ = ctrl.SetDeadline(time.Now().Add(10 * time.Second))
			c.mu.Lock()
			finish := end
			c.mu.Unlock()
			if finish.IsZero() {
				finish = time.Now()
			}
			elapsed := finish.Sub(start).Seconds()
			c.writeMu.Lock()
			err = writeJSON(ctrl, resultsOf(c.streams, elapsed, c.params.Protocol == UDP))
			c.writeMu.Unlock()
			if err == nil {
				err = readJSON(ctrl, &peer)
			}
			c.closeStreams()
		case stateDisplayResults:
			_ = c.signal(stateDone)
			c.mu.Lock()
			finish := end
			c.mu.Unlock()
			if finish.IsZero() {
				finish = time.Now()
			}
			elapsed := finish.Sub(start).Seconds()
			remote := peerResults(peer)
			peerRetrans := peer.SenderHasRetransmits == 1
			c.update(func(state *State) {
				state.Phase = PhaseDone
				state.Elapsed = elapsed
				state.Upload = 0
				state.Download = 0
				state.UploadResult = buildFlow(c.streams, true, remote, peerRetrans, elapsed)
				state.DownloadResult = buildFlow(c.streams, false, remote, peerRetrans, elapsed)
			})
			return
		case stateAccessDenied:
			err = errBusy
		case stateServerError:
			var buf [8]byte
			if _, readErr := io.ReadFull(ctrl, buf[:]); readErr != nil {
				err = errProtocol
			} else {
				err = &remoteError{code: int32(binary.BigEndian.Uint32(buf[:4]))}
			}
		case stateServerTerminate:
			err = errInterrupted
		default:
			err = errProtocol
		}
		if err != nil {
			c.fail(ctx, err)
			return
		}
	}
}

func (c *clientSession) createStreams(ctx context.Context) error {
	senders, receivers := 0, 0
	switch c.params.Direction {
	case Upload:
		senders = c.params.Parallel
	case Download:
		receivers = c.params.Parallel
	case Bidir:
		senders = c.params.Parallel
		receivers = c.params.Parallel
	}
	for i := 0; i < senders+receivers; i++ {
		if err := c.createStream(ctx, i, i < senders); err != nil {
			return err
		}
	}
	return nil
}

func (c *clientSession) startWorkers(ctx context.Context, workers *sync.WaitGroup, start time.Time) {
	rate := 0.0
	if c.params.Bandwidth > 0 {
		rate = c.params.Bandwidth / float64(c.params.Parallel)
	}
	for _, stream := range c.streams {
		stream := stream
		workers.Add(1)
		go func() {
			defer workers.Done()
			if stream.sender {
				stream.sendLoop(ctx, c.size, rate, nil)
			} else {
				stream.receiveLoop(c.size)
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		ticker := time.NewTicker(sampleInterval)
		defer ticker.Stop()
		lastAt := start
		lastTx, lastRx := int64(0), int64(0)
		c.update(func(state *State) {
			state.Phase = PhaseRunning
		})
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				for _, stream := range c.streams {
					stream.measureTCP()
				}
				tx := totalBytes(c.streams, true)
				rx := totalBytes(c.streams, false)
				seconds := now.Sub(lastAt).Seconds()
				if seconds <= 0 {
					continue
				}
				upload := float64(tx-lastTx) * 8 / seconds
				download := float64(rx-lastRx) * 8 / seconds
				lastAt, lastTx, lastRx = now, tx, rx
				c.update(func(state *State) {
					state.Elapsed = now.Sub(start).Seconds()
					state.Upload = upload
					state.Download = download
				})
			}
		}
	}()
}
