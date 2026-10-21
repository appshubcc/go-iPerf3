package iperf3

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"math"
	"net"
	"net/netip"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type testStream struct {
	id        int
	sender    bool
	udp       bool
	counter64 bool
	conn      net.Conn
	write     func([]byte) (int, error)
	writer    *udpWriter

	bytes       atomic.Int64
	counting    atomic.Bool
	retrans     atomic.Int64
	packets     atomic.Int64
	errors      atomic.Int64
	jitter      atomic.Uint64
	omitBytes   atomic.Int64
	omitPackets atomic.Int64
	omitErrors  atomic.Int64

	rttMu    sync.Mutex
	rttMin   int64
	rttMax   int64
	rttSum   int64
	rttCount int64

	transit int64
	seen    bool
	state   float64
}

func newTestStream(id int, sender, udp, counter64 bool) *testStream {
	stream := &testStream{id: id, sender: sender, udp: udp, counter64: counter64}
	stream.retrans.Store(-1)
	stream.counting.Store(true)
	return stream
}

func (s *testStream) add(n int) {
	if n > 0 && s.counting.Load() {
		s.bytes.Add(int64(n))
	}
}

func (s *testStream) receiveUDP(packet []byte, now int64) {
	if !s.counting.Load() {
		return
	}
	s.bytes.Add(int64(len(packet)))
	if len(packet) < 12 {
		return
	}
	sent := int64(binary.BigEndian.Uint32(packet[0:4]))*1e9 + int64(binary.BigEndian.Uint32(packet[4:8]))*1e3
	var seq int64
	if s.counter64 && len(packet) >= 16 {
		seq = int64(binary.BigEndian.Uint64(packet[8:16]))
	} else {
		seq = int64(binary.BigEndian.Uint32(packet[8:12]))
	}
	// Sequence gaps count as loss; a late datagram that fills a gap takes one back.
	packets := s.packets.Load()
	if seq >= packets+1 {
		if seq > packets+1 {
			s.errors.Add(seq - 1 - packets)
		}
		s.packets.Store(seq)
	} else if s.errors.Load() > 0 {
		s.errors.Add(-1)
	}
	// RFC 1889 jitter, the same estimator iperf3 uses.
	transit := now - sent
	if !s.seen {
		s.transit = transit
		s.seen = true
	}
	delta := float64(transit-s.transit) / 1e9
	if delta < 0 {
		delta = -delta
	}
	s.transit = transit
	s.state += (delta - s.state) / 16
	s.jitter.Store(math.Float64bits(s.state))
}

func (s *testStream) markOmit() {
	s.omitBytes.Store(s.bytes.Load())
	s.omitPackets.Store(s.packets.Load())
	s.omitErrors.Store(s.errors.Load())
}

func (s *testStream) measureTCP() {
	if !s.sender || s.udp || s.conn == nil {
		return
	}
	retrans, rtt, ok := tcpInfo(s.conn)
	if !ok {
		return
	}
	s.retrans.Store(retrans)
	s.rttMu.Lock()
	if s.rttCount == 0 || rtt < s.rttMin {
		s.rttMin = rtt
	}
	if rtt > s.rttMax {
		s.rttMax = rtt
	}
	s.rttSum += rtt
	s.rttCount++
	s.rttMu.Unlock()
}

func (s *testStream) result(elapsed float64) streamResult {
	result := streamResult{
		ID:             s.id,
		Bytes:          float64(s.bytes.Load() - s.omitBytes.Load()),
		Retransmits:    -1,
		Packets:        s.packets.Load(),
		OmittedPackets: s.omitPackets.Load(),
		EndTime:        elapsed,
	}
	if s.sender {
		if retrans := s.retrans.Load(); retrans >= 0 {
			result.Retransmits = retrans
		}
		s.rttMu.Lock()
		if s.rttCount > 0 {
			result.MaxRTT = s.rttMax
			result.MinRTT = s.rttMin
			result.MeanRTT = s.rttSum / s.rttCount
		}
		s.rttMu.Unlock()
		return result
	}
	result.Jitter = math.Float64frombits(s.jitter.Load())
	result.Errors = s.errors.Load()
	result.OmittedErrors = s.omitErrors.Load()
	return result
}

func (s *testStream) sendLoop(ctx context.Context, size int, rate float64, budget *byteBudget) {
	if s.udp {
		s.sendLoopUDP(ctx, size, rate, budget)
		return
	}
	buf := make([]byte, size)
	_, _ = rand.Read(buf)
	var sent int64
	start := time.Now()
	for ctx.Err() == nil {
		if rate > 0 && paceBatch(ctx, start, sent, size, rate, 1) <= 0 {
			return
		}
		if budget != nil && !budget.take(size) {
			return
		}
		n, err := s.write(buf)
		sent += int64(n)
		s.add(n)
		if err != nil {
			return
		}
	}
}

type udpWriter struct {
	conn net.Conn
	udp  *net.UDPConn
	addr netip.AddrPort
}

func newUDPWriter(conn net.Conn, addr netip.AddrPort) *udpWriter {
	udp, _ := conn.(*net.UDPConn)
	return &udpWriter{conn: conn, udp: udp, addr: addr}
}

func (w *udpWriter) write(bufs [][]byte, stamp func(int)) (int, error) {
	for i, buf := range bufs {
		stamp(i)
		var err error
		if w.udp != nil && w.addr.IsValid() {
			_, err = w.udp.WriteToUDPAddrPort(buf, w.addr)
		} else {
			_, err = w.conn.Write(buf)
		}
		if err != nil {
			return i, err
		}
	}
	return len(bufs), nil
}

func (s *testStream) sendLoopUDP(ctx context.Context, size int, rate float64, budget *byteBudget) {
	bufs := make([][]byte, udpBatchSize)
	for i := range bufs {
		bufs[i] = make([]byte, size)
		_, _ = rand.Read(bufs[i])
	}
	var sent, seq int64
	stamp := func(i int) {
		now := time.Now()
		binary.BigEndian.PutUint32(bufs[i][0:], uint32(now.Unix()))
		binary.BigEndian.PutUint32(bufs[i][4:], uint32(now.Nanosecond()/1000))
		if s.counter64 {
			binary.BigEndian.PutUint64(bufs[i][8:], uint64(seq+int64(i)+1))
		} else {
			binary.BigEndian.PutUint32(bufs[i][8:], uint32(seq+int64(i)+1))
		}
	}
	busy := 0
	start := time.Now()
	for ctx.Err() == nil {
		batch := udpBatchSize
		if rate > 0 {
			if batch = paceBatch(ctx, start, sent, size, rate, batch); batch <= 0 {
				return
			}
		}
		if budget != nil {
			if batch = budget.takeBatch(size, batch); batch <= 0 {
				return
			}
		}
		n, err := s.writer.write(bufs[:batch], stamp)
		if n > 0 {
			seq += int64(n)
			sent += int64(n * size)
			s.packets.Store(seq)
			s.add(n * size)
		}
		if budget != nil && n < batch {
			budget.refund(size, batch-n)
		}
		if err == nil {
			busy = 0
			continue
		}
		if !isSendBusy(err) {
			return
		}
		busy++
		if busy > 32 {
			time.Sleep(50 * time.Microsecond)
		} else {
			runtime.Gosched()
		}
	}
}

func (s *testStream) receiveLoop(size int) {
	if !s.udp {
		buf := make([]byte, size)
		for {
			n, err := s.conn.Read(buf)
			s.add(n)
			if err != nil {
				return
			}
		}
	}
	reader := newUDPReader(s.conn, size)
	handle := func(packet []byte, _ netip.AddrPort, now int64) {
		s.receiveUDP(packet, now)
	}
	for {
		if err := reader.read(handle); err != nil {
			return
		}
	}
}

// paceBatch waits until the stream may send more data to stay at rate bits per
// second, then returns how many blocks of size bytes to send, at most limit.
// It allows a burst of paceWindow seconds so short stalls are made up for.
func paceBatch(ctx context.Context, start time.Time, sent int64, size int, rate float64, limit int) int {
	perSecond := rate / 8
	burst := perSecond * paceWindow
	if burst < float64(size) {
		burst = float64(size)
	}
	target := burst
	if most := float64(size * limit); target > most {
		target = most
	}
	for {
		if ctx.Err() != nil {
			return 0
		}
		credit := time.Since(start).Seconds()*perSecond + burst - float64(sent)
		if n := int(credit / float64(size)); n >= 1 {
			if n > limit {
				n = limit
			}
			return n
		}
		wait := time.Duration((target - credit) / perSecond * float64(time.Second))
		switch {
		case wait > 50*time.Millisecond:
			time.Sleep(50 * time.Millisecond)
		case wait > 20*time.Microsecond:
			time.Sleep(wait)
		default:
			runtime.Gosched()
		}
	}
}

type byteBudget struct {
	bytes  int64
	blocks int64
	used   atomic.Int64
	count  atomic.Int64
}

func newByteBudget(params testParams) *byteBudget {
	if params.Num <= 0 && params.BlockCount <= 0 {
		return nil
	}
	return &byteBudget{bytes: int64(params.Num), blocks: int64(params.BlockCount)}
}

func (b *byteBudget) take(n int) bool {
	if b.bytes > 0 && b.used.Load() >= b.bytes {
		return false
	}
	if b.blocks > 0 && b.count.Load() >= b.blocks {
		return false
	}
	b.used.Add(int64(n))
	b.count.Add(1)
	return true
}

func (b *byteBudget) takeBatch(size, n int) int {
	for i := 0; i < n; i++ {
		if !b.take(size) {
			return i
		}
	}
	return n
}

func (b *byteBudget) refund(size, count int) {
	if count > 0 {
		b.used.Add(-int64(size * count))
		b.count.Add(-int64(count))
	}
}

func totalBytes(streams []*testStream, sender bool) int64 {
	var total int64
	for _, stream := range streams {
		if stream.sender == sender {
			total += stream.bytes.Load()
		}
	}
	return total
}
