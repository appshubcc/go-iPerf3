package iperf3

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Protocol is the transport used for the test data streams.
type Protocol string

// Direction tells which side sends the test data.
type Direction string

// Role tells whether a State was produced by a client or a server.
type Role string

// Phase is the stage a test is in.
type Phase string

// Supported transports.
const (
	TCP Protocol = "tcp"
	UDP Protocol = "udp"
)

const (
	// Upload sends data from the client to the server (the iperf3 default).
	Upload Direction = "upload"
	// Download sends data from the server to the client (iperf3 -R).
	Download Direction = "download"
	// Bidir sends data both ways at the same time (iperf3 --bidir).
	Bidir Direction = "bidir"
)

// Roles reported in State.Role.
const (
	RoleClient Role = "client"
	RoleServer Role = "server"
)

const (
	// PhaseConnecting covers the handshake, parameter exchange and stream setup.
	PhaseConnecting Phase = "connecting"
	// PhaseRunning means data is flowing; State.Upload and State.Download hold live bitrates.
	PhaseRunning Phase = "running"
	// PhaseDone is the final phase. State.Error is empty on success.
	PhaseDone Phase = "done"
)

// Error codes reported in State.Error.
const (
	// ErrorBusy means the server is already running another test.
	ErrorBusy = "busy"
	// ErrorRefused means nothing is listening on the server port.
	ErrorRefused = "refused"
	// ErrorTimeout means connecting or the handshake took too long.
	ErrorTimeout = "timeout"
	// ErrorUnreachable covers any other network failure.
	ErrorUnreachable = "unreachable"
	// ErrorUDP means the UDP data streams could not be set up, often because of a firewall.
	ErrorUDP = "udp"
	// ErrorProtocol means the peer is not an iPerf3 endpoint or sent invalid data.
	ErrorProtocol = "protocol"
	// ErrorInterrupted means the peer ended the test early.
	ErrorInterrupted = "interrupted"
	// ErrorCancelled means the test was stopped through its context or Server.Close.
	ErrorCancelled = "cancelled"
	// ServerErrorPrefix starts the error reported when the server rejects a test.
	// It is followed by the iPerf3 error number, for example "server:6" for too many streams.
	ServerErrorPrefix = "server:"
)

const (
	// DefaultPort is the standard iPerf3 port.
	DefaultPort = 5201
	// MaxStreams is the largest number of parallel streams per direction.
	MaxStreams = 128
	// MaxDuration is the longest test, in seconds.
	MaxDuration = 86400
	// DefaultDuration is used when ClientConfig.Duration is zero.
	DefaultDuration = 10 * time.Second
)

const (
	cookieSize      = 37
	protocolVersion = "3.21"
	tcpBlockSize    = 128 * 1024
	maxTCPBlockSize = 1024 * 1024
	udpBlockSize    = 1460
	udpBlockSize6   = 1440
	udpBatchSize    = 64
	maxUDPBlockSize = 65507
	udpConnectMsg   = 0x36373839
	udpConnectReply = 0x39383736
	udpLegacyReply  = 987654321
	sampleInterval  = 250 * time.Millisecond
	paceWindow      = 0.0002
)

const (
	stateTestStart       int8 = 1
	stateTestRunning     int8 = 2
	stateTestEnd         int8 = 4
	stateParamExchange   int8 = 9
	stateCreateStreams   int8 = 10
	stateServerTerminate int8 = 11
	stateClientTerminate int8 = 12
	stateExchangeResults int8 = 13
	stateDisplayResults  int8 = 14
	stateDone            int8 = 16
	stateAccessDenied    int8 = -1
	stateServerError     int8 = -2
)

const (
	codeDuration     int32 = 5
	codeNumStreams   int32 = 6
	codeBlockSize    int32 = 7
	codeOmit         int32 = 12
	codeNoSCTP       int32 = 18
	codeUDPBlockSize int32 = 20
	codeReverseBidir int32 = 25
	codeStreamListen int32 = 202
	codeStreamAccept int32 = 204
)

var (
	errProtocol    = errors.New(ErrorProtocol)
	errBusy        = errors.New(ErrorBusy)
	errUDP         = errors.New(ErrorUDP)
	errInterrupted = errors.New(ErrorInterrupted)
)

type remoteError struct {
	code int32
}

func (e *remoteError) Error() string {
	return ServerErrorPrefix + strconv.Itoa(int(e.code))
}

// Flow is the final result for one direction of a test, summed over its streams.
type Flow struct {
	// Bytes is the amount of data transferred.
	Bytes float64 `json:"bytes"`
	// Bitrate is the average rate in bits per second.
	Bitrate float64 `json:"bitrate"`
	// Retransmits is the TCP retransmit count, or -1 when the sender cannot report it.
	Retransmits int64 `json:"retransmits"`
	// Jitter is the UDP jitter in milliseconds.
	Jitter float64 `json:"jitter"`
	// Lost is the number of UDP datagrams that never arrived.
	Lost int64 `json:"lost"`
	// Packets is the number of UDP datagrams expected by the receiver.
	Packets int64 `json:"packets"`
	// RTTMin, RTTMean and RTTMax are TCP round-trip times in milliseconds,
	// sampled from the kernel. They are zero when unavailable.
	RTTMin  float64 `json:"rtt-min,omitempty"`
	RTTMean float64 `json:"rtt-mean,omitempty"`
	RTTMax  float64 `json:"rtt-max,omitempty"`
}

// State describes a test in progress or a finished test.
//
// Upload always means client to server and Download means server to client,
// whether the State comes from the client or the server.
type State struct {
	Role Role `json:"role"`
	// Session numbers the tests handled by a server, starting at 1. It is 0 on the client.
	Session int   `json:"session"`
	Phase   Phase `json:"phase"`
	// Peer is the address of the other side.
	Peer      string    `json:"peer"`
	Protocol  Protocol  `json:"protocol"`
	Direction Direction `json:"direction"`
	// Parallel is the number of streams per direction.
	Parallel int `json:"parallel"`
	// Duration is the planned test length and Elapsed the time run so far, both in seconds.
	Duration float64 `json:"duration"`
	Elapsed  float64 `json:"elapsed"`
	// Upload and Download are the current bitrates in bits per second while running.
	Upload   float64 `json:"upload"`
	Download float64 `json:"download"`
	// UploadResult and DownloadResult are set when the test finishes,
	// for each direction that was tested.
	UploadResult   *Flow `json:"upload-result,omitempty"`
	DownloadResult *Flow `json:"download-result,omitempty"`
	// Error is empty on success, otherwise one of the Error constants
	// or a code starting with ServerErrorPrefix.
	Error string `json:"error"`
}

type testParams struct {
	TCP             bool    `json:"tcp,omitempty"`
	UDP             bool    `json:"udp,omitempty"`
	SCTP            bool    `json:"sctp,omitempty"`
	Omit            float64 `json:"omit"`
	Time            float64 `json:"time"`
	Num             float64 `json:"num"`
	BlockCount      float64 `json:"blockcount"`
	Parallel        int     `json:"parallel"`
	Reverse         bool    `json:"reverse,omitempty"`
	Bidirectional   bool    `json:"bidirectional,omitempty"`
	Window          float64 `json:"window,omitempty"`
	Len             float64 `json:"len,omitempty"`
	Bandwidth       float64 `json:"bandwidth,omitempty"`
	PacingTimer     float64 `json:"pacing_timer,omitempty"`
	NoDelay         bool    `json:"nodelay,omitempty"`
	GetServerOutput int     `json:"get_server_output,omitempty"`
	UDPCounters64   int     `json:"udp_counters_64bit,omitempty"`
	ClientVersion   string  `json:"client_version,omitempty"`
}

func (p testParams) direction() Direction {
	switch {
	case p.Bidirectional:
		return Bidir
	case p.Reverse:
		return Download
	}
	return Upload
}

func (p testParams) protocol() Protocol {
	if p.UDP {
		return UDP
	}
	return TCP
}

type streamResult struct {
	ID             int     `json:"id"`
	Bytes          float64 `json:"bytes"`
	Retransmits    int64   `json:"retransmits"`
	Jitter         float64 `json:"jitter"`
	Errors         int64   `json:"errors"`
	OmittedErrors  int64   `json:"omitted_errors"`
	Packets        int64   `json:"packets"`
	OmittedPackets int64   `json:"omitted_packets"`
	StartTime      float64 `json:"start_time"`
	EndTime        float64 `json:"end_time"`
	MaxRTT         int64   `json:"max_rtt,omitempty"`
	MinRTT         int64   `json:"min_rtt,omitempty"`
	MeanRTT        int64   `json:"mean_rtt,omitempty"`
}

type testResults struct {
	CPUUtilTotal         float64        `json:"cpu_util_total"`
	CPUUtilUser          float64        `json:"cpu_util_user"`
	CPUUtilSystem        float64        `json:"cpu_util_system"`
	SenderHasRetransmits int            `json:"sender_has_retransmits"`
	ServerOutputText     string         `json:"server_output_text,omitempty"`
	Streams              []streamResult `json:"streams"`
}

func newCookie() []byte {
	const alphabet = "abcdefghijklmnopqrstuvwxyz234567"
	cookie := make([]byte, cookieSize)
	random := make([]byte, cookieSize-1)
	_, _ = rand.Read(random)
	for i, b := range random {
		cookie[i] = alphabet[int(b)%len(alphabet)]
	}
	return cookie
}

// streamID follows iperf3, which numbers streams 1, 3, 4, 5... and never uses 2.
func streamID(index int) int {
	if index == 0 {
		return 1
	}
	return index + 2
}

func writeState(w io.Writer, state int8) error {
	_, err := w.Write([]byte{byte(state)})
	return err
}

func readState(r io.Reader) (int8, error) {
	var buf [1]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return int8(buf[0]), nil
}

func writeJSON(w io.Writer, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	frame := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(frame, uint32(len(data)))
	copy(frame[4:], data)
	_, err = w.Write(frame)
	return err
}

func readJSON(r io.Reader, value any) error {
	var head [4]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return err
	}
	size := binary.BigEndian.Uint32(head[:])
	if size == 0 || size > 4<<20 {
		return errProtocol
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}
	if err := json.Unmarshal(data, value); err != nil {
		return errProtocol
	}
	return nil
}

func writeServerError(w io.Writer, code int32) {
	buf := make([]byte, 9)
	state := stateServerError
	buf[0] = byte(state)
	binary.BigEndian.PutUint32(buf[1:], uint32(code))
	_, _ = w.Write(buf)
}

type controlFunc = func(network, address string, c syscall.RawConn) error

func listenConfig(control controlFunc) *net.ListenConfig {
	return &net.ListenConfig{Control: control}
}

func udpListenConfig(control controlFunc) *net.ListenConfig {
	lc := listenConfig(control)
	if udpReuse == nil {
		return lc
	}
	lc.Control = func(network, address string, c syscall.RawConn) error {
		if control != nil {
			if err := control(network, address, c); err != nil {
				return err
			}
		}
		return udpReuse(network, address, c)
	}
	return lc
}

// udpSize picks a datagram size that fits the path without IP fragmentation:
// the iperf3 default for the address family, lowered to the control connection's MSS.
func udpSize(ctrl net.Conn) int {
	size := udpBlockSize
	if addr, ok := ctrl.RemoteAddr().(*net.TCPAddr); ok && addr.IP.To4() == nil {
		size = udpBlockSize6
	}
	if mss := tcpMSS(ctrl); mss >= 64 && mss < size {
		size = mss
	}
	return size
}

func hostOf(addr net.Addr) string {
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return strings.TrimPrefix(host, "::ffff:")
}
