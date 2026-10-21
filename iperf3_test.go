package iperf3

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type recorder struct {
	mu     sync.Mutex
	states []State
}

func (r *recorder) add(state State) {
	r.mu.Lock()
	r.states = append(r.states, state)
	r.mu.Unlock()
}

func (r *recorder) last() State {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.states) == 0 {
		return State{}
	}
	return r.states[len(r.states)-1]
}

func (r *recorder) waitDone(t *testing.T, session int) State {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if state := r.last(); state.Session == session && state.Phase == PhaseDone {
			return state
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server session %d did not finish: %+v", session, r.last())
	return State{}
}

func startTestServer(t *testing.T) (*Server, *recorder) {
	t.Helper()
	recorder := &recorder{}
	server, err := Listen(ServerConfig{Addr: ":0", OnUpdate: recorder.add})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	return server, recorder
}

func runTestClient(ctx context.Context, params ClientConfig) State {
	return RunClient(ctx, params)
}

func assertFlow(t *testing.T, name string, flow *Flow, udp bool) {
	t.Helper()
	if flow == nil || flow.Bytes <= 0 || flow.Bitrate <= 0 {
		t.Fatalf("%s flow is empty: %+v", name, flow)
	}
	if udp && flow.Packets <= 0 {
		t.Fatalf("%s flow has no packets: %+v", name, flow)
	}
}

func TestLoopback(t *testing.T) {
	server, recorder := startTestServer(t)
	cases := []ClientConfig{
		{Protocol: TCP, Direction: Upload, Parallel: 2},
		{Protocol: TCP, Direction: Download, Parallel: 1},
		{Protocol: TCP, Direction: Bidir, Parallel: 2},
		{Protocol: UDP, Direction: Upload, Parallel: 1, Bandwidth: 20e6},
		{Protocol: UDP, Direction: Download, Parallel: 2, Bandwidth: 20e6},
		{Protocol: UDP, Direction: Bidir, Parallel: 1, Bandwidth: 10e6},
	}
	for index, params := range cases {
		params := params
		params.Hosts = []string{"192.0.2.1", "127.0.0.1"}
		params.Port = server.Port()
		params.Duration = time.Second
		t.Run(string(params.Protocol)+"-"+string(params.Direction), func(t *testing.T) {
			state := runTestClient(context.Background(), params)
			if state.Error != "" || state.Phase != PhaseDone {
				t.Fatalf("client failed: %+v", state)
			}
			if state.Peer != "127.0.0.1" {
				t.Fatalf("unexpected peer %q", state.Peer)
			}
			udp := params.Protocol == UDP
			if params.Direction != Download {
				assertFlow(t, "upload", state.UploadResult, udp)
			}
			if params.Direction != Upload {
				assertFlow(t, "download", state.DownloadResult, udp)
			}
			if !udp && runtime.GOOS == "linux" {
				for _, flow := range []*Flow{state.UploadResult, state.DownloadResult} {
					if flow != nil && (flow.RTTMax <= 0 || flow.RTTMean <= 0) {
						t.Fatalf("missing rtt: %+v", flow)
					}
				}
			}
			if udp && state.UploadResult != nil && state.UploadResult.Bitrate > 30e6 {
				t.Fatalf("udp pacing ignored: %+v", state.UploadResult)
			}
			served := recorder.waitDone(t, index+1)
			if served.Error != "" || served.Direction != params.Direction || served.Protocol != params.Protocol {
				t.Fatalf("server state mismatch: %+v", served)
			}
		})
	}
}

func TestFlowRTT(t *testing.T) {
	first := newTestStream(1, true, false, false)
	first.rttMin, first.rttMax, first.rttSum, first.rttCount = 2000, 9000, 12000, 3
	second := newTestStream(3, true, false, false)
	idle := newTestStream(5, true, false, false)
	receiver := newTestStream(7, false, false, false)
	remote := map[int]streamResult{
		7: {ID: 7, MinRTT: 1000, MaxRTT: 20000, MeanRTT: 6000},
	}
	second.rttMin, second.rttMax, second.rttSum, second.rttCount = 1000, 3000, 4000, 2
	sent := buildFlow([]*testStream{first, second, idle}, true, nil, false, 1)
	if sent.RTTMin != 1 || sent.RTTMax != 9 || sent.RTTMean != 3 {
		t.Fatalf("unexpected sender rtt: %+v", sent)
	}
	received := buildFlow([]*testStream{receiver}, false, remote, true, 1)
	if received.RTTMin != 1 || received.RTTMax != 20 || received.RTTMean != 6 {
		t.Fatalf("unexpected receiver rtt: %+v", received)
	}
	if flow := buildFlow([]*testStream{receiver}, false, nil, true, 1); flow.RTTMax != 0 || flow.RTTMean != 0 {
		t.Fatalf("rtt without peer data: %+v", flow)
	}
	data, err := json.Marshal(idle.result(1))
	if err != nil || strings.Contains(string(data), "rtt") {
		t.Fatalf("idle stream leaks rtt fields: %s %v", data, err)
	}
}

func TestBusyAndCancel(t *testing.T) {
	server, recorder := startTestServer(t)
	params := ClientConfig{Hosts: []string{"127.0.0.1"}, Port: server.Port(), Duration: 10 * time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan State, 1)
	go func() {
		done <- runTestClient(ctx, params)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for recorder.last().Phase != PhaseRunning && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	busy := runTestClient(context.Background(), params)
	if busy.Error != ErrorBusy {
		t.Fatalf("expected busy, got %+v", busy)
	}
	cancel()
	select {
	case state := <-done:
		if state.Error != ErrorCancelled {
			t.Fatalf("expected cancelled, got %+v", state)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("client did not stop after cancel")
	}
	served := recorder.waitDone(t, 1)
	if served.Error == "" {
		t.Fatalf("server should report the interrupted test: %+v", served)
	}
	params.Duration = time.Second
	if state := runTestClient(context.Background(), params); state.Error != "" {
		t.Fatalf("server did not recover: %+v", state)
	}
}

func TestErrors(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	state := runTestClient(context.Background(), ClientConfig{Hosts: []string{"127.0.0.1"}, Port: port, Duration: time.Second})
	if state.Error != ErrorRefused {
		t.Fatalf("expected refused, got %+v", state)
	}
	fake, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer fake.Close()
	go func() {
		for {
			conn, err := fake.Accept()
			if err != nil {
				return
			}
			_, _ = conn.Write([]byte("HTTP/1.1 400 Bad Request\r\n\r\n"))
			_ = conn.Close()
		}
	}()
	state = runTestClient(context.Background(), ClientConfig{Hosts: []string{"127.0.0.1"}, Port: fake.Addr().(*net.TCPAddr).Port, Duration: time.Second})
	if state.Error != ErrorProtocol {
		t.Fatalf("expected protocol error, got %+v", state)
	}
	server, _ := startTestServer(t)
	if _, err := Listen(ServerConfig{Addr: ":" + strconv.Itoa(server.Port())}); !IsAddrInUse(err) {
		t.Fatalf("expected port in use, got %v", err)
	}
}

func TestServerReleasesResources(t *testing.T) {
	baseline := runtime.NumGoroutine()
	server, err := Listen(ServerConfig{Addr: ":0"})
	if err != nil {
		t.Fatal(err)
	}
	params := ClientConfig{Hosts: []string{"127.0.0.1"}, Port: server.Port(), Duration: time.Second, Parallel: 4, Direction: Bidir}
	if state := runTestClient(context.Background(), params); state.Error != "" {
		t.Fatalf("client failed: %+v", state)
	}
	_ = server.Close()
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > baseline && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > baseline {
		t.Fatalf("leaked goroutines %d -> %d", baseline, after)
	}
}

func officialBinary(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("short mode")
	}
	path, err := exec.LookPath("iperf3")
	if err != nil {
		t.Skip("iperf3 not installed")
	}
	return path
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func TestOfficialClient(t *testing.T) {
	binary := officialBinary(t)
	server, recorder := startTestServer(t)
	cases := [][]string{
		{"-P", "2"},
		{"-R"},
		{"--bidir", "-P", "2"},
		{"-u", "-b", "20M"},
		{"-u", "-b", "20M", "-R", "--udp-counters-64bit"},
		{"-u", "-b", "10M", "--bidir"},
		{"-O", "1", "--get-server-output"},
	}
	for index, extra := range cases {
		args := append([]string{"-c", "127.0.0.1", "-p", strconv.Itoa(server.Port()), "-t", "1", "-J"}, extra...)
		t.Run(fmt.Sprint(extra), func(t *testing.T) {
			output, err := exec.Command(binary, args...).Output()
			var report struct {
				Error string `json:"error"`
				End   struct {
					SumSent struct {
						Bytes float64 `json:"bytes"`
					} `json:"sum_sent"`
					SumReceived struct {
						Bytes float64 `json:"bytes"`
					} `json:"sum_received"`
					Sum struct {
						Bytes float64 `json:"bytes"`
					} `json:"sum"`
				} `json:"end"`
			}
			_ = json.Unmarshal(output, &report)
			if err != nil || report.Error != "" {
				t.Fatalf("official client failed: %v %s", err, report.Error)
			}
			if report.End.SumReceived.Bytes <= 0 && report.End.Sum.Bytes <= 0 {
				t.Fatalf("official client received nothing: %s", output)
			}
			served := recorder.waitDone(t, index+1)
			if served.Error != "" {
				t.Fatalf("server failed: %+v", served)
			}
		})
	}
}

func TestOfficialServer(t *testing.T) {
	binary := officialBinary(t)
	cases := []ClientConfig{
		{Protocol: TCP, Direction: Upload, Parallel: 2},
		{Protocol: TCP, Direction: Download, Parallel: 2},
		{Protocol: TCP, Direction: Bidir, Parallel: 1},
		{Protocol: UDP, Direction: Upload, Parallel: 1, Bandwidth: 20e6},
		{Protocol: UDP, Direction: Download, Parallel: 1, Bandwidth: 20e6},
		{Protocol: UDP, Direction: Bidir, Parallel: 2, Bandwidth: 20e6},
	}
	for _, params := range cases {
		params := params
		t.Run(string(params.Protocol)+"-"+string(params.Direction), func(t *testing.T) {
			port := freePort(t)
			cmd := exec.Command(binary, "-s", "-1", "-p", strconv.Itoa(port))
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			exited := make(chan error, 1)
			go func() { exited <- cmd.Wait() }()
			t.Cleanup(func() {
				_ = cmd.Process.Kill()
				select {
				case <-exited:
				case <-time.After(time.Second):
				}
			})
			params.Hosts = []string{"127.0.0.1"}
			params.Port = port
			params.Duration = time.Second
			var state State
			for attempt := 0; attempt < 20; attempt++ {
				state = runTestClient(context.Background(), params)
				if state.Error != ErrorRefused {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if state.Error != "" {
				t.Fatalf("client failed: %+v", state)
			}
			udp := params.Protocol == UDP
			if params.Direction != Download {
				assertFlow(t, "upload", state.UploadResult, udp)
			}
			if params.Direction != Upload {
				assertFlow(t, "download", state.DownloadResult, udp)
			}
			if !udp && state.DownloadResult != nil && state.DownloadResult.Retransmits < 0 {
				t.Fatalf("missing retransmits from official server: %+v", state.DownloadResult)
			}
			select {
			case err := <-exited:
				exited <- err
			case <-time.After(5 * time.Second):
				t.Fatal("official server did not finish the test")
			}
		})
	}
}

func TestFlowLoss(t *testing.T) {
	sender := newTestStream(1, true, true, false)
	sender.packets.Store(1000)
	receiver := newTestStream(2, false, true, false)
	receiver.packets.Store(960)
	receiver.errors.Store(12)
	tail := map[int]streamResult{1: {ID: 1, Packets: 960, Errors: 12}, 2: {ID: 2, Packets: 1000}}
	for _, flow := range []*Flow{
		buildFlow([]*testStream{sender}, true, tail, false, 1),
		buildFlow([]*testStream{receiver}, false, tail, false, 1),
	} {
		if flow.Lost != 12 || flow.Packets != 960 {
			t.Fatalf("in-flight tail counted as loss: %+v", flow)
		}
	}
	blocked := map[int]streamResult{1: {ID: 1}, 2: {ID: 2, Packets: 1000}}
	silent := newTestStream(2, false, true, false)
	for _, flow := range []*Flow{
		buildFlow([]*testStream{sender}, true, blocked, false, 1),
		buildFlow([]*testStream{silent}, false, blocked, false, 1),
	} {
		if flow.Lost != 1000 || flow.Packets != 1000 {
			t.Fatalf("blocked path not reported as loss: %+v", flow)
		}
	}
}

func TestRandomPayload(t *testing.T) {
	random := func(data []byte) bool {
		zeros := 0
		for _, b := range data {
			if b == 0 {
				zeros++
			}
		}
		return zeros < len(data)/16
	}
	ctx, cancel := context.WithCancel(context.Background())
	tcp := newTestStream(1, true, false, false)
	var sent []byte
	tcp.write = func(buf []byte) (int, error) {
		sent = append([]byte(nil), buf...)
		cancel()
		return len(buf), nil
	}
	tcp.sendLoop(ctx, tcpBlockSize, 0, nil)
	if len(sent) != tcpBlockSize || !random(sent) {
		t.Fatalf("tcp payload is not random")
	}

	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	udp := newTestStream(1, true, true, true)
	udp.conn = local
	udp.writer = newUDPWriter(local, netip.AddrPort{})
	go udp.sendLoop(ctx, udpBlockSize, 0, nil)
	packet := make([]byte, udpBlockSize)
	if _, err := io.ReadFull(remote, packet); err != nil {
		t.Fatal(err)
	}
	cancel()
	go func() { _, _ = io.Copy(io.Discard, remote) }()
	if binary.BigEndian.Uint64(packet[8:16]) != 1 || !random(packet[16:]) {
		t.Fatalf("udp payload is not random: %x", packet[:32])
	}
}
