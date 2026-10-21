//go:build !windows

package iperf3

import (
	"context"
	"syscall"
	"testing"
	"time"
)

func cpuTime() time.Duration {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

func benchRun(b *testing.B, params ClientConfig) {
	server, err := Listen(ServerConfig{Addr: ":0"})
	if err != nil {
		b.Fatal(err)
	}
	defer server.Close()
	params.Hosts = []string{"127.0.0.1"}
	params.Port = server.Port()
	if params.Duration == 0 {
		params.Duration = 3 * time.Second
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cpu := cpuTime()
		start := time.Now()
		state := runTestClient(context.Background(), params)
		wall := time.Since(start)
		cpu = cpuTime() - cpu
		if state.Error != "" {
			b.Fatalf("iperf failed: %+v", state)
		}
		flow := state.UploadResult
		if flow == nil {
			flow = state.DownloadResult
		}
		if flow == nil || flow.Bytes <= 0 {
			b.Fatalf("no data: %+v", state)
		}
		gigabits := flow.Bytes * 8 / 1e9
		b.ReportMetric(flow.Bitrate/1e6, "Mbps")
		b.ReportMetric(cpu.Seconds()/wall.Seconds(), "cores")
		b.ReportMetric(cpu.Seconds()/gigabits, "cpu-s/Gbit")
		if params.Protocol == UDP {
			loss := 0.0
			if flow.Packets > 0 {
				loss = float64(flow.Lost) / float64(flow.Packets) * 100
			}
			b.ReportMetric(loss, "loss%")
			b.ReportMetric(flow.Jitter, "jitter-ms")
		}
	}
}

func BenchmarkUDPUploadUnlimited(b *testing.B) {
	benchRun(b, ClientConfig{Protocol: UDP, Direction: Upload})
}

func BenchmarkUDPDownloadUnlimited(b *testing.B) {
	benchRun(b, ClientConfig{Protocol: UDP, Direction: Download})
}

func BenchmarkUDPUpload1G(b *testing.B) {
	benchRun(b, ClientConfig{Protocol: UDP, Direction: Upload, Bandwidth: 1e9})
}

func BenchmarkUDPDownload1G(b *testing.B) {
	benchRun(b, ClientConfig{Protocol: UDP, Direction: Download, Bandwidth: 1e9})
}

func BenchmarkUDPUpload100M(b *testing.B) {
	benchRun(b, ClientConfig{Protocol: UDP, Direction: Upload, Bandwidth: 100e6})
}

func BenchmarkTCPUpload4(b *testing.B) {
	benchRun(b, ClientConfig{Protocol: TCP, Direction: Upload, Parallel: 4})
}

func BenchmarkTCPDownload4(b *testing.B) {
	benchRun(b, ClientConfig{Protocol: TCP, Direction: Download, Parallel: 4})
}
