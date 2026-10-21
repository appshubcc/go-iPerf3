package iperf3_test

import (
	"context"
	"fmt"
	"time"

	iperf3 "github.com/appshubcc/go-iperf3"
)

func ExampleRunClient() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	state := iperf3.RunClient(ctx, iperf3.ClientConfig{
		Hosts:     []string{"192.168.1.10"},
		Direction: iperf3.Download,
		Parallel:  4,
		Duration:  10 * time.Second,
		OnUpdate: func(s iperf3.State) {
			if s.Phase == iperf3.PhaseRunning {
				fmt.Printf("%.1fs  %.2f Mbit/s\n", s.Elapsed, s.Download/1e6)
			}
		},
	})
	if state.Error != "" {
		fmt.Println("test failed:", state.Error)
		return
	}
	fmt.Printf("download: %.2f Mbit/s\n", state.DownloadResult.Bitrate/1e6)
}

func ExampleListen() {
	done := make(chan iperf3.State, 1)
	server, err := iperf3.Listen(iperf3.ServerConfig{
		Addr: "127.0.0.1:0",
		OnUpdate: func(s iperf3.State) {
			if s.Phase == iperf3.PhaseDone {
				done <- s
			}
		},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer server.Close()

	client := iperf3.RunClient(context.Background(), iperf3.ClientConfig{
		Hosts:    []string{"127.0.0.1"},
		Port:     server.Port(),
		Duration: time.Second,
	})
	served := <-done
	fmt.Println("client:", client.Phase, client.Error == "")
	fmt.Println("server:", served.Protocol, served.Direction, served.UploadResult.Bitrate > 0)
	// Output:
	// client: done true
	// server: tcp upload true
}
