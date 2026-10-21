//go:build linux

package iperf3

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"
)

func TestUDPKernelTimeLinux(t *testing.T) {
	cases := []struct {
		network string
		listen  string
		target  string
	}{
		{"udp", ":0", "127.0.0.1"},
		{"udp4", "127.0.0.1:0", "127.0.0.1"},
		{"udp6", "[::1]:0", "::1"},
	}
	for _, tc := range cases {
		t.Run(tc.network+"-"+tc.target, func(t *testing.T) {
			listener, err := udpListenConfig(nil).ListenPacket(context.Background(), tc.network, tc.listen)
			if err != nil {
				t.Skipf("listen %s: %v", tc.network, err)
			}
			defer listener.Close()
			server := listener.(*net.UDPConn)
			port := server.LocalAddr().(*net.UDPAddr).Port
			client, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.ParseIP(tc.target), Port: port})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			from := client.LocalAddr().(*net.UDPAddr).AddrPort()

			bufs := make([][]byte, 16)
			for i := range bufs {
				bufs[i] = make([]byte, 1200)
				bufs[i][0] = byte(i)
			}
			stamped := 0
			writer := newUDPWriter(client, netip.AddrPort{})
			if n, err := writer.write(bufs, func(int) { stamped++ }); n != len(bufs) || err != nil || stamped != len(bufs) {
				t.Fatalf("connected write n=%d err=%v stamped=%d", n, err, stamped)
			}
			time.Sleep(200 * time.Millisecond)

			reader := newUDPReader(server, 1500)
			if reader.oob == nil {
				t.Fatal("kernel timestamps unavailable")
			}
			var peer netip.AddrPort
			readAt := time.Now().UnixNano()
			for received := 0; received < len(bufs); received++ {
				_ = server.SetReadDeadline(time.Now().Add(2 * time.Second))
				if err := reader.read(func(packet []byte, addr netip.AddrPort, now int64) {
					if len(packet) != 1200 || packet[0] != byte(received) {
						t.Errorf("bad packet %d len=%d first=%d", received, len(packet), packet[0])
					}
					if now > readAt-int64(100*time.Millisecond) || now < readAt-int64(2*time.Second) {
						t.Errorf("packet %d not stamped by kernel: %dms before read", received, (readAt-now)/1e6)
					}
					peer = addr
				}); err != nil {
					t.Fatal(err)
				}
			}
			if peer.Port() != from.Port() || peer.Addr().Unmap() != from.Addr().Unmap() {
				t.Fatalf("wrong peer: peer=%v from=%v", peer, from)
			}

			for _, target := range []netip.AddrPort{peer, netip.AddrPortFrom(peer.Addr().Unmap(), peer.Port())} {
				back := newUDPWriter(server, target)
				if n, err := back.write(bufs, func(int) {}); n != len(bufs) || err != nil {
					t.Fatalf("write to %v n=%d err=%v", target, n, err)
				}
				got := make([]byte, 2048)
				for i := 0; i < len(bufs); i++ {
					_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
					if n, err := client.Read(got); err != nil || n != 1200 || got[0] != byte(i) {
						t.Fatalf("client read %d %v first=%d", n, err, got[0])
					}
				}
			}
		})
	}
	if kernelTime(nil) != 0 {
		t.Fatal("empty control message should have no timestamp")
	}
}
