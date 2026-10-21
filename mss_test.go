//go:build linux || darwin

package iperf3

import (
	"net"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

func setMSS(c syscall.RawConn, mss int) {
	_ = c.Control(func(fd uintptr) {
		_ = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_MAXSEG, mss)
	})
}

func TestUDPSizeFollowsMSS(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()
	dial := func(mss int) net.Conn {
		dialer := net.Dialer{Control: func(_, _ string, c syscall.RawConn) error {
			if mss > 0 {
				setMSS(c, mss)
			}
			return nil
		}}
		conn, err := dialer.Dial("tcp", listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		if raw, err := conn.(*net.TCPConn).SyscallConn(); err == nil && mss > 0 {
			setMSS(raw, mss)
		}
		return conn
	}
	wide := dial(0)
	defer wide.Close()
	if size := udpSize(wide); size != udpBlockSize {
		t.Fatalf("loopback size = %d, want %d", size, udpBlockSize)
	}
	narrow := dial(1200)
	defer narrow.Close()
	mss := tcpMSS(narrow)
	if mss <= 0 || mss > 1200 {
		t.Skipf("cannot clamp mss on this system: %d", mss)
	}
	if size := udpSize(narrow); size != mss {
		t.Fatalf("clamped size = %d, mss = %d", size, mss)
	}
}
