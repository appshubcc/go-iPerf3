//go:build darwin

package iperf3

import (
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

const retransmitsSupported = true

func tcpInfo(conn net.Conn) (retrans, rtt int64, ok bool) {
	sc, isSyscall := conn.(syscall.Conn)
	if !isSyscall {
		return 0, 0, false
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return 0, 0, false
	}
	var info *unix.TCPConnectionInfo
	var infoErr error
	if err := raw.Control(func(fd uintptr) {
		info, infoErr = unix.GetsockoptTCPConnectionInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_CONNECTION_INFO)
	}); err != nil || infoErr != nil || info == nil {
		return 0, 0, false
	}
	return int64(info.Txretransmitpackets), int64(info.Srtt) * 1000, true
}

func tcpMSS(conn net.Conn) int {
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return 0
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return 0
	}
	mss := 0
	_ = raw.Control(func(fd uintptr) {
		if value, err := unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_MAXSEG); err == nil {
			mss = value
		}
	})
	return mss
}
