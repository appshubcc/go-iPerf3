//go:build linux

package iperf3

import (
	"net"
	"net/netip"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

type udpReader struct {
	conn   net.Conn
	udp    *net.UDPConn
	single []byte
	oob    []byte
}

// newUDPReader asks the kernel to timestamp incoming datagrams (SO_TIMESTAMPNS).
// Measuring jitter from the kernel's arrival time instead of the moment Go reads
// the datagram keeps scheduling delays out of the result.
func newUDPReader(conn net.Conn, size int) *udpReader {
	if size < 2048 {
		size = 2048
	}
	reader := &udpReader{conn: conn, single: make([]byte, size)}
	udp, ok := conn.(*net.UDPConn)
	if !ok {
		return reader
	}
	reader.udp = udp
	raw, err := udp.SyscallConn()
	if err != nil {
		return reader
	}
	enabled := false
	_ = raw.Control(func(fd uintptr) {
		enabled = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_TIMESTAMPNS, 1) == nil
	})
	if enabled {
		reader.oob = make([]byte, unix.CmsgSpace(int(unsafe.Sizeof(unix.Timespec{}))))
	}
	return reader
}

func kernelTime(oob []byte) int64 {
	header := unix.CmsgLen(0)
	for len(oob) >= header {
		h := (*unix.Cmsghdr)(unsafe.Pointer(&oob[0]))
		length := int(h.Len)
		if length < header || length > len(oob) {
			return 0
		}
		if h.Level == unix.SOL_SOCKET && h.Type == unix.SCM_TIMESTAMPNS && length-header >= int(unsafe.Sizeof(unix.Timespec{})) {
			ts := (*unix.Timespec)(unsafe.Pointer(&oob[header]))
			return ts.Nano()
		}
		next := unix.CmsgSpace(length - header)
		if next > len(oob) {
			return 0
		}
		oob = oob[next:]
	}
	return 0
}

func (r *udpReader) read(fn func(packet []byte, addr netip.AddrPort, now int64)) error {
	if r.udp != nil && r.oob != nil {
		n, oobn, _, addr, err := r.udp.ReadMsgUDPAddrPort(r.single, r.oob)
		if err != nil {
			return err
		}
		now := kernelTime(r.oob[:oobn])
		if now == 0 {
			now = time.Now().UnixNano()
		}
		fn(r.single[:n], addr, now)
		return nil
	}
	if r.udp != nil {
		n, addr, err := r.udp.ReadFromUDPAddrPort(r.single)
		if err != nil {
			return err
		}
		fn(r.single[:n], addr, time.Now().UnixNano())
		return nil
	}
	n, err := r.conn.Read(r.single)
	if n > 0 {
		fn(r.single[:n], netip.AddrPort{}, time.Now().UnixNano())
	}
	return err
}

var udpReuse func(network, address string, c syscall.RawConn) error

func dialUDPSender(int, netip.AddrPort) (*net.UDPConn, error) {
	return nil, syscall.EOPNOTSUPP
}
