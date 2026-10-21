//go:build !linux

package iperf3

import (
	"net"
	"net/netip"
	"time"
)

type udpReader struct {
	conn   net.Conn
	udp    *net.UDPConn
	single []byte
}

func newUDPReader(conn net.Conn, size int) *udpReader {
	if size < 2048 {
		size = 2048
	}
	udp, _ := conn.(*net.UDPConn)
	return &udpReader{conn: conn, udp: udp, single: make([]byte, size)}
}

func (r *udpReader) read(fn func(packet []byte, addr netip.AddrPort, now int64)) error {
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
