//go:build darwin

package iperf3

import (
	"net"
	"net/netip"
	"syscall"
)

func udpReuseControl(_, _ string, c syscall.RawConn) error {
	var err error
	if controlErr := c.Control(func(fd uintptr) {
		if err = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err == nil {
			err = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEPORT, 1)
		}
	}); controlErr != nil {
		return controlErr
	}
	return err
}

var udpReuse = udpReuseControl

// dialUDPSender gives each server-to-client UDP stream its own connected
// socket on the server port (SO_REUSEPORT), so it can use Write instead of
// WriteTo on the shared listening socket.
func dialUDPSender(port int, peer netip.AddrPort) (*net.UDPConn, error) {
	d := net.Dialer{
		Control:   udpReuseControl,
		LocalAddr: &net.UDPAddr{IP: net.IPv6unspecified, Port: port},
	}
	conn, err := d.Dial("udp", peer.String())
	if err != nil {
		return nil, err
	}
	udp, ok := conn.(*net.UDPConn)
	if !ok {
		_ = conn.Close()
		return nil, syscall.EOPNOTSUPP
	}
	_ = udp.SetWriteBuffer(4 << 20)
	return udp, nil
}
