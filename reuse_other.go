//go:build !linux && !darwin

package iperf3

import (
	"net"
	"net/netip"
	"syscall"
)

var udpReuse func(network, address string, c syscall.RawConn) error

func dialUDPSender(int, netip.AddrPort) (*net.UDPConn, error) {
	return nil, syscall.EOPNOTSUPP
}
