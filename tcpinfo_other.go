//go:build !linux && !darwin

package iperf3

import "net"

const retransmitsSupported = false

func tcpInfo(net.Conn) (retrans, rtt int64, ok bool) {
	return 0, 0, false
}

func tcpMSS(net.Conn) int {
	return 0
}
