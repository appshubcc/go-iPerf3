//go:build !windows

package iperf3

import (
	"errors"
	"syscall"
)

// IsAddrInUse reports whether err from Listen means the port is already in use.
func IsAddrInUse(err error) bool {
	return errors.Is(err, syscall.EADDRINUSE)
}

func isConnRefused(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED)
}

func isSendBusy(err error) bool {
	return errors.Is(err, syscall.ENOBUFS) || errors.Is(err, syscall.EAGAIN)
}
