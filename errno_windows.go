//go:build windows

package iperf3

import (
	"errors"
	"syscall"

	"golang.org/x/sys/windows"
)

// IsAddrInUse reports whether err from Listen means the port is already in use.
func IsAddrInUse(err error) bool {
	return errors.Is(err, windows.WSAEADDRINUSE) || errors.Is(err, syscall.EADDRINUSE)
}

func isConnRefused(err error) bool {
	return errors.Is(err, windows.WSAECONNREFUSED) || errors.Is(err, syscall.ECONNREFUSED)
}

func isSendBusy(err error) bool {
	return errors.Is(err, windows.WSAENOBUFS) || errors.Is(err, windows.WSAEWOULDBLOCK)
}
