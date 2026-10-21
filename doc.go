// Package iperf3 implements the iPerf3 network throughput test protocol.
//
// It provides an iPerf3 client (RunClient) and server (Listen) that interoperate
// with the official iperf3 3.x tool over TCP and UDP, in upload, download
// (reverse) and bidirectional mode, with up to MaxStreams parallel streams.
//
// Progress is reported through a callback with a State value, about every
// 250 ms while a test runs, and the final State carries a Flow with the result
// of each direction: bytes, bitrate, TCP retransmits and RTT, and UDP jitter
// and loss. Tests stop when their context is cancelled or the server is closed.
//
// Network errors are not returned as Go errors; they are reported in
// State.Error as one of the Error codes, which makes them easy to show in a UI
// or to send over JSON.
//
// The sockets can be customised with ClientConfig.Dial and ServerConfig.Control,
// for example to bind to a physical interface or to keep the traffic out of a
// VPN tunnel.
package iperf3
