# go-iperf3

[![CI](https://github.com/appshubcc/go-iperf3/actions/workflows/ci.yml/badge.svg)](https://github.com/appshubcc/go-iperf3/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/appshubcc/go-iperf3.svg)](https://pkg.go.dev/github.com/appshubcc/go-iperf3)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

English | [简体中文](README.zh-CN.md)

A lightweight, high-performance implementation of the [iPerf3](https://github.com/esnet/iperf) protocol in pure Go.

go-iperf3 is a library first: embed an iPerf3 client or server in your own app, get live progress through a callback, and stop it with a `context`. It speaks the real iPerf3 wire protocol, so it works with the official `iperf3` binary in both directions. A small command-line tool is included.

It was built for the network toolbox of [Bettbox](https://github.com/appshubcc/Bettbox), where it runs on Android, Windows, macOS and Linux.

## Features

- **Compatible with official iperf3 3.x**, as client and as server, over TCP and UDP.
- **Upload, download (`-R`) and bidirectional (`--bidir`) tests**, with 1–128 parallel streams.
- **Bitrate pacing**: set a target rate for TCP or UDP, or leave it unlimited.
- **TCP statistics**: retransmits, plus min/mean/max RTT sampled from the kernel (Linux, Android, macOS).
- **UDP statistics**: jitter and loss, counted the same way as official iperf3. Datagram size follows the path MSS to avoid IP fragmentation. On Linux and Android, receive timestamps come from the kernel (`SO_TIMESTAMPNS`), which keeps jitter accurate under load.
- **Random payload**, so links that compress traffic cannot inflate the result.
- **Embedding hooks**: a custom `Dial` for the client and a socket `Control` for the server. For example, you can bind sockets outside a VPN/TUN interface, or call Android `VpnService.protect`.
- **Multi-address racing**: give the client several addresses (IPv4 and IPv6, say); it uses the first one whose handshake succeeds.
- **Clean shutdown**: cancelling the context or calling `Close` stops every goroutine and closes every socket.
- **No cgo.** The only dependency is `golang.org/x/sys`.

## Install

Library:

```sh
go get github.com/appshubcc/go-iperf3
```

Command-line tool:

```sh
go install github.com/appshubcc/go-iperf3/cmd/go-iperf3@latest
```

Go 1.20 or newer is required.

## Command-line tool

The flags follow official iperf3:

```sh
go-iperf3 -s                          # server on port 5201
go-iperf3 -s -1 -p 5300               # serve one test on port 5300, then exit
go-iperf3 -c 192.168.1.10             # TCP upload, 10 seconds
go-iperf3 -c 192.168.1.10 -R -P 4     # download with 4 streams
go-iperf3 -c 192.168.1.10 --bidir     # both directions at once
go-iperf3 -c 192.168.1.10 -u -b 100M  # UDP at 100 Mbit/s
go-iperf3 -c 192.168.1.10 -J          # print the result as JSON
```

| Flag | Meaning |
| --- | --- |
| `-s` | Run as a server |
| `-1` | Server: exit after one test |
| `-B <addr>` | Server: bind address |
| `-c <host>` | Run as a client and connect to `host` |
| `-p <port>` | Port (default 5201) |
| `-u` | Use UDP |
| `-R` | Reverse: the server sends, the client receives |
| `--bidir` | Test both directions at the same time |
| `-P <n>` | Parallel streams (1–128) |
| `-t <sec>` | Test length in seconds (default 10) |
| `-b <rate>` | Target bitrate per direction, such as `500K`, `100M` or `1G`, split across streams. Default 0 means unlimited, for UDP too |
| `-J` | Print the final result as JSON |

Example:

```text
$ go-iperf3 -c 127.0.0.1 -t 3 -P 2 --bidir
Connecting to 127.0.0.1, port 5201
[   1s]  upload   59.32 Gbits/sec  download   50.43 Gbits/sec
[   2s]  upload   54.51 Gbits/sec  download   54.40 Gbits/sec
[   3s]  upload   53.77 Gbits/sec  download   53.72 Gbits/sec
- - - - - - - - - - - - - - - - - - - - - - - - -
TCP bidir, 2 stream(s), 3.00 sec
upload     19.09 GBytes    54.66 Gbits/sec  retr 0  rtt 1.00/1.00/1.00 ms
download   18.72 GBytes    53.60 Gbits/sec  retr 0  rtt 1.00/1.04/2.00 ms
```

## Library usage

### Client

```go
package main

import (
	"context"
	"fmt"
	"time"

	iperf3 "github.com/appshubcc/go-iperf3"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	state := iperf3.RunClient(ctx, iperf3.ClientConfig{
		Hosts:     []string{"192.168.1.10"},
		Protocol:  iperf3.TCP,
		Direction: iperf3.Download,
		Parallel:  4,
		Duration:  10 * time.Second,
		OnUpdate: func(s iperf3.State) {
			if s.Phase == iperf3.PhaseRunning {
				fmt.Printf("%.1fs  %.2f Mbit/s\n", s.Elapsed, s.Download/1e6)
			}
		},
	})
	if state.Error != "" {
		fmt.Println("failed:", state.Error)
		return
	}
	fmt.Printf("download: %.2f Mbit/s\n", state.DownloadResult.Bitrate/1e6)
}
```

`RunClient` blocks until the test finishes, fails or `ctx` is cancelled, and returns the final `State`.

### Server

```go
server, err := iperf3.Listen(iperf3.ServerConfig{
	Addr: ":5201",
	OnUpdate: func(s iperf3.State) {
		if s.Phase == iperf3.PhaseDone {
			fmt.Printf("test #%d from %s: %+v\n", s.Session, s.Peer, s.UploadResult)
		}
	},
})
if err != nil {
	if iperf3.IsAddrInUse(err) {
		// the port is taken
	}
	return err
}
defer server.Close()
fmt.Println("listening on", server.Port(), "udp:", server.UDP())
```

The server runs one test at a time, like official iperf3. Clients that connect while a test is running get the standard "busy" reply. TCP and UDP share one port.

### Custom sockets

```go
iperf3.RunClient(ctx, iperf3.ClientConfig{
	Hosts: []string{"10.0.0.2"},
	Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		d := net.Dialer{Control: bindToPhysicalInterface}
		return d.DialContext(ctx, network, address)
	},
})

iperf3.Listen(iperf3.ServerConfig{
	Addr:    ":5201",
	Control: protectSocket,
})
```

`Dial` opens the control connection and every data stream. `Control` is applied to the server's TCP and UDP listening sockets. If it fails on the TCP socket, `Listen` returns the error; if it fails on the UDP socket, the server runs TCP-only and `UDP()` returns false.

## API reference

### `ClientConfig`

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `Hosts` | `[]string` | required | Server addresses (IP or host name). All are tried at once; the first successful handshake wins |
| `Port` | `int` | `5201` | Server port |
| `Protocol` | `Protocol` | `TCP` | `TCP` or `UDP` |
| `Direction` | `Direction` | `Upload` | `Upload`, `Download` (reverse) or `Bidir` |
| `Parallel` | `int` | `1` | Streams per direction, 1–128 |
| `Duration` | `time.Duration` | `10s` | Test length, rounded up to whole seconds, at most 24 h |
| `Bandwidth` | `float64` | `0` | Target bitrate per direction in bits/s, split across streams; `0` = unlimited |
| `Dial` | `DialFunc` | `net.Dialer` | Opens the control and data connections |
| `OnUpdate` | `func(State)` | `nil` | Called on every state change and about every 250 ms while running |

### `ServerConfig`

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `Addr` | `string` | `":5201"` | Listen address; use port `0` for a random port |
| `Control` | `func(network, address string, c syscall.RawConn) error` | `nil` | Applied to the listening sockets |
| `OnUpdate` | `func(State)` | `nil` | Progress and results of each test |

`*Server` provides `Port() int`, `UDP() bool` (whether the UDP socket is listening) and `Close() error`.

### `State`

| Field | Meaning |
| --- | --- |
| `Role` | `RoleClient` or `RoleServer` |
| `Session` | Server: test number, starting at 1. Client: always 0 |
| `Phase` | `PhaseConnecting`, `PhaseRunning` or `PhaseDone` |
| `Peer` | Address of the other side |
| `Protocol`, `Direction`, `Parallel` | Test parameters |
| `Duration`, `Elapsed` | Planned and elapsed time, in seconds |
| `Upload`, `Download` | Current bitrate in bits/s while running |
| `UploadResult`, `DownloadResult` | Final `*Flow` for each direction that was tested |
| `Error` | Empty on success, otherwise one of the error codes below |

Upload always means client → server and download means server → client, whether you read the state on the client or on the server.

### `Flow`

| Field | Unit | Meaning |
| --- | --- | --- |
| `Bytes` | bytes | Data transferred |
| `Bitrate` | bits/s | Average bitrate |
| `Retransmits` | count | TCP retransmits; `-1` when the sender's platform cannot report them |
| `RTTMin`, `RTTMean`, `RTTMax` | ms | TCP round-trip time; `0` when unavailable |
| `Jitter` | ms | UDP jitter |
| `Lost`, `Packets` | count | UDP datagrams lost and expected |

`State` and `Flow` have JSON tags, so they can be sent to a UI as they are.

### Error codes

| Code | Meaning |
| --- | --- |
| `busy` | The server is running another test |
| `refused` | Nothing is listening on that port |
| `timeout` | Connecting or the handshake timed out |
| `unreachable` | Any other network error |
| `udp` | The UDP data stream could not be set up (often blocked by a firewall) |
| `protocol` | The peer is not an iPerf3 server, or sent invalid data |
| `interrupted` | The peer ended the test early |
| `cancelled` | The context was cancelled |
| `server:<n>` | The server rejected the test with iPerf3 error number `n` |

## Compatibility

CI runs interop tests against official iperf3 3.x on Linux and macOS for every change; see `TestOfficialClient` and `TestOfficialServer`.

| | Client (go-iperf3 → iperf3) | Server (iperf3 → go-iperf3) |
| --- | --- | --- |
| TCP / UDP | ✓ | ✓ |
| `-R`, `--bidir`, `-P` | ✓ | ✓ |
| `-b` bitrate | ✓ | ✓ |
| `-O` omit | — | ✓ |
| `-n` / `-k` byte or block count | — | ✓ |
| `-w` window, `-N` no-delay, `-l` length | — | ✓ |
| `--udp-counters-64bit` | — | ✓ |
| `--get-server-output` | — | ✓ |
| SCTP, authentication, `--cport`, `-Z` | ✗ | ✗ |

The client always uses 128 KB writes for TCP and an MSS-sized datagram for UDP. Options marked "—" are not exposed by the client, but the server honours them when an official client sends them.

Two defaults differ from official iperf3 on purpose: the client's bitrate limit is the total for each direction rather than per stream, and UDP is unlimited unless a bitrate is set (official iperf3 defaults to 1 Mbit/s).

### Platform notes

| Platform | Notes |
| --- | --- |
| Linux, Android | TCP retransmits and RTT from `TCP_INFO`; kernel receive timestamps for UDP |
| macOS | TCP retransmits and RTT from `TCP_CONNECTION_INFO` |
| Windows | TCP and UDP in all directions. TCP retransmits and RTT are not reported (`-1` and `0`) |
| Others | Builds anywhere Go and `x/sys` build; TCP statistics are not reported |

## Development

```sh
go vet ./...
go test ./...                     # interop tests run when `iperf3` is on PATH
go test -run '^$' -bench . -benchtime 1x
```

## License

[MIT](LICENSE)
