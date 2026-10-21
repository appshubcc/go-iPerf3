# go-iperf3

[![CI](https://github.com/appshubcc/go-iperf3/actions/workflows/ci.yml/badge.svg)](https://github.com/appshubcc/go-iperf3/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/appshubcc/go-iperf3.svg)](https://pkg.go.dev/github.com/appshubcc/go-iperf3)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

[English](README.md) | 简体中文

用纯 Go 实现的轻量、高性能 [iPerf3](https://github.com/esnet/iperf) 协议。

go-iperf3 首先是一个库：可以把 iPerf3 客户端或服务端嵌入你自己的应用，通过回调拿到实时进度，用 `context` 随时停止。它实现的是真正的 iPerf3 通信协议，因此无论作为客户端还是服务端，都能和官方 `iperf3` 互通。仓库里也附带了一个小巧的命令行工具。

它最初为 [Bettbox](https://github.com/appshubcc/Bettbox) 的网络工具箱而写，在 Android、Windows、macOS 和 Linux 上运行。

## 功能

- **兼容官方 iperf3 3.x**：可作为客户端或服务端，支持 TCP 和 UDP。
- **上传、下载（`-R`）和双向（`--bidir`）测试**，支持 1–128 条并行流。
- **速率控制**：TCP 和 UDP 都可以设定目标速率，也可以不限速。
- **TCP 统计**：重传次数，以及从内核采样的最小/平均/最大 RTT（Linux、Android、macOS）。
- **UDP 统计**：抖动和丢包，统计口径与官方 iperf3 一致。数据报大小跟随路径 MSS，避免 IP 分片。在 Linux 和 Android 上，接收时间戳由内核提供（`SO_TIMESTAMPNS`），高负载下抖动依然准确。
- **随机负载**：会压缩流量的链路无法虚高测试结果。
- **嵌入扩展点**：客户端可以自定义 `Dial`，服务端可以自定义 socket `Control`。例如把 socket 绑定到 VPN/TUN 之外的网卡，或者调用 Android 的 `VpnService.protect`。
- **多地址竞速**：可以给客户端传入多个地址（比如同时传 IPv4 和 IPv6），哪个先握手成功就用哪个。
- **干净退出**：取消 context 或调用 `Close` 后，所有 goroutine 都会结束，所有 socket 都会关闭。
- **不需要 cgo**，唯一依赖是 `golang.org/x/sys`。

## 安装

库：

```sh
go get github.com/appshubcc/go-iperf3
```

命令行工具：

```sh
go install github.com/appshubcc/go-iperf3/cmd/go-iperf3@latest
```

需要 Go 1.20 或更高版本。

## 命令行工具

参数和官方 iperf3 保持一致：

```sh
go-iperf3 -s                          # 在 5201 端口启动服务端
go-iperf3 -s -1 -p 5300               # 在 5300 端口只服务一次测试，然后退出
go-iperf3 -c 192.168.1.10             # TCP 上传，持续 10 秒
go-iperf3 -c 192.168.1.10 -R -P 4     # 下载，4 条并行流
go-iperf3 -c 192.168.1.10 --bidir     # 上下行同时测试
go-iperf3 -c 192.168.1.10 -u -b 100M  # UDP，速率 100 Mbit/s
go-iperf3 -c 192.168.1.10 -J          # 以 JSON 输出结果
```

| 参数 | 含义 |
| --- | --- |
| `-s` | 以服务端运行 |
| `-1` | 服务端：完成一次测试后退出 |
| `-B <addr>` | 服务端：监听地址 |
| `-c <host>` | 以客户端运行，连接 `host` |
| `-p <port>` | 端口（默认 5201） |
| `-u` | 使用 UDP |
| `-R` | 反向：服务端发送，客户端接收 |
| `--bidir` | 上下行同时测试 |
| `-P <n>` | 并行流数量（1–128） |
| `-t <sec>` | 测试时长，单位秒（默认 10） |
| `-b <rate>` | 每个方向的目标速率，如 `500K`、`100M`、`1G`，平均分到各条流。默认 0 表示不限速，UDP 也一样 |
| `-J` | 以 JSON 输出最终结果 |

示例：

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

## 作为库使用

### 客户端

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

`RunClient` 会一直阻塞，直到测试完成、失败或 `ctx` 被取消，然后返回最终的 `State`。

### 服务端

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
		// 端口被占用
	}
	return err
}
defer server.Close()
fmt.Println("listening on", server.Port(), "udp:", server.UDP())
```

和官方 iperf3 一样，服务端同一时间只进行一个测试。测试进行中有其他客户端连入时，会收到标准的 "busy" 回复。TCP 和 UDP 共用同一个端口。

### 自定义 socket

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

`Dial` 用于建立控制连接和每一条数据流。`Control` 会作用于服务端的 TCP 和 UDP 监听 socket：如果它在 TCP socket 上返回错误，`Listen` 会返回该错误；如果在 UDP socket 上失败，服务端只提供 TCP，`UDP()` 返回 false。

## API 参考

### `ClientConfig`

| 字段 | 类型 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `Hosts` | `[]string` | 必填 | 服务端地址（IP 或域名）。所有地址同时尝试，先握手成功的胜出 |
| `Port` | `int` | `5201` | 服务端端口 |
| `Protocol` | `Protocol` | `TCP` | `TCP` 或 `UDP` |
| `Direction` | `Direction` | `Upload` | `Upload`、`Download`（反向）或 `Bidir` |
| `Parallel` | `int` | `1` | 每个方向的流数量，1–128 |
| `Duration` | `time.Duration` | `10s` | 测试时长，向上取整到秒，最长 24 小时 |
| `Bandwidth` | `float64` | `0` | 每个方向的目标速率，单位 bit/s，平均分到各条流；`0` 表示不限速 |
| `Dial` | `DialFunc` | `net.Dialer` | 建立控制连接和数据连接 |
| `OnUpdate` | `func(State)` | `nil` | 每次状态变化时调用，运行期间约每 250 毫秒调用一次 |

### `ServerConfig`

| 字段 | 类型 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `Addr` | `string` | `":5201"` | 监听地址；端口写 `0` 表示随机端口 |
| `Control` | `func(network, address string, c syscall.RawConn) error` | `nil` | 作用于监听 socket |
| `OnUpdate` | `func(State)` | `nil` | 每次测试的进度和结果 |

`*Server` 提供 `Port() int`、`UDP() bool`（UDP socket 是否在监听）和 `Close() error`。

### `State`

| 字段 | 含义 |
| --- | --- |
| `Role` | `RoleClient` 或 `RoleServer` |
| `Session` | 服务端：测试序号，从 1 开始；客户端：恒为 0 |
| `Phase` | `PhaseConnecting`、`PhaseRunning` 或 `PhaseDone` |
| `Peer` | 对端地址 |
| `Protocol`、`Direction`、`Parallel` | 测试参数 |
| `Duration`、`Elapsed` | 计划时长和已用时长，单位秒 |
| `Upload`、`Download` | 运行期间的实时速率，单位 bit/s |
| `UploadResult`、`DownloadResult` | 每个被测方向的最终结果 `*Flow` |
| `Error` | 成功时为空，否则为下方的错误码之一 |

无论在客户端还是服务端读取，上传都指客户端 → 服务端，下载都指服务端 → 客户端。

### `Flow`

| 字段 | 单位 | 含义 |
| --- | --- | --- |
| `Bytes` | 字节 | 传输的数据量 |
| `Bitrate` | bit/s | 平均速率 |
| `Retransmits` | 次 | TCP 重传次数；发送端平台无法提供时为 `-1` |
| `RTTMin`、`RTTMean`、`RTTMax` | 毫秒 | TCP 往返时间；不可用时为 `0` |
| `Jitter` | 毫秒 | UDP 抖动 |
| `Lost`、`Packets` | 个 | UDP 丢失的数据报数和应收数据报数 |

`State` 和 `Flow` 都带有 JSON 标签，可以直接序列化后发给界面。

### 错误码

| 错误码 | 含义 |
| --- | --- |
| `busy` | 服务端正在进行其他测试 |
| `refused` | 该端口没有服务在监听 |
| `timeout` | 连接或握手超时 |
| `unreachable` | 其他网络错误 |
| `udp` | 无法建立 UDP 数据流（通常被防火墙拦截） |
| `protocol` | 对端不是 iPerf3 服务，或发送了无效数据 |
| `interrupted` | 对端提前结束了测试 |
| `cancelled` | context 被取消 |
| `server:<n>` | 服务端拒绝了测试，`n` 为 iPerf3 的错误编号 |

## 兼容性

每次提交，CI 都会在 Linux 和 macOS 上与官方 iperf3 3.x 做互通测试，见 `TestOfficialClient` 和 `TestOfficialServer`。

| | 客户端（go-iperf3 → iperf3） | 服务端（iperf3 → go-iperf3） |
| --- | --- | --- |
| TCP / UDP | ✓ | ✓ |
| `-R`、`--bidir`、`-P` | ✓ | ✓ |
| `-b` 速率 | ✓ | ✓ |
| `-O` 忽略开头 | — | ✓ |
| `-n` / `-k` 按字节数或块数 | — | ✓ |
| `-w` 窗口、`-N` 禁用 Nagle、`-l` 块大小 | — | ✓ |
| `--udp-counters-64bit` | — | ✓ |
| `--get-server-output` | — | ✓ |
| SCTP、认证、`--cport`、`-Z` | ✗ | ✗ |

客户端的 TCP 每次固定写入 128 KB，UDP 使用与 MSS 相当的数据报大小。标记为"—"的选项客户端没有提供，但官方客户端发来这些选项时，服务端会遵守。

有两处默认行为刻意与官方 iperf3 不同：客户端的速率上限是每个方向的总速率，而不是每条流的速率；UDP 在未设定速率时不限速（官方 iperf3 默认 1 Mbit/s）。

### 平台说明

| 平台 | 说明 |
| --- | --- |
| Linux、Android | TCP 重传和 RTT 来自 `TCP_INFO`；UDP 使用内核接收时间戳 |
| macOS | TCP 重传和 RTT 来自 `TCP_CONNECTION_INFO` |
| Windows | TCP、UDP 全方向可用。不提供 TCP 重传和 RTT（分别为 `-1` 和 `0`） |
| 其他 | 凡是 Go 和 `x/sys` 能编译的平台都可以使用，但不提供 TCP 统计 |

## 开发

```sh
go vet ./...
go test ./...                     # PATH 中有 iperf3 时会运行互通测试
go test -run '^$' -bench . -benchtime 1x
```

## 许可证

[MIT](LICENSE)
