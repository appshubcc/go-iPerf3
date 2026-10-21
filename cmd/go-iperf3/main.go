// Command go-iperf3 is an iperf3-compatible client and server built on the
// go-iperf3 library. Its flags follow the official iperf3 tool.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	iperf3 "github.com/appshubcc/go-iperf3"
)

type options struct {
	server    bool
	once      bool
	host      string
	bind      string
	port      int
	udp       bool
	reverse   bool
	bidir     bool
	parallel  int
	seconds   int
	bandwidth string
	json      bool
}

func main() {
	opts := options{}
	flags := flag.NewFlagSet("go-iperf3", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	flags.BoolVar(&opts.server, "s", false, "run in server mode")
	flags.BoolVar(&opts.once, "1", false, "server: exit after one test")
	flags.StringVar(&opts.host, "c", "", "client: connect to `host`")
	flags.StringVar(&opts.bind, "B", "", "server: bind to `address`")
	flags.IntVar(&opts.port, "p", iperf3.DefaultPort, "server port")
	flags.BoolVar(&opts.udp, "u", false, "client: use UDP")
	flags.BoolVar(&opts.reverse, "R", false, "client: server sends, client receives")
	flags.BoolVar(&opts.bidir, "bidir", false, "client: test both directions at once")
	flags.IntVar(&opts.parallel, "P", 1, "client: number of parallel streams")
	flags.IntVar(&opts.seconds, "t", 10, "client: test length in seconds")
	flags.StringVar(&opts.bandwidth, "b", "", "client: target bitrate, e.g. 100M (0 = unlimited)")
	flags.BoolVar(&opts.json, "J", false, "print the final result as JSON")
	if err := flags.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	switch {
	case opts.server:
		err = serve(ctx, opts)
	case opts.host != "":
		err = connect(ctx, opts)
	default:
		flags.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "go-iperf3:", err)
		os.Exit(1)
	}
}

func parseBitrate(value string) (float64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	scale := 1.0
	switch strings.ToUpper(value[len(value)-1:]) {
	case "K":
		scale = 1e3
	case "M":
		scale = 1e6
	case "G":
		scale = 1e9
	}
	if scale != 1 {
		value = value[:len(value)-1]
	}
	number, err := strconv.ParseFloat(value, 64)
	if err != nil || number < 0 {
		return 0, fmt.Errorf("invalid bitrate %q", value)
	}
	return number * scale, nil
}

func formatBits(value float64) string {
	units := []string{"bits/sec", "Kbits/sec", "Mbits/sec", "Gbits/sec"}
	index := 0
	for value >= 1000 && index < len(units)-1 {
		value /= 1000
		index++
	}
	return fmt.Sprintf("%7.2f %s", value, units[index])
}

func formatBytes(value float64) string {
	units := []string{"Bytes", "KBytes", "MBytes", "GBytes", "TBytes"}
	index := 0
	for value >= 1024 && index < len(units)-1 {
		value /= 1024
		index++
	}
	return fmt.Sprintf("%7.2f %s", value, units[index])
}

type printer struct {
	out    io.Writer
	mu     sync.Mutex
	second map[int]int
}

func newPrinter(out io.Writer) *printer {
	return &printer{out: out, second: map[int]int{}}
}

func (p *printer) progress(state iperf3.State) {
	if state.Phase != iperf3.PhaseRunning {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	second := int(state.Elapsed)
	if second <= p.second[state.Session] {
		return
	}
	p.second[state.Session] = second
	line := fmt.Sprintf("[%4ds]", second)
	if state.Direction != iperf3.Download {
		line += "  upload " + formatBits(state.Upload)
	}
	if state.Direction != iperf3.Upload {
		line += "  download " + formatBits(state.Download)
	}
	fmt.Fprintln(p.out, line)
}

func (p *printer) summary(state iperf3.State) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if state.Error != "" {
		fmt.Fprintf(p.out, "test failed: %s\n", state.Error)
		return
	}
	fmt.Fprintf(p.out, "- - - - - - - - - - - - - - - - - - - - - - - - -\n%s %s, %d stream(s), %.2f sec\n", strings.ToUpper(string(state.Protocol)), state.Direction, state.Parallel, state.Elapsed)
	for _, item := range []struct {
		name string
		flow *iperf3.Flow
	}{{"upload  ", state.UploadResult}, {"download", state.DownloadResult}} {
		flow := item.flow
		if flow == nil {
			continue
		}
		line := fmt.Sprintf("%s %s  %s", item.name, formatBytes(flow.Bytes), formatBits(flow.Bitrate))
		if state.Protocol == iperf3.UDP {
			loss := 0.0
			if flow.Packets > 0 {
				loss = float64(flow.Lost) / float64(flow.Packets) * 100
			}
			line += fmt.Sprintf("  jitter %.3f ms  lost %d/%d (%.2f%%)", flow.Jitter, flow.Lost, flow.Packets, loss)
		} else {
			if flow.Retransmits >= 0 {
				line += fmt.Sprintf("  retr %d", flow.Retransmits)
			}
			if flow.RTTMax > 0 {
				line += fmt.Sprintf("  rtt %.2f/%.2f/%.2f ms", flow.RTTMin, flow.RTTMean, flow.RTTMax)
			}
		}
		fmt.Fprintln(p.out, line)
	}
}

func printJSON(state iperf3.State) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(state)
}

func connect(ctx context.Context, opts options) error {
	bandwidth, err := parseBitrate(opts.bandwidth)
	if err != nil {
		return err
	}
	cfg := iperf3.ClientConfig{
		Hosts:     []string{opts.host},
		Port:      opts.port,
		Protocol:  iperf3.TCP,
		Direction: iperf3.Upload,
		Parallel:  opts.parallel,
		Duration:  time.Duration(opts.seconds) * time.Second,
		Bandwidth: bandwidth,
	}
	if opts.udp {
		cfg.Protocol = iperf3.UDP
	}
	switch {
	case opts.bidir:
		cfg.Direction = iperf3.Bidir
	case opts.reverse:
		cfg.Direction = iperf3.Download
	}
	out := newPrinter(os.Stdout)
	if !opts.json {
		fmt.Printf("Connecting to %s, port %d\n", opts.host, opts.port)
		cfg.OnUpdate = out.progress
	}
	state := iperf3.RunClient(ctx, cfg)
	if opts.json {
		if err := printJSON(state); err != nil {
			return err
		}
	} else {
		out.summary(state)
	}
	if state.Error != "" {
		return errors.New(state.Error)
	}
	return nil
}

func serve(ctx context.Context, opts options) error {
	out := newPrinter(os.Stdout)
	done := make(chan iperf3.State, 1)
	server, err := iperf3.Listen(iperf3.ServerConfig{
		Addr: net.JoinHostPort(opts.bind, strconv.Itoa(opts.port)),
		OnUpdate: func(state iperf3.State) {
			if state.Phase != iperf3.PhaseDone {
				if !opts.json {
					out.progress(state)
				}
				return
			}
			if opts.json {
				_ = printJSON(state)
			} else {
				out.summary(state)
			}
			select {
			case done <- state:
			default:
			}
		},
	})
	if err != nil {
		return err
	}
	defer server.Close()
	if !opts.json {
		transports := "tcp"
		if server.UDP() {
			transports += "+udp"
		}
		fmt.Printf("Server listening on %d (%s)\n", server.Port(), transports)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-done:
			if opts.once {
				return nil
			}
		}
	}
}
