package iperf3

import (
	"fmt"
	"strings"
)

func buildFlow(streams []*testStream, sender bool, peer map[int]streamResult, peerRetrans bool, elapsed float64) *Flow {
	flow := &Flow{Retransmits: -1}
	var jitter, rttMean float64
	count, rtts := 0, 0
	addRTT := func(result streamResult) {
		if result.MaxRTT <= 0 {
			return
		}
		low, high := float64(result.MinRTT)/1000, float64(result.MaxRTT)/1000
		if rtts == 0 || low < flow.RTTMin {
			flow.RTTMin = low
		}
		if high > flow.RTTMax {
			flow.RTTMax = high
		}
		rttMean += float64(result.MeanRTT) / 1000
		rtts++
	}
	for _, stream := range streams {
		if stream.sender != sender {
			continue
		}
		count++
		local := stream.result(elapsed)
		remote, ok := peer[stream.id]
		if sender {
			bytes := local.Bytes
			if ok {
				bytes = remote.Bytes
			}
			flow.Bytes += bytes
			if local.Retransmits >= 0 {
				if flow.Retransmits < 0 {
					flow.Retransmits = 0
				}
				flow.Retransmits += local.Retransmits
			}
			addRTT(local)
			if stream.udp && ok {
				sent := local.Packets - local.OmittedPackets
				seen := remote.Packets - remote.OmittedPackets
				lost := remote.Errors - remote.OmittedErrors
				// Like iperf3, loss counts only gaps in the received sequence numbers,
				// so datagrams still in flight at the end are not lost. When nothing
				// arrived at all, every datagram sent is reported as lost.
				if seen <= 0 && sent > 0 {
					lost, seen = sent, sent
				}
				flow.Lost += lost
				flow.Packets += seen
				jitter += remote.Jitter
			}
			continue
		}
		flow.Bytes += local.Bytes
		if ok && peerRetrans && remote.Retransmits >= 0 {
			if flow.Retransmits < 0 {
				flow.Retransmits = 0
			}
			flow.Retransmits += remote.Retransmits
		}
		if ok && !stream.udp {
			addRTT(remote)
		}
		if stream.udp {
			seen := local.Packets - local.OmittedPackets
			lost := local.Errors - local.OmittedErrors
			if ok {
				if sent := remote.Packets - remote.OmittedPackets; seen <= 0 && sent > 0 {
					lost, seen = sent, sent
				}
			}
			flow.Lost += lost
			flow.Packets += seen
			jitter += local.Jitter
		}
	}
	if count == 0 {
		return nil
	}
	if flow.Lost < 0 {
		flow.Lost = 0
	}
	flow.Jitter = jitter / float64(count) * 1000
	if rtts > 0 {
		flow.RTTMean = rttMean / float64(rtts)
	}
	if elapsed > 0 {
		flow.Bitrate = flow.Bytes * 8 / elapsed
	}
	return flow
}

func resultsOf(streams []*testStream, elapsed float64, udp bool) testResults {
	results := testResults{SenderHasRetransmits: -1, Streams: make([]streamResult, 0, len(streams))}
	hasSender := false
	for _, stream := range streams {
		hasSender = hasSender || stream.sender
		results.Streams = append(results.Streams, stream.result(elapsed))
	}
	if hasSender {
		results.SenderHasRetransmits = 0
		if !udp && retransmitsSupported {
			results.SenderHasRetransmits = 1
		}
	}
	return results
}

func peerResults(results testResults) map[int]streamResult {
	peer := make(map[int]streamResult, len(results.Streams))
	for _, stream := range results.Streams {
		peer[stream.ID] = stream
	}
	return peer
}

func formatBytes(value float64) string {
	units := []string{"Bytes", "KBytes", "MBytes", "GBytes", "TBytes"}
	index := 0
	for value >= 1024 && index < len(units)-1 {
		value /= 1024
		index++
	}
	return fmt.Sprintf("%.2f %s", value, units[index])
}

func formatBits(value float64) string {
	units := []string{"bits/sec", "Kbits/sec", "Mbits/sec", "Gbits/sec"}
	index := 0
	for value >= 1000 && index < len(units)-1 {
		value /= 1000
		index++
	}
	return fmt.Sprintf("%.2f %s", value, units[index])
}

func serverOutput(streams []*testStream, elapsed float64) string {
	var builder strings.Builder
	builder.WriteString("go-iperf3 server\n[ ID][Role] Interval           Transfer     Bitrate\n")
	for _, stream := range streams {
		role := "RX-S"
		if stream.sender {
			role = "TX-S"
		}
		bytes := float64(stream.bytes.Load() - stream.omitBytes.Load())
		bitrate := 0.0
		if elapsed > 0 {
			bitrate = bytes * 8 / elapsed
		}
		fmt.Fprintf(&builder, "[%3d][%s]   0.00-%.2f  sec  %s  %s\n", stream.id, role, elapsed, formatBytes(bytes), formatBits(bitrate))
	}
	return builder.String()
}
