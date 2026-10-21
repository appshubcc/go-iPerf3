package main

import "testing"

func TestParseBitrate(t *testing.T) {
	cases := map[string]float64{"": 0, "0": 0, "800": 800, "10K": 1e4, "100m": 1e8, "1.5G": 1.5e9}
	for input, want := range cases {
		if got, err := parseBitrate(input); err != nil || got != want {
			t.Fatalf("parseBitrate(%q) = %v, %v; want %v", input, got, err, want)
		}
	}
	for _, input := range []string{"M", "-1M", "12x", "fast"} {
		if _, err := parseBitrate(input); err == nil {
			t.Fatalf("parseBitrate(%q) should fail", input)
		}
	}
}
