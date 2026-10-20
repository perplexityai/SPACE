package main

import "testing"

func TestDatadogTraceAgentAddrHonorsTraceAgentPort(t *testing.T) {
	cases := []struct {
		name, host, port, want string
	}{
		{"default port", "10.0.0.5", "", "10.0.0.5:8126"},
		{"configured port", "10.0.0.5", "18126", "10.0.0.5:18126"},
		{"surrounding space", "10.0.0.5", " 9126 ", "10.0.0.5:9126"},
		{"ipv6 host", "fd00::5", "18126", "[fd00::5]:18126"},
		{"bracketed ipv6 host", "[fd00::5]", "18126", "[fd00::5]:18126"},
		{"bracketed ipv6 zone", "[fe80::5%eth0]", "18126", "[fe80::5%eth0]:18126"},
		{"non-numeric port", "10.0.0.5", "apm", "10.0.0.5:8126"},
		{"zero port", "10.0.0.5", "0", "10.0.0.5:8126"},
		{"out of range port", "10.0.0.5", "70000", "10.0.0.5:8126"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := datadogTraceAgentAddr(tc.host, tc.port); got != tc.want {
				t.Fatalf("datadogTraceAgentAddr(%q, %q) = %q, want %q", tc.host, tc.port, got, tc.want)
			}
		})
	}
}
