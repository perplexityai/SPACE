package main

import (
	"bytes"
	"context"
	"flag"
	taskAPI "github.com/containerd/containerd/api/runtime/task/v2"
	"google.golang.org/protobuf/proto"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMetricsSinkSelection(t *testing.T) {
	for _, test := range []struct {
		raw  string
		want metricsSinkChoice
	}{
		{"<unset>", metricsDatadog}, {"auto", metricsDatadog}, {" DaTaDoG\t", metricsDatadog},
		{" OTLP ", metricsOTLP}, {"off", metricsOff}, {"", ""}, {"typo", ""}, {"journald", ""},
	} {
		t.Run(test.raw, func(t *testing.T) {
			t.Setenv("SPACE_METRICS_SINK", test.raw)
			if test.raw == "<unset>" {
				if err := os.Unsetenv("SPACE_METRICS_SINK"); err != nil {
					t.Fatal(err)
				}
			}
			got, err := configuredMetricsSink()
			if got != test.want || (err != nil) != (test.want == "") {
				t.Fatalf("choice=%q err=%v, want %q", got, err, test.want)
			}
		})
	}
}

func TestMetricsOffAndInitializationFailure(t *testing.T) {
	if os.Getenv("SPACE_METRICS_INIT_TEST") == "1" {
		for range 2 {
			emitCounter(shimOperationErrorMetric, 1, "op:create")
			emitTiming(shimOperationLatencyMetric, time.Now(), "op:create")
		}
		if _, ok := telemetry().(disabledMetricsSink); !ok {
			t.Fatal("metrics were not disabled")
		}
		stopTelemetry()
		return
	}
	for _, selector := range []string{"off", "otlp"} {
		t.Run(selector, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestMetricsOffAndInitializationFailure$")
			for _, item := range os.Environ() {
				if !strings.HasPrefix(item, "OTEL_") && !strings.HasPrefix(item, "SPACE_METRICS_") {
					cmd.Env = append(cmd.Env, item)
				}
			}
			cmd.Env = append(cmd.Env, "SPACE_METRICS_INIT_TEST=1", "SPACE_METRICS_SINK="+selector,
				"OTEL_EXPORTER_OTLP_HEADERS=authorization=fixture-secret")
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("%v\n%s", err, output)
			}
			warnings := 0
			if selector == "otlp" {
				warnings = 1
			}
			if bytes.Count(output, []byte("metrics sink initialization failed")) != warnings || bytes.Contains(output, []byte("fixture-secret")) {
				t.Fatalf("unexpected initialization output: %s", output)
			}
		})
	}
}

func TestDatadogMetricsShutdownFlushes(t *testing.T) {
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	t.Setenv("SPACE_METRICS_SINK", "auto")
	t.Setenv("DD_DOGSTATSD_SOCKET", "")
	t.Setenv("DD_AGENT_HOST", "127.0.0.1")
	t.Setenv("DD_DOGSTATSD_PORT", strconv.Itoa(listener.LocalAddr().(*net.UDPAddr).Port))
	// OTLP-only validation must not affect the default DogStatsD path.
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "authorization=fixture-secret")
	sink, err := newMetricsSink()
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.count("space.shim.operation.error", 3, []string{"op:create"}); err != nil {
		t.Fatal(err)
	}
	if err := sink.distribution("space.shim.operation.latency_ms", 12.5, []string{"op:create"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), telemetryShutdownBudget)
	defer cancel()
	if err := sink.shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if !sink.(*datadogMetricsSink).client.IsClosed() {
		t.Fatal("DogStatsD socket was not closed")
	}
	if err := listener.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	wire := ""
	for !strings.Contains(wire, "space.shim.operation.error:3|c") || !strings.Contains(wire, "space.shim.operation.latency_ms:12.5|d") {
		packet := make([]byte, 8192)
		n, err := listener.Read(packet)
		if err != nil {
			t.Fatalf("pending metrics not flushed: %v; packets=%q", err, wire)
		}
		wire += string(packet[:n])
	}
}

func TestIsValidClusterID(t *testing.T) {
	valid := []string{"prod-1", "prod-2", "testing", "p2", "a", "0", "a-b-c-9", strings.Repeat("a", maxClusterIDBytes)}
	for _, s := range valid {
		if !isValidClusterID(s) {
			t.Errorf("isValidClusterID(%q) = false, want true", s)
		}
	}
	invalid := []string{
		"",                                       // empty
		"Prod-2",                                 // uppercase
		"prod_2",                                 // underscore
		"prod.2",                                 // dot
		"prod 2",                                 // space
		"prod-1,region:foo",                      // comma — would inject a spurious DogStatsD tag
		"-prod",                                  // leading hyphen
		"prod-",                                  // trailing hyphen
		"prod/2",                                 // slash
		strings.Repeat("a", maxClusterIDBytes+1), // too long
	}
	for _, s := range invalid {
		if isValidClusterID(s) {
			t.Errorf("isValidClusterID(%q) = true, want false", s)
		}
	}
}

// A metrics selection that can never work must fail at bootstrap, so the
// mistake surfaces on the shim's own terms rather than as silent blindness.
func TestMetricsConfigurationValidation(t *testing.T) {
	const secret = "fixture-endpoint-secret"
	for _, tc := range []struct {
		name     string
		env      map[string]string
		wantErr  string
		wantPass bool
	}{
		{"unset is auto", nil, "", true},
		{"auto", map[string]string{"SPACE_METRICS_SINK": "auto"}, "", true},
		{"datadog", map[string]string{"SPACE_METRICS_SINK": "datadog"}, "", true},
		{"off", map[string]string{"SPACE_METRICS_SINK": "off"}, "", true},
		{
			"removed journald selector",
			map[string]string{"SPACE_METRICS_SINK": "journald"},
			"SPACE_METRICS_SINK", false,
		},
		{
			"unknown selector",
			map[string]string{"SPACE_METRICS_SINK": "bogus"},
			"SPACE_METRICS_SINK", false,
		},
		{
			"explicit otlp without an endpoint",
			map[string]string{"SPACE_METRICS_SINK": "otlp"},
			"OTLP endpoint", false,
		},
		{
			"explicit otlp with a non-plaintext endpoint",
			map[string]string{
				"SPACE_METRICS_SINK":                  "otlp",
				"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "https://receiver:4317",
			},
			"plaintext", false,
		},
		{
			"explicit otlp with a malformed endpoint",
			map[string]string{
				"SPACE_METRICS_SINK":                  "otlp",
				"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "http://user:" + secret + "@receiver:badport",
			},
			"", false,
		},
		{
			// A valid endpoint nothing is listening on is a runtime condition.
			"explicit otlp with an unreachable endpoint",
			map[string]string{
				"SPACE_METRICS_SINK":                  "otlp",
				"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "http://127.0.0.1:1",
			},
			"", true,
		},
		{
			"explicit otlp with an unusable cadence",
			map[string]string{
				"SPACE_METRICS_SINK":                  "otlp",
				"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "http://127.0.0.1:4317",
				"OTEL_METRIC_EXPORT_INTERVAL":         "10s",
			},
			"OTEL_METRIC_EXPORT_INTERVAL", false,
		},
		{
			// The default path must not read an OTLP-only setting, so a value
			// only OTLP would reject cannot fail a DogStatsD deployment.
			"datadog ignores the OTLP cadence",
			map[string]string{
				"SPACE_METRICS_SINK":          "datadog",
				"OTEL_METRIC_EXPORT_INTERVAL": "10s",
			},
			"", true,
		},
		{
			"off ignores the OTLP cadence",
			map[string]string{
				"SPACE_METRICS_SINK":          "off",
				"OTEL_METRIC_EXPORT_INTERVAL": "10s",
			},
			"", true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, key := range []string{
				"SPACE_METRICS_SINK",
				"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
				"OTEL_EXPORTER_OTLP_ENDPOINT",
				"OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
				"OTEL_EXPORTER_OTLP_PROTOCOL",
				"OTEL_EXPORTER_OTLP_METRICS_HEADERS",
				"OTEL_EXPORTER_OTLP_HEADERS",
				"OTEL_METRIC_EXPORT_INTERVAL",
			} {
				// t.Setenv registers the restore; unset for real afterwards so
				// "absent" is distinguishable from "present but blank", which
				// the shim rejects.
				t.Setenv(key, tc.env[key])
				if _, present := tc.env[key]; !present {
					if err := os.Unsetenv(key); err != nil {
						t.Fatal(err)
					}
				}
			}
			err := validateMetricsConfiguration()
			if tc.wantPass {
				if err != nil {
					t.Fatalf("valid configuration rejected: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("unusable configuration accepted")
			}
			if tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error must name the setting, got %v", err)
			}
			// The diagnostic names the setting; it never echoes credentials.
			if strings.Contains(err.Error(), secret) {
				t.Errorf("error leaked a configured value: %v", err)
			}
		})
	}
}

// An unreachable Collector must cost the shim its metrics, never a sandbox.
func TestUnreachableMetricsEndpointOnlyWarns(t *testing.T) {
	t.Setenv("SPACE_METRICS_SINK", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "grpc")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_HEADERS", "")
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "")
	if err := validateMetricsConfiguration(); err != nil {
		t.Fatalf("an unreachable receiver must not fail bootstrap: %v", err)
	}
	sink, err := newMetricsSink()
	if err != nil {
		t.Fatalf("sink construction must not fail: %v", err)
	}
	if err := sink.count("space.shim.operation.error", 1, nil); err != nil {
		t.Errorf("recording must not fail against an unreachable receiver: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), telemetryShutdownBudget)
	defer cancel()
	_ = sink.shutdown(ctx)
}

// Containerd's start must refuse an unusable metrics selection, while its
// separate delete still returns the dead shim's cleanup response.
func TestInvalidMetricsSinkFailsBootstrap(t *testing.T) {
	if action := os.Getenv("SPACE_METRICS_DISPATCH_TEST"); action != "" {
		flag.CommandLine = flag.NewFlagSet("shim", flag.ExitOnError)
		os.Args = []string{"shim", "-namespace", "metrics-test", "-id", "fixture", action}
		main()
		os.Exit(0)
	}
	for _, settings := range [][]string{
		{"SPACE_METRICS_SINK=bogus"},
		{"SPACE_METRICS_SINK=journald"},
		{"SPACE_METRICS_SINK=otlp"},
		{"SPACE_METRICS_SINK=otlp", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=https://receiver:4317"},
	} {
		for _, action := range []string{"start", "delete"} {
			t.Run(action+"/"+strings.Join(settings, "/"), func(t *testing.T) {
				cmd := exec.Command(os.Args[0], "-test.run=^TestInvalidMetricsSinkFailsBootstrap$")
				for _, item := range os.Environ() {
					if !strings.HasPrefix(item, "DD_") && !strings.HasPrefix(item, "OTEL_") &&
						!strings.HasPrefix(item, "SPACE_TRACE_") && !strings.HasPrefix(item, "SPACE_METRICS_") {
						cmd.Env = append(cmd.Env, item)
					}
				}
				cmd.Env = append(cmd.Env, settings...)
				cmd.Env = append(cmd.Env, "SPACE_METRICS_DISPATCH_TEST="+action)
				var stderr bytes.Buffer
				cmd.Stderr = &stderr
				output, err := cmd.Output()
				if action == "start" {
					if err == nil || !strings.Contains(stderr.String(), "invalid metrics configuration") {
						t.Fatalf("startup did not reject config: %v %s", err, &stderr)
					}
					return
				}
				if err != nil {
					t.Fatalf("cleanup blocked: %v %s", err, &stderr)
				}
				var response taskAPI.DeleteResponse
				if err := proto.Unmarshal(output, &response); err != nil || response.ExitedAt == nil {
					t.Fatalf("invalid cleanup response: %v %v", &response, err)
				}
			})
		}
	}
}

// A bad cadence must stop bootstrap, not surface at the first export.
func TestBootstrapRejectsAnUnusableExportInterval(t *testing.T) {
	t.Setenv("SPACE_METRICS_SINK", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://127.0.0.1:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "grpc")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_HEADERS", "")
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "")
	t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "0")
	if err := validateMetricsConfiguration(); err == nil {
		t.Fatal("bootstrap accepted a zero cadence")
	}
	if _, err := newMetricsSink(); err == nil {
		t.Fatal("sink construction accepted a zero cadence")
	}
}
