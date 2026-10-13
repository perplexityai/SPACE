package main

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	collector "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
)

func TestOTLPMetricsTransportConfiguration(t *testing.T) {
	for _, test := range []struct {
		name     string
		settings map[string]string
		endpoint string
		errorKey string
	}{
		{"required endpoint", nil, "", "missing OTLP endpoint"},
		{"generic endpoint", map[string]string{"ENDPOINT": " http://generic:4317\t"}, "http://generic:4317", ""},
		{"signal precedence", map[string]string{"ENDPOINT": "http://generic:4317", "METRICS_ENDPOINT": "http://metrics:4317", "PROTOCOL": "http/protobuf", "METRICS_PROTOCOL": " GrPc "}, "http://metrics:4317", ""},
		{"blank signal fallback", map[string]string{"ENDPOINT": "http://generic:4317", "METRICS_ENDPOINT": " \t", "HEADERS": "\t", "METRICS_HEADERS": " "}, "http://generic:4317", ""},
		{"https", map[string]string{"ENDPOINT": "https://receiver:4317"}, "", "plaintext"},
		{"bare host", map[string]string{"ENDPOINT": "receiver:4317"}, "", "plaintext"},
		{"userinfo", map[string]string{"ENDPOINT": "http://user:fixture-secret@receiver:4317"}, "", "must not embed credentials"},
		// Credentials outrank the unreadable port, as they do in Rust, and the
		// message still carries neither.
		{"malformed userinfo URL", map[string]string{"ENDPOINT": "http://user:fixture-secret@receiver:badport"}, "", "must not embed credentials"},
		{"missing host", map[string]string{"ENDPOINT": "http://:"}, "", "must include a host"},
		{"unusable port", map[string]string{"ENDPOINT": "http://receiver:0"}, "", "must use a port between 1 and 65535"},
		// The http/protobuf convention puts the signal in the path. Pasting one
		// of those addresses into the grpc profile has to say so.
		{"signal path", map[string]string{"ENDPOINT": "http://receiver:4318/v1/metrics"}, "", "must not include a path"},
		{"query and fragment", map[string]string{"ENDPOINT": "http://receiver:4317/path?q#f"}, "", "must not include a query or fragment"},
		// url.Parse reports an empty or unreadable port the same way it reports
		// none, so each of these once looked like a bare host.
		{"port above range", map[string]string{"ENDPOINT": "http://receiver:65536"}, "", "must use a port between 1 and 65535"},
		{"non-numeric port", map[string]string{"ENDPOINT": "http://receiver:abc"}, "", "must use a port between 1 and 65535"},
		{"empty port", map[string]string{"ENDPOINT": "http://receiver:"}, "", "must use a port between 1 and 65535"},
		// An empty query or fragment is still a query or fragment.
		{"empty query", map[string]string{"ENDPOINT": "http://receiver:4317?"}, "", "must not include a query or fragment"},
		{"empty fragment", map[string]string{"ENDPOINT": "http://receiver:4317#"}, "", "must not include a query or fragment"},
		{"ipv6 literal", map[string]string{"ENDPOINT": "http://[::1]:4317"}, "http://[::1]:4317", ""},
		{"no port", map[string]string{"ENDPOINT": "http://receiver"}, "http://receiver", ""},
		{"protocol", map[string]string{"ENDPOINT": "http://receiver:4317", "PROTOCOL": "http/protobuf"}, "", "require grpc"},
		{"generic headers", map[string]string{"HEADERS": "authorization=fixture-secret"}, "", "OTEL_EXPORTER_OTLP_HEADERS"},
		{"signal headers", map[string]string{"METRICS_HEADERS": "authorization=fixture-secret"}, "", "OTEL_EXPORTER_OTLP_METRICS_HEADERS"},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, key := range []string{"ENDPOINT", "METRICS_ENDPOINT", "PROTOCOL", "METRICS_PROTOCOL", "HEADERS", "METRICS_HEADERS"} {
				t.Setenv("OTEL_EXPORTER_OTLP_"+key, test.settings[key])
			}
			endpoint, err := metricsEndpoint()
			if endpoint != test.endpoint {
				t.Fatalf("endpoint=%q, want %q", endpoint, test.endpoint)
			}
			if test.errorKey == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.errorKey) || strings.Contains(err.Error(), "fixture-secret") {
				t.Fatalf("expected redacted error containing %q, got %v", test.errorKey, err)
			}
		})
	}
}

func TestOTLPMetricsResourceIdentity(t *testing.T) {
	for _, test := range []struct {
		name         string
		settings     map[string]string
		instanceHost string
		identity     map[string]string
	}{
		{"unknown host", nil, "unknown-host", nil},
		{"standalone host", map[string]string{"HOSTNAME": " worker ", "SPACE_NODE_ID": "fallback"}, "worker", map[string]string{"host.name": "worker"}},
		{"node ID fallback", map[string]string{"HOSTNAME": " \t", "SPACE_NODE_ID": " node-id "}, "node-id", nil},
		{"pod hostname is not a host", map[string]string{"HOSTNAME": "pod", "KUBERNETES_SERVICE_HOST": ""}, "pod", nil},
		{"explicit node and pod", map[string]string{"HOSTNAME": "pod", "SPACE_K8S_NODE_NAME": " node ", "KUBE_NODE_NAME": "fallback", "SPACE_K8S_POD_NAME": " pod-name "}, "node", map[string]string{"host.name": "node", "k8s.node.name": "node", "k8s.pod.name": "pod-name"}},
		{"Kubernetes node fallback", map[string]string{"HOSTNAME": "pod", "SPACE_K8S_NODE_NAME": " ", "KUBE_NODE_NAME": " kube-node "}, "kube-node", map[string]string{"host.name": "kube-node", "k8s.node.name": "kube-node"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, key := range []string{"HOSTNAME", "SPACE_NODE_ID", "SPACE_K8S_NODE_NAME", "KUBE_NODE_NAME", "SPACE_K8S_POD_NAME", "KUBERNETES_SERVICE_HOST"} {
				t.Setenv(key, test.settings[key])
			}
			if _, inKubernetes := test.settings["KUBERNETES_SERVICE_HOST"]; !inKubernetes {
				if err := os.Unsetenv("KUBERNETES_SERVICE_HOST"); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.instance.id=shared")
			res, err := otlpResource(metricResourceAttributes()...)
			if err != nil {
				t.Fatal(err)
			}
			instance, _ := res.Set().Value("service.instance.id")
			if want := fmt.Sprintf("%s:%d", test.instanceHost, os.Getpid()); instance.AsString() != want {
				t.Fatalf("instance=%q, want %q", instance.AsString(), want)
			}
			for _, key := range []string{"host.name", "k8s.node.name", "k8s.pod.name"} {
				value, exists := res.Set().Value(attribute.Key(key))
				want, wantExists := test.identity[key]
				if exists != wantExists || value.AsString() != want {
					t.Errorf("identity %s=%v (present=%t), want %q (present=%t)", key, value, exists, want, wantExists)
				}
			}
		})
	}
}

type metricsRecordingReceiver struct {
	collector.UnimplementedMetricsServiceServer
	requests chan *collector.ExportMetricsServiceRequest
}

func (r *metricsRecordingReceiver) Export(_ context.Context, request *collector.ExportMetricsServiceRequest) (*collector.ExportMetricsServiceResponse, error) {
	r.requests <- request
	return &collector.ExportMetricsServiceResponse{}, nil
}

func startMetricsReceiver(t *testing.T) (*metricsRecordingReceiver, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	receiver := &metricsRecordingReceiver{requests: make(chan *collector.ExportMetricsServiceRequest, 16)}
	server := grpc.NewServer()
	collector.RegisterMetricsServiceServer(server, receiver)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return receiver, "http://" + listener.Addr().String()
}

// receiveScope returns the next export carrying instruments under scope. The
// shim's own metrics and the Go runtime metrics leave on separate providers,
// so a flush of both arrives as separate requests in either order.
func receiveScope(t *testing.T, receiver *metricsRecordingReceiver, scope string) *collector.ExportMetricsServiceRequest {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case request := <-receiver.requests:
			for _, rm := range request.ResourceMetrics {
				for _, scoped := range rm.ScopeMetrics {
					if scoped.Scope.GetName() == scope {
						return request
					}
				}
			}
		case <-deadline:
			t.Fatalf("no OTLP export under scope %q received", scope)
			return nil
		}
	}
}

func TestOTLPMetricsProcessIdentity(t *testing.T) {
	if os.Getenv("SPACE_METRICS_IDENTITY_TEST") == "1" {
		sink, err := newMetricsSink()
		if err != nil {
			t.Fatal(err)
		}
		if err := sink.count("space.shim.operation.error", 1, []string{"op:create"}); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), telemetryShutdownBudget)
		defer cancel()
		if err := sink.shutdown(ctx); err != nil {
			t.Fatal(err)
		}
		return
	}
	receiver, endpoint := startMetricsReceiver(t)
	for range 2 {
		cmd := exec.Command(os.Args[0], "-test.run=^TestOTLPMetricsProcessIdentity$")
		for _, item := range os.Environ() {
			if !strings.HasPrefix(item, "DD_") && !strings.HasPrefix(item, "OTEL_") && !strings.HasPrefix(item, "SPACE_") && !strings.HasPrefix(item, "KUBERNETES_SERVICE_HOST=") && !strings.HasPrefix(item, "KUBE_NODE_NAME=") && !strings.HasPrefix(item, "HOSTNAME=") {
				cmd.Env = append(cmd.Env, item)
			}
		}
		cmd.Env = append(cmd.Env, "SPACE_METRICS_IDENTITY_TEST=1", "SPACE_METRICS_SINK=otlp",
			"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT="+endpoint, "HOSTNAME=worker-fixture",
			"OTEL_RESOURCE_ATTRIBUTES=service.name=shared-service,service.instance.id=shared-instance,host.name=shared-host")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%v\n%s", err, output)
		}
		{
			request := receiveScope(t, receiver, shimMeterScope)
			if len(request.ResourceMetrics) != 1 {
				t.Fatalf("resource metrics: %v", request)
			}
			rm := request.ResourceMetrics[0]
			instance := fmt.Sprintf("worker-fixture:%d", cmd.Process.Pid)
			assertMetricAttributes(t, rm.Resource.Attributes, map[string]string{
				"service.name": "shared-service", "service.instance.id": instance, "host.name": "worker-fixture",
			})
			shimMetrics := shimScopeMetrics(t, rm)
			if len(shimMetrics) != 1 {
				t.Fatalf("metric streams: %v", rm)
			}
			metric := shimMetrics[0]
			if metric.Name != "space.shim.operation.error" || metric.GetSum().GetAggregationTemporality() != metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA {
				t.Fatalf("expected identical delta streams distinguished by resource: %v", metric)
			}
		}
	}
}

func TestOTLPMetricsWireProfileAndShutdown(t *testing.T) {
	receiver, endpoint := startMetricsReceiver(t)
	t.Setenv("SPACE_METRICS_SINK", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", endpoint)
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_HEADERS", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "grpc")
	// The fixed Rust-compatible profile takes precedence over SDK preferences.
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE", "cumulative")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION", "explicit_bucket_histogram")
	t.Setenv("DD_SERVICE", "legacy-service")
	t.Setenv("DD_ENV", "legacy-env")
	t.Setenv("DD_VERSION", "legacy-version")
	t.Setenv("SPACE_CLUSTER_ID", "test-cluster")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.name=resource-service,service.version=resource-version,deployment.environment.name=test,team=space")
	t.Setenv("OTEL_SERVICE_NAME", "explicit-service")
	sink, err := newMetricsSink()
	if err != nil {
		t.Fatal(err)
	}
	otlp := sink.(*otlpMetricsSink)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), telemetryShutdownBudget)
		defer cancel()
		_ = sink.shutdown(ctx)
	})
	for batch := range 2 {
		if err := sink.count("space.shim.operation.error", int64(batch+1), []string{"op:create", "result:error", "reason:grpc:unavailable", "grpc_code:Unavailable"}); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"space.shim.operation.latency_ms", "space.shim.operation.step.latency_ms"} {
			if err := sink.distribution(name, 12.5, []string{"op:create", "result:success", "step:restore"}); err != nil {
				t.Fatal(err)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), telemetryShutdownBudget)
		if batch == 0 {
			err = otlp.provider.ForceFlush(ctx)
		} else {
			// A stalled trace export must leave metrics time to flush under the
			// same shutdown deadline used by the shim's containerd lifecycle.
			t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://127.0.0.1:1")
			t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "grpc")
			t.Setenv("OTEL_EXPORTER_OTLP_TRACES_HEADERS", "")
			t.Setenv("OTEL_TRACES_SAMPLER", "always_on")
			tracer, err := newOTLPTracePipeline()
			if err != nil {
				t.Fatal(err)
			}
			span, _ := tracer.start(context.Background(), "shim.create")
			span.End(nil)
			previousTracer := activeTracer.Swap(&activeTracePipeline{tracer})
			previousMetrics := activeMetrics.Swap(&activeMetricsSink{sink})
			started := time.Now()
			stopTelemetry()
			activeTracer.Store(previousTracer)
			activeMetrics.Store(previousMetrics)
			if elapsed := time.Since(started); elapsed > telemetryShutdownBudget+300*time.Millisecond {
				t.Fatalf("shared shutdown exceeded its budget: %v", elapsed)
			}
		}
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		request := receiveScope(t, receiver, shimMeterScope)
		if len(request.ResourceMetrics) != 1 {
			t.Fatalf("resource metrics: %v", request)
		}
		rm := request.ResourceMetrics[0]
		assertMetricAttributes(t, rm.Resource.Attributes, map[string]string{
			"service.name": "explicit-service", "service.version": "resource-version", "deployment.environment.name": "test", "team": "space",
		})
		shimMetrics := shimScopeMetrics(t, rm)
		if len(shimMetrics) != 3 {
			t.Fatalf("missing metric streams: %v", rm)
		}
		for _, m := range shimMetrics {
			var attrs []*commonpb.KeyValue
			switch m.Name {
			case "space.shim.operation.error":
				sum := m.GetSum()
				if m.Unit != "1" || sum == nil || !sum.IsMonotonic || sum.AggregationTemporality != metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA || len(sum.DataPoints) != 1 || sum.DataPoints[0].GetAsInt() != int64(batch+1) {
					t.Fatalf("counter did not preserve delta count: %v", m)
				}
				attrs = sum.DataPoints[0].Attributes
				assertMetricAttributes(t, attrs, map[string]string{"result": "error", "reason": "grpc:unavailable", "grpc_code": "Unavailable"})
			// OTLP declares the unit, so the name drops the DogStatsD `_ms`.
			case "space.shim.operation.latency", "space.shim.operation.step.latency":
				hist := m.GetExponentialHistogram()
				if m.Unit != "ms" || hist == nil || hist.AggregationTemporality != metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA || len(hist.DataPoints) != 1 {
					t.Fatalf("latency did not preserve exponential delta histogram: %v", m)
				}
				point := hist.DataPoints[0]
				if point.Count != 1 || point.GetSum() != 12.5 || point.GetMin() != 12.5 || point.GetMax() != 12.5 || point.Scale > 8 || len(point.GetPositive().GetBucketCounts()) > 160 {
					t.Fatalf("unexpected histogram bounds or values: %v", point)
				}
				attrs = point.Attributes
				assertMetricAttributes(t, attrs, map[string]string{"result": "success", "step": "restore"})
			default:
				t.Fatalf("unexpected metric name: %s", m.Name)
			}
			assertMetricAttributes(t, attrs, map[string]string{"op": "create", "cluster_id": "test-cluster"})
			// env and version belong to the resource asserted above; duplicating
			// them here would give the Agent two ambiguous identity sources.
			assertAttributesAbsent(t, attrs, "env", "version")
		}
	}
}

func assertAttributesAbsent(t *testing.T, attrs []*commonpb.KeyValue, keys ...string) {
	t.Helper()
	for _, attr := range attrs {
		for _, key := range keys {
			if attr.Key == key {
				t.Errorf("attribute %q must stay on the resource, got %q", key, attr.Value.GetStringValue())
			}
		}
	}
}

func assertMetricAttributes(t *testing.T, attrs []*commonpb.KeyValue, want map[string]string) {
	t.Helper()
	values := make(map[string]string, len(attrs))
	for _, attr := range attrs {
		values[attr.Key] = attr.Value.GetStringValue()
	}
	for key, value := range want {
		if values[key] != value {
			t.Errorf("attribute %q=%q, want %q", key, values[key], value)
		}
	}
}

// The cadence is OpenTelemetry's own setting, and the Rust sink reads it the
// same way, so a Collector can be told to expect one interval from every SPACE
// producer.
func TestMetricExportIntervalFromEnvironment(t *testing.T) {
	for _, test := range []struct {
		name  string
		raw   string
		want  time.Duration
		fails bool
	}{
		{name: "unset", want: defaultExportInterval},
		{name: "blank", raw: " \t", want: defaultExportInterval},
		{name: "milliseconds", raw: "2500", want: 2500 * time.Millisecond},
		{name: "padded", raw: " 30000 ", want: 30 * time.Second},
		{name: "zero", raw: "0", fails: true},
		{name: "negative", raw: "-1000", fails: true},
		{name: "seconds", raw: "10s", fails: true},
		{name: "fractional", raw: "1.5", fails: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", test.raw)
			interval, err := metricExportInterval()
			if test.fails {
				if err == nil {
					t.Fatalf("interval=%v, want a rejected setting", interval)
				}
				if !strings.Contains(err.Error(), "OTEL_METRIC_EXPORT_INTERVAL") {
					t.Fatalf("error must name the setting, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if interval != test.want {
				t.Fatalf("interval=%v, want %v", interval, test.want)
			}
		})
	}
}

// runtimeScopeName is the contrib runtime instrumentation's scope. Both its
// callback instruments and its scheduler Producer report under this name.
const runtimeScopeName = "go.opentelemetry.io/contrib/instrumentation/runtime"

// shimScopeMetrics returns only the shim's own instruments. Runtime metrics
// share the resource under runtimeScopeName, so space.shim.* assertions must
// select by scope rather than assume the export carries a single scope.
func shimScopeMetrics(t *testing.T, rm *metricspb.ResourceMetrics) []*metricspb.Metric {
	t.Helper()
	for _, scope := range rm.ScopeMetrics {
		if scope.Scope.GetName() == shimMeterScope {
			return scope.Metrics
		}
	}
	t.Fatalf("no %q scope in %v", shimMeterScope, rm)
	return nil
}

func configureOTLPMetricsEnv(t *testing.T, endpoint string) {
	t.Helper()
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", endpoint)
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "grpc")
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_HEADERS", "")
}

// Runtime metrics must reach the collector through the shim's runtime
// provider, so an all-OTLP deployment gets the standard go.* set rather than
// nothing. It is narrower than Datadog's runtime metrics v2; see
// docs/installation.md.
func TestOTLPMetricsExportGoRuntimeInstruments(t *testing.T) {
	receiver, endpoint := startMetricsReceiver(t)
	t.Setenv("SPACE_METRICS_SINK", "otlp")
	configureOTLPMetricsEnv(t, endpoint)
	t.Setenv("HOSTNAME", "worker-fixture")
	sink, err := newMetricsSink()
	if err != nil {
		t.Fatal(err)
	}
	otlp := sink.(*otlpMetricsSink)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), telemetryShutdownBudget)
		defer cancel()
		_ = sink.shutdown(ctx)
	})
	if err := sink.count("space.shim.operation.error", 1, []string{"op:create"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := otlp.runtime.ForceFlush(ctx); err != nil {
		t.Fatal(err)
	}
	request := receiveScope(t, receiver, runtimeScopeName)
	if len(request.ResourceMetrics) != 1 {
		t.Fatalf("runtime metrics must share the shim resource: %v", request)
	}
	rm := request.ResourceMetrics[0]
	assertMetricAttributes(t, rm.Resource.Attributes, map[string]string{
		"service.instance.id": fmt.Sprintf("worker-fixture:%d", os.Getpid()),
	})
	scopeByMetric := make(map[string]string)
	metricByName := make(map[string]*metricspb.Metric)
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			scopeByMetric[m.Name] = scope.Scope.GetName()
			metricByName[m.Name] = m
		}
	}
	for _, name := range []string{"go.memory.used", "go.goroutine.count", "go.schedule.duration"} {
		if got := scopeByMetric[name]; got != runtimeScopeName {
			t.Errorf("runtime metric %q scope=%q, want %q", name, got, runtimeScopeName)
		}
	}
	// The scheduler histogram comes from a Producer, which bypasses the
	// temporality and exponential-aggregation selectors entirely.
	schedule := metricByName["go.schedule.duration"]
	if hist := schedule.GetHistogram(); hist == nil || hist.AggregationTemporality != metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE {
		t.Errorf("go.schedule.duration is not a cumulative explicit histogram: %v", schedule)
	}
	if schedule.GetExponentialHistogram() != nil {
		t.Errorf("Producer output unexpectedly honoured the exponential aggregation selector: %v", schedule)
	}
	assertScheduleHistogramSumIsConsistent(t, schedule)
}

// The producer derives Sum itself, so presence and temporality say nothing
// about whether the number is right.
//
// It approximates each observation at its bucket's lower bound, so Sum must
// equal the counts weighted by those bounds. Runtime v0.66.0 multiplied each
// bound by the running total instead of that bucket's count, which let every
// empty bucket above an observation inflate the sum: one observation under 1us
// in boundaries [0, 1us, 2us] reported 3us rather than 0. Counts and quantiles
// stayed usable, but any average derived from Sum did not.
func assertScheduleHistogramSumIsConsistent(t *testing.T, metric *metricspb.Metric) {
	t.Helper()
	hist := metric.GetHistogram()
	if hist == nil {
		t.Fatalf("go.schedule.duration is not an explicit histogram: %v", metric)
	}
	for _, point := range hist.DataPoints {
		bounds := point.ExplicitBounds
		var expected float64
		var counted uint64
		for index, count := range point.BucketCounts {
			counted += count
			// Bucket 0 is everything below the first boundary, whose lower
			// bound the runtime reports as zero for this instrument.
			if index > 0 && index-1 < len(bounds) {
				expected += bounds[index-1] * float64(count)
			}
		}
		if counted != point.GetCount() {
			t.Errorf("bucket counts sum to %d, but Count is %d", counted, point.GetCount())
		}
		// Float accumulation order differs, so compare within a tolerance
		// scaled to the magnitude rather than bit-for-bit.
		tolerance := math.Max(expected*1e-9, 1e-12)
		if diff := math.Abs(point.GetSum() - expected); diff > tolerance {
			t.Errorf(
				"go.schedule.duration Sum=%g, want the lower-bound estimate %g (diff %g)",
				point.GetSum(), expected, diff,
			)
		}
	}
}

// The contrib instrumentation exposes no Stop: its callbacks and Producer hang
// off the provider and its reader, so provider shutdown is what releases them.
// Confirm that directly, and that it still fits containerd's budget.
func TestOTLPMetricsShutdownReleasesRuntimeObservers(t *testing.T) {
	receiver, endpoint := startMetricsReceiver(t)
	t.Setenv("SPACE_METRICS_SINK", "otlp")
	configureOTLPMetricsEnv(t, endpoint)
	sink, err := newMetricsSink()
	if err != nil {
		t.Fatal(err)
	}
	otlp := sink.(*otlpMetricsSink)
	flush, cancelFlush := context.WithTimeout(context.Background(), time.Second)
	defer cancelFlush()
	if err := otlp.runtime.ForceFlush(flush); err != nil {
		t.Fatal(err)
	}
	receiveScope(t, receiver, runtimeScopeName)
	ctx, cancel := context.WithTimeout(context.Background(), telemetryShutdownBudget)
	defer cancel()
	started := time.Now()
	if err := sink.shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > telemetryShutdownBudget {
		t.Fatalf("runtime observers delayed shutdown past its budget: %v", elapsed)
	}
	// Shutdown exports whatever was pending; drain that before proving the
	// observers are released rather than merely idle.
	for drained := false; !drained; {
		select {
		case <-receiver.requests:
		default:
			drained = true
		}
	}
	if err := otlp.runtime.ForceFlush(context.Background()); err == nil {
		t.Error("provider still collects after shutdown")
	}
	// A refused flush is the deterministic proof; the channel is checked rather
	// than waited on, since a shut-down provider has nothing left to send.
	if queued := len(receiver.requests); queued != 0 {
		t.Fatalf("runtime observers still exporting after shutdown: %d requests", queued)
	}
}

// Runtime instrumentation is bound to the OTLP provider, so the sinks that
// never build one must not gain an OTel runtime stream.
func TestNonOTLPMetricsSinksAddNoRuntimeInstruments(t *testing.T) {
	for _, mode := range []string{"datadog", "auto", "off"} {
		t.Run(mode, func(t *testing.T) {
			receiver, endpoint := startMetricsReceiver(t)
			t.Setenv("SPACE_METRICS_SINK", mode)
			configureOTLPMetricsEnv(t, endpoint)
			sink, err := newMetricsSink()
			if err != nil {
				t.Fatal(err)
			}
			// Runtime instrumentation is registered by the OTLP sink alone, so
			// the sink's type is the deterministic signal: no OTLP sink means
			// no runtime provider, no callback instruments and no producer.
			if _, isOTLP := sink.(*otlpMetricsSink); isOTLP {
				t.Fatalf("%s selected the OTLP sink", mode)
			}
			if err := sink.count("space.shim.operation.error", 1, []string{"op:create"}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), telemetryShutdownBudget)
			defer cancel()
			if err := sink.shutdown(ctx); err != nil {
				t.Fatal(err)
			}
			// Shutdown flushes synchronously, so anything this mode was going
			// to export has already arrived. No waiting needed to know.
			if queued := len(receiver.requests); queued != 0 {
				t.Fatalf("%s mode exported %d OTLP requests", mode, queued)
			}
		})
	}
}
