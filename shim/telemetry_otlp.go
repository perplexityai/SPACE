package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	otelruntime "go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// The collection cadence the Rust sink uses, so a Collector receives one
// cadence from every SPACE producer unless an operator changes it.
const defaultExportInterval = 10 * time.Second

type otlpMetricsSink struct {
	provider *sdkmetric.MeterProvider
	// runtime carries the cumulative Go runtime metrics; see
	// newOTLPMetricsSink.
	runtime    *sdkmetric.MeterProvider
	meter      metric.Meter
	attributes []attribute.KeyValue
	mu         sync.Mutex
	counters   map[string]metric.Int64Counter
	histograms map[string]metric.Float64Histogram
}

// shimMeterScope names the shim's own instrumentation scope. The Go runtime
// instrumentation reports under its own scope, on its own provider.
const shimMeterScope = "pplx-space-shim"

func metricsEndpoint() (string, error) {
	return otlpEndpoint("metric", "METRICS", "")
}

// metricResourceAttributes is the identity of every metric a shim exports:
// one per process, `<node>:<pid>`. The Collector's delta-to-cumulative
// conversion needs one writer per stream; it drops a point older than the
// stream's last as out of order, so concurrent shims sharing a node identity
// would undercount silently. Queries aggregate across a node's shims instead
// (`sum without (service_instance_id)`). A true per-node stream needs a single
// long-lived writer on the node, which the shim is not.
func metricResourceAttributes() []attribute.KeyValue {
	instance, attrs := shimHostIdentity()
	return append([]attribute.KeyValue{
		attribute.String("service.instance.id", fmt.Sprintf("%s:%d", instance, os.Getpid())),
	}, attrs...)
}

// shimHostIdentity names where the shim runs: the node, else the host, and the
// host and Kubernetes attributes that go with it. Neither HOSTNAME nor
// SPACE_NODE_ID reaches a shim under containerd, so the node name comes first.
func shimHostIdentity() (string, []attribute.KeyValue) {
	node := otlpSetting("SPACE_K8S_NODE_NAME", "KUBE_NODE_NAME", "")
	instance := node
	if instance == "" {
		instance = otlpSetting("HOSTNAME", "SPACE_NODE_ID", "unknown-host")
	}
	var attrs []attribute.KeyValue
	if node != "" {
		attrs = append(attrs, attribute.String("host.name", node), attribute.String("k8s.node.name", node))
	} else if _, inKubernetes := os.LookupEnv("KUBERNETES_SERVICE_HOST"); !inKubernetes {
		// Kubernetes HOSTNAME identifies a pod, not its underlying host.
		if host := strings.TrimSpace(os.Getenv("HOSTNAME")); host != "" {
			attrs = append(attrs, attribute.String("host.name", host))
		}
	}
	if pod := strings.TrimSpace(os.Getenv("SPACE_K8S_POD_NAME")); pod != "" {
		attrs = append(attrs, attribute.String("k8s.pod.name", pod))
	}
	return instance, attrs
}

func newOTLPMetricsSink() (metricsSink, error) {
	endpoint, err := metricsEndpoint()
	if err != nil {
		return nil, err
	}
	interval, err := metricExportInterval()
	if err != nil {
		return nil, err
	}
	res, err := otlpResource(metricResourceAttributes()...)
	if err != nil {
		return nil, fmt.Errorf("OTel metrics resource: %w", err)
	}
	initOTelErrorHandler()
	// Two providers on one resource, because the two families need different
	// temporalities. The shim's own counters and histograms are delta, the
	// profile the Rust sink exports. The Go runtime metrics are cumulative:
	// their state is the process's own, so it never needs the Collector's
	// delta-to-cumulative conversion or a slot among its streams. A reader
	// serves every instrument of its provider, so each temporality has its own
	// provider, reader and exporter. Both export to the same endpoint at the
	// same cadence, and shutdown flushes both.
	exporter, err := newOTLPMetricsExporter(endpoint,
		otlpmetricgrpc.WithTemporalitySelector(func(sdkmetric.InstrumentKind) metricdata.Temporality {
			return metricdata.DeltaTemporality
		}),
		otlpmetricgrpc.WithAggregationSelector(func(kind sdkmetric.InstrumentKind) sdkmetric.Aggregation {
			if kind == sdkmetric.InstrumentKindHistogram {
				return sdkmetric.AggregationBase2ExponentialHistogram{MaxSize: 160, MaxScale: 8}
			}
			return sdkmetric.DefaultAggregationSelector(kind)
		}),
	)
	if err != nil {
		return nil, err
	}
	runtimeExporter, err := newOTLPMetricsExporter(endpoint,
		otlpmetricgrpc.WithTemporalitySelector(sdkmetric.DefaultTemporalitySelector),
		otlpmetricgrpc.WithAggregationSelector(sdkmetric.DefaultAggregationSelector),
	)
	if err != nil {
		return nil, err
	}
	reader := sdkmetric.NewPeriodicReader(exporter,
		sdkmetric.WithInterval(interval), sdkmetric.WithTimeout(3*time.Second),
	)
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithResource(res), sdkmetric.WithCardinalityLimit(2000))
	// otelruntime.Start registers the memory, goroutine and GC instruments, but
	// the scheduler histogram only comes from its Producer. Both ride the
	// runtime reader, so they share its exporter and cadence.
	runtimeReader := sdkmetric.NewPeriodicReader(runtimeExporter,
		sdkmetric.WithInterval(interval), sdkmetric.WithTimeout(3*time.Second),
		sdkmetric.WithProducer(otelruntime.NewProducer()),
	)
	runtimeProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(runtimeReader), sdkmetric.WithResource(res))
	// Runtime visibility is supplementary to space.shim.*: report a failure
	// through the OTel error handler and keep the custom metrics flowing.
	if err := otelruntime.Start(otelruntime.WithMeterProvider(runtimeProvider)); err != nil {
		otel.Handle(fmt.Errorf("Go runtime metrics instrumentation: %w", err))
	}
	return &otlpMetricsSink{
		provider: provider, runtime: runtimeProvider, meter: provider.Meter(shimMeterScope),
		attributes: constantPointAttributes(telemetryTags()),
		counters:   make(map[string]metric.Int64Counter), histograms: make(map[string]metric.Float64Histogram),
	}, nil
}

// newOTLPMetricsExporter builds a plaintext OTLP/gRPC metrics exporter with the
// given temporality and aggregation profile. Converting delta exponential
// histograms to cumulative native histograms is a Collector responsibility,
// not something this exporter does.
func newOTLPMetricsExporter(endpoint string, profile ...otlpmetricgrpc.Option) (*otlpmetricgrpc.Exporter, error) {
	options := append([]otlpmetricgrpc.Option{
		otlpmetricgrpc.WithEndpointURL(endpoint), otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithTimeout(3 * time.Second),
	}, profile...)
	exporter, err := otlpmetricgrpc.New(context.Background(), options...)
	if err != nil {
		return nil, fmt.Errorf("OTLP metrics exporter: %w", err)
	}
	return exporter, nil
}

// Read the metric export interval in milliseconds, defaulting to
// defaultExportInterval. Anything that is not a positive whole number of
// milliseconds fails telemetry setup rather than being ignored.
func metricExportInterval() (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv("OTEL_METRIC_EXPORT_INTERVAL"))
	if raw == "" {
		return defaultExportInterval, nil
	}
	millis, err := strconv.ParseInt(raw, 10, 64)
	// Reject a value the Duration multiplication below would overflow, rather
	// than wrapping it into a short or negative cadence.
	if err != nil || millis <= 0 || millis > int64(math.MaxInt64)/int64(time.Millisecond) {
		return 0, fmt.Errorf("OTEL_METRIC_EXPORT_INTERVAL must be a positive whole number of milliseconds")
	}
	return time.Duration(millis) * time.Millisecond, nil
}

// The OTel resource owns service, env and version: service.name,
// deployment.environment.name and service.version. Point-level copies make
// receivers such as the Datadog Agent surface duplicate identity tags with
// ambiguous precedence, and a Prometheus-style backend a `service` label beside
// `service_name`. The DogStatsD sink keeps them because it has no resource to
// carry them.
func constantPointAttributes(tags []string) []attribute.KeyValue {
	kept := make([]string, 0, len(tags))
	for _, tag := range tags {
		if key, _, _ := strings.Cut(tag, ":"); key == "service" || key == "env" || key == "version" {
			continue
		}
		kept = append(kept, tag)
	}
	return metricAttributes(kept)
}

func metricAttributes(tags []string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(tags))
	for _, tag := range tags {
		key, value, hasValue := strings.Cut(tag, ":")
		if key == "" {
			continue
		}
		if hasValue {
			attrs = append(attrs, attribute.String(key, value))
		} else {
			attrs = append(attrs, attribute.Bool(key, true))
		}
	}
	return attrs
}

func (s *otlpMetricsSink) measurementAttributes(tags []string) metric.MeasurementOption {
	attrs := make([]attribute.KeyValue, 0, len(s.attributes)+len(tags))
	attrs = append(attrs, s.attributes...)
	attrs = append(attrs, metricAttributes(tags)...)
	return metric.WithAttributes(attrs...)
}

func (s *otlpMetricsSink) count(name string, value int64, tags []string) error {
	s.mu.Lock()
	counter, ok := s.counters[name]
	if !ok {
		var err error
		counter, err = s.meter.Int64Counter(name, metric.WithUnit("1"))
		if err != nil {
			s.mu.Unlock()
			return err
		}
		s.counters[name] = counter
	}
	s.mu.Unlock()
	counter.Add(context.Background(), value, s.measurementAttributes(tags))
	return nil
}

func (s *otlpMetricsSink) distribution(name string, value float64, tags []string) error {
	s.mu.Lock()
	histogram, ok := s.histograms[name]
	if !ok {
		var err error
		histogram, err = s.meter.Float64Histogram(otlpHistogramName(name), metric.WithUnit("ms"))
		if err != nil {
			s.mu.Unlock()
			return err
		}
		s.histograms[name] = histogram
	}
	s.mu.Unlock()
	histogram.Record(context.Background(), value, s.measurementAttributes(tags))
	return nil
}

// otlpHistogramName drops the `_ms` that DogStatsD names carry for want of a
// unit field. OTLP declares the unit instead, and a Prometheus-style backend
// appends its full word, so `space.shim.operation.latency_ms` would arrive as
// `..._latency_ms_milliseconds`. The Rust sink applies the same rule. The
// DogStatsD names are unchanged.
func otlpHistogramName(name string) string {
	if stripped := strings.TrimSuffix(name, "_ms"); stripped != "" && !strings.HasSuffix(stripped, ".") {
		return stripped
	}
	return name
}

func (s *otlpMetricsSink) shutdown(ctx context.Context) error {
	// PeriodicReader.Shutdown exports pending measurements before closing gRPC.
	// The providers flush concurrently under the caller's one deadline, so a
	// slow runtime export cannot consume the custom metrics' window.
	runtimeDone := make(chan error, 1)
	go func() { runtimeDone <- s.runtime.Shutdown(ctx) }()
	err := s.provider.Shutdown(ctx)
	return errors.Join(err, <-runtimeDone)
}
