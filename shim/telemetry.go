package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/status"
)

var Version = "dev"

const (
	telemetryShutdownBudget    = 500 * time.Millisecond
	defaultShimService         = "containerd-shim-space-v2"
	shimOperationLatencyMetric = "space.shim.operation.latency_ms"
	shimOperationStepMetric    = "space.shim.operation.step.latency_ms"
	shimOperationErrorMetric   = "space.shim.operation.error"

	shimMetricResultSuccess = "success"
	shimMetricResultError   = "error"
	shimMetricResultTimeout = "timeout"
)

var (
	telemetryOnce sync.Once
	activeMetrics atomic.Pointer[activeMetricsSink]
)

type metricsSink interface {
	count(string, int64, []string) error
	distribution(string, float64, []string) error
	shutdown(context.Context) error
}

type metricsSinkChoice string

const (
	metricsDatadog metricsSinkChoice = "datadog"
	metricsOTLP    metricsSinkChoice = "otlp"
	metricsOff     metricsSinkChoice = "off"
)

func configuredMetricsSink() (metricsSinkChoice, error) {
	raw, present := os.LookupEnv("SPACE_METRICS_SINK")
	if !present {
		raw = "auto"
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "auto", "datadog":
		return metricsDatadog, nil
	case "otlp":
		return metricsOTLP, nil
	case "off":
		return metricsOff, nil
	default:
		return "", fmt.Errorf("invalid SPACE_METRICS_SINK %q; expected auto, datadog, otlp, or off", raw)
	}
}

// validateMetricsConfiguration rejects a metrics selection that can never
// work, so containerd surfaces the mistake at bootstrap instead of the shim
// silently running without metrics. Runtime sink and export failures still
// warn and disable, so an unreachable Collector never blocks a sandbox.
func validateMetricsConfiguration() error {
	choice, err := configuredMetricsSink()
	if err != nil {
		return err
	}
	if choice == metricsDatadog {
		// At bootstrap, not when the first metric is recorded: the sink is
		// built lazily, so an idle shim would never say anything.
		mixedIdentityWarning.Do(func() {
			warnOnMixedDatadogIdentity(newIdentity(defaultShimService))
		})
	}
	if choice != metricsOTLP {
		return nil
	}
	// Names the setting; the helper's errors never echo endpoint credentials.
	if _, err := metricsEndpoint(); err != nil {
		return err
	}
	// Checked here as well as at setup so a bad cadence fails bootstrap rather
	// than the first export. Only the otlp path reads it; datadog and off never
	// touch an OTLP-only setting.
	if _, err := metricExportInterval(); err != nil {
		return err
	}
	return nil
}

func newMetricsSink() (metricsSink, error) {
	choice, err := configuredMetricsSink()
	if err != nil {
		return nil, err
	}
	switch choice {
	case metricsDatadog:
		return newDatadogMetricsSink()
	case metricsOTLP:
		return newOTLPMetricsSink()
	default:
		return disabledMetricsSink{}, nil
	}
}

type activeMetricsSink struct{ metricsSink }
type disabledMetricsSink struct{}

func (disabledMetricsSink) count(string, int64, []string) error          { return nil }
func (disabledMetricsSink) distribution(string, float64, []string) error { return nil }
func (disabledMetricsSink) shutdown(context.Context) error               { return nil }

func telemetry() metricsSink {
	telemetryOnce.Do(func() {
		sink, err := newMetricsSink()
		if err != nil {
			logShim("metrics sink initialization failed; metrics disabled: %v", err)
			sink = disabledMetricsSink{}
		}
		activeMetrics.Store(&activeMetricsSink{sink})
	})
	return activeMetrics.Load().metricsSink
}

func stopTelemetry() {
	ctx, cancel := context.WithTimeout(context.Background(), telemetryShutdownBudget)
	defer cancel()
	results := make(chan error, 2)
	pending := 0
	stop := func(signal string, shutdown func(context.Context) error) {
		pending++
		go func() {
			err := shutdown(ctx)
			if err != nil {
				err = fmt.Errorf("%s shutdown: %w", signal, err)
			}
			results <- err
		}()
	}
	// Both signals get the same deadline; an unavailable receiver cannot consume
	// the other signal's flush window or extend containerd's shutdown budget.
	if tracer := activeTracer.Load(); tracer != nil {
		stop("trace exporter", tracer.shutdown)
	}
	if sink := activeMetrics.Load(); sink != nil {
		stop("metrics sink", sink.shutdown)
	}
	for ; pending > 0; pending-- {
		select {
		case err := <-results:
			if err != nil {
				logShim("%v", err)
			}
		case <-ctx.Done():
			logShim("telemetry shutdown: %v", ctx.Err())
			return
		}
	}
}

func telemetryTags() []string {
	id := newIdentity(defaultShimService)
	tags := []string{
		fmt.Sprintf("service:%s", id.serviceName()),
		fmt.Sprintf("version:%s", id.metricVersion()),
	}
	if env := id.metricEnvironment(); env != "" {
		tags = append(tags, fmt.Sprintf("env:%s", env))
	}
	// First-class cluster identity, rendered into the shim wrapper by the
	// worker chart so space.shim.* series are queryable per cluster.
	if clusterID := os.Getenv("SPACE_CLUSTER_ID"); clusterID != "" {
		if isValidClusterID(clusterID) {
			tags = append(tags, fmt.Sprintf("cluster_id:%s", clusterID))
		} else {
			// Drop malformed values rather than forward them raw: a comma or
			// other delimiter would inject spurious DogStatsD tags, and the
			// Rust control plane rejects the same values, so emitting them
			// here would make shim identity disagree with metrics/events.
			logShim("ignoring invalid SPACE_CLUSTER_ID %q for shim telemetry", clusterID)
		}
	}
	return tags
}

func shimServiceName() string {
	return newIdentity(defaultShimService).serviceName()
}

// maxClusterIDBytes mirrors Rust's `MAX_CLUSTER_ID_BYTES` (one RFC-1123 label).
// The shim can't import Rust constants, so keep this in sync with
// `ClusterId` in crates/libs/cluster-id/src/lib.rs.
const maxClusterIDBytes = 63

// isValidClusterID reports whether s is a single lowercase RFC-1123 label
// (`[a-z0-9]([-a-z0-9]*[a-z0-9])?`, at most maxClusterIDBytes bytes). It mirrors
// the Rust `ClusterId` validation so the shim never emits a cluster_id tag the
// control plane would reject, and never lets a stray delimiter inject extra
// DogStatsD tags.
func isValidClusterID(s string) bool {
	if len(s) == 0 || len(s) > maxClusterIDBytes {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			continue
		}
		// Hyphens are allowed only in interior positions.
		if c == '-' && i != 0 && i != len(s)-1 {
			continue
		}
		return false
	}
	return true
}

func metricTag(key string, value any) string {
	return fmt.Sprintf("%s:%v", key, value)
}

func operationMetricTags(op string, result string, tags ...string) []string {
	metricTags := []string{metricTag("op", op), metricTag("result", result)}
	return append(metricTags, tags...)
}

func stepMetricTags(op string, step string, result string, tags ...string) []string {
	metricTags := operationMetricTags(op, result, metricTag("step", step))
	return append(metricTags, tags...)
}

func errorMetricTags(op string, reason string, err error, tags ...string) []string {
	metricTags := operationMetricTags(op, shimMetricResultError, metricTag("reason", reason))
	if err != nil {
		if st, ok := status.FromError(err); ok {
			metricTags = append(metricTags, metricTag("grpc_code", st.Code().String()))
		}
	}
	return append(metricTags, tags...)
}

func emitCounter(name string, value int64, tags ...string) {
	if err := telemetry().count(name, value, tags); err != nil {
		logShim("failed to emit counter %s: %v", name, err)
	}
}

func emitShimOperationLatency(op string, startedAt time.Time, result string, tags ...string) {
	emitTiming(shimOperationLatencyMetric, startedAt, operationMetricTags(op, result, tags...)...)
}

func emitShimStepLatency(op string, step string, startedAt time.Time, result string, tags ...string) {
	emitTiming(shimOperationStepMetric, startedAt, stepMetricTags(op, step, result, tags...)...)
}

func emitShimOperationError(op string, reason string, err error, tags ...string) {
	emitCounter(shimOperationErrorMetric, 1, errorMetricTags(op, reason, err, tags...)...)
}

func emitShimStepError(op string, step string, reason string, err error, tags ...string) {
	stepTags := append([]string{metricTag("step", step)}, tags...)
	emitShimOperationError(op, reason, err, stepTags...)
}

func emitTiming(name string, startedAt time.Time, tags ...string) {
	if err := telemetry().distribution(name, time.Since(startedAt).Seconds()*1000, tags); err != nil {
		logShim("failed to emit distribution %s: %v", name, err)
	}
}

// Reported once per process, from whichever bootstrap path runs first.
var mixedIdentityWarning sync.Once

// Warn once when a SPACE or OTel identity setting outranks a Datadog one while
// the DogStatsD sink is selected.
//
// The resolver decides the tags this shim attaches, but the Datadog client
// attaches its own env, service and version from the DD_* settings, and those
// are not this process's to suppress: removing them would change what a
// Datadog-only deployment emits. So a mixed configuration lands two identities
// on the same series and only the operator can decide which to drop. Naming
// the settings is the whole remedy available here; see docs/observability.md.
//
// A Datadog-only or OTLP-only deployment never reaches this.
func warnOnMixedDatadogIdentity(id identity) {
	var shadowed []string
	for _, field := range []struct {
		name    string
		setting setting
	}{
		{"service", id.service},
		{"env", id.environment},
		{"version", id.version},
	} {
		// The client injects a DD_* value as a tag whenever it is non-empty,
		// untrimmed, so ' prod ' and 'prod' are two values on one series and
		// comparing trimmed text would call them equal.
		preferred := field.setting.preferred
		datadog := field.setting.datadog
		if preferred != "" && datadog != "" && preferred != datadog {
			shadowed = append(shadowed, field.name)
		}
	}
	if len(shadowed) == 0 {
		return
	}
	// Names the fields, never the values: an environment or version can carry
	// deployment detail that does not belong in logs.
	logShim(
		"metrics: SPACE_* or OTEL_* identity outranks DD_* for %s, "+
			"but the Datadog client still tags its own metrics from DD_SERVICE, "+
			"DD_ENV and DD_VERSION; set only one family to avoid two identities "+
			"on the same series",
		strings.Join(shadowed, ", "),
	)
}
