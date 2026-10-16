package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
)

// Every current shim build includes this adapter.
const nativeDatadogTracingAvailable = true

type datadogTracePipeline struct{}
type datadogSpan struct{ span *tracer.Span }

func newDatadogTracePipeline() tracePipeline {
	id := newIdentity(defaultShimService)
	opts := []tracer.StartOption{tracer.WithService(id.serviceName()), tracer.WithLogStartup(false)}
	// Gate on a nonblank value but pass it raw, exactly as main does, so a
	// Datadog-only deployment starts the tracer with identical options.
	if env := id.tracerEnvironment(); strings.TrimSpace(env) != "" {
		opts = append(opts, tracer.WithEnv(env))
	}
	if version := id.tracerVersion(); strings.TrimSpace(version) != "" {
		opts = append(opts, tracer.WithServiceVersion(version))
	}
	if host := os.Getenv("DD_AGENT_HOST"); host != "" {
		opts = append(opts, tracer.WithAgentAddr(datadogTraceAgentAddr(host, os.Getenv("DD_TRACE_AGENT_PORT"))))
	}
	opts = append(opts, originSamplingRules()...)
	tracer.Start(opts...)
	return datadogTracePipeline{}
}

// originSamplingRules keeps or drops a root that links to an origin as the
// origin was sampled, as the Rust services' rules do: two rules on
// origin_sampled, which only such roots carry, ahead of the rules
// DD_TRACE_SAMPLING_RULES configures. The tracer lets those environment rules
// replace any given in code, so they are read here, appended, and taken out
// of the environment it reads. Rules it cannot parse are left for the tracer
// to refuse, as it did before.
func originSamplingRules() []tracer.StartOption {
	configured := []tracer.SamplingRule{}
	raw := os.Getenv("DD_TRACE_SAMPLING_RULES")
	if strings.TrimSpace(raw) == "" {
		if path := os.Getenv("DD_TRACE_SAMPLING_RULES_FILE"); path != "" {
			contents, err := os.ReadFile(path)
			if err != nil {
				logShim("trace sampling rules file unreadable: %v", err)
				return nil
			}
			raw = string(contents)
		}
	}
	if strings.TrimSpace(raw) != "" {
		if err := json.Unmarshal([]byte(raw), &configured); err != nil {
			logShim("trace sampling rules invalid; origin rules not added: %v", err)
			return nil
		}
	}
	rules := tracer.TraceSamplingRules(
		tracer.Rule{Tags: map[string]string{originSampledAttribute: "true"}, Rate: 1},
		tracer.Rule{Tags: map[string]string{originSampledAttribute: "false"}, Rate: 0},
	)
	rules = append(rules, configured...)
	_ = os.Unsetenv("DD_TRACE_SAMPLING_RULES")
	_ = os.Unsetenv("DD_TRACE_SAMPLING_RULES_FILE")
	return []tracer.StartOption{tracer.WithSamplingRules(rules)}
}

const defaultDatadogTraceAgentPort = 8126

// Preserve the configured APM port when an explicit agent host overrides the
// tracer's environment configuration. Invalid ports retain the default intake.
func datadogTraceAgentAddr(host, rawPort string) string {
	port := defaultDatadogTraceAgentPort
	if parsed, err := strconv.Atoi(strings.TrimSpace(rawPort)); err == nil && parsed > 0 && parsed <= 65535 {
		port = parsed
	} else if strings.TrimSpace(rawPort) != "" {
		logShim("ignoring invalid DD_TRACE_AGENT_PORT %q; using %d", rawPort, defaultDatadogTraceAgentPort)
	}
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}
	return net.JoinHostPort(host, strconv.Itoa(port))
}

func (datadogTracePipeline) start(ctx context.Context, name string, attrs ...attribute.KeyValue) (shimSpan, context.Context) {
	opts := make([]tracer.StartSpanOption, 0, len(attrs))
	for _, attr := range attrs {
		opts = append(opts, tracer.Tag(string(attr.Key), attr.Value.AsInterface()))
	}
	span, ctx := tracer.StartSpanFromContext(ctx, name, opts...)
	return datadogSpan{span}, ctx
}

func (datadogTracePipeline) startLinked(ctx context.Context, name string, carrier propagation.MapCarrier, attrs ...attribute.KeyValue) (shimSpan, context.Context) {
	opts := make([]tracer.StartSpanOption, 0, len(attrs)+1)
	for _, attr := range attrs {
		opts = append(opts, tracer.Tag(string(attr.Key), attr.Value.AsInterface()))
	}
	if linked, err := tracer.Extract(tracer.TextMapCarrier(carrier)); err == nil && linked != nil {
		opts = append(opts, tracer.WithSpanLinks([]tracer.SpanLink{{
			TraceID:     linked.TraceIDLower(),
			TraceIDHigh: linked.TraceIDUpper(),
			SpanID:      linked.SpanID(),
		}}))
	}
	// No ChildOf: the span is a root of its own trace.
	span := tracer.StartSpan(name, opts...)
	return datadogSpan{span}, tracer.ContextWithSpan(ctx, span)
}

func (s datadogSpan) SetAttributes(attrs ...attribute.KeyValue) {
	for _, attr := range attrs {
		s.span.SetTag(string(attr.Key), attr.Value.AsInterface())
	}
}
func (s datadogSpan) End(err error) { s.span.Finish(tracer.WithError(err)) }

func (datadogTracePipeline) inject(ctx context.Context, carrier propagation.MapCarrier) {
	propagation.Baggage{}.Inject(ctx, carrier)
	if span, ok := tracer.SpanFromContext(ctx); ok {
		if err := tracer.Inject(span.Context(), tracer.TextMapCarrier(carrier)); err != nil {
			logShim("trace context injection failed: %v", err)
		}
	}
}
func (datadogTracePipeline) shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() { tracer.Stop(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
