package main

import (
	"context"
	"os"
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
		opts = append(opts, tracer.WithAgentAddr(host+":8126"))
	}
	tracer.Start(opts...)
	return datadogTracePipeline{}
}

func (datadogTracePipeline) start(ctx context.Context, name string, attrs ...attribute.KeyValue) (shimSpan, context.Context) {
	opts := make([]tracer.StartSpanOption, 0, len(attrs))
	for _, attr := range attrs {
		opts = append(opts, tracer.Tag(string(attr.Key), attr.Value.AsInterface()))
	}
	if parent, ok := ctx.Value(datadogParentKey{}).(*tracer.SpanContext); ok {
		opts = append(opts, tracer.ChildOf(parent))
		span := tracer.StartSpan(name, opts...)
		// Consume the remote parent once. Descendants must use this local span,
		// otherwise every nested operation becomes a sibling of shim.create.
		ctx = context.WithValue(ctx, datadogParentKey{}, false)
		return datadogSpan{span}, tracer.ContextWithSpan(ctx, span)
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

// A remote parent is represented with ChildOf at start time, not a fabricated
// live Datadog span. This preserves all 128 trace-ID bits from W3C extraction.
type datadogParentKey struct{}

func (datadogTracePipeline) extract(ctx context.Context, carrier propagation.MapCarrier) context.Context {
	ctx = propagation.Baggage{}.Extract(ctx, carrier)
	parent, err := tracer.Extract(tracer.TextMapCarrier(carrier))
	if err != nil || parent == nil {
		return ctx
	}
	return context.WithValue(ctx, datadogParentKey{}, parent)
}
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
