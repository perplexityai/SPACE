package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

var tracePropagation = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})

type otlpTracePipeline struct {
	tracer   trace.Tracer
	provider *sdktrace.TracerProvider
}
type otlpSpan struct{ trace.Span }

func traceEndpoint() (string, error) {
	return otlpEndpoint("trace", "TRACES", "http://localhost:4317")
}

func validateTraceConfiguration() error {
	exporter, err := configuredTraceExporter()
	if err != nil {
		return err
	}
	if exporter == traceOTLP {
		_, err = traceEndpoint()
	}
	return err
}

func newOTLPTracePipeline() (tracePipeline, error) {
	endpoint, err := traceEndpoint()
	if err != nil {
		return nil, err
	}
	// Traces identify their producer exactly as metrics do; without this a
	// shared resource would merge every shim on a node into one producer.
	res, err := otlpResource(metricResourceAttributes()...)
	if err != nil {
		return nil, fmt.Errorf("OTel trace resource: %w", err)
	}
	initOTelErrorHandler()
	// New is nonblocking; a missing receiver cannot delay sandbox creation.
	exporter, err := otlptracegrpc.New(context.Background(), otlptracegrpc.WithEndpointURL(endpoint), otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("OTLP trace exporter: %w", err)
	}
	// SDK v1.41 reads OTEL_BSP_* itself; the sampler OTEL_TRACES_SAMPLER
	// selects is wrapped to follow a linked root's origin and a span's parent.
	// The default batch queue is bounded and drops on overflow without blocking.
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(followOrigin{configured: configuredSampler()}),
	)
	return &otlpTracePipeline{tracer: provider.Tracer("pplx-space-shim"), provider: provider}, nil
}
func (p *otlpTracePipeline) start(ctx context.Context, name string, attrs ...attribute.KeyValue) (shimSpan, context.Context) {
	// Preserve existing backend operation/resource naming through the migration.
	ctx, span := p.tracer.Start(ctx, name, trace.WithAttributes(attrs...), trace.WithAttributes(
		attribute.String("operation.name", name), attribute.String("resource.name", name),
	))
	return otlpSpan{span}, ctx
}
func (p *otlpTracePipeline) startLinked(ctx context.Context, name string, carrier propagation.MapCarrier, attrs ...attribute.KeyValue) (shimSpan, context.Context) {
	opts := []trace.SpanStartOption{
		trace.WithNewRoot(),
		trace.WithAttributes(attrs...),
		trace.WithAttributes(attribute.String("operation.name", name), attribute.String("resource.name", name)),
	}
	if linked := trace.SpanContextFromContext(tracePropagation.Extract(context.Background(), carrier)); linked.IsValid() {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: linked}))
	}
	ctx, span := p.tracer.Start(ctx, name, opts...)
	return otlpSpan{span}, ctx
}
func (s otlpSpan) End(err error) {
	if err != nil {
		s.RecordError(err)
		s.SetStatus(codes.Error, err.Error())
	}
	s.Span.End()
}
func (p *otlpTracePipeline) inject(ctx context.Context, carrier propagation.MapCarrier) {
	tracePropagation.Inject(ctx, carrier)
}
func (p *otlpTracePipeline) shutdown(ctx context.Context) error {
	if p.provider == nil {
		return nil
	}
	return p.provider.Shutdown(ctx)
}

// followOrigin keeps or drops a root that links to an origin as the origin was
// sampled, so a kept request keeps the work it caused, as the Rust services'
// sampler does. A root without a valid link keeps the sampler
// OTEL_TRACES_SAMPLER selects. A span with a parent follows its parent
// whatever that sampler is: one that is not parent-based, such as always_on or
// traceidratio, would otherwise sample the children of a dropped root, which
// would export without it and propagate a sampled traceparent from an
// unsampled origin. As in the Rust sampler, a span under an unsampled parent is
// recorded but not sampled; the shim's only span processor, the batcher,
// exports sampled spans alone, so none of it leaves the shim.
type followOrigin struct{ configured sdktrace.Sampler }

func (f followOrigin) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	if parent := trace.SpanContextFromContext(p.ParentContext); parent.IsValid() {
		decision := sdktrace.RecordOnly
		if parent.IsSampled() {
			decision = sdktrace.RecordAndSample
		}
		return sdktrace.SamplingResult{Decision: decision, Tracestate: parent.TraceState()}
	}
	linked, sampled := false, false
	for _, link := range p.Links {
		if link.SpanContext.IsValid() {
			linked = true
			sampled = sampled || link.SpanContext.IsSampled()
		}
	}
	if !linked {
		return f.configured.ShouldSample(p)
	}
	decision := sdktrace.Drop
	if sampled {
		decision = sdktrace.RecordAndSample
	}
	return sdktrace.SamplingResult{Decision: decision}
}

func (f followOrigin) Description() string {
	return "FollowOrigin{" + f.configured.Description() + "}"
}

// configuredSampler is the sampler the SDK would build from
// OTEL_TRACES_SAMPLER and OTEL_TRACES_SAMPLER_ARG; giving the provider a
// sampler stops it reading them. As in the SDK, a value it cannot use is
// reported and the default, parentbased_always_on, applies.
func configuredSampler() sdktrace.Sampler {
	fallback := sdktrace.ParentBased(sdktrace.AlwaysSample())
	name, present := os.LookupEnv("OTEL_TRACES_SAMPLER")
	if !present {
		return fallback
	}
	arg, hasArg := os.LookupEnv("OTEL_TRACES_SAMPLER_ARG")
	arg = strings.TrimSpace(arg)
	ratio := func() (float64, bool) {
		if !hasArg {
			return 1, true
		}
		value, err := strconv.ParseFloat(arg, 64)
		return value, err == nil && value >= 0 && value <= 1
	}
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "always_on":
		return sdktrace.AlwaysSample()
	case "always_off":
		return sdktrace.NeverSample()
	case "traceidratio":
		if value, ok := ratio(); ok {
			return sdktrace.TraceIDRatioBased(value)
		}
	case "parentbased_always_on":
		return fallback
	case "parentbased_always_off":
		return sdktrace.ParentBased(sdktrace.NeverSample())
	case "parentbased_traceidratio":
		if value, ok := ratio(); ok {
			return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(value))
		}
	}
	logShim("unusable OTEL_TRACES_SAMPLER %q or argument %q; using parentbased_always_on", name, arg)
	return fallback
}
