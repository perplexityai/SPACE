package main

import (
	"context"
	"fmt"

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
	// SDK v1.41 reads OTEL_TRACES_SAMPLER[_ARG] and OTEL_BSP_* itself.
	// The default batch queue is bounded and drops on overflow without blocking.
	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res))
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
func (p *otlpTracePipeline) extract(ctx context.Context, carrier propagation.MapCarrier) context.Context {
	return tracePropagation.Extract(ctx, carrier)
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
