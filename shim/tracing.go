package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// The adapter covers only the operations used by the shim.
type shimSpan interface {
	SetAttributes(...attribute.KeyValue)
	End(error)
}

type tracePipeline interface {
	start(context.Context, string, ...attribute.KeyValue) (shimSpan, context.Context)
	// startLinked starts a new root span linked to the context serialized in
	// the carrier, or an unlinked root when the carrier holds none.
	startLinked(context.Context, string, propagation.MapCarrier, ...attribute.KeyValue) (shimSpan, context.Context)
	inject(context.Context, propagation.MapCarrier)
	shutdown(context.Context) error
}

type traceExporter string

const (
	traceDatadog traceExporter = "datadog"
	traceOTLP    traceExporter = "otlp"
	traceOff     traceExporter = "off"
)

func configuredTraceExporter() (traceExporter, error) {
	raw, present := os.LookupEnv("SPACE_TRACE_EXPORTER")
	if !present {
		raw = "auto"
	}
	return resolveTraceExporter(raw, nativeDatadogTracingAvailable, otlpEndpointConfigured("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"))
}

// See docs/observability.md for the shared Rust/Go selector contract.
// resolveTraceExporter mirrors the Rust contract: native Datadog when compiled
// in, otherwise OTLP when an endpoint says where to send it, otherwise off.
// Capabilities and endpoint presence are arguments so the matrix is testable
// from a build that always has Datadog.
func resolveTraceExporter(raw string, datadogAvailable bool, otlpEndpointConfigured bool) (traceExporter, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "auto":
		if datadogAvailable {
			return traceDatadog, nil
		}
		if otlpEndpointConfigured {
			return traceOTLP, nil
		}
		return traceOff, nil
	case "datadog":
		if !datadogAvailable {
			return "", fmt.Errorf("trace exporter datadog is not compiled in")
		}
		return traceDatadog, nil
	case "otlp":
		return traceOTLP, nil
	case "off":
		return traceOff, nil
	default:
		return "", fmt.Errorf("invalid SPACE_TRACE_EXPORTER %q", raw)
	}
}

// otlpEndpointConfigured reports a nonblank signal-specific or generic OTLP
// endpoint. It deliberately skips the exporter's stricter checks: a present but
// unusable endpoint must fail as a configuration error under an explicit
// selection, not quietly turn auto into off.
func otlpEndpointConfigured(signalEnv string) bool {
	for _, name := range [2]string{signalEnv, "OTEL_EXPORTER_OTLP_ENDPOINT"} {
		if strings.TrimSpace(os.Getenv(name)) != "" {
			return true
		}
	}
	return false
}

func newTracePipeline() (tracePipeline, error) {
	exporter, err := configuredTraceExporter()
	if err != nil {
		return nil, err
	}
	switch exporter {
	case traceDatadog:
		return newDatadogTracePipeline(), nil
	case traceOTLP:
		return newOTLPTracePipeline()
	default:
		return newDisabledTracePipeline(), nil
	}
}

type activeTracePipeline struct{ tracePipeline }

// Off disables propagation as well as export, without changing the caller's
// cancellation, authentication, or other request context.
type disabledTracePipeline struct{}
type disabledShimSpan struct{}

func newDisabledTracePipeline() tracePipeline { return disabledTracePipeline{} }
func (disabledTracePipeline) start(ctx context.Context, _ string, _ ...attribute.KeyValue) (shimSpan, context.Context) {
	return disabledShimSpan{}, ctx
}
func (disabledTracePipeline) startLinked(ctx context.Context, _ string, _ propagation.MapCarrier, _ ...attribute.KeyValue) (shimSpan, context.Context) {
	return disabledShimSpan{}, ctx
}
func (disabledShimSpan) SetAttributes(...attribute.KeyValue)                 {}
func (disabledShimSpan) End(error)                                           {}
func (disabledTracePipeline) inject(context.Context, propagation.MapCarrier) {}
func (disabledTracePipeline) shutdown(context.Context) error                 { return nil }

var activeTracer atomic.Pointer[activeTracePipeline]
var tracerOnce sync.Once

// Initialize only on the first traced sandbox operation. Container-share shims
// do not need an exporter or its background goroutines.
func initTracer() tracePipeline {
	tracerOnce.Do(func() {
		pipeline, err := newTracePipeline()
		if err != nil {
			// Manager/service startup validates configuration before serving requests.
			// An unexpected SDK initialization failure must not fail a sandbox request.
			logShim("trace exporter initialization failed: %v", err)
			pipeline = newDisabledTracePipeline()
		}
		activeTracer.Store(&activeTracePipeline{pipeline})
	})
	return activeTracer.Load().tracePipeline
}

func startSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (shimSpan, context.Context) {
	return initTracer().start(ctx, name, attrs...)
}

// The span attributes a root that links to its origin records, so a search can
// find it by the origin's trace ID, and so the samplers, which match the
// attribute, keep it exactly when the origin was sampled.
const (
	originTraceIDAttribute = "origin_trace_id"
	originSampledAttribute = "origin_sampled"
)

// Work that outlives the request that started it, such as a task's teardown
// after its create, starts a new root linked to that request's serialized
// context and sampled as that request was. An empty or malformed traceparent
// leaves the root unlinked and sampled on its own.
func startLinkedSpan(ctx context.Context, name, traceparent string, attrs ...attribute.KeyValue) (shimSpan, context.Context) {
	carrier := propagation.MapCarrier{"traceparent": traceparent}
	if origin := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), carrier)); origin.IsValid() {
		attrs = append(attrs,
			attribute.String(originTraceIDAttribute, origin.TraceID().String()),
			attribute.Bool(originSampledAttribute, origin.IsSampled()),
		)
	}
	return initTracer().startLinked(ctx, name, carrier, attrs...)
}

// The W3C traceparent of ctx's current span, or "" when tracing is off or no
// span is current.
func serializedTraceparent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	initTracer().inject(ctx, carrier)
	return carrier["traceparent"]
}

func injectTraceMetadata(ctx context.Context, pipeline tracePipeline) context.Context {
	carrier := propagation.MapCarrier{}
	pipeline.inject(ctx, carrier)
	if _, off := pipeline.(disabledTracePipeline); !off && carrier["traceparent"] == "" {
		// A call made outside any shim operation, such as containerd relaying
		// kubelet's periodic stats polls, carries an unsampled context, as a
		// Rust caller with no traced request does: the server records it, so
		// its APM stats and a failure still see it, without starting a
		// sampled trace for every poll.
		if traceparent := unsampledTraceparent(); traceparent != "" {
			carrier["traceparent"] = traceparent
			// Marks the context as one no request saw, as the Rust services
			// do, so no server saves it as the origin of later work.
			carrier["tracestate"] = "space=untraced"
		}
	}
	if len(carrier) == 0 {
		return ctx
	}
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	for key, value := range carrier {
		md.Set(key, value)
	}
	return metadata.NewOutgoingContext(ctx, md)
}

// Existing shim.sbxmgr spans own the client operation. This interceptor only
// carries their context across gRPC; it does not create another span.
func traceparentInjectingUnaryInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if pipeline := activeTracer.Load(); pipeline != nil {
		ctx = injectTraceMetadata(ctx, pipeline.tracePipeline)
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

// A W3C context for a span nobody records: fresh IDs, not sampled.
func unsampledTraceparent() string {
	var ids [24]byte
	if _, err := rand.Read(ids[:]); err != nil {
		return ""
	}
	return fmt.Sprintf("00-%x-%x-00", ids[:16], ids[16:])
}
