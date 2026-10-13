package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
	taskAPI "github.com/containerd/containerd/api/runtime/task/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func TestTraceExporterResolution(t *testing.T) {
	for _, tc := range []struct {
		name             string
		raw              string
		datadogAvailable bool
		otlpEndpoint     bool
		want             traceExporter
		wantError        bool
	}{
		// A Datadog build is unchanged, with or without an endpoint.
		{"auto with native adapter", "auto", true, false, traceDatadog, false},
		{"auto with native adapter and endpoint", "auto", true, true, traceDatadog, false},
		// Without Datadog, an endpoint is what makes auto mean OTLP.
		{"auto without native adapter but endpoint", "auto", false, true, traceOTLP, false},
		{"auto without native adapter or endpoint", "auto", false, false, traceOff, false},
		// Explicit selections ignore the endpoint entirely.
		{"explicit native adapter", "datadog", true, false, traceDatadog, false},
		{"missing native adapter", "datadog", false, true, "", true},
		{"OTLP with native adapter", "otlp", true, false, traceOTLP, false},
		{"OTLP without native adapter", "otlp", false, false, traceOTLP, false},
		{"off with native adapter", "off", true, true, traceOff, false},
		{"off without native adapter", "off", false, false, traceOff, false},
		{"normalized auto", " AUTO \t", true, false, traceDatadog, false},
		// journald is not a trace exporter.
		{"journald", "journald", true, true, "", true},
		{"empty", "", true, false, "", true},
		{"invalid", "typo", true, false, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveTraceExporter(tc.raw, tc.datadogAvailable, tc.otlpEndpoint)
			if got != tc.want || (err != nil) != tc.wantError {
				t.Fatalf("got %q, %v; want %q, error=%t", got, err, tc.want, tc.wantError)
			}
		})
	}
}

func TestDefaultTraceExporterKeepsNativeDatadog(t *testing.T) {
	t.Setenv("SPACE_TRACE_EXPORTER", "unused")
	if err := os.Unsetenv("SPACE_TRACE_EXPORTER"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4317")
	if got, err := configuredTraceExporter(); err != nil || got != traceDatadog {
		t.Fatalf("default exporter=%q err=%v", got, err)
	}
}

// Separate processes isolate both SDKs' global state and the lazy initializer.
func TestStartSpanWithRemoteTraceparent(t *testing.T) {
	if os.Getenv("SPACE_TRACE_PARENT_TEST") != "1" {
		for _, mode := range []string{"datadog", "otlp", "off"} {
			t.Run(mode, func(t *testing.T) {
				cmd := exec.Command(os.Args[0], "-test.run=^TestStartSpanWithRemoteTraceparent$")
				for _, item := range os.Environ() {
					if !strings.HasPrefix(item, "DD_") && !strings.HasPrefix(item, "OTEL_") && !strings.HasPrefix(item, "SPACE_TRACE_") {
						cmd.Env = append(cmd.Env, item)
					}
				}
				cmd.Env = append(cmd.Env, "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=grpc", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://127.0.0.1:1", "DD_TRACE_AGENT_URL=http://127.0.0.1:1", "SPACE_TRACE_PARENT_TEST=1", "SPACE_TRACE_EXPORTER="+mode, "DD_INSTRUMENTATION_TELEMETRY_ENABLED=false")
				headers := "authorization=header-test-secret"
				if mode == "otlp" {
					headers = " \t"
				}
				cmd.Env = append(cmd.Env, "OTEL_EXPORTER_OTLP_HEADERS="+headers, "OTEL_EXPORTER_OTLP_TRACES_HEADERS="+headers)
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("%v\n%s", err, output)
				} else {
					t.Logf("%s", output)
				}
			})
		}
		return
	}
	const validParent = "00-69f2ccfd00000000e88861448bd50939-ea1dc2f49db4d7a9-01"
	for name, parent := range map[string]string{"valid": validParent, "missing": "", "malformed": "invalid"} {
		t.Run(name, func(t *testing.T) {
			span, ctx := startSpanWithRemoteParent(context.Background(), "shim.create", parent)
			child, ctx := startSpan(ctx, "shim.sbxmgr.create")
			if native, ok := span.(datadogSpan); ok {
				rootID := native.span.Context().SpanID()
				if got := child.(datadogSpan).span.AsMap()[ext.MapSpanParentID]; got != rootID {
					t.Fatalf("native child parent=%v, want shim.create %d", got, rootID)
				}
				if name == "valid" {
					parentID, _ := strconv.ParseUint("ea1dc2f49db4d7a9", 16, 64)
					if got := native.span.AsMap()[ext.MapSpanParentID]; got != parentID {
						t.Fatalf("native root parent=%v, want remote %d", got, parentID)
					}
				}
			}
			ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "fixture"))
			ctx = injectTraceMetadata(ctx, initTracer())
			md, _ := metadata.FromOutgoingContext(ctx)
			value := md.Get("traceparent")
			if os.Getenv("SPACE_TRACE_EXPORTER") == "off" {
				if len(value) != 0 {
					t.Fatalf("disabled tracing propagated context: %v", value)
				}
			} else {
				if len(value) != 1 {
					t.Fatalf("missing outgoing traceparent: %v", value)
				}
				if name == "valid" && (!strings.HasPrefix(value[0], parent[:35]) || value[0] == parent) {
					t.Fatalf("remote traceparent lost: %v", value)
				}
				if native, ok := child.(datadogSpan); ok {
					if got := md.Get("x-datadog-parent-id"); len(got) != 1 || got[0] != strconv.FormatUint(native.span.Context().SpanID(), 10) {
						t.Fatalf("default native propagation lost child: %v", md)
					}
				}
			}
			if got := md.Get("authorization"); len(got) != 1 || got[0] != "fixture" {
				t.Fatalf("metadata lost: %v", md)
			}
			child.End(nil)
			span.End(nil)
		})
	}
	started := time.Now()
	stopTelemetry()
	if elapsed := time.Since(started); elapsed > telemetryShutdownBudget+300*time.Millisecond {
		t.Fatalf("shutdown took %v", elapsed)
	}
}

func TestOTLPParentageErrorsAndBaggage(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	p := &otlpTracePipeline{tracer: provider.Tracer("test"), provider: provider}
	const parent = "00-69f2ccfd00000000e88861448bd50939-ea1dc2f49db4d7a9-01"
	ctx := p.extract(context.Background(), propagation.MapCarrier{"traceparent": parent, "baggage": "job=restore"})
	root, ctx := p.start(ctx, "shim.create", attribute.String("template", "test"))
	child, ctx := p.start(ctx, "shim.sbxmgr.start")
	child.SetAttributes(attribute.String("sandbox_id", "fixture"))
	carrier := propagation.MapCarrier{}
	p.inject(ctx, carrier)
	if carrier["baggage"] != "job=restore" {
		t.Fatalf("lost baggage: %v", carrier)
	}
	child.End(errors.New("fixture error"))
	root.End(nil)
	spans := recorder.Ended()
	if len(spans) != 2 {
		t.Fatalf("spans: %d", len(spans))
	}
	if spans[0].Parent().SpanID() != spans[1].SpanContext().SpanID() || spans[1].Parent().SpanID().String() != "ea1dc2f49db4d7a9" {
		t.Fatal("broken parent chain")
	}
	if spans[0].SpanContext().TraceID().String() != "69f2ccfd00000000e88861448bd50939" {
		t.Fatal("128-bit trace ID lost")
	}
	if spans[0].Status().Code != codes.Error || len(spans[0].Events()) != 1 || spans[0].Events()[0].Name != "exception" {
		t.Fatal("missing error status/event")
	}
	if spans[1].Status().Code == codes.Error {
		t.Fatal("successful span classified as error")
	}
}

func TestOTLPConfigurationAndIdentity(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://generic:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", " http://traces:4317\n")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "\tGRPC ")
	if endpoint, err := traceEndpoint(); err != nil || endpoint != "http://traces:4317" {
		t.Fatalf("endpoint=%s err=%v", endpoint, err)
	}
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/protobuf")
	if _, err := traceEndpoint(); err == nil {
		t.Fatal("unsupported protocol accepted")
	}
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "grpc")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "https://receiver:4317")
	if _, err := traceEndpoint(); err == nil {
		t.Fatal("unsupported transport accepted")
	}
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://user:fixture-secret@receiver:badport")
	if _, err := traceEndpoint(); err == nil || strings.Contains(err.Error(), "fixture-secret") {
		t.Fatalf("expected a redacted endpoint error, got %v", err)
	}
	t.Setenv("DD_SERVICE", "legacy")
	t.Setenv("DD_ENV", "legacy-env")
	t.Setenv("DD_VERSION", "legacy-version")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.name=resource,service.version=new-version,deployment.environment.name=test")
	t.Setenv("OTEL_SERVICE_NAME", "explicit")
	res, err := otlpResource()
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"service.name": "explicit", "service.version": "new-version", "deployment.environment.name": "test"} {
		value, ok := res.Set().Value(attribute.Key(key))
		if !ok || value.AsString() != want {
			t.Fatalf("%s=%v, want %s", key, value, want)
		}
	}
	for _, key := range []attribute.Key{"service.instance.id", "host.name", "k8s.node.name", "k8s.pod.name"} {
		if value, exists := res.Set().Value(key); exists {
			t.Errorf("traces unexpectedly received generated metric identity %s=%v", key, value)
		}
	}
}

func TestOTLPSamplingConfiguration(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "grpc")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment.name=prod,malformed_no_equals,team=space")
	for _, policy := range []struct {
		name                     string
		arg                      string
		root, sampled, unsampled bool
	}{
		{"always_on", "", true, true, true},
		{"always_off", "", false, false, false},
		{"parentbased_always_on", "", true, true, false},
		{"parentbased_always_off", "", false, true, false},
		{"traceidratio", "0", false, false, false},
		{"traceidratio", "1", true, true, true},
		{"parentbased_traceidratio", "0", false, true, false},
	} {
		t.Run(policy.name+policy.arg, func(t *testing.T) {
			t.Setenv("OTEL_TRACES_SAMPLER", policy.name)
			t.Setenv("OTEL_TRACES_SAMPLER_ARG", policy.arg)
			pipeline, err := newOTLPTracePipeline()
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), telemetryShutdownBudget)
				defer cancel()
				_ = pipeline.shutdown(ctx)
			}()
			for _, input := range []struct {
				flags string
				want  bool
			}{{"", policy.root}, {"01", policy.sampled}, {"00", policy.unsampled}} {
				ctx := context.Background()
				if input.flags != "" {
					ctx = tracePropagation.Extract(ctx, propagation.MapCarrier{"traceparent": "00-69f2ccfd00000000e88861448bd50939-ea1dc2f49db4d7a9-" + input.flags})
				}
				span, _ := pipeline.start(ctx, "sampling")
				if sampled := span.(otlpSpan).SpanContext().IsSampled(); sampled != input.want {
					t.Errorf("parent=%s sampled=%t want=%t", input.flags, sampled, input.want)
				}
				span.End(nil)
			}
		})
	}
}

func TestInvalidTraceExporterFailsConfiguration(t *testing.T) {
	if action := os.Getenv("SPACE_TRACE_DISPATCH_TEST"); action != "" {
		// Exercise containerd's real flag parser and command dispatch in isolation.
		flag.CommandLine = flag.NewFlagSet("shim", flag.ExitOnError)
		os.Args = []string{"shim", "-namespace", "trace-test", "-id", "fixture", action}
		main()
		os.Exit(0)
	}
	for _, settings := range [][]string{
		{"SPACE_TRACE_EXPORTER="},
		{"SPACE_TRACE_EXPORTER=typo"},
		{"SPACE_TRACE_EXPORTER=otlp", "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=http/protobuf"},
		{"SPACE_TRACE_EXPORTER=otlp", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=https://receiver:4317"},
		{"SPACE_TRACE_EXPORTER=otlp", "OTEL_EXPORTER_OTLP_HEADERS=authorization=header-test-secret"},
		{"SPACE_TRACE_EXPORTER=otlp", "OTEL_EXPORTER_OTLP_TRACES_HEADERS=authorization=header-test-secret"},
	} {
		for _, action := range []string{"start", "delete"} {
			t.Run(action+"/"+strings.Join(settings, "/"), func(t *testing.T) {
				cmd := exec.Command(os.Args[0], "-test.run=^TestInvalidTraceExporterFailsConfiguration$")
				for _, item := range os.Environ() {
					if !strings.HasPrefix(item, "DD_") && !strings.HasPrefix(item, "OTEL_") && !strings.HasPrefix(item, "SPACE_TRACE_") {
						cmd.Env = append(cmd.Env, item)
					}
				}
				cmd.Env = append(cmd.Env, settings...)
				cmd.Env = append(cmd.Env, "SPACE_TRACE_DISPATCH_TEST="+action)
				var stderr bytes.Buffer
				cmd.Stderr = &stderr
				output, err := cmd.Output()
				if bytes.Contains(output, []byte("header-test-secret")) || strings.Contains(stderr.String(), "header-test-secret") {
					t.Fatal("header value exposed in process output")
				}
				if action == "start" {
					if err == nil || !strings.Contains(stderr.String(), "invalid tracing configuration") {
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
