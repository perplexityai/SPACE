package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/mocktracer"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
)

func TestDatadogCreateTraceBoundary(t *testing.T) {
	t.Setenv("DD_TRACE_PROPAGATION_STYLE", "tracecontext")
	mt := mocktracer.Start()
	defer mt.Stop()
	p := datadogTracePipeline{}
	for _, tc := range createTraceCases {
		t.Run(tc.name, func(t *testing.T) {
			unrelated, ctx := p.start(context.Background(), "containerd")
			create, ctx := startCreateSpan(ctx, p, tc.origin, tc.joinUntil, time.UnixMilli(1000),
				attribute.String("task_id", "attempt-one"))
			child, childCtx := p.start(ctx, "shim.sbxmgr.create")
			outgoing := propagation.MapCarrier{}
			p.inject(childCtx, outgoing)
			span := create.(datadogSpan).span
			if joined := span.Context().TraceID() == "11111111111111112222222222222222"; joined != tc.joined {
				t.Fatalf("joined=%t, want %t", joined, tc.joined)
			}
			parent := span.AsMap()[ext.MapSpanParentID]
			wantParent := uint64(0)
			if tc.joined {
				wantParent = 0x3333333333333333
			}
			if parent != wantParent {
				t.Fatalf("parent=%v, want %v", parent, wantParent)
			}
			tags := span.AsMap()
			if tags["task_id"] != "attempt-one" {
				t.Fatal("attempt identity missing")
			}
			if tc.joined || tc.linked {
				if tags[originTraceIDAttribute] != "11111111111111112222222222222222" ||
					tags[originSampledAttribute] != "true" {
					t.Fatalf("origin search attributes missing: %v", tags)
				}
			}
			childSpan := child.(datadogSpan).span
			if childSpan.AsMap()[ext.MapSpanParentID] != span.Context().SpanID() ||
				childSpan.Context().TraceID() != span.Context().TraceID() {
				t.Fatal("manager call is not a child of create")
			}
			if !strings.Contains(outgoing["traceparent"], span.Context().TraceID()) {
				t.Fatal("manager propagation lost create trace")
			}
			child.End(nil)
			create.End(nil)
			unrelated.End(nil)
		})
	}
	// Parent sampling must survive both the create join and outgoing RPC.
	for _, flags := range []string{"00", "01"} {
		t.Run("sampling/"+flags, func(t *testing.T) {
			create, ctx := startCreateSpan(context.Background(), p, strings.TrimSuffix(createOrigin, "01")+flags, "2000", time.UnixMilli(1000))
			child, ctx := p.start(ctx, "shim.sbxmgr.create")
			outgoing := propagation.MapCarrier{}
			p.inject(ctx, outgoing)
			if got := traceparentFlags(t, outgoing["traceparent"]); got != flags {
				t.Fatalf("outgoing sampling=%s, want %s", got, flags)
			}
			child.End(nil)
			create.End(nil)
		})
	}
}

func TestDatadogTraceAgentAddrHonorsTraceAgentPort(t *testing.T) {
	cases := []struct {
		name, host, port, want string
	}{
		{"default port", "10.0.0.5", "", "10.0.0.5:8126"},
		{"configured port", "10.0.0.5", "18126", "10.0.0.5:18126"},
		{"surrounding space", "10.0.0.5", " 9126 ", "10.0.0.5:9126"},
		{"ipv6 host", "fd00::5", "18126", "[fd00::5]:18126"},
		{"bracketed ipv6 host", "[fd00::5]", "18126", "[fd00::5]:18126"},
		{"bracketed ipv6 zone", "[fe80::5%eth0]", "18126", "[fe80::5%eth0]:18126"},
		{"non-numeric port", "10.0.0.5", "apm", "10.0.0.5:8126"},
		{"zero port", "10.0.0.5", "0", "10.0.0.5:8126"},
		{"out of range port", "10.0.0.5", "70000", "10.0.0.5:8126"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := datadogTraceAgentAddr(tc.host, tc.port); got != tc.want {
				t.Fatalf("datadogTraceAgentAddr(%q, %q) = %q, want %q", tc.host, tc.port, got, tc.want)
			}
		})
	}
}
