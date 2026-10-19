package main

// Opt-in process fixtures for scripts/trace_parity.py and trace_portability.py.
// Routine Go tests do not open these listeners or publish external telemetry.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	taskAPI "github.com/containerd/containerd/api/runtime/task/v3"
	pb "github.com/perplexityai/SPACE/shim/proto/sandbox/manager/v1"
	"go.opentelemetry.io/otel/attribute"
	collector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestTraceParityFixture(t *testing.T) {
	if os.Getenv("SPACE_TRACE_PARITY_MODE") == "" {
		return
	}
	if os.Getenv("SPACE_TRACE_PARITY_MODE") == "operation-errors" {
		traceOperationErrors(t)
		stopTelemetry()
		return
	}
	for _, failed := range []bool{false, true} {
		label := "success"
		var failure error
		if failed {
			label = "error"
			failure = errors.New("fixture failure")
		}
		root, ctx := startSpan(context.Background(), "shim.create", attribute.String("parity.case", label), attribute.String("sandbox_id", "parity-sandbox"))
		child, _ := startSpan(ctx, "shim.sbxmgr.create", attribute.String("template", "parity"))
		child.End(failure)
		root.End(failure)
	}
	logShim("trace parity workload completed")
	stopTelemetry()
}

type failingRestoreClient struct{ pb.SandboxManagerClient }

func (failingRestoreClient) Start(context.Context, *pb.StartRequest, ...grpc.CallOption) (*pb.StartResponse, error) {
	return nil, status.Error(codes.Unavailable, "fixture restore failure")
}

// Exercise real Create/redirect error exits; the external receiver checks spans.
func traceOperationErrors(t *testing.T) {
	bundle := t.TempDir()
	spec := ociSpec{Annotations: map[string]string{
		sandboxIDAnnotation:         "01a0c6f3-de33-7635-bb94-572d5acb30c5",
		sandboxCheckpointAnnotation: "latest",
	}}
	wire, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "config.json"), wire, 0600); err != nil {
		t.Fatal(err)
	}
	service := &spaceTaskService{
		tasks: make(map[string]*task),
		sandboxManagerClientFactory: func() (pb.SandboxManagerClient, io.Closer, error) {
			return failingRestoreClient{}, fakeConn{}, nil
		},
	}
	if _, err := service.Create(context.Background(), &taskAPI.CreateTaskRequest{ID: "fixture", Bundle: bundle}); status.Code(err) != codes.Unavailable {
		t.Fatalf("restore error lost: %v", err)
	}
	if len(service.tasks) != 0 {
		t.Fatal("failed restore retained a task")
	}
	result := make(chan dataPlaneRedirectResult, 1)
	// The opt-in process suite exercises the real five-second timeout, without
	// creating network namespaces or touching the host sandbox-manager socket.
	prepareDataPlaneRedirect(context.Background(), "fixture", filepath.Join(bundle, "missing-netns"), "fixture", result)
	if got := <-result; got.err == nil || got.podIP != "" {
		t.Fatalf("missing netns accepted: %+v", got)
	}
}

type traceRecordingReceiver struct {
	collector.UnimplementedTraceServiceServer
	mutex sync.Mutex
}

func (r *traceRecordingReceiver) Export(ctx context.Context, request *collector.ExportTraceServiceRequest) (*collector.ExportTraceServiceResponse, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	wire, err := protojson.Marshal(request)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(os.Getenv("SPACE_TRACE_FIXTURE_OUTPUT"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	_, writeErr := file.Write(append(wire, '\n'))
	closeErr := file.Close()
	if writeErr != nil {
		return nil, writeErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if os.Getenv("SPACE_TRACE_RECEIVER_FAIL") == "stall" {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if os.Getenv("SPACE_TRACE_RECEIVER_FAIL") == "1" {
		return nil, status.Error(codes.Unavailable, "fixture receiver outage")
	}
	return &collector.ExportTraceServiceResponse{}, nil
}

func TestTraceFixtureServer(t *testing.T) {
	role := os.Getenv("SPACE_TRACE_FIXTURE_ROLE")
	if role == "" {
		return
	}
	address := os.Getenv("SPACE_TRACE_FIXTURE_LISTEN")
	if address == "" {
		address = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	if role != "receiver" {
		listener.Close()
		t.Fatalf("invalid fixture role: %s", role)
	}
	server := grpc.NewServer()
	collector.RegisterTraceServiceServer(server, &traceRecordingReceiver{})
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(listener) }()
	ready := os.Getenv("SPACE_TRACE_FIXTURE_READY")
	if err := os.WriteFile(ready+".tmp", []byte(listener.Addr().String()), 0600); err != nil {
		server.Stop()
		t.Fatal(err)
	}
	if err := os.Rename(ready+".tmp", ready); err != nil {
		server.Stop()
		t.Fatal(err)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, os.Interrupt)
	defer signal.Stop(signals)
	select {
	case <-signals:
	case <-time.After(5 * time.Minute):
		server.Stop()
		t.Fatal("fixture deadline exceeded")
	}
	server.GracefulStop()
	if err := <-serveErrors; err != nil {
		t.Fatal(fmt.Errorf("fixture server: %w", err))
	}
}
