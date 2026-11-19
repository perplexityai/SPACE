package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	taskAPI "github.com/containerd/containerd/api/runtime/task/v3"
	pb "github.com/perplexityai/SPACE/shim/proto/sandbox/manager/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// requiredEnvironmentClient records the cold Create it's sent, and serves
// the create-required-environment capability when supported says so.
type requiredEnvironmentClient struct {
	pb.SandboxManagerClient
	grpc.ClientConnInterface
	supported bool
	probes    int
	creates   []*pb.CreateRequest
}

func (c *requiredEnvironmentClient) Close() error { return nil }
func (c *requiredEnvironmentClient) Invoke(_ context.Context, method string, request, response any, _ ...grpc.CallOption) error {
	if method != healthpb.Health_Check_FullMethodName || request.(*healthpb.HealthCheckRequest).Service != "space.sandbox-manager.capability.create-required-environment.v1" {
		return status.Error(codes.Unimplemented, "unexpected health request")
	}
	c.probes++
	if !c.supported {
		return status.Error(codes.NotFound, "unknown capability")
	}
	response.(*healthpb.HealthCheckResponse).Status = healthpb.HealthCheckResponse_SERVING
	return nil
}
func (c *requiredEnvironmentClient) Create(_ context.Context, r *pb.CreateRequest, _ ...grpc.CallOption) (*pb.CreateResponse, error) {
	c.creates = append(c.creates, r)
	return &pb.CreateResponse{SandboxId: r.GetSandboxId()}, nil
}
func (c *requiredEnvironmentClient) Wake(context.Context, *pb.WakeRequest, ...grpc.CallOption) (*pb.WakeResponse, error) {
	return &pb.WakeResponse{}, nil
}
func (c *requiredEnvironmentClient) Start(_ context.Context, r *pb.StartRequest, _ ...grpc.CallOption) (*pb.StartResponse, error) {
	return &pb.StartResponse{SandboxId: r.GetSandboxId(), Pid: 123}, nil
}
func (c *requiredEnvironmentClient) State(_ context.Context, r *pb.StateRequest, _ ...grpc.CallOption) (*pb.StateResponse, error) {
	return &pb.StateResponse{Sandboxes: []*pb.SandboxInfo{{SandboxId: r.SandboxIds[0], Pid: ptr(uint32(123)), Lifecycle: pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STARTED.Enum()}}}, nil
}

// createPod runs the shim's Create for a pod sandbox with annotations, against
// client.
func createPod(t *testing.T, client *requiredEnvironmentClient, annotations map[string]string) error {
	t.Helper()
	bundle := t.TempDir()
	all := map[string]string{
		sandboxIDAnnotation:    "01900000-0000-7000-8000-000000000001",
		sandboxOwnerAnnotation: "review@example.test",
	}
	for key, value := range annotations {
		all[key] = value
	}
	raw, err := json.Marshal(map[string]any{"annotations": all})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(bundle, "config.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	svc := &spaceTaskService{tasks: map[string]*task{}, sandboxManagerClientFactory: func() (pb.SandboxManagerClient, io.Closer, error) { return client, client, nil }}
	_, err = svc.Create(context.Background(), &taskAPI.CreateTaskRequest{ID: "bound-create", Bundle: bundle})
	return err
}

// The Rust pod renderer pins the same file
// (bound_create_environment_reaches_the_shim_contract). A pod the gate
// requires an environment of sends it as the create's required environment,
// to a sandbox-manager that honors it; one that only records it for the
// census, in shadow, creates exactly as an unbound pod does.
func TestBoundCreateSendsTheEnvironmentThePodRequires(t *testing.T) {
	wire, err := os.ReadFile("testdata/bound-create-environment.json")
	if err != nil {
		t.Fatal(err)
	}
	var contract map[string]map[string]string
	if err := json.Unmarshal(wire, &contract); err != nil {
		t.Fatal(err)
	}
	reference := contract["on"]["space.pplx.net/environment"]
	for _, tc := range []struct {
		name, mode   string
		supported    bool
		wantCode     codes.Code
		wantRequired *string
		wantProbes   int
	}{
		{"shadow", "shadow", false, codes.OK, nil, 0},
		{"on", "on", true, codes.OK, &reference, 1},
		// An older sandbox-manager would ignore the field and boot whichever
		// build it prefers, so the create fails before it's sent.
		{"on_older_worker", "on", false, codes.Unavailable, nil, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &requiredEnvironmentClient{supported: tc.supported}
			err := createPod(t, client, contract[tc.mode])
			if status.Code(err) != tc.wantCode {
				t.Fatalf("Create: got %v, want %s", err, tc.wantCode)
			}
			if client.probes != tc.wantProbes {
				t.Fatalf("capability probes = %d, want %d", client.probes, tc.wantProbes)
			}
			if tc.wantCode != codes.OK {
				if len(client.creates) != 0 {
					t.Fatalf("sent %d creates after a refusal", len(client.creates))
				}
				return
			}
			if len(client.creates) != 1 {
				t.Fatalf("sent %d creates, want 1", len(client.creates))
			}
			got := client.creates[0].RequiredEnvironment
			switch {
			case tc.wantRequired == nil && got != nil:
				t.Fatalf("required environment = %q, want none", *got)
			case tc.wantRequired != nil && (got == nil || *got != *tc.wantRequired):
				t.Fatalf("required environment = %v, want %q", got, *tc.wantRequired)
			}
			// A required create names no runtime, so a sandbox-manager that
			// drops required_environment refuses it; any other names "fc".
			wantRuntime := "fc"
			if tc.wantRequired != nil {
				wantRuntime = ""
			}
			if runtime := client.creates[0].Runtime; runtime != wantRuntime {
				t.Fatalf("runtime = %q, want %q", runtime, wantRuntime)
			}
		})
	}
}

func TestBoundCreateRefusesAMalformedEnvironmentRequirement(t *testing.T) {
	reference := "sha256:" + "e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0"
	for _, tc := range []struct {
		name        string
		annotations map[string]string
	}{
		{"required without an environment", map[string]string{sandboxEnvironmentRequiredAnnotation: "true"}},
		{"unknown required value", map[string]string{sandboxEnvironmentAnnotation: reference, sandboxEnvironmentRequiredAnnotation: "yes"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &requiredEnvironmentClient{supported: true}
			if err := createPod(t, client, tc.annotations); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("Create: got %v, want InvalidArgument", err)
			}
			if len(client.creates) != 0 {
				t.Fatalf("sent %d creates for a malformed pod", len(client.creates))
			}
		})
	}
}

// "false" reads as not required, like an absent annotation, rather than
// failing the pod.
func TestBoundCreateTreatsFalseAsNotRequired(t *testing.T) {
	client := &requiredEnvironmentClient{supported: true}
	err := createPod(t, client, map[string]string{
		sandboxEnvironmentAnnotation:         "sha256:" + "e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0",
		sandboxEnvironmentRequiredAnnotation: "false",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if client.probes != 0 || len(client.creates) != 1 || client.creates[0].RequiredEnvironment != nil {
		t.Fatalf("probes = %d, creates = %d, want one create with no required environment", client.probes, len(client.creates))
	}
}
