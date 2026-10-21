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

type activationPolicyClient struct {
	pb.SandboxManagerClient
	grpc.ClientConnInterface
	installedSeconds int64
	runningSeconds   int64
	supportsPolicy   bool
}

func (c *activationPolicyClient) Close() error { return nil }
func (c *activationPolicyClient) Invoke(_ context.Context, method string, request, response any, _ ...grpc.CallOption) error {
	if method != healthpb.Health_Check_FullMethodName || request.(*healthpb.HealthCheckRequest).Service != "space.sandbox-manager.capability.activation-policy.v1" {
		return status.Error(codes.Unimplemented, "unexpected health request")
	}
	if !c.supportsPolicy {
		return status.Error(codes.NotFound, "unknown capability")
	}
	response.(*healthpb.HealthCheckResponse).Status = healthpb.HealthCheckResponse_SERVING
	return nil
}
func (c *activationPolicyClient) Start(_ context.Context, r *pb.StartRequest, _ ...grpc.CallOption) (*pb.StartResponse, error) {
	if r.ActivationAutoLifecycle != nil {
		c.installedSeconds = r.ActivationAutoLifecycle.Stages[0].At.Seconds
	}
	c.runningSeconds = c.installedSeconds
	return &pb.StartResponse{SandboxId: r.GetSandboxId(), Pid: 123}, nil
}
func (c *activationPolicyClient) Create(_ context.Context, r *pb.CreateRequest, _ ...grpc.CallOption) (*pb.CreateResponse, error) {
	if r.AutoLifecycle != nil {
		c.installedSeconds = r.AutoLifecycle.Stages[0].At.Seconds
	}
	return &pb.CreateResponse{SandboxId: r.GetSandboxId()}, nil
}
func (c *activationPolicyClient) Wake(context.Context, *pb.WakeRequest, ...grpc.CallOption) (*pb.WakeResponse, error) {
	c.runningSeconds = c.installedSeconds
	return &pb.WakeResponse{}, nil
}
func (c *activationPolicyClient) State(_ context.Context, r *pb.StateRequest, _ ...grpc.CallOption) (*pb.StateResponse, error) {
	return &pb.StateResponse{Sandboxes: []*pb.SandboxInfo{{SandboxId: r.SandboxIds[0], Pid: ptr(uint32(123)), Lifecycle: pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STARTED.Enum()}}}, nil
}

const unsupportedNetworkPolicy = `{"version":"v1","egress":{"default_action":"deny"},"future_field":[]}`

func TestActivationConsumesCurrentPolicy(t *testing.T) {
	for _, tc := range []struct {
		name, checkpoint  string
		policy, supported bool
		wantSeconds       int64
		wantCode          codes.Code
		networkPolicy     string
	}{
		{"cold", "", true, true, 60, codes.OK, ""},
		{"suspend", "suspend", true, true, 60, codes.OK, ""},
		{"checkpoint", "f-example", true, true, 60, codes.OK, ""},
		{"older_worker", "suspend", true, false, 0, codes.Unavailable, ""},
		{"omitted_policy", "suspend", false, false, 30, codes.OK, ""},
		// Only a cold create consumes the network-policy annotation.
		{"cold_unsupported_network_policy", "", true, true, 0, codes.InvalidArgument, unsupportedNetworkPolicy},
		{"suspend_unsupported_network_policy", "suspend", true, true, 60, codes.OK, unsupportedNetworkPolicy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := t.TempDir()
			annotations := map[string]string{
				sandboxIDAnnotation:         "01900000-0000-7000-8000-000000000001",
				sandboxCheckpointAnnotation: tc.checkpoint,
				sandboxOwnerAnnotation:      "review@example.test",
			}
			if tc.policy {
				annotations[sandboxAutoLifecycleAuthorityAnnotation] = "cluster"
				annotations[sandboxAutoLifecycleAnnotation] = `{"wake":{"on":["access"]},"condition_sets":{"access_idle":{"kind":"inactivity","activity_source":"access"}},"stages":[{"condition_set":"access_idle","at":"60s","action":"suspend"}]}`
			}
			if tc.networkPolicy != "" {
				annotations[sandboxNetworkPolicyAnnotation] = tc.networkPolicy
				annotations[sandboxNetworkPolicyGenerationAnnotation] = "1"
			}
			raw, err := json.Marshal(map[string]any{"annotations": annotations})
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(bundle, "config.json"), raw, 0600); err != nil {
				t.Fatal(err)
			}
			client := &activationPolicyClient{installedSeconds: 30, supportsPolicy: tc.supported}
			svc := &spaceTaskService{tasks: map[string]*task{}, sandboxManagerClientFactory: func() (pb.SandboxManagerClient, io.Closer, error) { return client, client, nil }}
			_, err = svc.Create(context.Background(), &taskAPI.CreateTaskRequest{ID: "activation-policy", Bundle: bundle})
			if status.Code(err) != tc.wantCode {
				t.Fatalf("Create: got %v, want %s", err, tc.wantCode)
			}
			if client.runningSeconds != tc.wantSeconds {
				t.Fatalf("activated with policy %ds, want %ds", client.runningSeconds, tc.wantSeconds)
			}
		})
	}
}
