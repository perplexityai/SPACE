package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	eventstypes "github.com/containerd/containerd/api/events"
	taskAPI "github.com/containerd/containerd/api/runtime/task/v3"
	tasktypes "github.com/containerd/containerd/api/types/task"
	"github.com/containerd/containerd/v2/core/events"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	gatewaypb "github.com/perplexityai/SPACE/shim/proto/net/gateway/v1"
	netpb "github.com/perplexityai/SPACE/shim/proto/net/policy/v1"
	pb "github.com/perplexityai/SPACE/shim/proto/sandbox/manager/v1"
	operationspb "github.com/perplexityai/SPACE/shim/proto/spaced/operations/v1"
)

type fakeConn struct{}

func (fakeConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (fakeConn) Write(b []byte) (int, error)      { return len(b), nil }
func (fakeConn) Close() error                     { return nil }
func (fakeConn) LocalAddr() net.Addr              { return nil }
func (fakeConn) RemoteAddr() net.Addr             { return nil }
func (fakeConn) SetDeadline(time.Time) error      { return nil }
func (fakeConn) SetReadDeadline(time.Time) error  { return nil }
func (fakeConn) SetWriteDeadline(time.Time) error { return nil }

// Contract test: the SPACE API matches on "[space-err=<Code>]" in the shim's
// error message to detect permanent sandbox creation failures. The gRPC
// status code itself is lost at the containerd CRI boundary (see
// annotateStatus comment in space.go), so the namespaced marker is the
// contract surface.
func TestAnnotateStatusSbxManagerContract(t *testing.T) {
	tests := []struct {
		name string
		code codes.Code
	}{
		{"NotFound", codes.NotFound},
		{"Unavailable", codes.Unavailable},
		{"ResourceExhausted", codes.ResourceExhausted},
		{"Internal", codes.Internal},
		// sbx-manager answers these three when the node it landed on cannot
		// execute the sandbox at all (a missing guest kernel, an incomplete
		// runtime installation, a rejected request shape, a path it refuses on
		// principle). They are the codes that distinguish "fail this create"
		// from "retry it", so the marker has to carry them intact.
		{"FailedPrecondition", codes.FailedPrecondition},
		{"InvalidArgument", codes.InvalidArgument},
		{"PermissionDenied", codes.PermissionDenied},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			grpcErr := status.Errorf(tt.code, "some sbx-manager error")
			shimErr := annotateStatus("sbx-manager Create", grpcErr)
			msg := shimErr.Error()

			wantTag := fmt.Sprintf("[space-err=%s]", tt.code.String())
			if !strings.Contains(msg, wantTag) {
				t.Fatalf("expected %q in message, got %q", wantTag, msg)
			}

			st, ok := status.FromError(shimErr)
			if !ok {
				t.Fatalf("expected gRPC status, got %T", shimErr)
			}
			if st.Code() != tt.code {
				t.Fatalf("expected code %v, got %v", tt.code, st.Code())
			}
		})
	}
}

func TestCurrentNodeIDNormalizesKubernetesNodeName(t *testing.T) {
	t.Setenv("SPACE_NODE_ID", "ip-10-0-0-1.ec2.internal")

	got := currentNodeID()
	if got == nil {
		t.Fatal("currentNodeID returned nil")
	}
	if *got != "cf764732-f94b-5b17-a7c2-8aba4d40e511" {
		t.Fatalf("currentNodeID = %q, want deterministic worker node UUID", *got)
	}
}

func TestRouteIdentityDeclaresKubernetesSubstrate(t *testing.T) {
	generation := int64(1)

	identity := routeIdentity(nil, nil, &generation, nil, nil)

	if got, want := identity.GetSubstrateKind(), gatewaypb.SandboxSubstrateKind_SANDBOX_SUBSTRATE_KIND_KUBERNETES; got != want {
		t.Fatalf("substrate kind = %v, want %v", got, want)
	}
}

func TestCurrentNodeIDPreservesUuid(t *testing.T) {
	t.Setenv("SPACE_NODE_ID", "019f8000-0000-7000-8000-0000000000aa")

	got := currentNodeID()
	if got == nil {
		t.Fatal("currentNodeID returned nil")
	}
	if *got != "019f8000-0000-7000-8000-0000000000aa" {
		t.Fatalf("currentNodeID = %q, want original UUID", *got)
	}
}

func TestDialWithConnectRetryRetriesSocketConnectErrors(t *testing.T) {
	policy := connectRetryPolicy{
		budget:        time.Second,
		initialDelay:  0,
		maxDelay:      0,
		jitterPercent: 0,
	}
	attempts := 0
	sleeps := 0

	conn, err := dialWithConnectRetry(
		context.Background(),
		"sandbox-manager",
		sbxMgrSocket,
		policy,
		func(context.Context) (net.Conn, error) {
			attempts++
			if attempts == 1 {
				return nil, os.ErrNotExist
			}
			if attempts == 2 {
				return nil, syscall.ECONNREFUSED
			}
			return fakeConn{}, nil
		},
		func(context.Context, time.Duration) error {
			sleeps++
			return nil
		},
	)
	if err != nil {
		t.Fatalf("dialWithConnectRetry returned error: %v", err)
	}
	_ = conn.Close()
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	if sleeps != 2 {
		t.Fatalf("sleeps = %d, want 2", sleeps)
	}
}

func TestDialWithConnectRetryDoesNotRetryNonConnectError(t *testing.T) {
	boom := errors.New("permission denied")
	attempts := 0

	_, err := dialWithConnectRetry(
		context.Background(),
		"sandbox-manager",
		sbxMgrSocket,
		defaultConnectRetryPolicy,
		func(context.Context) (net.Conn, error) {
			attempts++
			return nil, boom
		},
		func(context.Context, time.Duration) error {
			t.Fatal("sleep should not be called for non-retryable errors")
			return nil
		},
	)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestDialWithConnectRetryExhaustsBudget(t *testing.T) {
	policy := connectRetryPolicy{
		budget:        time.Nanosecond,
		initialDelay:  time.Millisecond,
		maxDelay:      time.Millisecond,
		jitterPercent: 0,
	}
	attempts := 0

	_, err := dialWithConnectRetry(
		context.Background(),
		"sandbox-manager",
		sbxMgrSocket,
		policy,
		func(context.Context) (net.Conn, error) {
			attempts++
			return nil, syscall.ECONNREFUSED
		},
		func(context.Context, time.Duration) error {
			t.Fatal("sleep should not be called after budget exhaustion")
			return nil
		},
	)
	if err == nil {
		t.Fatal("expected retry exhaustion error")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestRequestedSandboxID(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        string
	}{
		{
			name: "annotation takes precedence",
			annotations: map[string]string{
				sandboxIDAnnotation:              "019ddc5a-0ab7-7722-85ab-aa26ccd74259",
				"io.kubernetes.cri.sandbox-name": "space-sbx-00000000000000000000000000000000",
			},
			want: "019ddc5a-0ab7-7722-85ab-aa26ccd74259",
		},
		{
			name: "generation pod name fallback",
			annotations: map[string]string{
				"io.kubernetes.cri.sandbox-name": "space-sbx-019ddc5a0ab7772285abaa26ccd74259-g1",
			},
			want: "019ddc5a-0ab7-7722-85ab-aa26ccd74259",
		},
		{
			name: "legacy pod name fallback",
			annotations: map[string]string{
				"io.kubernetes.cri.sandbox-name": "space-sbx-019ddc5a0ab7772285abaa26ccd74259",
			},
			want: "019ddc5a-0ab7-7722-85ab-aa26ccd74259",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := &ociSpec{Annotations: tt.annotations}
			got := spec.requestedSandboxID()
			if got == nil {
				t.Fatalf("requestedSandboxID() = nil, want %s", tt.want)
			}
			if got.String() != tt.want {
				t.Fatalf("requestedSandboxID() = %s, want %s", got.String(), tt.want)
			}
		})
	}
}

func TestSandboxEnv(t *testing.T) {
	tests := []struct {
		name string
		env  []string
		want map[string]string
	}{
		{name: "nil process", want: nil},
		{
			name: "parses process env",
			env: []string{
				"PATH=/usr/bin",
				"SPACE_TOKEN=kept",
				"MALFORMED",
				"=empty-key",
				"VALUE_WITH_EQUALS=a=b=c",
				"PATH=/bin",
			},
			want: map[string]string{
				"PATH":              "/bin",
				"SPACE_TOKEN":       "kept",
				"VALUE_WITH_EQUALS": "a=b=c",
			},
		},
		{
			name: "ignores entries without keys",
			env:  []string{"MALFORMED", "=empty-key"},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := &ociSpec{}
			if tt.env != nil {
				spec.Process = &ociProcess{Env: tt.env}
			}
			if got := spec.sandboxEnv(); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("sandboxEnv() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestSpacedProcessExitCode(t *testing.T) {
	signal := int32(9)
	tests := []struct {
		name string
		exit *operationspb.ProcessExit
		want uint32
	}{
		{
			name: "exited",
			exit: &operationspb.ProcessExit{Outcome: &operationspb.ProcessExit_Exited{
				Exited: &operationspb.ProcessExited{ExitCode: 17},
			}},
			want: 17,
		},
		{
			name: "signaled",
			exit: &operationspb.ProcessExit{Outcome: &operationspb.ProcessExit_Signaled{
				Signaled: &operationspb.ProcessSignaled{Signal: &signal},
			}},
			want: 137,
		},
		{
			name: "timed out without signal",
			exit: &operationspb.ProcessExit{Outcome: &operationspb.ProcessExit_TimedOut{
				TimedOut: &operationspb.ProcessTimedOut{},
			}},
			want: 124,
		},
		{
			name: "unknown",
			exit: &operationspb.ProcessExit{Outcome: &operationspb.ProcessExit_Unknown{
				Unknown: &operationspb.ProcessUnknown{},
			}},
			want: 255,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := spacedProcessExitCode(tt.exit); got != tt.want {
				t.Fatalf("spacedProcessExitCode() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestSandboxCheckpoint(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        *string
	}{
		{
			name:        "missing",
			annotations: map[string]string{},
		},
		{
			name: "empty",
			annotations: map[string]string{
				sandboxCheckpointAnnotation: " ",
			},
		},
		{
			name: "trims suspend checkpoint",
			annotations: map[string]string{
				sandboxCheckpointAnnotation: " suspend ",
			},
			want: ptr("suspend"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := &ociSpec{Annotations: tt.annotations}
			got := spec.sandboxCheckpoint()
			if tt.want == nil {
				if got != nil {
					t.Fatalf("sandboxCheckpoint() = %q, want nil", *got)
				}
				return
			}
			if got == nil || *got != *tt.want {
				t.Fatalf("sandboxCheckpoint() = %v, want %q", got, *tt.want)
			}
		})
	}
}

func TestSandboxResourceAnnotationsOverrideOCIResources(t *testing.T) {
	cpuQuota := int64(100000)
	cpuPeriod := uint64(100000)
	memoryLimit := int64(1024 * 1024 * 1024)
	spec := &ociSpec{
		Annotations: map[string]string{
			sandboxVCPUAnnotation:      "2",
			sandboxMemoryMiBAnnotation: "8192",
		},
		Linux: &ociLinux{Resources: &ociLinuxResources{
			CPU:    &ociLinuxCPU{Quota: &cpuQuota, Period: &cpuPeriod},
			Memory: &ociLinuxMemory{Limit: &memoryLimit},
		}},
	}

	vcpu, err := spec.sandboxVCPU()
	if err != nil {
		t.Fatalf("sandboxVCPU() error = %v", err)
	}
	memoryMiB, err := spec.sandboxMemoryMiB()
	if err != nil {
		t.Fatalf("sandboxMemoryMiB() error = %v", err)
	}

	if vcpu != 2 {
		t.Fatalf("sandboxVCPU() = %d, want 2", vcpu)
	}
	if memoryMiB != 8192 {
		t.Fatalf("sandboxMemoryMiB() = %d, want 8192", memoryMiB)
	}
}

func TestSandboxResourceAnnotationsRejectInvalidValues(t *testing.T) {
	tests := []struct {
		name       string
		annotation string
		parse      func(*ociSpec) (uint32, error)
	}{
		{
			name:       "vcpu",
			annotation: sandboxVCPUAnnotation,
			parse:      func(spec *ociSpec) (uint32, error) { return spec.sandboxVCPU() },
		},
		{
			name:       "memory",
			annotation: sandboxMemoryMiBAnnotation,
			parse:      func(spec *ociSpec) (uint32, error) { return spec.sandboxMemoryMiB() },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := &ociSpec{Annotations: map[string]string{test.annotation: "0"}}

			_, err := test.parse(spec)

			if err == nil {
				t.Fatal("expected zero annotation value to be rejected")
			}
		})
	}
}

func TestPositiveUint64Annotation(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        *uint64
		wantErr     bool
	}{
		{name: "missing", annotations: map[string]string{}},
		{name: "valid", annotations: map[string]string{sandboxDiskIOMaxOperationsPerSecAnnotation: "5000"}, want: ptr(uint64(5000))},
		{name: "zero", annotations: map[string]string{sandboxDiskIOMaxOperationsPerSecAnnotation: "0"}, wantErr: true},
		{name: "negative", annotations: map[string]string{sandboxDiskIOMaxOperationsPerSecAnnotation: "-1"}, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := (&ociSpec{Annotations: test.annotations}).positiveUint64Annotation(sandboxDiskIOMaxOperationsPerSecAnnotation)
			if (err != nil) != test.wantErr {
				t.Fatalf("positiveUint64Annotation() error = %v, wantErr %t", err, test.wantErr)
			}
			if test.want == nil {
				if got != nil {
					t.Fatalf("positiveUint64Annotation() = %d, want nil", *got)
				}
				return
			}
			if got == nil || *got != *test.want {
				t.Fatalf("positiveUint64Annotation() = %v, want %d", got, *test.want)
			}
		})
	}
}

func TestSandboxSuspendRestoreStorageURI(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        *string
	}{
		{
			name:        "missing",
			annotations: map[string]string{},
		},
		{
			name: "empty",
			annotations: map[string]string{
				sandboxSuspendRestoreStorageURIAnnotation: " ",
			},
		},
		{
			name: "trims URI",
			annotations: map[string]string{
				sandboxSuspendRestoreStorageURIAnnotation: " s3://space/snapshot?region=us-east-1 ",
			},
			want: ptr("s3://space/snapshot?region=us-east-1"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := &ociSpec{Annotations: tt.annotations}
			got := spec.sandboxSuspendRestoreStorageURI()
			if tt.want == nil {
				if got != nil {
					t.Fatalf("sandboxSuspendRestoreStorageURI() = %q, want nil", *got)
				}
				return
			}
			if got == nil || *got != *tt.want {
				t.Fatalf("sandboxSuspendRestoreStorageURI() = %v, want %q", got, *tt.want)
			}
		})
	}
}

func TestSandboxArtifactRestoreAsNew(t *testing.T) {
	const valid = `{"sourceSandboxId":"019f8000-0000-7000-8000-000000000002","artifactSnapshotId":"019f8000-0000-7000-8000-000000000003","targetOwner":"user:019f8000-0000-7000-8000-000000000004"}`
	// Keep this literal aligned with the annotation emitted by the Rust pod builder.
	spec := &ociSpec{Annotations: map[string]string{"space.pplx.net/fork-disk-restore": valid}}

	context, err := spec.sandboxArtifactRestoreAsNew()
	if err != nil {
		t.Fatalf("sandboxArtifactRestoreAsNew() error = %v", err)
	}
	if context == nil ||
		context.SourceSandboxId != "019f8000-0000-7000-8000-000000000002" ||
		context.ArtifactSnapshotId != "019f8000-0000-7000-8000-000000000003" ||
		context.ArtifactKind != pb.ArtifactKind_ARTIFACT_KIND_DISK {
		t.Fatalf("sandboxArtifactRestoreAsNew() = %#v", context)
	}

	spec.Annotations[sandboxArtifactRestoreAsNewAnnotation] = `{"artifactSnapshotId":"019f8000-0000-7000-8000-000000000003","targetOwner":"user:019f8000-0000-7000-8000-000000000004"}`
	if _, err := spec.sandboxArtifactRestoreAsNew(); err == nil {
		t.Fatal("sandboxArtifactRestoreAsNew() accepted an invalid context")
	}
}

func TestArtifactRestoreAsNewUsesColdStartRPC(t *testing.T) {
	const valid = `{"sourceSandboxId":"019f8000-0000-7000-8000-000000000002","artifactSnapshotId":"019f8000-0000-7000-8000-000000000003","targetOwner":"user:019f8000-0000-7000-8000-000000000004"}`
	spec := &ociSpec{Annotations: map[string]string{sandboxArtifactRestoreAsNewAnnotation: valid}}

	checkpoint, artifactRestoreAsNew, useStartRPC, err := spec.sandboxStartIntent()
	if err != nil {
		t.Fatalf("sandboxStartIntent() error = %v", err)
	}
	if checkpoint != nil {
		t.Fatalf("artifact restore-as-new checkpoint = %q, want nil", *checkpoint)
	}
	if artifactRestoreAsNew == nil {
		t.Fatal("artifact restore-as-new context is nil")
	}
	if !useStartRPC {
		t.Fatal("artifact restore-as-new would bypass Start and lose its restore context")
	}
	if got := sandboxRestoreModeForLog(checkpoint, artifactRestoreAsNew); got != "artifact_disk" {
		t.Fatalf("artifact restore-as-new log mode = %q, want artifact_disk", got)
	}
}

func TestSandboxSuspendRestoreSourceNode(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        *string
	}{
		{name: "missing", annotations: map[string]string{}, want: nil},
		{
			name: "empty",
			annotations: map[string]string{
				sandboxSuspendRestoreSourceNodeAnnotation: " ",
			},
			want: nil,
		},
		{
			name: "trims node name",
			annotations: map[string]string{
				sandboxSuspendRestoreSourceNodeAnnotation: " space-worker-a ",
			},
			want: ptr("space-worker-a"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := &ociSpec{Annotations: tt.annotations}
			got := spec.sandboxSuspendRestoreSourceNode()
			if tt.want == nil {
				if got != nil {
					t.Fatalf("sandboxSuspendRestoreSourceNode() = %q, want nil", *got)
				}
				return
			}
			if got == nil || *got != *tt.want {
				t.Fatalf("sandboxSuspendRestoreSourceNode() = %v, want %q", got, *tt.want)
			}
		})
	}
}

func TestSandboxExecutionGeneration(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        *int64
		wantErr     bool
	}{
		{name: "missing", annotations: map[string]string{}, want: nil},
		{
			name: "empty",
			annotations: map[string]string{
				sandboxExecutionGenerationAnnotation: " ",
			},
			want: nil,
		},
		{
			name: "valid",
			annotations: map[string]string{
				sandboxExecutionGenerationAnnotation: "2",
			},
			want: ptr(int64(2)),
		},
		{
			name: "invalid",
			annotations: map[string]string{
				sandboxExecutionGenerationAnnotation: "g2",
			},
			wantErr: true,
		},
		{
			name: "non-positive",
			annotations: map[string]string{
				sandboxExecutionGenerationAnnotation: "0",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := &ociSpec{Annotations: tt.annotations}
			got, err := spec.sandboxExecutionGeneration()
			if tt.wantErr {
				if err == nil {
					t.Fatal("sandboxExecutionGeneration() error = nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("sandboxExecutionGeneration() error = %v", err)
			}
			if tt.want == nil {
				if got != nil {
					t.Fatalf("sandboxExecutionGeneration() = %d, want nil", *got)
				}
				return
			}
			if got == nil || *got != *tt.want {
				t.Fatalf("sandboxExecutionGeneration() = %v, want %d", got, *tt.want)
			}
		})
	}
}

func TestSandboxOwner(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        string
		wantErr     string
	}{
		{
			name: "valid",
			annotations: map[string]string{
				sandboxOwnerAnnotation: "user:019de674-63d7-7ef0-a588-8e106a878093",
			},
			want: "user:019de674-63d7-7ef0-a588-8e106a878093",
		},
		{
			name: "trims whitespace",
			annotations: map[string]string{
				sandboxOwnerAnnotation: " root@local ",
			},
			want: "root@local",
		},
		{
			name: "falls back to legacy sandbox user annotation",
			annotations: map[string]string{
				sandboxUserAnnotation: " deploy@local ",
			},
			want: "deploy@local",
		},
		{
			name: "prefers sandbox owner annotation",
			annotations: map[string]string{
				sandboxOwnerAnnotation: "user:019de674-63d7-7ef0-a588-8e106a878093",
				sandboxUserAnnotation:  "deploy@local",
			},
			want: "user:019de674-63d7-7ef0-a588-8e106a878093",
		},
		{
			name:        "missing",
			annotations: map[string]string{},
			wantErr:     sandboxUserAnnotation,
		},
		{
			name: "empty",
			annotations: map[string]string{
				sandboxOwnerAnnotation: " ",
			},
			wantErr: sandboxOwnerAnnotation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := &ociSpec{Annotations: tt.annotations}
			got, err := spec.sandboxOwner()
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("sandboxOwner() error = nil, want %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("sandboxOwner() error = %q, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("sandboxOwner() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("sandboxOwner() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSandboxVolumeEncryption(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        bool
		wantErr     string
	}{
		{
			name:        "absent means plaintext",
			annotations: map[string]string{},
			want:        false,
		},
		{
			name: "empty means plaintext",
			annotations: map[string]string{
				sandboxVolumeEncryptionAnnotation: "  ",
			},
			want: false,
		},
		{
			name: "explicit false",
			annotations: map[string]string{
				sandboxVolumeEncryptionAnnotation: "false",
			},
			want: false,
		},
		{
			name: "true",
			annotations: map[string]string{
				sandboxVolumeEncryptionAnnotation: "true",
			},
			want: true,
		},
		{
			name: "trims whitespace",
			annotations: map[string]string{
				sandboxVolumeEncryptionAnnotation: " true ",
			},
			want: true,
		},
		{
			// A malformed value must fail the create rather than silently
			// downgrade a BYOK-mandated sandbox to plaintext.
			name: "malformed value is rejected",
			annotations: map[string]string{
				sandboxVolumeEncryptionAnnotation: "yes",
			},
			wantErr: sandboxVolumeEncryptionAnnotation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := &ociSpec{Annotations: tt.annotations}
			got, err := spec.sandboxVolumeEncryption()
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("sandboxVolumeEncryption() error = nil, want %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("sandboxVolumeEncryption() error = %q, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("sandboxVolumeEncryption() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("sandboxVolumeEncryption() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSandboxRootfsSizeMb(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        *uint32
		wantErr     string
	}{
		{
			name:        "missing",
			annotations: map[string]string{},
		},
		{
			name: "empty",
			annotations: map[string]string{
				sandboxDiskMibAnnotation: "",
			},
		},
		{
			name: "valid",
			annotations: map[string]string{
				sandboxDiskMibAnnotation: "8192",
			},
			want: ptr(uint32(8192)),
		},
		{
			name: "zero",
			annotations: map[string]string{
				sandboxDiskMibAnnotation: "0",
			},
			wantErr: "must be positive",
		},
		{
			name: "invalid",
			annotations: map[string]string{
				sandboxDiskMibAnnotation: "large",
			},
			wantErr: "must be a positive uint32",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := &ociSpec{Annotations: tt.annotations}
			got, err := spec.sandboxRootfsSizeMb()
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("sandboxRootfsSizeMb() error = nil, want %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("sandboxRootfsSizeMb() error = %q, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("sandboxRootfsSizeMb() error = %v", err)
			}
			if tt.want == nil {
				if got != nil {
					t.Fatalf("sandboxRootfsSizeMb() = %v, want nil", *got)
				}
				return
			}
			if got == nil || *got != *tt.want {
				t.Fatalf("sandboxRootfsSizeMb() = %v, want %v", got, *tt.want)
			}
		})
	}
}

func TestSandboxCreateBootMode(t *testing.T) {
	if got := sandboxCreateBootMode(nil); got != pb.BootMode_BOOT_MODE_UNSPECIFIED {
		t.Fatalf("sandboxCreateBootMode(nil) = %v, want unspecified", got)
	}
	diskMib := uint32(32768)
	if got := sandboxCreateBootMode(&diskMib); got != pb.BootMode_BOOT_MODE_COLD {
		t.Fatalf("sandboxCreateBootMode(disk) = %v, want cold", got)
	}
}

func TestSandboxAutoLifecycleUsesConditionSetIDs(t *testing.T) {
	spec := &ociSpec{
		Annotations: map[string]string{
			sandboxAutoLifecycleAnnotation: `{
				"wake": {"on": ["access", "command"]},
				"condition_sets": {
					"access_idle": {
						"kind": "inactivity",
						"activity_source": "access"
					},
					"command_idle": {
						"kind": "inactivity",
						"activity_source": "command",
						"max_guest_idle_extension": "5m"
					},
					"guest_activity": {
						"kind": "guest_activity"
					}
				},
				"stages": [
					{"condition_set": "access_idle", "at": "30s", "action": "pause"},
					{"condition_set": "command_idle", "at": "1m", "action": "stop"}
				]
			}`,
		},
	}

	policy, err := spec.sandboxAutoLifecycle()
	if err != nil {
		t.Fatalf("sandboxAutoLifecycle() error = %v", err)
	}

	conditionSets := map[pb.AutoLifecycleConditionSetId]*pb.AutoLifecycleConditionSet{}
	for _, conditionSet := range policy.GetConditionSets() {
		conditionSets[conditionSet.GetId()] = conditionSet
	}

	accessIdle := conditionSets[pb.AutoLifecycleConditionSetId_AUTO_LIFECYCLE_CONDITION_SET_ID_ACCESS_IDLE]
	if accessIdle == nil {
		t.Fatalf("missing access_idle condition set")
	}
	if got, want := accessIdle.GetActivitySource(), pb.AutoLifecycleActivitySource_AUTO_LIFECYCLE_ACTIVITY_SOURCE_ACCESS; got != want {
		t.Fatalf("access_idle activity source = %v, want %v", got, want)
	}

	commandIdle := conditionSets[pb.AutoLifecycleConditionSetId_AUTO_LIFECYCLE_CONDITION_SET_ID_COMMAND_IDLE]
	if commandIdle == nil {
		t.Fatalf("missing command_idle condition set")
	}
	if got, want := commandIdle.GetMaxGuestIdleExtension().AsDuration(), 5*time.Minute; got != want {
		t.Fatalf("command_idle max guest idle extension = %v, want %v", got, want)
	}

	if got, want := policy.GetStages()[0].GetConditionSet(), pb.AutoLifecycleConditionSetId_AUTO_LIFECYCLE_CONDITION_SET_ID_ACCESS_IDLE; got != want {
		t.Fatalf("first stage condition_set = %v, want %v", got, want)
	}
	if got, want := policy.GetStages()[0].GetAt().AsDuration(), 30*time.Second; got != want {
		t.Fatalf("first stage at = %v, want %v", got, want)
	}
	if got, want := policy.GetStages()[1].GetConditionSet(), pb.AutoLifecycleConditionSetId_AUTO_LIFECYCLE_CONDITION_SET_ID_COMMAND_IDLE; got != want {
		t.Fatalf("second stage condition_set = %v, want %v", got, want)
	}
}

func TestSandboxAutoLifecycleRejectsUnknownConditionSet(t *testing.T) {
	spec := &ociSpec{
		Annotations: map[string]string{
			sandboxAutoLifecycleAnnotation: `{
				"condition_sets": {
					"legacy_string_name": {"kind": "inactivity"}
				}
			}`,
		},
	}

	_, err := spec.sandboxAutoLifecycle()
	if err == nil {
		t.Fatalf("sandboxAutoLifecycle() error = nil, want unsupported condition set")
	}
	if !strings.Contains(err.Error(), "unsupported auto lifecycle condition set") {
		t.Fatalf("sandboxAutoLifecycle() error = %q, want unsupported condition set", err)
	}
}

func TestSandboxNetworkPolicyFromAnnotations(t *testing.T) {
	spec := &ociSpec{
		Annotations: map[string]string{
			sandboxNetworkPolicyAnnotation: `{
				"version": "v1",
				"egress": {
					"default_action": "deny",
					"allow": [
						{"domain": "example.com"},
						{"cidr": "10.0.0.0/8"}
					]
				}
			}`,
			sandboxNetworkPolicyGenerationAnnotation: "42",
		},
	}

	config, err := spec.sandboxNetworkPolicy()
	if err != nil {
		t.Fatalf("sandboxNetworkPolicy() error = %v", err)
	}
	if config.Generation != 42 {
		t.Fatalf("generation = %d, want 42", config.Generation)
	}
	if config.Authority != netpb.PolicyAuthority_POLICY_AUTHORITY_CLUSTER {
		t.Fatalf("authority = %v, want cluster", config.Authority)
	}
	if config.Policy.GetEgressDefaultAction() != netpb.EgressDefaultAction_EGRESS_DEFAULT_ACTION_DENY {
		t.Fatalf("default action = %v, want deny", config.Policy.GetEgressDefaultAction())
	}
	if got := config.Policy.GetAllow()[0].GetDomain(); got != "example.com" {
		t.Fatalf("allow[0].domain = %q, want example.com", got)
	}
	if got := config.Policy.GetAllow()[1].GetCidr(); got != "10.0.0.0/8" {
		t.Fatalf("allow[1].cidr = %q, want 10.0.0.0/8", got)
	}
}

func TestSandboxNetworkPolicyTLSInterception(t *testing.T) {
	for _, test := range []struct {
		name     string
		field    string
		wantWire string
	}{
		// Off retains the pre-field protobuf bytes, including when explicit.
		{"missing", "", "08011001"},
		{"off", `,"tls_interception":"off"`, "08011001"},
		{"on", `,"tls_interception":"on"`, "080110012802"},
		{"unknown", `,"tls_interception":"enforce"`, ""},
		{"empty", `,"tls_interception":""`, ""},
		{"null", `,"tls_interception":null`, ""},
		{"boolean", `,"tls_interception":true`, ""},
		{"uppercase", `,"tls_interception":"ON"`, ""},
		{"whitespace", `,"tls_interception":" on "`, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			spec := &ociSpec{Annotations: map[string]string{
				sandboxNetworkPolicyAnnotation:           `{"version":"v1","egress":{"default_action":"allow"}` + test.field + `}`,
				sandboxNetworkPolicyGenerationAnnotation: "1",
			}}
			config, err := spec.sandboxNetworkPolicy()
			if test.wantWire == "" {
				if err == nil || !strings.Contains(err.Error(), "tls_interception") {
					t.Fatalf("error = %v, want invalid tls_interception", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			wire, err := proto.Marshal(config.Policy)
			if err != nil {
				t.Fatal(err)
			}
			if got := fmt.Sprintf("%x", wire); got != test.wantWire {
				t.Fatalf("policy protobuf = %s, want %s", got, test.wantWire)
			}
		})
	}
}

func TestSandboxNetworkPolicyTLSInterceptionExemptions(t *testing.T) {
	for _, test := range []struct {
		name       string
		exemptions string
		valid      bool
	}{
		{"all destinations", `[{"ip":"192.0.2.10"},{"cidr":"198.51.100.0/24"},{"domain":"sensor.example.test"},{"wildcard_domain":"*.example.test"}]`, true},
		{"empty destination", `[{}]`, false},
		{"ambiguous destination", `[{"ip":"192.0.2.10","domain":"sensor.example.test"}]`, false},
		{"wrong shape", `"sensor.example.test"`, false},
		{"not an object", `["sensor.example.test"]`, false},
		{"port without destination", `[{"port":5228}]`, false},
		{"port zero", `[{"domain":"push.example.test","port":0}]`, false},
		{"port too large", `[{"domain":"push.example.test","port":65536}]`, false},
		{"string port", `[{"domain":"push.example.test","port":"5228"}]`, false},
		{"null port", `[{"domain":"push.example.test","port":null}]`, false},
		{"null destination", `[{"domain":null,"ip":"192.0.2.10"}]`, false},
		{"unknown key", `[{"domain":"push.example.test","ports":[5228]}]`, false},
		{"repeated key", `[{"domain":"push.example.test","port":5228,"port":443}]`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			spec := &ociSpec{Annotations: map[string]string{
				sandboxNetworkPolicyAnnotation:           `{"version":"v1","egress":{"default_action":"deny","allow":[{"domain":"sensor.example.test"}],"deny":[{"ip":"192.0.2.20"}]},"tls_interception":"on","tls_interception_exemptions":` + test.exemptions + `}`,
				sandboxNetworkPolicyGenerationAnnotation: "1",
			}}
			config, err := spec.sandboxNetworkPolicy()
			if !test.valid {
				if err == nil || !strings.Contains(err.Error(), "tls_interception_exemptions") {
					t.Fatalf("error = %v, want invalid tls_interception_exemptions", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			wire, err := proto.Marshal(config.Policy)
			if err != nil {
				t.Fatal(err)
			}
			var received netpb.NetworkPolicyV1
			if err := proto.Unmarshal(wire, &received); err != nil {
				t.Fatal(err)
			}
			want := &netpb.NetworkPolicyV1{
				Version:             netpb.NetworkPolicyVersion_NETWORK_POLICY_VERSION_V1,
				EgressDefaultAction: netpb.EgressDefaultAction_EGRESS_DEFAULT_ACTION_DENY,
				TlsInterception:     netpb.TlsInterception_TLS_INTERCEPTION_ON,
				Allow:               []*netpb.EgressDestinationV1{{Destination: &netpb.EgressDestinationV1_Domain{Domain: "sensor.example.test"}}},
				Deny:                []*netpb.EgressDestinationV1{{Destination: &netpb.EgressDestinationV1_Ip{Ip: "192.0.2.20"}}},
				TlsInterceptionExemptions: []*netpb.EgressDestinationV1{
					{Destination: &netpb.EgressDestinationV1_Ip{Ip: "192.0.2.10"}},
					{Destination: &netpb.EgressDestinationV1_Cidr{Cidr: "198.51.100.0/24"}},
					{Destination: &netpb.EgressDestinationV1_Domain{Domain: "sensor.example.test"}},
					{Destination: &netpb.EgressDestinationV1_WildcardDomain{WildcardDomain: "*.example.test"}},
				},
			}
			if !proto.Equal(&received, want) {
				t.Fatalf("received policy = %v, want %v", &received, want)
			}
		})
	}
}

func TestSandboxNetworkPolicyCredentialRules(t *testing.T) {
	const rule = `{"rule_id":"artifact-read","destination":{"exact_host":"artifacts.example.test","path_prefix":"/files"},"credential_header_bindings":[{"header_name":"X-Artifact-Key","credential_reference_id":"00000000-0000-4000-8000-000000000001"}],"supported_http_methods":["GET","POST","HEAD"]}`
	for _, test := range []struct {
		name  string
		rules string
		valid bool
	}{
		{"default port", `[` + rule + `]`, true},
		{"explicit port", `[` + strings.Replace(rule, `"exact_host":`, `"port":8443,"exact_host":`, 1) + `]`, true},
		{"unknown method", `[` + strings.Replace(rule, `"HEAD"`, `"PUT"`, 1) + `]`, false},
		{"numeric method", `[` + strings.Replace(rule, `"HEAD"`, `3`, 1) + `]`, false},
		{"missing methods", `[` + strings.Replace(rule, `["GET","POST","HEAD"]`, `[]`, 1) + `]`, false},
		{"unknown destination field", `[` + strings.Replace(rule, `"exact_host":`, `"wildcard_host":"*.example.test","exact_host":`, 1) + `]`, false},
		{"missing destination", `[` + strings.Replace(rule, `{"exact_host":"artifacts.example.test","path_prefix":"/files"}`, `null`, 1) + `]`, false},
		{"invalid reference", `[` + strings.Replace(rule, `00000000-0000-4000-8000-000000000001`, `invalid`, 1) + `]`, false},
		{"null rule", `[null]`, false},
		{"null binding", `[` + strings.Replace(rule, `{"header_name":"X-Artifact-Key","credential_reference_id":"00000000-0000-4000-8000-000000000001"}`, `null`, 1) + `]`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			spec := &ociSpec{Annotations: map[string]string{
				sandboxNetworkPolicyAnnotation:           `{"version":"v1","egress":{"default_action":"allow"},"tls_interception":"on","network_credential_rules":` + test.rules + `}`,
				sandboxNetworkPolicyGenerationAnnotation: "42",
			}}
			config, err := spec.sandboxNetworkPolicy()
			if !test.valid {
				if err == nil {
					t.Fatal("malformed credential rules were accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			wire, err := proto.Marshal(&pb.CreateRequest{NetworkPolicy: config.Policy, NetworkPolicyGeneration: config.Generation, NetworkPolicyAuthority: config.Authority})
			if err != nil {
				t.Fatal(err)
			}
			var received pb.CreateRequest
			if err := proto.Unmarshal(wire, &received); err != nil {
				t.Fatal(err)
			}
			var port *uint32
			if test.name == "explicit port" {
				port = ptr(uint32(8443))
			}
			wantRule := &netpb.NetworkCredentialRuleV1{
				RuleId:                   "artifact-read",
				Destination:              &netpb.NetworkCredentialDestinationV1{ExactHost: "artifacts.example.test", Port: port, PathPrefix: ptr("/files")},
				CredentialHeaderBindings: []*netpb.CredentialHeaderBindingV1{{HeaderName: "X-Artifact-Key", CredentialReferenceId: "00000000-0000-4000-8000-000000000001"}},
				SupportedHttpMethods:     []netpb.NetworkCredentialHttpMethodV1{netpb.NetworkCredentialHttpMethodV1_NETWORK_CREDENTIAL_HTTP_METHOD_V1_GET, netpb.NetworkCredentialHttpMethodV1_NETWORK_CREDENTIAL_HTTP_METHOD_V1_POST, netpb.NetworkCredentialHttpMethodV1_NETWORK_CREDENTIAL_HTTP_METHOD_V1_HEAD},
			}
			if received.NetworkPolicyGeneration != 42 || received.NetworkPolicyAuthority != netpb.PolicyAuthority_POLICY_AUTHORITY_CLUSTER {
				t.Fatalf("policy identity changed: %v", &received)
			}
			rules := received.GetNetworkPolicy().GetNetworkCredentialRules()
			if len(rules) != 1 || !proto.Equal(rules[0], wantRule) {
				t.Fatalf("credential rules = %v, want %v", rules, wantRule)
			}
		})
	}
}

// The Rust policy test checks the same fixture, so the shim forwards exactly
// the bytes the Rust encoder produces, including port-scoped exemptions.
func TestSandboxNetworkPolicyMatchesRustEncoding(t *testing.T) {
	fixture, err := os.ReadFile("testdata/network-policy-tls-exemptions.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name        string          `json:"name"`
		Policy      json.RawMessage `json:"policy"`
		ProtobufHex string          `json:"protobuf_hex"`
	}
	if err := json.Unmarshal(fixture, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) == 0 {
		t.Fatal("fixture has no cases")
	}
	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			spec := &ociSpec{Annotations: map[string]string{
				sandboxNetworkPolicyAnnotation:           string(test.Policy),
				sandboxNetworkPolicyGenerationAnnotation: "1",
			}}
			config, err := spec.sandboxNetworkPolicy()
			if err != nil {
				t.Fatal(err)
			}
			wire, err := proto.Marshal(config.Policy)
			if err != nil {
				t.Fatal(err)
			}
			if got := hex.EncodeToString(wire); got != test.ProtobufHex {
				t.Fatalf("protobuf = %s, want %s", got, test.ProtobufHex)
			}
		})
	}
}

func TestSandboxNetworkPolicyRequiresGeneration(t *testing.T) {
	spec := &ociSpec{
		Annotations: map[string]string{
			sandboxNetworkPolicyAnnotation: `{
				"version": "v1",
				"egress": {"default_action": "allow"}
			}`,
		},
	}

	_, err := spec.sandboxNetworkPolicy()
	if err == nil {
		t.Fatalf("sandboxNetworkPolicy() error = nil, want missing generation")
	}
	if !strings.Contains(err.Error(), sandboxNetworkPolicyGenerationAnnotation) {
		t.Fatalf("sandboxNetworkPolicy() error = %q, want generation annotation", err)
	}
}

func TestSandboxNetworkPolicyDefaultsMissingVersionToV1(t *testing.T) {
	spec := &ociSpec{
		Annotations: map[string]string{
			sandboxNetworkPolicyAnnotation:           `{"egress":{"default_action":"allow"}}`,
			sandboxNetworkPolicyGenerationAnnotation: "7",
		},
	}
	config, err := spec.sandboxNetworkPolicy()
	if err != nil {
		t.Fatalf("sandboxNetworkPolicy() error = %v", err)
	}
	if config.Policy.GetVersion() != netpb.NetworkPolicyVersion_NETWORK_POLICY_VERSION_V1 {
		t.Fatalf("version = %v, want v1", config.Policy.GetVersion())
	}
}

func TestSandboxNetworkPolicyRejectsV2FromAnnotations(t *testing.T) {
	spec := &ociSpec{
		Annotations: map[string]string{
			sandboxNetworkPolicyAnnotation: `{
				"version": "v2",
				"egress": {
					"deny": [{"wildcard_domain": "*.evil.example"}],
					"allow": [{"cidr": "0.0.0.0/0"}, {"domain": "example.com"}]
				}
			}`,
			sandboxNetworkPolicyGenerationAnnotation: "9",
		},
	}

	_, err := spec.sandboxNetworkPolicy()
	if err == nil {
		t.Fatal("sandboxNetworkPolicy() error = nil, want unsupported V2")
	}
	if !strings.Contains(err.Error(), `unsupported network policy version "v2"`) {
		t.Fatalf("sandboxNetworkPolicy() error = %q, want unsupported V2", err)
	}
}

func TestSandboxNetworkPolicyRejectsInvalidAnnotations(t *testing.T) {
	tests := []struct {
		name        string
		policy      string
		authority   string
		wantMessage string
	}{
		{
			name:        "unknown version",
			policy:      `{"version":"v3","egress":{"default_action":"allow"}}`,
			wantMessage: "unsupported network policy version",
		},
		{
			name:        "empty version",
			policy:      `{"version":"","egress":{"default_action":"allow"}}`,
			wantMessage: "unsupported network policy version",
		},
		{
			name:        "multi destination",
			policy:      `{"version":"v1","egress":{"default_action":"allow","allow":[{"ip":"1.1.1.1","domain":"example.com"}]}}`,
			wantMessage: "exactly one of ip, cidr, domain, wildcard_domain is required",
		},
		{
			name:        "bad default action",
			policy:      `{"version":"v1","egress":{"default_action":"sometimes"}}`,
			wantMessage: "unsupported network policy egress default_action",
		},
		{
			name:        "bad authority",
			policy:      `{"version":"v1","egress":{"default_action":"allow"}}`,
			authority:   "global",
			wantMessage: sandboxNetworkPolicyAuthorityAnnotation,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			annotations := map[string]string{
				sandboxNetworkPolicyAnnotation:           test.policy,
				sandboxNetworkPolicyGenerationAnnotation: "1",
			}
			if test.authority != "" {
				annotations[sandboxNetworkPolicyAuthorityAnnotation] = test.authority
			}
			spec := &ociSpec{Annotations: annotations}
			_, err := spec.sandboxNetworkPolicy()
			if err == nil {
				t.Fatalf("sandboxNetworkPolicy() error = nil, want %q", test.wantMessage)
			}
			if !strings.Contains(err.Error(), test.wantMessage) {
				t.Fatalf("sandboxNetworkPolicy() error = %q, want %q", err, test.wantMessage)
			}
		})
	}
}

func TestTeardownWaitsForSandboxDurability(t *testing.T) {
	identity := taskTeardownIdentity{sandboxID: "sbx", executionGeneration: 7}
	client := &fakeSandboxManagerClient{
		teardownResults: []teardownSuspendResult{
			{outcome: pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_PENDING},
			{outcome: pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_PENDING},
			{outcome: pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_DURABLE},
		},
	}

	startedAt := time.Now()
	if err := releaseTaskAfterGenerationTeardown(context.Background(), client, identity); err != nil {
		t.Fatalf("releaseTaskAfterGenerationTeardown() error = %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed < 2*teardownSuspendPoll {
		t.Fatalf("teardown wait elapsed = %v, want at least two %v poll delays", elapsed, teardownSuspendPoll)
	}
	assertTeardownRequests(t, client.teardownRequests, "sbx", 7)
	if client.stopCalls != 0 || client.deleteCalls != 0 {
		t.Fatalf("destructive RPC calls = stop:%d delete:%d, want zero", client.stopCalls, client.deleteCalls)
	}
}

func TestTeardownAcceptsLegacySupersededOutcome(t *testing.T) {
	identity := taskTeardownIdentity{sandboxID: "sbx", executionGeneration: 7}
	client := &fakeSandboxManagerClient{
		teardownResults: []teardownSuspendResult{
			{outcome: pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_SUPERSEDED},
		},
	}

	if err := releaseTaskAfterGenerationTeardown(context.Background(), client, identity); err != nil {
		t.Fatalf("releaseTaskAfterGenerationTeardown() error = %v", err)
	}
	assertTeardownRequests(t, client.teardownRequests, "sbx", 7)
	if client.stopCalls != 0 || client.deleteCalls != 0 {
		t.Fatalf("destructive RPC calls = stop:%d delete:%d, want zero", client.stopCalls, client.deleteCalls)
	}

	replacementActiveClient := &fakeSandboxManagerClient{}
	identity.sandboxDetached = true
	if err := releaseTaskAfterGenerationTeardown(context.Background(), replacementActiveClient, identity); err != nil {
		t.Fatalf("released releaseTaskAfterGenerationTeardown() error = %v", err)
	}
	if len(replacementActiveClient.teardownRequests) != 0 || replacementActiveClient.stopCalls != 0 || replacementActiveClient.deleteCalls != 0 {
		t.Fatal("a released old task attempted to mutate a replacement execution")
	}
}

func TestLaterExecutionUsesDurableSuspendWithoutOptIn(t *testing.T) {
	podTask := &task{
		id:                  "task",
		sandboxID:           "sbx",
		executionGeneration: 2,
		generationTeardown:  requiresGenerationTeardown(2, ""),
	}
	service := &spaceTaskService{tasks: map[string]*task{"task": podTask}}
	client := &fakeSandboxManagerClient{
		teardownResults: []teardownSuspendResult{
			{outcome: pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_DURABLE},
		},
	}

	detached, err := service.waitForTaskTeardownRelease(context.Background(), client, "delete", "task", podTask)

	if err != nil {
		t.Fatalf("waitForTaskTeardownRelease() error = %v", err)
	}
	if !detached {
		t.Fatal("later execution was not detached after durable suspend")
	}
	assertTeardownRequests(t, client.teardownRequests, "sbx", 2)
	if client.calls != 0 || client.stopCalls != 0 || client.deleteCalls != 0 {
		t.Fatalf("legacy teardown calls = state:%d stop:%d delete:%d, want zero", client.calls, client.stopCalls, client.deleteCalls)
	}
}

func TestRequiresGenerationTeardown(t *testing.T) {
	tests := []struct {
		name                string
		executionGeneration int64
		teardownProtocol    string
		want                bool
	}{
		{name: "first generation without annotation", executionGeneration: 1},
		{
			name:                "first generation with annotation",
			executionGeneration: 1,
			teardownProtocol:    sandboxTeardownProtocolDurableSuspend,
			want:                true,
		},
		{name: "later generation without annotation", executionGeneration: 2, want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := requiresGenerationTeardown(test.executionGeneration, test.teardownProtocol); got != test.want {
				t.Fatalf("requiresGenerationTeardown() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestAutoSuspendPodTeardownWaitsForDurablePublication(t *testing.T) {
	pendingObserved := make(chan struct{})
	durablePollStarted := make(chan struct{})
	publishDurable := make(chan struct{})
	var releaseDurable sync.Once
	t.Cleanup(func() {
		releaseDurable.Do(func() { close(publishDurable) })
	})
	teardownCalls := 0
	scenario := newTeardownScenario(t, func(scenario *teardownScenario) {
		scenario.client.teardownResults = []teardownSuspendResult{
			{outcome: pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_PENDING},
			{outcome: pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_DURABLE},
		}
		scenario.client.onTeardownCall = func() {
			teardownCalls++
			switch teardownCalls {
			case 1:
				close(pendingObserved)
			case 2:
				close(durablePollStarted)
				<-publishDurable
			}
		}
	})

	killDone := make(chan error, 1)
	go func() {
		_, err := scenario.service.Kill(context.Background(), &taskAPI.KillRequest{ID: "task"})
		killDone <- err
	}()

	select {
	case <-pendingObserved:
	case err := <-killDone:
		t.Fatalf("Kill completed before suspend was durable: %v", err)
	case <-time.After(time.Second):
		t.Fatal("Kill did not request durable suspend")
	}
	select {
	case err := <-killDone:
		t.Fatalf("Kill completed while suspend was pending: %v", err)
	default:
	}
	scenario.service.mu.Lock()
	statusWhilePending := scenario.task.status
	detachedWhilePending := scenario.task.sandboxDetached
	scenario.service.mu.Unlock()
	if statusWhilePending != tasktypes.Status_RUNNING || detachedWhilePending {
		t.Fatalf("task released while suspend was pending: status=%s detached=%t", statusWhilePending, detachedWhilePending)
	}

	select {
	case <-durablePollStarted:
	case err := <-killDone:
		t.Fatalf("Kill completed before durable publication: %v", err)
	case <-time.After(time.Second):
		t.Fatal("Kill did not continue polling for durability")
	}
	releaseDurable.Do(func() { close(publishDurable) })
	select {
	case err := <-killDone:
		if err != nil {
			t.Fatalf("Kill() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Kill did not complete after suspend became durable")
	}

	if scenario.task.status != tasktypes.Status_STOPPED || !scenario.task.sandboxDetached {
		t.Fatalf("task was not detached after durable suspend: %+v", scenario.task)
	}
	assertTeardownRequests(t, scenario.client.teardownRequests, "sbx", 2)
	if scenario.client.calls != 0 || scenario.client.stopCalls != 0 || scenario.client.deleteCalls != 0 {
		t.Fatalf("legacy teardown calls = state:%d stop:%d delete:%d, want zero", scenario.client.calls, scenario.client.stopCalls, scenario.client.deleteCalls)
	}
}

func TestConcurrentKillAndDeleteWaitForOneDurableTeardown(t *testing.T) {
	pendingObserved := make(chan struct{})
	durablePollStarted := make(chan struct{})
	publishDurable := make(chan struct{})
	var releaseDurable sync.Once
	t.Cleanup(func() {
		releaseDurable.Do(func() { close(publishDurable) })
	})
	teardownCalls := 0
	scenario := newTeardownScenario(t, func(scenario *teardownScenario) {
		scenario.client.teardownResults = []teardownSuspendResult{
			{outcome: pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_PENDING},
			{outcome: pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_DURABLE},
		}
		scenario.client.onTeardownCall = func() {
			teardownCalls++
			switch teardownCalls {
			case 1:
				close(pendingObserved)
			case 2:
				close(durablePollStarted)
				<-publishDurable
			}
		}
	})

	killDone := make(chan error, 1)
	go func() {
		_, err := scenario.service.Kill(context.Background(), &taskAPI.KillRequest{ID: "task"})
		killDone <- err
	}()
	select {
	case <-pendingObserved:
	case <-time.After(time.Second):
		t.Fatal("Kill did not reach pending teardown")
	}
	deleteDone := make(chan error, 1)
	go func() {
		_, err := scenario.service.Delete(context.Background(), &taskAPI.DeleteRequest{ID: "task"})
		deleteDone <- err
	}()

	select {
	case err := <-deleteDone:
		t.Fatalf("Delete completed while teardown was pending: %v", err)
	default:
	}
	select {
	case <-durablePollStarted:
	case <-time.After(time.Second):
		t.Fatal("Kill did not poll for durable teardown")
	}
	releaseDurable.Do(func() { close(publishDurable) })
	for operation, done := range map[string]<-chan error{"Kill": killDone, "Delete": deleteDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s() error = %v", operation, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s did not complete after durable teardown", operation)
		}
	}

	if _, ok := scenario.service.tasks["task"]; ok {
		t.Fatal("concurrent Kill and Delete left the released task in the task map")
	}
	assertTeardownRequests(t, scenario.client.teardownRequests, "sbx", 2)
	if scenario.client.stopCalls != 0 || scenario.client.deleteCalls != 0 {
		t.Fatalf("destructive RPC calls = stop:%d delete:%d, want zero", scenario.client.stopCalls, scenario.client.deleteCalls)
	}
}

func TestFirecrackerExitDeleteFailsClosedWithoutDurablePublication(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scenario := newTeardownScenario(t, func(scenario *teardownScenario) {
		scenario.task.pid = 999_999_999
		scenario.client.teardownResults = []teardownSuspendResult{
			{outcome: pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_PENDING},
		}
		scenario.client.onTeardownCall = cancel
	})

	_, err := scenario.service.Wait(context.Background(), &taskAPI.WaitRequest{ID: "task"})
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if scenario.task.status != tasktypes.Status_STOPPED || scenario.task.sandboxDetached {
		t.Fatalf("unexpected task state after runtime exit: %+v", scenario.task)
	}
	_, err = scenario.service.Delete(ctx, &taskAPI.DeleteRequest{ID: "task"})

	if status.Code(err) != codes.Canceled {
		t.Fatalf("Delete() code = %v, want Canceled (error: %v)", status.Code(err), err)
	}
	if scenario.service.tasks["task"] != scenario.task || scenario.task.sandboxDetached {
		t.Fatal("Delete released the task without durable publication")
	}
	if len(scenario.client.teardownRequests) != 1 || scenario.client.calls != 0 || scenario.client.stopCalls != 0 || scenario.client.deleteCalls != 0 {
		t.Fatalf("teardown calls = prepare:%d state:%d stop:%d delete:%d", len(scenario.client.teardownRequests), scenario.client.calls, scenario.client.stopCalls, scenario.client.deleteCalls)
	}
}

func TestLaterExecutionReleasesAfterGenerationFencedDelete(t *testing.T) {
	podTask := &task{id: "task", sandboxID: "sbx", executionGeneration: 2, generationTeardown: true}
	service := &spaceTaskService{tasks: map[string]*task{"task": podTask}}
	client := &fakeSandboxManagerClient{
		teardownResults: []teardownSuspendResult{
			{outcome: pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_DELETED},
		},
	}

	detached, err := service.waitForTaskTeardownRelease(context.Background(), client, "delete", "task", podTask)

	if err != nil {
		t.Fatalf("waitForTaskTeardownRelease() error = %v", err)
	}
	if !detached {
		t.Fatal("deleted sandbox task was not detached")
	}
	if len(client.teardownRequests) != 1 || client.calls != 0 {
		t.Fatalf("teardown calls = prepare:%d state:%d, want prepare:1 state:0", len(client.teardownRequests), client.calls)
	}
	if client.stopCalls != 0 || client.deleteCalls != 0 {
		t.Fatalf("destructive RPC calls = stop:%d delete:%d, want zero", client.stopCalls, client.deleteCalls)
	}
}

func TestTeardownDoesNotTreatMissingLocalRowAsDeleteProof(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &fakeSandboxManagerClient{
		teardownResults: []teardownSuspendResult{
			{outcome: pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_PENDING},
		},
		results: []sandboxStateResult{
			{resp: &pb.StateResponse{MissingIds: []string{"sbx"}}},
		},
		onTeardownCall: cancel,
	}

	err := releaseTaskAfterGenerationTeardown(ctx, client, taskTeardownIdentity{sandboxID: "sbx", executionGeneration: 2})

	if status.Code(err) != codes.Canceled {
		t.Fatalf("releaseTaskAfterGenerationTeardown() code = %v, want Canceled (error: %v)", status.Code(err), err)
	}
	if client.calls != 0 {
		t.Fatalf("State() calls = %d, want zero", client.calls)
	}
}

func TestFirstExecutionWithoutOptInKeepsLegacyTeardown(t *testing.T) {
	podTask := &task{
		id:                  "task",
		sandboxID:           "sbx",
		executionGeneration: 1,
		generationTeardown:  requiresGenerationTeardown(1, ""),
	}
	service := &spaceTaskService{tasks: map[string]*task{"task": podTask}}
	client := &fakeSandboxManagerClient{
		results: []sandboxStateResult{
			{resp: &pb.StateResponse{Sandboxes: []*pb.SandboxInfo{{Lifecycle: pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STARTED.Enum()}}}},
		},
	}

	detached, err := service.waitForTaskTeardownRelease(context.Background(), client, "delete", "task", podTask)

	if err != nil {
		t.Fatalf("waitForTaskTeardownRelease() error = %v", err)
	}
	if detached {
		t.Fatal("first execution was unexpectedly detached")
	}
	if client.calls != 1 || client.stopCalls != 0 || client.deleteCalls != 1 {
		t.Fatalf("legacy teardown calls = state:%d stop:%d delete:%d, want state:1 stop:0 delete:1", client.calls, client.stopCalls, client.deleteCalls)
	}
	if len(client.teardownRequests) != 0 {
		t.Fatal("first execution without opt-in invoked durable suspend")
	}
}

func TestDetachedTaskKillAndDeleteDoNotCallSandboxManager(t *testing.T) {
	for _, operation := range []string{"kill", "delete"} {
		t.Run(operation, func(t *testing.T) {
			podTask := &task{
				id:                  "task",
				sandboxID:           "sbx",
				executionGeneration: 7,
				status:              tasktypes.Status_RUNNING,
				sandboxDetached:     true,
			}
			client := &fakeSandboxManagerClient{
				teardownResults: []teardownSuspendResult{
					{outcome: pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_DURABLE},
				},
			}
			managerCalls := 0
			service := &spaceTaskService{
				tasks: map[string]*task{"task": podTask},
				sandboxManagerClientFactory: func() (pb.SandboxManagerClient, io.Closer, error) {
					managerCalls++
					return client, fakeConn{}, nil
				},
			}

			var err error
			if operation == "kill" {
				_, err = service.Kill(context.Background(), &taskAPI.KillRequest{ID: "task"})
			} else {
				_, err = service.Delete(context.Background(), &taskAPI.DeleteRequest{ID: "task"})
			}
			if err != nil {
				t.Fatalf("%s() error = %v", operation, err)
			}
			if managerCalls != 0 || len(client.teardownRequests) != 0 {
				t.Fatalf("%s contacted sbx-manager for an already-detached task", operation)
			}
		})
	}
}

func TestKillOfExitedTaskWaitsForTeardownRelease(t *testing.T) {
	pending := teardownSuspendResult{outcome: pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_PENDING}
	durable := teardownSuspendResult{outcome: pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_DURABLE}

	t.Run("polls until durable", func(t *testing.T) {
		publisher := &fakePublisher{}
		scenario := newTeardownScenario(t, func(scenario *teardownScenario) {
			scenario.task.status = tasktypes.Status_STOPPED
			scenario.task.pid = 42
			scenario.client.teardownResults = []teardownSuspendResult{pending, durable}
		})
		scenario.service.publisher = publisher

		if _, err := scenario.service.Kill(context.Background(), &taskAPI.KillRequest{ID: "task"}); err != nil {
			t.Fatalf("Kill() error = %v", err)
		}
		if !scenario.task.sandboxDetached {
			t.Fatal("Kill() did not record the release proof")
		}
		assertTeardownRequests(t, scenario.client.teardownRequests, "sbx", 2)
		if len(scenario.client.teardownRequests) != 2 {
			t.Fatalf("Kill() polled %d times, want 2", len(scenario.client.teardownRequests))
		}
		// containerd's sandbox exit handling stopped when its Delete timed out;
		// the re-announced exit is what lets it delete the released task.
		if len(publisher.events) != 1 || publisher.topics[0] != "/tasks/exit" {
			t.Fatalf("published %v %v, want one task exit", publisher.topics, publisher.events)
		}
		if exit, ok := publisher.events[0].(*eventstypes.TaskExit); !ok || exit.ID != "task" || exit.ContainerID != "task" || exit.Pid != 42 {
			t.Fatalf("published exit = %+v, want the sandbox task's exit", publisher.events[0])
		}

		// Until Delete, each later Kill re-announces the exit without polling.
		if _, err := scenario.service.Kill(context.Background(), &taskAPI.KillRequest{ID: "task"}); err != nil {
			t.Fatalf("second Kill() error = %v", err)
		}
		if len(scenario.client.teardownRequests) != 2 || len(publisher.events) != 2 {
			t.Fatalf("second Kill: %d requests, %d events; want 2 and 2", len(scenario.client.teardownRequests), len(publisher.events))
		}
		if _, err := scenario.service.Delete(context.Background(), &taskAPI.DeleteRequest{ID: "task"}); err != nil {
			t.Fatalf("Delete() error = %v", err)
		}
		if _, exists := scenario.service.tasks["task"]; exists {
			t.Fatal("Delete() left the released task in the task map")
		}
		// containerd's handling has succeeded once Delete removed the task.
		if _, err := scenario.service.Kill(context.Background(), &taskAPI.KillRequest{ID: "task"}); err != nil {
			t.Fatalf("Kill() after Delete error = %v", err)
		}
		if len(scenario.client.teardownRequests) != 2 || len(publisher.events) != 2 {
			t.Fatalf("deleted task polled or published: %d requests, %d events", len(scenario.client.teardownRequests), len(publisher.events))
		}
	})

	t.Run("failed announcement is retried by the next Kill", func(t *testing.T) {
		publisher := &fakePublisher{failures: 1}
		scenario := newTeardownScenario(t, func(scenario *teardownScenario) {
			scenario.task.status = tasktypes.Status_STOPPED
			scenario.client.teardownResults = []teardownSuspendResult{durable}
		})
		scenario.service.publisher = publisher

		for attempt := 1; attempt <= 2; attempt++ {
			if _, err := scenario.service.Kill(context.Background(), &taskAPI.KillRequest{ID: "task"}); err != nil {
				t.Fatalf("Kill() attempt %d error = %v", attempt, err)
			}
		}
		if publisher.attempts != 2 || len(publisher.events) != 1 {
			t.Fatalf("publish attempts = %d, delivered = %d; want a retry after the failed send", publisher.attempts, len(publisher.events))
		}
		if len(scenario.client.teardownRequests) != 1 {
			t.Fatalf("retry polled sbx-manager again: %d requests", len(scenario.client.teardownRequests))
		}
	})

	t.Run("overlapping Kills each announce the exit", func(t *testing.T) {
		publisher := &fakePublisher{}
		firstPoll := make(chan struct{})
		releaseFirstPoll := make(chan struct{})
		scenario := newTeardownScenario(t, func(scenario *teardownScenario) {
			scenario.task.status = tasktypes.Status_STOPPED
			scenario.client.teardownResults = []teardownSuspendResult{durable}
			scenario.client.onTeardownCall = func() {
				close(firstPoll)
				<-releaseFirstPoll
			}
		})
		scenario.service.publisher = publisher

		errs := make(chan error, 2)
		kill := func() {
			_, err := scenario.service.Kill(context.Background(), &taskAPI.KillRequest{ID: "task"})
			errs <- err
		}
		go kill()
		<-firstPoll
		// The second Kill either waits behind the first or arrives after the
		// release; both paths announce, so the count holds for either order.
		go kill()
		close(releaseFirstPoll)
		for range 2 {
			if err := <-errs; err != nil {
				t.Fatalf("Kill() error = %v", err)
			}
		}
		if len(publisher.events) != 2 || len(scenario.client.teardownRequests) != 1 {
			t.Fatalf("overlapping Kills: %d events, %d polls; want 2 and 1", len(publisher.events), len(scenario.client.teardownRequests))
		}
	})

	t.Run("running task stop publishes nothing", func(t *testing.T) {
		publisher := &fakePublisher{}
		scenario := newTeardownScenario(t, func(scenario *teardownScenario) {
			scenario.client.teardownResults = []teardownSuspendResult{durable}
		})
		scenario.service.publisher = publisher

		if _, err := scenario.service.Kill(context.Background(), &taskAPI.KillRequest{ID: "task"}); err != nil {
			t.Fatalf("Kill() error = %v", err)
		}
		// containerd observes this stop through Wait and handles it itself,
		// so a later Kill of the stopped task has no exit to re-announce.
		if _, err := scenario.service.Kill(context.Background(), &taskAPI.KillRequest{ID: "task"}); err != nil {
			t.Fatalf("second Kill() error = %v", err)
		}
		if len(publisher.events) != 0 {
			t.Fatalf("Kill() of a running task published %v", publisher.events)
		}
	})

	t.Run("pending stays retryable", func(t *testing.T) {
		publisher := &fakePublisher{}
		scenario := newTeardownScenario(t, func(scenario *teardownScenario) {
			scenario.task.status = tasktypes.Status_STOPPED
			scenario.client.teardownResults = []teardownSuspendResult{pending}
		})
		scenario.service.publisher = publisher
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		_, err := scenario.service.Kill(ctx, &taskAPI.KillRequest{ID: "task"})

		if status.Code(err) != codes.DeadlineExceeded || !retryableSandboxStatusError(err) {
			t.Fatalf("Kill() error = %v, want retryable DeadlineExceeded", err)
		}
		if scenario.service.tasks["task"] != scenario.task || scenario.task.sandboxDetached || len(publisher.events) != 0 {
			t.Fatal("Kill() released the task without durable publication")
		}
	})

	for _, tc := range []struct {
		name string
		task func(*task)
	}{
		{"released", func(task *task) { task.sandboxDetached = true }},
		{"legacy", func(task *task) {
			task.executionGeneration = 1
			task.generationTeardown = requiresGenerationTeardown(1, "")
		}},
	} {
		t.Run(tc.name+" returns immediately", func(t *testing.T) {
			managerCalls := 0
			scenario := newTeardownScenario(t, func(scenario *teardownScenario) {
				scenario.task.status = tasktypes.Status_STOPPED
				tc.task(scenario.task)
			})
			scenario.service.sandboxManagerClientFactory = func() (pb.SandboxManagerClient, io.Closer, error) {
				managerCalls++
				return scenario.client, fakeConn{}, nil
			}

			if _, err := scenario.service.Kill(context.Background(), &taskAPI.KillRequest{ID: "task"}); err != nil {
				t.Fatalf("Kill() error = %v", err)
			}
			if managerCalls != 0 {
				t.Fatalf("Kill() contacted sbx-manager %d times for a stopped %s task", managerCalls, tc.name)
			}
		})
	}
}

func TestWaitForTaskTeardownReleaseDoesNotHoldServiceLockWhilePolling(t *testing.T) {
	podTask := &task{id: "task", sandboxID: "sbx", executionGeneration: 7, generationTeardown: true}
	service := &spaceTaskService{tasks: map[string]*task{"task": podTask}}
	pollStarted := make(chan struct{})
	releasePoll := make(chan struct{})
	client := &fakeSandboxManagerClient{
		teardownResults: []teardownSuspendResult{
			{outcome: pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_DURABLE},
		},
		onTeardownCall: func() {
			close(pollStarted)
			<-releasePoll
		},
	}

	waitDone := make(chan error, 1)
	go func() {
		_, err := service.waitForTaskTeardownRelease(context.Background(), client, "delete", "task", podTask)
		waitDone <- err
	}()
	<-pollStarted

	stateDone := make(chan error, 1)
	go func() {
		_, err := service.State(context.Background(), &taskAPI.StateRequest{ID: "task"})
		stateDone <- err
	}()
	select {
	case err := <-stateDone:
		if err != nil {
			t.Fatalf("State() while teardown polls returned error: %v", err)
		}
	case <-time.After(time.Second):
		close(releasePoll)
		<-waitDone
		t.Fatal("task service lock was held while teardown polled sbx-manager")
	}

	close(releasePoll)
	if err := <-waitDone; err != nil {
		t.Fatalf("waitForTaskTeardownRelease() error = %v", err)
	}
}

func TestTaskTeardownAbortsWhenTaskIsReplacedDuringManagerPoll(t *testing.T) {
	for _, operation := range []string{"kill", "delete"} {
		t.Run(operation, func(t *testing.T) {
			original := &task{
				id:                  "task",
				sandboxID:           "sbx",
				executionGeneration: 7,
				generationTeardown:  true,
				status:              tasktypes.Status_RUNNING,
			}
			replacement := &task{
				id:                  "task",
				sandboxID:           "sbx",
				executionGeneration: 8,
				generationTeardown:  true,
				status:              tasktypes.Status_RUNNING,
			}
			pollStarted := make(chan struct{})
			releasePoll := make(chan struct{})
			client := &fakeSandboxManagerClient{
				teardownResults: []teardownSuspendResult{
					{outcome: pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_DURABLE},
				},
				onTeardownCall: func() {
					close(pollStarted)
					<-releasePoll
				},
			}
			service := &spaceTaskService{
				tasks: map[string]*task{"task": original},
				sandboxManagerClientFactory: func() (pb.SandboxManagerClient, io.Closer, error) {
					return client, fakeConn{}, nil
				},
			}

			callDone := make(chan error, 1)
			go func() {
				if operation == "kill" {
					_, err := service.Kill(context.Background(), &taskAPI.KillRequest{ID: "task"})
					callDone <- err
					return
				}
				_, err := service.Delete(context.Background(), &taskAPI.DeleteRequest{ID: "task"})
				callDone <- err
			}()

			<-pollStarted
			service.mu.Lock()
			service.tasks["task"] = replacement
			service.mu.Unlock()
			close(releasePoll)

			if err := <-callDone; status.Code(err) != codes.Aborted {
				t.Fatalf("%s code = %v, want Aborted (error: %v)", operation, status.Code(err), err)
			}
			service.mu.Lock()
			current := service.tasks["task"]
			service.mu.Unlock()
			if current != replacement {
				t.Fatalf("%s changed the replacement task entry", operation)
			}
			if replacement.status != tasktypes.Status_RUNNING || replacement.sandboxDetached {
				t.Fatalf("%s mutated replacement task: %+v", operation, replacement)
			}
			if original.status != tasktypes.Status_RUNNING || original.sandboxDetached {
				t.Fatalf("%s mutated stale task: %+v", operation, original)
			}
		})
	}
}

func TestKillAndDeleteReleaseRetiredExecution(t *testing.T) {
	for _, outcome := range []pb.TeardownSuspendOutcome{
		pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_DURABLE,
		pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_SUPERSEDED,
		pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_DELETED,
	} {
		for _, operation := range []string{"kill", "delete"} {
			t.Run(outcome.String()+"/"+operation, func(t *testing.T) {
				scenario := newTeardownScenario(t, func(scenario *teardownScenario) {
					scenario.client.teardownResults = []teardownSuspendResult{{outcome: outcome}}
				})
				var err error
				if operation == "kill" {
					_, err = scenario.service.Kill(context.Background(), &taskAPI.KillRequest{ID: "task"})
					if scenario.task.status != tasktypes.Status_STOPPED || !scenario.task.sandboxDetached {
						t.Fatalf("Kill left the retired task attached: %+v", scenario.task)
					}
				} else {
					_, err = scenario.service.Delete(context.Background(), &taskAPI.DeleteRequest{ID: "task"})
					if _, exists := scenario.service.tasks["task"]; exists {
						t.Fatal("Delete left the retired task in the task map")
					}
				}
				if err != nil {
					t.Fatalf("%s() error = %v", operation, err)
				}
				assertTeardownRequests(t, scenario.client.teardownRequests, "sbx", 2)
				if scenario.client.stopCalls != 0 || scenario.client.deleteCalls != 0 {
					t.Fatalf("release mutated sandbox: Stop=%d Delete=%d", scenario.client.stopCalls, scenario.client.deleteCalls)
				}
			})
		}
	}
}

func TestDeleteRemovesCurrentTaskAfterDurableTeardownProof(t *testing.T) {
	commands := withFakeCommands(t, func(_ string, args []string) ([]byte, error) {
		command := strings.Join(args, " ")
		switch {
		case strings.Contains(command, "-a list chain ip space_dataplane prerouting"):
			return []byte("comment \"space-sbx-sbx\" # handle 7\n"), nil
		case strings.Contains(command, "-a list chain ip space_dataplane postrouting"):
			return []byte("comment \"space-sbx-sbx\" # handle 8\n"), nil
		default:
			return nil, nil
		}
	})
	stdout, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatalf("create stdout: %v", err)
	}
	stderr, err := os.CreateTemp(t.TempDir(), "stderr")
	if err != nil {
		t.Fatalf("create stderr: %v", err)
	}
	podTask := &task{
		id:                  "task",
		sandboxID:           "sbx",
		executionGeneration: 7,
		generationTeardown:  true,
		status:              tasktypes.Status_RUNNING,
		netnsPath:           "/var/run/netns/cni-test",
		stdout:              stdout,
		stderr:              stderr,
	}
	client := &fakeSandboxManagerClient{
		teardownResults: []teardownSuspendResult{
			{outcome: pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_DURABLE},
		},
	}
	service := &spaceTaskService{
		tasks: map[string]*task{"task": podTask},
		sandboxManagerClientFactory: func() (pb.SandboxManagerClient, io.Closer, error) {
			return client, fakeConn{}, nil
		},
	}

	if _, err := service.Delete(context.Background(), &taskAPI.DeleteRequest{ID: "task"}); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, ok := service.tasks["task"]; ok {
		t.Fatal("Delete() left the released task in the task map")
	}
	if podTask.stdout != nil || podTask.stderr != nil {
		t.Fatalf("Delete() left task FIFOs open: stdout=%v stderr=%v", podTask.stdout, podTask.stderr)
	}
	assertCommand(t, *commands, "ip netns exec cni-test nft delete rule ip space_dataplane prerouting handle 7")
	assertCommand(t, *commands, "ip netns exec cni-test nft delete rule ip space_dataplane postrouting handle 8")
}

func TestTeardownSuspendBudget(t *testing.T) {
	tests := []struct {
		name        string
		remaining   time.Duration
		hasDeadline bool
		want        time.Duration
	}{
		{name: "no caller deadline", want: teardownSuspendNoDeadlineWait},
		{name: "short caller deadline", remaining: 2 * time.Second, hasDeadline: true, want: 2 * time.Second},
		{name: "long caller deadline", remaining: time.Minute, hasDeadline: true, want: teardownSuspendMaxWait},
		{name: "expired caller deadline", remaining: -time.Second, hasDeadline: true, want: time.Nanosecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := teardownSuspendBudget(tt.remaining, tt.hasDeadline); got != tt.want {
				t.Fatalf("teardownSuspendBudget() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTeardownFailsClosed(t *testing.T) {
	tests := []struct {
		name       string
		identity   taskTeardownIdentity
		result     teardownSuspendResult
		wantCode   codes.Code
		wantCalls  int
		cancelCall bool
	}{
		{
			name:      "missing execution generation fails before the RPC",
			identity:  taskTeardownIdentity{sandboxID: "sbx"},
			result:    teardownSuspendResult{outcome: pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_DURABLE},
			wantCode:  codes.FailedPrecondition,
			wantCalls: 0,
		},
		{
			name:      "sandbox manager unavailable",
			identity:  taskTeardownIdentity{sandboxID: "sbx", executionGeneration: 8},
			result:    teardownSuspendResult{err: status.Error(codes.Unavailable, "unavailable")},
			wantCode:  codes.Unavailable,
			wantCalls: 1,
		},
		{
			name:       "still pending at cancellation",
			identity:   taskTeardownIdentity{sandboxID: "sbx", executionGeneration: 8},
			result:     teardownSuspendResult{outcome: pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_PENDING},
			wantCode:   codes.Canceled,
			wantCalls:  1,
			cancelCall: true,
		},
		{
			name:      "unspecified outcome",
			identity:  taskTeardownIdentity{sandboxID: "sbx", executionGeneration: 8},
			result:    teardownSuspendResult{},
			wantCode:  codes.Internal,
			wantCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &fakeSandboxManagerClient{teardownResults: []teardownSuspendResult{tt.result}}
			if tt.cancelCall {
				client.onTeardownCall = cancel
			}

			err := releaseTaskAfterGenerationTeardown(ctx, client, tt.identity)
			if status.Code(err) != tt.wantCode {
				t.Fatalf("releaseTaskAfterGenerationTeardown() code = %v, want %v (error: %v)", status.Code(err), tt.wantCode, err)
			}
			if len(client.teardownRequests) != tt.wantCalls {
				t.Fatalf("PrepareTeardownSuspend() calls = %d, want %d", len(client.teardownRequests), tt.wantCalls)
			}
			if client.stopCalls != 0 || client.deleteCalls != 0 {
				t.Fatalf("destructive RPC calls = stop:%d delete:%d, want zero", client.stopCalls, client.deleteCalls)
			}
		})
	}
}

func TestLegacyPodTeardownPreservesUserStoppedRootfs(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state sandboxStateResult
	}{
		{"stopped", sandboxStateResult{resp: &pb.StateResponse{Sandboxes: []*pb.SandboxInfo{{Lifecycle: pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STOPPED.Enum()}}}}},
		{"missing", sandboxStateResult{err: status.Error(codes.NotFound, "sandbox absent")}},
		{"empty", sandboxStateResult{resp: &pb.StateResponse{}}},
	} {
		for _, operation := range []string{"kill", "delete"} {
			t.Run(tc.name+"/"+operation, func(t *testing.T) {
				client := &fakeSandboxManagerClient{results: []sandboxStateResult{tc.state}}
				identity := taskTeardownIdentity{sandboxID: "sbx", executionGeneration: 7}

				detached, err := releaseLegacyTaskTeardown(context.Background(), client, operation, identity)
				if err != nil {
					t.Fatalf("releaseLegacyTaskTeardown() error = %v", err)
				}
				if !detached {
					t.Fatal("user-stopped sandbox must be detached from its terminating pod")
				}
				if len(client.teardownRequests) != 0 {
					t.Fatal("legacy pod teardown invoked planned suspend")
				}
				if client.stopCalls != 0 || client.deleteCalls != 0 {
					t.Fatalf("user-stopped sandbox mutated: stop:%d delete:%d", client.stopCalls, client.deleteCalls)
				}
			})
		}
	}
}

func TestLegacyKillThenDeleteStillCallsManagerDelete(t *testing.T) {
	podTask := &task{
		id:        "task",
		sandboxID: "sbx",
		status:    tasktypes.Status_RUNNING,
	}
	client := &fakeSandboxManagerClient{
		results: []sandboxStateResult{
			{resp: &pb.StateResponse{Sandboxes: []*pb.SandboxInfo{{Lifecycle: pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STARTED.Enum()}}}},
			{resp: &pb.StateResponse{Sandboxes: []*pb.SandboxInfo{{Lifecycle: pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STOPPED.Enum()}}}},
		},
	}
	managerCalls := 0
	service := &spaceTaskService{
		tasks: map[string]*task{"task": podTask},
		sandboxManagerClientFactory: func() (pb.SandboxManagerClient, io.Closer, error) {
			managerCalls++
			return client, fakeConn{}, nil
		},
	}

	if _, err := service.Kill(context.Background(), &taskAPI.KillRequest{ID: "task"}); err != nil {
		t.Fatalf("Kill() error = %v", err)
	}
	if _, err := service.Delete(context.Background(), &taskAPI.DeleteRequest{ID: "task"}); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	if managerCalls != 2 {
		t.Fatalf("sandbox manager connections = %d, want 2", managerCalls)
	}
	if client.stopCalls != 1 || client.deleteCalls != 1 {
		t.Fatalf("legacy teardown calls = stop:%d delete:%d, want stop:1 delete:1", client.stopCalls, client.deleteCalls)
	}
	if _, ok := service.tasks["task"]; ok {
		t.Fatal("Delete() left the legacy task in the task map")
	}
}

func TestLegacyDeleteAfterLiveStateHandlesManagerError(t *testing.T) {
	for _, code := range []codes.Code{codes.NotFound, codes.Unavailable} {
		t.Run(code.String(), func(t *testing.T) {
			client := &fakeSandboxManagerClient{
				results: []sandboxStateResult{
					{resp: &pb.StateResponse{Sandboxes: []*pb.SandboxInfo{{Lifecycle: pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STARTED.Enum()}}}},
				},
				deleteError: status.Error(code, "delete raced with teardown"),
			}
			service := &spaceTaskService{
				tasks: map[string]*task{"task": {id: "task", sandboxID: "sbx", status: tasktypes.Status_RUNNING}},
				sandboxManagerClientFactory: func() (pb.SandboxManagerClient, io.Closer, error) {
					return client, fakeConn{}, nil
				},
			}
			_, err := service.Delete(context.Background(), &taskAPI.DeleteRequest{ID: "task"})
			_, retained := service.tasks["task"]
			if code == codes.NotFound {
				if err != nil || retained {
					t.Fatalf("already-deleted task was not released: retained=%v err=%v", retained, err)
				}
			} else if err == nil || !retained {
				t.Fatalf("unavailable manager must retain task: retained=%v err=%v", retained, err)
			}
			if client.calls != 1 || client.deleteCalls != 1 {
				t.Fatalf("manager calls = state:%d delete:%d, want one each", client.calls, client.deleteCalls)
			}
		})
	}
}

func TestLegacyKillAndDeleteSerializeManagerTeardown(t *testing.T) {
	podTask := &task{
		id:        "task",
		sandboxID: "sbx",
		status:    tasktypes.Status_RUNNING,
	}
	client := &blockingLegacyTeardownClient{
		firstStateEntered:  make(chan struct{}),
		secondStateEntered: make(chan struct{}),
		releaseFirstState:  make(chan struct{}),
		operations:         make(chan string, 2),
	}
	service := &spaceTaskService{
		tasks: map[string]*task{"task": podTask},
		sandboxManagerClientFactory: func() (pb.SandboxManagerClient, io.Closer, error) {
			return client, fakeConn{}, nil
		},
	}

	killDone := make(chan error, 1)
	go func() {
		_, err := service.Kill(context.Background(), &taskAPI.KillRequest{ID: "task"})
		killDone <- err
	}()
	<-client.firstStateEntered

	deleteDone := make(chan error, 1)
	go func() {
		_, err := service.Delete(context.Background(), &taskAPI.DeleteRequest{ID: "task"})
		deleteDone <- err
	}()

	overlappedManagerDecision := false
	select {
	case <-client.secondStateEntered:
		overlappedManagerDecision = true
	case <-time.After(100 * time.Millisecond):
	}
	close(client.releaseFirstState)

	if err := <-killDone; err != nil {
		t.Fatalf("Kill() error = %v", err)
	}
	if err := <-deleteDone; err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if overlappedManagerDecision {
		t.Fatal("Delete reached the legacy manager decision while Kill teardown was still in flight")
	}

	first := <-client.operations
	second := <-client.operations
	if first != "stop" || second != "delete" {
		t.Fatalf("legacy manager operations = [%s %s], want [stop delete]", first, second)
	}
}

func TestLegacyKillHandoffDetachesBeforeNewerExecutionDelete(t *testing.T) {
	podTask := &task{
		id:        "task",
		sandboxID: "sbx",
		status:    tasktypes.Status_RUNNING,
	}
	client := &fakeSandboxManagerClient{
		results: []sandboxStateResult{
			{resp: &pb.StateResponse{Sandboxes: []*pb.SandboxInfo{{Lifecycle: pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_SUSPENDING.Enum()}}}},
			{resp: &pb.StateResponse{Sandboxes: []*pb.SandboxInfo{{Lifecycle: pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STARTED.Enum()}}}},
		},
	}
	managerCalls := 0
	service := &spaceTaskService{
		tasks: map[string]*task{"task": podTask},
		sandboxManagerClientFactory: func() (pb.SandboxManagerClient, io.Closer, error) {
			managerCalls++
			return client, fakeConn{}, nil
		},
	}

	if _, err := service.Kill(context.Background(), &taskAPI.KillRequest{ID: "task"}); err != nil {
		t.Fatalf("Kill() error = %v", err)
	}
	if !podTask.sandboxDetached {
		t.Fatal("Kill() did not latch the observed suspend handoff")
	}
	if _, err := service.Delete(context.Background(), &taskAPI.DeleteRequest{ID: "task"}); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	if managerCalls != 1 {
		t.Fatalf("sandbox manager connections = %d, want 1", managerCalls)
	}
	if client.stopCalls != 0 || client.deleteCalls != 0 {
		t.Fatalf("legacy handoff teardown calls = stop:%d delete:%d, want zero", client.stopCalls, client.deleteCalls)
	}
	if _, ok := service.tasks["task"]; ok {
		t.Fatal("Delete() left the detached legacy task in the task map")
	}
}

func assertTeardownRequests(t *testing.T, requests []*pb.PrepareTeardownSuspendRequest, sandboxID string, legacyJournalGeneration int64) {
	t.Helper()
	if len(requests) == 0 {
		t.Fatal("PrepareTeardownSuspend() received no requests")
	}
	for _, request := range requests {
		if request.GetSandboxId() != sandboxID || request.GetExecutionGeneration() != legacyJournalGeneration {
			t.Fatalf("PrepareTeardownSuspend() request = %+v, want sandbox %q and legacy journal generation %d", request, sandboxID, legacyJournalGeneration)
		}
	}
	for _, request := range requests {
		if request.GetAction() != pb.TeardownSuspendAction_TEARDOWN_SUSPEND_ACTION_BEGIN_OR_JOIN {
			t.Fatalf("teardown action = %v, want BEGIN_OR_JOIN", request.GetAction())
		}
	}
}

func TestSandboxStateWithRetry(t *testing.T) {
	tests := []struct {
		name      string
		results   []sandboxStateResult
		wantErr   bool
		wantCalls int
	}{
		{
			name: "retries transient state error",
			results: []sandboxStateResult{
				{err: status.Error(codes.Unavailable, "sbx-manager unavailable")},
				{resp: &pb.StateResponse{Sandboxes: []*pb.SandboxInfo{{Lifecycle: pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STARTED.Enum()}}}},
			},
			wantCalls: 2,
		},
		{
			name: "does not retry not found",
			results: []sandboxStateResult{
				{err: status.Error(codes.NotFound, "missing sandbox")},
			},
			wantErr:   true,
			wantCalls: 1,
		},
		{
			name: "does not retry non transient state error",
			results: []sandboxStateResult{
				{err: status.Error(codes.InvalidArgument, "bad sandbox id")},
			},
			wantErr:   true,
			wantCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &fakeSandboxManagerClient{results: tt.results}

			_, err := sandboxStateWithRetry(context.Background(), client, "sbx")

			if (err != nil) != tt.wantErr {
				t.Fatalf("sandboxStateWithRetry() error = %v, wantErr %v", err, tt.wantErr)
			}
			if client.calls != tt.wantCalls {
				t.Fatalf("State() calls = %d, want %d", client.calls, tt.wantCalls)
			}
		})
	}
}

type sandboxStateResult struct {
	resp *pb.StateResponse
	err  error
}

type teardownScenario struct {
	task    *task
	client  *fakeSandboxManagerClient
	service *spaceTaskService
}

func newTeardownScenario(t *testing.T, configure ...func(*teardownScenario)) *teardownScenario {
	t.Helper()
	scenario := &teardownScenario{
		task: &task{
			id:                  "task",
			sandboxID:           "sbx",
			executionGeneration: 2,
			generationTeardown:  requiresGenerationTeardown(2, ""),
			status:              tasktypes.Status_RUNNING,
		},
		client: &fakeSandboxManagerClient{},
	}
	for _, configureScenario := range configure {
		configureScenario(scenario)
	}
	scenario.service = &spaceTaskService{
		tasks: map[string]*task{"task": scenario.task},
		sandboxManagerClientFactory: func() (pb.SandboxManagerClient, io.Closer, error) {
			return scenario.client, fakeConn{}, nil
		},
	}
	return scenario
}

type fakePublisher struct {
	mu       sync.Mutex
	failures int // fail this many sends before delivering
	attempts int
	topics   []string
	events   []events.Event
}

func (f *fakePublisher) Publish(_ context.Context, topic string, event events.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if f.attempts <= f.failures {
		return errors.New("events service unavailable")
	}
	f.topics = append(f.topics, topic)
	f.events = append(f.events, event)
	return nil
}

type fakeSandboxManagerClient struct {
	pb.SandboxManagerClient

	results          []sandboxStateResult
	calls            int
	gotSandboxIDs    []string
	teardownResults  []teardownSuspendResult
	teardownRequests []*pb.PrepareTeardownSuspendRequest
	onTeardownCall   func()
	stopCalls        int
	deleteCalls      int
	deleteError      error
}

func TestStoppedRestartCheckpoint(t *testing.T) {
	for _, tc := range []struct {
		name       string
		lifecycle  pb.SandboxLifecycleState
		stateError error
		storageURI *string
		checkpoint *string
		code       codes.Code
	}{
		{"retained disk", pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STOPPED, nil, nil, nil, codes.OK},
		{"completed retry", pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STARTED, nil, nil, ptr("suspend"), codes.OK},
		{"missing disk refuses historical snapshot", 0, status.Error(codes.NotFound, "absent"), nil, nil, codes.FailedPrecondition},
		{"suspended is not stopped", pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_SUSPENDED, nil, nil, nil, codes.FailedPrecondition},
		{"failed stays failed", pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_FAILED, nil, nil, nil, codes.FailedPrecondition},
		{"authorization failure", 0, status.Error(codes.PermissionDenied, "denied"), nil, nil, codes.PermissionDenied},
		{"interrupted remote stage", pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_RESTORING, nil, ptr("s3://example/disk"), ptr("suspend"), codes.OK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeSandboxManagerClient{results: []sandboxStateResult{{resp: &pb.StateResponse{Sandboxes: []*pb.SandboxInfo{{Lifecycle: tc.lifecycle.Enum()}}}, err: tc.stateError}}}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			checkpoint, err := stoppedRestartCheckpoint(ctx, client, "sandbox", tc.storageURI)
			if status.Code(err) != tc.code {
				t.Fatalf("code = %s, want %s: %v", status.Code(err), tc.code, err)
			}
			if (checkpoint == nil) != (tc.checkpoint == nil) || (checkpoint != nil && tc.checkpoint != nil && *checkpoint != *tc.checkpoint) {
				t.Fatalf("checkpoint = %v, want %v", checkpoint, tc.checkpoint)
			}
		})
	}
}

func TestStoppedRestartWaitsForTransitionAndRequiresAnExplicitRemoteArtifact(t *testing.T) {
	for _, state := range []pb.SandboxLifecycleState{pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STARTING, pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STOPPING, pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_RESTORING} {
		client := &fakeSandboxManagerClient{results: []sandboxStateResult{
			{resp: &pb.StateResponse{Sandboxes: []*pb.SandboxInfo{{Lifecycle: state.Enum()}}}},
			{resp: &pb.StateResponse{Sandboxes: []*pb.SandboxInfo{{Lifecycle: pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STOPPED.Enum()}}}},
		}}
		checkpoint, err := stoppedRestartCheckpoint(context.Background(), client, "sandbox", nil)
		if err != nil || checkpoint != nil {
			t.Fatalf("transition %s: checkpoint=%v err=%v", state, checkpoint, err)
		}
	}
	for _, stateError := range []error{nil, status.Error(codes.NotFound, "absent")} {
		client := &fakeSandboxManagerClient{results: []sandboxStateResult{{err: stateError}}}
		checkpoint, err := stoppedRestartCheckpoint(context.Background(), client, "sandbox", ptr("s3://example/disk"))
		if err != nil || checkpoint == nil || *checkpoint != "suspend" {
			t.Fatalf("selected artifact: checkpoint=%v err=%v", checkpoint, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := &fakeSandboxManagerClient{results: []sandboxStateResult{{resp: &pb.StateResponse{Sandboxes: []*pb.SandboxInfo{{Lifecycle: pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STOPPING.Enum()}}}}}}
	if _, err := stoppedRestartCheckpoint(ctx, client, "sandbox", nil); status.Code(err) != codes.Canceled {
		t.Fatalf("cancelled wait: %v", err)
	}
}

type checkpointStartClient struct {
	pb.SandboxManagerClient
	start func(context.Context, *pb.StartRequest) (*pb.StartResponse, error)
}

func (f checkpointStartClient) Start(ctx context.Context, request *pb.StartRequest, _ ...grpc.CallOption) (*pb.StartResponse, error) {
	return f.start(ctx, request)
}

func TestStoppedCheckpointContinuation(t *testing.T) {
	for _, tc := range []struct {
		name string
		code codes.Code
	}{
		{"interrupted stage", codes.OK},
		{"busy then admitted", codes.OK},
		{"healthy slow transfer", codes.OK},
		{"permanent rejection", codes.FailedPrecondition},
		{"cancelled admission", codes.Canceled},
		{"busy until deadline", codes.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()
				request := &pb.StartRequest{
					SandboxId: "sandbox", Checkpoint: ptr("suspend"),
					SuspendRestoreStorageUri: ptr("s3://example/disk"),
				}
				original := proto.Clone(request)
				calls := 0
				client := checkpointStartClient{start: func(callCtx context.Context, got *pb.StartRequest) (*pb.StartResponse, error) {
					calls++
					if !proto.Equal(got, original) {
						t.Fatal("retry changed the selected restore intent")
					}
					switch tc.name {
					case "busy then admitted":
						if calls == 1 {
							return nil, status.Error(codes.Unavailable, "staging in progress")
						}
					case "healthy slow transfer":
						time.Sleep(35 * time.Second)
						if callCtx.Err() != nil {
							t.Fatal("admission budget cancelled a healthy transfer")
						}
					case "permanent rejection":
						return nil, status.Error(codes.FailedPrecondition, "pending deletion")
					case "cancelled admission":
						cancel()
						return nil, status.Error(codes.Unavailable, "staging in progress")
					case "busy until deadline":
						return nil, status.Error(codes.Unavailable, "staging in progress")
					}
					return &pb.StartResponse{Pid: 123}, nil
				}}
				began := time.Now()
				response, err := startStoppedCheckpoint(ctx, client, request)
				if status.Code(err) != tc.code {
					t.Fatalf("code=%s want=%s err=%v", status.Code(err), tc.code, err)
				}
				if err == nil && response.GetPid() != 123 {
					t.Fatal("lost admitted runtime")
				}
				if tc.name == "busy then admitted" && calls != 2 {
					t.Fatalf("Start calls=%d", calls)
				}
				if tc.name == "permanent rejection" && calls != 1 {
					t.Fatal("retried permanent rejection")
				}
				if tc.name == "busy until deadline" && time.Since(began) != 30*time.Second {
					t.Fatalf("admission wait=%s", time.Since(began))
				}
			})
		})
	}
}

type blockingLegacyTeardownClient struct {
	pb.SandboxManagerClient

	mu                 sync.Mutex
	stateCalls         int
	firstStateEntered  chan struct{}
	secondStateEntered chan struct{}
	releaseFirstState  chan struct{}
	operations         chan string
}

func (f *blockingLegacyTeardownClient) State(context.Context, *pb.StateRequest, ...grpc.CallOption) (*pb.StateResponse, error) {
	f.mu.Lock()
	f.stateCalls++
	call := f.stateCalls
	f.mu.Unlock()

	if call == 1 {
		close(f.firstStateEntered)
		<-f.releaseFirstState
		return &pb.StateResponse{Sandboxes: []*pb.SandboxInfo{{Lifecycle: pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STARTED.Enum()}}}, nil
	}
	if call == 2 {
		close(f.secondStateEntered)
		return &pb.StateResponse{Sandboxes: []*pb.SandboxInfo{{Lifecycle: pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STOPPED.Enum()}}}, nil
	}
	return nil, status.Errorf(codes.Internal, "unexpected State call %d", call)
}

func (f *blockingLegacyTeardownClient) Stop(context.Context, *pb.StopRequest, ...grpc.CallOption) (*pb.StopResponse, error) {
	f.operations <- "stop"
	return &pb.StopResponse{}, nil
}

func (f *blockingLegacyTeardownClient) Delete(context.Context, *pb.DeleteRequest, ...grpc.CallOption) (*pb.DeleteResponse, error) {
	f.operations <- "delete"
	return &pb.DeleteResponse{}, nil
}

func (f *fakeSandboxManagerClient) State(ctx context.Context, in *pb.StateRequest, opts ...grpc.CallOption) (*pb.StateResponse, error) {
	f.gotSandboxIDs = in.GetSandboxIds()
	idx := f.calls
	if idx >= len(f.results) {
		idx = len(f.results) - 1
	}
	f.calls++
	result := f.results[idx]
	return result.resp, result.err
}

type teardownSuspendResult struct {
	outcome pb.TeardownSuspendOutcome
	err     error
}

func (f *fakeSandboxManagerClient) PrepareTeardownSuspend(ctx context.Context, in *pb.PrepareTeardownSuspendRequest, opts ...grpc.CallOption) (*pb.PrepareTeardownSuspendResponse, error) {
	f.teardownRequests = append(f.teardownRequests, in)
	idx := len(f.teardownRequests) - 1
	if idx >= len(f.teardownResults) {
		idx = len(f.teardownResults) - 1
	}
	result := f.teardownResults[idx]
	if f.onTeardownCall != nil {
		f.onTeardownCall()
	}
	return &pb.PrepareTeardownSuspendResponse{Outcome: result.outcome}, result.err
}

func (f *fakeSandboxManagerClient) Stop(ctx context.Context, in *pb.StopRequest, opts ...grpc.CallOption) (*pb.StopResponse, error) {
	f.stopCalls++
	return &pb.StopResponse{}, nil
}

func (f *fakeSandboxManagerClient) Delete(ctx context.Context, in *pb.DeleteRequest, opts ...grpc.CallOption) (*pb.DeleteResponse, error) {
	f.deleteCalls++
	return &pb.DeleteResponse{}, f.deleteError
}

// The Rust renderer verifies this same pod contract. CRI forwards annotations
// to the infra OCI spec; workload env is intentionally NOT copied into Process.
func TestColdCreateReceivesRenderedPodEnv(t *testing.T) {
	wire, err := os.ReadFile("testdata/cold-create-env.json")
	if err != nil {
		t.Fatal(err)
	}
	var pod struct {
		Metadata struct{ Annotations map[string]string }
		Spec     struct {
			Containers []struct {
				Env []struct {
					Name  string
					Value string
				}
			}
		}
	}
	if err := json.Unmarshal(wire, &pod); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{}
	for _, entry := range pod.Spec.Containers[0].Env {
		want[entry.Name] = entry.Value
	}
	spec := &ociSpec{Annotations: pod.Metadata.Annotations, Process: &ociProcess{Env: []string{"PATH=/usr/bin"}}}
	client := &initialCreateRecorder{}
	if _, err := spec.createSandbox(context.Background(), client, &pb.CreateRequest{Template: "seed"}); err != nil {
		t.Fatal(err)
	}
	if client.calls != 1 || !reflect.DeepEqual(client.request.Env, want) {
		t.Fatal("first manager Create did not receive the exact rendered desired environment")
	}
}

type initialCreateRecorder struct {
	pb.SandboxManagerClient
	calls   int
	request *pb.CreateRequest
}

func (c *initialCreateRecorder) Create(_ context.Context, r *pb.CreateRequest, _ ...grpc.CallOption) (*pb.CreateResponse, error) {
	c.calls++
	c.request = r
	return &pb.CreateResponse{SandboxId: "test"}, nil
}

func TestInitialCreateEnvMetadataBoundaries(t *testing.T) {
	encode := func(raw string) string { return base64.StdEncoding.EncodeToString([]byte(raw)) }
	maximum := map[string]string{}
	var maximumWire strings.Builder
	for i := 0; i < 256; i++ {
		key := fmt.Sprintf("K%0127d", i)
		value := strings.Repeat("\x01", 128)
		maximum[key] = value
		maximumWire.WriteString(key + "=" + value + "\x00")
	}

	for _, tc := range []struct {
		name        string
		annotations map[string]string
		want        map[string]string
		invalid     bool
	}{
		{name: "maximum EnvMap", annotations: map[string]string{"space.pplx.net/process-env-v1": encode(maximumWire.String())}, want: maximum},
		{name: "legacy missing", want: map[string]string{"PATH": "/usr/bin"}},
		{name: "explicit empty", annotations: map[string]string{"space.pplx.net/process-env-v1": ""}, want: map[string]string{}},
		{name: "reserved filtered", annotations: map[string]string{"space.pplx.net/process-env-v1": encode("SPACE_INTERNAL_TOKEN=secret\x00SPACE_INTERNAL_CONTROL_TOKEN=secret\x00KEEP=\x00")}, want: map[string]string{"KEEP": ""}},
		{name: "control characters", annotations: map[string]string{"space.pplx.net/process-env-v1": encode("CONTROL=\x01\r\n\t=雪\x00")}, want: map[string]string{"CONTROL": "\x01\r\n\t=雪"}},
		{name: "unsupported version", annotations: map[string]string{"space.pplx.net/process-env-v2": ""}, invalid: true},
		{name: "encoded allocation bound", annotations: map[string]string{"space.pplx.net/process-env-v1": strings.Repeat("A", base64.StdEncoding.EncodedLen(64*1024+2*256)+4)}, invalid: true},
		{name: "maximum value", annotations: map[string]string{"space.pplx.net/process-env-v1": encode("KEY=" + strings.Repeat("x", 8192) + "\x00")}, want: map[string]string{"KEY": strings.Repeat("x", 8192)}},
		{name: "oversized key", annotations: map[string]string{"space.pplx.net/process-env-v1": encode(strings.Repeat("K", 129) + "=x\x00")}, invalid: true},
		{name: "too many entries", annotations: map[string]string{"space.pplx.net/process-env-v1": encode(func() string {
			var wire strings.Builder
			for i := 0; i < 257; i++ {
				fmt.Fprintf(&wire, "K%d=\x00", i)
			}
			return wire.String()
		}())}, invalid: true},
		{name: "too many total bytes", annotations: map[string]string{"space.pplx.net/process-env-v1": encode(func() string {
			var wire strings.Builder
			for i := 0; i < 8; i++ {
				fmt.Fprintf(&wire, "K%d=%s\x00", i, strings.Repeat("x", 8190))
			}
			wire.WriteString("Z=\x00")
			return wire.String()
		}())}, invalid: true},
		{name: "oversized value", annotations: map[string]string{"space.pplx.net/process-env-v1": encode("KEY=" + strings.Repeat("x", 8193) + "\x00")}, invalid: true},
		{name: "base64 newline", annotations: map[string]string{"space.pplx.net/process-env-v1": encode("KEY=x\x00") + "\n"}, invalid: true},
		{name: "base64 pad bits", annotations: map[string]string{"space.pplx.net/process-env-v1": "Sz0AAB=="}, invalid: true},
		{name: "bad base64", annotations: map[string]string{"space.pplx.net/process-env-v1": "!"}, invalid: true},
		{name: "missing terminator", annotations: map[string]string{"space.pplx.net/process-env-v1": encode("KEY=value")}, invalid: true},
		{name: "bad UTF8", annotations: map[string]string{"space.pplx.net/process-env-v1": encode("KEY=\xff\x00")}, invalid: true},
		{name: "key grammar", annotations: map[string]string{"space.pplx.net/process-env-v1": encode("_aZ09=value\x00")}, want: map[string]string{"_aZ09": "value"}},
		{name: "bad continuation", annotations: map[string]string{"space.pplx.net/process-env-v1": encode("A-B=value\x00")}, invalid: true},
		{name: "Unicode first", annotations: map[string]string{"space.pplx.net/process-env-v1": encode("éA=value\x00")}, invalid: true},
		{name: "Unicode continuation", annotations: map[string]string{"space.pplx.net/process-env-v1": encode("Aé=value\x00")}, invalid: true},
		{name: "bad key", annotations: map[string]string{"space.pplx.net/process-env-v1": encode("1KEY=value\x00")}, invalid: true},
		{name: "duplicate key", annotations: map[string]string{"space.pplx.net/process-env-v1": encode("KEY=a\x00KEY=b\x00")}, invalid: true},
		{name: "embedded NUL", annotations: map[string]string{"space.pplx.net/process-env-v1": encode("KEY=a\x00b\x00")}, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := &ociSpec{Annotations: tc.annotations, Process: &ociProcess{Env: []string{"PATH=/usr/bin"}}}
			client := &initialCreateRecorder{}
			_, err := spec.createSandbox(context.Background(), client, &pb.CreateRequest{})
			if tc.invalid {
				if status.Code(err) != codes.InvalidArgument || status.Convert(err).Message() != "invalid cold-create environment metadata" || client.calls != 0 {
					t.Fatal("malformed explicit metadata reached initial Create")
				}
			} else if err != nil || client.calls != 1 || !reflect.DeepEqual(client.request.Env, tc.want) {
				t.Fatal("initial Create environment contract mismatch")
			}
		})
	}
}
