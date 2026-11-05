package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	taskAPI "github.com/containerd/containerd/api/runtime/task/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/perplexityai/SPACE/shim/proto/sandbox/manager/v1"
)

const (
	upgradeTestSandboxID   = "019f8000-0000-7000-8000-000000000001"
	upgradeTestOperationID = "019f8000-0000-7000-8000-000000000011"
	upgradeTestArtifactID  = "019f8000-0000-7000-8000-000000000012"
	upgradeTestPodUID      = "4f6b0d4e-6a59-4b5c-9e43-3a1d0d6f2a10"
)

// runtimeUpgradeAnnotationsFor is a runtime upgrade pod's annotations, as
// the control plane renders them, for a stopped or a suspended source.
func runtimeUpgradeAnnotationsFor(targetState string, suspended bool) map[string]string {
	annotations := map[string]string{
		"io.kubernetes.cri.container-type":                  "sandbox",
		kubernetesPodUIDAnnotation:                          upgradeTestPodUID,
		sandboxIDAnnotation:                                 upgradeTestSandboxID,
		sandboxExecutionGenerationAnnotation:                "4",
		sandboxCheckpointAnnotation:                         runtimeUpgradeCheckpoint,
		sandboxRuntimeUpgradeOperationIDAnnotation:          upgradeTestOperationID,
		sandboxRuntimeUpgradeTargetEnvironmentAnnotation:    "sha256:0707",
		sandboxRuntimeUpgradeAuthorizationIDAnnotation:      "environment-block-e0",
		sandboxRuntimeUpgradeAuthorizationReasonAnnotation:  "e0 runs a blocked runtime",
		sandboxRuntimeUpgradeMemoryLossAnnotation:           "false",
		sandboxRuntimeUpgradeTargetStateAnnotation:          targetState,
		sandboxSuspendRestoreStorageURIAnnotation:           "s3://space-test/suspend-snapshots/current?region=us-east-1",
		sandboxSuspendRestoreSourceNodeAnnotation:           "worker-source",
		sandboxRuntimeUpgradeCheckpointArtifactIDAnnotation: "",
	}
	if suspended {
		annotations[sandboxRuntimeUpgradeMemoryLossAnnotation] = "true"
		annotations[sandboxRuntimeUpgradeCheckpointArtifactIDAnnotation] = upgradeTestArtifactID
	}
	return annotations
}

// upgradeRecordingClient answers UpgradeRuntime with upgrade and records
// every call a fallback would make: Start, and the State a stopped restart
// reads first.
type upgradeRecordingClient struct {
	pb.SandboxManagerClient
	upgrade  func(*pb.UpgradeRuntimeRequest) (*pb.UpgradeRuntimeResponse, error)
	upgrades []*pb.UpgradeRuntimeRequest
	starts   int
	states   int
}

func (c *upgradeRecordingClient) UpgradeRuntime(_ context.Context, request *pb.UpgradeRuntimeRequest, _ ...grpc.CallOption) (*pb.UpgradeRuntimeResponse, error) {
	c.upgrades = append(c.upgrades, request)
	return c.upgrade(request)
}

func (c *upgradeRecordingClient) Start(context.Context, *pb.StartRequest, ...grpc.CallOption) (*pb.StartResponse, error) {
	c.starts++
	return &pb.StartResponse{Pid: 4242}, nil
}

func (c *upgradeRecordingClient) State(context.Context, *pb.StateRequest, ...grpc.CallOption) (*pb.StateResponse, error) {
	c.states++
	return &pb.StateResponse{Sandboxes: []*pb.SandboxInfo{{Lifecycle: pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STOPPED.Enum()}}}, nil
}

// answering is a client whose UpgradeRuntime answers response and err.
func answering(response *pb.UpgradeRuntimeResponse, err error) *upgradeRecordingClient {
	return &upgradeRecordingClient{upgrade: func(*pb.UpgradeRuntimeRequest) (*pb.UpgradeRuntimeResponse, error) {
		return response, err
	}}
}

// createRuntimeUpgradePod runs the shim's Create for a pod with annotations
// against client.
func createRuntimeUpgradePod(t *testing.T, client *upgradeRecordingClient, annotations map[string]string) (*spaceTaskService, *taskAPI.CreateTaskResponse, error) {
	t.Helper()
	bundle := t.TempDir()
	config, err := json.Marshal(map[string]any{"annotations": annotations})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "config.json"), config, 0o600); err != nil {
		t.Fatal(err)
	}
	service := &spaceTaskService{
		tasks: map[string]*task{},
		sandboxManagerClientFactory: func() (pb.SandboxManagerClient, io.Closer, error) {
			return client, fakeConn{}, nil
		},
	}
	response, err := service.Create(context.Background(), &taskAPI.CreateTaskRequest{ID: "pod", Bundle: bundle})
	return service, response, err
}

// Whatever UpgradeRuntime answers short of a usable outcome fails the create,
// and nothing falls back to the Start the same pod would have sent without
// its runtime-upgrade checkpoint: that Start boots the environment the
// upgrade moves the sandbox off. A refusal, an unknown outcome, and a
// started target with no running execution (what a sandbox-manager without
// activation answers for a sandbox already on its target) are all the
// create's error.
func TestARuntimeUpgradePodNeverFallsBackToStart(t *testing.T) {
	for _, tc := range []struct {
		name        string
		targetState string
		response    *pb.UpgradeRuntimeResponse
		err         error
		code        codes.Code
	}{
		{name: "rolled back", targetState: "started", err: status.Error(codes.FailedPrecondition, "runtime upgrade rolled back to the old disk/checkpoint pair"), code: codes.FailedPrecondition},
		{name: "retryable refusal", targetState: "started", err: status.Error(codes.Unavailable, "another runtime upgrade step is running; retry"), code: codes.Unavailable},
		{name: "unknown checkpoint from an old sandbox-manager", targetState: "started", err: status.Error(codes.FailedPrecondition, "cannot start sandbox from checkpoint 'runtime-upgrade'"), code: codes.FailedPrecondition},
		{name: "unspecified outcome", targetState: "started", response: &pb.UpgradeRuntimeResponse{}, code: codes.Internal},
		{name: "unspecified outcome for a stopped target", targetState: "stopped", response: &pb.UpgradeRuntimeResponse{}, code: codes.Internal},
		{name: "already on target without an execution", targetState: "started", response: &pb.UpgradeRuntimeResponse{Outcome: pb.RuntimeUpgradeOutcome_RUNTIME_UPGRADE_OUTCOME_ALREADY_ON_TARGET}, code: codes.FailedPrecondition},
		{name: "upgraded without a pid", targetState: "started", response: &pb.UpgradeRuntimeResponse{Outcome: pb.RuntimeUpgradeOutcome_RUNTIME_UPGRADE_OUTCOME_UPGRADED, Start: &pb.StartResponse{}}, code: codes.FailedPrecondition},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := answering(tc.response, tc.err)
			service, _, err := createRuntimeUpgradePod(t, client, runtimeUpgradeAnnotationsFor(tc.targetState, false))
			if status.Code(err) != tc.code {
				t.Fatalf("code = %s, want %s: %v", status.Code(err), tc.code, err)
			}
			if len(client.upgrades) != 1 || client.starts != 0 || client.states != 0 {
				t.Fatalf("upgrades:%d starts:%d states:%d, want one upgrade and no fallback", len(client.upgrades), client.starts, client.states)
			}
			if _, ok := service.tasks["pod"]; ok {
				t.Fatal("a failed create must not record the pod's task")
			}
		})
	}
}

// A started target attaches the execution UpgradeRuntime returns, as a
// Start's would be. The request is the pod's own Start turned into the
// plain blocking cold start the upgrade boots: no checkpoint, restore or
// stopped-target fields, and for a stopped source nothing to stage.
func TestARuntimeUpgradePodRunsTheUpgradedExecution(t *testing.T) {
	for _, outcome := range []pb.RuntimeUpgradeOutcome{
		pb.RuntimeUpgradeOutcome_RUNTIME_UPGRADE_OUTCOME_UPGRADED,
		pb.RuntimeUpgradeOutcome_RUNTIME_UPGRADE_OUTCOME_ALREADY_ON_TARGET,
	} {
		t.Run(outcome.String(), func(t *testing.T) {
			client := answering(&pb.UpgradeRuntimeResponse{Outcome: outcome, Start: &pb.StartResponse{Pid: 77}}, nil)
			service, response, err := createRuntimeUpgradePod(t, client, runtimeUpgradeAnnotationsFor("started", false))
			if err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			if response.GetPid() != 77 {
				t.Fatalf("pid = %d, want the upgraded execution's", response.GetPid())
			}
			pod := service.tasks["pod"]
			if pod == nil || pod.sandboxID != upgradeTestSandboxID || pod.sandboxDetached || pod.materializedStopped {
				t.Fatalf("task = %+v, want an attached running sandbox", pod)
			}
			if client.starts != 0 || len(client.upgrades) != 1 {
				t.Fatalf("starts:%d upgrades:%d", client.starts, len(client.upgrades))
			}
			request := client.upgrades[0]
			start := request.GetStart()
			if request.GetSandboxId() != upgradeTestSandboxID || request.GetOperationId() != upgradeTestOperationID ||
				request.GetTargetEnvironment() != "sha256:0707" ||
				request.GetTargetState() != pb.WakeTargetState_WAKE_TARGET_STATE_STARTED ||
				request.GetAuthorization().GetAuthorizationId() != "environment-block-e0" ||
				request.GetAuthorization().GetReason() != "e0 runs a blocked runtime" ||
				request.GetAuthorization().GetMemoryLoss() {
				t.Fatalf("upgrade request = %+v", request)
			}
			if !start.GetBlocking() || start.Checkpoint != nil || start.SuspendRestoreStorageUri != nil ||
				start.SuspendRestoreSourceNode != nil || start.ExpectedLocalArtifactId != nil ||
				start.GetTargetState() != pb.WakeTargetState_WAKE_TARGET_STATE_UNSPECIFIED {
				t.Fatalf("the upgrade's start must be a plain blocking cold start: %+v", start)
			}
			if start.GetRouteIdentity().GetPodUid() != upgradeTestPodUID || start.GetRouteIdentity().GetExecutionGeneration() != 4 {
				t.Fatalf("the upgrade's start must carry the pod's route identity: %+v", start.GetRouteIdentity())
			}
			if request.StageFrom != nil || request.CheckpointLocalArtifactId != nil {
				t.Fatalf("a stopped source has nothing to stage or check: %+v", request)
			}
		})
	}
}

// A stopped target leaves the pod a placeholder task with no VM, as a
// stopped materialization does, whether the upgrade booted and stopped the
// sandbox again (and so returned the boot's execution) or found it already
// on its target. Kill and Delete of that task never ask sbx-manager to
// suspend or delete the stopped sandbox.
func TestARuntimeUpgradePodToStoppedRunsAPlaceholder(t *testing.T) {
	for _, response := range []*pb.UpgradeRuntimeResponse{
		{Outcome: pb.RuntimeUpgradeOutcome_RUNTIME_UPGRADE_OUTCOME_UPGRADED, Start: &pb.StartResponse{Pid: 99}},
		{Outcome: pb.RuntimeUpgradeOutcome_RUNTIME_UPGRADE_OUTCOME_UPGRADED},
		{Outcome: pb.RuntimeUpgradeOutcome_RUNTIME_UPGRADE_OUTCOME_ALREADY_ON_TARGET},
	} {
		client := answering(response, nil)
		service, created, err := createRuntimeUpgradePod(t, client, runtimeUpgradeAnnotationsFor("stopped", false))
		if err != nil {
			t.Fatalf("%v: Create() error = %v", response, err)
		}
		pod := service.tasks["pod"]
		if created.GetPid() != uint32(os.Getpid()) || pod == nil || !pod.materializedStopped || !pod.sandboxDetached {
			t.Fatalf("%v: pid=%d task=%+v, want a detached placeholder on the shim's pid", response, created.GetPid(), pod)
		}
		if got := client.upgrades[0].GetTargetState(); got != pb.WakeTargetState_WAKE_TARGET_STATE_STOPPED {
			t.Fatalf("target state = %s", got)
		}
		if client.starts != 0 || client.states != 0 {
			t.Fatalf("%v: starts:%d states:%d", response, client.starts, client.states)
		}
	}
}

// A suspended source names the checkpoint it was decided from, and the
// restore's storage URI and source node become where sbx-manager stages a
// sandbox absent from the node, inside the upgrade.
func TestARuntimeUpgradeOfASuspendedSourceStagesInsideTheUpgrade(t *testing.T) {
	client := answering(&pb.UpgradeRuntimeResponse{Outcome: pb.RuntimeUpgradeOutcome_RUNTIME_UPGRADE_OUTCOME_UPGRADED, Start: &pb.StartResponse{Pid: 5}}, nil)
	if _, _, err := createRuntimeUpgradePod(t, client, runtimeUpgradeAnnotationsFor("started", true)); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	request := client.upgrades[0]
	if request.GetCheckpointLocalArtifactId() != upgradeTestArtifactID || !request.GetAuthorization().GetMemoryLoss() {
		t.Fatalf("upgrade request = %+v", request)
	}
	stage := request.GetStageFrom()
	if stage.GetSuspendRestoreStorageUri() != "s3://space-test/suspend-snapshots/current?region=us-east-1" ||
		stage.GetSuspendRestoreSourceNode() != "worker-source" {
		t.Fatalf("stage_from = %+v", stage)
	}
	if request.GetStart().SuspendRestoreStorageUri != nil || request.GetStart().Checkpoint != nil {
		t.Fatalf("the staging fields must leave the cold start: %+v", request.GetStart())
	}
}

// The upgrade annotations are the upgrade's authorization, so a pod that
// carries any of them without the runtime-upgrade checkpoint, or the
// checkpoint without a well-formed set, is refused before anything reaches
// sbx-manager.
func TestRuntimeUpgradeAnnotationsAreParsedStrictly(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(map[string]string)
	}{
		{"upgrade annotation on a stopped pod", func(a map[string]string) { a[sandboxCheckpointAnnotation] = "stopped" }},
		{"upgrade annotation without a checkpoint", func(a map[string]string) { delete(a, sandboxCheckpointAnnotation) }},
		{"operation id", func(a map[string]string) { a[sandboxRuntimeUpgradeOperationIDAnnotation] = "op" }},
		{"no target", func(a map[string]string) { delete(a, sandboxRuntimeUpgradeTargetEnvironmentAnnotation) }},
		{"no authorization id", func(a map[string]string) { a[sandboxRuntimeUpgradeAuthorizationIDAnnotation] = " " }},
		{"no reason", func(a map[string]string) { delete(a, sandboxRuntimeUpgradeAuthorizationReasonAnnotation) }},
		{"memory loss", func(a map[string]string) { a[sandboxRuntimeUpgradeMemoryLossAnnotation] = "yes" }},
		{"no memory loss", func(a map[string]string) { delete(a, sandboxRuntimeUpgradeMemoryLossAnnotation) }},
		{"paused target", func(a map[string]string) { a[sandboxRuntimeUpgradeTargetStateAnnotation] = "paused" }},
		{"checkpoint artifact", func(a map[string]string) { a[sandboxRuntimeUpgradeCheckpointArtifactIDAnnotation] = "artifact" }},
		{"stopped materialization too", func(a map[string]string) {
			a[sandboxStoppedMaterializationArtifactID] = "7f9c2a52-3f0e-4d55-9c1a-6b3f0f6e2a11"
		}},
		{"fork disk restore too", func(a map[string]string) { a[sandboxArtifactRestoreAsNewAnnotation] = "{}" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			annotations := runtimeUpgradeAnnotationsFor("started", true)
			tc.mutate(annotations)
			if _, err := (&ociSpec{Annotations: annotations}).sandboxRuntimeUpgrade(); err == nil {
				t.Fatal("sandboxRuntimeUpgrade() accepted a malformed pod")
			}
			client := &upgradeRecordingClient{}
			if _, _, err := createRuntimeUpgradePod(t, client, annotations); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("Create() error = %v, want InvalidArgument", err)
			}
			if len(client.upgrades) != 0 || client.starts != 0 || client.states != 0 {
				t.Fatalf("a malformed pod reached sbx-manager: upgrades:%d starts:%d states:%d", len(client.upgrades), client.starts, client.states)
			}
		})
	}

	intent, err := (&ociSpec{Annotations: runtimeUpgradeAnnotationsFor("stopped", false)}).sandboxRuntimeUpgrade()
	if err != nil || intent == nil || intent.targetState != pb.WakeTargetState_WAKE_TARGET_STATE_STOPPED || intent.checkpointLocalArtifactID != nil {
		t.Fatalf("stopped source: intent=%+v err=%v", intent, err)
	}
	ordinary := map[string]string{sandboxCheckpointAnnotation: "stopped"}
	if intent, err := (&ociSpec{Annotations: ordinary}).sandboxRuntimeUpgrade(); intent != nil || err != nil {
		t.Fatalf("an ordinary pod is no upgrade: intent=%+v err=%v", intent, err)
	}
}

// renderedPodAnnotations reads the annotations of a pod shape the Rust pod
// renderer's golden test pins, so the shim is tested against the pod the
// control plane actually writes rather than a copy of its keys.
func renderedPodAnnotations(t *testing.T, shape string) map[string]string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "crates", "libs", "common", "core", "src", "fixtures", shape+".snap"))
	if err != nil {
		t.Fatal(err)
	}
	annotations := map[string]string{}
	inAnnotations := false
	for _, line := range strings.Split(string(data), "\n") {
		switch {
		case line == "  annotations:":
			inAnnotations = true
		case inAnnotations && strings.HasPrefix(line, "    "):
			key, value, ok := strings.Cut(strings.TrimSpace(line), ": ")
			if !ok {
				t.Fatalf("%s: unparsable annotation line %q", shape, line)
			}
			if strings.HasPrefix(value, `"`) {
				if value, err = strconv.Unquote(value); err != nil {
					t.Fatalf("%s: %q: %v", shape, line, err)
				}
			}
			annotations[key] = value
		case inAnnotations:
			return annotations
		}
	}
	t.Fatalf("%s: no annotations", shape)
	return nil
}

// Each runtime upgrade pod shape the control plane renders parses into the
// upgrade it describes, and only the suspended source stages.
func TestTheShimParsesEveryRenderedRuntimeUpgradePod(t *testing.T) {
	for _, tc := range []struct {
		shape       string
		targetState pb.WakeTargetState
		stages      bool
	}{
		{"sandbox_pod_restore_runtime_upgrade_stopped", pb.WakeTargetState_WAKE_TARGET_STATE_STOPPED, false},
		{"sandbox_pod_restore_runtime_upgrade_started", pb.WakeTargetState_WAKE_TARGET_STATE_STARTED, false},
		{"sandbox_pod_restore_runtime_upgrade_suspended", pb.WakeTargetState_WAKE_TARGET_STATE_STARTED, true},
	} {
		t.Run(tc.shape, func(t *testing.T) {
			spec := &ociSpec{Annotations: renderedPodAnnotations(t, tc.shape)}
			checkpoint, _, useStart, err := spec.sandboxStartIntent()
			if err != nil || !useStart || checkpoint == nil || *checkpoint != runtimeUpgradeCheckpoint {
				t.Fatalf("start intent: checkpoint=%v useStart=%v err=%v", checkpoint, useStart, err)
			}
			intent, err := spec.sandboxRuntimeUpgrade()
			if err != nil || intent == nil {
				t.Fatalf("sandboxRuntimeUpgrade() = %+v, %v", intent, err)
			}
			request := runtimeUpgradeRequest(&pb.StartRequest{
				SandboxId:                spec.Annotations[sandboxIDAnnotation],
				Checkpoint:               checkpoint,
				SuspendRestoreStorageUri: spec.sandboxSuspendRestoreStorageURI(),
				SuspendRestoreSourceNode: spec.sandboxSuspendRestoreSourceNode(),
			}, intent)
			if request.GetTargetState() != tc.targetState || request.GetOperationId() == "" ||
				request.GetTargetEnvironment() == "" || request.GetAuthorization().GetAuthorizationId() == "" {
				t.Fatalf("upgrade request = %+v", request)
			}
			if (request.StageFrom != nil) != tc.stages || (request.CheckpointLocalArtifactId != nil) != tc.stages {
				t.Fatalf("stage_from=%v checkpoint=%v, want staging %v", request.StageFrom, request.CheckpointLocalArtifactId, tc.stages)
			}
			if request.GetAuthorization().GetMemoryLoss() != tc.stages {
				t.Fatalf("memory loss = %v", request.GetAuthorization().GetMemoryLoss())
			}
		})
	}
}
