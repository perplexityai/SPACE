package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/perplexityai/SPACE/shim/proto/sandbox/manager/v1"
)

// The annotations of a runtime upgrade pod beyond its checkpoint; see
// runtimeUpgradeCheckpoint. They mirror the constants of the same names in
// crates/libs/common/core/src/k8s/runtime_upgrade.rs, which renders them.
const (
	sandboxRuntimeUpgradeOperationIDAnnotation          = "space.pplx.net/runtime-upgrade-operation-id"
	sandboxRuntimeUpgradeTargetEnvironmentAnnotation    = "space.pplx.net/runtime-upgrade-target-environment"
	sandboxRuntimeUpgradeAuthorizationIDAnnotation      = "space.pplx.net/runtime-upgrade-authorization-id"
	sandboxRuntimeUpgradeAuthorizationReasonAnnotation  = "space.pplx.net/runtime-upgrade-authorization-reason"
	sandboxRuntimeUpgradeMemoryLossAnnotation           = "space.pplx.net/runtime-upgrade-memory-loss"
	sandboxRuntimeUpgradeTargetStateAnnotation          = "space.pplx.net/runtime-upgrade-target-state"
	sandboxRuntimeUpgradeCheckpointArtifactIDAnnotation = "space.pplx.net/runtime-upgrade-checkpoint-local-artifact-id"
)

// runtimeUpgradeCheckpoint is the checkpoint annotation of a pod that boots
// its sandbox under another execution environment. The shim calls
// sbx-manager's UpgradeRuntime with the cold start it would otherwise send as
// a Start. A shim that predates it forwards the value to sbx-manager as a
// Start checkpoint, which sbx-manager refuses whatever the node holds for
// the sandbox, so an old shim fails closed.
const runtimeUpgradeCheckpoint = "runtime-upgrade"

// runtimeUpgradeAnnotations are every annotation a runtime upgrade pod
// carries beyond its checkpoint. Any of them on another pod is refused.
var runtimeUpgradeAnnotations = []string{
	sandboxRuntimeUpgradeOperationIDAnnotation,
	sandboxRuntimeUpgradeTargetEnvironmentAnnotation,
	sandboxRuntimeUpgradeAuthorizationIDAnnotation,
	sandboxRuntimeUpgradeAuthorizationReasonAnnotation,
	sandboxRuntimeUpgradeMemoryLossAnnotation,
	sandboxRuntimeUpgradeTargetStateAnnotation,
	sandboxRuntimeUpgradeCheckpointArtifactIDAnnotation,
}

// runtimeUpgradeIntent is a runtime upgrade pod's annotations, parsed: the
// fields of UpgradeRuntimeRequest the rest of the pod doesn't give.
type runtimeUpgradeIntent struct {
	operationID               string
	targetEnvironment         string
	authorization             *pb.RuntimeUpgradeAuthorization
	targetState               pb.WakeTargetState
	checkpointLocalArtifactID *string
}

// sandboxRuntimeUpgrade parses a runtime upgrade pod's annotations, or
// returns nil for any other pod. Only the control plane writes sandbox pods,
// and these annotations are the upgrade's authorization, so a pod that
// carries any of them without the runtime-upgrade checkpoint, or the
// checkpoint without all of the required ones, is malformed and refused.
func (s *ociSpec) sandboxRuntimeUpgrade() (*runtimeUpgradeIntent, error) {
	annotation := func(key string) string { return strings.TrimSpace(s.Annotations[key]) }
	checkpoint := s.sandboxCheckpoint()
	if checkpoint == nil || *checkpoint != runtimeUpgradeCheckpoint {
		for _, key := range runtimeUpgradeAnnotations {
			if annotation(key) != "" {
				return nil, fmt.Errorf("%s requires %s %s", key, sandboxCheckpointAnnotation, runtimeUpgradeCheckpoint)
			}
		}
		return nil, nil
	}
	// An upgrade pod names no other restore intent; one that does is
	// malformed, rather than one of the two silently winning.
	for _, key := range []string{sandboxStoppedMaterializationArtifactID, sandboxArtifactRestoreAsNewAnnotation} {
		if annotation(key) != "" {
			return nil, fmt.Errorf("%s %s excludes %s", sandboxCheckpointAnnotation, runtimeUpgradeCheckpoint, key)
		}
	}
	operationID, err := uuid.Parse(annotation(sandboxRuntimeUpgradeOperationIDAnnotation))
	if err != nil {
		return nil, fmt.Errorf("%s must be a UUID: %w", sandboxRuntimeUpgradeOperationIDAnnotation, err)
	}
	intent := &runtimeUpgradeIntent{
		operationID:       operationID.String(),
		targetEnvironment: annotation(sandboxRuntimeUpgradeTargetEnvironmentAnnotation),
		authorization: &pb.RuntimeUpgradeAuthorization{
			AuthorizationId: annotation(sandboxRuntimeUpgradeAuthorizationIDAnnotation),
			Reason:          annotation(sandboxRuntimeUpgradeAuthorizationReasonAnnotation),
		},
	}
	for _, required := range []struct{ key, value string }{
		{sandboxRuntimeUpgradeTargetEnvironmentAnnotation, intent.targetEnvironment},
		{sandboxRuntimeUpgradeAuthorizationIDAnnotation, intent.authorization.AuthorizationId},
		{sandboxRuntimeUpgradeAuthorizationReasonAnnotation, intent.authorization.Reason},
	} {
		if required.value == "" {
			return nil, fmt.Errorf("%s %s requires %s", sandboxCheckpointAnnotation, runtimeUpgradeCheckpoint, required.key)
		}
	}
	switch annotation(sandboxRuntimeUpgradeMemoryLossAnnotation) {
	case "true":
		intent.authorization.MemoryLoss = true
	case "false":
	default:
		return nil, fmt.Errorf("%s must be true or false", sandboxRuntimeUpgradeMemoryLossAnnotation)
	}
	switch annotation(sandboxRuntimeUpgradeTargetStateAnnotation) {
	case "started":
		intent.targetState = pb.WakeTargetState_WAKE_TARGET_STATE_STARTED
	case "stopped":
		intent.targetState = pb.WakeTargetState_WAKE_TARGET_STATE_STOPPED
	default:
		return nil, fmt.Errorf("%s must be started or stopped", sandboxRuntimeUpgradeTargetStateAnnotation)
	}
	if raw := annotation(sandboxRuntimeUpgradeCheckpointArtifactIDAnnotation); raw != "" {
		artifactID, err := uuid.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("%s must be a UUID: %w", sandboxRuntimeUpgradeCheckpointArtifactIDAnnotation, err)
		}
		intent.checkpointLocalArtifactID = ptr(artifactID.String())
	}
	return intent, nil
}

// runtimeUpgradeRequest turns the Start a runtime upgrade pod would send into
// its UpgradeRuntime: the same request, with the pod's namespace and route
// identity, as the plain blocking cold start the upgrade boots, which guest
// readiness verifies. A suspended source (one that names its checkpoint)
// carries the restore's storage URI and source node as where the node stages
// a sandbox absent from it; a stopped one must be on this node already.
func runtimeUpgradeRequest(start *pb.StartRequest, intent *runtimeUpgradeIntent) *pb.UpgradeRuntimeRequest {
	var stageFrom *pb.RuntimeUpgradeStageFrom
	if intent.checkpointLocalArtifactID != nil && start.SuspendRestoreStorageUri != nil {
		stageFrom = &pb.RuntimeUpgradeStageFrom{
			SuspendRestoreStorageUri: *start.SuspendRestoreStorageUri,
			SuspendRestoreSourceNode: start.SuspendRestoreSourceNode,
		}
	}
	start.Checkpoint = nil
	start.SuspendRestoreStorageUri = nil
	start.SuspendRestoreSourceNode = nil
	start.RestoreArtifactAsNew = nil
	start.ExpectedLocalArtifactId = nil
	start.TargetState = pb.WakeTargetState_WAKE_TARGET_STATE_UNSPECIFIED
	start.Blocking = true
	return &pb.UpgradeRuntimeRequest{
		SandboxId:                 start.SandboxId,
		OperationId:               intent.operationID,
		TargetEnvironment:         intent.targetEnvironment,
		Authorization:             intent.authorization,
		Start:                     start,
		TargetState:               intent.targetState,
		StageFrom:                 stageFrom,
		CheckpointLocalArtifactId: intent.checkpointLocalArtifactID,
	}
}

// stopped reports whether the upgrade leaves the sandbox stopped, so the pod
// runs a placeholder task with no VM.
func (intent *runtimeUpgradeIntent) stopped() bool {
	return intent.targetState == pb.WakeTargetState_WAKE_TARGET_STATE_STOPPED
}

// upgradeRuntime runs a runtime upgrade pod's UpgradeRuntime. For a started
// target it returns the running execution, which sbx-manager reports
// whether it booted it now, activated a sandbox already on the target, or
// found this pod's execution from an earlier attempt. For a stopped target
// it returns no execution: the VM, if one booted, is stopped again. Every
// error is the create's; there is no fallback to Start.
func upgradeRuntime(ctx context.Context, client pb.SandboxManagerClient, start *pb.StartRequest, intent *runtimeUpgradeIntent) (*pb.StartResponse, error) {
	response, err := client.UpgradeRuntime(ctx, runtimeUpgradeRequest(start, intent))
	if err != nil {
		return nil, err
	}
	switch response.GetOutcome() {
	case pb.RuntimeUpgradeOutcome_RUNTIME_UPGRADE_OUTCOME_UPGRADED,
		pb.RuntimeUpgradeOutcome_RUNTIME_UPGRADE_OUTCOME_ALREADY_ON_TARGET:
	default:
		return nil, status.Errorf(codes.Internal, "sbx-manager UpgradeRuntime returned outcome %s", response.GetOutcome())
	}
	if intent.stopped() {
		return nil, nil
	}
	// A sandbox-manager that predates activation answers ALREADY_ON_TARGET
	// with no execution; the pod has nothing to run.
	if response.GetStart().GetPid() == 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "sbx-manager UpgradeRuntime (%s) returned no running sandbox for a started target", response.GetOutcome())
	}
	return response.GetStart(), nil
}
