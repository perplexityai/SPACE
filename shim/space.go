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
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	cgroupsv2 "github.com/containerd/cgroups/v3/cgroup2/stats"
	taskAPI "github.com/containerd/containerd/api/runtime/task/v3"
	apitypes "github.com/containerd/containerd/api/types"
	tasktypes "github.com/containerd/containerd/api/types/task"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	ptypes "github.com/containerd/containerd/v2/pkg/protobuf/types"
	"github.com/containerd/containerd/v2/pkg/shim"
	"github.com/containerd/containerd/v2/pkg/shutdown"
	"github.com/containerd/containerd/v2/plugins"
	"github.com/containerd/errdefs"
	"github.com/containerd/plugin"
	"github.com/containerd/plugin/registry"
	"github.com/containerd/ttrpc"
	"github.com/containerd/typeurl/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	gatewaypb "github.com/perplexityai/SPACE/shim/proto/net/gateway/v1"
	netpb "github.com/perplexityai/SPACE/shim/proto/net/policy/v1"
	pb "github.com/perplexityai/SPACE/shim/proto/sandbox/manager/v1"
	operationspb "github.com/perplexityai/SPACE/shim/proto/spaced/operations/v1"
)

const (
	sbxMgrSocket       = "/space/run/sandbox-manager/sandbox-manager.sock"
	runtime            = "fc"
	defaultTemplate    = "seed"
	defaultVCPU        = 2
	defaultMemoryMiB   = 1024
	defaultProxyPort   = 9800
	defaultNodeAPIPort = 9800
	maxRouteGeneration = uint64(1<<63 - 1)

	sandboxIDAnnotation                          = "space.pplx.net/sandbox-id"
	sandboxAutoLifecycleAnnotation               = "space.pplx.net/auto-lifecycle"
	sandboxAutoLifecycleAuthorityAnnotation      = "space.pplx.net/auto-lifecycle-authority"
	sandboxNetworkPolicyAnnotation               = "space.pplx.net/network-policy"
	sandboxNetworkPolicyGenerationAnnotation     = "space.pplx.net/network-policy-generation"
	sandboxNetworkPolicyAuthorityAnnotation      = "space.pplx.net/network-policy-authority"
	sandboxCheckpointAnnotation                  = "space.pplx.net/checkpoint"
	sandboxVCPUAnnotation                        = "space.pplx.net/vcpu"
	sandboxMemoryMiBAnnotation                   = "space.pplx.net/memory-mib"
	sandboxExecutionGenerationAnnotation         = "space.pplx.net/execution-generation"
	sandboxActivationAttemptIDAnnotation         = "space.pplx.net/activation-attempt-id"
	sandboxRouteGenerationAnnotation             = "space.pplx.net/route-generation"
	sandboxPodUIDAnnotation                      = "space.pplx.net/pod-uid"
	sandboxTeardownProtocolAnnotation            = "space.pplx.net/teardown-protocol"
	sandboxTeardownProtocolDurableSuspend        = "generation-suspend-v1"
	sandboxSuspendRestoreStorageURIAnnotation    = "space.pplx.net/suspend-restore-storage-uri"
	sandboxSuspendRestoreSourceNodeAnnotation    = "space.pplx.net/suspend-restore-source-node"
	sandboxArtifactRestoreAsNewAnnotation        = "space.pplx.net/fork-disk-restore"
	sandboxDiskMibAnnotation                     = "space.pplx.net/disk-mib"
	sandboxDiskIOMaxOperationsPerSecAnnotation   = "space.pplx.net/disk-io-max-operations-per-sec"
	sandboxDiskIOMaxBandwidthMiBPerSecAnnotation = "space.pplx.net/disk-io-max-bandwidth-mib-per-sec"
	sandboxOwnerAnnotation                       = "space.pplx.net/sandbox-owner"
	sandboxUserAnnotation                        = "space.pplx.net/sandbox-user"
	sandboxVolumeEncryptionAnnotation            = "space.pplx.net/volume-encryption"
	kubernetesPodUIDAnnotation                   = "io.kubernetes.pod.uid"
	kubernetesCriSandboxUIDAnnotation            = "io.kubernetes.cri.sandbox-uid"

	sandboxStateMaxAttempts       = 3
	sandboxStateRetryDelay        = 100 * time.Millisecond
	teardownSuspendMaxWait        = 45 * time.Second
	teardownSuspendNoDeadlineWait = 5 * time.Second
	teardownSuspendPoll           = 250 * time.Millisecond

	sbxMgrConnectTimeout      = 5 * time.Second
	connectRetryBudget        = 10 * time.Second
	connectRetryInitialDelay  = 100 * time.Millisecond
	connectRetryMaxDelay      = 1 * time.Second
	connectRetryJitterPercent = 20
)

var (
	errReadOCISpec  = errors.New("read OCI spec")
	errParseOCISpec = errors.New("parse OCI spec")

	spaceK8sNodeIDNamespace = uuid.MustParse("2a5fef2d-8a55-4d2a-b87f-1d2f77d0c3a1")
)

func ptr[T any](value T) *T {
	return &value
}

func proxyPort() int {
	return defaultProxyPort
}

func nodeAPIProxyTargetPort() int {
	return defaultNodeAPIPort
}

func init() {
	registry.Register(&plugin.Registration{
		Type: plugins.TTRPCPlugin,
		ID:   "task",
		Requires: []plugin.Type{
			plugins.EventPlugin,
			plugins.InternalPlugin,
		},
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			pp, err := ic.GetByID(plugins.EventPlugin, "publisher")
			if err != nil {
				return nil, err
			}
			ss, err := ic.GetByID(plugins.InternalPlugin, "shutdown")
			if err != nil {
				return nil, err
			}
			return newTaskService(pp.(shim.Publisher), ss.(shutdown.Service))
		},
	})
}

// --- Manager ---

func NewManager(name string) shim.Manager { return &manager{name: name} }

type manager struct{ name string }

func (m manager) Name() string { return m.name }

func (m manager) Start(ctx context.Context, id string, opts shim.StartOpts) (_ shim.BootstrapParams, retErr error) {
	// Reject bad startup configuration without blocking containerd's separate
	// delete command, which must still return the dead shim's cleanup response.
	if err := validateMetricsConfiguration(); err != nil {
		return shim.BootstrapParams{}, fmt.Errorf("invalid metrics configuration: %w", err)
	}
	if err := validateTraceConfiguration(); err != nil {
		return shim.BootstrapParams{}, fmt.Errorf("invalid tracing configuration: %w", err)
	}
	var params shim.BootstrapParams
	params.Version = 3
	params.Protocol = "ttrpc"

	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return params, err
	}

	self, err := os.Executable()
	if err != nil {
		return params, err
	}

	cwd, err := os.Getwd()
	if err != nil {
		return params, err
	}

	// Group tasks by sandbox-id so pod sandbox + container share one shim.
	grouping := id
	if spec, err := readSpec(); err == nil {
		if sandboxID, ok := spec.Annotations["io.kubernetes.cri.sandbox-id"]; ok {
			grouping = sandboxID
		}
	}

	address, err := shim.SocketAddress(ctx, opts.Address, grouping, false)
	if err != nil {
		return params, err
	}

	socket, err := shim.NewSocket(address)
	if err != nil {
		if !shim.SocketEaddrinuse(err) {
			return params, fmt.Errorf("create shim socket: %w", err)
		}
		if shim.CanConnect(address) {
			params.Address = address
			return params, nil
		}
		if err := shim.RemoveSocket(address); err != nil {
			return params, fmt.Errorf("remove stale socket: %w", err)
		}
		if socket, err = shim.NewSocket(address); err != nil {
			return params, fmt.Errorf("recreate shim socket: %w", err)
		}
	}

	defer func() {
		if retErr != nil {
			socket.Close()
			shim.RemoveSocket(address)
		}
	}()

	f, err := socket.File()
	if err != nil {
		return params, err
	}
	defer f.Close()

	cmd := exec.Command(self, "-namespace", ns, "-id", id, "-address", opts.Address)
	cmd.Dir = cwd
	cmd.Env = append(os.Environ(), "GOMAXPROCS=2")
	cmd.ExtraFiles = append(cmd.ExtraFiles, f)

	if err := cmd.Start(); err != nil {
		return params, fmt.Errorf("start shim process: %w", err)
	}
	defer func() {
		if retErr != nil {
			cmd.Process.Kill()
		}
	}()
	go cmd.Wait()

	_ = shim.AdjustOOMScore(cmd.Process.Pid)

	params.Address = address
	return params, nil
}

func (m manager) Stop(ctx context.Context, id string) (shim.StopStatus, error) {
	return shim.StopStatus{ExitedAt: time.Now()}, nil
}

func (m manager) Info(ctx context.Context, optionsR io.Reader) (*apitypes.RuntimeInfo, error) {
	return &apitypes.RuntimeInfo{
		Name:    m.name,
		Version: &apitypes.RuntimeVersion{Version: "v0.2.0"},
	}, nil
}

// --- Task Service ---

// annotateStatus wraps a gRPC error from sbx-manager with the operation name
// and a namespaced marker carrying the gRPC code name
// (e.g. "[space-err=NotFound]"). The code is embedded in the message because
// containerd's CRI layer loses the gRPC status code by the time it surfaces
// as a K8s pod event:
//
//	shim (ttrpc) → containerd errgrpc.ToNative [1] → fmt.Errorf wrapping [2] → CRI gRPC → kubelet event
//
// The "space-err=" prefix namespaces the marker so the SPACE API's classifier
// won't false-match arbitrary bracketed text in upstream error messages.
//
// [1] https://github.com/containerd/errdefs/blob/main/pkg/errgrpc/grpc.go#L207
// [2] https://github.com/containerd/containerd/blob/main/internal/cri/server/podsandbox/sandbox_run.go#L267
func annotateStatus(op string, originalError error) error {
	if originalError == nil {
		return nil
	}

	st, ok := status.FromError(originalError)
	if !ok {
		return status.Errorf(codes.Unknown, "%s: %v", op, originalError)
	}

	p := st.Proto()
	p.Message = fmt.Sprintf("%s [space-err=%s]: %s", op, st.Code().String(), p.Message)
	return status.FromProto(p).Err()
}

type task struct {
	teardownMu          sync.Mutex // serializes manager teardown decisions with local task mutation
	id                  string
	sandboxID           string // sbx-manager sandbox ID (only set for pod sandbox tasks)
	executionGeneration int64  // canonical Kubernetes execution generation from the OCI annotation
	traceparent         string // create's serialized trace context, which teardown spans link to
	generationTeardown  bool   // final durable-teardown policy captured from the pod contract
	sandboxDetached     bool   // teardown was released without mutating a newer execution
	legacyStopIssued    bool   // this task initiated the stop preceding legacy deletion
	containerID         string // OCI container type
	pid                 int
	podIP               string
	netnsPath           string
	status              tasktypes.Status
	exitCode            uint32
	exitedAt            time.Time
	bundle              string
	stdout              *os.File
	stderr              *os.File
	execs               map[string]*execProcess
}

type execProcess struct {
	id       string
	args     []string
	status   tasktypes.Status
	exitCode uint32
	exitedAt time.Time
	stdout   *os.File
	stderr   *os.File
	done     chan struct{} // closed when exec completes
}

type sandboxManagerClientFactory func() (pb.SandboxManagerClient, io.Closer, error)

type spaceTaskService struct {
	mu                          sync.Mutex
	tasks                       map[string]*task
	sd                          shutdown.Service
	sandboxManagerClientFactory sandboxManagerClientFactory
}

func newTaskService(_ shim.Publisher, sd shutdown.Service) (*spaceTaskService, error) {
	if err := validateMetricsConfiguration(); err != nil {
		return nil, fmt.Errorf("invalid metrics configuration: %w", err)
	}
	if err := validateTraceConfiguration(); err != nil {
		return nil, fmt.Errorf("invalid tracing configuration: %w", err)
	}
	return &spaceTaskService{
		tasks: make(map[string]*task),
		sd:    sd,
	}, nil
}

var _ shim.TTRPCService = (*spaceTaskService)(nil)

func (s *spaceTaskService) RegisterTTRPC(server *ttrpc.Server) error {
	taskAPI.RegisterTTRPCTaskService(server, s)
	return nil
}

func (s *spaceTaskService) Create(ctx context.Context, r *taskAPI.CreateTaskRequest) (_ *taskAPI.CreateTaskResponse, retErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	logShim("Create: id=%s bundle=%s", r.ID, r.Bundle)
	createStartedAt := time.Now()

	spec, err := readSpecFrom(r.Bundle)
	if err != nil {
		emitShimStepError("create", "read_spec", "read_spec", err)
		if errors.Is(err, errParseOCISpec) {
			return nil, status.Errorf(codes.InvalidArgument, "%s: %v", r.Bundle, err)
		}
		return nil, status.Errorf(codes.Internal, "%s: %v", r.Bundle, err)
	}
	containerType := spec.Annotations["io.kubernetes.cri.container-type"]
	sandboxTemplate := spec.sandboxTemplate()
	sandboxContentHash := spec.sandboxContentHash()
	sandboxAutoLifecycle, err := spec.sandboxAutoLifecycle()
	if err != nil {
		emitShimStepError("create", "parse_auto_lifecycle", "parse_auto_lifecycle", err)
		return nil, status.Errorf(codes.InvalidArgument, "parse sandbox auto lifecycle: %v", err)
	}
	sandboxAutoLifecycleAuthority, err := spec.sandboxAutoLifecycleAuthority(sandboxAutoLifecycle != nil)
	if err != nil {
		emitShimStepError("create", "parse_auto_lifecycle_authority", "parse_auto_lifecycle_authority", err)
		return nil, status.Errorf(codes.InvalidArgument, "parse sandbox auto lifecycle authority: %v", err)
	}
	sandboxNetworkPolicy, err := spec.sandboxNetworkPolicy()
	if err != nil {
		emitShimStepError("create", "parse_network_policy", "parse_network_policy", err)
		return nil, status.Errorf(codes.InvalidArgument, "parse sandbox network policy: %v", err)
	}
	sandboxEnv := spec.sandboxEnv()
	sandboxExecutionGeneration, err := spec.sandboxExecutionGeneration()
	if err != nil {
		emitShimStepError("create", "parse_execution_generation", "parse_execution_generation", err)
		return nil, status.Errorf(codes.InvalidArgument, "parse sandbox execution generation: %v", err)
	}
	sandboxRouteGeneration, err := spec.sandboxRouteGeneration(sandboxExecutionGeneration)
	if err != nil {
		emitShimStepError("create", "parse_route_generation", "parse_route_generation", err)
		return nil, status.Errorf(codes.InvalidArgument, "parse sandbox route generation: %v", err)
	}
	sandboxCurrentNodeID := currentNodeID()
	sandboxPodUID := spec.sandboxPodUID()
	sandboxActivationAttemptID, err := spec.sandboxActivationAttemptID()
	if err != nil {
		emitShimStepError("create", "parse_activation_attempt_id", "parse_activation_attempt_id", err)
		return nil, status.Errorf(codes.InvalidArgument, "parse sandbox activation attempt id: %v", err)
	}
	sandboxVCPU, err := spec.sandboxVCPU()
	if err != nil {
		emitShimStepError("create", "parse_vcpu", "parse_vcpu", err)
		return nil, status.Errorf(codes.InvalidArgument, "parse sandbox vcpu: %v", err)
	}
	sandboxMemoryMiB, err := spec.sandboxMemoryMiB()
	if err != nil {
		emitShimStepError("create", "parse_memory_mib", "parse_memory_mib", err)
		return nil, status.Errorf(codes.InvalidArgument, "parse sandbox memory: %v", err)
	}
	sandboxDiskIOMaxOperationsPerSec, err := spec.positiveUint64Annotation(sandboxDiskIOMaxOperationsPerSecAnnotation)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse sandbox disk IOPS: %v", err)
	}
	sandboxDiskIOMaxBandwidthMiBPerSec, err := spec.positiveUint64Annotation(sandboxDiskIOMaxBandwidthMiBPerSecAnnotation)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse sandbox disk bandwidth: %v", err)
	}

	t := &task{id: r.ID, status: tasktypes.Status_CREATED, bundle: r.Bundle, containerID: containerType}
	if sandboxExecutionGeneration != nil {
		t.executionGeneration = *sandboxExecutionGeneration
	}
	t.generationTeardown = requiresGenerationTeardown(
		t.executionGeneration,
		spec.Annotations[sandboxTeardownProtocolAnnotation],
	)

	// Open I/O FIFOs to prevent containerd from killing the task.
	if r.Stdout != "" {
		t.stdout, _ = openFIFO(r.Stdout)
	}
	if r.Stderr != "" {
		t.stderr, _ = openFIFO(r.Stderr)
	}

	if containerType == "container" {
		// Container task — share the pod sandbox's FC process.
		for _, existing := range s.tasks {
			if existing.sandboxID != "" {
				t.pid = existing.pid
				logShim("Create: container %s sharing sandbox %s (pid %d)", r.ID, existing.sandboxID, t.pid)
				s.tasks[r.ID] = t
				emitShimOperationLatency("create", createStartedAt, shimMetricResultSuccess, metricTag("kind", "container"))
				return &taskAPI.CreateTaskResponse{Pid: uint32(t.pid)}, nil
			}
		}
		// No sandbox found — return shim PID as fallback.
		t.pid = os.Getpid()
		s.tasks[r.ID] = t
		emitShimOperationLatency("create", createStartedAt, shimMetricResultSuccess, metricTag("kind", "container_fallback"))
		return &taskAPI.CreateTaskResponse{Pid: uint32(t.pid)}, nil
	}

	// Pod sandbox task — join pod cgroup, then create + start via sbx-manager.
	createSpanOpts := []attribute.KeyValue{
		attribute.String("template", sandboxTemplate),
		attribute.String("task_id", r.ID),
	}
	span, ctx := startSpanWithRemoteParent(
		ctx,
		"shim.create",
		spec.traceparent(),
		createSpanOpts...,
	)
	defer func() { span.End(retErr) }()
	t.traceparent = serializedTraceparent(ctx)

	joinedCgroup, err := joinPodCgroup(r.Bundle)
	if err != nil {
		logShim("Create: joinPodCgroup failed (non-fatal): %v", err)
	}
	createSucceeded := false
	defer func() {
		if !createSucceeded {
			joinedCgroup.cleanupAfterFailedCreate()
		}
	}()

	client, conn, err := s.sandboxManagerClient()
	if err != nil {
		emitShimStepError("create", "dial_sbxmgr", "dial_sbxmgr", err)
		return nil, status.Errorf(codes.Unavailable, "connect to sbx-manager: %v", err)
	}
	defer conn.Close()

	// Create the sandbox.
	requestedSandboxID := spec.requestedSandboxID()
	sandboxCheckpoint, artifactRestore, useStartRPC, parseErr := spec.sandboxStartIntent()
	if parseErr != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid sandbox start annotations: %v", parseErr)
	}
	if artifactRestore != nil {
		artifactRestore.DiskIoMaxOperationsPerSec = sandboxDiskIOMaxOperationsPerSec
		artifactRestore.DiskIoMaxBandwidthMibPerSec = sandboxDiskIOMaxBandwidthMiBPerSec
	}
	if useStartRPC {
		if requestedSandboxID == nil {
			emitShimStepError("create", "parse_checkpoint_sandbox_id", "parse_checkpoint_sandbox_id", fmt.Errorf("%s requires %s", sandboxCheckpointAnnotation, sandboxIDAnnotation))
			return nil, status.Errorf(codes.InvalidArgument, "%s requires %s", sandboxCheckpointAnnotation, sandboxIDAnnotation)
		}
		sbxID := requestedSandboxID.String()
		span.SetAttributes(attribute.String("sandbox_id", sbxID))
		netnsPath := spec.networkNamespace()
		if netnsPath != "" {
			logShim("Create: using CNI network namespace %s", netnsPath)
		}
		restoreMode := sandboxRestoreModeForLog(sandboxCheckpoint, artifactRestore)
		redirectCh := make(chan dataPlaneRedirectResult, 1)
		go prepareDataPlaneRedirect(ctx, sbxID, netnsPath, sandboxTemplate, redirectCh)
		startRPCStartedAt := time.Now()
		startSpanSbxmgr, startCtx := startSpan(ctx, "shim.sbxmgr.start",
			attribute.String("template", sandboxTemplate),
			attribute.String("sandbox_id", sbxID),
			attribute.String("restore_mode", restoreMode),
		)
		startRequest := &pb.StartRequest{
			SandboxId:                sbxID,
			Vcpu:                     ptr(sandboxVCPU),
			MemoryMib:                ptr(sandboxMemoryMiB),
			Checkpoint:               sandboxCheckpoint,
			Blocking:                 false,
			NetnsPath:                ptr(netnsPath),
			SuspendRestoreStorageUri: spec.sandboxSuspendRestoreStorageURI(),
			SuspendRestoreSourceNode: spec.sandboxSuspendRestoreSourceNode(),
			RestoreArtifactAsNew:     artifactRestore,
			RouteIdentity: routeIdentity(
				sandboxRouteGeneration,
				sandboxCurrentNodeID,
				sandboxExecutionGeneration,
				sandboxPodUID,
				sandboxActivationAttemptID,
			),
		}
		if sandboxCheckpoint != nil && *sandboxCheckpoint == "stopped" {
			startRequest.Checkpoint, err = stoppedRestartCheckpoint(startCtx, client, sbxID, startRequest.SuspendRestoreStorageUri)
			if err != nil {
				startSpanSbxmgr.End(err)
				emitShimStepError("create", "stopped_restart", "stopped_restart", err)
				redirect := <-redirectCh
				if redirect.podIP != "" {
					removeDataPlaneRedirect(netnsPath, sbxID)
				}
				return nil, err
			}
		}
		var startResp *pb.StartResponse
		if sandboxAutoLifecycle != nil && sandboxAutoLifecycleAuthority == netpb.PolicyAuthority_POLICY_AUTHORITY_CLUSTER {
			startRequest.ActivationAutoLifecycle = sandboxAutoLifecycle
			err = requireActivationPolicySupport(startCtx, conn)
		}
		if err == nil {
			if sandboxCheckpoint != nil && *sandboxCheckpoint == "stopped" && startRequest.Checkpoint != nil {
				startResp, err = startStoppedCheckpoint(startCtx, client, startRequest)
			} else {
				startResp, err = client.Start(startCtx, startRequest)
			}
		}
		startSpanSbxmgr.End(err)
		redirect := <-redirectCh
		if err != nil {
			if redirect.podIP != "" {
				removeDataPlaneRedirect(netnsPath, sbxID)
			}
			emitShimStepError("create", "sbxmgr_start", "sbxmgr_start", err, metricTag("template", sandboxTemplate))
			return nil, annotateStatus("sbx-manager Start", err)
		}
		if redirect.err != nil {
			if redirect.podIP != "" {
				removeDataPlaneRedirect(netnsPath, sbxID)
			}
			emitShimStepError("create", "dataplane_redirect", "dataplane_redirect", redirect.err, metricTag("template", sandboxTemplate))
			return nil, status.Errorf(codes.Unavailable, "prepare data-plane redirect: %v", redirect.err)
		}
		emitShimStepLatency("create", "sbxmgr_start", startRPCStartedAt, shimMetricResultSuccess, metricTag("template", sandboxTemplate))
		if startResp.GetPid() == 0 {
			if redirect.podIP != "" {
				removeDataPlaneRedirect(netnsPath, sbxID)
			}
			emitShimStepError("create", "sbxmgr_start_pid", "missing_sandbox_pid", nil, metricTag("template", sandboxTemplate))
			return nil, status.Errorf(codes.Internal, "sbx-manager Start did not return a running sandbox pid")
		}

		t.sandboxID = sbxID
		t.pid = int(startResp.GetPid())
		t.podIP = redirect.podIP
		t.netnsPath = netnsPath
		s.tasks[r.ID] = t
		createSucceeded = true
		if t.podIP != "" {
			logShim(
				"Create: data-plane redirect installed for sandbox %s at %s:%d -> node-api:%d",
				sbxID,
				t.podIP,
				proxyPort(),
				nodeAPIProxyTargetPort(),
			)
		}
		logShim("Create: restored sandbox %s active (template=%s restore_mode=%s vcpu=%d memory_mib=%d env_count=%d pid=%d, ns=%s)",
			sbxID, sandboxTemplate, restoreMode, sandboxVCPU, sandboxMemoryMiB, len(sandboxEnv), t.pid, startResp.GetNamespace())
		emitShimOperationLatency("create", createStartedAt, shimMetricResultSuccess, metricTag("template", sandboxTemplate))
		return &taskAPI.CreateTaskResponse{Pid: uint32(t.pid)}, nil
	}
	sandboxOwner, err := spec.sandboxOwner()
	if err != nil {
		emitShimStepError("create", "parse_sandbox_owner", "parse_sandbox_owner", err)
		return nil, status.Errorf(codes.InvalidArgument, "parse sandbox owner: %v", err)
	}
	rootfsSizeMb, err := spec.sandboxRootfsSizeMb()
	if err != nil {
		emitShimStepError("create", "parse_rootfs_size", "parse_rootfs_size", err)
		return nil, status.Errorf(codes.InvalidArgument, "parse sandbox rootfs size: %v", err)
	}
	volumeEncryption, err := spec.sandboxVolumeEncryption()
	if err != nil {
		emitShimStepError("create", "parse_volume_encryption", "parse_volume_encryption", err)
		return nil, status.Errorf(codes.InvalidArgument, "parse sandbox volume encryption: %v", err)
	}
	createReq := &pb.CreateRequest{
		Template:                sandboxTemplate,
		Owner:                   sandboxOwner,
		Runtime:                 runtime,
		ContentHash:             sandboxContentHash,
		AutoLifecycle:           sandboxAutoLifecycle,
		AutoLifecycleAuthority:  sandboxAutoLifecycleAuthority,
		RootfsSizeMb:            rootfsSizeMb,
		BootMode:                sandboxCreateBootMode(rootfsSizeMb),
		NetworkPolicy:           sandboxNetworkPolicy.Policy,
		NetworkPolicyGeneration: sandboxNetworkPolicy.Generation,
		NetworkPolicyAuthority:  sandboxNetworkPolicy.Authority,
		RouteIdentity: routeIdentity(
			sandboxRouteGeneration,
			sandboxCurrentNodeID,
			sandboxExecutionGeneration,
			sandboxPodUID,
			sandboxActivationAttemptID,
		),
		VolumeEncryption:            volumeEncryption,
		DiskIoMaxOperationsPerSec:   sandboxDiskIOMaxOperationsPerSec,
		DiskIoMaxBandwidthMibPerSec: sandboxDiskIOMaxBandwidthMiBPerSec,
	}
	if requestedSandboxID != nil {
		createReq.SandboxId = ptr(requestedSandboxID.String())
	}
	createRPCStartedAt := time.Now()
	createSpan, createCtx := startSpan(ctx, "shim.sbxmgr.create", attribute.String("template", sandboxTemplate))
	createResp, err := spec.createSandbox(createCtx, client, createReq)
	createSpan.End(err)
	if err != nil {
		emitShimStepError("create", "sbxmgr_create", "sbxmgr_create", err, metricTag("template", sandboxTemplate))
		return nil, annotateStatus("sbx-manager Create", err)
	}
	emitShimStepLatency("create", "sbxmgr_create", createRPCStartedAt, shimMetricResultSuccess, metricTag("template", sandboxTemplate))
	sbxID := createResp.SandboxId
	reusedExisting := createResp.ReusedExisting
	span.SetAttributes(attribute.String("sandbox_id", sbxID))
	logShim("Create: sbx-manager created sandbox %s", sbxID)
	cleanupCreatedSandbox := func(stage string) {
		if reusedExisting {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, cleanupErr := client.Delete(cleanupCtx, &pb.DeleteRequest{SandboxId: sbxID}); cleanupErr != nil {
			logShim("Create: cleanup after %s failed for sandbox %s: %v", stage, sbxID, cleanupErr)
		}
	}

	// Wake the sandbox. For newly-created sandboxes this starts it; for existing
	// sandboxes it reattaches and resumes/restores as needed. The dataplane
	// redirect can be installed as soon as CNI assigns the pod IP, so do that in
	// parallel with sbx-manager Wake to avoid API readiness probes racing the nft
	// rule.
	netnsPath := spec.networkNamespace()
	if netnsPath != "" {
		logShim("Create: using CNI network namespace %s", netnsPath)
	}
	redirectCh := make(chan dataPlaneRedirectResult, 1)
	go prepareDataPlaneRedirect(ctx, sbxID, netnsPath, sandboxTemplate, redirectCh)

	wakeRPCStartedAt := time.Now()
	wakeSpanSbxmgr, wakeCtx := startSpan(ctx, "shim.sbxmgr.wake",
		attribute.String("template", sandboxTemplate),
		attribute.String("sandbox_id", sbxID),
	)
	_, err = client.Wake(wakeCtx, &pb.WakeRequest{
		SandboxId: sbxID,
		Blocking:  false,
		StartContext: &pb.WakeStartContext{
			Vcpu:      ptr(sandboxVCPU),
			MemoryMib: ptr(sandboxMemoryMiB),
			NetnsPath: ptr(netnsPath),
			RouteIdentity: routeIdentity(
				sandboxRouteGeneration,
				sandboxCurrentNodeID,
				sandboxExecutionGeneration,
				sandboxPodUID,
				sandboxActivationAttemptID,
			),
		},
	})
	wakeSpanSbxmgr.End(err)
	redirect := <-redirectCh
	if err != nil {
		// Clean up the created sandbox on wake failure, but never delete a
		// pre-existing sandbox we merely reattached to.
		if redirect.podIP != "" {
			removeDataPlaneRedirect(netnsPath, sbxID)
		}
		cleanupCreatedSandbox("wake failure")
		emitShimStepError("create", "sbxmgr_wake", "sbxmgr_wake", err, metricTag("template", sandboxTemplate))
		return nil, annotateStatus("sbx-manager Wake", err)
	}
	if redirect.err != nil {
		if redirect.podIP != "" {
			removeDataPlaneRedirect(netnsPath, sbxID)
		}
		cleanupCreatedSandbox("data-plane redirect failure")
		emitShimStepError("create", "dataplane_redirect", "dataplane_redirect", redirect.err, metricTag("template", sandboxTemplate))
		return nil, status.Errorf(codes.Unavailable, "prepare data-plane redirect: %v", redirect.err)
	}
	emitShimStepLatency("create", "sbxmgr_wake", wakeRPCStartedAt, shimMetricResultSuccess, metricTag("template", sandboxTemplate))

	stateResp, err := sandboxStateWithRetry(ctx, client, sbxID)
	if err != nil {
		if redirect.podIP != "" {
			removeDataPlaneRedirect(netnsPath, sbxID)
		}
		cleanupCreatedSandbox("post-wake state failure")
		emitShimStepError("create", "sbxmgr_state", "sbxmgr_state", err, metricTag("template", sandboxTemplate))
		return nil, annotateStatus("sbx-manager State after Wake", err)
	}
	sandboxState := firstSandboxState(stateResp)
	if sandboxState == nil || sandboxState.GetPid() == 0 {
		if redirect.podIP != "" {
			removeDataPlaneRedirect(netnsPath, sbxID)
		}
		cleanupCreatedSandbox("post-wake state pid failure")
		emitShimStepError("create", "sbxmgr_state_pid", "missing_sandbox_pid", nil, metricTag("template", sandboxTemplate))
		return nil, status.Errorf(codes.Internal, "sbx-manager State after Wake did not return a running sandbox pid")
	}

	t.sandboxID = sbxID
	t.pid = int(sandboxState.GetPid())
	t.podIP = redirect.podIP
	t.netnsPath = netnsPath
	if t.podIP != "" {
		logShim(
			"Create: data-plane redirect installed for sandbox %s at %s:%d -> node-api:%d",
			sbxID,
			t.podIP,
			proxyPort(),
			nodeAPIProxyTargetPort(),
		)
	}
	logShim("Create: sandbox %s active (template=%s vcpu=%d memory_mib=%d env_count=%d pid=%d, ns=%s, reused_existing=%t)",
		sbxID, sandboxTemplate, sandboxVCPU, sandboxMemoryMiB, len(createReq.Env), t.pid, sandboxState.GetNamespace(), reusedExisting)

	s.tasks[r.ID] = t
	createSucceeded = true
	emitShimOperationLatency("create", createStartedAt, shimMetricResultSuccess, metricTag("template", sandboxTemplate))
	return &taskAPI.CreateTaskResponse{Pid: uint32(t.pid)}, nil
}

func (s *spaceTaskService) Start(ctx context.Context, r *taskAPI.StartRequest) (*taskAPI.StartResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	logShim("Start: id=%s exec_id=%s", r.ID, r.ExecID)
	startedAt := time.Now()

	t, ok := s.tasks[r.ID]
	if !ok {
		return nil, errdefs.ErrNotFound
	}

	if r.ExecID != "" {
		ep, ok := t.execs[r.ExecID]
		if !ok {
			return nil, errdefs.ErrNotFound
		}
		ep.status = tasktypes.Status_RUNNING
		sandboxID := s.findSandboxIDLocked()
		if sandboxID == "" {
			return nil, fmt.Errorf("no sandbox found for exec")
		}
		go s.runExec(sandboxID, ep)
		emitShimOperationLatency("start", startedAt, shimMetricResultSuccess, metricTag("kind", "exec"))
		return &taskAPI.StartResponse{Pid: uint32(t.pid)}, nil
	}

	t.status = tasktypes.Status_RUNNING
	emitShimOperationLatency("start", startedAt, shimMetricResultSuccess, metricTag("kind", "task"))
	return &taskAPI.StartResponse{Pid: uint32(t.pid)}, nil
}

type taskTeardownIdentity struct {
	sandboxID           string
	executionGeneration int64
	generationTeardown  bool
	sandboxDetached     bool
	legacyStopIssued    bool
}

func (task *task) teardownIdentity() taskTeardownIdentity {
	return taskTeardownIdentity{
		sandboxID:           task.sandboxID,
		executionGeneration: task.executionGeneration,
		generationTeardown:  task.generationTeardown,
		sandboxDetached:     task.sandboxDetached,
		legacyStopIssued:    task.legacyStopIssued,
	}
}

// logContext names the sbx-manager row and the teardown path a refusal log
// line is about.
func (identity taskTeardownIdentity) logContext() string {
	path := "legacy"
	if identity.generationTeardown {
		path = "generation"
	}
	return fmt.Sprintf("sandbox=%s generation=%d teardown=%s", identity.sandboxID, identity.executionGeneration, path)
}

// Generation one requires the pod annotation because it predates the durable
// teardown contract. Every later generation must use generation-fenced
// teardown even when an older controller omitted the annotation.
func requiresGenerationTeardown(executionGeneration int64, teardownProtocol string) bool {
	return executionGeneration > 1 || strings.TrimSpace(teardownProtocol) == sandboxTeardownProtocolDurableSuspend
}

// releaseTaskAfterGenerationTeardown waits for generation-fenced proof that
// the task is durable, superseded, or intentionally deleted.
func releaseTaskAfterGenerationTeardown(ctx context.Context, client pb.SandboxManagerClient, identity taskTeardownIdentity) error {
	if identity.sandboxDetached {
		return nil
	}
	if identity.executionGeneration <= 0 {
		return status.Error(codes.FailedPrecondition, "durable teardown requires an execution generation")
	}
	waitCtx, cancel := teardownSuspendWaitContext(ctx)
	defer cancel()
	for {
		if err := waitCtx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		response, err := client.PrepareTeardownSuspend(waitCtx, &pb.PrepareTeardownSuspendRequest{
			SandboxId:           identity.sandboxID,
			ExecutionGeneration: identity.executionGeneration,
			Action:              pb.TeardownSuspendAction_TEARDOWN_SUSPEND_ACTION_BEGIN_OR_JOIN,
		})
		if err != nil {
			return annotateStatus("sbx-manager PrepareTeardownSuspend", err)
		}
		switch response.GetOutcome() {
		case pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_DURABLE,
			pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_SUPERSEDED,
			pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_DELETED:
			return nil
		case pb.TeardownSuspendOutcome_TEARDOWN_SUSPEND_OUTCOME_PENDING:
			if err := sleepContext(waitCtx, teardownSuspendPoll); err != nil {
				return status.FromContextError(err).Err()
			}
		default:
			return status.Errorf(
				codes.Internal,
				"sbx-manager returned unknown teardown suspend outcome %d",
				response.GetOutcome(),
			)
		}
	}
}

func teardownSuspendWaitContext(ctx context.Context) (context.Context, context.CancelFunc) {
	remaining := time.Duration(0)
	hasDeadline := false
	if deadline, ok := ctx.Deadline(); ok {
		remaining = time.Until(deadline)
		hasDeadline = true
	}
	return context.WithTimeout(ctx, teardownSuspendBudget(remaining, hasDeadline))
}

func teardownSuspendBudget(remaining time.Duration, hasDeadline bool) time.Duration {
	if !hasDeadline {
		return teardownSuspendNoDeadlineWait
	}
	if remaining <= 0 {
		return time.Nanosecond
	}
	if remaining > teardownSuspendMaxWait {
		return teardownSuspendMaxWait
	}
	return remaining
}

func sandboxStateWithRetry(ctx context.Context, client pb.SandboxManagerClient, sandboxID string) (*pb.StateResponse, error) {
	var lastErr error
	for attempt := 1; attempt <= sandboxStateMaxAttempts; attempt++ {
		resp, err := client.State(ctx, &pb.StateRequest{SandboxIds: []string{sandboxID}})
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if !retryableSandboxStatusError(err) || attempt == sandboxStateMaxAttempts {
			break
		}
		logShim("State: retrying sbx-manager State after transient error: sandbox_id=%s attempt=%d err=%v", sandboxID, attempt, err)
		if err := sleepContext(ctx, sandboxStateRetryDelay); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

// releaseLegacyTaskTeardown preserves the behavior for pods created before
// the durable-teardown marker was rolled out. Their controller first
// stops and backs up retirements, so the shim must not invoke the new suspend
// protocol against an already-stopped row. It returns true when the observed
// lifecycle proves the sandbox is gone or this task belongs to a suspend/restore
// handoff and must not issue a later destructive teardown against the sandbox.
func releaseLegacyTaskTeardown(ctx context.Context, client pb.SandboxManagerClient, operation string, identity taskTeardownIdentity) (bool, error) {
	if identity.sandboxDetached {
		return true, nil
	}
	resp, err := sandboxStateWithRetry(ctx, client, identity.sandboxID)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return true, nil
		}
		return false, annotateStatus("sbx-manager State", err)
	}
	sandbox := firstSandboxState(resp)
	if sandbox == nil {
		return true, nil
	}
	lifecycle, err := observedSandboxLifecycle(sandbox)
	if err != nil {
		return false, err
	}
	switch lifecycle {
	case pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STOPPED:
		// A user STOP parks the rootfs for a later START. Releasing its pod
		// must not turn that intent into DELETE. A stop issued by this task's
		// own Kill remains part of intentional legacy pod deletion.
		if !identity.legacyStopIssued {
			return true, nil
		}
	case pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_SUSPENDING,
		pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_SUSPENDED,
		pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_RESTORING:
		return true, nil
	}

	switch operation {
	case "kill":
		_, err = client.Stop(ctx, &pb.StopRequest{SandboxId: identity.sandboxID})
	case "delete":
		_, err = client.Delete(ctx, &pb.DeleteRequest{SandboxId: identity.sandboxID})
		// Durable deletion may finish after State observed the live sandbox.
		if status.Code(err) == codes.NotFound {
			return true, nil
		}
	default:
		return false, status.Errorf(codes.Internal, "unsupported legacy teardown operation %q", operation)
	}
	if err != nil {
		return false, annotateStatus("sbx-manager legacy teardown", err)
	}
	return false, nil
}

func firstSandboxState(resp *pb.StateResponse) *pb.SandboxInfo {
	if resp == nil {
		return nil
	}
	sandboxes := resp.GetSandboxes()
	if len(sandboxes) == 0 {
		return nil
	}
	return sandboxes[0]
}

// A stopped restart uses its retained disk or an explicitly selected artifact.
// Never fall back to the latest historical suspend snapshot when that disk is
// missing: it may predate writes the user already observed.
func stoppedRestartCheckpoint(ctx context.Context, client pb.SandboxManagerClient, sandboxID string, storageURI *string) (*string, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		resp, err := sandboxStateWithRetry(waitCtx, client, sandboxID)
		if err != nil && status.Code(err) != codes.NotFound {
			return nil, annotateStatus("sbx-manager State for stopped restart", err)
		}
		sandbox := firstSandboxState(resp)
		if status.Code(err) == codes.NotFound || sandbox == nil {
			if storageURI != nil && strings.TrimSpace(*storageURI) != "" {
				return ptr("suspend"), nil
			}
			return nil, status.Error(codes.FailedPrecondition, "stopped rootfs is absent and no exact disk artifact was selected")
		}
		lifecycle, err := observedSandboxLifecycle(sandbox)
		if err != nil {
			return nil, err
		}
		switch lifecycle {
		case pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STOPPED:
			return nil, nil
		case pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STARTED:
			return ptr("suspend"), nil
		case pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_RESTORING:
			if storageURI != nil && strings.TrimSpace(*storageURI) != "" {
				// An interrupted stage can retain this state with no active
				// request. Start owns continuation and checks staging ownership,
				// runtime evidence and cleanup intent before resuming it.
				return ptr("suspend"), nil
			}
			fallthrough
		case pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STARTING,
			pb.SandboxLifecycleState_SANDBOX_LIFECYCLE_STATE_STOPPING:
			// containerd turns a failed task Create into an exited container.
			// With restartPolicy Never, wait here within the admission budget
			// instead of requiring a fresh Kubernetes execution for each poll.
			if err := sleepContext(waitCtx, sandboxStateRetryDelay); err != nil {
				return nil, status.FromContextError(err).Err()
			}
		default:
			return nil, status.Errorf(codes.FailedPrecondition, "stopped restart cannot use local lifecycle %s", lifecycle)
		}
	}
}

// A selected checkpoint Start may find another request staging its artifact.
// Retry the same intent for at most 30 seconds of admission waiting. An
// admitted transfer keeps the caller's original RPC deadline: the wait budget
// must not repeatedly cancel a healthy artifact pull that takes longer.
func startStoppedCheckpoint(ctx context.Context, client pb.SandboxManagerClient, request *pb.StartRequest) (*pb.StartResponse, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		if err := waitCtx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
		response, err := client.Start(ctx, request)
		if status.Code(err) != codes.Unavailable {
			return response, err
		}
		if err := sleepContext(waitCtx, sandboxStateRetryDelay); err != nil {
			return nil, status.FromContextError(err).Err()
		}
	}
}

func retryableSandboxStatusError(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
		return true
	default:
		return false
	}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// waitForTaskTeardownRelease snapshots one task under the service lock, then
// releases the lock for the potentially long manager poll. The caller holds
// the task's teardown lock and must revalidate and mutate the task atomically
// after this returns.
func (s *spaceTaskService) waitForTaskTeardownRelease(
	ctx context.Context,
	client pb.SandboxManagerClient,
	operation string,
	taskID string,
	expected *task,
) (bool, error) {
	s.mu.Lock()
	current, ok := s.tasks[taskID]
	if !ok {
		s.mu.Unlock()
		return false, nil
	}
	if current != expected {
		s.mu.Unlock()
		logShim("%s: task %s changed while preparing teardown; containerd will retry", operation, taskID)
		return false, status.Error(codes.Aborted, "task changed while preparing teardown")
	}
	identity := current.teardownIdentity()
	s.mu.Unlock()

	var detach bool
	var err error
	if identity.generationTeardown {
		err = releaseTaskAfterGenerationTeardown(ctx, client, identity)
		detach = err == nil
	} else {
		detach, err = releaseLegacyTaskTeardown(ctx, client, operation, identity)
	}
	// The task stays running until sbx-manager releases it, and each retry
	// otherwise leaves only a metric; this line is what names the refusal
	// when a pod sits in Terminating.
	if err != nil {
		logShim("%s: task %s keeps running because sbx-manager has not released it (%s); containerd will retry: %v", operation, taskID, identity.logContext(), err)
	}
	return detach, err
}

// Probe the same connection used by Start, without caching across reconnects.
// An old worker would otherwise accept Start and silently discard its policy.
func requireActivationPolicySupport(ctx context.Context, connection io.Closer) error {
	channel, ok := connection.(grpc.ClientConnInterface)
	if !ok {
		return status.Error(codes.Internal, "sandbox-manager connection cannot check activation policy support")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	response, err := healthpb.NewHealthClient(channel).Check(ctx, &healthpb.HealthCheckRequest{
		Service: "space.sandbox-manager.capability.activation-policy.v1",
	})
	if err != nil {
		return status.Errorf(codes.Unavailable, "sandbox-manager activation policy support: %v", err)
	}
	if response.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return status.Error(codes.Unavailable, "sandbox-manager does not support activation policy")
	}
	return nil
}

func (s *spaceTaskService) sandboxManagerClient() (pb.SandboxManagerClient, io.Closer, error) {
	if s.sandboxManagerClientFactory != nil {
		return s.sandboxManagerClientFactory()
	}
	conn, err := dialSbxManager()
	if err != nil {
		return nil, nil, err
	}
	return pb.NewSandboxManagerClient(conn), conn, nil
}

func (s *spaceTaskService) mutateTaskAfterTeardownRelease(
	ctx context.Context,
	operation string,
	taskID string,
	expected *task,
	needsTeardown bool,
	mutate func(*task),
) (bool, error) {
	expected.teardownMu.Lock()
	defer expected.teardownMu.Unlock()

	detachSandbox := false
	if needsTeardown {
		client, closer, err := s.sandboxManagerClient()
		if err != nil {
			emitShimStepError(operation, "dial_sbxmgr", "dial_sbxmgr", err)
			s.mu.Lock()
			identity := expected.teardownIdentity()
			s.mu.Unlock()
			logShim("%s: task %s keeps running because sbx-manager is unreachable (%s); containerd will retry: %v", operation, taskID, identity.logContext(), err)
			return false, status.Errorf(codes.Unavailable, "connect to sbx-manager during task %s: %v", operation, err)
		}
		defer func() {
			if closeErr := closer.Close(); closeErr != nil {
				logShim("%s: close sbx-manager connection failed: %v", operation, closeErr)
			}
		}()

		s.mu.Lock()
		sandboxID := expected.sandboxID
		s.mu.Unlock()
		// The wait for sbx-manager to release the task is what holds a pod in
		// Terminating, so it is the span a slow or refused teardown shows up in.
		releaseSpan, releaseCtx := startSpan(ctx, "shim.sbxmgr.teardown_release",
			attribute.String("op", operation),
			attribute.String("task_id", taskID),
			attribute.String("sandbox_id", sandboxID),
		)
		detachSandbox, err = s.waitForTaskTeardownRelease(releaseCtx, client, operation, taskID, expected)
		releaseSpan.End(err)
		if err != nil {
			emitShimStepError(operation, "prepare_teardown_suspend", "prepare_teardown_suspend", err)
			return false, err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.tasks[taskID]
	if !ok {
		return false, nil
	}
	if current != expected {
		logShim("%s: task %s changed after sbx-manager released it; containerd will retry", operation, taskID)
		return false, status.Error(codes.Aborted, "task changed after teardown release")
	}
	if detachSandbox {
		current.sandboxDetached = true
	}
	if needsTeardown && operation == "kill" && !detachSandbox && !current.generationTeardown {
		current.legacyStopIssued = true
	}
	mutate(current)
	return true, nil
}

func (s *spaceTaskService) Delete(ctx context.Context, r *taskAPI.DeleteRequest) (_ *taskAPI.DeleteResponse, retErr error) {
	s.mu.Lock()
	logShim("Delete: id=%s exec_id=%s", r.ID, r.ExecID)
	startedAt := time.Now()

	t, ok := s.tasks[r.ID]
	if !ok {
		s.mu.Unlock()
		return &taskAPI.DeleteResponse{ExitedAt: timestamppb.Now()}, nil
	}

	if r.ExecID != "" {
		ep, ok := t.execs[r.ExecID]
		if !ok {
			s.mu.Unlock()
			return &taskAPI.DeleteResponse{ExitedAt: timestamppb.Now()}, nil
		}
		pid := t.pid
		closeExecFIFOs(ep)
		delete(t.execs, r.ExecID)
		s.mu.Unlock()
		emitShimOperationLatency("delete", startedAt, shimMetricResultSuccess, metricTag("kind", "exec"))
		return &taskAPI.DeleteResponse{
			Pid:        uint32(pid),
			ExitedAt:   timestamppb.New(ep.exitedAt),
			ExitStatus: ep.exitCode,
		}, nil
	}
	needsTeardown := t.sandboxID != "" && !t.sandboxDetached
	sandboxID := t.sandboxID
	traceparent := t.traceparent
	s.mu.Unlock()

	// Only a sandbox task is traced: starting a span initializes the exporter,
	// which a container-share shim does not need.
	if sandboxID != "" {
		span, spanCtx := startLinkedSpan(ctx, "shim.delete", traceparent,
			attribute.String("task_id", r.ID),
			attribute.String("sandbox_id", sandboxID),
			attribute.Bool("needs_teardown", needsTeardown),
		)
		ctx = spanCtx
		defer func() { span.End(retErr) }()
	}

	var response *taskAPI.DeleteResponse
	found, err := s.mutateTaskAfterTeardownRelease(ctx, "delete", r.ID, t, needsTeardown, func(current *task) {
		pid := current.pid
		removeDataPlaneRedirect(current.netnsPath, current.sandboxID)
		closeFIFOs(current)
		delete(s.tasks, r.ID)
		emitShimOperationLatency("delete", startedAt, shimMetricResultSuccess, metricTag("kind", "task"))
		response = &taskAPI.DeleteResponse{
			Pid:        uint32(pid),
			ExitedAt:   timestamppb.Now(),
			ExitStatus: 0,
		}
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return &taskAPI.DeleteResponse{ExitedAt: timestamppb.Now()}, nil
	}
	return response, nil
}

func (s *spaceTaskService) State(ctx context.Context, r *taskAPI.StateRequest) (*taskAPI.StateResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tasks[r.ID]
	if !ok {
		// Critical: return ErrNotFound for unknown tasks.
		// Returning Status_STOPPED causes stale NOTREADY sandbox cascading kills.
		return nil, errdefs.ErrNotFound
	}

	if r.ExecID != "" {
		ep, ok := t.execs[r.ExecID]
		if !ok {
			return nil, errdefs.ErrNotFound
		}
		return &taskAPI.StateResponse{
			ID:         r.ID,
			ExecID:     r.ExecID,
			Pid:        uint32(t.pid),
			Status:     ep.status,
			ExitStatus: ep.exitCode,
			ExitedAt:   timestamppb.New(ep.exitedAt),
		}, nil
	}

	return &taskAPI.StateResponse{
		ID:         r.ID,
		Pid:        uint32(t.pid),
		Status:     t.status,
		Bundle:     t.bundle,
		ExitStatus: t.exitCode,
		ExitedAt:   timestamppb.New(t.exitedAt),
	}, nil
}

func (s *spaceTaskService) Kill(ctx context.Context, r *taskAPI.KillRequest) (_ *ptypes.Empty, retErr error) {
	s.mu.Lock()
	logShim("Kill: id=%s exec_id=%s signal=%d", r.ID, r.ExecID, r.Signal)

	t, ok := s.tasks[r.ID]
	if !ok {
		s.mu.Unlock()
		return &ptypes.Empty{}, nil
	}

	// Exec process cancellation is not currently implemented; the stream
	// completes naturally and Wait/Delete handle cleanup.
	if r.ExecID != "" {
		s.mu.Unlock()
		return &ptypes.Empty{}, nil
	}

	if t.status == tasktypes.Status_STOPPED {
		s.mu.Unlock()
		return &ptypes.Empty{}, nil
	}
	needsTeardown := t.sandboxID != "" && !t.sandboxDetached
	sandboxID := t.sandboxID
	traceparent := t.traceparent
	s.mu.Unlock()

	// Teardown starts here, so a refused or failed one shows as this span's
	// error. Only a sandbox task is traced, as in Delete.
	if sandboxID != "" {
		span, spanCtx := startLinkedSpan(ctx, "shim.kill", traceparent,
			attribute.String("task_id", r.ID),
			attribute.String("sandbox_id", sandboxID),
			attribute.Int64("signal", int64(r.Signal)),
			attribute.Bool("needs_teardown", needsTeardown),
		)
		ctx = spanCtx
		defer func() { span.End(retErr) }()
	}

	// A pod task may stop only after its exact execution has a remotely durable
	// suspend or has been superseded. Any inconclusive outcome leaves the task
	// running so containerd can retry without destroying guest state.
	_, err := s.mutateTaskAfterTeardownRelease(ctx, "kill", r.ID, t, needsTeardown, func(current *task) {
		if current.status == tasktypes.Status_STOPPED {
			return
		}
		current.status = tasktypes.Status_STOPPED
		current.exitedAt = time.Now()
		removeDataPlaneRedirect(current.netnsPath, current.sandboxID)
		closeFIFOs(current)
	})
	if err != nil {
		return nil, err
	}

	return &ptypes.Empty{}, nil
}

func (s *spaceTaskService) Wait(ctx context.Context, r *taskAPI.WaitRequest) (*taskAPI.WaitResponse, error) {
	// Handle exec process wait.
	if r.ExecID != "" {
		s.mu.Lock()
		t := s.tasks[r.ID]
		if t == nil {
			s.mu.Unlock()
			return nil, errdefs.ErrNotFound
		}
		ep := t.execs[r.ExecID]
		if ep == nil {
			s.mu.Unlock()
			return nil, errdefs.ErrNotFound
		}
		done := ep.done
		s.mu.Unlock()

		select {
		case <-done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		s.mu.Lock()
		defer s.mu.Unlock()
		return &taskAPI.WaitResponse{
			ExitStatus: ep.exitCode,
			ExitedAt:   timestamppb.New(ep.exitedAt),
		}, nil
	}

	for {
		s.mu.Lock()
		t := s.tasks[r.ID]
		if t == nil || t.status == tasktypes.Status_STOPPED {
			s.mu.Unlock()
			ea := time.Time{}
			ec := uint32(0)
			if t != nil {
				ea = t.exitedAt
				ec = t.exitCode
			}
			return &taskAPI.WaitResponse{ExitStatus: ec, ExitedAt: timestamppb.New(ea)}, nil
		}
		pid := t.pid
		s.mu.Unlock()

		// FC is not a child of this shim (jailer double-forks), so we poll.
		if pid > 0 && !processAlive(pid) {
			s.mu.Lock()
			if t := s.tasks[r.ID]; t != nil && t.status != tasktypes.Status_STOPPED {
				t.status = tasktypes.Status_STOPPED
				t.exitedAt = time.Now()
				removeDataPlaneRedirect(t.netnsPath, t.sandboxID)
				closeFIFOs(t)
			}
			s.mu.Unlock()
			continue
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}
}

func (s *spaceTaskService) Connect(ctx context.Context, r *taskAPI.ConnectRequest) (*taskAPI.ConnectResponse, error) {
	return &taskAPI.ConnectResponse{ShimPid: uint32(os.Getpid())}, nil
}

func (s *spaceTaskService) Shutdown(ctx context.Context, r *taskAPI.ShutdownRequest) (*ptypes.Empty, error) {
	s.mu.Lock()
	remaining := len(s.tasks)
	s.mu.Unlock()
	if remaining == 0 {
		stopTelemetry()
		s.sd.Shutdown()
	}
	return &ptypes.Empty{}, nil
}

// --- Lifecycle hooks ---

func (s *spaceTaskService) Pause(ctx context.Context, r *taskAPI.PauseRequest) (*ptypes.Empty, error) {
	s.mu.Lock()
	t := s.tasks[r.ID]
	s.mu.Unlock()
	if t == nil || t.sandboxID == "" {
		return &ptypes.Empty{}, nil
	}
	logShim("Pause: %s (sandbox %s)", r.ID, t.sandboxID)
	startedAt := time.Now()
	conn, err := dialSbxManager()
	if err != nil {
		emitShimStepError("pause", "dial_sbxmgr", "dial_sbxmgr", err)
		return nil, err
	}
	defer conn.Close()
	_, err = pb.NewSandboxManagerClient(conn).Pause(ctx, &pb.PauseRequest{SandboxId: t.sandboxID})
	if err == nil {
		emitShimOperationLatency("pause", startedAt, shimMetricResultSuccess)
	} else {
		emitShimStepError("pause", "sbxmgr_pause", "sbxmgr_pause", err)
	}
	return &ptypes.Empty{}, err
}

func (s *spaceTaskService) Resume(ctx context.Context, r *taskAPI.ResumeRequest) (*ptypes.Empty, error) {
	s.mu.Lock()
	t := s.tasks[r.ID]
	s.mu.Unlock()
	if t == nil || t.sandboxID == "" {
		return &ptypes.Empty{}, nil
	}
	logShim("Resume: %s (sandbox %s)", r.ID, t.sandboxID)
	startedAt := time.Now()
	conn, err := dialSbxManager()
	if err != nil {
		emitShimStepError("resume", "dial_sbxmgr", "dial_sbxmgr", err)
		return nil, err
	}
	defer conn.Close()
	_, err = pb.NewSandboxManagerClient(conn).Resume(ctx, &pb.ResumeRequest{SandboxId: t.sandboxID})
	if err == nil {
		emitShimOperationLatency("resume", startedAt, shimMetricResultSuccess)
	} else {
		emitShimStepError("resume", "sbxmgr_resume", "sbxmgr_resume", err)
	}
	return &ptypes.Empty{}, err
}

func (s *spaceTaskService) Checkpoint(ctx context.Context, r *taskAPI.CheckpointTaskRequest) (*ptypes.Empty, error) {
	s.mu.Lock()
	t := s.tasks[r.ID]
	s.mu.Unlock()
	if t == nil || t.sandboxID == "" {
		return &ptypes.Empty{}, nil
	}
	logShim("Checkpoint (Suspend): %s (sandbox %s)", r.ID, t.sandboxID)
	startedAt := time.Now()
	conn, err := dialSbxManager()
	if err != nil {
		emitShimStepError("checkpoint", "dial_sbxmgr", "dial_sbxmgr", err)
		return nil, err
	}
	defer conn.Close()
	_, err = pb.NewSandboxManagerClient(conn).Suspend(ctx, &pb.SuspendRequest{SandboxId: t.sandboxID})
	if err == nil {
		emitShimOperationLatency("checkpoint", startedAt, shimMetricResultSuccess)
	} else {
		emitShimStepError("checkpoint", "sbxmgr_suspend", "sbxmgr_suspend", err)
	}
	return &ptypes.Empty{}, err
}

// --- Exec ---

func (s *spaceTaskService) Exec(ctx context.Context, r *taskAPI.ExecProcessRequest) (*ptypes.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	logShim("Exec: id=%s exec_id=%s terminal=%v", r.ID, r.ExecID, r.Terminal)

	t, ok := s.tasks[r.ID]
	if !ok {
		return nil, errdefs.ErrNotFound
	}

	if t.execs != nil {
		if _, exists := t.execs[r.ExecID]; exists {
			return nil, errdefs.ErrAlreadyExists
		}
	}

	// Parse the OCI process spec to extract the command args.
	var proc ociProcess
	if r.Spec != nil && r.Spec.Value != nil {
		if err := json.Unmarshal(r.Spec.Value, &proc); err != nil {
			return nil, fmt.Errorf("unmarshal exec process spec: %w", err)
		}
	}
	if len(proc.Args) == 0 {
		return nil, fmt.Errorf("exec process spec has no args")
	}
	logShim("Exec: id=%s exec_id=%s arg_count=%d", r.ID, r.ExecID, len(proc.Args))

	ep := &execProcess{
		id:     r.ExecID,
		args:   proc.Args,
		status: tasktypes.Status_CREATED,
		done:   make(chan struct{}),
	}

	if r.Stdout != "" {
		ep.stdout, _ = openFIFO(r.Stdout)
	}
	if r.Stderr != "" {
		ep.stderr, _ = openFIFO(r.Stderr)
	}
	if t.execs == nil {
		t.execs = make(map[string]*execProcess)
	}
	t.execs[r.ExecID] = ep

	return &ptypes.Empty{}, nil
}

func (s *spaceTaskService) ResizePty(ctx context.Context, r *taskAPI.ResizePtyRequest) (*ptypes.Empty, error) {
	return &ptypes.Empty{}, nil
}

func (s *spaceTaskService) CloseIO(ctx context.Context, r *taskAPI.CloseIORequest) (*ptypes.Empty, error) {
	return &ptypes.Empty{}, nil
}

func (s *spaceTaskService) Update(ctx context.Context, r *taskAPI.UpdateTaskRequest) (*ptypes.Empty, error) {
	return &ptypes.Empty{}, nil
}

func (s *spaceTaskService) Stats(ctx context.Context, r *taskAPI.StatsRequest) (*taskAPI.StatsResponse, error) {
	s.mu.Lock()
	var sandboxID string
	if t, ok := s.tasks[r.ID]; ok {
		sandboxID = t.sandboxID
	}
	if sandboxID == "" {
		// Container-type tasks share the pod sandbox's microVM; report the
		// sandbox's stats for them too.
		sandboxID = s.findSandboxIDLocked()
	}
	s.mu.Unlock()

	// Always return a typed cgroup v2 Metrics so containerd's CRI can
	// typeurl.UnmarshalAny it without erroring. Missing data leaves the
	// corresponding fields zero — we'd rather report zeros than fail
	// kubelet's /stats/summary for the entire node.
	metrics := &cgroupsv2.Metrics{
		CPU:    &cgroupsv2.CPUStat{},
		Memory: &cgroupsv2.MemoryStat{},
		Pids:   &cgroupsv2.PidsStat{},
	}
	if sandboxID != "" {
		if resp, err := fetchSandboxStats(ctx, sandboxID); err == nil {
			metrics.CPU.UsageUsec = resp.CpuUsageUsec
			metrics.Memory.Usage = resp.MemoryCurrent
			metrics.Memory.UsageLimit = resp.MemoryMax
			metrics.Pids.Current = resp.PidsCurrent
			metrics.Pids.Limit = resp.PidsMax
		} else {
			logShim("Stats: sbx-manager Stats(%s) failed: %v", sandboxID, err)
		}
	}

	any, err := typeurl.MarshalAny(metrics)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal stats: %v", err)
	}
	return &taskAPI.StatsResponse{
		Stats: &anypb.Any{
			TypeUrl: any.GetTypeUrl(),
			Value:   any.GetValue(),
		},
	}, nil
}

func fetchSandboxStats(ctx context.Context, sandboxID string) (*pb.StatsResponse, error) {
	conn, err := dialSbxManager()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return pb.NewSandboxManagerClient(conn).Stats(ctx, &pb.StatsRequest{SandboxId: sandboxID})
}

func (s *spaceTaskService) Pids(ctx context.Context, r *taskAPI.PidsRequest) (*taskAPI.PidsResponse, error) {
	return &taskAPI.PidsResponse{}, nil
}

// --- Helpers ---

type connectRetryPolicy struct {
	budget        time.Duration
	initialDelay  time.Duration
	maxDelay      time.Duration
	jitterPercent int
}

var defaultConnectRetryPolicy = connectRetryPolicy{
	budget:        connectRetryBudget,
	initialDelay:  connectRetryInitialDelay,
	maxDelay:      connectRetryMaxDelay,
	jitterPercent: connectRetryJitterPercent,
}

func dialWithConnectRetry(
	ctx context.Context,
	service string,
	endpoint string,
	policy connectRetryPolicy,
	dial func(context.Context) (net.Conn, error),
	sleep func(context.Context, time.Duration) error,
) (net.Conn, error) {
	startedAt := time.Now()
	attempts := 0

	for {
		attempts++
		conn, err := dial(ctx)
		if err == nil {
			return conn, nil
		}
		if !retryableConnectError(err) {
			return nil, err
		}

		delay := connectRetryDelay(policy, attempts-1)
		elapsed := time.Since(startedAt)
		if elapsed >= policy.budget || delay > policy.budget-elapsed {
			logShim(
				"connect retry exhausted service=%s endpoint=%s elapsed=%s attempts=%d last_error=%v",
				service,
				endpoint,
				elapsed,
				attempts,
				err,
			)
			return nil, fmt.Errorf("connect %s at %s after %d attempts in %s: %w", service, endpoint, attempts, elapsed, err)
		}

		if sleepErr := sleep(ctx, delay); sleepErr != nil {
			return nil, sleepErr
		}
	}
}

func retryableConnectError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return errors.Is(err, os.ErrNotExist) ||
		errors.Is(err, syscall.ENOENT) ||
		errors.Is(err, syscall.ECONNREFUSED)
}

func connectRetryDelay(policy connectRetryPolicy, retry int) time.Duration {
	delay := policy.initialDelay
	for i := 0; i < retry && delay < policy.maxDelay; i++ {
		delay *= 2
		if delay > policy.maxDelay {
			delay = policy.maxDelay
		}
	}
	if delay <= 0 || policy.jitterPercent <= 0 {
		return delay
	}

	jitterRange := policy.jitterPercent*2 + 1
	offset := int((time.Now().UnixNano() + int64(retry*7919)) % int64(jitterRange))
	if offset < 0 {
		offset = -offset
	}
	percent := 100 - policy.jitterPercent + offset
	return delay * time.Duration(percent) / 100
}

func sleepForConnectRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func dialSbxManager() (*grpc.ClientConn, error) {
	return grpc.NewClient(
		"unix://"+sbxMgrSocket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return dialWithConnectRetry(
				ctx,
				"sandbox-manager",
				sbxMgrSocket,
				defaultConnectRetryPolicy,
				func(ctx context.Context) (net.Conn, error) {
					dialCtx, cancel := context.WithTimeout(ctx, sbxMgrConnectTimeout)
					defer cancel()
					var dialer net.Dialer
					return dialer.DialContext(dialCtx, "unix", sbxMgrSocket)
				},
				sleepForConnectRetry,
			)
		}),
		grpc.WithUnaryInterceptor(traceparentInjectingUnaryInterceptor),
	)
}

func processAlive(pid int) bool {
	_, err := os.Stat(fmt.Sprintf("/proc/%d/stat", pid))
	return err == nil
}

func openFIFO(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func closeFIFOs(t *task) {
	if t.stdout != nil {
		t.stdout.Close()
		t.stdout = nil
	}
	if t.stderr != nil {
		t.stderr.Close()
		t.stderr = nil
	}
}

func closeExecFIFOs(ep *execProcess) {
	if ep.stdout != nil {
		ep.stdout.Close()
		ep.stdout = nil
	}
	if ep.stderr != nil {
		ep.stderr.Close()
		ep.stderr = nil
	}
}

// findSandboxIDLocked returns the sandbox ID from the pod sandbox task.
// Must be called with s.mu held.
func (s *spaceTaskService) findSandboxIDLocked() string {
	for _, t := range s.tasks {
		if t.sandboxID != "" {
			return t.sandboxID
		}
	}
	return ""
}

func spacedProcessExitCode(exit *operationspb.ProcessExit) uint32 {
	if exit == nil {
		return 255
	}
	switch outcome := exit.Outcome.(type) {
	case *operationspb.ProcessExit_Exited:
		if outcome.Exited.ExitCode < 0 {
			return 255
		}
		return uint32(outcome.Exited.ExitCode)
	case *operationspb.ProcessExit_Signaled:
		return spacedSignalExitCode(outcome.Signaled.Signal, 255)
	case *operationspb.ProcessExit_TimedOut:
		return spacedSignalExitCode(outcome.TimedOut.Signal, 124)
	default:
		return 255
	}
}

func spacedSignalExitCode(signal *int32, fallback uint32) uint32 {
	if signal == nil || *signal <= 0 {
		return fallback
	}
	return uint32(128 + *signal)
}

// runExec forwards a containerd exec through the spaced process stream.
func (s *spaceTaskService) runExec(sandboxID string, ep *execProcess) {
	defer close(ep.done)
	logShim("runExec %s: starting arg_count=%d sandbox=%s", ep.id, len(ep.args), sandboxID)
	execStartedAt := time.Now()

	conn, err := dialSbxManager()
	if err != nil {
		logShim("runExec %s: connect to sbx-manager failed: %v", ep.id, err)
		emitShimStepError("exec", "dial_sbxmgr", "dial_sbxmgr", err)
		s.mu.Lock()
		ep.exitCode = 255
		ep.status = tasktypes.Status_STOPPED
		ep.exitedAt = time.Now()
		closeExecFIFOs(ep)
		s.mu.Unlock()
		return
	}
	defer conn.Close()

	stream, err := pb.NewSandboxManagerClient(conn).SpacedStartProcess(context.Background(), &pb.SpacedStartProcessRequest{
		SandboxId: sandboxID,
		Operation: &operationspb.StartRequest{Command: ep.args},
	})
	if err != nil {
		logShim("runExec %s: sbx-manager SpacedStartProcess failed: %v", ep.id, err)
		emitShimStepError("exec", "sbxmgr_spaced_start_process", "sbxmgr_spaced_start_process", err)
		s.mu.Lock()
		ep.exitCode = 255
		if ep.stderr != nil {
			fmt.Fprintf(ep.stderr, "exec failed: %v\n", err)
		}
		ep.status = tasktypes.Status_STOPPED
		ep.exitedAt = time.Now()
		closeExecFIFOs(ep)
		s.mu.Unlock()
		return
	}

	exitCode := uint32(255)
	var streamErr error
	for {
		event, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			streamErr = errors.New("spaced process stream ended before an exit event")
			break
		}
		if recvErr != nil {
			streamErr = recvErr
			break
		}
		if output := event.GetStdout(); output != nil {
			s.mu.Lock()
			if ep.stdout != nil {
				_, _ = ep.stdout.Write(output.Data)
			}
			s.mu.Unlock()
		}
		if output := event.GetStderr(); output != nil {
			s.mu.Lock()
			if ep.stderr != nil {
				_, _ = ep.stderr.Write(output.Data)
			}
			s.mu.Unlock()
		}
		if exit := event.GetExit(); exit != nil {
			exitCode = spacedProcessExitCode(exit)
			break
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if streamErr != nil {
		logShim("runExec %s: sbx-manager process stream failed: %v", ep.id, streamErr)
		emitShimStepError("exec", "sbxmgr_spaced_process_stream", "sbxmgr_spaced_process_stream", streamErr)
		ep.exitCode = 255
		if ep.stderr != nil {
			fmt.Fprintf(ep.stderr, "exec failed: %v\n", streamErr)
		}
	} else {
		logShim("runExec %s: exit_code=%d", ep.id, exitCode)
		emitShimOperationLatency("exec", execStartedAt, shimMetricResultSuccess)
		ep.exitCode = exitCode
	}

	ep.status = tasktypes.Status_STOPPED
	ep.exitedAt = time.Now()
	closeExecFIFOs(ep)
}

func logShim(f string, a ...any) {
	fmt.Fprintf(os.Stderr, "[space-shim] "+f+"\n", a...)
}

type ociProcess struct {
	Args []string `json:"args"`
	Env  []string `json:"env,omitempty"`
}

type ociSpec struct {
	Annotations map[string]string `json:"annotations,omitempty"`
	Process     *ociProcess       `json:"process,omitempty"`
	Linux       *ociLinux         `json:"linux,omitempty"`
}

type ociLinux struct {
	CgroupsPath string             `json:"cgroupsPath,omitempty"`
	Namespaces  []ociNamespace     `json:"namespaces,omitempty"`
	Resources   *ociLinuxResources `json:"resources,omitempty"`
}

type ociLinuxResources struct {
	CPU    *ociLinuxCPU    `json:"cpu,omitempty"`
	Memory *ociLinuxMemory `json:"memory,omitempty"`
}

type ociLinuxCPU struct {
	Quota  *int64  `json:"quota,omitempty"`
	Period *uint64 `json:"period,omitempty"`
}

type ociLinuxMemory struct {
	Limit *int64 `json:"limit,omitempty"`
}

type ociNamespace struct {
	Type string `json:"type"`
	Path string `json:"path,omitempty"`
}

type autoLifecyclePolicyJSON struct {
	Wake          autoLifecycleWakePolicyJSON           `json:"wake"`
	ConditionSets map[string]autoLifecycleConditionJSON `json:"condition_sets"`
	Stages        []autoLifecycleStageJSON              `json:"stages"`
}

type autoLifecycleWakePolicyJSON struct {
	On []string `json:"on"`
}

type autoLifecycleConditionJSON struct {
	Kind                  string `json:"kind"`
	ActivitySource        string `json:"activity_source"`
	MaxGuestIdleExtension string `json:"max_guest_idle_extension"`
}

type autoLifecycleStageJSON struct {
	ConditionSet string `json:"condition_set"`
	At           string `json:"at"`
	Action       string `json:"action"`
}

type networkPolicyJSON struct {
	Version         *string           `json:"version"`
	Egress          *egressPolicyJSON `json:"egress"`
	TLSInterception json.RawMessage   `json:"tls_interception"`
}

type sandboxNetworkPolicyConfig struct {
	Policy     *netpb.NetworkPolicyV1
	Generation uint64
	Authority  netpb.PolicyAuthority
}

type egressPolicyJSON struct {
	DefaultAction string                  `json:"default_action"`
	Allow         []egressDestinationJSON `json:"allow"`
	Deny          []egressDestinationJSON `json:"deny"`
}

type egressDestinationJSON struct {
	IP             string `json:"ip"`
	CIDR           string `json:"cidr"`
	Domain         string `json:"domain"`
	WildcardDomain string `json:"wildcard_domain"`
}

// networkNamespace returns the network namespace path from the OCI spec, if set.
func (s *ociSpec) networkNamespace() string {
	if s.Linux == nil {
		return ""
	}
	for _, ns := range s.Linux.Namespaces {
		if ns.Type == "network" && ns.Path != "" {
			return ns.Path
		}
	}
	return ""
}

func (s *ociSpec) sandboxTemplate() string {
	if value, ok := s.Annotations["space.pplx.net/template"]; ok && value != "" {
		return value
	}
	return defaultTemplate
}

func (s *ociSpec) sandboxContentHash() *string {
	if value, ok := s.Annotations["space.pplx.net/content-hash"]; ok && value != "" {
		return ptr(value)
	}
	return nil
}

func (s *ociSpec) sandboxCheckpoint() *string {
	value := strings.TrimSpace(s.Annotations[sandboxCheckpointAnnotation])
	if value == "" {
		return nil
	}
	return ptr(value)
}

func (s *ociSpec) sandboxSuspendRestoreStorageURI() *string {
	value := strings.TrimSpace(s.Annotations[sandboxSuspendRestoreStorageURIAnnotation])
	if value == "" {
		return nil
	}
	return ptr(value)
}

func (s *ociSpec) sandboxSuspendRestoreSourceNode() *string {
	value := strings.TrimSpace(s.Annotations[sandboxSuspendRestoreSourceNodeAnnotation])
	if value == "" {
		return nil
	}
	return ptr(value)
}

func (s *ociSpec) sandboxArtifactRestoreAsNew() (*pb.RestoreArtifactAsNewContext, error) {
	value := strings.TrimSpace(s.Annotations[sandboxArtifactRestoreAsNewAnnotation])
	if value == "" {
		return nil, nil
	}
	var context struct {
		SourceSandboxID    string `json:"sourceSandboxId"`
		ArtifactSnapshotID string `json:"artifactSnapshotId"`
		TargetOwner        string `json:"targetOwner"`
	}
	if err := json.Unmarshal([]byte(value), &context); err != nil {
		return nil, err
	}
	if _, err := uuid.Parse(context.SourceSandboxID); err != nil {
		return nil, fmt.Errorf("sourceSandboxId must be a UUID: %w", err)
	}
	if _, err := uuid.Parse(context.ArtifactSnapshotID); err != nil {
		return nil, fmt.Errorf("artifactSnapshotId must be a UUID: %w", err)
	}
	if strings.TrimSpace(context.TargetOwner) == "" {
		return nil, fmt.Errorf("targetOwner must not be empty")
	}
	return &pb.RestoreArtifactAsNewContext{
		SourceSandboxId:    context.SourceSandboxID,
		ArtifactSnapshotId: context.ArtifactSnapshotID,
		TargetOwner:        context.TargetOwner,
		ArtifactKind:       pb.ArtifactKind_ARTIFACT_KIND_DISK,
	}, nil
}

func (s *ociSpec) sandboxStartIntent() (*string, *pb.RestoreArtifactAsNewContext, bool, error) {
	checkpoint := s.sandboxCheckpoint()
	artifactRestoreAsNew, err := s.sandboxArtifactRestoreAsNew()
	if err != nil {
		return nil, nil, false, err
	}
	if checkpoint != nil && artifactRestoreAsNew != nil {
		return nil, nil, false, fmt.Errorf("%s cannot be combined with %s", sandboxCheckpointAnnotation, sandboxArtifactRestoreAsNewAnnotation)
	}
	return checkpoint, artifactRestoreAsNew, checkpoint != nil || artifactRestoreAsNew != nil, nil
}

func sandboxRestoreModeForLog(checkpoint *string, artifactRestoreAsNew *pb.RestoreArtifactAsNewContext) string {
	if checkpoint != nil {
		return *checkpoint
	}
	if artifactRestoreAsNew != nil {
		return "artifact_" + strings.ToLower(strings.TrimPrefix(artifactRestoreAsNew.ArtifactKind.String(), "ARTIFACT_KIND_"))
	}
	return "unknown"
}

func (s *ociSpec) sandboxExecutionGeneration() (*int64, error) {
	raw := strings.TrimSpace(s.Annotations[sandboxExecutionGenerationAnnotation])
	if raw == "" {
		return nil, nil
	}
	generation, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%s must be an int64: %w", sandboxExecutionGenerationAnnotation, err)
	}
	if generation <= 0 {
		return nil, fmt.Errorf("%s must be positive", sandboxExecutionGenerationAnnotation)
	}
	return ptr(generation), nil
}

func (s *ociSpec) sandboxRouteGeneration(executionGeneration *int64) (*uint64, error) {
	raw := strings.TrimSpace(s.Annotations[sandboxRouteGenerationAnnotation])
	if raw == "" {
		if executionGeneration == nil {
			generation := uint64(1)
			return &generation, nil
		}
		generation := uint64(*executionGeneration)
		return &generation, nil
	}
	generation, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%s must be a uint64: %w", sandboxRouteGenerationAnnotation, err)
	}
	if generation == 0 {
		return nil, fmt.Errorf("%s must be positive", sandboxRouteGenerationAnnotation)
	}
	if generation > maxRouteGeneration {
		return nil, fmt.Errorf("%s must fit in int64", sandboxRouteGenerationAnnotation)
	}
	return ptr(generation), nil
}

func currentNodeID() *string {
	value := strings.TrimSpace(os.Getenv("SPACE_NODE_ID"))
	if value == "" {
		return nil
	}
	if parsed, err := uuid.Parse(value); err == nil {
		return ptr(parsed.String())
	}
	nodeID := uuid.NewSHA1(spaceK8sNodeIDNamespace, []byte(value+"\x00worker")).String()
	return ptr(nodeID)
}

func routeIdentity(
	routeGeneration *uint64,
	currentNodeID *string,
	executionGeneration *int64,
	podUID *string,
	activationAttemptID *string,
) *gatewaypb.SpaceApiRouteIdentity {
	if routeGeneration == nil && currentNodeID == nil && executionGeneration == nil && podUID == nil && activationAttemptID == nil {
		return nil
	}
	return &gatewaypb.SpaceApiRouteIdentity{
		RouteGeneration:     routeGeneration,
		CurrentNodeId:       currentNodeID,
		ExecutionGeneration: executionGeneration,
		PodUid:              podUID,
		ActivationAttemptId: activationAttemptID,
		SubstrateKind:       gatewaypb.SandboxSubstrateKind_SANDBOX_SUBSTRATE_KIND_KUBERNETES,
	}
}

func (s *ociSpec) sandboxActivationAttemptID() (*string, error) {
	value := strings.TrimSpace(s.Annotations[sandboxActivationAttemptIDAnnotation])
	if value == "" {
		return nil, nil
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return nil, fmt.Errorf("%s must be a UUID: %w", sandboxActivationAttemptIDAnnotation, err)
	}
	return ptr(parsed.String()), nil
}

func (s *ociSpec) sandboxPodUID() *string {
	for _, annotation := range []string{
		sandboxPodUIDAnnotation,
		kubernetesPodUIDAnnotation,
		kubernetesCriSandboxUIDAnnotation,
	} {
		if value := strings.TrimSpace(s.Annotations[annotation]); value != "" {
			return ptr(value)
		}
	}
	return nil
}

func (s *ociSpec) sandboxOwner() (string, error) {
	value := strings.TrimSpace(s.Annotations[sandboxOwnerAnnotation])
	if value == "" {
		value = strings.TrimSpace(s.Annotations[sandboxUserAnnotation])
	}
	if value == "" {
		return "", fmt.Errorf("%s or %s is required", sandboxOwnerAnnotation, sandboxUserAnnotation)
	}
	return value, nil
}

func (s *ociSpec) sandboxRootfsSizeMb() (*uint32, error) {
	value, ok := s.Annotations[sandboxDiskMibAnnotation]
	value = strings.TrimSpace(value)
	if !ok || value == "" {
		return nil, nil
	}

	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("%s must be a positive uint32: %w", sandboxDiskMibAnnotation, err)
	}
	if parsed == 0 {
		return nil, fmt.Errorf("%s must be positive", sandboxDiskMibAnnotation)
	}

	return ptr(uint32(parsed)), nil
}

func sandboxCreateBootMode(rootfsSizeMb *uint32) pb.BootMode {
	if rootfsSizeMb != nil {
		return pb.BootMode_BOOT_MODE_COLD
	}
	return pb.BootMode_BOOT_MODE_UNSPECIFIED
}

// sandboxVolumeEncryption reads the BYOK immutable encrypted-volumes flag.
// Absent/empty means plaintext (every pod predating the annotation reads as
// plaintext); only exact "true"/"false" are accepted otherwise, so a
// malformed value fails the create instead of silently downgrading an
// encrypted sandbox to plaintext.
func (s *ociSpec) sandboxVolumeEncryption() (bool, error) {
	value := strings.TrimSpace(s.Annotations[sandboxVolumeEncryptionAnnotation])
	switch value {
	case "", "false":
		return false, nil
	case "true":
		return true, nil
	default:
		return false, fmt.Errorf("%s must be \"true\" or \"false\", got %q", sandboxVolumeEncryptionAnnotation, value)
	}
}

func (s *ociSpec) sandboxAutoLifecycle() (*pb.AutoLifecyclePolicy, error) {
	raw, ok := s.Annotations[sandboxAutoLifecycleAnnotation]
	if !ok || strings.TrimSpace(raw) == "" {
		return nil, nil
	}

	var decoded autoLifecyclePolicyJSON
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	policy := &pb.AutoLifecyclePolicy{
		Wake: &pb.AutoLifecycleWakePolicy{},
	}

	for _, source := range decoded.Wake.On {
		parsed, err := parseAutoLifecycleWakeSource(source)
		if err != nil {
			return nil, err
		}
		policy.Wake.On = append(policy.Wake.On, parsed)
	}

	for name, condition := range decoded.ConditionSets {
		parsed, err := parseAutoLifecycleCondition(name, condition)
		if err != nil {
			return nil, err
		}
		policy.ConditionSets = append(policy.ConditionSets, parsed)
	}

	for _, stage := range decoded.Stages {
		parsed, err := parseAutoLifecycleStage(stage)
		if err != nil {
			return nil, err
		}
		policy.Stages = append(policy.Stages, parsed)
	}

	return policy, nil
}

func (s *ociSpec) sandboxAutoLifecycleAuthority(hasPolicy bool) (netpb.PolicyAuthority, error) {
	return s.sandboxPolicyAuthority(sandboxAutoLifecycleAuthorityAnnotation, hasPolicy)
}

func (s *ociSpec) sandboxNetworkPolicy() (sandboxNetworkPolicyConfig, error) {
	raw, ok := s.Annotations[sandboxNetworkPolicyAnnotation]
	if !ok || strings.TrimSpace(raw) == "" {
		return sandboxNetworkPolicyConfig{}, nil
	}

	policy, err := parseNetworkPolicyJSON(raw)
	if err != nil {
		return sandboxNetworkPolicyConfig{}, err
	}
	generation, err := s.sandboxNetworkPolicyGeneration()
	if err != nil {
		return sandboxNetworkPolicyConfig{}, err
	}
	authority, err := s.sandboxPolicyAuthority(sandboxNetworkPolicyAuthorityAnnotation, true)
	if err != nil {
		return sandboxNetworkPolicyConfig{}, err
	}
	return sandboxNetworkPolicyConfig{
		Policy:     policy,
		Generation: generation,
		Authority:  authority,
	}, nil
}

func (s *ociSpec) sandboxNetworkPolicyGeneration() (uint64, error) {
	raw, ok := s.Annotations[sandboxNetworkPolicyGenerationAnnotation]
	raw = strings.TrimSpace(raw)
	if !ok || raw == "" {
		return 0, fmt.Errorf("%s is required when %s is set", sandboxNetworkPolicyGenerationAnnotation, sandboxNetworkPolicyAnnotation)
	}
	generation, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a uint64: %w", sandboxNetworkPolicyGenerationAnnotation, err)
	}
	return generation, nil
}

func (s *ociSpec) sandboxPolicyAuthority(annotation string, defaultToCluster bool) (netpb.PolicyAuthority, error) {
	raw := strings.TrimSpace(s.Annotations[annotation])
	if raw == "" {
		if defaultToCluster {
			return netpb.PolicyAuthority_POLICY_AUTHORITY_CLUSTER, nil
		}
		return netpb.PolicyAuthority_POLICY_AUTHORITY_UNSPECIFIED, nil
	}
	switch strings.ToLower(raw) {
	case "cluster":
		return netpb.PolicyAuthority_POLICY_AUTHORITY_CLUSTER, nil
	case "local":
		return netpb.PolicyAuthority_POLICY_AUTHORITY_LOCAL, nil
	default:
		return netpb.PolicyAuthority_POLICY_AUTHORITY_UNSPECIFIED, fmt.Errorf("%s must be cluster or local", annotation)
	}
}

func parseNetworkPolicyJSON(raw string) (*netpb.NetworkPolicyV1, error) {
	var decoded networkPolicyJSON
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	version := "v1"
	if decoded.Version != nil {
		version = *decoded.Version
	}
	if version != "v1" {
		return nil, fmt.Errorf("unsupported network policy version %q", version)
	}
	if decoded.Egress == nil {
		return nil, fmt.Errorf("network policy egress is required")
	}
	// Unspecified means Off and preserves the pre-field protobuf encoding.
	tlsInterception := netpb.TlsInterception_TLS_INTERCEPTION_UNSPECIFIED
	if len(decoded.TLSInterception) != 0 {
		var mode string
		if err := json.Unmarshal(decoded.TLSInterception, &mode); err != nil {
			return nil, fmt.Errorf("invalid network policy tls_interception: %w", err)
		}
		switch mode {
		case "off":
		case "on":
			tlsInterception = netpb.TlsInterception_TLS_INTERCEPTION_ON
		default:
			return nil, fmt.Errorf("unsupported network policy tls_interception %q", mode)
		}
	}
	action, err := parseEgressDefaultAction(decoded.Egress.DefaultAction)
	if err != nil {
		return nil, err
	}
	allow, err := parseEgressDestinations(decoded.Egress.Allow, "allow")
	if err != nil {
		return nil, err
	}
	deny, err := parseEgressDestinations(decoded.Egress.Deny, "deny")
	if err != nil {
		return nil, err
	}
	return &netpb.NetworkPolicyV1{
		Version:             netpb.NetworkPolicyVersion_NETWORK_POLICY_VERSION_V1,
		EgressDefaultAction: action,
		Allow:               allow,
		Deny:                deny,
		TlsInterception:     tlsInterception,
	}, nil
}

func parseEgressDefaultAction(raw string) (netpb.EgressDefaultAction, error) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "allow":
		return netpb.EgressDefaultAction_EGRESS_DEFAULT_ACTION_ALLOW, nil
	case "deny":
		return netpb.EgressDefaultAction_EGRESS_DEFAULT_ACTION_DENY, nil
	default:
		return netpb.EgressDefaultAction_EGRESS_DEFAULT_ACTION_UNSPECIFIED, fmt.Errorf("unsupported network policy egress default_action %q", raw)
	}
}

func parseEgressDestinations(raw []egressDestinationJSON, field string) ([]*netpb.EgressDestinationV1, error) {
	out := make([]*netpb.EgressDestinationV1, 0, len(raw))
	for index, destination := range raw {
		parsed, err := parseEgressDestination(destination)
		if err != nil {
			return nil, fmt.Errorf("network policy egress %s[%d]: %w", field, index, err)
		}
		out = append(out, parsed)
	}
	return out, nil
}

func parseEgressDestination(destination egressDestinationJSON) (*netpb.EgressDestinationV1, error) {
	values := []string{
		strings.TrimSpace(destination.IP),
		strings.TrimSpace(destination.CIDR),
		strings.TrimSpace(destination.Domain),
		strings.TrimSpace(destination.WildcardDomain),
	}
	count := 0
	for _, value := range values {
		if value != "" {
			count++
		}
	}
	if count != 1 {
		return nil, fmt.Errorf("exactly one of ip, cidr, domain, wildcard_domain is required")
	}
	switch {
	case values[0] != "":
		return &netpb.EgressDestinationV1{Destination: &netpb.EgressDestinationV1_Ip{Ip: values[0]}}, nil
	case values[1] != "":
		return &netpb.EgressDestinationV1{Destination: &netpb.EgressDestinationV1_Cidr{Cidr: values[1]}}, nil
	case values[2] != "":
		return &netpb.EgressDestinationV1{Destination: &netpb.EgressDestinationV1_Domain{Domain: values[2]}}, nil
	default:
		return &netpb.EgressDestinationV1{Destination: &netpb.EgressDestinationV1_WildcardDomain{WildcardDomain: values[3]}}, nil
	}
}

// traceparent returns the W3C context of the Rust operation that materialized
// the Pod. Containerd copies the annotation into the OCI spec so shim.create
// can continue that trace after asynchronous scheduling.
func (s *ociSpec) traceparent() string {
	return s.Annotations["space.pplx.net/traceparent"]
}

// createSandbox assembles the initial manager request before the caller can Wake.
func (s *ociSpec) createSandbox(ctx context.Context, client pb.SandboxManagerClient, request *pb.CreateRequest) (*pb.CreateResponse, error) {
	env, err := s.initialSandboxEnv()
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid cold-create environment metadata")
	}
	request.Env = env
	return client.Create(ctx, request)
}

// Only initial Create consumes this annotation. Restore retains the manager's
// environment, and warm adoption updates it through its existing explicit RPC.
func (s *ociSpec) initialSandboxEnv() (map[string]string, error) {
	// EnvMap's generated limits, plus one '=' and NUL per entry. Bound both
	// encoded input and decoded storage before the base64 decoder allocates.
	const maxWireBytes = envMaxTotalBytes + 2*envMaxEntries
	for key := range s.Annotations {
		if strings.HasPrefix(key, "space.pplx.net/process-env-") && key != "space.pplx.net/process-env-v1" {
			return nil, errors.New("unsupported environment metadata version")
		}
	}
	raw, present := s.Annotations["space.pplx.net/process-env-v1"]
	if !present {
		return s.sandboxEnv(), nil
	}
	if len(raw) > base64.StdEncoding.EncodedLen(maxWireBytes) || base64.StdEncoding.DecodedLen(len(raw)) > maxWireBytes {
		return nil, errors.New("environment metadata exceeds EnvMap bounds")
	}
	wire, err := base64.StdEncoding.Strict().DecodeString(raw)
	if err != nil || !utf8.Valid(wire) || base64.StdEncoding.EncodeToString(wire) != raw {
		return nil, errors.New("invalid environment encoding")
	}
	env := map[string]string{}
	totalBytes := 0
	remaining := string(wire)
	for remaining != "" {
		entry, rest, terminated := strings.Cut(remaining, "\x00")
		if !terminated {
			return nil, errors.New("unterminated environment entry")
		}
		remaining = rest
		key, value, found := strings.Cut(entry, "=")
		if !found || key == "" {
			return nil, errors.New("invalid environment entry")
		}
		totalBytes += len(key) + len(value)
		if len(env) >= envMaxEntries || len(key) > envMaxKeyBytes || len(value) > envMaxValueBytes || totalBytes > envMaxTotalBytes {
			return nil, errors.New("environment exceeds EnvMap bounds")
		}
		if !validEnvKeyGrammar(key) {
			return nil, errors.New("invalid environment key")
		}
		if _, duplicate := env[key]; duplicate {
			return nil, errors.New("duplicate environment key")
		}
		env[key] = value
	}
	delete(env, "SPACE_INTERNAL_TOKEN")
	delete(env, "SPACE_INTERNAL_CONTROL_TOKEN")
	return env, nil
}

func (s *ociSpec) sandboxEnv() map[string]string {
	if s.Process == nil || len(s.Process.Env) == 0 {
		return nil
	}
	env := map[string]string{}
	for _, entry := range s.Process.Env {
		key, value, ok := strings.Cut(entry, "=")
		if ok && key != "" {
			env[key] = value
		}
	}
	if len(env) == 0 {
		return nil
	}
	return env
}

func (s *ociSpec) requestedSandboxID() *uuid.UUID {
	if rawID := strings.TrimSpace(s.Annotations[sandboxIDAnnotation]); rawID != "" {
		parsed, err := uuid.Parse(rawID)
		if err == nil {
			return &parsed
		}
	}

	name := s.Annotations["io.kubernetes.cri.sandbox-name"]
	const prefix = "space-sbx-"
	if !strings.HasPrefix(name, prefix) {
		return nil
	}

	rawHex := strings.TrimPrefix(name, prefix)
	if sandboxHex, _, ok := strings.Cut(rawHex, "-g"); ok {
		rawHex = sandboxHex
	}
	if len(rawHex) != 32 {
		return nil
	}

	raw, err := hex.DecodeString(rawHex)
	if err != nil {
		return nil
	}

	parsed, err := uuid.FromBytes(raw)
	if err != nil {
		return nil
	}

	return &parsed
}

func (s *ociSpec) sandboxVCPU() (uint32, error) {
	if value, err := s.positiveUint32Annotation(sandboxVCPUAnnotation); err != nil {
		return 0, err
	} else if value != nil {
		return *value, nil
	}
	if s.Linux != nil && s.Linux.Resources != nil && s.Linux.Resources.CPU != nil &&
		s.Linux.Resources.CPU.Quota != nil && s.Linux.Resources.CPU.Period != nil &&
		*s.Linux.Resources.CPU.Quota > 0 && *s.Linux.Resources.CPU.Period > 0 {
		quota := uint64(*s.Linux.Resources.CPU.Quota)
		period := *s.Linux.Resources.CPU.Period
		rounded := (quota + period - 1) / period
		if rounded > 0 {
			return uint32(rounded), nil
		}
	}
	return defaultVCPU, nil
}

func (s *ociSpec) sandboxMemoryMiB() (uint32, error) {
	if value, err := s.positiveUint32Annotation(sandboxMemoryMiBAnnotation); err != nil {
		return 0, err
	} else if value != nil {
		return *value, nil
	}
	if s.Linux != nil && s.Linux.Resources != nil && s.Linux.Resources.Memory != nil &&
		s.Linux.Resources.Memory.Limit != nil && *s.Linux.Resources.Memory.Limit > 0 {
		limitBytes := uint64(*s.Linux.Resources.Memory.Limit)
		return uint32((limitBytes + 1024*1024 - 1) / (1024 * 1024)), nil
	}
	return defaultMemoryMiB, nil
}

func (s *ociSpec) positiveUint32Annotation(annotation string) (*uint32, error) {
	raw := strings.TrimSpace(s.Annotations[annotation])
	if raw == "" {
		return nil, nil
	}
	value, err := strconv.ParseUint(raw, 10, 32)
	if err != nil || value == 0 {
		return nil, fmt.Errorf("%s must be a positive uint32", annotation)
	}
	parsed := uint32(value)
	return &parsed, nil
}

func (s *ociSpec) positiveUint64Annotation(annotation string) (*uint64, error) {
	raw := strings.TrimSpace(s.Annotations[annotation])
	if raw == "" {
		return nil, nil
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || value == 0 {
		return nil, fmt.Errorf("%s must be a positive uint64", annotation)
	}
	return ptr(value), nil
}

func parseAutoLifecycleWakeSource(raw string) (pb.AutoLifecycleWakeSource, error) {
	switch raw {
	case "access":
		return pb.AutoLifecycleWakeSource_AUTO_LIFECYCLE_WAKE_SOURCE_ACCESS, nil
	case "command":
		return pb.AutoLifecycleWakeSource_AUTO_LIFECYCLE_WAKE_SOURCE_COMMAND, nil
	default:
		return pb.AutoLifecycleWakeSource_AUTO_LIFECYCLE_WAKE_SOURCE_UNSPECIFIED, fmt.Errorf("unsupported auto lifecycle wake source %q", raw)
	}
}

func parseAutoLifecycleConditionSetID(raw string) (pb.AutoLifecycleConditionSetId, error) {
	switch strings.TrimSpace(raw) {
	case "access_idle":
		return pb.AutoLifecycleConditionSetId_AUTO_LIFECYCLE_CONDITION_SET_ID_ACCESS_IDLE, nil
	case "command_idle":
		return pb.AutoLifecycleConditionSetId_AUTO_LIFECYCLE_CONDITION_SET_ID_COMMAND_IDLE, nil
	case "guest_activity":
		return pb.AutoLifecycleConditionSetId_AUTO_LIFECYCLE_CONDITION_SET_ID_GUEST_ACTIVITY, nil
	case "":
		return pb.AutoLifecycleConditionSetId_AUTO_LIFECYCLE_CONDITION_SET_ID_UNSPECIFIED, fmt.Errorf("auto lifecycle condition set cannot be empty")
	default:
		return pb.AutoLifecycleConditionSetId_AUTO_LIFECYCLE_CONDITION_SET_ID_UNSPECIFIED, fmt.Errorf("unsupported auto lifecycle condition set %q", raw)
	}
}

func parseAutoLifecycleCondition(name string, condition autoLifecycleConditionJSON) (*pb.AutoLifecycleConditionSet, error) {
	id, err := parseAutoLifecycleConditionSetID(name)
	if err != nil {
		return nil, err
	}

	result := &pb.AutoLifecycleConditionSet{Id: id}
	switch condition.Kind {
	case "inactivity":
		result.Kind = pb.AutoLifecycleConditionKind_AUTO_LIFECYCLE_CONDITION_KIND_INACTIVITY
	case "guest_activity":
		result.Kind = pb.AutoLifecycleConditionKind_AUTO_LIFECYCLE_CONDITION_KIND_GUEST_ACTIVITY
	default:
		return nil, fmt.Errorf("unsupported auto lifecycle condition kind %q", condition.Kind)
	}

	switch condition.ActivitySource {
	case "":
		result.ActivitySource = nil
	case "access":
		result.ActivitySource = ptr(pb.AutoLifecycleActivitySource_AUTO_LIFECYCLE_ACTIVITY_SOURCE_ACCESS)
	case "command":
		result.ActivitySource = ptr(pb.AutoLifecycleActivitySource_AUTO_LIFECYCLE_ACTIVITY_SOURCE_COMMAND)
	default:
		return nil, fmt.Errorf("unsupported auto lifecycle activity source %q", condition.ActivitySource)
	}

	if condition.MaxGuestIdleExtension != "" {
		duration, err := parseSimpleDuration(condition.MaxGuestIdleExtension)
		if err != nil {
			return nil, fmt.Errorf("invalid auto lifecycle max_guest_idle_extension: %w", err)
		}
		result.MaxGuestIdleExtension = durationpb.New(duration)
	}

	return result, nil
}

func parseAutoLifecycleStage(stage autoLifecycleStageJSON) (*pb.AutoLifecycleStage, error) {
	conditionSet, err := parseAutoLifecycleConditionSetID(stage.ConditionSet)
	if err != nil {
		return nil, fmt.Errorf("invalid auto lifecycle stage condition_set: %w", err)
	}

	seconds, err := parseAutoLifecycleStageSeconds(stage.At)
	if err != nil {
		return nil, err
	}

	result := &pb.AutoLifecycleStage{
		ConditionSet: conditionSet,
		At:           durationpb.New(time.Duration(seconds) * time.Second),
	}
	switch stage.Action {
	case "none":
		result.Action = pb.AutoLifecycleIdleAction_AUTO_LIFECYCLE_IDLE_ACTION_NONE
	case "pause":
		result.Action = pb.AutoLifecycleIdleAction_AUTO_LIFECYCLE_IDLE_ACTION_PAUSE
	case "suspend":
		result.Action = pb.AutoLifecycleIdleAction_AUTO_LIFECYCLE_IDLE_ACTION_SUSPEND
	case "stop":
		result.Action = pb.AutoLifecycleIdleAction_AUTO_LIFECYCLE_IDLE_ACTION_STOP
	case "delete":
		result.Action = pb.AutoLifecycleIdleAction_AUTO_LIFECYCLE_IDLE_ACTION_DELETE
	default:
		return nil, fmt.Errorf("unsupported auto lifecycle action %q", stage.Action)
	}

	return result, nil
}

func parseAutoLifecycleStageSeconds(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("auto lifecycle stage duration cannot be empty")
	}

	if strings.HasSuffix(raw, "d") {
		daysRaw := strings.TrimSuffix(raw, "d")
		days, err := strconv.ParseInt(daysRaw, 10, 64)
		if err != nil || days <= 0 {
			return 0, fmt.Errorf("invalid auto lifecycle stage duration %q", raw)
		}
		return days * 24 * 60 * 60, nil
	}

	duration, err := time.ParseDuration(raw)
	if err != nil || duration <= 0 || duration%time.Second != 0 {
		return 0, fmt.Errorf("invalid auto lifecycle stage duration %q", raw)
	}

	return int64(duration / time.Second), nil
}

func parseSimpleDuration(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("duration cannot be empty")
	}

	unit := raw[len(raw)-1]
	number := strings.TrimSpace(raw[:len(raw)-1])
	if number == "" {
		return 0, fmt.Errorf("invalid duration %q", raw)
	}

	scale := time.Second
	switch unit {
	case 's':
		scale = time.Second
	case 'm':
		scale = time.Minute
	case 'h':
		scale = time.Hour
	case 'd':
		scale = 24 * time.Hour
	default:
		return 0, fmt.Errorf("unsupported duration unit in %q", raw)
	}

	value, err := strconv.ParseFloat(number, 64)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("invalid duration %q", raw)
	}

	nanos := value * float64(scale)
	if nanos > float64(int64(^uint64(0)>>1)) {
		return 0, fmt.Errorf("duration %q is too large", raw)
	}

	return time.Duration(nanos), nil
}

func readSpec() (*ociSpec, error) {
	return readSpecFrom(".")
}

func readSpecFrom(bundlePath string) (*ociSpec, error) {
	data, err := os.ReadFile(filepath.Join(bundlePath, "config.json"))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errReadOCISpec, err)
	}
	var spec ociSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("%w: %w", errParseOCISpec, err)
	}
	return &spec, nil
}
