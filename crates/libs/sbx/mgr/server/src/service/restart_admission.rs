use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::{Request, Response};
use http_body::Body as HttpBody;
use tower::{BoxError, Layer, Service};

use super::SandboxManagerService;
use super::drain::{DrainState, acquire_state_writer, with_state_writer};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::Instrument;

tokio::task_local! {
    static CALLER_CANCELLED: CancellationToken;
}

/// Preserve cancellation at passive admission boundaries in an owned child.
/// This never aborts already admitted mutation or cleanup work.
pub(super) fn inherit_admission_cancellation<T>(
    future: impl Future<Output = T>,
) -> impl Future<Output = T> {
    let cancelled = CALLER_CANCELLED.try_with(Clone::clone).ok();
    async move {
        match cancelled {
            Some(cancelled) => CALLER_CANCELLED.scope(cancelled, future).await,
            None => future.await,
        }
    }
}

/// Wait at a cancellation-safe activation admission boundary for at most five
/// seconds. A disconnected caller loses its queue position immediately; a live
/// caller gets a retryable contention error. Internal callers have the same cap.
///
/// Only wrap lock acquisition or passive observation, never an operation with
/// side effects or a completion fence: those must retain their writer lease and
/// finish even after the HTTP response future is dropped.
pub(super) async fn wait_for_activation_admission<T>(
    sandbox_id: &str,
    admission: impl Future<Output = T>,
) -> Result<T, tonic::Status> {
    let cancelled = CALLER_CANCELLED.try_with(Clone::clone).unwrap_or_default();
    tokio::select! {
        biased;
        _ = cancelled.cancelled() => Err(tonic::Status::cancelled(format!(
            "activation caller disconnected before admission for sandbox {sandbox_id}"
        ))),
        result = tokio::time::timeout(std::time::Duration::from_secs(5), admission) => {
            result.map_err(|_| tonic::Status::unavailable(format!(
                "sandbox {sandbox_id} activation admission is busy; retry"
            )))
        }
    }
}

/// Run a passive observation at an activation admission boundary, abandoning
/// it as soon as the caller disconnects. Like [`wait_for_activation_admission`],
/// only for observations with no side effects: a lock the caller holds is then
/// released with the caller rather than when the observation gives up.
pub(super) async fn observe_unless_caller_cancelled<T>(
    sandbox_id: &str,
    observation: impl Future<Output = T>,
) -> Result<T, tonic::Status> {
    let cancelled = CALLER_CANCELLED.try_with(Clone::clone).unwrap_or_default();
    tokio::select! {
        biased;
        _ = cancelled.cancelled() => Err(tonic::Status::cancelled(format!(
            "activation caller disconnected before admission for sandbox {sandbox_id}"
        ))),
        result = observation => Ok(result),
    }
}

/// How a guest process start ended.
#[derive(Debug)]
pub(super) enum GuestStartOutcome<T> {
    Opened(T),
    Failed(tonic::Status),
    /// The caller disconnected first; nothing failed in the guest.
    CallerGone,
}

/// Abandon a guest process start whose caller has disconnected.
///
/// The guest accepts a start only at its idempotency `begin()`. A start
/// dropped before then releases its reservation and never runs; one already
/// begun keeps running and is recovered by the caller's same-key retry. So the
/// start call itself is safe to abandon. Waking the sandbox before it is not,
/// and stays owned.
pub(super) async fn abandon_start_if_caller_cancelled<T>(
    start: impl Future<Output = Result<T, tonic::Status>>,
) -> GuestStartOutcome<T> {
    let cancelled = CALLER_CANCELLED.try_with(Clone::clone).unwrap_or_default();
    tokio::select! {
        biased;
        _ = cancelled.cancelled() => GuestStartOutcome::CallerGone,
        result = start => match result {
            Ok(opened) => GuestStartOutcome::Opened(opened),
            Err(status) => GuestStartOutcome::Failed(status),
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RestartAdmission {
    ReadOnly,
    ReadOnlyWithNarrowStateWrites,
    StateWriter,
}

impl RestartAdmission {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::ReadOnlyWithNarrowStateWrites => "read_only_with_narrow_state_writes",
            Self::StateWriter => "state_writer",
        }
    }
}

impl fmt::Display for RestartAdmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("restart admission is not classified for RPC path {path}")]
struct UnclassifiedRestartRpc {
    path: String,
}

macro_rules! restart_rpcs {
    ($($variant:ident => ($path:literal, $admission:ident),)+) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        enum RestartRpc {
            $($variant,)+
        }

        impl RestartRpc {
            #[cfg(test)]
            const ALL: &'static [Self] = &[$(Self::$variant,)+];

            const fn admission(self) -> RestartAdmission {
                match self {
                    $(Self::$variant => RestartAdmission::$admission,)+
                }
            }

            const fn handoff_cancellable_response_stream(self) -> bool {
                matches!(
                    self,
                    Self::HealthWatch
                        | Self::SandboxSpacedList
                        | Self::SandboxSpacedRead
                        | Self::SandboxSpacedWatch
                        | Self::SandboxSpacedStartProcess
                        | Self::SandboxSpacedConnectProcess
                        | Self::SandboxExportSnapshot
                        | Self::SandboxExportSandbox
                        | Self::SandboxScanCleanupTargets
                        | Self::SandboxServicePortIngress
                )
            }

            const fn handoff_cancellable_response_stream_name(self) -> Option<&'static str> {
                if self.handoff_cancellable_response_stream() {
                    Some(self.as_str())
                } else {
                    None
                }
            }

            const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $path,)+
                }
            }
        }

        impl fmt::Display for RestartRpc {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl FromStr for RestartRpc {
            type Err = UnclassifiedRestartRpc;

            fn from_str(path: &str) -> Result<Self, Self::Err> {
                match path {
                    $($path => Ok(Self::$variant),)+
                    _ => Err(UnclassifiedRestartRpc {
                        path: path.to_string(),
                    }),
                }
            }
        }
    };
}

restart_rpcs! {
    SandboxGetExecutionCapabilities => ("/space.sandbox.manager.v1.SandboxManager/GetExecutionCapabilities", ReadOnly),
    HealthCheck => ("/grpc.health.v1.Health/Check", ReadOnly),
    HealthWatch => ("/grpc.health.v1.Health/Watch", ReadOnly),
    SandboxCreate => ("/space.sandbox.manager.v1.SandboxManager/Create", StateWriter),
    SandboxStart => ("/space.sandbox.manager.v1.SandboxManager/Start", StateWriter),
    SandboxStop => ("/space.sandbox.manager.v1.SandboxManager/Stop", StateWriter),
    SandboxKill => ("/space.sandbox.manager.v1.SandboxManager/Kill", StateWriter),
    SandboxDelete => ("/space.sandbox.manager.v1.SandboxManager/Delete", StateWriter),
    SandboxListAutoLifecycleDeleteReceipts => ("/space.sandbox.manager.v1.SandboxManager/ListAutoLifecycleDeleteReceipts", ReadOnlyWithNarrowStateWrites),
    SandboxAcknowledgeAutoLifecycleDeleteReceipts => ("/space.sandbox.manager.v1.SandboxManager/AcknowledgeAutoLifecycleDeleteReceipts", StateWriter),
    SandboxListPendingLocalBackups => ("/space.sandbox.manager.v1.SandboxManager/ListPendingLocalBackups", ReadOnly),
    SandboxState => ("/space.sandbox.manager.v1.SandboxManager/State", ReadOnly),
    SandboxGetHealth => ("/space.sandbox.manager.v1.SandboxManager/GetSandboxHealth", ReadOnly),
    SandboxGetEnv => ("/space.sandbox.manager.v1.SandboxManager/GetEnv", ReadOnly),
    SandboxList => ("/space.sandbox.manager.v1.SandboxManager/List", ReadOnly),
    SandboxScanCleanupTargets => ("/space.sandbox.manager.v1.SandboxManager/ScanCleanupTargets", ReadOnly),
    SandboxListBuildReferencingSandboxes => ("/space.sandbox.manager.v1.SandboxManager/ListBuildReferencingSandboxes", ReadOnly),
    SandboxGetDiskUsage => ("/space.sandbox.manager.v1.SandboxManager/GetDiskUsage", ReadOnly),
    SandboxCleanup => ("/space.sandbox.manager.v1.SandboxManager/Cleanup", StateWriter),
    SandboxListTemplates => ("/space.sandbox.manager.v1.SandboxManager/ListTemplates", ReadOnly),
    SandboxPause => ("/space.sandbox.manager.v1.SandboxManager/Pause", StateWriter),
    SandboxResume => ("/space.sandbox.manager.v1.SandboxManager/Resume", StateWriter),
    SandboxWake => ("/space.sandbox.manager.v1.SandboxManager/Wake", StateWriter),
    SandboxCreateSnapshot => ("/space.sandbox.manager.v1.SandboxManager/CreateSnapshot", StateWriter),
    SandboxWaitTemplateApps => ("/space.sandbox.manager.v1.SandboxManager/WaitTemplateApps", StateWriter),
    SandboxCreateTemplateBaseSnapshot => ("/space.sandbox.manager.v1.SandboxManager/CreateTemplateBaseSnapshot", StateWriter),
    SandboxSuspend => ("/space.sandbox.manager.v1.SandboxManager/Suspend", StateWriter),
    SandboxPrepareTeardownSuspend => ("/space.sandbox.manager.v1.SandboxManager/PrepareTeardownSuspend", StateWriter),
    SandboxEnqueueArtifactBackup => ("/space.sandbox.manager.v1.SandboxManager/EnqueueArtifactBackup", StateWriter),
    SandboxEvictClusterDeletedSandbox => ("/space.sandbox.manager.v1.SandboxManager/EvictClusterDeletedSandbox", StateWriter),
    SandboxUpgradeRuntime => ("/space.sandbox.manager.v1.SandboxManager/UpgradeRuntime", StateWriter),
    SandboxEnableSnapshots => ("/space.sandbox.manager.v1.SandboxManager/EnableSnapshots", StateWriter),
    SandboxDisableSnapshots => ("/space.sandbox.manager.v1.SandboxManager/DisableSnapshots", StateWriter),
    SandboxListSnapshots => ("/space.sandbox.manager.v1.SandboxManager/ListSnapshots", ReadOnly),
    SandboxDeleteSnapshots => ("/space.sandbox.manager.v1.SandboxManager/DeleteSnapshots", StateWriter),
    SandboxRollback => ("/space.sandbox.manager.v1.SandboxManager/Rollback", StateWriter),
    SandboxStats => ("/space.sandbox.manager.v1.SandboxManager/Stats", StateWriter),
    SandboxUpdate => ("/space.sandbox.manager.v1.SandboxManager/Update", StateWriter),
    SandboxSetAutoLifecyclePolicy => ("/space.sandbox.manager.v1.SandboxManager/SetAutoLifecyclePolicy", StateWriter),
    SandboxTouchAutoLifecycle => ("/space.sandbox.manager.v1.SandboxManager/TouchAutoLifecycle", StateWriter),
    SandboxGetNetworkPolicy => ("/space.sandbox.manager.v1.SandboxManager/GetNetworkPolicy", ReadOnly),
    SandboxSetNetworkPolicy => ("/space.sandbox.manager.v1.SandboxManager/SetNetworkPolicy", StateWriter),
    SandboxTemplateNetworkPolicyUpdated => ("/space.sandbox.manager.v1.SandboxManager/TemplateNetworkPolicyUpdated", StateWriter),
    SandboxCycleNetGateway => ("/space.sandbox.manager.v1.SandboxManager/CycleNetGateway", StateWriter),
    SandboxSpacedPingGuest => ("/space.sandbox.manager.v1.SandboxManager/SpacedPingGuest", ReadOnly),
    SandboxSpacedGetStatus => ("/space.sandbox.manager.v1.SandboxManager/SpacedGetStatus", ReadOnly),
    SandboxSpacedWaitReady => ("/space.sandbox.manager.v1.SandboxManager/SpacedWaitReady", ReadOnly),
    SandboxSpacedStat => ("/space.sandbox.manager.v1.SandboxManager/SpacedStat", ReadOnlyWithNarrowStateWrites),
    SandboxSpacedList => ("/space.sandbox.manager.v1.SandboxManager/SpacedList", ReadOnlyWithNarrowStateWrites),
    SandboxSpacedRead => ("/space.sandbox.manager.v1.SandboxManager/SpacedRead", ReadOnlyWithNarrowStateWrites),
    SandboxSpacedWrite => ("/space.sandbox.manager.v1.SandboxManager/SpacedWrite", StateWriter),
    SandboxSpacedWriteBatch => ("/space.sandbox.manager.v1.SandboxManager/SpacedWriteBatch", StateWriter),
    SandboxSpacedMkdir => ("/space.sandbox.manager.v1.SandboxManager/SpacedMkdir", StateWriter),
    SandboxSpacedRemove => ("/space.sandbox.manager.v1.SandboxManager/SpacedRemove", StateWriter),
    SandboxSpacedRename => ("/space.sandbox.manager.v1.SandboxManager/SpacedRename", StateWriter),
    SandboxSpacedNormalizeWatchRoot => ("/space.sandbox.manager.v1.SandboxManager/SpacedNormalizeWatchRoot", ReadOnlyWithNarrowStateWrites),
    SandboxSpacedWatch => ("/space.sandbox.manager.v1.SandboxManager/SpacedWatch", ReadOnlyWithNarrowStateWrites),
    SandboxSpacedCreateWatcher => ("/space.sandbox.manager.v1.SandboxManager/SpacedCreateWatcher", StateWriter),
    SandboxSpacedGetWatcherEvents => ("/space.sandbox.manager.v1.SandboxManager/SpacedGetWatcherEvents", ReadOnlyWithNarrowStateWrites),
    SandboxSpacedRemoveWatcher => ("/space.sandbox.manager.v1.SandboxManager/SpacedRemoveWatcher", StateWriter),
    SandboxSpacedEnableFileChanges => ("/space.sandbox.manager.v1.SandboxManager/SpacedEnableFileChanges", StateWriter),
    SandboxSpacedDisableFileChanges => ("/space.sandbox.manager.v1.SandboxManager/SpacedDisableFileChanges", StateWriter),
    SandboxSpacedGetModifiedFiles => ("/space.sandbox.manager.v1.SandboxManager/SpacedGetModifiedFiles", ReadOnlyWithNarrowStateWrites),
    SandboxSpacedStartProcess => ("/space.sandbox.manager.v1.SandboxManager/SpacedStartProcess", StateWriter),
    SandboxSpacedConnectProcess => ("/space.sandbox.manager.v1.SandboxManager/SpacedConnectProcess", ReadOnlyWithNarrowStateWrites),
    SandboxSpacedListProcesses => ("/space.sandbox.manager.v1.SandboxManager/SpacedListProcesses", ReadOnlyWithNarrowStateWrites),
    SandboxSpacedSendSignal => ("/space.sandbox.manager.v1.SandboxManager/SpacedSendSignal", StateWriter),
    SandboxSpacedSendInput => ("/space.sandbox.manager.v1.SandboxManager/SpacedSendInput", StateWriter),
    SandboxSpacedCloseStdin => ("/space.sandbox.manager.v1.SandboxManager/SpacedCloseStdin", StateWriter),
    SandboxSpacedResizePty => ("/space.sandbox.manager.v1.SandboxManager/SpacedResizePty", StateWriter),
    SandboxSpacedKillProcess => ("/space.sandbox.manager.v1.SandboxManager/SpacedKillProcess", StateWriter),
    SandboxSpacedOpenLoopbackBridge => ("/space.sandbox.manager.v1.SandboxManager/SpacedOpenLoopbackBridge", StateWriter),
    SandboxSpacedCloseLoopbackBridge => ("/space.sandbox.manager.v1.SandboxManager/SpacedCloseLoopbackBridge", StateWriter),
    SandboxAdoptPoolSandbox => ("/space.sandbox.manager.v1.SandboxManager/AdoptPoolSandbox", StateWriter),
    SandboxWait => ("/space.sandbox.manager.v1.SandboxManager/Wait", ReadOnly),
    SandboxCloneFromDonor => ("/space.sandbox.manager.v1.SandboxManager/CloneFromDonor", StateWriter),
    SandboxExportSnapshot => ("/space.sandbox.manager.v1.SandboxManager/ExportSnapshot", ReadOnly),
    SandboxImportSnapshot => ("/space.sandbox.manager.v1.SandboxManager/ImportSnapshot", StateWriter),
    SandboxExportSandbox => ("/space.sandbox.manager.v1.SandboxManager/ExportSandbox", ReadOnly),
    SandboxImportSandbox => ("/space.sandbox.manager.v1.SandboxManager/ImportSandbox", StateWriter),
    SandboxResolveSandboxMemory => ("/space.sandbox.manager.v1.SandboxManager/ResolveSandboxMemory", ReadOnly),
    SandboxServicePortIngress => ("/space.sandbox.manager.v1.SandboxManager/ServicePortIngress", ReadOnlyWithNarrowStateWrites),
    HostSetActivityState => ("/space.spaced.host.control.v1.HostControl/SetActivityState", StateWriter),
    HostRequestActiveFor => ("/space.spaced.host.control.v1.HostControl/RequestActiveFor", StateWriter),
    HostRequestShutdown => ("/space.spaced.host.control.v1.HostControl/RequestShutdown", ReadOnly),
    HostGetNetworkPolicy => ("/space.spaced.host.control.v1.HostControl/GetNetworkPolicy", ReadOnlyWithNarrowStateWrites),
    HostIssueGatewayIntermediateCertificate => ("/space.spaced.host.control.v1.HostControl/IssueGatewayIntermediateCertificate", StateWriter),
}

pub fn handoff_cancellable_response_stream_name(path: &str) -> Option<&'static str> {
    path.parse::<RestartRpc>()
        .ok()
        .and_then(RestartRpc::handoff_cancellable_response_stream_name)
}

#[derive(Clone)]
pub struct SandboxManagerRestartAdmissionLayer {
    drain_state: Arc<DrainState>,
    task_tracker: TaskTracker,
}

impl SandboxManagerService {
    pub fn restart_admission_layer(&self) -> SandboxManagerRestartAdmissionLayer {
        SandboxManagerRestartAdmissionLayer {
            drain_state: Arc::clone(&self.drain_state),
            task_tracker: self.task_tracker.clone(),
        }
    }
}

impl<S> Layer<S> for SandboxManagerRestartAdmissionLayer {
    type Service = SandboxManagerRestartAdmissionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        SandboxManagerRestartAdmissionService {
            inner,
            drain_state: Arc::clone(&self.drain_state),
            task_tracker: self.task_tracker.clone(),
        }
    }
}

#[derive(Clone)]
pub struct SandboxManagerRestartAdmissionService<S> {
    inner: S,
    drain_state: Arc<DrainState>,
    task_tracker: TaskTracker,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for SandboxManagerRestartAdmissionService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError> + Send,
    ReqBody: Send + 'static,
    ResBody: HttpBody<Data = bytes::Bytes> + Send + 'static,
    ResBody::Error: Into<BoxError> + Send + 'static,
{
    type Response = Response<tonic::body::Body>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context).map_err(Into::into)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        let path = request.uri().path();
        let rpc = match path.parse::<RestartRpc>() {
            Ok(rpc) => rpc,
            Err(error) => {
                tracing::error!(method = path, %error, "rejecting unclassified RPC");
                let status = tonic::Status::unimplemented(error.to_string());
                return Box::pin(async move { Ok(status.into_http()) });
            }
        };
        let admission = rpc.admission();
        let writer = match admission {
            RestartAdmission::ReadOnly | RestartAdmission::ReadOnlyWithNarrowStateWrites => None,
            RestartAdmission::StateWriter => {
                match acquire_state_writer(&self.drain_state, rpc.as_str()) {
                    Ok(writer) => Some(writer),
                    Err(status) => {
                        tracing::debug!(method = %rpc, %admission, "rejecting RPC during restart drain");
                        return Box::pin(async move { Ok(status.into_http()) });
                    }
                }
            }
        };
        let future = self.inner.call(request);
        let response = async move {
            let response = future.await.map_err(Into::into)?;
            Ok::<_, BoxError>(response.map(tonic::body::Body::new))
        };
        match writer {
            Some(writer) => {
                // Keep admitted side effects owned, but let safe admission
                // waits observe HTTP cancellation (including transport timeout).
                // Create the guard outside the future so an unpolled response
                // dropped by the transport still cancels its queued admission.
                let cancelled = CancellationToken::new();
                let cancel_on_drop = cancelled.clone().drop_guard();
                // The outer trace layer enters the request span only while it
                // polls its own future. The handler now runs on a spawned task,
                // so carry the request span explicitly; otherwise outbound
                // client spans from state-writer handlers start new root traces.
                let task = self.task_tracker.spawn(
                    with_state_writer(writer, CALLER_CANCELLED.scope(cancelled, response))
                        .in_current_span(),
                );
                Box::pin(async move {
                    let _cancel_on_drop = cancel_on_drop;
                    task.await.map_err(BoxError::from)?
                })
            }
            None => Box::pin(response),
        }
    }
}

#[cfg(test)]
mod tests;
