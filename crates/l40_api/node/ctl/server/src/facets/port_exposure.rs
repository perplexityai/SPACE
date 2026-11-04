//! Reconciliation of a sandbox's exposed ports, including the loopback bridges behind them.

use super::*;

/// Node-manager port RPCs issued while reconciling one exposure. Kept as an
/// enum so terminal-code classification keys off the exact operation instead
/// of substring-matching a formatted message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PortExposureRpc {
    Allow,
    Expose,
    Unexpose,
    Deny,
}

impl PortExposureRpc {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow port",
            Self::Expose => "expose port",
            Self::Unexpose => "unexpose port",
            Self::Deny => "deny port",
        }
    }
}

impl std::fmt::Display for PortExposureRpc {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Terminal-vs-retryable disposition for a tonic `Status` returned by an
/// sbx-manager desired-state apply sub-op (loopback-bridge open, snapshot,
/// auto-lifecycle). Terminal means retrying on this node cannot converge while
/// state is unchanged, so the queue ref must complete; a lifecycle or cluster
/// repair changes state and periodic desired-state sync re-applies afterward.
///
/// Terminal **only** when sbx-manager marks the status. sbx-manager answers
/// `FailedPrecondition` for both terminal cases (guest unreachable in a
/// settled non-reachable state; auto-lifecycle target stuck in
/// `Failed`/`CleanupRequired`/`CleanupFailed`/`Quarantined`) and transient ones (a guest still
/// initializing — `guest_not_ready`; a mid-transition lifecycle state that
/// settles), and `NotFound` for both a gone row and the startup race where the
/// local row has not appeared yet (`ApplyPortExposuresAndRetry`). Neither bare
/// code can distinguish these, so classification defers entirely to the marker
/// the server sets at its own precondition gate; everything unmarked —
/// transport, `Unavailable`, `Internal`, timeouts, rollout-skew
/// `Unimplemented`, unmarked `FailedPrecondition`/`NotFound` — stays retryable.
///
/// The node-manager `ExposePort` path does **not** use this: its
/// `FailedPrecondition`/`NotFound` are unambiguously the terminal stale-netns /
/// gone-allocation cases, so it classifies by code directly (see
/// `SandboxPortExposureApplyError`).
pub(super) fn sbx_mgr_desired_state_apply_is_terminal(status: &tonic::Status) -> bool {
    pplx_space_common_grpc::is_desired_state_apply_terminal(status)
}

/// Failure applying one sandbox port exposure. RPC failures preserve the
/// tonic `Status` (code + message) instead of flattening it to a `String` so
/// the reconcile loop can distinguish terminal dispositions from retryable
/// ones.
#[derive(Debug, thiserror::Error)]
pub(super) enum SandboxPortExposureApplyError {
    /// Local pre-validation failed before any node-manager RPC.
    #[error("{0}")]
    Invalid(String),
    /// A node-manager port RPC failed.
    #[error("{operation}: {status}")]
    Rpc {
        operation: PortExposureRpc,
        #[source]
        status: tonic::Status,
    },
    /// The in-VM loopback bridge re-arm failed after the exposure RPCs
    /// succeeded.
    #[error(transparent)]
    Bridge(OpenLoopbackBridgeError),
}

impl SandboxPortExposureApplyError {
    /// Terminal: this node cannot make progress by retrying while cluster
    /// state is unchanged. Completion policy is handled separately because
    /// stale-netns and unreachable-guest failures have different repair paths.
    ///
    /// The two terminal sub-ops classify differently because their servers do.
    ///
    /// `ExposePort` (node-manager): terminal on `NotFound` (no allocation for
    /// this sandbox — authoritatively absent from this node) or a
    /// `FailedPrecondition` **tagged with the stale-pod-netns marker** (the CNI
    /// namespace behind the allocation is gone). `FailedPrecondition` is *not*
    /// terminal on its own — the RPC also returns it for the root-namespace
    /// ingress-safety rejection, which has a live allocation the reconciler
    /// won't repair — so keying on the exact marker keeps the two apart.
    ///
    /// The loopback-bridge open (sbx-manager) answers `FailedPrecondition` for
    /// both the terminal guest-unreachable gate and the transient
    /// `guest_not_ready` case, so it defers to
    /// [`sbx_mgr_desired_state_apply_is_terminal`], which trusts the server-set
    /// terminal marker rather than the bare code.
    ///
    /// Transport errors, `Internal` (e.g. netlink timeouts), the bridge
    /// not-yet-ready status, and every other RPC stay retryable.
    pub(super) fn is_terminal(&self) -> bool {
        match self {
            Self::Rpc {
                operation: PortExposureRpc::Expose,
                status,
            } => status.code() == Code::NotFound || Self::has_stale_pod_netns_marker(status),
            Self::Bridge(OpenLoopbackBridgeError::Rpc(status)) => {
                sbx_mgr_desired_state_apply_is_terminal(status)
            }
            _ => false,
        }
    }

    /// Whether this classified failure may complete the enclosing sandbox ref.
    ///
    /// A stale-netns `ExposePort` failure never completes the ref: the ref stays
    /// retryable so a stale-netns sandbox cannot be stranded with no repair
    /// owner. A marked
    /// sbx-manager bridge failure is different: local state exists but the
    /// guest cannot become reachable without a separate lifecycle transition,
    /// so retrying this ref cannot help. Complete it immediately and let the
    /// periodic full reconcile reapply after the lifecycle changes.
    pub(super) fn completes_ref(&self) -> bool {
        match self {
            Self::Bridge(OpenLoopbackBridgeError::Rpc(status)) => {
                sbx_mgr_desired_state_apply_is_terminal(status)
            }
            _ => false,
        }
    }

    pub(super) fn has_stale_pod_netns_marker(status: &tonic::Status) -> bool {
        status
            .metadata()
            .get(EXPOSE_TERMINAL_REASON_METADATA_KEY)
            .and_then(|value| value.to_str().ok())
            == Some(EXPOSE_TERMINAL_REASON_STALE_POD_NETNS)
    }
}

/// Outcome of applying one sandbox port exposure. The `result` decides
/// `apply_state` (Ok → applied, Err → failed); `bridge_status` and
/// `service_generation` are observation metadata persisted alongside so the
/// reconciler can skip steady-state rows and a future UI can render
/// reachability without a live node probe.
pub(super) struct SandboxPortExposureApplyOutcome {
    pub(super) result: Result<(), SandboxPortExposureApplyError>,
    pub(super) bridge_status: Option<i32>,
    pub(super) service_generation: Option<String>,
}

pub(super) trait SandboxPortExposureApplier {
    fn apply<'a>(
        &'a mut self,
        sandbox_id: &'a str,
        exposure: &'a SandboxPortExposureDesiredState,
    ) -> Pin<Box<dyn Future<Output = SandboxPortExposureApplyOutcome> + Send + 'a>>;
}

impl SandboxPortExposureApplier for pplx_space_node_mgr_client::NodeManagerGrpcClient {
    fn apply<'a>(
        &'a mut self,
        sandbox_id: &'a str,
        exposure: &'a SandboxPortExposureDesiredState,
    ) -> Pin<Box<dyn Future<Output = SandboxPortExposureApplyOutcome> + Send + 'a>> {
        Box::pin(apply_sandbox_port_exposure_desired(
            self, sandbox_id, exposure,
        ))
    }
}

/// How one sandbox's port-exposure batch is reconciled.
#[derive(Clone, Copy)]
pub(super) struct PortExposureReconcileMode {
    pub(super) action: SandboxDesiredReconcileAction,
}

pub(super) async fn reconcile_sandbox_port_exposures<A: SandboxPortExposureApplier>(
    mode: PortExposureReconcileMode,
    applier: &mut A,
    sandbox_id: &str,
    exposures: Vec<SandboxPortExposureDesiredState>,
    stats: &mut ReconcileStats,
    element_errors: &mut BTreeMap<String, String>,
    exposure_apply_states: &mut Vec<SandboxPortExposureApplyState>,
) {
    let action = mode.action;
    if action == SandboxDesiredReconcileAction::SkipNonLocal {
        return;
    }

    let mut terminal_failure = false;
    for exposure in exposures {
        let outcome = applier.apply(sandbox_id, &exposure).await;
        let (apply_state, error) = match outcome.result {
            Ok(()) => {
                stats.sandboxes_applied = stats.sandboxes_applied.saturating_add(1);
                (
                    SandboxPortApplyStateKind::SandboxPortApplyStateApplied,
                    None,
                )
            }
            Err(error) => {
                // Completion policy depends on the failing boundary. A stale
                // node-manager netns needs the rollout-gated orphan repair
                // owner, while a marked sbx-manager bridge failure has local
                // state and completes until a lifecycle change makes the guest
                // reachable again. Keep classification and completion separate
                // so the stale-netns safety gate cannot suppress the actual
                // unreachable-guest poison-ref fix.
                let classified_terminal = error.is_terminal();
                let complete_ref = error.completes_ref();
                tracing::warn!(
                    sandbox_id,
                    protocol = port_protocol_label(exposure.protocol),
                    guest_port = exposure.guest_port,
                    desired_state = port_desired_state_label(exposure.desired_state),
                    classified_terminal,
                    complete_ref,
                    %error,
                    "failed to apply sandbox port exposure desired state"
                );
                let error = error.to_string();
                let result_kind = if complete_ref {
                    // Publish the failed observation but do not fail the
                    // sandbox ref: retrying on this node can never succeed
                    // (the pod netns / allocation is gone, or the guest is
                    // unreachable). The owning reconciler/lifecycle transition
                    // repairs durable state; periodic desired-state sync then
                    // re-applies anything that becomes actionable.
                    terminal_failure = true;
                    pplx_space_metrics::reconcile::DesiredStateApplyResult::Terminal
                } else {
                    record_element_error(element_errors, sandbox_id, &error);
                    pplx_space_metrics::reconcile::DesiredStateApplyResult::Retryable
                };
                pplx_space_metrics::reconcile::desired_state_apply(result_kind);
                (
                    SandboxPortApplyStateKind::SandboxPortApplyStateFailed,
                    Some(error),
                )
            }
        };
        exposure_apply_states.push(SandboxPortExposureApplyState {
            sandbox_id: sandbox_id.to_string(),
            protocol: exposure.protocol,
            guest_port: exposure.guest_port,
            apply_state: apply_state as i32,
            error,
            bridge_status: outcome.bridge_status,
            service_generation: outcome.service_generation,
        });
    }

    // Missing local sandbox state normally keeps the ref failing so the node
    // retries until sbx-manager state shows up. A terminal exposure failure
    // proves that state never will (the netns behind the allocation is gone),
    // so recording the retry error would poison the queue with a ref that can
    // never converge.
    if action == SandboxDesiredReconcileAction::ApplyPortExposuresAndRetry && !terminal_failure {
        record_element_error(
            element_errors,
            sandbox_id,
            "local sandbox state unavailable",
        );
    }
}

pub(super) async fn apply_sandbox_port_exposure_desired(
    client: &mut pplx_space_node_mgr_client::NodeManagerGrpcClient,
    sandbox_id: &str,
    exposure: &SandboxPortExposureDesiredState,
) -> SandboxPortExposureApplyOutcome {
    let mut outcome = SandboxPortExposureApplyOutcome {
        result: Ok(()),
        bridge_status: None,
        service_generation: None,
    };
    if SandboxPortProtocol::try_from(exposure.protocol).unwrap_or(SandboxPortProtocol::Unspecified)
        != SandboxPortProtocol::Tcp
    {
        outcome.result = Err(SandboxPortExposureApplyError::Invalid(format!(
            "unsupported sandbox port exposure protocol {:?}",
            port_protocol_label(exposure.protocol)
        )));
        return outcome;
    }
    let port = match u16::try_from(exposure.guest_port)
        .ok()
        .filter(|port| *port > 0)
    {
        Some(port) => u32::from(port),
        None => {
            outcome.result = Err(SandboxPortExposureApplyError::Invalid(format!(
                "invalid sandbox port {}",
                exposure.guest_port
            )));
            return outcome;
        }
    };
    match SandboxPortDesiredStateKind::try_from(exposure.desired_state)
        .unwrap_or(SandboxPortDesiredStateKind::SandboxPortDesiredStateUnspecified)
    {
        SandboxPortDesiredStateKind::SandboxPortDesiredStateOpen => {
            if let Err(status) = client
                .allow_port(Request::new(AllowPortRequest {
                    sandbox_id: sandbox_id.to_string(),
                    port,
                    protocol: NetworkProtocol::Tcp as i32,
                }))
                .await
            {
                outcome.result = Err(SandboxPortExposureApplyError::Rpc {
                    operation: PortExposureRpc::Allow,
                    status,
                });
                return outcome;
            }
            if let Err(status) = client
                .expose_port(Request::new(ExposePortRequest {
                    sandbox_id: sandbox_id.to_string(),
                    port,
                    protocol: NetworkProtocol::Tcp as i32,
                    guest_port: Some(port),
                }))
                .await
            {
                outcome.result = Err(SandboxPortExposureApplyError::Rpc {
                    operation: PortExposureRpc::Expose,
                    status,
                });
                return outcome;
            }
            // Re-arm the in-VM loopback bridge. This matters on (a) node
            // restart / state recovery — `LoopbackBridgeManager` starts
            // empty so authorized ports must be re-installed — and (b)
            // sandbox guest restart, where spaced-svc forgets the
            // authorized set. The observed bridge status and service
            // generation are persisted regardless of result so the
            // reconciler's steady-state skip and the future UI both have
            // current data.
            let bridge = open_loopback_bridge_for_exposure(sandbox_id, port).await;
            // `bridge.bridge_status` and `bridge.service_generation` are
            // already `Option`s — `None` for transport failures (no
            // observation possible) so the publish path's `COALESCE`
            // preserves any prior observation rather than overwriting
            // with `UNSPECIFIED` (which the cluster parser rejects).
            outcome.bridge_status = bridge.bridge_status;
            outcome.service_generation = bridge.service_generation;
            outcome.result = bridge.result.map_err(SandboxPortExposureApplyError::Bridge);
        }
        SandboxPortDesiredStateKind::SandboxPortDesiredStateClosed => {
            if let Err(status) = client
                .unexpose_port(Request::new(UnexposePortRequest {
                    sandbox_id: sandbox_id.to_string(),
                    port: Some(port),
                    protocol: NetworkProtocol::Tcp as i32,
                }))
                .await
            {
                outcome.result = Err(SandboxPortExposureApplyError::Rpc {
                    operation: PortExposureRpc::Unexpose,
                    status,
                });
                return outcome;
            }
            if let Err(status) = client
                .deny_port(Request::new(DenyPortRequest {
                    sandbox_id: sandbox_id.to_string(),
                    port: Some(port),
                    protocol: NetworkProtocol::Tcp as i32,
                }))
                .await
            {
                outcome.result = Err(SandboxPortExposureApplyError::Rpc {
                    operation: PortExposureRpc::Deny,
                    status,
                });
                return outcome;
            }
            // Tear down the in-VM bridge symmetric to the open arm. This
            // is purely housekeeping: the bridge dies when the sandbox
            // does, but if the sandbox keeps running across many open/
            // close cycles, leaving stale forwarders authorized burns one
            // tokio task per stale port. Best-effort.
            if let Err(error) = close_loopback_bridge_for_exposure(sandbox_id, port).await {
                tracing::warn!(
                    sandbox_id,
                    port,
                    %error,
                    "loopback bridge close failed during reconcile (forwarder dies with sandbox)"
                );
            }
        }
        SandboxPortDesiredStateKind::SandboxPortDesiredStateUnspecified => {
            outcome.result = Err(SandboxPortExposureApplyError::Invalid(
                "sandbox port desired state is required".to_string(),
            ));
        }
    }
    outcome
}

/// Failure opening the in-VM loopback bridge for one exposure. The RPC arm
/// preserves the tonic `Status` so a guest-unreachable `FailedPrecondition`
/// (sandbox failed/suspending/suspended/stopped) is distinguishable from
/// transient failures; `Connect` and `NotReady` are always retryable.
#[derive(Debug, thiserror::Error)]
pub(super) enum OpenLoopbackBridgeError {
    /// Could not connect to the local sbx-manager socket — transport, so
    /// retryable.
    #[error("connect sbx-mgr: {0}")]
    Connect(String),
    /// The `SpacedOpenLoopbackBridge` RPC returned an error status. The tonic
    /// `Status` is preserved so the guest-unreachable `FailedPrecondition`
    /// case can be classified terminal by the shared classifier.
    #[error("spaced_open_loopback_bridge: {0}")]
    Rpc(#[source] tonic::Status),
    /// The RPC succeeded but the bridge is not in a serving status yet
    /// (`WaitingForListener`/`BindConflict`/`Failed`/`Unspecified`). The
    /// reconciler re-ticks to promote it, so this stays retryable.
    #[error("loopback bridge not ready: {0}")]
    NotReady(String),
}

/// Outcome of one `SpacedOpenLoopbackBridge` call. `bridge_status` is
/// `Some(numeric proto enum value)` only when we received a response —
/// transport failures (sbx-mgr connect, RPC error) return `None` so the
/// publish path leaves the column unchanged via `COALESCE` instead of
/// overwriting with `UNSPECIFIED` (which the cluster parser rejects and
/// would fail the entire batch). `service_generation` is the
/// spaced-guest-svc per-process random id — likewise `None` on transport
/// failure.
pub(super) struct OpenLoopbackBridgeOutcome {
    pub(super) result: Result<(), OpenLoopbackBridgeError>,
    pub(super) bridge_status: Option<i32>,
    pub(super) service_generation: Option<String>,
}

pub(super) async fn open_loopback_bridge_for_exposure(
    sandbox_id: &str,
    port: u32,
) -> OpenLoopbackBridgeOutcome {
    use pplx_space_sbx_mgr_client::proto::SpacedLoopbackBridgeStatus as S;

    let mut client = match pplx_space_sbx_mgr_client::connect().await {
        Ok(client) => client,
        Err(error) => {
            return OpenLoopbackBridgeOutcome {
                result: Err(OpenLoopbackBridgeError::Connect(error.to_string())),
                bridge_status: None,
                service_generation: None,
            };
        }
    };
    let response = match client
        .spaced_open_loopback_bridge(
            pplx_space_sbx_mgr_client::proto::SpacedOpenLoopbackBridgeRequest {
                sandbox_id: sandbox_id.to_string(),
                operation: Some(
                    pplx_space_sbx_mgr_client::operations::OpenLoopbackBridgeRequest {
                        port,
                        wait_timeout: Some(prost_types::Duration {
                            seconds: 2,
                            nanos: 0,
                        }),
                    },
                ),
            },
        )
        .await
    {
        Ok(response) => response.into_inner(),
        Err(status) => {
            return OpenLoopbackBridgeOutcome {
                result: Err(OpenLoopbackBridgeError::Rpc(status)),
                bridge_status: None,
                service_generation: None,
            };
        }
    };
    let status = S::try_from(response.status).unwrap_or(S::Unspecified);
    let service_generation = response.service_generation.clone();
    // Only `Direct` and `LoopbackForwarded` count as `applied` — the
    // exposure represents the whole externally visible port contract, not
    // just the host DNAT half. `WaitingForListener` means the bridge is
    // armed but no loopback listener has appeared yet, so the port still
    // returns 502 to clients; that's not `applied`. The reconciler ticks
    // every poll interval and will promote the row once the watcher sees
    // the listener and transitions to `LoopbackForwarded`. `BindConflict`
    // / `Failed` / `Unspecified` are propagated for the same reason.
    // Symmetric with the api-side caller in `controllers/sandbox/ports.rs`.
    let result = match status {
        S::Direct | S::LoopbackForwarded => {
            tracing::debug!(
                sandbox_id,
                port,
                ?status,
                detail = %response.detail,
                service_generation = %service_generation,
                "reconcile installed loopback bridge"
            );
            Ok(())
        }
        S::WaitingForListener | S::BindConflict | S::Failed | S::Unspecified => {
            Err(OpenLoopbackBridgeError::NotReady(format!(
                "status={status:?} detail={} service_generation={service_generation}",
                response.detail
            )))
        }
    };
    OpenLoopbackBridgeOutcome {
        result,
        bridge_status: Some(response.status),
        service_generation: Some(service_generation),
    }
}

pub(super) async fn close_loopback_bridge_for_exposure(
    sandbox_id: &str,
    port: u32,
) -> Result<(), String> {
    let mut client = pplx_space_sbx_mgr_client::connect()
        .await
        .map_err(|error| format!("connect sbx-mgr: {error}"))?;
    let _ = client
        .spaced_close_loopback_bridge(
            pplx_space_sbx_mgr_client::proto::SpacedCloseLoopbackBridgeRequest {
                sandbox_id: sandbox_id.to_string(),
                operation: Some(
                    pplx_space_sbx_mgr_client::operations::CloseLoopbackBridgeRequest { port },
                ),
            },
        )
        .await
        .map_err(|error| format!("spaced_close_loopback_bridge: {error}"))?;
    Ok(())
}

pub(super) fn port_protocol_label(value: i32) -> &'static str {
    match SandboxPortProtocol::try_from(value).unwrap_or(SandboxPortProtocol::Unspecified) {
        SandboxPortProtocol::Tcp => "tcp",
        SandboxPortProtocol::Unspecified => "unspecified",
    }
}

pub(super) fn port_desired_state_label(value: i32) -> &'static str {
    match SandboxPortDesiredStateKind::try_from(value)
        .unwrap_or(SandboxPortDesiredStateKind::SandboxPortDesiredStateUnspecified)
    {
        SandboxPortDesiredStateKind::SandboxPortDesiredStateOpen => "open",
        SandboxPortDesiredStateKind::SandboxPortDesiredStateClosed => "closed",
        SandboxPortDesiredStateKind::SandboxPortDesiredStateUnspecified => "unspecified",
    }
}
