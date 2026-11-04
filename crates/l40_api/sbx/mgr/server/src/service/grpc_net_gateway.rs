//! Operator-triggered in-place net-gateway cycling.
//!
//! Gateways keep their launch-time binary until the process is replaced, so
//! after a node deploy the fleet accumulates gateways running a pre-deploy
//! binary (they miss newer control RPCs and answer them `Unimplemented`).
//! `CycleNetGateway` kills one sandbox's gateway and relaunches it from the
//! node's current on-disk binary while leaving the Firecracker VM and its
//! lifecycle unchanged: the sandbox netns and TPROXY rules are owned outside
//! the gateway, so the replacement re-binds fresh transparent sockets and a
//! queued policy reconcile re-hydrates policy and runtime-policy mode.
//!
//! Deliberately single-sandbox and synchronous. Batching, staleness
//! selection, dry-run, and pacing live in the CLI (`space-sbxmgr
//! cycle-net-gateway`) so an operator's terminal is the rate limit and abort
//! point. In-flight guest connections through the old gateway drop; that is
//! the accepted cost of cycling and why nothing cycles automatically.

use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
    emit_sandbox_event,
};

use super::tail_support::connect_node;
use super::*;
use crate::proto::{CycleNetGatewayRequest, CycleNetGatewayResponse};

/// Bounded waits for the old gateway to exit: after SIGTERM, then after the
/// SIGKILL escalation. The relaunch must observe the old process dead — the
/// runtime-supervisor returns the existing pid while the gateway's pid
/// symlink still resolves to a live process.
const NETGTW_CYCLE_TERM_GRACE: Duration = Duration::from_secs(3);
const NETGTW_CYCLE_KILL_GRACE: Duration = Duration::from_secs(2);
const NETGTW_CYCLE_DEATH_POLL: Duration = Duration::from_millis(100);

/// How the old gateway process ended during a cycle, recorded on the
/// completed event. `OldProcessAlreadyGone` means a signal found the target
/// pid dead (node-manager reports `success: false` without erroring): the
/// gateway died — or the watchdog replaced it — between our liveness check
/// and the signal, which is convergence rather than failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NetGatewayKillOutcome {
    Sigterm,
    Sigkill,
    OldProcessAlreadyGone,
}

impl NetGatewayKillOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Sigterm => "sigterm",
            Self::Sigkill => "sigkill",
            Self::OldProcessAlreadyGone => "old_process_already_gone",
        }
    }
}

impl std::fmt::Display for NetGatewayKillOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl SandboxManagerService {
    pub(super) async fn cycle_net_gateway(
        &self,
        request: Request<CycleNetGatewayRequest>,
    ) -> Result<Response<CycleNetGatewayResponse>, Status> {
        self.authorize_peer(&request, "CycleNetGateway", ALLOW_SANDBOX_OPERATOR)?;
        let req = request.get_ref();
        let sandbox = self.lookup_sandbox(&req.sandbox_id)?;
        let id = sandbox.id.clone();

        if !sandbox.state.can_cycle_net_gateway() {
            return Err(Status::failed_precondition(format!(
                "cannot cycle net-gateway while sandbox is {}",
                sandbox.state
            )));
        }
        let uid = sandbox.uid.ok_or_else(|| {
            Status::failed_precondition("sandbox has no allocated uid; cannot relaunch net-gateway")
        })?;
        let old_pid = sandbox
            .netgtw_pid
            .ok_or_else(|| Status::failed_precondition("sandbox has no tracked net-gateway pid"))?;
        if !crate::watchdog::netgtw_alive(&id) {
            return Err(Status::failed_precondition(
                "net-gateway is not alive; the watchdog restarts dead gateways automatically",
            ));
        }

        let old_build_git_sha = self
            .watchdog
            .get_health(&id)
            .await
            .and_then(|health| health.netgtw_git_sha)
            .unwrap_or_default();

        let base_details = || {
            SandboxEventDetails::default()
                .trigger(SandboxEventTrigger::User)
                .operation("net_gateway_cycle")
                .net_gateway_pid(old_pid.to_string())
                .net_gateway_build_git_sha(old_build_git_sha.clone())
        };
        emit_sandbox_event(
            &id,
            SandboxEventType::NetGatewayCycleRequested,
            SandboxEventSource::SbxManager,
            base_details(),
        );
        let fail = |error: String| {
            pplx_space_metrics::sandbox::worker::net_gateway_cycle(
                pplx_space_metrics::sandbox::worker::NetGatewayCycleResult::Error,
            );
            emit_sandbox_event(
                &id,
                SandboxEventType::NetGatewayCycleFailed,
                SandboxEventSource::SbxManager,
                base_details().error(error.clone()),
            );
            Status::internal(error)
        };

        // Kill the old gateway through node-manager: SIGTERM with a bounded
        // wait, then SIGKILL. The explicit pid matters — node-manager's
        // fallback pid is whichever sidecar launched most recently.
        // `success: false` (no error) means the target pid was already gone —
        // the gateway died (or the watchdog replaced it) between our liveness
        // check and the signal. That is not a failure: skip further signaling
        // and fall through to the relaunch, which either forks a fresh
        // gateway or recovers the watchdog's already-live replacement.
        let mut node_client = connect_node(&self.node_manager)
            .await
            .map_err(|error| fail(format!("cannot connect to node-manager: {error}")))?;
        let term_signalled = node_client
            .stop_process(StopProcessRequest {
                execution_fence: sandbox.node_execution_fence(),
                sandbox_id: id.clone(),
                signal: libc::SIGTERM as u32,
                pid: Some(old_pid),
            })
            .await
            .map_err(|error| fail(format!("stop net-gateway (SIGTERM) failed: {error}")))?
            .get_ref()
            .success;
        let mut kill_outcome = if term_signalled {
            NetGatewayKillOutcome::Sigterm
        } else {
            NetGatewayKillOutcome::OldProcessAlreadyGone
        };
        if term_signalled && !wait_for_netgtw_death(&id, NETGTW_CYCLE_TERM_GRACE).await {
            let kill_signalled = node_client
                .stop_process(StopProcessRequest {
                    execution_fence: sandbox.node_execution_fence(),
                    sandbox_id: id.clone(),
                    signal: libc::SIGKILL as u32,
                    pid: Some(old_pid),
                })
                .await
                .map_err(|error| fail(format!("stop net-gateway (SIGKILL) failed: {error}")))?
                .get_ref()
                .success;
            kill_outcome = if kill_signalled {
                NetGatewayKillOutcome::Sigkill
            } else {
                NetGatewayKillOutcome::OldProcessAlreadyGone
            };
            // Only a pid we actually signalled can legitimately still be
            // alive; if it survives SIGKILL past the grace, something is
            // wrong on the node. An unsignalled (already-gone) pid with a
            // live symlink means the watchdog already relaunched — converge.
            if kill_signalled && !wait_for_netgtw_death(&id, NETGTW_CYCLE_KILL_GRACE).await {
                return Err(fail("net-gateway did not exit after SIGKILL".to_string()));
            }
        }

        // Relaunch from the current on-disk binary. The watchdog method also
        // drops its cached (old-process) sha so State/--stale report the new
        // gateway as "unknown" until its first stats probe. If the watchdog's
        // dead-sidecar sweep races us and restarts the gateway first, the
        // runtime-supervisor recovers the already-live pid instead of forking
        // a duplicate, so this converges to one gateway either way.
        let new_pid = self
            .watchdog
            .relaunch_netgtw(&id, uid)
            .await
            .map_err(|error| fail(format!("relaunch net-gateway failed: {error}")))?;

        pplx_space_metrics::sandbox::worker::net_gateway_cycle(
            pplx_space_metrics::sandbox::worker::NetGatewayCycleResult::Ok,
        );
        emit_sandbox_event(
            &id,
            SandboxEventType::NetGatewayCycleCompleted,
            SandboxEventSource::SbxManager,
            base_details()
                .pid(new_pid.to_string())
                .result(kill_outcome.as_str()),
        );

        Ok(Response::new(CycleNetGatewayResponse {
            old_pid,
            new_pid,
            old_build_git_sha,
        }))
    }
}

/// Poll the gateway's pid symlink until it stops resolving to a live process
/// or `grace` elapses. Returns true when the gateway is observed dead.
async fn wait_for_netgtw_death(sandbox_id: &str, grace: Duration) -> bool {
    let started = tokio::time::Instant::now();
    while crate::watchdog::netgtw_alive(sandbox_id) {
        if started.elapsed() >= grace {
            return false;
        }
        tokio::time::sleep(NETGTW_CYCLE_DEATH_POLL).await;
    }
    true
}
