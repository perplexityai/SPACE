//! Host-control policy loading and validation at the source boundary.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventType, emit_sandbox_event,
};
use pplx_space_net_common::{
    EffectiveNetworkPolicyRef, EffectiveNetworkPolicyRefParseError, NetworkPolicyV1,
    proto::NetworkPolicyV1 as NetworkPolicyProto,
};
use pplx_space_spaced_common::proto::host_control::{
    GetNetworkPolicyRequest, GetNetworkPolicyResponse, host_control_client::HostControlClient,
};

use super::credentials::{InvalidNetworkCredentialSnapshot, NetworkCredentialSnapshot};
use crate::stats::ProxyStats;
use pplx_space_common_core::types::SandboxId;
use pplx_space_common_grpc::ExpectedGrpcStatus;

pub type PolicyLoader = Arc<
    dyn Fn() -> Pin<
            Box<dyn Future<Output = Result<PulledNetworkPolicy, HydrateNetworkStateError>> + Send>,
        > + Send
        + Sync,
>;

#[derive(Debug)]
pub struct PulledNetworkPolicy {
    pub policy: NetworkPolicyV1,
    /// Validated reference; snapshots render its canonical wire representation.
    pub policy_ref: EffectiveNetworkPolicyRef,
    pub network_credentials: NetworkCredentialSnapshot,
}

/// Why a host-control response did not yield a usable V1 network policy.
#[derive(Debug, thiserror::Error)]
pub enum NetworkPolicyFromHostControlError {
    #[error("host-control returned no V1 policy")]
    MissingV1Policy,
    #[error(transparent)]
    InvalidV1Policy(#[from] pplx_space_net_common::NetworkPolicyProtoError),
    #[error("host-control returned invalid network-credential material: {0}")]
    InvalidNetworkCredentials(#[from] InvalidNetworkCredentialSnapshot),
}

/// Why the gateway could not hydrate its policy from host-control. Reported
/// through a sandbox event and, for a refresh, back to the caller as a string,
/// so each variant names the hop that failed.
#[derive(Debug, thiserror::Error)]
pub enum HydrateNetworkStateError {
    #[error("failed to connect to host-control while hydrating policy for {sandbox_id}: {source}")]
    ConnectHostControl {
        sandbox_id: SandboxId,
        #[source]
        source: pplx_space_spaced_common::UdsConnectError,
    },
    #[error("failed to pull initial network policy for {sandbox_id} from host-control: {source}")]
    GetNetworkPolicy {
        sandbox_id: SandboxId,
        #[source]
        source: tonic::Status,
    },
    #[error(transparent)]
    Policy(#[from] NetworkPolicyFromHostControlError),
    #[error("policy ref is invalid: {source}: {reference}")]
    InvalidReference {
        reference: String,
        #[source]
        source: EffectiveNetworkPolicyRefParseError,
    },
}

fn network_policy_from_host_control_proto(
    policy: Option<&NetworkPolicyProto>,
) -> Result<NetworkPolicyV1, NetworkPolicyFromHostControlError> {
    let policy = policy.ok_or(NetworkPolicyFromHostControlError::MissingV1Policy)?;
    Ok(NetworkPolicyV1::try_from(policy)?)
}

struct InitialNetworkState {
    policy: NetworkPolicyV1,
    network_credentials: NetworkCredentialSnapshot,
    policy_ref: EffectiveNetworkPolicyRef,
    network_tx_bytes: u64,
    network_rx_bytes: u64,
}

/// How long after launch host-control may not know this gateway yet.
/// sandbox-manager registers a gateway's process only once its launch returns,
/// so a pull before then is refused as from an unknown caller. A refusal of a
/// pull sent within the window, and within [`MAX_PENDING_ATTEMPTS`] pulls, is
/// pending registration; a refusal of a pull sent after either is a failure.
const REGISTRATION_WINDOW: Duration = Duration::from_secs(10);
const MAX_PENDING_ATTEMPTS: u32 = 40;
/// The backoff between pending pulls: doubling from the first to the cap, each
/// wait drawn uniformly below it (full jitter). Every pull costs host-control a
/// connection's work and a database read behind the lock that registration
/// itself takes, so retries must thin out rather than compete with it.
const PENDING_RETRY_INITIAL: Duration = Duration::from_millis(10);
const PENDING_RETRY_CAP: Duration = Duration::from_millis(250);

impl HydrateNetworkStateError {
    /// Host-control refused the caller: it is not the registered gateway of
    /// any sandbox, which is also the answer before registration.
    fn is_unregistered_caller(&self) -> bool {
        matches!(
            self,
            Self::GetNetworkPolicy { source, .. } if source.code() == tonic::Code::PermissionDenied
        )
    }
}

/// When a refused pull is still pending registration, and what ends a wait.
struct RegistrationWait {
    launched_at: tokio::time::Instant,
    /// Set by the owner's first control call, which follows its registration
    /// of this gateway: a pending pull retries at once instead of waiting out
    /// its backoff.
    owner_contacted: tokio::sync::watch::Receiver<bool>,
    /// Draws a wait at or below a backoff bound: [`jittered`] in production.
    jitter: fn(Duration) -> Duration,
}

/// What a refusal of the next pull means, decided once before it is sent so
/// that the pull's span and the retry after it agree.
#[derive(Clone, Copy, Debug)]
struct RefusalOutcome {
    /// The pull is sent within [`REGISTRATION_WINDOW`] and short of the last
    /// attempt, so its refusal is retried. A pull sent after either is the
    /// last, and its refusal is the failure.
    retried: bool,
    /// Set only for a retried refusal that the owner's first call has not yet
    /// ruled out as pending registration. The pull's span records it as
    /// expected, so a marked refusal is always retried and the refusal that
    /// ends the wait is never marked.
    expected: Option<ExpectedGrpcStatus>,
}

impl RegistrationWait {
    fn refusal_outcome(&self, attempts: u32) -> RefusalOutcome {
        let retried =
            attempts < MAX_PENDING_ATTEMPTS && self.launched_at.elapsed() < REGISTRATION_WINDOW;
        RefusalOutcome {
            retried,
            expected: (retried && !*self.owner_contacted.borrow())
                .then(|| ExpectedGrpcStatus::new(tonic::Code::PermissionDenied)),
        }
    }
}

/// Run `pull`, retrying while host-control does not know this gateway yet,
/// for pulls sent within [`REGISTRATION_WINDOW`] and [`MAX_PENDING_ATTEMPTS`].
/// The first pull is immediate; later ones back off with jitter, and the
/// owner's first call cuts the current wait short. Only an error the window
/// does not cover is returned. Each pull is told whether a refusal is still
/// pending registration, so the call can mark it expected.
async fn pull_once_registered<T, F, Fut>(
    mut wait: RegistrationWait,
    mut pull: F,
) -> Result<T, HydrateNetworkStateError>
where
    F: FnMut(Option<ExpectedGrpcStatus>) -> Fut,
    Fut: std::future::Future<Output = Result<T, HydrateNetworkStateError>>,
{
    let mut backoff = PENDING_RETRY_INITIAL;
    let mut attempts = 0_u32;
    loop {
        attempts += 1;
        let refusal = wait.refusal_outcome(attempts);
        match pull(refusal.expected).await {
            Err(error) if error.is_unregistered_caller() && refusal.retried => {
                tracing::debug!(target: "pplx_space_net_gateway::run", %error, attempts, "host-control does not know this net-gateway yet; retrying");
                let pause = (wait.jitter)(backoff);
                backoff = backoff.saturating_mul(2).min(PENDING_RETRY_CAP);
                if *wait.owner_contacted.borrow() {
                    tokio::time::sleep(pause).await;
                } else {
                    tokio::select! {
                        () = tokio::time::sleep(pause) => {}
                        _ = wait.owner_contacted.wait_for(|contacted| *contacted) => {}
                    }
                }
            }
            result => return result,
        }
    }
}

fn jittered(bound: Duration) -> Duration {
    use rand::RngExt as _;
    let micros = u64::try_from(bound.as_micros()).unwrap_or(u64::MAX);
    Duration::from_micros(rand::rng().random_range(0..=micros))
}

async fn pull_initial_network_state(
    sandbox_id: SandboxId,
    wait: RegistrationWait,
) -> Result<InitialNetworkState, HydrateNetworkStateError> {
    hydrate_network_state(
        sandbox_id,
        wait,
        connect_host_control(sandbox_id),
        |client, pending_refusal| pull_network_state(sandbox_id, client, pending_refusal),
    )
    .await
}

/// Connect, then pull through [`pull_once_registered`] on that one connection,
/// reporting the outcome as sandbox events: one start, then one completion or
/// one failure. Every pending pull reuses the connection, since each new one
/// would cost host-control a handshake while it is registering this gateway.
async fn hydrate_network_state<C, Connect, F, Fut>(
    sandbox_id: SandboxId,
    wait: RegistrationWait,
    connect: Connect,
    mut pull: F,
) -> Result<InitialNetworkState, HydrateNetworkStateError>
where
    C: Clone,
    Connect: std::future::Future<Output = Result<C, HydrateNetworkStateError>>,
    F: FnMut(C, Option<ExpectedGrpcStatus>) -> Fut,
    Fut: std::future::Future<Output = Result<InitialNetworkState, HydrateNetworkStateError>>,
{
    let details = || SandboxEventDetails::default().operation("initial_policy_hydration");
    emit_sandbox_event(
        &sandbox_id.to_string(),
        SandboxEventType::NetworkPolicyEnforcementStarted,
        SandboxEventSource::NetGateway,
        details(),
    );
    let result = match connect.await {
        Ok(client) => {
            pull_once_registered(wait, |pending_refusal| {
                pull(client.clone(), pending_refusal)
            })
            .await
        }
        Err(error) => Err(error),
    };
    match &result {
        Ok(state) => emit_sandbox_event(
            &sandbox_id.to_string(),
            SandboxEventType::NetworkPolicyEnforcementCompleted,
            SandboxEventSource::NetGateway,
            details()
                .installed_network_policy_ref(state.policy_ref.to_string())
                .result("policy_hydrated"),
        ),
        Err(error) => emit_sandbox_event(
            &sandbox_id.to_string(),
            SandboxEventType::NetworkPolicyEnforcementFailed,
            SandboxEventSource::NetGateway,
            details().error(error.to_string()),
        ),
    }
    result
}

type HostControl =
    HostControlClient<pplx_space_common_grpc::GrpcClientTrace<tonic::transport::Channel>>;

async fn connect_host_control(
    sandbox_id: SandboxId,
) -> Result<HostControl, HydrateNetworkStateError> {
    let socket = std::path::Path::new(
        pplx_space_common_core::host::linux::paths::SANDBOX_HOST_CONTROL_SOCKET,
    );
    let channel = pplx_space_spaced_common::connect_uds_channel("sandbox-host-control", socket)
        .await
        .map_err(|source| HydrateNetworkStateError::ConnectHostControl { sandbox_id, source })?;
    Ok(HostControlClient::new(
        pplx_space_common_svc::bootstrap::wrap_grpc_client_channel(channel),
    ))
}

/// Pull the policy over `client`, marking the request when its refusal is
/// still pending registration. Generic over the transport so a test can see
/// the request the client sends.
async fn pull_network_state<T>(
    sandbox_id: SandboxId,
    mut client: HostControlClient<T>,
    pending_refusal: Option<ExpectedGrpcStatus>,
) -> Result<InitialNetworkState, HydrateNetworkStateError>
where
    T: tonic::client::GrpcService<tonic::body::Body>,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    let mut request = tonic::Request::new(GetNetworkPolicyRequest {});
    if let Some(expected) = pending_refusal {
        expected.mark(&mut request);
    }
    let response = client
        .get_network_policy(request)
        .await
        .map_err(|source| HydrateNetworkStateError::GetNetworkPolicy { sandbox_id, source })?
        .into_inner();
    InitialNetworkState::from_host_control(response)
}

pub(crate) fn host_control_policy_loader(
    sandbox_id: SandboxId,
    stats: Arc<ProxyStats>,
    owner_contacted: tokio::sync::watch::Receiver<bool>,
) -> PolicyLoader {
    let launched_at = tokio::time::Instant::now();
    Arc::new(move || {
        let stats = stats.clone();
        let wait = RegistrationWait {
            launched_at,
            owner_contacted: owner_contacted.clone(),
            jitter: jittered,
        };
        Box::pin(async move {
            let pulled = pull_initial_network_state(sandbox_id, wait).await?;
            stats.merge_persisted_network_bytes(pulled.network_tx_bytes, pulled.network_rx_bytes);
            Ok(pulled.into_policy())
        })
    })
}

impl InitialNetworkState {
    fn from_host_control(
        response: GetNetworkPolicyResponse,
    ) -> Result<Self, HydrateNetworkStateError> {
        let policy = network_policy_from_host_control_proto(response.effective_policy.as_ref())?;
        let network_credentials =
            NetworkCredentialSnapshot::from_wire(response.network_credentials, &policy)
                .map_err(NetworkPolicyFromHostControlError::InvalidNetworkCredentials)?;
        let policy_ref = response.effective_policy_ref.parse().map_err(|source| {
            HydrateNetworkStateError::InvalidReference {
                reference: response.effective_policy_ref,
                source,
            }
        })?;
        Ok(Self {
            policy,
            network_credentials,
            policy_ref,
            network_tx_bytes: response.network_tx_bytes,
            network_rx_bytes: response.network_rx_bytes,
        })
    }

    fn into_policy(self) -> PulledNetworkPolicy {
        PulledNetworkPolicy {
            policy: self.policy,
            policy_ref: self.policy_ref,
            network_credentials: self.network_credentials,
        }
    }
}

#[cfg(test)]
mod tests;
