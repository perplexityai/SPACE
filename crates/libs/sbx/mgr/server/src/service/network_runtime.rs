use super::*;
use pplx_space_common_core::types::SpaceApiRouteIdentity;
use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventType, emit_sandbox_event,
};
use pplx_space_metrics::gateway::readiness::{
    self as readiness_metrics, Outcome as ReadinessOutcome,
};
use pplx_space_metrics::sandbox::worker::{
    NetGatewayRouteSecurityPushOperation as Operation,
    NetGatewayRouteSecurityPushOutcome as Outcome, net_gateway_route_security_push,
};
use pplx_space_net_common::{
    DomainName, EffectiveNetworkPolicyRef, NetworkPolicy, NetworkPolicyValidationError,
    effective_network_policy_refs_match,
};

const MAX_KNOWN_EGRESS_DOMAINS: usize = 64;

/// Comma-separated domain names, used when no assignment resolves. Without it a deployment that configures no flag provider cannot
/// populate the list by any means.
const KNOWN_EGRESS_DOMAINS_ENV: &str = "SPACE_NET_GATEWAY_KNOWN_EGRESS_DOMAINS";

/// The cap both sources share. A provider assignment and the local list differ
/// only in how they are decoded — by the time either has a `Vec<DomainName>`
/// the limit is one rule, so it is written once and wrapped by both errors.
#[derive(Debug, thiserror::Error)]
#[error("domain count {count} exceeds {MAX_KNOWN_EGRESS_DOMAINS}")]
struct TooManyDomains {
    count: usize,
}

#[derive(Debug, thiserror::Error)]
enum KnownEgressAssignmentError {
    #[error(
        "expected exact domain names as an array, as {{\"domains\": [...]}}, or as that array's JSON text: {0}"
    )]
    InvalidDomains(#[source] serde_json::Error),
    #[error(transparent)]
    TooManyDomains(#[from] TooManyDomains),
}

#[derive(Debug, thiserror::Error)]
enum KnownEgressLocalError {
    #[error("{KNOWN_EGRESS_DOMAINS_ENV} entry {entry:?} is not a domain name: {source}")]
    InvalidDomain {
        entry: String,
        #[source]
        source: NetworkPolicyValidationError,
    },
    #[error(transparent)]
    TooManyDomains(#[from] TooManyDomains),
}

fn within_known_egress_cap(domains: Vec<DomainName>) -> Result<Vec<DomainName>, TooManyDomains> {
    let count = domains.len();
    if count > MAX_KNOWN_EGRESS_DOMAINS {
        return Err(TooManyDomains { count });
    }
    Ok(domains)
}

/// The deploy-time half of the known-egress configuration: the list that
/// applies when no assignment resolves.
///
/// Read once per process. These are launch arguments rather than live
/// configuration — an existing gateway only picks a change up when it is
/// cycled — so re-reading the environment per launch would buy nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct KnownEgressConfig {
    domains: Vec<DomainName>,
    /// Set when the local list was configured but rejected.
    ///
    /// Both a rejected list and an unset one leave `domains` empty, and only
    /// the first is a misconfiguration to go and fix. Without this the two are
    /// indistinguishable from outside the process, so a typo reads as "no
    /// domains configured" — which is also the default, and therefore looks
    /// deliberate.
    local_list_rejected: bool,
}

impl KnownEgressConfig {
    /// Parse from the value the caller already read.
    ///
    /// Split from [`Self::from_env`] so the list parsing is testable without
    /// mutating the process environment, which every test in this binary
    /// shares.
    fn from_values(domains: Option<&str>) -> Self {
        let (domains, local_list_rejected) = match local_known_egress_domains(domains) {
            Ok(domains) => (domains, false),
            Err(error) => {
                // Same all-or-nothing answer the assignment path gives, and for
                // the same reason: a partially applied domain list is a list
                // nobody configured, and silently dropping one entry hides a
                // deploy-time typo behind diagnostics that look healthy.
                tracing::error!(
                    %error,
                    env = KNOWN_EGRESS_DOMAINS_ENV,
                    "invalid local known-egress domain list; ignoring all of it"
                );
                (Vec::new(), true)
            }
        };
        Self {
            domains,
            local_list_rejected,
        }
    }

    fn from_env() -> Self {
        Self::from_values(std::env::var(KNOWN_EGRESS_DOMAINS_ENV).ok().as_deref())
    }

    /// The process-wide configuration, parsed on first use.
    ///
    /// Process-global on purpose, and that makes it useless to a test: service
    /// construction forces it, many unit tests construct a service, and the
    /// first one to arrive wins for the whole binary. A test that needs a
    /// particular configuration passes a [`KnownEgressConfig`] to
    /// [`net_gateway_launch_config_with`] instead of setting the environment.
    fn current() -> &'static Self {
        static CONFIG: std::sync::OnceLock<KnownEgressConfig> = std::sync::OnceLock::new();
        CONFIG.get_or_init(Self::from_env)
    }
}

/// Resolve the known-egress configuration during startup and state what it
/// resolved to.
///
/// [`KnownEgressConfig::current`] is lazy, and its only other caller is a
/// gateway launch — so without this the env parse, and the error a rejected
/// list logs, would first happen on whichever sandbox create came first. An
/// operator who mistyped the list would see a clean rollout, then one error
/// line buried in the traffic of an unrelated request, and silence afterwards.
///
/// The log is what makes the states tellable apart from outside the process:
/// nothing configured, a list in use, and a list that was thrown away.
pub(crate) fn log_known_egress_configuration() {
    let config = KnownEgressConfig::current();
    tracing::info!(
        env_domains = KNOWN_EGRESS_DOMAINS_ENV,
        local_domains = config.domains.len(),
        local_list_rejected = config.local_list_rejected,
        "resolved net-gateway known-egress diagnostics configuration"
    );
}

/// Parse the local list through the same `DomainName` validation an assignment
/// goes through: `DomainName`'s `Deserialize` delegates to its `FromStr`, so a
/// *name* a provider assignment would reject is rejected here too.
///
/// The *framing* is this caller's, and only the framing: splitting on commas
/// and dropping empty members is what lets `"a.test, b.test"` and a trailing
/// comma through, where `[""]` would fail as an assignment. Trimming is not
/// the difference — `normalize_domain` trims either way
/// (`pplx-space-net-common`), so `[" a.test "]` is accepted as an assignment
/// too.
fn local_known_egress_domains(raw: Option<&str>) -> Result<Vec<DomainName>, KnownEgressLocalError> {
    let domains = raw
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            entry
                .parse::<DomainName>()
                .map_err(|source| KnownEgressLocalError::InvalidDomain {
                    entry: entry.to_string(),
                    source,
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(within_known_egress_cap(domains)?)
}

/// Resolve the domains one gateway launch should carry.
///
/// The provider assignment wins when it resolves; with nothing resolved the
/// deployment's own list stands.
fn known_egress_domains(
    feature_flags: &pplx_space_feature_flags::FeatureFlagClient,
    sandbox_id: &str,
    config: &KnownEgressConfig,
) -> Vec<DomainName> {
    use pplx_space_feature_flags::policy::Override;

    let flag = pplx_space_feature_flags::SPACE_NET_GATEWAY_KNOWN_EGRESS_DOMAINS_FLAG;
    match feature_flags.json_override(true, flag, sandbox_id) {
        // A rejected assignment falls back to the local list, not to nothing.
        // `JsonFlag` declares that contract — "a read that resolves nothing and
        // a read that resolves something the caller rejects both land on that
        // caller's own local configuration" — and the sibling quota-override
        // caller already honours it. Discarding the local list here would let a
        // malformed document in a console this deployment may not own silently
        // kill diagnostics the deployment configured for itself.
        //
        // That is not in tension with a rejected *local* list yielding nothing:
        // each source is all-or-nothing, and the local list is simply the last
        // one there is.
        Override::Resolved(assignment) => known_egress_domains_from_assignment(assignment)
            .unwrap_or_else(|error| {
                tracing::warn!(
                    sandbox_id,
                    %error,
                    flag = flag.key(),
                    local_domains = config.domains.len(),
                    "invalid known-egress domain assignment; falling back to the local list"
                );
                config.domains.clone()
            }),
        Override::Local => config.domains.clone(),
        Override::Disabled => Vec::new(),
    }
}

/// Accept the assignment as a bare array, or as an object carrying one under
/// `domains`.
///
/// The bare array preserves compatibility with direct JSON assignments.
/// OpenFeature's JSON resolution returns a structure, so providers behind that
/// adapter need the object form to carry a list.
///
/// The object is necessary for an OpenFeature backend but not sufficient for
/// flagd specifically: flagd 0.2.2's in-process resolvers stringify any nested
/// array on the way out, so a populated list arrives as the *text*
/// `["a.com"]` rather than as an array. `domains` is therefore also accepted
/// as a JSON-encoded string.
///
/// That last shape is a workaround for an upstream defect, and it is confined
/// here on purpose: this is the one place that knows `domains` must be a list
/// of domain names, so decoding a string found there is a typed decision
/// rather than a guess. The adapter could not do the same — to it every
/// stringified member is indistinguishable from a flag whose author meant a
/// string. Pinned upstream by
/// `pplx-space-feature-flags`'s `tests/flagd_end_to_end.rs`; when flagd stops
/// stringifying, that test fails and this arm can go.
fn known_egress_domains_from_assignment(
    assignment: serde_json::Value,
) -> Result<Vec<DomainName>, KnownEgressAssignmentError> {
    let assignment = match assignment {
        serde_json::Value::Object(mut fields) => match fields.remove("domains") {
            // A string here is only ever flagd's stringified array. If it does
            // not parse, hand the original back so the error below reports the
            // value the operator actually wrote.
            // An empty string is the operator clearing the value, not a
            // stringified array that failed to parse.
            Some(serde_json::Value::String(encoded)) if encoded.trim().is_empty() => {
                serde_json::Value::Array(Vec::new())
            }
            Some(serde_json::Value::String(encoded)) => {
                serde_json::from_str(&encoded).unwrap_or(serde_json::Value::String(encoded))
            }
            // An explicit null under the key is an operator clearing the list.
            Some(serde_json::Value::Null) => serde_json::Value::Array(Vec::new()),
            Some(domains) => domains,
            // An object with no keys at all is how an OpenFeature backend
            // spells "nothing configured": it resolves a *structure*, so there
            // is no absent value for it to hand back. Reading that as an error
            // would make clearing the list in flagd reinstate the deployment's
            // local one, which is the opposite of what the operator asked for.
            //
            // An object with *other* keys is a different thing — a misspelled
            // envelope, or another flag's document pointed here by mistake —
            // and still has to fail rather than read as "no domains".
            None if fields.is_empty() => serde_json::Value::Array(Vec::new()),
            None => serde_json::Value::Object(fields),
        },
        // A top-level null is the fourth spelling of "cleared". Eppo's
        // `get_json_assignment` can return null. Without this arm it fails the
        // array parse and reinstates the local list, which is the opposite of
        // what clearing the flag asks for.
        serde_json::Value::Null => serde_json::Value::Array(Vec::new()),
        assignment => assignment,
    };
    let domains = serde_json::from_value::<Vec<DomainName>>(assignment)
        .map_err(KnownEgressAssignmentError::InvalidDomains)?;
    Ok(within_known_egress_cap(domains)?)
}

#[derive(Debug, thiserror::Error)]
pub(super) enum NetworkPolicyRefreshError {
    #[error(transparent)]
    DatabaseMutexPoisoned(#[from] crate::node_state_sync::DatabaseMutexPoisoned),
    #[error("failed to refresh network policy: {0}")]
    Abandoned(RefreshAbandonReason),
    #[error("failed to refresh network policy: network-readiness deadline exceeded: {0}")]
    Retryable(#[source] NetworkPolicyRefreshAttemptError),
}

impl NetworkPolicyRefreshError {
    pub(super) fn is_abandoned(&self) -> bool {
        matches!(self, Self::Abandoned(_))
    }

    fn event_detail(&self) -> String {
        match self {
            Self::DatabaseMutexPoisoned(error) => error.to_string(),
            Self::Abandoned(reason) => reason.to_string(),
            Self::Retryable(error) => error.to_string(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum NetworkPolicyRefreshAttemptError {
    #[error(
        "RefreshNetworkPolicy returned state={state} installed_policy_ref={installed_policy_ref} expected_policy_ref={expected_policy_ref} last_error_kind={last_error_kind} last_error={last_error}"
    )]
    NotConverged {
        state: pplx_space_net_gateway_client::proto::NetworkState,
        installed_policy_ref: String,
        expected_policy_ref: String,
        last_error_kind: pplx_space_net_gateway_client::proto::NetworkPolicyErrorKind,
        last_error: String,
    },
    #[error("RefreshNetworkPolicy failed: {0}")]
    Rpc(#[source] Status),
    #[error("netgtw connect failed: {0}")]
    Connect(#[source] pplx_space_net_gateway_client::ConnectError),
    #[error("netgtw unavailable")]
    Unavailable,
}

pub(super) fn network_policy_refresh_error_to_status(error: NetworkPolicyRefreshError) -> Status {
    let code = match &error {
        NetworkPolicyRefreshError::DatabaseMutexPoisoned(_) => tonic::Code::Internal,
        NetworkPolicyRefreshError::Abandoned(_) => tonic::Code::FailedPrecondition,
        NetworkPolicyRefreshError::Retryable(_) => tonic::Code::Unavailable,
    };
    Status::new(code, error.to_string())
}

/// Resolved for one process launch, never persisted or applied to a live gateway.
#[derive(Debug, Default)]
pub(crate) struct NetGatewayLaunchConfig {
    known_egress_domains: Vec<DomainName>,
}

pub(crate) fn net_gateway_launch_config(
    feature_flags: &pplx_space_feature_flags::FeatureFlagClient,
    sandbox_id: &str,
) -> NetGatewayLaunchConfig {
    net_gateway_launch_config_with(feature_flags, sandbox_id, KnownEgressConfig::current())
}

/// [`net_gateway_launch_config`] against a supplied deploy-time configuration.
///
/// The public entry point differs only in reading that configuration from the
/// process environment, which a test cannot vary. Every launch path composes
/// the launch config exactly this way, so a test here covers what create,
/// start, and watchdog relaunch each hand to the gateway.
fn net_gateway_launch_config_with(
    feature_flags: &pplx_space_feature_flags::FeatureFlagClient,
    sandbox_id: &str,
    known_egress: &KnownEgressConfig,
) -> NetGatewayLaunchConfig {
    NetGatewayLaunchConfig {
        known_egress_domains: known_egress_domains(feature_flags, sandbox_id, known_egress),
    }
}

/// Build process arguments from already-resolved manager configuration.
pub(crate) fn netgtw_launch_args(
    sandbox_id: &str,
    no_identity: bool,
    route_identity: Option<&SpaceApiRouteIdentity>,
    config: &NetGatewayLaunchConfig,
) -> Vec<String> {
    let netgtw_socket = format!("{RUN_DIR}/{sandbox_id}/netgtw-control.sock");
    let mut args = vec![
        "--socket".to_string(),
        netgtw_socket,
        "--sandbox-id".to_string(),
        sandbox_id.to_string(),
    ];
    if no_identity {
        args.push("--no-identity".to_string());
    }
    if !no_identity {
        append_route_identity_launch_args(&mut args, route_identity);
    }
    for domain in &config.known_egress_domains {
        args.push("--known-egress-domain".to_string());
        args.push(domain.to_string());
    }
    args
}

fn append_route_identity_launch_args(
    args: &mut Vec<String>,
    route_identity: Option<&SpaceApiRouteIdentity>,
) {
    let Some(route_identity) = route_identity else {
        return;
    };
    if let Some(route_generation) = route_identity.route_generation {
        args.push("--route-generation".to_string());
        args.push(route_generation.get().to_string());
    }
    if let Some(current_node_id) = route_identity.current_node_id {
        args.push("--current-node-id".to_string());
        args.push(current_node_id.to_string());
    }
    let substrate_kind = route_identity.effective_substrate_kind();
    if let Some(substrate_kind) = substrate_kind {
        args.push("--substrate-kind".to_string());
        args.push(substrate_kind.to_string());
    }
    if let Some(execution_generation) = route_identity.execution_generation {
        args.push("--execution-generation".to_string());
        args.push(execution_generation.get().to_string());
    }
    if let Some(pod_uid) = route_identity.pod_uid.as_ref() {
        args.push("--pod-uid".to_string());
        args.push(pod_uid.to_string());
    }
    if let Some(activation_attempt_id) = route_identity.activation_attempt_id {
        args.push("--activation-attempt-id".to_string());
        args.push(activation_attempt_id.to_string());
    }
}

pub(super) async fn launch_net_gateway_via_node(
    node_client: &mut NodeManagerGrpcClient,
    execution_fence: pplx_space_node_mgr_client::execution_fence::ExecutionFence,
    sandbox_id: &str,
    uid: u32,
    no_identity: bool,
    route_identity: Option<&SpaceApiRouteIdentity>,
    config: &NetGatewayLaunchConfig,
) -> Result<pplx_space_node_mgr_client::proto::LaunchResponse, Status> {
    let args = netgtw_launch_args(sandbox_id, no_identity, route_identity, config);
    let process_uid = super::gateway_identity::launch_uid(uid)?;
    let span = tracing::info_span!("sbxmgr.start.node.launch_net_gateway", sandbox_id);
    let mut env = crate::sidecar_env::net_gateway_env();
    // The gateway's startup links to this launch; see its `startup_span`.
    if let Some(traceparent) =
        pplx_space_observability::trace_propagation::traceparent_for_span(&span)
    {
        env.insert(
            crate::sidecar_env::LAUNCH_TRACEPARENT_ENV.to_string(),
            traceparent,
        );
    }
    node_client
        .launch(LaunchRequest {
            execution_fence: Some(execution_fence.into()),
            sandbox_id: sandbox_id.to_string(),
            program: pplx_space_common_core::host::linux::paths::NET_GATEWAY_SVC_BIN.to_string(),
            args,
            uid: process_uid,
            gid: process_uid,
            env,
        })
        .instrument(span)
        .await
        .map(Response::into_inner)
        .map_err(|e| Status::internal(format!("net-gateway launch failed: {e}")))
}

/// Await a net-gateway launch and persist the resulting PID registration
/// before returning.
///
/// The gateway process begins pulling its network policy from host-control
/// the moment it starts, and host-control authorizes that caller by matching
/// the peer (pid, uid) against the registration this writes. Launch flows run
/// the gateway launch concurrently with slower work (the Firecracker jailer
/// launch, rootfs prepare), so the registration must become visible as soon
/// as the gateway arm completes — persisting it after the join leaves the
/// gateway racing its own authorization for the entire duration of the
/// slower sibling. Callers must wrap the gateway launch future in this
/// helper instead of writing the PID after the join.
///
/// A registration-write failure is returned as the gateway arm's error, so
/// call sites handle it exactly like a gateway launch failure (cleanup and
/// mark-failed). There is no await point between the launch resolving and
/// the write, so task cancellation cannot observe a launched-but-unregistered
/// gateway from this helper.
pub(super) async fn register_net_gateway_launch<N>(
    db: &crate::db::SharedConnection,
    sandbox_id: &str,
    uid: u32,
    launch_netgtw: N,
) -> Result<pplx_space_node_mgr_client::proto::LaunchResponse, Status>
where
    N: std::future::Future<
            Output = Result<pplx_space_node_mgr_client::proto::LaunchResponse, Status>,
        >,
{
    let response = launch_netgtw.await?;
    {
        let db = crate::node_state_sync::connection_or_signal_failure(db)
            .map_err(|error| Status::internal(error.to_string()))?;
        crate::db::update_netgtw_pid(&db, sandbox_id, uid, response.pid)
            .map_err(|e| Status::internal(format!("db net-gateway update failed: {e}")))?;
    }
    Ok(response)
}

/// Wait at most 60s for Ready and the requested policy ref, including connection,
/// RPCs and backoff. A shorter caller deadline still wins. Channels are reused
/// between polls; the gateway owns all configured readiness dependencies.
pub(super) async fn refresh_network_policy(
    db: &crate::db::SharedConnection,
    sandbox_id: &str,
    expected_policy_ref: Option<&EffectiveNetworkPolicyRef>,
    reason: &'static str,
) -> Result<(), NetworkPolicyRefreshError> {
    let client = pplx_space_spaced_common::LazyClient::new();
    refresh_network_policy_with_request(db, sandbox_id, expected_policy_ref, reason, |request| {
        let client = &client;
        async move {
            client
                .get_or_connect(|| pplx_space_net_gateway_client::connect(sandbox_id))
                .await
                .map_err(NetworkPolicyRefreshAttemptError::Connect)?
                .refresh_network_policy(request)
                .await
                .map(Response::into_inner)
                .map_err(NetworkPolicyRefreshAttemptError::Rpc)
        }
    })
    .await
}

pub(super) async fn refresh_network_policy_with_request<F, Fut>(
    db: &crate::db::SharedConnection,
    sandbox_id: &str,
    expected_policy_ref: Option<&EffectiveNetworkPolicyRef>,
    reason: &'static str,
    mut refresh: F,
) -> Result<(), NetworkPolicyRefreshError>
where
    F: FnMut(pplx_space_net_gateway_client::proto::RefreshNetworkPolicyRequest) -> Fut,
    Fut: std::future::Future<
            Output = Result<
                pplx_space_net_gateway_client::proto::RefreshNetworkPolicyResponse,
                NetworkPolicyRefreshAttemptError,
            >,
        >,
{
    let mut observation = readiness_metrics::manager_wait(reason);
    let deadline = tokio::time::Instant::now() + NETWORK_GATEWAY_READINESS_TIMEOUT;
    // An empty expected ref tells the net-gateway (and the client-side match
    // below) to accept whatever the current effective policy is once the
    // gateway reaches Ready, rather than asserting a specific ref. The cold
    // start gate uses this so a SetNetworkPolicy / template change landing
    // mid-start can't surface as a policy_ref_mismatch and fail the start.
    let request = pplx_space_net_gateway_client::proto::RefreshNetworkPolicyRequest {
        expected_policy_ref: expected_policy_ref
            .map(ToString::to_string)
            .unwrap_or_default(),
        reason: reason.to_string(),
    };
    emit_sandbox_event(
        sandbox_id,
        SandboxEventType::NetworkPolicyRefreshStarted,
        SandboxEventSource::SbxManager,
        SandboxEventDetails::default()
            .reason(reason)
            .expected_network_policy_ref(request.expected_policy_ref.clone()),
    );

    let mut last_error = None;
    let mut abandon_reason = None;
    // Keep the fast initial retries, then poll at the capped interval until the
    // overall deadline instead of exhausting attempts before startup completes.
    for delay in NETWORK_POLICY_PUSH_RETRY_DELAYS
        .into_iter()
        .chain(std::iter::repeat(Duration::from_secs(10)))
    {
        tokio::select! {
            biased;
            () = tokio::time::sleep_until(deadline) => break,
            () = tokio::time::sleep(delay) => {}
        }
        // A refresh can outlive its sandbox: the delete path tears down the
        // gateway and its control socket while this loop is mid-schedule, and
        // each connect below then burns its full multi-second retry budget
        // against a socket that can never come back. Re-read the row so a
        // teardown landing mid-schedule stops the loop at the next iteration.
        if let Some(reason) = refresh_abandon_reason(db, sandbox_id)? {
            abandon_reason = Some(reason);
            break;
        }
        let response = tokio::select! {
            biased;
            () = tokio::time::sleep_until(deadline) => break,
            response = refresh(request.clone()) => response,
        };
        // A lifecycle transition can race the RPC. Do not publish
        // readiness for an activation that is no longer current.
        if let Some(reason) = refresh_abandon_reason(db, sandbox_id)? {
            abandon_reason = Some(reason);
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        match response {
            Ok(response) => {
                let state =
                    pplx_space_net_gateway_client::proto::NetworkState::try_from(response.state)
                        .unwrap_or(pplx_space_net_gateway_client::proto::NetworkState::Unspecified);
                if state == pplx_space_net_gateway_client::proto::NetworkState::Ready
                    && effective_network_policy_refs_match(
                        &response.installed_policy_ref,
                        &request.expected_policy_ref,
                    )
                {
                    observation.outcome = ReadinessOutcome::Ready;
                    pplx_space_metrics::reconcile::network_policy_push("ok");
                    // Ready + matching ref is the whole start gate. The
                    // gateway reports TLS-interception dependency readiness
                    // separately; when it is not ready the sandbox still
                    // starts and guest egress fails closed per connection
                    // until the CA arrives. Surface it, never
                    // fail on it.
                    let interception_readiness =
                        pplx_space_net_gateway_client::proto::InterceptionReadiness::try_from(
                            response.interception_readiness,
                        )
                        .unwrap_or(
                            pplx_space_net_gateway_client::proto::InterceptionReadiness::Unspecified,
                        );
                    if interception_readiness.blocks_admission() {
                        pplx_space_metrics::reconcile::network_policy_interception_not_ready(
                            reason,
                        );
                        tracing::warn!(
                            sandbox_id,
                            expected_policy_ref = %request.expected_policy_ref,
                            interception_readiness = interception_readiness.as_str(),
                            "network policy refreshed in net-gateway; TLS interception dependencies not ready, guest egress fails closed until they are"
                        );
                    } else {
                        tracing::info!(
                            sandbox_id,
                            expected_policy_ref = %request.expected_policy_ref,
                            interception_readiness = interception_readiness.as_str(),
                            "network policy refreshed in net-gateway"
                        );
                    }
                    emit_sandbox_event(
                        sandbox_id,
                        SandboxEventType::NetworkPolicyRefreshCompleted,
                        SandboxEventSource::SbxManager,
                        SandboxEventDetails::default()
                            .reason(reason)
                            .network_state(state.to_string())
                            .interception_readiness(interception_readiness.as_str())
                            .expected_network_policy_ref(request.expected_policy_ref.clone())
                            .installed_network_policy_ref(response.installed_policy_ref),
                    );
                    return Ok(());
                }
                let last_error_kind =
                    pplx_space_net_gateway_client::proto::NetworkPolicyErrorKind::try_from(
                        response.last_error_kind,
                    )
                    .unwrap_or(pplx_space_net_gateway_client::proto::NetworkPolicyErrorKind::None);
                last_error = Some(NetworkPolicyRefreshAttemptError::NotConverged {
                    state,
                    installed_policy_ref: response.installed_policy_ref,
                    expected_policy_ref: request.expected_policy_ref.clone(),
                    last_error_kind,
                    last_error: response.last_error,
                });
            }
            Err(error) => {
                last_error = Some(error);
            }
        }
    }

    pplx_space_metrics::reconcile::network_policy_push("error");
    let error = if let Some(reason) = abandon_reason {
        observation.outcome = ReadinessOutcome::Abandoned;
        NetworkPolicyRefreshError::Abandoned(reason)
    } else {
        observation.outcome = ReadinessOutcome::TimedOut;
        NetworkPolicyRefreshError::Retryable(
            last_error.unwrap_or(NetworkPolicyRefreshAttemptError::Unavailable),
        )
    };
    emit_sandbox_event(
        sandbox_id,
        SandboxEventType::NetworkPolicyRefreshFailed,
        SandboxEventSource::SbxManager,
        SandboxEventDetails::default()
            .reason(reason)
            .expected_network_policy_ref(request.expected_policy_ref)
            .error(error.event_detail()),
    );
    Err(error)
}

/// Why a policy refresh must stop retrying before finishing its schedule.
///
/// Both variants mean the net-gateway control socket can never become
/// connectable again, so the remaining connect attempts would only burn the
/// retry budget against a dead socket. Its error display is the single source
/// of truth for the log line and the `Status` failure message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(super) enum RefreshAbandonReason {
    /// The sandbox row is gone — the sandbox has been deleted.
    #[error("refresh abandoned: sandbox row no longer exists")]
    SandboxRowGone,
    /// The row's state has left the launch-through-teardown window in which a
    /// live, policy-pulling net-gateway can exist
    /// ([`SandboxState::can_authorize_net_gateway`]).
    #[error("refresh abandoned: sandbox state {0} cannot have a live net-gateway")]
    StateCannotAuthorizeGateway(SandboxState),
}

/// The reason a policy refresh should stop retrying, or `None` to keep going.
///
/// Abandons when the sandbox row is gone or its state has left the
/// launch-through-teardown window in which a live, policy-pulling net-gateway
/// can exist ([`SandboxState::can_authorize_net_gateway`]). A row read failure
/// keeps retrying: a transient DB error must not abandon a refresh the gateway
/// could still confirm.
fn refresh_abandon_reason(
    db: &crate::db::SharedConnection,
    sandbox_id: &str,
) -> Result<Option<RefreshAbandonReason>, crate::node_state_sync::DatabaseMutexPoisoned> {
    let db = crate::node_state_sync::connection_or_signal_failure(db)?;
    Ok(match crate::db::read::sandbox(&db, sandbox_id) {
        Ok(None) => Some(RefreshAbandonReason::SandboxRowGone),
        Ok(Some(sandbox)) if !sandbox.state.can_authorize_net_gateway() => Some(
            RefreshAbandonReason::StateCannotAuthorizeGateway(sandbox.state),
        ),
        Ok(Some(_)) => None,
        Err(error) => {
            tracing::debug!(
                sandbox_id,
                %error,
                "sandbox row read failed during network policy refresh; continuing"
            );
            None
        }
    })
}

/// Nudge a freshly launched net-gateway to hydrate while the rest of the
/// activation proceeds.
///
/// This asserts no policy ref. The gateway pulls the current effective policy
/// from host-control, so a ref resolved by the caller can only match it or be
/// older. With a policy change committed after that resolution (a per-sandbox
/// override or a template policy update), an asserted ref would make a poll
/// force a re-pull that the gateway rejects as a mismatch, clearing its
/// installed policy until something re-pulls. Later policy changes are pushed
/// by their own write path and the reconciler, not by this refresh.
pub(super) fn spawn_network_policy_refresh(
    db: crate::db::SharedConnection,
    sandbox_id: String,
    reason: &'static str,
) {
    let span_sandbox_id = sandbox_id.clone();
    tokio::spawn(
        async move {
            if let Err(error) = refresh_network_policy(&db, &sandbox_id, None, reason).await {
                tracing::warn!(
                    sandbox_id,
                    reason,
                    %error,
                    "background net-gateway policy push failed"
                );
                super::drain::record_detached_failure(&error.to_string());
            }
        }
        .instrument(super::drain::detached_trace_span(
            "NetworkPolicyRefresh",
            &span_sandbox_id,
        )),
    );
}

pub(super) async fn push_route_identity(
    sandbox_id: &str,
    identity: &SpaceApiRouteIdentity,
) -> Result<NetGatewayRouteIdentityPushOutcome, Status> {
    let Some(identity_payload) = prepare_route_identity_push(sandbox_id, identity) else {
        return Ok(NetGatewayRouteIdentityPushOutcome::Absent);
    };
    let span = net_gateway_client_span("SetRouteIdentity", sandbox_id);
    let push = set_route_identity(identity_payload, sandbox_id, &span).await;
    finish_route_identity_push(sandbox_id, &span, push)
}

fn prepare_route_identity_push(
    sandbox_id: &str,
    identity: &SpaceApiRouteIdentity,
) -> Option<pplx_space_net_gateway_client::proto::SpaceApiRouteIdentity> {
    let Some(identity_payload) = route_identity_to_netgtw_proto(sandbox_id, identity) else {
        net_gateway_route_security_push(Operation::Identity, Outcome::Absent);
        tracing::debug!(sandbox_id, "no local route identity to push");
        return None;
    };
    Some(identity_payload)
}

fn finish_route_identity_push(
    sandbox_id: &str,
    span: &tracing::Span,
    push: Result<u32, NetGatewayRouteIdentityPushError>,
) -> Result<NetGatewayRouteIdentityPushOutcome, Status> {
    let rejection = push.as_ref().err().map(ToString::to_string);
    let classified = classify_route_identity_push(push);
    record_route_security_push_span(
        span,
        Operation::Identity,
        classified.as_ref().map(|outcome| outcome.metric_outcome()),
        rejection.as_deref(),
    );
    match classified {
        Ok(outcome) => {
            match outcome {
                NetGatewayRouteIdentityPushOutcome::Applied { .. } => {}
                NetGatewayRouteIdentityPushOutcome::Absent => {}
                NetGatewayRouteIdentityPushOutcome::Diverged => tracing::warn!(
                    sandbox_id,
                    rejection,
                    "net-gateway route identity is ahead of or conflicts with local state"
                ),
            }
            Ok(outcome)
        }
        Err(error) => Err(route_identity_push_status(error)),
    }
}

fn classify_route_identity_push(
    result: Result<u32, NetGatewayRouteIdentityPushError>,
) -> Result<NetGatewayRouteIdentityPushOutcome, NetGatewayRouteIdentityPushError> {
    match result {
        Ok(version) if version < pplx_space_net_gateway_client::IDENTITY_PROFILE_VERSION => Err(
            NetGatewayRouteIdentityPushError::UnsupportedProfile(version),
        ),
        Ok(identity_profile_version) => Ok(NetGatewayRouteIdentityPushOutcome::Applied {
            identity_profile_version,
        }),
        // Sandbox mismatch, a stale generation, and a same-generation conflict
        // are all persistent for an unchanged payload, so retrying the identical
        // identity cannot repair them and must not starve verification-key push.
        Err(NetGatewayRouteIdentityPushError::Rpc(error))
            if error.code() == tonic::Code::FailedPrecondition =>
        {
            Ok(NetGatewayRouteIdentityPushOutcome::Diverged)
        }
        Err(error) => Err(error),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NetGatewayRouteIdentityPushOutcome {
    Applied {
        identity_profile_version: u32,
    },
    /// There was no local route identity to send, so no RPC was made.
    Absent,
    /// The gateway holds route identity state this push cannot reconcile.
    Diverged,
}

impl NetGatewayRouteIdentityPushOutcome {
    pub(super) const fn identity_profile_version(self) -> Option<u32> {
        match self {
            Self::Applied {
                identity_profile_version,
            } => Some(identity_profile_version),
            Self::Absent | Self::Diverged => None,
        }
    }

    const fn metric_outcome(self) -> Outcome {
        match self {
            Self::Applied { .. } => Outcome::Applied,
            Self::Absent => Outcome::Absent,
            Self::Diverged => Outcome::Diverged,
        }
    }
}

async fn set_route_identity(
    identity: pplx_space_net_gateway_client::proto::SpaceApiRouteIdentity,
    sandbox_id: &str,
    span: &tracing::Span,
) -> Result<u32, NetGatewayRouteIdentityPushError> {
    let request = pplx_space_net_gateway_client::proto::SetRouteIdentityRequest {
        identity: Some(identity),
    };
    async {
        let mut client = pplx_space_net_gateway_client::connect(sandbox_id)
            .await
            .map_err(NetGatewayRouteIdentityPushError::Connect)?;
        set_route_identity_with_client(&mut client, request).await
    }
    .instrument(span.clone())
    .await
}

async fn set_route_identity_with_client(
    client: &mut pplx_space_net_gateway_client::NetGatewayControlGrpcClient,
    request: pplx_space_net_gateway_client::proto::SetRouteIdentityRequest,
) -> Result<u32, NetGatewayRouteIdentityPushError> {
    let response = client
        .set_route_identity(request)
        .await
        .map_err(NetGatewayRouteIdentityPushError::Rpc)?;
    Ok(response.into_inner().identity_profile_version)
}

pub(super) async fn push_route_verification_keys(
    sandbox_id: &str,
    identity_profile_version: Option<u32>,
    bundle: &pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519VerificationBundle,
) -> Result<NetGatewayRouteVerificationKeyPushOutcome, Status> {
    validate_route_verification_profile(identity_profile_version)?;
    let (expected_generation, request) = route_verification_keys_request(bundle);
    let span = net_gateway_client_span("SetRouteVerificationKeys", sandbox_id);
    span.record("expected_generation", expected_generation.get());
    let result = async {
        let mut client = pplx_space_net_gateway_client::connect(sandbox_id)
            .await
            .map_err(NetGatewayRouteVerificationKeyPushError::Connect)?;
        set_route_verification_keys_with_client(&mut client, request, expected_generation).await
    }
    .instrument(span.clone())
    .await;
    finish_route_verification_key_push(&span, result)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct NetGatewayRouteSecurityPushOutcome {
    pub(super) identity: NetGatewayRouteIdentityPushOutcome,
    pub(super) verification_keys: Option<NetGatewayRouteVerificationKeyPushOutcome>,
}

/// Publish the route identity and verification keys over one gateway connection.
/// Wake uses a shorter connection policy than background reconciliation because
/// this operation runs synchronously on the lifecycle request path.
pub(super) async fn push_route_security_with_retry_policy(
    sandbox_id: &str,
    identity: &SpaceApiRouteIdentity,
    verification_bundle: Option<
        &pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519VerificationBundle,
    >,
    retry_policy: pplx_space_common_grpc::GrpcRetryPolicy,
) -> Result<NetGatewayRouteSecurityPushOutcome, Status> {
    let Some(identity_payload) = prepare_route_identity_push(sandbox_id, identity) else {
        return Ok(NetGatewayRouteSecurityPushOutcome {
            identity: NetGatewayRouteIdentityPushOutcome::Absent,
            verification_keys: None,
        });
    };

    let identity_span = net_gateway_client_span("SetRouteIdentity", sandbox_id);
    let client = pplx_space_net_gateway_client::connect_with_retry_policy(sandbox_id, retry_policy);
    let mut client = match client.instrument(identity_span.clone()).await {
        Ok(client) => client,
        Err(error) => {
            return finish_route_identity_push(
                sandbox_id,
                &identity_span,
                Err(NetGatewayRouteIdentityPushError::Connect(error)),
            )
            .map(|identity| NetGatewayRouteSecurityPushOutcome {
                identity,
                verification_keys: None,
            });
        }
    };
    let request = pplx_space_net_gateway_client::proto::SetRouteIdentityRequest {
        identity: Some(identity_payload),
    };
    let push = set_route_identity_with_client(&mut client, request)
        .instrument(identity_span.clone())
        .await;
    let identity = finish_route_identity_push(sandbox_id, &identity_span, push)?;
    let verification_keys = match (verification_bundle, identity.identity_profile_version()) {
        (Some(bundle), Some(identity_profile_version)) => Some(
            push_route_verification_keys_with_client(
                &mut client,
                sandbox_id,
                Some(identity_profile_version),
                bundle,
            )
            .await?,
        ),
        (None, _) | (Some(_), None) => None,
    };
    Ok(NetGatewayRouteSecurityPushOutcome {
        identity,
        verification_keys,
    })
}

async fn push_route_verification_keys_with_client(
    client: &mut pplx_space_net_gateway_client::NetGatewayControlGrpcClient,
    sandbox_id: &str,
    identity_profile_version: Option<u32>,
    bundle: &pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519VerificationBundle,
) -> Result<NetGatewayRouteVerificationKeyPushOutcome, Status> {
    validate_route_verification_profile(identity_profile_version)?;
    let (expected_generation, request) = route_verification_keys_request(bundle);
    let span = net_gateway_client_span("SetRouteVerificationKeys", sandbox_id);
    span.record("expected_generation", expected_generation.get());
    let result = set_route_verification_keys_with_client(client, request, expected_generation)
        .instrument(span.clone())
        .await;
    finish_route_verification_key_push(&span, result)
}

fn route_verification_keys_request(
    bundle: &pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519VerificationBundle,
) -> (
    pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519VerificationGeneration,
    pplx_space_net_gateway_client::proto::SetRouteVerificationKeysRequest,
) {
    (
        bundle.generation,
        pplx_space_net_gateway_client::proto::SetRouteVerificationKeysRequest {
            generation: bundle.generation.get(),
            keys: bundle
                .keys
                .iter()
                .map(
                    |key| pplx_space_net_gateway_client::proto::RouteVerificationKey {
                        key_id: key.key_id.to_string(),
                        ed25519_public_key: key.public_key.clone(),
                    },
                )
                .collect(),
            issuer: bundle.issuer.to_string(),
            cluster_id: bundle.cluster_id.to_string(),
        },
    )
}

async fn set_route_verification_keys_with_client(
    client: &mut pplx_space_net_gateway_client::NetGatewayControlGrpcClient,
    request: pplx_space_net_gateway_client::proto::SetRouteVerificationKeysRequest,
    expected_generation: pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519VerificationGeneration,
) -> Result<NetGatewayRouteVerificationKeyPushOutcome, NetGatewayRouteVerificationKeyPushError> {
    classify_route_verification_key_response(
        client
            .set_route_verification_keys(request)
            .await
            .map(tonic::Response::into_inner),
        expected_generation,
    )
}

fn classify_route_verification_key_response(
    response: Result<
        pplx_space_net_gateway_client::proto::SetRouteVerificationKeysResponse,
        Status,
    >,
    expected_generation: pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519VerificationGeneration,
) -> Result<NetGatewayRouteVerificationKeyPushOutcome, NetGatewayRouteVerificationKeyPushError> {
    let applied_generation = response
        .map_err(NetGatewayRouteVerificationKeyPushError::Rpc)?
        .generation;
    if applied_generation != expected_generation.get() {
        return Err(
            NetGatewayRouteVerificationKeyPushError::GenerationMismatch {
                expected: expected_generation.get(),
                applied: applied_generation,
            },
        );
    }
    Ok(NetGatewayRouteVerificationKeyPushOutcome::Applied)
}

fn finish_route_verification_key_push(
    span: &tracing::Span,
    result: Result<
        NetGatewayRouteVerificationKeyPushOutcome,
        NetGatewayRouteVerificationKeyPushError,
    >,
) -> Result<NetGatewayRouteVerificationKeyPushOutcome, Status> {
    let failure = result.as_ref().err().map(ToString::to_string);
    record_route_security_push_span(
        span,
        Operation::VerificationKeys,
        result.as_ref().map(|outcome| outcome.metric_outcome()),
        failure.as_deref(),
    );
    result.map_err(route_verification_key_push_status)
}

/// The client span both route-security pushes share. Fields the two RPCs do
/// not both set are declared `Empty` so a single builder can serve both; an
/// `Empty` field that is never recorded is omitted from the emitted span.
fn net_gateway_client_span(rpc_method: &'static str, sandbox_id: &str) -> tracing::Span {
    tracing::info_span!(
        "net_gateway.route_security_push",
        otel.kind = "client",
        "operation.name" = "grpc.client",
        rpc.system = "grpc",
        rpc.service = "net_gateway.control",
        rpc.method = rpc_method,
        network.transport = "unix",
        sandbox_id,
        expected_generation = tracing::field::Empty,
        route_security.outcome = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
    )
}

/// Record the outcome of a route-security RPC on its client span and emit the
/// matching counter, so the span tag and the metric tag cannot drift when an
/// RPC was attempted. Pre-RPC outcomes emit only the counter because no client
/// span exists.
///
/// Identity divergence is a classified outcome; unsupported RPCs remain errors.
fn record_route_security_push_span<E>(
    span: &tracing::Span,
    operation: Operation,
    outcome: Result<Outcome, &E>,
    failure: Option<&str>,
) {
    let outcome = outcome.unwrap_or(Outcome::Failed);
    span.record("route_security.outcome", outcome.as_str());
    if outcome == Outcome::Failed {
        span.record("otel.status_code", "ERROR");
        if let Some(failure) = failure {
            span.record("otel.status_description", failure);
        }
    } else {
        span.record("otel.status_code", "OK");
    }
    net_gateway_route_security_push(operation, outcome);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NetGatewayRouteVerificationKeyPushOutcome {
    Applied,
}

impl NetGatewayRouteVerificationKeyPushOutcome {
    const fn metric_outcome(
        self,
    ) -> pplx_space_metrics::sandbox::worker::NetGatewayRouteSecurityPushOutcome {
        use pplx_space_metrics::sandbox::worker::NetGatewayRouteSecurityPushOutcome as Outcome;
        match self {
            Self::Applied => Outcome::Applied,
        }
    }
}

/// An unknown profile comes from a failed identity update, so still probe the
/// key RPC. A known pre-verification gateway must be replaced, not acknowledged.
fn validate_route_verification_profile(
    identity_profile_version: Option<u32>,
) -> Result<(), Status> {
    if identity_profile_version.is_some_and(|version| {
        version < pplx_space_net_gateway_client::MIN_IDENTITY_PROFILE_VERSION_FOR_VERIFICATION_KEYS
    }) {
        return Err(Status::failed_precondition(
            "net-gateway identity profile cannot accept verification keys; cycle the gateway",
        ));
    }
    Ok(())
}

fn route_verification_key_push_status(error: NetGatewayRouteVerificationKeyPushError) -> Status {
    Status::internal(format!(
        "could not push route verification keys to netgtw: {error}"
    ))
}

fn route_identity_push_status(error: NetGatewayRouteIdentityPushError) -> Status {
    if let NetGatewayRouteIdentityPushError::UnsupportedProfile(version) = error {
        return Status::unimplemented(format!(
            "net-gateway identity profile {version} is unsupported; cycle the gateway"
        ));
    }
    if let NetGatewayRouteIdentityPushError::Rpc(status) = &error
        && status.code() == tonic::Code::Unimplemented
    {
        return Status::unimplemented(
            "net-gateway does not implement route identity synchronization; cycle the gateway",
        );
    }
    Status::internal(format!("could not push route identity to netgtw: {error}"))
}

#[derive(Debug, thiserror::Error)]
enum NetGatewayRouteIdentityPushError {
    #[error("netgtw identity profile {0} is unsupported")]
    UnsupportedProfile(u32),
    #[error("netgtw connect failed: {0}")]
    Connect(#[from] pplx_space_net_gateway_client::ConnectError),
    #[error("netgtw SetRouteIdentity failed: {0}")]
    Rpc(#[source] Status),
}

#[derive(Debug, thiserror::Error)]
enum NetGatewayRouteVerificationKeyPushError {
    #[error("netgtw connect failed: {0}")]
    Connect(#[from] pplx_space_net_gateway_client::ConnectError),
    #[error("netgtw SetRouteVerificationKeys failed: {0}")]
    Rpc(#[source] Status),
    #[error(
        "netgtw route verification key generation mismatch: expected={expected}, applied={applied}"
    )]
    GenerationMismatch { expected: u64, applied: u64 },
}

fn route_identity_to_netgtw_proto(
    sandbox_id: &str,
    identity: &SpaceApiRouteIdentity,
) -> Option<pplx_space_net_gateway_client::proto::SpaceApiRouteIdentity> {
    (!identity.is_absent()).then(
        || pplx_space_net_gateway_client::proto::SpaceApiRouteIdentity {
            sandbox_id: Some(sandbox_id.to_string()),
            route_generation: identity.route_generation.map(|value| value.get()),
            current_node_id: identity.current_node_id.map(|value| value.to_string()),
            substrate_kind: match identity.effective_substrate_kind() {
                Some(pplx_space_common_core::types::NodeManagedBy::Space) => {
                    pplx_space_net_gateway_client::proto::SandboxSubstrateKind::Direct as i32
                }
                Some(pplx_space_common_core::types::NodeManagedBy::Kubernetes) => {
                    pplx_space_net_gateway_client::proto::SandboxSubstrateKind::Kubernetes as i32
                }
                None => {
                    pplx_space_net_gateway_client::proto::SandboxSubstrateKind::Unspecified as i32
                }
            },
            execution_generation: identity.execution_generation.map(|value| value.get()),
            pod_uid: identity.pod_uid.as_ref().map(ToString::to_string),
            activation_attempt_id: identity
                .activation_attempt_id
                .map(|value| value.to_string()),
        },
    )
}

pub(super) async fn set_network_no_identity(
    sandbox_id: &str,
    reason: &'static str,
) -> Result<(), Status> {
    let event_details = || {
        SandboxEventDetails::default()
            .reason(reason)
            .operation("set_no_identity")
            .expected_network_policy_ref(EffectiveNetworkPolicyRef::no_identity().to_string())
    };
    emit_sandbox_event(
        sandbox_id,
        SandboxEventType::NetworkPolicyEnforcementStarted,
        SandboxEventSource::SbxManager,
        event_details(),
    );
    let request = pplx_space_net_gateway_client::proto::SetNoIdentityRequest {
        reason: reason.to_string(),
    };
    let mut client = pplx_space_net_gateway_client::connect(sandbox_id)
        .await
        .map_err(|error| {
            emit_sandbox_event(
                sandbox_id,
                SandboxEventType::NetworkPolicyEnforcementFailed,
                SandboxEventSource::SbxManager,
                event_details().error(error.to_string()),
            );
            Status::internal(format!("netgtw connect failed: {error}"))
        })?;
    let response = client
        .set_no_identity(request)
        .await
        .map_err(|error| {
            emit_sandbox_event(
                sandbox_id,
                SandboxEventType::NetworkPolicyEnforcementFailed,
                SandboxEventSource::SbxManager,
                event_details().error(error.to_string()),
            );
            Status::internal(format!("SetNoIdentity failed: {error}"))
        })?
        .into_inner();
    let state = pplx_space_net_gateway_client::proto::NetworkState::try_from(response.state)
        .unwrap_or(pplx_space_net_gateway_client::proto::NetworkState::Unspecified);
    if state == pplx_space_net_gateway_client::proto::NetworkState::NoIdentity {
        emit_sandbox_event(
            sandbox_id,
            SandboxEventType::NetworkPolicyEnforcementCompleted,
            SandboxEventSource::SbxManager,
            event_details()
                .network_state(state.to_string())
                .installed_network_policy_ref(response.installed_policy_ref)
                .result("no_identity"),
        );
        Ok(())
    } else {
        emit_sandbox_event(
            sandbox_id,
            SandboxEventType::NetworkPolicyEnforcementFailed,
            SandboxEventSource::SbxManager,
            event_details()
                .network_state(state.to_string())
                .installed_network_policy_ref(response.installed_policy_ref.clone())
                .error(response.last_error.clone()),
        );
        Err(Status::internal(format!(
            "SetNoIdentity returned state={state} installed_policy_ref={} last_error={}",
            response.installed_policy_ref, response.last_error
        )))
    }
}

pub(super) struct FetchedNetworkPolicy {
    pub policy: NetworkPolicy,
    pub installed_policy_ref: String,
}

pub(super) async fn fetch_network_policy(sandbox_id: &str) -> Result<FetchedNetworkPolicy, Status> {
    let mut client = pplx_space_net_gateway_client::connect(sandbox_id)
        .await
        .map_err(|error| Status::internal(format!("netgtw connect failed: {error}")))?;
    let response = client
        .get_network_policy(pplx_space_net_gateway_client::proto::GetNetworkPolicyRequest {})
        .await
        .map_err(|error| Status::internal(format!("GetNetworkPolicy failed: {error}")))?
        .into_inner();
    let policy = response
        .policy
        .as_ref()
        .ok_or_else(|| Status::internal("netgtw GetNetworkPolicy returned no policy"))
        .and_then(network_policy_from_proto)?;
    Ok(FetchedNetworkPolicy {
        policy,
        installed_policy_ref: response.installed_policy_ref,
    })
}

/// A local override created without a generation starts its lineage at 1. A
/// cluster override keeps the caller's generation, including 0: a cluster
/// create may seed an inherited sandbox with its template's effective policy
/// at 0, below the sandbox's own lineage, and the first reconcile, at the
/// sandbox's generation (never below 1), must replace that seed with
/// inheritance rather than conflict with it.
pub(super) fn create_network_policy_generation(
    network_policy_override: Option<&NetworkPolicy>,
    requested_generation: u64,
    authority: crate::db::PolicyAuthority,
) -> u64 {
    if network_policy_override.is_some()
        && requested_generation == 0
        && authority == crate::db::PolicyAuthority::Local
    {
        1
    } else {
        requested_generation
    }
}

pub(super) fn create_network_policy_authority(
    network_policy_override: Option<&NetworkPolicy>,
    requested_authority: i32,
    inherited_authority: crate::db::PolicyAuthority,
) -> Result<crate::db::PolicyAuthority, Status> {
    if network_policy_override.is_none() {
        return Ok(inherited_authority);
    }

    crate::db::PolicyAuthority::try_from(requested_authority)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

pub(super) fn set_network_policy_desired_generation(
    sandbox: &crate::db::SandboxRow,
    policy_override: &Option<NetworkPolicy>,
    requested_generation: u64,
) -> u64 {
    if requested_generation != 0 {
        return requested_generation;
    }

    if !sandbox.network_policy_override_is_corrupt_fallback
        && sandbox.network_policy_override.as_ref() == policy_override.as_ref()
    {
        sandbox.network_policy_generation
    } else {
        sandbox.network_policy_generation.saturating_add(1).max(1)
    }
}

/// Reject `authority=local` from a forwarded service peer. `Local` is a
/// node-administrator-owned policy, used for normal standalone operation and
/// cluster overrides (a later `authority=cluster` reconcile is a no-op).
/// Only an on-node operator may set it; the node-api and
/// node-ctl service peers always push `authority=cluster`. Delegates to the
/// shared boundary in `common/svc` so sandbox-manager and template-manager stay
/// in lockstep.
pub(super) fn require_local_authority_peer(
    principal: UdsPeerPrincipal,
    authority: crate::db::PolicyAuthority,
) -> Result<(), Status> {
    pplx_space_common_svc::require_local_authority_peer(
        principal,
        authority == crate::db::PolicyAuthority::Local,
    )
}

pub(super) fn apply_decision_to_proto(decision: ApplyDecisionKind) -> i32 {
    match decision {
        ApplyDecisionKind::Skip => NetworkPolicyApplyDecision::Skip as i32,
        ApplyDecisionKind::Apply => NetworkPolicyApplyDecision::Apply as i32,
        ApplyDecisionKind::Repair => NetworkPolicyApplyDecision::Repair as i32,
        ApplyDecisionKind::Conflict => NetworkPolicyApplyDecision::Conflict as i32,
    }
}

#[cfg(test)]
mod tests;
