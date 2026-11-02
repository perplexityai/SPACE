use pplx_space_common_core::types::SandboxId;

use std::time::Instant;

use crate::emit::{counter, distribution, elapsed_ms, tag, timing_ms};

use super::sandbox_tags;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SandboxApiSurface {
    Native,
    Ecw,
}

impl SandboxApiSurface {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Ecw => "ecw",
        }
    }
}

impl std::fmt::Display for SandboxApiSurface {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Bounded client classification. Deployments map client identifiers to these
/// classes; raw client identifiers never become metric tags.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ClientClass {
    Interactive,
    Cron,
    Other,
}

impl ClientClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::Cron => "cron",
            Self::Other => "other",
        }
    }
}

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("client class must be interactive, cron, or other")]
pub struct ParseClientClassError;

impl std::str::FromStr for ClientClass {
    type Err = ParseClientClassError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "interactive" => Ok(Self::Interactive),
            "cron" => Ok(Self::Cron),
            "other" => Ok(Self::Other),
            _ => Err(ParseClientClassError),
        }
    }
}

impl std::fmt::Display for ClientClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The `client_id` tag value: an allowlisted `X-Client-ID`, or `unknown`.
///
/// Only operator-configured IDs may be allowlisted, so the tag's cardinality is
/// bounded by configuration, never by what a caller sent. A missing, blank, or
/// unlisted header is [`Self::UNKNOWN`], so a caller rotating IDs can't mint
/// new series. The variants are private: an allowlisted value exists only
/// through [`Self::allowlisted`], whose caller owns the bound.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ClientIdTag(ClientIdTagValue);

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ClientIdTagValue {
    Allowlisted(&'static str),
    Overflow,
    Unknown,
}

impl ClientIdTag {
    /// Every caller the allowlist doesn't name.
    pub const UNKNOWN: Self = Self(ClientIdTagValue::Unknown);
    /// An allowlisted client the resolver couldn't name without unbounded
    /// memory. It still has its own limiter bucket.
    pub const OVERFLOW: Self = Self(ClientIdTagValue::Overflow);

    /// The tag for an allowlisted client. `id` must come from operator
    /// configuration, interned by the resolver: a caller-supplied value here
    /// would put unbounded caller text on every series that carries the tag.
    pub const fn allowlisted(id: &'static str) -> Self {
        Self(ClientIdTagValue::Allowlisted(id))
    }

    pub const fn as_str(self) -> &'static str {
        match self.0 {
            ClientIdTagValue::Allowlisted(id) => id,
            ClientIdTagValue::Overflow => "overflow",
            ClientIdTagValue::Unknown => "unknown",
        }
    }

    /// Whether the tag names a client with its own limiter bucket.
    const fn has_own_bucket(self) -> bool {
        !matches!(self.0, ClientIdTagValue::Unknown)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SandboxApiMetricIdentity {
    pub op: &'static str,
    pub surface: SandboxApiSurface,
    /// Coarse connecting-client classification. Only the reconnect retry-storm
    /// metric reads this; the shared [`api_identity_tags`] used by every other
    /// API metric ignores it, so stamping it never widens their cardinality.
    /// Defaults to [`ClientClass::Other`]; the connect entrypoints stamp the
    /// real class from `X-Client-ID` via [`Self::with_client_class`].
    pub client_class: ClientClass,
    /// Bounded connecting-client ID. Read by the same reconnect metrics as
    /// `client_class` and ignored by [`api_identity_tags`]. Defaults to
    /// [`ClientIdTag::UNKNOWN`]; the connect entrypoint stamps it via
    /// [`Self::with_client_id`].
    pub client_id: ClientIdTag,
}

impl SandboxApiMetricIdentity {
    pub const fn new(op: &'static str, surface: SandboxApiSurface) -> Self {
        Self {
            op,
            surface,
            client_class: ClientClass::Other,
            client_id: ClientIdTag::UNKNOWN,
        }
    }

    pub const fn native(op: &'static str) -> Self {
        Self::new(op, SandboxApiSurface::Native)
    }

    pub const fn ecw(op: &'static str) -> Self {
        Self::new(op, SandboxApiSurface::Ecw)
    }

    /// Stamp the coarse connecting-client class onto the identity, leaving
    /// `op`/`surface` untouched. Used at the reconnect entrypoints so the
    /// retry-storm metric can split interactive vs cron traffic.
    pub const fn with_client_class(mut self, client_class: ClientClass) -> Self {
        self.client_class = client_class;
        self
    }

    /// Stamp the bounded connecting-client ID onto the identity, leaving every
    /// other field untouched. Used at the reconnect entrypoint so the
    /// reconnect metrics can name the caller, not only its class.
    pub const fn with_client_id(mut self, client_id: ClientIdTag) -> Self {
        self.client_id = client_id;
        self
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TemplateSelectorKind {
    Latest,
    Tag,
    Digest,
    DigestPrefix,
}

impl TemplateSelectorKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Latest => "latest",
            Self::Tag => "tag",
            Self::Digest => "digest",
            Self::DigestPrefix => "digest_prefix",
        }
    }
}

impl std::fmt::Display for TemplateSelectorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TemplateTagShape {
    Latest,
    GitSha40,
    Other,
    NotTag,
}

impl TemplateTagShape {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Latest => "latest",
            Self::GitSha40 => "git_sha_40",
            Self::Other => "other",
            Self::NotTag => "not_tag",
        }
    }
}

impl std::fmt::Display for TemplateTagShape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

pub struct TemplateSelectorRequest<'a> {
    pub identity: SandboxApiMetricIdentity,
    pub template: &'a str,
    pub selector_kind: TemplateSelectorKind,
    pub tag_shape: TemplateTagShape,
    pub requested_tag: &'a str,
}

pub const DORMANT_ACTIVATION_THRESHOLD_SECS: u64 = 7 * 24 * 60 * 60;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DormantActivationAgeSource {
    LastUsedAt,
    UpdatedAt,
}

impl DormantActivationAgeSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LastUsedAt => "last_used_at",
            Self::UpdatedAt => "updated_at",
        }
    }
}

impl std::fmt::Display for DormantActivationAgeSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DormantActivationAgeBucket {
    SevenToTenDays,
    TenToFourteenDays,
    FourteenToTwentyDays,
    TwentyToThirtyDays,
    ThirtyToSixtyDays,
    SixtyToNinetyDays,
    NinetyDaysPlus,
}

impl DormantActivationAgeBucket {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SevenToTenDays => "7d_10d",
            Self::TenToFourteenDays => "10d_14d",
            Self::FourteenToTwentyDays => "14d_20d",
            Self::TwentyToThirtyDays => "20d_30d",
            Self::ThirtyToSixtyDays => "30d_60d",
            Self::SixtyToNinetyDays => "60d_90d",
            Self::NinetyDaysPlus => "90d_plus",
        }
    }
}

impl std::fmt::Display for DormantActivationAgeBucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

pub struct DormantActivation<'a> {
    pub identity: SandboxApiMetricIdentity,
    pub action: &'a str,
    pub from_lifecycle: &'a str,
    pub idle_age_secs: u64,
    pub age_source: DormantActivationAgeSource,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ReconnectEntrypoint {
    Connect,
    Dataplane,
}

impl ReconnectEntrypoint {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::Dataplane => "dataplane",
        }
    }
}

impl std::fmt::Display for ReconnectEntrypoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ReconnectOutcome {
    Connected,
    WakeSubmitted,
    WakeInProgress,
    WakeFailed,
    Exhausted,
    Rejected,
}

impl ReconnectOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Connected => "connected",
            Self::WakeSubmitted => "wake_submitted",
            Self::WakeInProgress => "wake_in_progress",
            Self::WakeFailed => "wake_failed",
            Self::Exhausted => "exhausted",
            Self::Rejected => "rejected",
        }
    }
}

impl std::fmt::Display for ReconnectOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ReconnectTerminalPhase {
    Ready,
    NoWake,
    Wake,
    Restore,
    PodNeverReady,
    ProxyUnreachable,
    GuestNotReady,
    Api,
}

impl ReconnectTerminalPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::NoWake => "no_wake",
            Self::Wake => "wake",
            Self::Restore => "restore",
            Self::PodNeverReady => "pod_never_ready",
            Self::ProxyUnreachable => "proxy_unreachable",
            Self::GuestNotReady => "guest_not_ready",
            Self::Api => "api",
        }
    }
}

impl std::fmt::Display for ReconnectTerminalPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ReconnectReason {
    Ok,
    NoWakeNeeded,
    RestoreInFlight,
    /// Recovery has no verified state to materialize. Emitted with
    /// `outcome:wake_failed`; increasing the request budget cannot fix it.
    RestoreContextUnavailable,
    WaitBudget,
    PodNeverReady,
    ProxyUnreachable,
    GuestNotReady,
    Substrate,
    TerminalLifecycle,
    Conflict,
    Unavailable,
    Failed,
    Timeout,
    Cancelled,
    NotFound,
    Unsupported,
    PostWakeCasLost,
    DrainBlocked,
    RateLimited,
    LockContention,
}

impl ReconnectReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::NoWakeNeeded => "no_wake_needed",
            Self::RestoreInFlight => "restore_in_flight",
            Self::RestoreContextUnavailable => "restore_context_unavailable",
            Self::WaitBudget => "wait_budget",
            Self::PodNeverReady => "pod_never_ready",
            Self::ProxyUnreachable => "proxy_unreachable",
            Self::GuestNotReady => "guest_not_ready",
            Self::Substrate => "substrate",
            Self::TerminalLifecycle => "terminal_lifecycle",
            Self::Conflict => "conflict",
            Self::Unavailable => "unavailable",
            Self::Failed => "failed",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::NotFound => "not_found",
            Self::Unsupported => "unsupported",
            Self::PostWakeCasLost => "post_wake_cas_lost",
            Self::DrainBlocked => "drain_blocked",
            Self::RateLimited => "rate_limited",
            Self::LockContention => "lock_contention",
        }
    }
}

impl std::fmt::Display for ReconnectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

pub struct ReconnectAttempt<'a> {
    pub entrypoint: ReconnectEntrypoint,
    pub outcome: ReconnectOutcome,
    pub initial_lifecycle: &'a str,
    pub terminal_phase: ReconnectTerminalPhase,
    pub reason: ReconnectReason,
    pub client_class: ClientClass,
    pub client_id: ClientIdTag,
    pub started_at: Instant,
}

pub fn reconnect_attempt(sample: ReconnectAttempt<'_>) {
    let counter_tags = [
        tag("entrypoint", sample.entrypoint),
        tag("outcome", sample.outcome),
        tag("initial_lifecycle", sample.initial_lifecycle),
        tag("terminal_phase", sample.terminal_phase),
        tag("reason", sample.reason),
        tag("client_class", sample.client_class),
        tag("client_id", sample.client_id.as_str()),
    ];
    counter("sandbox.reconnect.attempts", 1, &counter_tags);
    let duration_tags = [
        tag("entrypoint", sample.entrypoint),
        tag("outcome", sample.outcome),
        tag("terminal_phase", sample.terminal_phase),
        tag("client_id", sample.client_id.as_str()),
    ];
    timing_ms(
        "sandbox.reconnect.duration_ms",
        elapsed_ms(sample.started_at),
        &duration_tags,
    );
}

/// Which branch of the user wake/restore claim loop re-drove a losing lap.
/// The branches issue the same four SQL statements per lap, so database-side
/// query counts cannot attribute a claim-lap storm; this tag can.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RestoreClaimLap {
    /// The follower join observed `Claimable` and the caller re-raced.
    ClaimableRejoin,
    /// A lifecycle-conflict claim miss (e.g. a fence-rejected takeover)
    /// re-drove the loop.
    ConflictRedrive,
}

impl RestoreClaimLap {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClaimableRejoin => "claimable_rejoin",
            Self::ConflictRedrive => "conflict_redrive",
        }
    }
}

impl std::fmt::Display for RestoreClaimLap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Emit one losing lap of the user wake/restore claim loop.
pub fn restore_claim_lap(identity: SandboxApiMetricIdentity, lap: RestoreClaimLap) {
    let mut tags = api_identity_tags(identity);
    tags.push(tag("lap", lap));
    counter("sandbox.api.restore_claim.laps", 1, &tags);
}

/// How one caller's pass through the pod-local wake coalescer resolved. A
/// healthy stampede shows one `owned` per pod and the rest `joined`;
/// sustained `joiner_reraced` means runs keep failing under waiting demand.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WakeCoalesceOutcome {
    /// This caller started the shared run and returned its success.
    Owned,
    /// This caller shared another caller's successful run.
    Joined,
    /// This caller started the shared run and returned its failure.
    OwnerFailed,
    /// This caller shared a failed run and re-entered the gate to re-race.
    JoinerReraced,
    /// The caller's readiness budget elapsed before the shared run published.
    DeadlineElapsed,
    /// The shared run died without publishing; the caller re-entered.
    RunAbandoned,
}

impl WakeCoalesceOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Owned => "owned",
            Self::Joined => "joined",
            Self::OwnerFailed => "owner_failed",
            Self::JoinerReraced => "joiner_reraced",
            Self::DeadlineElapsed => "deadline_elapsed",
            Self::RunAbandoned => "run_abandoned",
        }
    }
}

impl std::fmt::Display for WakeCoalesceOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Emit one caller's resolution through the pod-local wake coalescer.
pub fn wake_coalesce_outcome(identity: SandboxApiMetricIdentity, outcome: WakeCoalesceOutcome) {
    let mut tags = api_identity_tags(identity);
    tags.push(tag("outcome", outcome));
    counter("sandbox.api.wake_coalescer.outcome", 1, &tags);
}

metric_vocabulary!(
    /// How one timeout update (ECW or native `/v1`) chose between the
    /// lock-free read and the sandbox row lock. Exactly one value is
    /// recorded per call, before any lock wait, so calls that later expire
    /// waiting for the lock still count. The `skipped_*` share of the total
    /// is how often `space-api-timeout-lock-bypass` avoids the lock; the
    /// `locked_*` values say why the rest still take it.
    TimeoutUpdateLockDecision {
        /// Unchanged policy on a created, paused or suspended sandbox.
        SkippedAbsent => "skipped_absent",
        /// Unchanged policy on a started sandbox with a known execution
        /// generation.
        SkippedPresent => "skipped_present",
        /// The flag is off for this sandbox; nothing was read unlocked.
        LockedFlagOff => "locked_flag_off",
        /// The call changes the stored policy.
        LockedPolicyChanged => "locked_policy_changed",
        /// The caller's expected policy differs from the stored one.
        LockedExpectedMismatch => "locked_expected_mismatch",
        /// Unchanged policy, but only a locked read can settle whether a
        /// runtime must receive it.
        LockedPresenceUnknown => "locked_presence_unknown",
    }
);

/// Record how a timeout update chose its read. No per-sandbox tags.
pub fn timeout_update_lock_decision(decision: TimeoutUpdateLockDecision) {
    counter(
        "sandbox.api.timeout_update.lock_decision",
        1,
        &[tag("decision", decision)],
    );
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RouteIdentityDecisionReason {
    ResidentTargetIdentityReused,
    ExistingExecutionTransition,
    FirstIdentityInitialized,
    ExecutionReplaced,
    RotationRequiredUnclassified,
}

impl RouteIdentityDecisionReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ResidentTargetIdentityReused => "resident_target_identity_reused",
            Self::ExistingExecutionTransition => "existing_execution_transition",
            Self::FirstIdentityInitialized => "first_identity_initialized",
            Self::ExecutionReplaced => "execution_replaced",
            Self::RotationRequiredUnclassified => "rotation_required_unclassified",
        }
    }
}

impl std::fmt::Display for RouteIdentityDecisionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Record the bounded reason that selected a persisted Kubernetes route
/// identity. Identifiers stay in structured logs; this counter deliberately
/// exposes only the closed decision vocabulary.
pub fn route_identity_decision(reason: RouteIdentityDecisionReason) {
    counter(
        "sandbox.api.k8s.route_identity.decision",
        1,
        &[tag("reason", reason)],
    );
}

pub fn template_selector_requested(sample: TemplateSelectorRequest<'_>) {
    let mut tags = api_identity_tags(sample.identity);
    tags.push(tag("template", sample.template));
    tags.push(tag("selector_kind", sample.selector_kind));
    tags.push(tag("tag_shape", sample.tag_shape));
    tags.push(tag("requested_tag", sample.requested_tag));
    counter("sandbox.api.template_selector.requested", 1, &tags);
}

/// Record one successful sandbox activation's idle age. Every sample feeds
/// the ungated `sandbox.api.activation_idle_age_secs` distribution — the
/// sub-threshold mass is what sizes archival-TTL candidates, so the exact age
/// must not be gated or bucketed away. Samples older than
/// [`DORMANT_ACTIVATION_THRESHOLD_SECS`] additionally increment the bucketed
/// `sandbox.api.dormant_activation` counter.
pub fn dormant_activation(sample: DormantActivation<'_>) {
    distribution(
        "sandbox.api.activation_idle_age_secs",
        sample.idle_age_secs as f64,
        &activation_idle_age_tags(&sample),
    );
    if let Some(bucket) = dormant_activation_age_bucket(sample.idle_age_secs) {
        counter(
            "sandbox.api.dormant_activation",
            1,
            &dormant_activation_tags(&sample, bucket),
        );
    }
}

/// Records the outcome of one sandbox API route response.
///
/// A 4xx or 5xx response increments `sandbox.api.request_error` exactly once,
/// tagged with `op`, `surface`, `status_code` and `result_class`. This is the
/// failed-request count.
///
/// It is deliberately a separate metric from `sandbox.api.error`, which holds
/// the step-level errors that handlers classify by `reason`
/// (`api_step_error`, `lifecycle_error`). Those can fire several times for one
/// request, or for a request that later succeeds, and many route failures
/// (404s, validation errors) have no step error at all. If both were emitted
/// under one name, summing that metric would count each classified failure
/// twice.
pub fn route_outcome(
    identity: SandboxApiMetricIdentity,
    status_code: u16,
    started_at: Instant,
    template_name: Option<&str>,
) {
    let result_class = match status_code {
        100..=399 => "success",
        400..=499 => "client_error",
        500..=599 => "server_error",
        _ => "error",
    };
    let result = if status_code < 400 {
        "success"
    } else {
        "error"
    };
    let mut tags = api_identity_tags(identity);
    tags.push(tag("result", result));
    tags.push(tag("result_class", result_class));
    tags.push(tag("status_code", status_code));
    if let Some(template_name) = template_name {
        tags.push(tag("template_name", template_name));
    }
    timing_ms("sandbox.api.latency_ms", elapsed_ms(started_at), &tags);

    if status_code >= 400 {
        counter("sandbox.api.request_error", 1, &tags);
    }
}

pub struct ApiStep<'a> {
    /// Caller-facing operation and surface, such as `create/native` or
    /// `resume/ecw`.
    pub identity: SandboxApiMetricIdentity,
    pub backend: &'a str,
    pub template: Option<&'a str>,
    /// Stable step name within the operation.
    pub step: &'a str,
    pub result: &'a str,
    pub started_at: Instant,
    /// Optional lifecycle phase only when a single operation metric spans more
    /// than one lifecycle phase. Do not repeat `operation` here.
    pub phase: Option<&'a str>,
    /// Optional sub-phase for wait loops such as command probes.
    pub wait_phase: Option<&'a str>,
    /// Optional origin of the state being waited on.
    pub since: Option<&'a str>,
}

pub struct ApiStepError<'a> {
    pub identity: SandboxApiMetricIdentity,
    pub backend: &'a str,
    pub template: Option<&'a str>,
    pub step: &'a str,
    pub reason: &'a str,
    pub phase: Option<&'a str>,
    pub wait_phase: Option<&'a str>,
    pub grpc_code: Option<&'a str>,
}

pub struct CreateStep<'a> {
    pub identity: SandboxApiMetricIdentity,
    pub backend: &'a str,
    pub template: Option<&'a str>,
    pub step: &'a str,
    pub result: &'a str,
    pub started_at: Instant,
    pub phase: Option<&'a str>,
}

pub fn api_step_latency(_sandbox_id: SandboxId, sample: ApiStep<'_>) {
    let tags = api_step_tags(
        sample.identity,
        sample.backend,
        sample.template,
        sample.result,
        sample.step,
        StepTagOptions {
            phase: sample.phase,
            wait_phase: sample.wait_phase,
            since: sample.since,
        },
    );
    timing_ms(
        "sandbox.api.step.latency_ms",
        elapsed_ms(sample.started_at),
        &tags,
    );
}

pub fn api_step_error(_sandbox_id: SandboxId, sample: ApiStepError<'_>) {
    let mut tags = api_step_tags(
        sample.identity,
        sample.backend,
        sample.template,
        "error",
        sample.step,
        StepTagOptions {
            phase: sample.phase,
            wait_phase: sample.wait_phase,
            since: None,
        },
    );
    tags.push(tag("reason", sample.reason));
    if let Some(grpc_code) = sample.grpc_code {
        tags.push(tag("grpc_code", grpc_code));
    }
    counter("sandbox.api.error", 1, &tags);
}

/// One sandbox returned successfully from a warm or cold create, including
/// accepted creates whose guest readiness is still pending.
pub fn sandbox_created(tls_interception_on: bool) {
    counter(
        "sandbox.api.created",
        1,
        &[tag(
            "tls.interception",
            if tls_interception_on { "on" } else { "off" },
        )],
    );
}

/// One completed create, tagged with where its vCPU and memory came from.
/// `shape_source:default` means the platform default shape applied.
pub fn create_shape_source(shape_source: &str, runtime: &str, template: &str) {
    let tags = [
        tag("shape_source", shape_source),
        tag("runtime", runtime),
        tag("template", template),
    ];
    counter("sandbox.api.create.shape_source", 1, &tags);
}

pub fn create_step_latency(sandbox_id: SandboxId, sample: CreateStep<'_>) {
    api_step_latency(
        sandbox_id,
        ApiStep {
            identity: sample.identity,
            backend: sample.backend,
            template: sample.template,
            step: sample.step,
            result: sample.result,
            started_at: sample.started_at,
            phase: sample.phase,
            wait_phase: None,
            since: None,
        },
    );
}

pub fn create_step_error(
    sandbox_id: SandboxId,
    identity: SandboxApiMetricIdentity,
    backend: &str,
    template: Option<&str>,
    step: &str,
    reason: &str,
    phase: Option<&str>,
) {
    api_step_error(
        sandbox_id,
        ApiStepError {
            identity,
            backend,
            template,
            step,
            reason,
            phase,
            wait_phase: None,
            grpc_code: None,
        },
    );
}

/// In-process counter for the create-path `poll_until_command_ready` deadline
/// firing. Emitted only when the create operation's deadline elapses; not
/// emitted for wake/start/access call sites of the same poll loop. Increments
/// are an alertable signal of kube-apiserver / APF saturation on the create
/// hot path: under load the sandbox pod proxy never becomes reachable within
/// the configured timeout.
pub fn create_command_ready_timeout(_sandbox_id: SandboxId, template: Option<&str>) {
    let tags = sandbox_tags("fc", template, "timeout");
    counter("sandbox.api.create.command_ready_timeout", 1, &tags);
}

/// Cascade-amplifier signal: emitted when `create_execution`'s
/// `CancelGuard` drops while a node-mgr-unreachable cause was marked, so
/// pod cleanup was deferred to the reconciler instead of being spawned
/// onto a saturated node-mgr. A rising rate indicates the cancel-storm
/// defer path is being hit; the reconciler should then GC the leaked pods.
pub fn create_cancel_cleanup_deferred() {
    counter("sandbox.api.create.cancel_cleanup_deferred", 1, &[]);
}

/// Which lifecycle action's completion the wake shield was guarding — the
/// `action` tag on [`wake_completion_cancelled`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeCompletionAction {
    Wake,
    Restore,
}

impl WakeCompletionAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Wake => "wake",
            Self::Restore => "restore",
        }
    }
}

impl std::fmt::Display for WakeCompletionAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Terminal outcome of one shielded wake-completion redrive — the `outcome`
/// tag on [`wake_completion_redrive`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeCompletionRedriveOutcome {
    /// The redrive hydrated the execution and ran the fenced
    /// `restoring -> started` CAS: the row converged as if the cancelled
    /// request had survived.
    Completed,
    /// Another actor — a client retry, the node published-state
    /// convergence, the sweeper — moved the row first; nothing written.
    AlreadyResolved,
    /// The execution never became routable (or an unexpired restore lease
    /// kept another attempt's ownership) within the budget; the row stays
    /// `restoring` and the sweeper remains the backstop.
    Abandoned,
    /// The concurrent-redrive cap was full when the cancellation fired; no
    /// redrive was spawned and the sweeper backstops the row. A sustained
    /// rate means a cancel storm is in progress.
    Shed,
}

impl WakeCompletionRedriveOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::AlreadyResolved => "already_resolved",
            Self::Abandoned => "abandoned",
            Self::Shed => "shed",
        }
    }
}

impl std::fmt::Display for WakeCompletionRedriveOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A wake/restore request was cancelled mid-flight (client disconnect
/// dropped the handler future) after the restore had been dispatched to the
/// node, and the shielded completion redrive was spawned (or shed) in its
/// place. Before the shield existed these deaths were invisible (an APM
/// span with no HTTP status) and each one left the row stuck at `restoring`
/// until the transient-lifecycle sweeper's age threshold passed.
pub fn wake_completion_cancelled(action: WakeCompletionAction) {
    counter(
        "sandbox.api.wake_completion.cancelled",
        1,
        &[crate::emit::tag("action", action.as_str())],
    );
}

/// Terminal outcome of one shielded wake-completion redrive; see
/// [`WakeCompletionRedriveOutcome`] for the closed outcome set. A sustained
/// `abandoned` rate means node-side restores are failing after their
/// callers hang up; a sustained `shed` rate means a cancel storm exhausted
/// the concurrent-redrive cap.
pub fn wake_completion_redrive(outcome: WakeCompletionRedriveOutcome) {
    counter(
        "sandbox.api.wake_completion.redrive",
        1,
        &[crate::emit::tag("outcome", outcome.as_str())],
    );
}

/// Outcome of a single `read_sandbox_pod` call. Tagged with `outcome` ∈
/// `cache_settled` (cache hit and pod was settled — no GET, no wait),
/// `event_settled` (cache miss/unsettled, but a reflector event arrived in
/// time and the cache became settled — no GET), or `get_fallback` (event
/// budget elapsed or event arrived but cache still unsettled — direct GET to
/// kube-apiserver). Drop ratio `get_fallback` / total is the headline metric
/// for A4 effectiveness.
pub fn read_sandbox_pod_outcome(outcome: &str) {
    let tags = vec![tag("backend", "k8s"), tag("outcome", outcome)];
    counter("sandbox.api.k8s.read_sandbox_pod", 1, &tags);
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ServiceRoutePodVerifyOutcome {
    Ready,
    Missing,
    Terminal,
    Unschedulable,
    NoIp,
    UidMismatch,
    Timeout,
    KubeError,
}

impl ServiceRoutePodVerifyOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Missing => "missing",
            Self::Terminal => "terminal",
            Self::Unschedulable => "unschedulable",
            Self::NoIp => "no_ip",
            Self::UidMismatch => "uid_mismatch",
            Self::Timeout => "timeout",
            Self::KubeError => "kube_error",
        }
    }
}

impl std::fmt::Display for ServiceRoutePodVerifyOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn service_route_pod_verify(outcome: ServiceRoutePodVerifyOutcome, started_at: Instant) {
    let tags = vec![tag("backend", "k8s"), tag("outcome", outcome.as_str())];
    counter("sandbox.api.k8s.service_route_pod_verify", 1, &tags);
    timing_ms(
        "sandbox.api.k8s.service_route_pod_verify.latency_ms",
        elapsed_ms(started_at),
        &tags,
    );
}

pub fn k8s_blocking_delete_latency(
    resource: &str,
    op: &str,
    result: &str,
    waited: bool,
    started_at: Instant,
) {
    let tags = vec![
        tag("backend", "k8s"),
        tag("resource", resource),
        tag("op", op),
        tag("result", result),
        tag("waited", waited),
    ];
    timing_ms(
        "sandbox.api.k8s.blocking_delete.latency_ms",
        elapsed_ms(started_at),
        &tags,
    );
}

pub fn lifecycle_error(_sandbox_id: SandboxId, identity: SandboxApiMetricIdentity, reason: &str) {
    let mut tags = sandbox_tags_with_identity(identity, "fc", None, "error");
    tags.push(tag("reason", reason));
    counter("sandbox.api.error", 1, &tags);
}

struct StepTagOptions<'a> {
    phase: Option<&'a str>,
    wait_phase: Option<&'a str>,
    since: Option<&'a str>,
}

fn api_step_tags(
    identity: SandboxApiMetricIdentity,
    backend: &str,
    template: Option<&str>,
    result: &str,
    step: &str,
    options: StepTagOptions<'_>,
) -> Vec<String> {
    let mut tags = sandbox_tags_with_identity(identity, backend, template, result);
    tags.push(tag("step", step));
    if let Some(phase) = options.phase {
        tags.push(tag("phase", phase));
    }
    if let Some(wait_phase) = options.wait_phase {
        tags.push(tag("wait_phase", wait_phase));
    }
    if let Some(since) = options.since {
        tags.push(tag("since", since));
    }
    tags
}

fn sandbox_tags_with_identity(
    identity: SandboxApiMetricIdentity,
    backend: &str,
    template: Option<&str>,
    result: &str,
) -> Vec<String> {
    let mut tags = api_identity_tags(identity);
    tags.extend(sandbox_tags(backend, template, result));
    tags
}

fn api_identity_tags(identity: SandboxApiMetricIdentity) -> Vec<String> {
    vec![
        tag("op", identity.op),
        tag("surface", identity.surface.as_str()),
    ]
}

fn dormant_activation_age_bucket(idle_age_secs: u64) -> Option<DormantActivationAgeBucket> {
    if idle_age_secs <= DORMANT_ACTIVATION_THRESHOLD_SECS {
        return None;
    }

    let days = idle_age_secs / (24 * 60 * 60);
    match days {
        7..=9 => Some(DormantActivationAgeBucket::SevenToTenDays),
        10..=13 => Some(DormantActivationAgeBucket::TenToFourteenDays),
        14..=19 => Some(DormantActivationAgeBucket::FourteenToTwentyDays),
        20..=29 => Some(DormantActivationAgeBucket::TwentyToThirtyDays),
        30..=59 => Some(DormantActivationAgeBucket::ThirtyToSixtyDays),
        60..=89 => Some(DormantActivationAgeBucket::SixtyToNinetyDays),
        _ => Some(DormantActivationAgeBucket::NinetyDaysPlus),
    }
}

fn activation_idle_age_tags(sample: &DormantActivation<'_>) -> Vec<String> {
    let mut tags = api_identity_tags(sample.identity);
    tags.push(tag("action", sample.action));
    tags.push(tag("from_lifecycle", sample.from_lifecycle));
    tags.push(tag("age_source", sample.age_source));
    tags
}

fn dormant_activation_tags(
    sample: &DormantActivation<'_>,
    bucket: DormantActivationAgeBucket,
) -> Vec<String> {
    let mut tags = activation_idle_age_tags(sample);
    tags.push(tag("idle_age_bucket", bucket));
    tags
}

/// Metrics for the space-api per-client concurrency limiter, which caps how
/// many sandbox create/reconnect operations a single client (identified by the
/// `X-Client-ID` header) may have in flight at once.
pub mod client_limit {
    use super::ClientIdTag;
    use crate::emit::{counter, gauge, tag};

    /// Which control-plane-touching operation the limiter is gating.
    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    pub enum Operation {
        Create,
        Fork,
        Reconnect,
    }

    impl Operation {
        pub const fn as_str(self) -> &'static str {
            match self {
                Self::Create => "create",
                Self::Fork => "fork",
                Self::Reconnect => "reconnect",
            }
        }
    }

    impl std::fmt::Display for Operation {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.as_str())
        }
    }

    /// The limiter's verdict for one request. `WouldReject` is the observe-mode
    /// counterpart of `Rejected`: the request was admitted anyway, but it would
    /// have been rejected under enforcement.
    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    pub enum Decision {
        Admitted,
        Rejected,
        WouldReject,
    }

    impl Decision {
        pub const fn as_str(self) -> &'static str {
            match self {
                Self::Admitted => "admitted",
                Self::Rejected => "rejected",
                Self::WouldReject => "would_reject",
            }
        }
    }

    impl std::fmt::Display for Decision {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.as_str())
        }
    }

    /// Why the limiter reached its decision.
    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    pub enum Reason {
        /// Admitted with capacity to spare.
        None,
        /// The client is at its concurrent-operation cap.
        ConcurrencyFull,
        /// The limiter is turned off (env kill-switch or a zero cap), so every
        /// request is admitted without tracking a slot.
        Disabled,
        /// The shared Redis store was reachable but a runtime op failed; the
        /// limiter fails open (admits). Accompanies an `admitted` decision —
        /// alert on a sustained rate.
        StoreUnavailable,
        /// The limiter is enabled but has no usable Redis store (missing/invalid
        /// `SPACE_REDIS_URL`), so it admits everything for the process lifetime.
        /// Accompanies an `admitted` decision — page on it: the guardrail is
        /// silently absent.
        Misconfigured,
    }

    impl Reason {
        pub const fn as_str(self) -> &'static str {
            match self {
                Self::None => "none",
                Self::ConcurrencyFull => "concurrency_full",
                Self::Disabled => "disabled",
                Self::StoreUnavailable => "store_unavailable",
                Self::Misconfigured => "misconfigured",
            }
        }
    }

    impl std::fmt::Display for Reason {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.as_str())
        }
    }

    /// Whether the request was tracked in its own client bucket. Requests
    /// without an `X-Client-ID`, or with one the allowlist doesn't list, share
    /// a single `unspecified` bucket. [`decision`] derives it from the bounded
    /// [`ClientIdTag`]; the raw header value never becomes a tag.
    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    pub enum ClientScope {
        Client,
        Unspecified,
    }

    impl ClientScope {
        pub const fn as_str(self) -> &'static str {
            match self {
                Self::Client => "client",
                Self::Unspecified => "unspecified",
            }
        }
    }

    impl std::fmt::Display for ClientScope {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.as_str())
        }
    }

    impl From<ClientIdTag> for ClientScope {
        fn from(client_id: ClientIdTag) -> Self {
            if client_id.has_own_bucket() {
                Self::Client
            } else {
                Self::Unspecified
            }
        }
    }

    /// One limiter verdict, tagged with the bounded `client_id` so an observe
    /// or enforce rollout can be read per caller. `client_scope` is derived
    /// from it, so the two can't disagree.
    pub fn decision(
        operation: Operation,
        decision: Decision,
        reason: Reason,
        client_id: ClientIdTag,
    ) {
        counter(
            "sandbox.api.client_concurrency.decision",
            1,
            &[
                tag("operation", operation.as_str()),
                tag("decision", decision.as_str()),
                tag("reason", reason.as_str()),
                tag("client_scope", ClientScope::from(client_id).as_str()),
                tag("client_id", client_id.as_str()),
            ],
        );
    }

    /// Liveness tick for the per-client concurrency limiter, written on a timer
    /// rather than on a gated request.
    ///
    /// The per-client sibling of
    /// `sandbox::admission::cluster_heartbeat`, which documents the
    /// mechanism and its limits; the counter it breaks the silence of is
    /// [`decision`], written only when a request is actually gated.
    pub fn heartbeat() {
        counter("sandbox.api.client_concurrency.heartbeat", 1, &[]);
    }

    /// The acquiring client's cluster-wide in-flight slot count at admit time,
    /// sampled per admit. A single untagged gauge, so it reflects whichever
    /// client (or the shared `unspecified` bucket) last admitted — not a total
    /// across clients or a per-replica count.
    pub fn in_flight(value: usize) {
        gauge(
            "sandbox.api.client_concurrency.in_flight",
            value as f64,
            &[],
        );
    }
}

/// Metrics emitted by the orphaned-sandbox reconciler running in the
/// `space-cluster-controller` singleton binary.
pub mod reconciler {
    use std::{fmt, time::Instant};

    use pplx_space_common_core::types::SandboxLifecycle;

    use crate::emit::{counter, elapsed_ms, tag, timing_ms};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ClassificationReason {
        Normal,
        Forgotten,
        LegacyDeleting,
    }

    impl ClassificationReason {
        pub const fn as_str(self) -> &'static str {
            match self {
                Self::Normal => "normal",
                Self::Forgotten => "forgotten",
                Self::LegacyDeleting => "legacy_deleting",
            }
        }
    }

    impl fmt::Display for ClassificationReason {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.as_str())
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ClassificationOutcome {
        Deleted,
        Suspended,
        Quarantined,
        QuarantineBlocked,
        Deferred,
    }

    impl ClassificationOutcome {
        pub const fn as_str(self) -> &'static str {
            match self {
                Self::Deleted => "deleted",
                Self::Suspended => "suspended",
                Self::Quarantined => "quarantined",
                Self::QuarantineBlocked => "quarantine_blocked",
                Self::Deferred => "deferred",
            }
        }
    }

    impl fmt::Display for ClassificationOutcome {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.as_str())
        }
    }

    /// Mode tag distinguishing dry-run observation from live writes. Both
    /// modes increment the same metric; dashboards filter by `mode`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Mode {
        DryRun,
        Live,
    }

    impl Mode {
        pub const fn as_str(self) -> &'static str {
            match self {
                Mode::DryRun => "dry_run",
                Mode::Live => "live",
            }
        }
    }

    impl fmt::Display for Mode {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.as_str())
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum PodEvidence {
        TerminalSucceeded,
        TerminalFailed,
        Unready,
        Missing,
        NotApplicable,
    }

    impl PodEvidence {
        pub const fn as_str(self) -> &'static str {
            match self {
                Self::TerminalSucceeded => "terminal_succeeded",
                Self::TerminalFailed => "terminal_failed",
                Self::Unready => "unready",
                Self::Missing => "missing",
                Self::NotApplicable => "not_applicable",
            }
        }
    }

    impl fmt::Display for PodEvidence {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.as_str())
        }
    }

    pub fn rows_scanned(count: u32) {
        counter("sandbox.api.reconciler.rows_scanned", u64::from(count), &[]);
    }

    /// `reason` is bounded to the orphan-kind tag set: `normal`, `forgotten`,
    /// `legacy_deleting` (matching the active `OrphanKind` variants in the
    /// reconciler).
    /// `outcome` is bounded to the shared planned/committed disposition set:
    /// `deleted`, `suspended`, `quarantined`, or `deferred`. `mode`
    /// distinguishes dry-run planning from a committed live result.
    /// `from_lifecycle` is the closed sandbox lifecycle vocabulary and
    /// `pod_evidence` is bounded by the reconciler's presence enum plus
    /// `not_applicable` for non-pod classifier paths.
    pub fn rows_classified(
        reason: ClassificationReason,
        outcome: ClassificationOutcome,
        mode: Mode,
        from_lifecycle: SandboxLifecycle,
        pod_evidence: PodEvidence,
    ) {
        counter(
            "sandbox.api.reconciler.rows_classified",
            1,
            &[
                tag("reason", reason),
                tag("outcome", outcome),
                tag("mode", mode.as_str()),
                tag("from_lifecycle", from_lifecycle),
                tag("pod_evidence", pod_evidence),
            ],
        );
    }

    /// `reason` is bounded: `pod_present`, `within_grace`, `rate_limited`,
    /// `pod_presence_unknown`.
    pub fn rows_skipped(reason: &str, count: u32) {
        counter(
            "sandbox.api.reconciler.rows_skipped",
            u64::from(count),
            &[tag("reason", reason)],
        );
    }

    /// End-to-end duration of one reconcile tick.
    pub fn loop_latency(started_at: Instant) {
        timing_ms(
            "sandbox.api.reconciler.loop.latency_ms",
            elapsed_ms(started_at),
            &[],
        );
    }
}

#[cfg(test)]
mod tests;
