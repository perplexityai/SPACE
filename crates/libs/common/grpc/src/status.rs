use std::time::Duration;

use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::{Code, Status};

pub const RETRY_AFTER_MS_METADATA: &str = "x-space-retry-after-ms";
/// Metadata key marking a `Status` as a *terminal* sandbox desired-state apply
/// precondition: the node cannot make the desired state converge by retrying
/// while cluster state is unchanged — the guest is unreachable because the
/// sandbox is in a non-thawing state, or the auto-lifecycle target state is
/// stuck (`Failed`/`CleanupRequired`/`CleanupFailed`/`Quarantined`). The reconcile-queue ref
/// should complete until a lifecycle or cluster repair changes the state.
/// Absence means the failure is transient — a still-initializing guest (`guest_not_ready`),
/// a mid-transition lifecycle state (`Starting`/`Resuming`/`Suspending`) — and
/// the queue should re-drive it. The tonic `Code` alone can't carry this
/// because sbx-manager answers `FailedPrecondition` for both cases.
pub const DESIRED_STATE_APPLY_TERMINAL_METADATA: &str = "space-desired-state-apply-terminal";
const DESIRED_STATE_APPLY_TERMINAL: &str = "1";
const SANDBOX_SERVICE_ROUTE_ERROR_METADATA: &str = "space-route-error";
const SANDBOX_SERVICE_ROUTE_NOT_READY: &str = "sandbox_service_route_not_ready";
const NODE_API_ERROR_METADATA: &str = "space-node-api-error";
const UDS_PER_SANDBOX_LIMIT_EXCEEDED: &str = "uds_per_sandbox_limit_exceeded";
const GUEST_CAPABILITY_ERROR_METADATA: &str = "space-guest-capability-error";
const GUEST_CAPABILITY_UNSUPPORTED: &str = "guest_capability_unsupported";
const GUEST_CAPABILITY_METADATA: &str = "space-guest-capability";

/// A guest application listener refused TCP establishment. This is not a
/// platform transport failure and must not invalidate runtime routing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GuestServiceUnavailable;

const GUEST_SERVICE_ERROR_METADATA: &str = "space-guest-service-error";
const GUEST_CONNECTION_REFUSED: &str = "connection_refused";

impl GuestServiceUnavailable {
    pub fn from_status(status: &Status) -> Option<Self> {
        (status.code() == Code::FailedPrecondition
            && status
                .metadata()
                .get(GUEST_SERVICE_ERROR_METADATA)
                .and_then(|value| value.to_str().ok())
                == Some(GUEST_CONNECTION_REFUSED))
        .then_some(Self)
    }

    pub fn into_status(self) -> Status {
        let mut status = Status::failed_precondition("Sandbox service is not reachable");
        status.metadata_mut().insert(
            GUEST_SERVICE_ERROR_METADATA,
            MetadataValue::from_static(GUEST_CONNECTION_REFUSED),
        );
        status
    }
}

/// Classify only errors from a TCP connect to an authenticated guest endpoint.
/// Internal RPC socket errors must never pass through this boundary.
pub fn guest_service_connect_status(error: &std::io::Error) -> Status {
    if error.kind() == std::io::ErrorKind::ConnectionRefused {
        GuestServiceUnavailable.into_status()
    } else {
        Status::unavailable(format!("service port ingress connect failed: {error}"))
    }
}

const GUEST_PROCESS_ERROR_METADATA: &str = "space-guest-process-error";

/// Mark definitive process-request validation or authenticated guest rejections.
/// A node lookup or route-token failure may use the same gRPC code.
pub fn mark_guest_process_error(mut status: Status) -> Status {
    // The guest is not authoritative about the host's retry contract.
    status.metadata_mut().remove(GUEST_PROCESS_ERROR_METADATA);
    let storage_full = status.code() == Code::ResourceExhausted
        && status
            .metadata()
            .get(pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY)
            .and_then(|value| value.to_str().ok())
            == Some(pplx_space_common_core::spaced::GUEST_STORAGE_FULL);
    if storage_full
        || matches!(
            status.code(),
            Code::NotFound
                | Code::InvalidArgument
                | Code::OutOfRange
                | Code::PermissionDenied
                | Code::FailedPrecondition
        )
    {
        status.metadata_mut().insert(
            GUEST_PROCESS_ERROR_METADATA,
            MetadataValue::from_static("terminal"),
        );
    }
    status
}

pub fn is_terminal_guest_process_error(status: &Status) -> bool {
    status
        .metadata()
        .get(GUEST_PROCESS_ERROR_METADATA)
        .and_then(|value| value.to_str().ok())
        == Some("terminal")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GuestCapability {
    InteractiveProcessControl,
}

impl GuestCapability {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InteractiveProcessControl => "interactive_process_control",
        }
    }
}

impl std::fmt::Display for GuestCapability {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for GuestCapability {
    type Err = GuestCapabilityParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value {
            "interactive_process_control" => Self::InteractiveProcessControl,
            _ => {
                return Err(GuestCapabilityParseError {
                    value: value.to_owned(),
                });
            }
        })
    }
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[error("unsupported guest capability {value:?}")]
pub struct GuestCapabilityParseError {
    value: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GuestCapabilityUnsupported {
    pub capability: GuestCapability,
}

impl GuestCapabilityUnsupported {
    pub fn from_metadata(metadata: &MetadataMap) -> Option<Self> {
        guest_capability_unsupported_from_metadata(metadata)
    }

    pub fn from_status(status: &Status) -> Option<Self> {
        if status.code() != Code::FailedPrecondition {
            return None;
        }
        Self::from_metadata(status.metadata())
    }
}

pub fn guest_capability_unsupported_status(
    capability: GuestCapability,
    message: impl Into<String>,
) -> Status {
    let mut status = Status::failed_precondition(message.into());
    status.metadata_mut().insert(
        GUEST_CAPABILITY_ERROR_METADATA,
        MetadataValue::from_static(GUEST_CAPABILITY_UNSUPPORTED),
    );
    status.metadata_mut().insert(
        GUEST_CAPABILITY_METADATA,
        MetadataValue::from_static(capability.as_str()),
    );
    status
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateLimited {
    pub try_again: Option<Duration>,
}

impl RateLimited {
    pub fn from_status(status: &Status) -> Option<Self> {
        if status.code() != Code::ResourceExhausted {
            return None;
        }
        Some(Self {
            try_again: retry_after_from_metadata(status.metadata())
                .or_else(|| retry_after_from_message(status.message())),
        })
    }
}

pub fn rate_limited_status(message: impl Into<String>, try_again: Option<Duration>) -> Status {
    let status = Status::resource_exhausted(message.into());
    match try_again {
        Some(try_again) => with_retry_after(status, try_again),
        None => status,
    }
}

/// Attach a retry hint to any `Status` via [`RETRY_AFTER_MS_METADATA`]. The
/// refusing server knows when its precondition will next be satisfiable
/// (for example the end of a quiescence window); a reconciling client that
/// reads it with [`retry_after`] can back the item off exactly that long
/// instead of applying its flat defer back-off to every refusal.
pub fn with_retry_after(mut status: Status, try_again: Duration) -> Status {
    if let Ok(value) = MetadataValue::try_from(try_again.as_millis().to_string()) {
        status.metadata_mut().insert(RETRY_AFTER_MS_METADATA, value);
    }
    status
}

/// Mark `status` as a request refused before any of its work started (see
/// [`pplx_space_common_core::spaced::REQUEST_REFUSED`]), with a retry hint.
/// Hops relay it unchanged and do not retry it; the public surfaces answer it
/// as retryable, with `Retry-After`.
pub fn refused_before_start(mut status: Status, try_again: Duration) -> Status {
    status.metadata_mut().insert(
        pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY,
        MetadataValue::from_static(pplx_space_common_core::spaced::REQUEST_REFUSED),
    );
    with_retry_after(status, try_again)
}

/// Whether `status` was marked by [`refused_before_start`].
pub fn is_refused_before_start(status: &Status) -> bool {
    status
        .metadata()
        .get(pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY)
        .and_then(|value| value.to_str().ok())
        == Some(pplx_space_common_core::spaced::REQUEST_REFUSED)
}

/// Mark a NOT_FOUND returned by a guest filesystem call on a path as the path
/// being missing (see [`pplx_space_common_core::spaced::GUEST_PATH_NOT_FOUND`]).
///
/// Only the hop that has already resolved the sandbox to a running guest may
/// call this: there, a NOT_FOUND from the guest's filesystem service can only
/// be the path. Any other status passes through unchanged.
pub fn mark_guest_path_not_found(mut status: Status) -> Status {
    if status.code() == Code::NotFound {
        status.metadata_mut().insert(
            pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY,
            MetadataValue::from_static(pplx_space_common_core::spaced::GUEST_PATH_NOT_FOUND),
        );
    }
    status
}

/// Whether `status` was marked by [`mark_guest_path_not_found`].
pub fn is_guest_path_not_found(status: &Status) -> bool {
    status.code() == Code::NotFound
        && status
            .metadata()
            .get(pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY)
            .and_then(|value| value.to_str().ok())
            == Some(pplx_space_common_core::spaced::GUEST_PATH_NOT_FOUND)
}

const SESSION_ROUTE_AT_REST: &str = "session_route_at_rest";

/// Mark a FAILED_PRECONDITION for a session route the sandbox has no node
/// for as one nothing is about to give it: the sandbox is not heading to
/// running, no wake is queued, and none could be queued for it. The marker
/// names the sandbox and its state, so the answer can say how to wake it.
/// Absence keeps the old meaning, a route that is not ready yet, so a client
/// may wait and attach again.
pub fn mark_session_route_at_rest(
    mut status: Status,
    sandbox_id: pplx_space_common_core::types::SandboxId,
    state: pplx_space_common_core::types::SandboxLifecycle,
) -> Status {
    status.metadata_mut().insert(
        SANDBOX_SERVICE_ROUTE_ERROR_METADATA,
        MetadataValue::from_static(SESSION_ROUTE_AT_REST),
    );
    status.metadata_mut().insert(
        SANDBOX_STATE_METADATA,
        MetadataValue::from_static(state.as_str()),
    );
    if let Ok(sandbox_id) = MetadataValue::try_from(sandbox_id.to_string()) {
        status
            .metadata_mut()
            .insert(SANDBOX_ID_METADATA, sandbox_id);
    }
    status
}

/// What [`mark_session_route_at_rest`] recorded, each part `None` when it did
/// not survive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionRouteAtRest {
    pub sandbox_id: Option<pplx_space_common_core::types::SandboxId>,
    pub state: Option<pplx_space_common_core::types::SandboxLifecycle>,
}

/// The at-rest mark on `status`, or `None` when it has none.
pub fn session_route_at_rest(status: &Status) -> Option<SessionRouteAtRest> {
    let metadata = status.metadata();
    let marked = status.code() == Code::FailedPrecondition
        && metadata
            .get(SANDBOX_SERVICE_ROUTE_ERROR_METADATA)
            .and_then(|value| value.to_str().ok())
            == Some(SESSION_ROUTE_AT_REST);
    marked.then(|| SessionRouteAtRest {
        sandbox_id: metadata
            .get(SANDBOX_ID_METADATA)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok()),
        state: metadata
            .get(SANDBOX_STATE_METADATA)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok()),
    })
}

const SANDBOX_ID_METADATA: &str = "space-sandbox-id";
const SANDBOX_STATE_METADATA: &str = "space-sandbox-state";

/// Mark `status` as a [`pplx_space_common_core::spaced::SANDBOX_NOT_RUNNING`]
/// refusal, naming the sandbox's `state` as the refusing hop knows it.
pub fn mark_sandbox_not_running(mut status: Status, state: &str) -> Status {
    status.metadata_mut().insert(
        pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY,
        MetadataValue::from_static(pplx_space_common_core::spaced::SANDBOX_NOT_RUNNING),
    );
    if let Ok(state) = MetadataValue::try_from(state) {
        status.metadata_mut().insert(SANDBOX_STATE_METADATA, state);
    }
    status
}

/// The state named by [`mark_sandbox_not_running`], or `None` when `status`
/// is not that refusal. A refusal whose state did not survive names `""`.
pub fn sandbox_not_running_state(status: &Status) -> Option<&str> {
    let marked = status.code() == Code::FailedPrecondition
        && status
            .metadata()
            .get(pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY)
            .and_then(|value| value.to_str().ok())
            == Some(pplx_space_common_core::spaced::SANDBOX_NOT_RUNNING);
    marked.then(|| {
        status
            .metadata()
            .get(SANDBOX_STATE_METADATA)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
    })
}

/// The retry hint carried by [`RETRY_AFTER_MS_METADATA`], if the server set
/// one (see [`with_retry_after`]). Metadata only: the message-text fallback
/// exists solely for the rate-limit status.
pub fn retry_after(status: &Status) -> Option<Duration> {
    retry_after_from_metadata(status.metadata())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SandboxServiceRouteNotReady;

impl SandboxServiceRouteNotReady {
    pub fn from_metadata(metadata: &MetadataMap) -> Option<Self> {
        sandbox_service_route_not_ready_from_metadata(metadata).then_some(Self)
    }

    pub fn from_status(status: &Status) -> Option<Self> {
        if !matches!(
            status.code(),
            Code::FailedPrecondition | Code::Unavailable | Code::DeadlineExceeded
        ) {
            return None;
        }
        (Self::from_metadata(status.metadata()).is_some()
            || sandbox_service_route_not_ready_from_message(status.message()))
        .then_some(Self)
    }
}

pub fn sandbox_service_route_not_ready_status(code: Code, message: impl Into<String>) -> Status {
    let mut status = Status::new(code, message.into());
    status.metadata_mut().insert(
        SANDBOX_SERVICE_ROUTE_ERROR_METADATA,
        MetadataValue::from_static(SANDBOX_SERVICE_ROUTE_NOT_READY),
    );
    status
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UdsPerSandboxLimitExceeded;

impl UdsPerSandboxLimitExceeded {
    pub fn from_metadata(metadata: &MetadataMap) -> Option<Self> {
        uds_per_sandbox_limit_exceeded_from_metadata(metadata).then_some(Self)
    }

    pub fn from_status(status: &Status) -> Option<Self> {
        if status.code() != Code::ResourceExhausted {
            return None;
        }
        Self::from_metadata(status.metadata())
    }
}

/// The node API refuses the call before it reaches the guest, so it is also a
/// [`refused_before_start`] refusal.
pub fn uds_per_sandbox_limit_exceeded_status(message: impl Into<String>) -> Status {
    let mut status = refused_before_start(
        Status::resource_exhausted(message.into()),
        UDS_PER_SANDBOX_RETRY_AFTER,
    );
    status.metadata_mut().insert(
        NODE_API_ERROR_METADATA,
        MetadataValue::from_static(UDS_PER_SANDBOX_LIMIT_EXCEEDED),
    );
    status
}

// A slot frees when one of the sandbox's other calls ends.
const UDS_PER_SANDBOX_RETRY_AFTER: Duration = Duration::from_secs(1);

/// Tag `status` as a terminal sandbox desired-state apply precondition (see
/// [`DESIRED_STATE_APPLY_TERMINAL_METADATA`]). The caller passes the
/// already-built precondition `Status`; the marker rides the trailing metadata
/// to the reconciling client, which classifies it with
/// [`is_desired_state_apply_terminal`].
pub fn mark_desired_state_apply_terminal(mut status: Status) -> Status {
    status.metadata_mut().insert(
        DESIRED_STATE_APPLY_TERMINAL_METADATA,
        MetadataValue::from_static(DESIRED_STATE_APPLY_TERMINAL),
    );
    status
}

/// Whether `status` carries the terminal desired-state apply marker set by
/// [`mark_desired_state_apply_terminal`].
pub fn is_desired_state_apply_terminal(status: &Status) -> bool {
    status
        .metadata()
        .get(DESIRED_STATE_APPLY_TERMINAL_METADATA)
        .and_then(|value| value.to_str().ok())
        == Some(DESIRED_STATE_APPLY_TERMINAL)
}

fn retry_after_from_metadata(metadata: &MetadataMap) -> Option<Duration> {
    let millis = metadata
        .get(RETRY_AFTER_MS_METADATA)?
        .to_str()
        .ok()?
        .parse::<u64>()
        .ok()?;
    Some(Duration::from_millis(millis))
}

fn retry_after_from_message(message: &str) -> Option<Duration> {
    let marker = "retry_after_ms=";
    let start = message.find(marker)? + marker.len();
    let digits: String = message
        .get(start..)?
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect();
    if digits.is_empty() {
        return None;
    }
    Some(Duration::from_millis(digits.parse().ok()?))
}

fn sandbox_service_route_not_ready_from_metadata(metadata: &MetadataMap) -> bool {
    metadata
        .get(SANDBOX_SERVICE_ROUTE_ERROR_METADATA)
        .and_then(|value| value.to_str().ok())
        == Some(SANDBOX_SERVICE_ROUTE_NOT_READY)
}

fn sandbox_service_route_not_ready_from_message(message: &str) -> bool {
    message.contains("sandbox service route")
}

fn uds_per_sandbox_limit_exceeded_from_metadata(metadata: &MetadataMap) -> bool {
    metadata
        .get(NODE_API_ERROR_METADATA)
        .and_then(|value| value.to_str().ok())
        == Some(UDS_PER_SANDBOX_LIMIT_EXCEEDED)
}

fn guest_capability_unsupported_from_metadata(
    metadata: &MetadataMap,
) -> Option<GuestCapabilityUnsupported> {
    let marker = metadata
        .get(GUEST_CAPABILITY_ERROR_METADATA)
        .and_then(|value| value.to_str().ok())?;
    if marker != GUEST_CAPABILITY_UNSUPPORTED {
        return None;
    }
    let capability = metadata
        .get(GUEST_CAPABILITY_METADATA)
        .and_then(|value| value.to_str().ok())?
        .parse()
        .ok()?;
    Some(GuestCapabilityUnsupported { capability })
}

#[cfg(test)]
mod tests;
