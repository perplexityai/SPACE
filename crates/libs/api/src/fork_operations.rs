use pplx_space_common_core::types::{OrgId, SnapshotId, UserId};

use std::{collections::BTreeMap, str::FromStr, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};

use pplx_space_common_core::types::{
    AutoLifecyclePolicy, BackupAttemptId, EcwCredentialEpoch, IdempotencyKey, LocalArtifactId,
    SANDBOX_FORK_SNAPSHOT_RETENTION_DEFAULT_GRACE_SECONDS,
    SNAPSHOT_STORAGE_SWEEPER_DEFAULT_MIN_AGE_SECONDS, SandboxId, SandboxLifecycle,
    SandboxLifecycleParseError, TemplateReference, UpstreamIssuer, UpstreamOrgId,
    UpstreamSubjectId,
};
use pplx_space_net_common::NetworkPolicyV1;
use pplx_space_public_api::{
    OperationId, OperationKind, OperationTargetType, PublicApiParseError, RequestFingerprint,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::public_operations::{
    ApiProduct, CreatePublicOperation, CreatePublicOperationIdempotency,
    CreatePublicOperationResult, HttpOperationMethod, OperationPathTemplate,
    PublicOperationAdmission, PublicOperationPayloadScrubRule, PublicOperationStore,
    PublicOperationStoreError,
};

pub const DEFAULT_FORK_COUNT: u32 = 1;
pub const DEFAULT_FORK_TIMEOUT_SECONDS: u32 = 300;
pub const DEFAULT_FORK_HARD_DEADLINE_SECONDS: u64 = 240;
const FORK_RESTORE_ACTIVATION_LEASE_MIN_SECONDS: u64 = 600;

pub(crate) fn fork_restore_activation_lease_seconds(hard_deadline: Duration) -> u32 {
    let lease = Duration::from_secs(FORK_RESTORE_ACTIVATION_LEASE_MIN_SECONDS).max(hard_deadline);
    u32::try_from(lease.as_secs()).unwrap_or(u32::MAX)
}

pub(crate) const FORK_FEDERATED_SERVICE_GRANT_JSON_PATH: &[&str] =
    &["payload", "federated_create", "service_grant_base64"];

pub const FORK_OPERATION_PAYLOAD_SCRUB_RULES: &[PublicOperationPayloadScrubRule] =
    &[PublicOperationPayloadScrubRule::new(
        OperationKind::ForkSandbox,
        FORK_FEDERATED_SERVICE_GRANT_JSON_PATH,
    )];

const FORK_PATH_TEMPLATE: OperationPathTemplate =
    OperationPathTemplate::new("/sandboxes/{sandboxID}/fork");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForkAvailabilityConfig {
    hard_deadline: std::time::Duration,
}

/// Fork admission defaults on; an explicit false value stops new admissions.
const FORK_ENABLED_ENV: &str = "SPACE_SANDBOX_FORK_ENABLED";

impl ForkAvailabilityConfig {
    pub fn from_env() -> Result<Self, ForkAvailabilityError> {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn from_environment(
        env: &dyn pplx_space_common_core::env::Environment,
    ) -> Result<Self, ForkAvailabilityError> {
        if !pplx_space_common_core::env::env_flag(env.var(FORK_ENABLED_ENV).ok().as_deref(), true) {
            return Err(ForkAvailabilityError::Disabled);
        }
        let hard_deadline = match env.var(FORK_HARD_DEADLINE_ENV) {
            Ok(value) => value
                .trim()
                .parse::<u64>()
                .ok()
                .filter(|seconds| *seconds > 0)
                .ok_or(ForkAvailabilityError::InvalidSeconds {
                    name: FORK_HARD_DEADLINE_ENV,
                })?,
            Err(_) => DEFAULT_FORK_HARD_DEADLINE_SECONDS,
        };
        let retention_grace = SANDBOX_FORK_SNAPSHOT_RETENTION_DEFAULT_GRACE_SECONDS;
        let storage_grace = SNAPSHOT_STORAGE_SWEEPER_DEFAULT_MIN_AGE_SECONDS;
        Self::validate(hard_deadline, retention_grace, storage_grace)
    }

    pub fn validate(
        hard_deadline_seconds: u64,
        retention_grace_seconds: u64,
        storage_grace_seconds: u64,
    ) -> Result<Self, ForkAvailabilityError> {
        if hard_deadline_seconds == 0 {
            return Err(ForkAvailabilityError::ZeroDeadline);
        }
        let restore_activation_lease_seconds = u64::from(fork_restore_activation_lease_seconds(
            Duration::from_secs(hard_deadline_seconds),
        ));
        if retention_grace_seconds <= restore_activation_lease_seconds {
            return Err(ForkAvailabilityError::GraceTooShort {
                grace: ForkArtifactGrace::Retention,
            });
        }
        if storage_grace_seconds <= restore_activation_lease_seconds {
            return Err(ForkAvailabilityError::GraceTooShort {
                grace: ForkArtifactGrace::Storage,
            });
        }
        Ok(Self {
            hard_deadline: std::time::Duration::from_secs(hard_deadline_seconds),
        })
    }

    pub const fn hard_deadline(self) -> std::time::Duration {
        self.hard_deadline
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForkArtifactGrace {
    Retention,
    Storage,
}

impl std::fmt::Display for ForkArtifactGrace {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Retention => "snapshot retention",
            Self::Storage => "snapshot storage",
        })
    }
}

const FORK_HARD_DEADLINE_ENV: &str = "SPACE_SANDBOX_FORK_HARD_DEADLINE_SECS";

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ForkAvailabilityError {
    #[error("sandbox fork is disabled")]
    Disabled,
    #[error("{name} must be a positive integer number of seconds")]
    InvalidSeconds { name: &'static str },
    #[error("sandbox fork hard deadline must be greater than zero")]
    ZeroDeadline,
    #[error("sandbox fork restore activation lease must be shorter than {grace} grace")]
    GraceTooShort { grace: ForkArtifactGrace },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct ForkCount(u8);

impl ForkCount {
    pub const fn get(self) -> u8 {
        self.0
    }
}

impl TryFrom<u32> for ForkCount {
    type Error = ForkRequestError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        if value != DEFAULT_FORK_COUNT {
            return Err(ForkRequestError::UnsupportedCount { value });
        }
        Ok(Self(value as u8))
    }
}

impl From<ForkCount> for u32 {
    fn from(value: ForkCount) -> Self {
        u32::from(value.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct ForkSandboxLifetimeSeconds(u32);

impl ForkSandboxLifetimeSeconds {
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl TryFrom<u32> for ForkSandboxLifetimeSeconds {
    type Error = ForkRequestError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        if value == 0 {
            return Err(ForkRequestError::ZeroTimeout);
        }
        Ok(Self(value))
    }
}

impl From<ForkSandboxLifetimeSeconds> for u32 {
    fn from(value: ForkSandboxLifetimeSeconds) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForkRequest {
    timeout: ForkSandboxLifetimeSeconds,
    count: ForkCount,
}

impl ForkRequest {
    pub fn from_wire(timeout: Option<u32>, count: Option<u32>) -> Result<Self, ForkRequestError> {
        Ok(Self {
            timeout: timeout.unwrap_or(DEFAULT_FORK_TIMEOUT_SECONDS).try_into()?,
            count: count.unwrap_or(DEFAULT_FORK_COUNT).try_into()?,
        })
    }

    pub const fn timeout(self) -> ForkSandboxLifetimeSeconds {
        self.timeout
    }

    pub const fn count(self) -> ForkCount {
        self.count
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ForkRequestError {
    #[error("fork count must be 1, got {value}")]
    UnsupportedCount { value: u32 },
    #[error("fork sandbox timeout must be greater than zero")]
    ZeroTimeout,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkSandboxPayloadV1 {
    source_sandbox_id: SandboxId,
    requested_timeout_seconds: ForkSandboxLifetimeSeconds,
    requested_count: ForkCount,
    hard_deadline_seconds: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ecw_host_domain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    federated_create: Option<ForkFederatedCreateContext>,
    #[serde(default, skip_serializing_if = "is_zero")]
    source_refreeze_count: u8,
    target_sandbox_id: SandboxId,
    workflow: ForkWorkflowState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", content = "state", rename_all = "snake_case")]
enum ForkWorkflowState {
    AwaitingSource,
    MaterializingArtifact(ForkMaterializingArtifact),
    RestoringTarget(ForkRestoringTarget),
    TargetCompleted(ForkCompletedTarget),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ForkMaterializingArtifact {
    source: ForkFrozenSource,
    capture_id: Uuid,
    progress: ForkArtifactProgress,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ForkFrozenSource {
    execution_generation: i64,
    lifecycle: ForkSourceLifecycle,
    target_config: ForkTargetConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "progress", rename_all = "snake_case")]
enum ForkArtifactProgress {
    Live(ForkLiveCaptureProgress),
    Suspended(ForkSuspendedArtifact),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ForkLiveCaptureProgress {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    capture_operation_id: Option<OperationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    local_artifact_id: Option<LocalArtifactId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    backup_attempt_id: Option<BackupAttemptId>,
    #[serde(default, skip_serializing_if = "is_zero")]
    backup_attempt_count: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ForkSuspendedArtifact {
    source_snapshot_id: SnapshotId,
    source_snapshot_storage_location_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ForkRestoringTarget {
    source: ForkFrozenSource,
    fork_snapshot_id: SnapshotId,
    fork_snapshot_storage_location_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ForkCompletedTarget {
    restore: ForkRestoringTarget,
    result: ForkTargetOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ForkWorkflowPhase {
    SourceFreeze,
    ArtifactMaterialization,
    TargetRestore,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum ForkTargetOutcome {
    Succeeded {
        sandbox_id: SandboxId,
        template_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ecw_host_domain: Option<String>,
        credential_epoch: EcwCredentialEpoch,
    },
    Failed {
        code: i32,
        message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForkCaptureProgress {
    Capture,
    EnqueueBackup(LocalArtifactId),
    ObserveBackup {
        local_artifact_id: LocalArtifactId,
        attempt_id: BackupAttemptId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkFederatedCreateContext {
    issuer: UpstreamIssuer,
    org_id: UpstreamOrgId,
    subject_id: UpstreamSubjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    service_grant_base64: Option<String>,
    /// The credential provider's expiry for the grant above, in unix seconds. Travels
    /// with the blob so the deferred create stores the same lifetime the request
    /// asserted. Not scrubbed: unlike the blob it is not a secret, and keeping it lets
    /// a restore distinguish "grant already consumed" from "payload predates the
    /// expiry".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    service_grant_expires_at_unix: Option<i64>,
    /// The credential provider's mint moment for the grant, in unix seconds. Carried so
    /// a restored fork-create persists with producer ordering intact; `default` keeps
    /// payloads queued before the field existed deserializable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    service_grant_issued_at_unix: Option<i64>,
}

impl ForkFederatedCreateContext {
    pub(crate) fn owner(&self) -> crate::federated_identity::FederatedOwner {
        crate::federated_identity::FederatedOwner {
            issuer: self.issuer.clone(),
            org_id: self.org_id.clone(),
            subject_id: self.subject_id.clone(),
        }
    }

    pub(crate) fn from_request(
        owner: &crate::federated_identity::FederatedOwner,
        service_grant: &crate::federated_identity::FederatedServiceGrant,
    ) -> Self {
        Self {
            issuer: owner.issuer.clone(),
            org_id: owner.org_id.clone(),
            subject_id: owner.subject_id.clone(),
            service_grant_base64: Some(BASE64_STANDARD.encode(service_grant.as_bytes())),
            service_grant_expires_at_unix: Some(service_grant.expires_at_unix()),
            service_grant_issued_at_unix: service_grant
                .issued_at()
                .map(|issued_at| issued_at.unix_timestamp()),
        }
    }

    pub fn restore(
        &self,
    ) -> Result<
        (
            crate::federated_identity::FederatedOwner,
            crate::federated_identity::FederatedServiceGrant,
        ),
        ForkFederatedCreateContextError,
    > {
        let encoded = self
            .service_grant_base64
            .as_deref()
            .ok_or(ForkFederatedCreateContextError::MissingGrant)?;
        let expires_at_unix = self
            .service_grant_expires_at_unix
            .ok_or(ForkFederatedCreateContextError::MissingGrantExpiry)?;
        let service_grant = BASE64_STANDARD
            .decode(encoded)
            .map_err(ForkFederatedCreateContextError::Decode)?;
        let mut grant = crate::federated_identity::FederatedServiceGrant::from_unix_expiry(
            service_grant,
            expires_at_unix,
        )
        .map_err(ForkFederatedCreateContextError::Grant)?;
        if let Some(issued_at_unix) = self.service_grant_issued_at_unix {
            grant = grant
                .with_issued_at_unix(issued_at_unix)
                .map_err(ForkFederatedCreateContextError::Grant)?;
        }
        Ok((
            crate::federated_identity::FederatedOwner {
                issuer: self.issuer.clone(),
                org_id: self.org_id.clone(),
                subject_id: self.subject_id.clone(),
            },
            grant,
        ))
    }

    fn scrub_service_grant(&mut self) {
        self.service_grant_base64 = None;
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ForkFederatedCreateContextError {
    #[error("federated service grant was already consumed")]
    MissingGrant,
    #[error("federated service grant payload carries no expiry")]
    MissingGrantExpiry,
    #[error("federated service grant encoding is invalid: {0}")]
    Decode(#[source] base64::DecodeError),
    #[error("federated service grant is invalid: {0}")]
    Grant(#[source] crate::federated_identity::FederatedServiceGrantError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForkSourceLifecycle {
    Started,
    Paused,
    Suspended,
}

#[derive(Debug, thiserror::Error)]
pub enum ForkSourceLifecycleParseError {
    #[error(transparent)]
    Invalid(#[from] SandboxLifecycleParseError),
    #[error("sandbox lifecycle {lifecycle} cannot be forked")]
    Unsupported { lifecycle: SandboxLifecycle },
}

impl FromStr for ForkSourceLifecycle {
    type Err = ForkSourceLifecycleParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.parse::<SandboxLifecycle>()? {
            SandboxLifecycle::Started => Ok(Self::Started),
            SandboxLifecycle::Paused => Ok(Self::Paused),
            SandboxLifecycle::Suspended => Ok(Self::Suspended),
            lifecycle => Err(ForkSourceLifecycleParseError::Unsupported { lifecycle }),
        }
    }
}

impl ForkSourceLifecycle {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Paused => "paused",
            Self::Suspended => "suspended",
        }
    }
}

impl std::fmt::Display for ForkSourceLifecycle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkTargetConfig {
    pub template: TemplateReference,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_template_id: Option<String>,
    pub runtime: Option<String>,
    pub env: BTreeMap<String, String>,
    pub metadata: crate::models::SandboxMetadata,
    pub vcpu: Option<u32>,
    pub memory_mib: Option<u32>,
    pub disk_mib: Option<u32>,
    pub auto_lifecycle: AutoLifecyclePolicy,
    pub network_policy: Option<NetworkPolicyV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "version", content = "payload")]
pub(crate) enum ForkSandboxPayload {
    #[serde(rename = "1")]
    V1(ForkSandboxPayloadV1),
}

impl ForkSandboxPayload {
    pub fn into_v1(self) -> ForkSandboxPayloadV1 {
        let Self::V1(payload) = self;
        payload
    }

    pub fn v1(payload: ForkSandboxPayloadV1) -> Self {
        Self::V1(payload)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("failed to deserialize fork payload: {0}")]
pub struct ForkPayloadDecodeError(#[from] serde_json::Error);

pub fn decode_fork_payload(
    value: serde_json::Value,
) -> Result<ForkSandboxPayloadV1, ForkPayloadDecodeError> {
    Ok(serde_json::from_value::<ForkSandboxPayload>(value)?.into_v1())
}

impl ForkSandboxPayloadV1 {
    pub fn new(
        source_sandbox_id: SandboxId,
        request: ForkRequest,
        ecw_host_domain: String,
    ) -> Self {
        Self::new_with_deadline(
            source_sandbox_id,
            request,
            ecw_host_domain,
            std::time::Duration::from_secs(DEFAULT_FORK_HARD_DEADLINE_SECONDS),
        )
    }

    pub fn new_with_deadline(
        source_sandbox_id: SandboxId,
        request: ForkRequest,
        ecw_host_domain: String,
        hard_deadline: std::time::Duration,
    ) -> Self {
        Self {
            source_sandbox_id,
            requested_timeout_seconds: request.timeout(),
            requested_count: request.count(),
            hard_deadline_seconds: hard_deadline.as_secs(),
            ecw_host_domain: Some(ecw_host_domain),
            federated_create: None,
            source_refreeze_count: 0,
            target_sandbox_id: SandboxId::new(),
            workflow: ForkWorkflowState::AwaitingSource,
        }
    }

    pub const fn source_sandbox_id(&self) -> SandboxId {
        self.source_sandbox_id
    }

    pub const fn requested_timeout_seconds(&self) -> ForkSandboxLifetimeSeconds {
        self.requested_timeout_seconds
    }

    pub const fn requested_count(&self) -> ForkCount {
        self.requested_count
    }

    pub const fn hard_deadline(&self) -> Duration {
        Duration::from_secs(self.hard_deadline_seconds)
    }

    pub fn ecw_host_domain(&self) -> Option<&str> {
        self.ecw_host_domain.as_deref()
    }

    pub fn federated_create(&self) -> Option<&ForkFederatedCreateContext> {
        self.federated_create.as_ref()
    }

    fn set_federated_create(
        &mut self,
        owner: &crate::federated_identity::FederatedOwner,
        service_grant: &crate::federated_identity::FederatedServiceGrant,
    ) {
        self.federated_create = Some(ForkFederatedCreateContext::from_request(
            owner,
            service_grant,
        ));
    }

    pub fn scrub_federated_service_grant(&mut self) -> bool {
        let Some(context) = self.federated_create.as_mut() else {
            return false;
        };
        if context.service_grant_base64.is_none() {
            return false;
        }
        context.scrub_service_grant();
        true
    }

    pub const fn target_sandbox_id(&self) -> SandboxId {
        self.target_sandbox_id
    }

    fn frozen_source(&self) -> Option<&ForkFrozenSource> {
        match &self.workflow {
            ForkWorkflowState::AwaitingSource => None,
            ForkWorkflowState::MaterializingArtifact(state) => Some(&state.source),
            ForkWorkflowState::RestoringTarget(state) => Some(&state.source),
            ForkWorkflowState::TargetCompleted(state) => Some(&state.restore.source),
        }
    }

    pub(crate) fn workflow_phase(&self) -> ForkWorkflowPhase {
        match self.workflow {
            ForkWorkflowState::AwaitingSource => ForkWorkflowPhase::SourceFreeze,
            ForkWorkflowState::MaterializingArtifact(_) => {
                ForkWorkflowPhase::ArtifactMaterialization
            }
            ForkWorkflowState::RestoringTarget(_) => ForkWorkflowPhase::TargetRestore,
            ForkWorkflowState::TargetCompleted(_) => ForkWorkflowPhase::Completed,
        }
    }

    pub fn source_execution_generation(&self) -> Option<i64> {
        self.frozen_source()
            .map(|source| source.execution_generation)
    }

    pub fn source_lifecycle(&self) -> Option<ForkSourceLifecycle> {
        self.frozen_source().map(|source| source.lifecycle)
    }

    pub fn target_config(&self) -> Option<&ForkTargetConfig> {
        self.frozen_source().map(|source| &source.target_config)
    }

    pub fn capture_id(&self) -> Option<Uuid> {
        match self.workflow {
            ForkWorkflowState::MaterializingArtifact(ref state) => Some(state.capture_id),
            _ => None,
        }
    }

    fn live_capture(&self) -> Option<&ForkLiveCaptureProgress> {
        match &self.workflow {
            ForkWorkflowState::MaterializingArtifact(ForkMaterializingArtifact {
                progress: ForkArtifactProgress::Live(progress),
                ..
            }) => Some(progress),
            _ => None,
        }
    }

    fn live_capture_mut(&mut self) -> Option<&mut ForkLiveCaptureProgress> {
        match &mut self.workflow {
            ForkWorkflowState::MaterializingArtifact(ForkMaterializingArtifact {
                progress: ForkArtifactProgress::Live(progress),
                ..
            }) => Some(progress),
            _ => None,
        }
    }

    pub fn local_artifact_id(&self) -> Option<LocalArtifactId> {
        self.live_capture()
            .and_then(|progress| progress.local_artifact_id)
    }

    pub fn capture_progress(&self) -> ForkCaptureProgress {
        let progress = self.live_capture();
        match (
            progress.and_then(|state| state.local_artifact_id),
            progress.and_then(|state| state.backup_attempt_id),
        ) {
            (None, _) => ForkCaptureProgress::Capture,
            (Some(local_artifact_id), None) => {
                ForkCaptureProgress::EnqueueBackup(local_artifact_id)
            }
            (Some(local_artifact_id), Some(attempt_id)) => ForkCaptureProgress::ObserveBackup {
                local_artifact_id,
                attempt_id,
            },
        }
    }

    fn restore_state(&self) -> Option<&ForkRestoringTarget> {
        match &self.workflow {
            ForkWorkflowState::RestoringTarget(state) => Some(state),
            ForkWorkflowState::TargetCompleted(state) => Some(&state.restore),
            _ => None,
        }
    }

    pub fn fork_snapshot_id(&self) -> Option<SnapshotId> {
        self.restore_state().map(|state| state.fork_snapshot_id)
    }

    fn suspended_artifact(&self) -> Option<&ForkSuspendedArtifact> {
        match &self.workflow {
            ForkWorkflowState::MaterializingArtifact(ForkMaterializingArtifact {
                progress: ForkArtifactProgress::Suspended(artifact),
                ..
            }) => Some(artifact),
            _ => None,
        }
    }

    pub fn source_snapshot_id(&self) -> Option<SnapshotId> {
        self.suspended_artifact()
            .map(|artifact| artifact.source_snapshot_id)
    }

    pub fn source_snapshot_storage_location_id(&self) -> Option<Uuid> {
        self.suspended_artifact()
            .map(|artifact| artifact.source_snapshot_storage_location_id)
    }

    pub fn fork_snapshot_storage_location_id(&self) -> Option<Uuid> {
        self.restore_state()
            .map(|state| state.fork_snapshot_storage_location_id)
    }

    pub fn capture_operation_id(&self) -> Option<OperationId> {
        self.live_capture()
            .and_then(|progress| progress.capture_operation_id)
    }

    pub fn set_capture_operation_id(&mut self, operation_id: OperationId) {
        self.live_capture_mut()
            .expect("capture leader is selected only for a live artifact")
            .capture_operation_id = Some(operation_id);
    }

    pub fn clear_capture_operation_id(&mut self) {
        self.live_capture_mut()
            .expect("capture leader is cleared only for a live artifact")
            .capture_operation_id = None;
    }

    pub fn freeze_source(
        &mut self,
        execution_generation: i64,
        lifecycle: ForkSourceLifecycle,
        target_config: ForkTargetConfig,
        source_snapshot: Option<(SnapshotId, Uuid)>,
    ) {
        let source = ForkFrozenSource {
            execution_generation,
            lifecycle,
            target_config,
        };
        let progress = match (lifecycle, source_snapshot) {
            (
                ForkSourceLifecycle::Suspended,
                Some((source_snapshot_id, source_snapshot_storage_location_id)),
            ) => ForkArtifactProgress::Suspended(ForkSuspendedArtifact {
                source_snapshot_id,
                source_snapshot_storage_location_id,
            }),
            (ForkSourceLifecycle::Started | ForkSourceLifecycle::Paused, None) => {
                ForkArtifactProgress::Live(ForkLiveCaptureProgress {
                    capture_operation_id: None,
                    local_artifact_id: None,
                    backup_attempt_id: None,
                    backup_attempt_count: 0,
                })
            }
            _ => unreachable!("source artifact identity must match source lifecycle"),
        };
        self.workflow = ForkWorkflowState::MaterializingArtifact(ForkMaterializingArtifact {
            source,
            capture_id: Uuid::now_v7(),
            progress,
        });
    }

    pub fn record_local_artifact(&mut self, local_artifact_id: LocalArtifactId) {
        self.live_capture_mut()
            .expect("local artifacts belong only to live capture")
            .local_artifact_id = Some(local_artifact_id);
    }

    pub fn record_backup_attempt(&mut self, attempt_id: BackupAttemptId) {
        let progress = self
            .live_capture_mut()
            .expect("backup attempts belong only to live capture");
        progress.backup_attempt_id = Some(attempt_id);
        progress.backup_attempt_count = progress.backup_attempt_count.saturating_add(1);
    }

    pub fn backup_attempt_count(&self) -> u8 {
        self.live_capture()
            .map_or(0, |progress| progress.backup_attempt_count)
    }

    pub fn retry_backup(&mut self) {
        self.live_capture_mut()
            .expect("backup retries belong only to live capture")
            .backup_attempt_id = None;
    }

    pub const fn source_refreeze_count(&self) -> u8 {
        self.source_refreeze_count
    }

    pub fn reset_source_for_refreeze(&mut self) {
        self.source_refreeze_count = self.source_refreeze_count.saturating_add(1);
        self.workflow = ForkWorkflowState::AwaitingSource;
    }

    pub(crate) fn record_target_result(
        &mut self,
        sandbox_id: SandboxId,
        result: ForkTargetOutcome,
    ) {
        assert_eq!(sandbox_id, self.target_sandbox_id);
        self.workflow = match self.workflow.clone() {
            ForkWorkflowState::RestoringTarget(restore) => {
                ForkWorkflowState::TargetCompleted(ForkCompletedTarget { restore, result })
            }
            ForkWorkflowState::TargetCompleted(mut completed) => {
                completed.result = result;
                ForkWorkflowState::TargetCompleted(completed)
            }
            _ => unreachable!("target results follow durable artifact resolution"),
        };
    }

    pub(crate) fn target_result(&self, sandbox_id: SandboxId) -> Option<&ForkTargetOutcome> {
        if sandbox_id != self.target_sandbox_id {
            return None;
        }
        match &self.workflow {
            ForkWorkflowState::TargetCompleted(completed) => Some(&completed.result),
            _ => None,
        }
    }

    pub(crate) fn completed_target_results(&self) -> Option<Vec<ForkTargetOutcome>> {
        match &self.workflow {
            ForkWorkflowState::TargetCompleted(completed) => Some(vec![completed.result.clone()]),
            _ => None,
        }
    }

    pub fn resolve_fork_snapshot(&mut self, snapshot_id: SnapshotId, storage_location_id: Uuid) {
        let source = match self.workflow.clone() {
            ForkWorkflowState::MaterializingArtifact(materializing) => materializing.source,
            _ => unreachable!("durable artifact resolution follows materialization"),
        };
        self.workflow = ForkWorkflowState::RestoringTarget(ForkRestoringTarget {
            source,
            fork_snapshot_id: snapshot_id,
            fork_snapshot_storage_location_id: storage_location_id,
        });
    }
}

const fn is_zero(value: &u8) -> bool {
    *value == 0
}

pub struct AdmitForkOperation<'a> {
    pub org_id: OrgId,
    pub actor_user_id: Option<UserId>,
    pub source_sandbox_id: SandboxId,
    pub request: ForkRequest,
    pub ecw_host_domain: &'a str,
    pub hard_deadline: std::time::Duration,
    pub federated_owner: Option<&'a crate::federated_identity::FederatedOwner>,
    pub federated_service_grant: Option<&'a crate::federated_identity::FederatedServiceGrant>,
    pub idempotency_key: &'a IdempotencyKey,
}

#[derive(Debug, thiserror::Error)]
pub enum AdmitForkOperationError {
    #[error("failed to serialize fork operation state: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("generated fork request fingerprint is invalid: {0}")]
    Fingerprint(#[from] PublicApiParseError),
    #[error("failed to persist fork operation: {0}")]
    Store(#[from] PublicOperationStoreError),
    #[error("federated fork admission requires both owner and service grant")]
    IncompleteFederatedContext,
}

#[cfg(test)]
async fn admit_fork_operation(
    store: &PublicOperationStore,
    input: AdmitForkOperation<'_>,
) -> Result<CreatePublicOperationResult, AdmitForkOperationError> {
    let prepared = prepare_fork_operation(&input)?;
    store
        .create_operation(fork_operation_input(&input, &prepared))
        .await
        .map_err(Into::into)
}

pub async fn admit_locked_fork_operation(
    mut admission: PublicOperationAdmission,
    input: AdmitForkOperation<'_>,
) -> Result<CreatePublicOperationResult, AdmitForkOperationError> {
    let prepared = prepare_fork_operation(&input)?;
    if matches!(
        admission.observation(),
        crate::public_operations::ObservePublicOperationIdempotency::Missing
    ) && !admission
        .lock_target_lifecycle(input.source_sandbox_id)
        .await?
    {
        return Ok(CreatePublicOperationResult::AdmissionDenied);
    }
    admission
        .create(fork_operation_input(&input, &prepared))
        .await
        .map_err(Into::into)
}
struct PreparedForkOperation {
    source_id: String,
    payload_json: serde_json::Value,
    fingerprint: RequestFingerprint,
}

fn prepare_fork_operation(
    input: &AdmitForkOperation<'_>,
) -> Result<PreparedForkOperation, AdmitForkOperationError> {
    let mut payload = ForkSandboxPayloadV1::new_with_deadline(
        input.source_sandbox_id,
        input.request,
        input.ecw_host_domain.to_string(),
        input.hard_deadline,
    );
    match (input.federated_owner, input.federated_service_grant) {
        (Some(owner), Some(service_grant)) => {
            payload.set_federated_create(owner, service_grant);
        }
        (None, None) => {}
        _ => return Err(AdmitForkOperationError::IncompleteFederatedContext),
    }
    let fingerprint = fork_request_fingerprint(
        input.source_sandbox_id,
        input.request,
        input.federated_owner.map(|owner| &owner.subject_id),
    )?;
    let source_id = input.source_sandbox_id.to_string();
    let payload_json = serde_json::to_value(ForkSandboxPayload::v1(payload))?;
    Ok(PreparedForkOperation {
        source_id,
        payload_json,
        fingerprint,
    })
}

fn fork_operation_input<'a>(
    input: &'a AdmitForkOperation<'a>,
    prepared: &'a PreparedForkOperation,
) -> CreatePublicOperation<'a> {
    CreatePublicOperation {
        org_id: input.org_id,
        actor_user_id: input.actor_user_id,
        api_product: ApiProduct::Native,
        kind: OperationKind::ForkSandbox,
        target_type: Some(OperationTargetType::Sandbox),
        target_id: Some(&prepared.source_id),
        payload_json: prepared.payload_json.clone(),
        idempotency: Some(CreatePublicOperationIdempotency {
            method: HttpOperationMethod::Post,
            path_template: FORK_PATH_TEMPLATE,
            key: input.idempotency_key,
            request_fingerprint: &prepared.fingerprint,
        }),
        traceparent: crate::public_operations::request_traceparent(),
    }
}

pub async fn begin_fork_operation_admission(
    store: &PublicOperationStore,
    org_id: OrgId,
    source_sandbox_id: SandboxId,
    request: ForkRequest,
    federated_subject_id: Option<&UpstreamSubjectId>,
    idempotency_key: &IdempotencyKey,
) -> Result<PublicOperationAdmission, AdmitForkOperationError> {
    let fingerprint = fork_request_fingerprint(source_sandbox_id, request, federated_subject_id)?;
    store
        .begin_operation_admission(
            org_id,
            ApiProduct::Native,
            &CreatePublicOperationIdempotency {
                method: HttpOperationMethod::Post,
                path_template: FORK_PATH_TEMPLATE,
                key: idempotency_key,
                request_fingerprint: &fingerprint,
            },
        )
        .await
        .map_err(Into::into)
}

pub async fn observe_fork_operation_idempotency(
    store: &PublicOperationStore,
    org_id: OrgId,
    source_sandbox_id: SandboxId,
    request: ForkRequest,
    federated_subject_id: Option<&UpstreamSubjectId>,
    idempotency_key: &IdempotencyKey,
) -> Result<crate::public_operations::ObservePublicOperationIdempotency, AdmitForkOperationError> {
    let fingerprint = fork_request_fingerprint(source_sandbox_id, request, federated_subject_id)?;
    store
        .observe_operation_idempotency(
            org_id,
            ApiProduct::Native,
            &CreatePublicOperationIdempotency {
                method: HttpOperationMethod::Post,
                path_template: FORK_PATH_TEMPLATE,
                key: idempotency_key,
                request_fingerprint: &fingerprint,
            },
        )
        .await
        .map_err(Into::into)
}

#[derive(Serialize)]
struct ForkRequestFingerprint {
    source_sandbox_id: SandboxId,
    requested_timeout_seconds: ForkSandboxLifetimeSeconds,
    requested_count: ForkCount,
    #[serde(skip_serializing_if = "Option::is_none")]
    federated_subject_id: Option<UpstreamSubjectId>,
}

fn fork_request_fingerprint(
    source_sandbox_id: SandboxId,
    request: ForkRequest,
    federated_subject_id: Option<&UpstreamSubjectId>,
) -> Result<RequestFingerprint, AdmitForkOperationError> {
    let bytes = serde_json::to_vec(&ForkRequestFingerprint {
        source_sandbox_id,
        requested_timeout_seconds: request.timeout(),
        requested_count: request.count(),
        federated_subject_id: federated_subject_id.cloned(),
    })?;
    RequestFingerprint::parse(hex::encode(Sha256::digest(bytes))).map_err(Into::into)
}

#[cfg(test)]
mod tests;
