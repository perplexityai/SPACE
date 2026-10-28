use std::str::FromStr;

use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, ExecutionGeneration, PodUid, SandboxCleanupEpisodeId, SandboxId,
    SandboxQuarantineReasonCode, SandboxTransitionTargetState,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ConsumeActivationReceiptsError {
    #[error("consume activation completion receipts: {0}")]
    Consume(#[source] sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SelectActivationRollbacksError {
    #[error("select activation rollback receipts: {0}")]
    Select(#[source] sqlx::Error),
    #[error("parse activation rollback target for {sandbox_id}: {source}")]
    Target {
        sandbox_id: SandboxId,
        #[source]
        source: <SandboxTransitionTargetState as FromStr>::Err,
    },
    #[error("parse activation rollback reason for {sandbox_id}: {source}")]
    Reason {
        sandbox_id: SandboxId,
        #[source]
        source: <SandboxQuarantineReasonCode as FromStr>::Err,
    },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ApplyActivationRollbacksError {
    #[error("retire rollback execution: {0}")]
    RetireExecution(#[source] sqlx::Error),
    #[error("invalidate rollback port state: {0}")]
    InvalidatePorts(#[source] sqlx::Error),
    #[error("converge activation rollback: {0}")]
    Converge(#[source] sqlx::Error),
    #[error("activation rollback receipt {0} lost sandbox ownership")]
    LostOwnership(SandboxCleanupEpisodeId),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RecoverPauseFailedError {
    #[error("select pause-failed quarantine recoveries: {0}")]
    Select(#[source] sqlx::Error),
    #[error("parse pause-failed quarantine recovery target for {sandbox_id}: {source}")]
    Target {
        sandbox_id: SandboxId,
        #[source]
        source: <SandboxTransitionTargetState as FromStr>::Err,
    },
    #[error("unsupported node-reported quarantine recovery target {0}")]
    UnsupportedTarget(SandboxTransitionTargetState),
    #[error("record node quarantine recovery: {0}")]
    Record(#[source] sqlx::Error),
    #[error("pause-failed recovery CAS failed for {0}")]
    LostOwnership(SandboxId),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SelectSuspendedProjectionsError {
    #[error("select suspended projection candidates: {0}")]
    Select(#[source] sqlx::Error),
    #[error("parse suspended projection artifact kind for {sandbox_id}: {source}")]
    ArtifactKind {
        sandbox_id: SandboxId,
        #[source]
        source: <ArtifactKind as FromStr>::Err,
    },
    #[error("parse suspended projection artifact origin for {sandbox_id}: {source}")]
    ArtifactOrigin {
        sandbox_id: SandboxId,
        #[source]
        source: <ArtifactOrigin as FromStr>::Err,
    },
    #[error("suspended projection candidate {0} has a partial published artifact")]
    PartialArtifact(SandboxId),
    #[error("parse suspended projection generation for {sandbox_id}: {source}")]
    Generation {
        sandbox_id: SandboxId,
        #[source]
        source: <ExecutionGeneration as FromStr>::Err,
    },
    #[error("parse suspended projection pod UID for {sandbox_id}: {source}")]
    PodUid {
        sandbox_id: SandboxId,
        #[source]
        source: <PodUid as FromStr>::Err,
    },
    #[error("suspended projection candidate {0} has a partial execution identity")]
    PartialExecutionIdentity(SandboxId),
    #[error("quarantine recovery candidate {0} has no episode")]
    MissingEpisode(SandboxId),
    #[error("parse quarantine recovery reason for {sandbox_id}: {source}")]
    RecoveryReason {
        sandbox_id: SandboxId,
        #[source]
        source: <SandboxQuarantineReasonCode as FromStr>::Err,
    },
    #[error("quarantine recovery candidate {0} has a partial receipt")]
    PartialRecoveryReceipt(SandboxId),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RecordQuarantineRecoveryError {
    #[error("record quarantine recovery: {0}")]
    Record(#[source] sqlx::Error),
    #[error("quarantine recovery history CAS affected {rows} rows for {sandbox_id}")]
    HistoryCas { sandbox_id: SandboxId, rows: u64 },
    #[error("retire quarantine recovery: {0}")]
    Retire(#[source] sqlx::Error),
    #[error("quarantine recovery retirement CAS affected {rows} rows for {sandbox_id}")]
    RetirementCas { sandbox_id: SandboxId, rows: u64 },
}
