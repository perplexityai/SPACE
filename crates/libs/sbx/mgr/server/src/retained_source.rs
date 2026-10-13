//! Retained btrfs send sources from deferred capture.
//!
//! Deferred capture skips `rootfs.btrfs-send` and leaves an immutable source
//! bundle under `rootfs.btrfs-source/` in the artifact directory. The bundle's
//! source subvolume keeps the sandbox jail's ownership, which this service
//! cannot traverse. Node-manager is therefore the only authority over a
//! retained source. Sandbox-manager knows a source by the identity node-manager
//! returned at capture, persisted as `snapshot.json` `rootfs_send_source_id`,
//! and asks node-manager whether it is still usable. It never opens the bundle.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use futures_util::StreamExt as _;

use crate::snapshot_upload::{ROOTFS_BTRFS_SEND_FILENAME, SnapshotMeta};

/// Node-manager's bundle directory. Named in errors only; never opened here.
pub(crate) const BUNDLE_DIRECTORY: &str = "rootfs.btrfs-source";

/// Per-call deadline for one inspection.
const INSPECTION_DEADLINE: Duration = Duration::from_secs(2);
/// Background inspections in flight per node.
const BACKGROUND_INSPECTIONS: usize = 2;
/// Inspection time a background pass may spend before deferring the rest.
pub(crate) const BACKGROUND_INSPECTION_BUDGET: Duration = Duration::from_secs(10);

/// A retained source as sandbox-manager knows it: which sandbox and artifact
/// directory hold it, and its identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct RetainedSource {
    pub(crate) sandbox_id: String,
    pub(crate) artifact_dir: PathBuf,
    pub(crate) source_id: uuid::Uuid,
}

/// The btrfs-send rootfs of a local artifact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RootfsArtifact {
    /// A materialized `rootfs.btrfs-send` stream. A retained source, if any,
    /// regenerates it after reclaim and sizes the artifact once it is gone.
    SendFile { path: PathBuf },
    /// No stream file: a persisted identity names the retained source.
    RetainedSource(RetainedSource),
}

impl RootfsArtifact {
    /// A stream file is the rootfs while it exists. Otherwise a persisted
    /// identity names a retained source, and without either the btrfs-send
    /// rootfs is missing. Reads no path under the bundle.
    pub(crate) fn resolve(
        sandbox_id: &str,
        artifact_dir: &Path,
        source_id: Option<uuid::Uuid>,
    ) -> Result<Option<Self>, RootfsResolveError> {
        let path = artifact_dir.join(ROOTFS_BTRFS_SEND_FILENAME);
        match std::fs::symlink_metadata(&path) {
            Ok(_) => Ok(Some(Self::SendFile { path })),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(source_id.map(|source_id| {
                    Self::RetainedSource(RetainedSource {
                        sandbox_id: sandbox_id.to_owned(),
                        artifact_dir: artifact_dir.to_owned(),
                        source_id,
                    })
                }))
            }
            Err(source) => Err(RootfsResolveError { path, source }),
        }
    }

    pub(crate) fn resolve_meta(
        meta: &SnapshotMeta,
        artifact_dir: &Path,
    ) -> Result<Option<Self>, RootfsResolveError> {
        Self::resolve(&meta.sandbox_id, artifact_dir, meta.rootfs_send_source_id)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("stat {path} failed: {source}")]
pub(crate) struct RootfsResolveError {
    pub(crate) path: PathBuf,
    #[source]
    pub(crate) source: std::io::Error,
}

/// Node-manager's answer about one retained source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RetainedSourceCheck {
    /// Usable. The length is the retained rootfs image, never a send stream.
    Present { rootfs_bytes: u64 },
    /// The bundle is gone.
    Absent,
    /// The bundle exists but cannot be used: its identity, lineage, or
    /// read-only state changed, or its rootfs is missing.
    Invalid { reason: String },
    /// No answer, including from a node-manager without inspection. Never
    /// evidence about the artifact.
    Unavailable { reason: String },
}

impl RetainedSourceCheck {
    pub(crate) fn from_status(status: &tonic::Status) -> Self {
        match status.code() {
            tonic::Code::NotFound => Self::Absent,
            tonic::Code::FailedPrecondition => Self::Invalid {
                reason: status.message().to_owned(),
            },
            _ => Self::Unavailable {
                reason: status.to_string(),
            },
        }
    }

    fn record_metric(&self, code: Option<tonic::Code>) {
        let outcome = match self {
            Self::Present { .. } => "present",
            Self::Absent => "absent",
            Self::Invalid { .. } => "invalid",
            Self::Unavailable { .. } => "unavailable",
        };
        pplx_space_metrics::snapshot::retained_source_inspection(
            outcome,
            code.map(code_tag)
                .filter(|_| matches!(self, Self::Unavailable { .. })),
        );
    }
}

impl std::fmt::Display for RetainedSourceCheck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Present { rootfs_bytes } => write!(f, "present ({rootfs_bytes} rootfs bytes)"),
            Self::Absent => f.write_str("absent"),
            Self::Invalid { reason } => write!(f, "unusable: {reason}"),
            Self::Unavailable { reason } => write!(f, "unverified: {reason}"),
        }
    }
}

fn code_tag(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Unimplemented => "unimplemented",
        tonic::Code::Unavailable => "unavailable",
        tonic::Code::DeadlineExceeded => "deadline_exceeded",
        tonic::Code::ResourceExhausted => "resource_exhausted",
        tonic::Code::PermissionDenied => "permission_denied",
        tonic::Code::Unauthenticated => "unauthenticated",
        tonic::Code::Internal => "internal",
        _ => "other",
    }
}

/// Asks node-manager about retained sources. Injected so tests can substitute
/// verdicts without a btrfs node-manager.
#[async_trait::async_trait]
pub(crate) trait RetainedSourceInspector: Send + Sync {
    async fn inspect(&self, source: &RetainedSource) -> RetainedSourceCheck;
}

/// One node-manager call with a bounded deadline.
pub(crate) struct NodeManagerSourceInspector(
    pub(crate) crate::node_manager_transport::NodeManagerConnector,
);

#[async_trait::async_trait]
impl RetainedSourceInspector for NodeManagerSourceInspector {
    async fn inspect(&self, source: &RetainedSource) -> RetainedSourceCheck {
        let call = crate::btrfs_via_nodemgr::inspect_send_source(
            &source.sandbox_id,
            &source.artifact_dir,
            source.source_id,
            &self.0,
        );
        let (check, code) = match tokio::time::timeout(INSPECTION_DEADLINE, call).await {
            Ok(Ok(rootfs_bytes)) => (RetainedSourceCheck::Present { rootfs_bytes }, None),
            Ok(Err(error)) => {
                let status = error.into_status();
                (
                    RetainedSourceCheck::from_status(&status),
                    Some(status.code()),
                )
            }
            Err(_) => (
                RetainedSourceCheck::Unavailable {
                    reason: format!(
                        "node-manager inspection exceeded {}s",
                        INSPECTION_DEADLINE.as_secs()
                    ),
                },
                Some(tonic::Code::DeadlineExceeded),
            ),
        };
        check.record_metric(code);
        if let RetainedSourceCheck::Unavailable { reason } = &check {
            tracing::debug!(
                sandbox_id = %source.sandbox_id,
                artifact_dir = %source.artifact_dir.display(),
                source_id = %source.source_id,
                reason,
                "retained send source inspection unavailable"
            );
        }
        check
    }
}

/// Presence of a btrfs-send rootfs for a backup precondition. A retained
/// source is verified by node-manager, never by reading its bundle.
pub(crate) enum BtrfsSendRootfsPresence {
    Present,
    Missing { path: PathBuf },
    Unusable { detail: String },
    Unverified { detail: String },
}

pub(crate) async fn btrfs_send_rootfs_presence(
    inspector: &dyn RetainedSourceInspector,
    sandbox_id: &str,
    artifact_dir: &Path,
    source_id: Option<uuid::Uuid>,
) -> BtrfsSendRootfsPresence {
    let source = match RootfsArtifact::resolve(sandbox_id, artifact_dir, source_id) {
        Ok(Some(RootfsArtifact::SendFile { .. })) => return BtrfsSendRootfsPresence::Present,
        Ok(Some(RootfsArtifact::RetainedSource(source))) => source,
        Ok(None) => {
            return BtrfsSendRootfsPresence::Missing {
                path: artifact_dir.join(ROOTFS_BTRFS_SEND_FILENAME),
            };
        }
        Err(error) => {
            return BtrfsSendRootfsPresence::Unverified {
                detail: error.to_string(),
            };
        }
    };
    match inspector.inspect(&source).await {
        RetainedSourceCheck::Present { .. } => BtrfsSendRootfsPresence::Present,
        RetainedSourceCheck::Absent => BtrfsSendRootfsPresence::Missing {
            path: artifact_dir.join(BUNDLE_DIRECTORY),
        },
        RetainedSourceCheck::Invalid { reason } => BtrfsSendRootfsPresence::Unusable {
            detail: format!(
                "retained send source {} in {} is unusable: {reason}",
                source.source_id,
                artifact_dir.display()
            ),
        },
        RetainedSourceCheck::Unavailable { reason } => BtrfsSendRootfsPresence::Unverified {
            detail: format!(
                "could not verify retained send source {} in {}: {reason}",
                source.source_id,
                artifact_dir.display()
            ),
        },
    }
}

/// Verdicts gathered by an async caller for synchronous validation, which
/// must not wait on node-manager. A source without a verdict is pending:
/// neither complete nor evidence of loss. Validation records every source it
/// could not resolve, for the pending signals.
#[derive(Debug, Default)]
pub(crate) struct RetainedSourceVerdicts {
    verdicts: HashMap<RetainedSource, RetainedSourceCheck>,
    unresolved: Mutex<BTreeSet<RetainedSource>>,
}

impl RetainedSourceVerdicts {
    pub(crate) fn record(&mut self, source: RetainedSource, check: RetainedSourceCheck) {
        self.verdicts.insert(source, check);
    }

    /// The verdict for `source`, or `Unavailable` when none was gathered.
    pub(crate) fn check(&self, source: &RetainedSource) -> RetainedSourceCheck {
        let check = self.verdicts.get(source).cloned().unwrap_or_else(|| {
            RetainedSourceCheck::Unavailable {
                reason: "awaiting node-manager inspection".to_owned(),
            }
        });
        if matches!(check, RetainedSourceCheck::Unavailable { .. }) {
            self.unresolved
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(source.clone());
        }
        check
    }

    /// Sources with an answer other than unavailable.
    pub(crate) fn answered(&self) -> impl Iterator<Item = &RetainedSource> {
        self.verdicts
            .iter()
            .filter(|(_, check)| !matches!(check, RetainedSourceCheck::Unavailable { .. }))
            .map(|(source, _)| source)
    }

    /// Sources a validation could not resolve since the last call.
    pub(crate) fn take_unresolved(&self) -> Vec<RetainedSource> {
        std::mem::take(
            &mut *self
                .unresolved
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
        .into_iter()
        .collect()
    }
}

/// When each source was last handed to node-manager. Scheduling state only:
/// it orders the next batch so slow sources cannot keep the rest waiting, and
/// it never answers for a source.
#[derive(Debug, Default)]
pub(crate) struct InspectionSchedule {
    attempted: HashMap<RetainedSource, tokio::time::Instant>,
}

impl InspectionSchedule {
    /// Never-tried sources first, then the longest since their last try.
    fn order(
        &mut self,
        sources: BTreeSet<RetainedSource>,
        now: tokio::time::Instant,
    ) -> Vec<RetainedSource> {
        self.attempted
            .retain(|_, at| now.saturating_duration_since(*at) < PENDING_FORGET_AFTER);
        let mut sources: Vec<_> = sources.into_iter().collect();
        // Stable, so ties keep a deterministic identity order.
        sources.sort_by_key(|source| self.attempted.get(source).copied());
        sources
    }
}

/// Inspect `sources` in the background lane: bounded concurrency, and at most
/// `budget` before the rest are left unanswered. Callers inspect exactly the
/// batch they are about to validate; the next batch asks again, starting with
/// the sources this one did not reach.
pub(crate) async fn inspect_in_background(
    inspector: &dyn RetainedSourceInspector,
    sources: impl IntoIterator<Item = RetainedSource>,
    budget: Duration,
    schedule: &mut InspectionSchedule,
) -> RetainedSourceVerdicts {
    let start = tokio::time::Instant::now();
    let order = schedule.order(sources.into_iter().collect(), start);
    let started = Mutex::new(Vec::new());
    let mut inspections = futures_util::stream::iter(order)
        .map(|source| {
            let started = &started;
            async move {
                started
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(source.clone());
                let check = inspector.inspect(&source).await;
                (source, check)
            }
        })
        .buffer_unordered(BACKGROUND_INSPECTIONS);
    let mut verdicts = RetainedSourceVerdicts::default();
    while let Ok(Some((source, check))) =
        tokio::time::timeout_at(start + budget, inspections.next()).await
    {
        verdicts.record(source, check);
    }
    drop(inspections);
    let now = tokio::time::Instant::now();
    for source in started
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
    {
        schedule.attempted.insert(source, now);
    }
    verdicts
}

/// A source left unresolved by periodic recovery stops counting as pending
/// once no pass has asked about it for this long, such as after a resume.
const PENDING_FORGET_AFTER: Duration = Duration::from_secs(60 * 60);

/// Sources periodic recovery could not resolve, across its paginated passes.
#[derive(Debug, Default)]
pub(crate) struct PendingRetainedSources {
    /// First and latest pass that could not resolve each source.
    seen: BTreeMap<RetainedSource, (Instant, Instant)>,
}

impl PendingRetainedSources {
    /// Fold in the passes of one tick. A source stays pending from its first
    /// unresolved pass until a pass gets an answer for it, so pages that do
    /// not include it neither reset nor clear its age.
    pub(crate) fn observe(&mut self, passes: &[&RetainedSourceVerdicts], now: Instant) {
        for verdicts in passes {
            for source in verdicts.take_unresolved() {
                self.seen
                    .entry(source)
                    .and_modify(|(_, latest)| *latest = now)
                    .or_insert((now, now));
            }
            for source in verdicts.answered() {
                self.seen.remove(source);
            }
        }
        self.seen
            .retain(|_, (_, latest)| now.saturating_duration_since(*latest) < PENDING_FORGET_AFTER);
        let oldest = self
            .seen
            .values()
            .map(|(first, _)| now.saturating_duration_since(*first))
            .max()
            .unwrap_or_default();
        pplx_space_metrics::snapshot::retained_source_pending(
            self.seen.len(),
            oldest.as_secs_f64(),
        );
    }
}

/// Answers every inspection with one configurable verdict and records calls.
#[cfg(test)]
pub(crate) struct FakeInspector {
    answer: Mutex<RetainedSourceCheck>,
    calls: Mutex<Vec<RetainedSource>>,
}

#[cfg(test)]
impl FakeInspector {
    pub(crate) fn new(answer: RetainedSourceCheck) -> Self {
        Self {
            answer: Mutex::new(answer),
            calls: Mutex::default(),
        }
    }

    pub(crate) fn answer(&self, answer: RetainedSourceCheck) {
        *self
            .answer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = answer;
    }

    pub(crate) fn calls(&self) -> Vec<RetainedSource> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl RetainedSourceInspector for FakeInspector {
    async fn inspect(&self, source: &RetainedSource) -> RetainedSourceCheck {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(source.clone());
        self.answer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[cfg(test)]
mod tests;
