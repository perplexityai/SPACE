//! The drain's work: the suspends, disk publications and uploads it starts
//! as detached state writers, and what it remembers of them between calls.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use super::planner::*;
use crate::db::activation_seal::DrainPrior;
use crate::proto::EnqueueArtifactBackupRequest;
use crate::service::grpc_lifecycle::SuspendRequestContext;
use crate::service::*;
use pplx_space_events::SandboxEventTrigger;

/// How many of the drain's suspends and publications may be in flight on the
/// node at once, the uploads they leave pending included, unless
/// [`DRAIN_NODE_WINDOW_ENV`] sets another positive number. Beside the idle
/// auto-suspend scheduler's 8, at most 16 suspends from these two callers
/// reach node-manager's Filesystem lane at once, below the 20 arrivals (4
/// permits and 16 waiting, its default) past which the lane refuses one,
/// which would roll that suspend back. The lane is shared: a caller's
/// suspend or snapshot, a snapshot rollback or restore, the rootfs sends and
/// receives, a sandbox instance's creation and a guest sync arrive there
/// too, so this keeps the drain and the scheduler from filling it, not every
/// caller. It is also vol-mgr's push concurrency per kind, so a VM the drain
/// has paused uploads rather than waiting behind the node's backlog.
pub(super) const DEFAULT_DRAIN_NODE_WINDOW: usize = 8;

/// The largest window [`DRAIN_NODE_WINDOW_ENV`] may set: with the idle
/// auto-suspend scheduler's 8, 12 drain suspends make the lane's default 20
/// arrivals, the most those two callers take without refusing one of their
/// own suspends, whatever else shares the lane.
pub(super) const MAX_DRAIN_NODE_WINDOW: usize = 12;

pub(super) const DRAIN_NODE_WINDOW_ENV: &str = "SPACE_DRAIN_NODE_WINDOW";

/// [`DEFAULT_DRAIN_NODE_WINDOW`], or the positive number
/// [`DRAIN_NODE_WINDOW_ENV`] sets, at most [`MAX_DRAIN_NODE_WINDOW`].
pub(in crate::service) fn drain_node_window_from_env() -> usize {
    drain_node_window(std::env::var(DRAIN_NODE_WINDOW_ENV).ok().as_deref())
}

/// The window `raw`, [`DRAIN_NODE_WINDOW_ENV`]'s value, sets: a number
/// above [`MAX_DRAIN_NODE_WINDOW`] is lowered to it.
pub(super) fn drain_node_window(raw: Option<&str>) -> usize {
    let Some(raw) = raw else {
        return DEFAULT_DRAIN_NODE_WINDOW;
    };
    match raw
        .trim()
        .parse::<usize>()
        .ok()
        .filter(|window| *window > 0)
    {
        Some(window) if window > MAX_DRAIN_NODE_WINDOW => {
            tracing::warn!(
                var = DRAIN_NODE_WINDOW_ENV,
                raw = %raw,
                max = MAX_DRAIN_NODE_WINDOW,
                "drain window above the most the drain and the idle auto-suspend can have on \
                 node-manager's Filesystem lane; using the largest window"
            );
            MAX_DRAIN_NODE_WINDOW
        }
        Some(window) => window,
        None => {
            tracing::warn!(
                var = DRAIN_NODE_WINDOW_ENV,
                raw = %raw,
                default = DEFAULT_DRAIN_NODE_WINDOW,
                "not a positive drain window; using the default"
            );
            DEFAULT_DRAIN_NODE_WINDOW
        }
    }
}

/// Each row's status as the drain last saw it, and since when.
type SeenLifecycles = BTreeMap<String, (SandboxStatus, tokio::time::Instant)>;

/// The drain's work in flight in this process, by sandbox, and what it
/// remembers between calls.
pub(in crate::service) struct DrainActions {
    /// How many suspends and disk publications may be in flight at once.
    pub(super) window: usize,
    running: Mutex<BTreeMap<String, DrainAction>>,
    /// The failures counted under the epoch they were counted under; a call
    /// under another epoch starts the count again.
    failures: Mutex<(Option<u64>, DrainFailures)>,
    /// Each row's lifecycle as the drain last saw it, and since when, under
    /// the epoch it was seen under; a call under another epoch starts the
    /// clock again.
    seen: Mutex<(Option<u64>, SeenLifecycles)>,
    /// Since when each row has waited for cache disk for its suspend, under
    /// the epoch it waited under.
    disk_waits: Mutex<(Option<u64>, BTreeMap<String, tokio::time::Instant>)>,
}

impl Default for DrainActions {
    fn default() -> Self {
        Self::with_window(DEFAULT_DRAIN_NODE_WINDOW)
    }
}

impl DrainActions {
    pub(in crate::service) fn with_window(window: usize) -> Self {
        Self {
            window: window.max(1),
            running: Mutex::default(),
            failures: Mutex::default(),
            seen: Mutex::default(),
            disk_waits: Mutex::default(),
        }
    }

    pub(super) fn running(&self) -> BTreeMap<String, DrainAction> {
        lock(&self.running).clone()
    }

    /// Mark `action` running for `sandbox_id`; false when something already
    /// is.
    pub(super) fn start(&self, sandbox_id: &str, action: DrainAction) -> bool {
        let mut running = lock(&self.running);
        if running.contains_key(sandbox_id) {
            return false;
        }
        running.insert(sandbox_id.to_owned(), action);
        true
    }

    /// End `sandbox_id`'s running action, started under `epoch`, counting
    /// `failure` if it failed and the counts are still that epoch's.
    pub(super) fn finish(&self, epoch: u64, sandbox_id: &str, failure: Option<String>) {
        let action = lock(&self.running).remove(sandbox_id);
        if let (Some(reason), Some(action)) = (failure, action) {
            let mut failures = lock(&self.failures);
            if failures.0 == Some(epoch) {
                let entry = failures
                    .1
                    .entry((sandbox_id.to_owned(), action))
                    .or_insert((0, String::new()));
                entry.0 = entry.0.saturating_add(1);
                entry.1 = reason;
            }
        }
    }

    /// The counts under `epoch`; another epoch's are forgotten first.
    pub(super) fn failures(&self, epoch: u64) -> DrainFailures {
        let mut failures = lock(&self.failures);
        if failures.0 != Some(epoch) {
            *failures = (Some(epoch), DrainFailures::new());
        }
        failures.1.clone()
    }

    /// Forget the failures of every sandbox the drain no longer works on.
    pub(super) fn keep_failures_of(&self, working: &BTreeSet<String>) {
        lock(&self.failures)
            .1
            .retain(|(sandbox_id, _), _| working.contains(sandbox_id));
    }

    /// Note each row's lifecycle at `now` under `epoch`, and answer the rows
    /// that have been in the same one for [`SETTLE_BOUND`] or longer under
    /// it.
    pub(super) fn unchanged_since_bound(
        &self,
        epoch: u64,
        rows: &[(String, SandboxStatus)],
        now: tokio::time::Instant,
    ) -> BTreeSet<String> {
        let mut guard = lock(&self.seen);
        if guard.0 != Some(epoch) {
            *guard = (Some(epoch), SeenLifecycles::new());
        }
        let seen = &mut guard.1;
        seen.retain(|sandbox_id, _| rows.iter().any(|(id, _)| id == sandbox_id));
        let mut unchanged = BTreeSet::new();
        for (sandbox_id, state) in rows {
            let entry = seen.entry(sandbox_id.clone()).or_insert((*state, now));
            if entry.0 != *state {
                *entry = (*state, now);
            }
            if now.saturating_duration_since(entry.1) >= SETTLE_BOUND {
                unchanged.insert(sandbox_id.clone());
            }
        }
        unchanged
    }

    /// The rows that have waited for cache disk for [`SETTLE_BOUND`] or
    /// longer under `epoch`.
    pub(super) fn disk_waits_past_bound(
        &self,
        epoch: u64,
        now: tokio::time::Instant,
    ) -> BTreeSet<String> {
        let guard = lock(&self.disk_waits);
        if guard.0 != Some(epoch) {
            return BTreeSet::new();
        }
        guard
            .1
            .iter()
            .filter(|(_, since)| now.saturating_duration_since(**since) >= SETTLE_BOUND)
            .map(|(sandbox_id, _)| sandbox_id.clone())
            .collect()
    }

    /// Note the rows waiting for cache disk at `now` under `epoch`: a row
    /// keeps the time it began waiting, and one no longer waiting is
    /// forgotten.
    pub(super) fn note_disk_waits(
        &self,
        epoch: u64,
        waiting: &[String],
        now: tokio::time::Instant,
    ) {
        let mut guard = lock(&self.disk_waits);
        if guard.0 != Some(epoch) {
            *guard = (Some(epoch), BTreeMap::new());
        }
        guard.1.retain(|sandbox_id, _| waiting.contains(sandbox_id));
        for sandbox_id in waiting {
            guard.1.entry(sandbox_id.clone()).or_insert(now);
        }
    }

    /// Wait, at most ten seconds, until no drain action is running.
    #[cfg(test)]
    pub(super) async fn wait_idle(&self) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !self.running().is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("drain actions finish within ten seconds");
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    // Every critical section leaves its map whole before it can panic.
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl SandboxManagerService {
    /// Start what the plan asks for that isn't already running.
    pub(super) fn start_actions(&self, epoch: u64, plan: &mut DrainPlan) {
        for action in std::mem::take(&mut plan.actions) {
            let (sandbox_id, kind) = action.target();
            let sandbox_id = sandbox_id.to_owned();
            if !self.drain_actions.start(&sandbox_id, kind) {
                continue;
            }
            let service = self.clone();
            let task_sandbox_id = sandbox_id.clone();
            // A state writer, so a restart handoff waits for it rather than
            // letting the next process start the same work beside it.
            let spawned = self.spawn_detached_state_writer("NodeDrain", &sandbox_id, async move {
                let result = service.run_drain_action(epoch, action).await;
                if let Err(status) = &result {
                    tracing::warn!(
                        sandbox_id = %task_sandbox_id,
                        action = kind.as_str(),
                        error = %status,
                        "node drain action failed; the next drain call plans it again"
                    );
                }
                service.drain_actions.finish(
                    epoch,
                    &task_sandbox_id,
                    result.err().map(|status| status.message().to_owned()),
                );
            });
            if spawned.is_err() {
                // Draining for a restart: the next process's call starts it.
                self.drain_actions.finish(epoch, &sandbox_id, None);
            }
        }
    }

    /// Run one action. A suspend or a publication runs only while the node
    /// is still sealed under `epoch`: once a cancel or a later drain has
    /// taken over, a later call plans again. An upload runs either way: a
    /// cancel hands its sandbox back once its snapshot is durable.
    pub(super) async fn run_drain_action(
        &self,
        epoch: u64,
        action: PlannedAction,
    ) -> Result<(), Status> {
        if !matches!(action, PlannedAction::Upload { .. })
            && self.activation_seal.status().sealed_epoch != Some(epoch)
        {
            return Ok(());
        }
        match action {
            PlannedAction::Suspend { sandbox_id } => self.drain_suspend(epoch, &sandbox_id).await,
            PlannedAction::PublishDisk { sandbox_id } => {
                self.publish_current_instance(sandbox_id, crate::proto::ArtifactKind::Disk)
                    .await
            }
            // The backup adopts the unpinned suspend snapshot as the row's
            // full artifact and enqueues its upload.
            PlannedAction::PublishSnapshot { sandbox_id } => {
                self.publish_current_instance(sandbox_id, crate::proto::ArtifactKind::Full)
                    .await
            }
            PlannedAction::Upload {
                sandbox_id,
                artifact_id,
                kind,
            } => {
                // The backup worker retries an upload itself; one it gave up on
                // counts as one failure of the drain's, and the enqueue below
                // starts it again.
                let gave_up = self.failed_upload(&artifact_id)?;
                self.enqueue_artifact_backup_authorized(EnqueueArtifactBackupRequest {
                    sandbox_id,
                    expected_artifact_kind: crate::proto::ArtifactKind::from(kind) as i32,
                    source: Some(
                        crate::proto::enqueue_artifact_backup_request::Source::LocalArtifactId(
                            artifact_id,
                        ),
                    ),
                })
                .await?;
                match gave_up {
                    Some(error) => Err(Status::unavailable(format!(
                        "the last upload failed: {error}"
                    ))),
                    None => Ok(()),
                }
            }
        }
    }

    /// `EnqueueArtifactBackup` of `sandbox_id`'s current instance as a
    /// `kind` artifact, which publishes it first when the row pins none.
    async fn publish_current_instance(
        &self,
        sandbox_id: String,
        kind: crate::proto::ArtifactKind,
    ) -> Result<(), Status> {
        self.enqueue_artifact_backup_authorized(EnqueueArtifactBackupRequest {
            sandbox_id,
            expected_artifact_kind: kind as i32,
            source: Some(
                crate::proto::enqueue_artifact_backup_request::Source::CurrentInstance(
                    crate::proto::CurrentInstanceSource::default(),
                ),
            ),
        })
        .await
        .map(|_| ())
    }

    /// Whether `artifact_id`'s upload job is still queued or running. A job
    /// that can't be read counts as none, so the upload is enqueued again.
    pub(super) fn upload_job_active(&self, artifact_id: &str) -> bool {
        let Ok(artifact) = artifact_id.parse::<pplx_space_common_core::types::LocalArtifactId>()
        else {
            return false;
        };
        self.snapshot_upload_worker
            .find_job_by_dedup_key(
                crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
                &crate::snapshot_upload_jobs::artifact_backup_dedup_key(artifact),
            )
            .ok()
            .flatten()
            .and_then(|job| job.status.parse::<pplx_space_common_jobs::JobStatus>().ok())
            .is_some_and(|status| !status.is_terminal())
    }

    /// Why the latest upload of `artifact_id` failed, if the backup worker
    /// gave up on it.
    fn failed_upload(&self, artifact_id: &str) -> Result<Option<String>, Status> {
        let artifact = artifact_id
            .parse::<pplx_space_common_core::types::LocalArtifactId>()
            .map_err(|error| {
                Status::internal(format!("invalid local artifact id {artifact_id}: {error}"))
            })?;
        let job = self.snapshot_upload_worker.find_job_by_dedup_key(
            crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
            &crate::snapshot_upload_jobs::artifact_backup_dedup_key(artifact),
        )?;
        Ok(job
            .filter(|job| job.status == pplx_space_common_jobs::JobStatus::Failed.as_str())
            .map(|job| {
                job.error
                    .unwrap_or_else(|| "the backup worker gave up on it".to_owned())
            }))
    }

    /// The idle auto-suspend's suspend of a started or paused VM, for the
    /// drain under `epoch`: under the wake and artifact locks, with a full
    /// snapshot published and its upload enqueued before the runtime is torn
    /// down. The lifecycle it suspends from is recorded under the wake lock
    /// first, and only while the node is still sealed under `epoch`; a row
    /// something else moved on first keeps its own lifecycle, which the next
    /// call plans from.
    pub(super) async fn drain_suspend(&self, epoch: u64, sandbox_id: &str) -> Result<(), Status> {
        let wake_lock = self.auto_lifecycle.wake_lock(sandbox_id);
        let wake_guard = wake_lock.lock_owned().await;
        let sandbox = self.lookup_sandbox(sandbox_id)?;
        let prior = match sandbox.state.unowned() {
            Some(SandboxState::Started) => DrainPrior::Started,
            Some(SandboxState::Paused) => DrainPrior::Paused,
            _ => return Ok(()),
        };
        let Some(recorded) =
            self.activation_seal
                .record_drain_suspend(&self.db, epoch, &sandbox.id, prior)?
        else {
            return Ok(());
        };
        let artifact_lock = self.auto_lifecycle.artifact_lock(&sandbox.id);
        let artifact_guard = artifact_lock.lock_for_capture().await;
        let context = SuspendRequestContext::new(
            sandbox.id.clone(),
            true,
            0,
            true,
            Some(SandboxEventTrigger::System),
        );
        let suspended = async {
            let (_, publication) = self
                .prepare_suspend_snapshot_publication_after_locks(
                    &context,
                    &wake_guard,
                    &artifact_guard,
                )
                .await?;
            self.complete_suspend_snapshot_publication(publication)
                .await
        }
        .await;
        let recorded_state = match recorded {
            DrainPrior::Started => SandboxState::Started,
            DrainPrior::Paused => SandboxState::Paused,
            DrainPrior::Stopped => SandboxState::Stopped,
        };
        if suspended.is_err()
            && self
                .lookup_sandbox(sandbox_id)
                .is_ok_and(|row| row.state.is_unowned(recorded_state))
        {
            // The suspend didn't land and the row is back in the lifecycle it
            // recorded, so the record would name the lifecycle of a suspend
            // that never happened if a user's own suspend came next. The next
            // call records it again. A row it left elsewhere, `paused` after
            // its pause, keeps the record: that is the lifecycle to restore.
            let forgotten = self.db_conn().ok().is_some_and(|conn| {
                crate::db::activation_seal::forget_drain_suspend(&conn, sandbox_id).is_ok()
            });
            if !forgotten {
                tracing::warn!(
                    sandbox_id,
                    "can't forget the record of a drain suspend that failed"
                );
            }
        }
        drop(artifact_guard);
        drop(wake_guard);
        suspended
    }
}
