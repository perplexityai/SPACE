//! The drain's work: the suspends, snapshot publications and uploads it starts
//! as detached state writers, and what it remembers of them between passes.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};

use super::planner::*;
use crate::proto::EnqueueArtifactBackupRequest;
use crate::service::grpc_lifecycle::{BackupCaller, SuspendRequestContext};
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

/// How long one `DrainNode` call may run, its scans included, before it
/// answers `DeadlineExceeded` and frees the node for the next call: under
/// the control plane's own timeout for the call, so the node answers first.
pub(super) const DRAIN_CALL_TIMEOUT: Duration = Duration::from_secs(45);

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

/// The drain's work in flight in this process, by sandbox, and what it
/// remembers between calls.
pub(in crate::service) struct DrainActions {
    /// How many suspends and snapshot publications may be in flight at once.
    pub(super) window: usize,
    /// Taken for the whole of one `DrainNode` call, drain or cancel, so one
    /// runs at a time and each plans with the work earlier ones started,
    /// from the seal as it stands.
    call: CallSlot,
    /// Taken for the whole of one `PreviewDrainNode` call, so one preview
    /// runs at a time.
    preview: CallSlot,
    /// How long one call may run ([`DRAIN_CALL_TIMEOUT`]).
    pub(super) call_timeout: Duration,
    /// The generation a call has said to evacuate under: the drain suspends
    /// started sandboxes under it from then on, until a cancel or another
    /// generation, whatever a later call under it says. In memory only, so
    /// after a restart the drain holds again until a call says to evacuate.
    evacuating: Mutex<Option<u64>>,
    running: Mutex<BTreeMap<String, DrainAction>>,
    /// The failures under the generation they happened under; a pass under
    /// another generation starts from none.
    failures: Mutex<(Option<u64>, DrainFailures)>,
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
            call: CallSlot::default(),
            preview: CallSlot::default(),
            call_timeout: DRAIN_CALL_TIMEOUT,
            evacuating: Mutex::default(),
            running: Mutex::default(),
            failures: Mutex::default(),
        }
    }

    /// Take the call slot for one `DrainNode` call until the guard drops,
    /// or refuse the call with `Aborted` and
    /// [`DRAIN_CALL_IN_PROGRESS_MARKER`] while another holds it.
    pub(super) fn enter_call(&self) -> Result<CallGuard<'_>, Status> {
        self.call.enter().ok_or_else(|| {
            Status::aborted(format!(
                "{DRAIN_CALL_IN_PROGRESS_MARKER}; this call changed nothing, retry it"
            ))
        })
    }

    /// Take the preview slot for one `PreviewDrainNode` call until the guard
    /// drops, or refuse the preview with `Aborted` and
    /// [`DRAIN_CALL_IN_PROGRESS_MARKER`] while another holds it.
    pub(super) fn enter_preview(&self) -> Result<CallGuard<'_>, Status> {
        self.preview.enter().ok_or_else(|| {
            Status::aborted(format!(
                "{DRAIN_CALL_IN_PROGRESS_MARKER}; this preview changed nothing, retry it"
            ))
        })
    }

    /// Evacuate under `generation` from now on, the node sealed under it.
    pub(super) fn evacuate(&self, generation: u64) {
        *lock(&self.evacuating) = Some(generation);
    }

    /// Whether a call under `generation` has said to evacuate.
    pub(super) fn evacuating(&self, generation: u64) -> bool {
        *lock(&self.evacuating) == Some(generation)
    }

    /// Forget the evacuating, at a cancel.
    pub(super) fn stop_evacuating(&self) {
        *lock(&self.evacuating) = None;
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

    /// End `sandbox_id`'s running action, started under `generation`, keeping
    /// `failure` as its last one if it failed and the failures are still that
    /// generation's.
    pub(super) fn finish(&self, generation: u64, sandbox_id: &str, failure: Option<String>) {
        let action = lock(&self.running).remove(sandbox_id);
        if let (Some(reason), Some(action)) = (failure, action) {
            let mut failures = lock(&self.failures);
            if failures.0 == Some(generation) {
                failures.1.insert((sandbox_id.to_owned(), action), reason);
            }
        }
    }

    /// The failures under `generation`; another generation's are forgotten first.
    pub(super) fn failures(&self, generation: u64) -> DrainFailures {
        let mut failures = lock(&self.failures);
        if failures.0 != Some(generation) {
            *failures = (Some(generation), DrainFailures::new());
        }
        failures.1.clone()
    }

    /// Forget the failures of every sandbox the drain no longer works on.
    pub(super) fn keep_failures_of(&self, working: &BTreeSet<String>) {
        lock(&self.failures)
            .1
            .retain(|(sandbox_id, _), _| working.contains(sandbox_id));
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

pub(super) use pplx_space_rpc_contracts::drain_node_signals::DRAIN_CALL_IN_PROGRESS_MARKER;

/// The failure of an upload the backup worker gave up on with `error`: the
/// error alone, which the planner names the last upload's failure.
pub(super) fn upload_gave_up(error: String) -> Status {
    Status::unavailable(error)
}

/// A slot one call holds at a time ([`CallGuard`]).
#[derive(Debug, Default)]
pub(super) struct CallSlot(AtomicBool);

impl CallSlot {
    /// The slot until the guard drops, or none while another call holds it.
    pub(super) fn enter(&self) -> Option<CallGuard<'_>> {
        self.0
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| CallGuard(&self.0))
    }
}

/// Holds a [`CallSlot`] until dropped, however the call ends: answered,
/// failed, timed out, or dropped by a caller that went away.
#[must_use = "dropping the guard frees the slot"]
#[derive(Debug)]
pub(super) struct CallGuard<'slot>(&'slot AtomicBool);

impl Drop for CallGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Every critical section leaves its map whole before it can panic.
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl SandboxManagerService {
    /// Start what the plan asks for that isn't already running. Each action
    /// is a detached state writer: once a restart handoff closes admission,
    /// nothing starts, and a call to the next process starts it.
    pub(super) fn start_actions(&self, generation: u64, plan: &mut DrainPlan) {
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
                let result = service.run_drain_action(generation, action).await;
                if let Err(status) = &result {
                    tracing::warn!(
                        sandbox_id = %task_sandbox_id,
                        action = kind.as_str(),
                        error = %status,
                        "node drain action failed; the next drain call plans it again"
                    );
                }
                service.drain_actions.finish(
                    generation,
                    &task_sandbox_id,
                    result.err().map(|status| status.message().to_owned()),
                );
            });
            if spawned.is_err() {
                // Draining for a restart: a call to the next process starts it.
                self.drain_actions.finish(generation, &sandbox_id, None);
            }
        }
    }

    /// Run one action, only while the node is still sealed under `generation`:
    /// once a cancel or a later drain has taken over, a later call plans
    /// again.
    pub(super) async fn run_drain_action(
        &self,
        generation: u64,
        action: PlannedAction,
    ) -> Result<(), Status> {
        if self.activation_seal.status().sealed_generation != Some(generation) {
            return Ok(());
        }
        match action {
            PlannedAction::Suspend { sandbox_id, from } => {
                self.drain_suspend(generation, &sandbox_id, from).await
            }
            PlannedAction::PublishSnapshot { sandbox_id } => {
                self.publish_unpinned_snapshot(sandbox_id).await
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
                self.enqueue_artifact_backup_authorized(
                    EnqueueArtifactBackupRequest {
                        sandbox_id,
                        expected_artifact_kind: crate::proto::ArtifactKind::from(kind) as i32,
                        source: Some(
                            crate::proto::enqueue_artifact_backup_request::Source::LocalArtifactId(
                                artifact_id,
                            ),
                        ),
                    },
                    BackupCaller::NodeDrain,
                )
                .await?;
                gave_up.map_or(Ok(()), |error| Err(upload_gave_up(error)))
            }
        }
    }

    /// `EnqueueArtifactBackup` of suspended `sandbox_id`'s current instance
    /// as a full artifact: the backup adopts the unpinned suspend snapshot as
    /// the row's full artifact and enqueues its upload.
    async fn publish_unpinned_snapshot(&self, sandbox_id: String) -> Result<(), Status> {
        self.enqueue_artifact_backup_authorized(
            EnqueueArtifactBackupRequest {
                sandbox_id,
                expected_artifact_kind: crate::proto::ArtifactKind::Full as i32,
                source: Some(
                    crate::proto::enqueue_artifact_backup_request::Source::CurrentInstance(
                        crate::proto::CurrentInstanceSource::default(),
                    ),
                ),
            },
            BackupCaller::NodeDrain,
        )
        .await
        .map(|_| ())
    }

    /// Whether `artifact_id`'s upload job is still queued or running. A job
    /// that can't be read counts as none, so the upload is enqueued again.
    pub(super) fn upload_job_active(&self, artifact_id: &str) -> bool {
        let Ok(artifact) = artifact_id.parse::<pplx_space_snapshot_model::LocalArtifactId>() else {
            return false;
        };
        self.snapshot_upload_worker
            .find_job_by_dedup_key(
                crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
                &crate::snapshot_upload_jobs::artifact_backup_dedup_key(artifact),
            )
            .ok()
            .flatten()
            .and_then(|job| job.status.parse::<crate::db::jobs::JobStatus>().ok())
            .is_some_and(|status| !status.is_terminal())
    }

    /// Why the latest upload of `artifact_id` failed, if the backup worker
    /// gave up on it.
    fn failed_upload(&self, artifact_id: &str) -> Result<Option<String>, Status> {
        let artifact = artifact_id
            .parse::<pplx_space_snapshot_model::LocalArtifactId>()
            .map_err(|error| {
                Status::internal(format!("invalid local artifact id {artifact_id}: {error}"))
            })?;
        let job = self.snapshot_upload_worker.find_job_by_dedup_key(
            crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
            &crate::snapshot_upload_jobs::artifact_backup_dedup_key(artifact),
        )?;
        Ok(job
            .filter(|job| job.status == crate::db::jobs::JobStatus::Failed.as_str())
            .map(|job| {
                job.error
                    .unwrap_or_else(|| "the backup worker gave up on it".to_owned())
            }))
    }

    /// The idle auto-suspend's suspend of a VM in a lifecycle `from` allows,
    /// for the drain under `generation`: under the wake and artifact locks, with a
    /// full snapshot published and its upload enqueued before the runtime is
    /// torn down. A row something else moved on first, a paused row resumed
    /// meanwhile among them, keeps its own lifecycle, which the next call
    /// plans from.
    ///
    /// The waits for the two locks can outlast the seal the suspend was
    /// planned under: once both are held, it suspends nothing unless the node
    /// is still sealed under `generation` and, for a started VM, the drain
    /// evacuates under `generation`. A suspend already past that check finishes,
    /// even if a cancel lands meanwhile.
    pub(super) async fn drain_suspend(
        &self,
        generation: u64,
        sandbox_id: &str,
        from: SuspendFrom,
    ) -> Result<(), Status> {
        let wake_lock = self.auto_lifecycle.wake_lock(sandbox_id);
        let wake_guard = wake_lock.lock_owned().await;
        let sandbox = self.lookup_sandbox(sandbox_id)?;
        if !from.allows(sandbox.state.unowned()) {
            return Ok(());
        }
        let artifact_lock = self.auto_lifecycle.artifact_lock(&sandbox.id);
        let artifact_guard = artifact_lock.lock_for_capture().await;
        let started = sandbox.state.is_unowned(SandboxState::Started);
        if self.activation_seal.status().sealed_generation != Some(generation)
            || (started && !self.drain_actions.evacuating(generation))
        {
            return Ok(());
        }
        let context = SuspendRequestContext::new(
            sandbox.id.clone(),
            true,
            0,
            true,
            Some(SandboxEventTrigger::System),
        );
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
}
