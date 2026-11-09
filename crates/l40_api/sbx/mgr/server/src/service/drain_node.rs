//! `DrainNode`: the node's own half of a forced drain.
//!
//! The control plane names an epoch; the node seals itself under it (see
//! [`crate::activation_seal`]) and, on every call, makes each of its
//! residents durable elsewhere and reports what it has done. It acts only
//! through the paths the node already owns: the suspend of a started or paused
//! VM is the idle auto-suspend's (a full snapshot published and uploaded under
//! node-local durability), a stopped sandbox's disk is published through
//! `EnqueueArtifactBackup`, and the upload of every artifact without a
//! durable copy, named snapshots included, is finished the same way. Each
//! runs as a detached state writer and checks the seal again first. The control plane learns of each artifact as it always does,
//! through the backup it registers; the drain only reports the artifact, the
//! row's durable pin, once it is durable, with the lifecycle the sandbox had.
//! A drain suspend records that lifecycle (`drain_evacuations`) under the
//! sandbox's wake lock, while the node is still sealed under the drain's
//! epoch, so it survives a restart; a stopped sandbox's row keeps its own.
//!
//! A call never waits for that work. It plans from the rows as they are,
//! starts what is not already running, and answers. It paces what it starts:
//! at most a window of suspends and disk publications is in flight at once,
//! uploads they leave pending included, and a suspend starts only while the
//! cache disk covers its snapshot; the rest wait, `pending`, for a later
//! call. An upload whose job is still queued or running is left to it. [`DrainPlan`] is that
//! decision, a pure function of what the call read, and `done` is true only
//! when nothing is left: no VM, nothing pending or blocked, every scan that
//! can reveal a resident or an artifact succeeded, and the seal held, under
//! the call's epoch, from before the walk to its end, with no activation in
//! flight at its first read. Every row that keeps `done` false says why: work
//! that keeps failing, and a row stuck in a transition the drain doesn't
//! drive, turn from `pending` into `blocked` after a bound.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use super::grpc_lifecycle::SuspendRequestContext;
use super::*;
use crate::activation_seal::ActivationSealStatus;
use crate::db::activation_seal::{DrainEvacuation, DrainPrior};
use crate::proto::{
    DrainBlocked, DrainEvacuated, DrainNodeRequest, DrainNodeResponse, DrainPending,
    EnqueueArtifactBackupRequest,
};
use pplx_space_events::SandboxEventTrigger;

/// How long one call may spend reading node-manager's whole orphan
/// inventory: well under the control plane's own timeout for the call, so
/// a slow node-manager answers `blocked` rather than an error. The drain's
/// work goes on between calls.
pub(super) const ORPHAN_INVENTORY_DEADLINE: Duration = Duration::from_secs(20);
/// How long a call waits before asking a busy orphan inventory again.
const ORPHAN_INVENTORY_BUSY_RETRY: Duration = Duration::from_millis(200);

/// How many times one kind of the drain's work on a sandbox may fail, under
/// one epoch, before the drain reports it `blocked` with the last failure.
/// A success doesn't reset the count: an upload's cycle alternates a failed
/// re-enqueue with a join of the pending job, which would never reach the
/// bound. It still retries on every call.
pub(super) const MAX_ACTION_FAILURES: u32 = 3;
/// How long a row may sit in a transition the drain doesn't drive before the
/// drain reports it `blocked`: a start the seal refused leaves a row
/// `created`, and a failed restore's row stays `restoring`, with nothing
/// moving them on.
pub(super) const SETTLE_BOUND: Duration = Duration::from_secs(15 * 60);

/// How many of the drain's suspends and disk publications may be in flight
/// on the node at once, the uploads they leave pending included, unless
/// [`DRAIN_NODE_WINDOW_ENV`] sets another positive number. Beside the idle
/// auto-suspend scheduler's 8, at most 16 suspends reach node-manager's
/// Filesystem lane at once, below the 20 arrivals (4 permits and 16 waiting)
/// past which it refuses one, which would roll that suspend back. It is also
/// vol-mgr's push concurrency per kind, so a VM the drain has paused uploads
/// rather than waiting behind the node's backlog.
pub(super) const DEFAULT_DRAIN_NODE_WINDOW: usize = 8;
pub(super) const DRAIN_NODE_WINDOW_ENV: &str = "SPACE_DRAIN_NODE_WINDOW";

/// [`DEFAULT_DRAIN_NODE_WINDOW`], or the positive number
/// [`DRAIN_NODE_WINDOW_ENV`] sets.
pub(super) fn drain_node_window_from_env() -> usize {
    drain_node_window(std::env::var(DRAIN_NODE_WINDOW_ENV).ok().as_deref())
}

/// The window `raw`, [`DRAIN_NODE_WINDOW_ENV`]'s value, sets.
pub(super) fn drain_node_window(raw: Option<&str>) -> usize {
    match raw {
        Some(raw) => raw
            .trim()
            .parse::<usize>()
            .ok()
            .filter(|window| *window > 0)
            .unwrap_or_else(|| {
                tracing::warn!(
                    var = DRAIN_NODE_WINDOW_ENV,
                    raw = %raw,
                    default = DEFAULT_DRAIN_NODE_WINDOW,
                    "not a positive drain window; using the default"
                );
                DEFAULT_DRAIN_NODE_WINDOW
            }),
        None => DEFAULT_DRAIN_NODE_WINDOW,
    }
}

/// The bytes of cache disk the drain may still commit to snapshots before
/// the cache reaches the floor its disk admission keeps free, the margin
/// every new allocation leaves; none while admission refuses allocations.
pub(super) fn drain_disk_headroom(
    config: pplx_space_common_core::host::disk_admission::CacheDiskAdmissionConfig,
    measurement: pplx_space_common_core::host::disk_admission::CacheDiskMeasurement,
) -> u64 {
    let decision = pplx_space_common_core::host::disk_admission::evaluate_cache_disk_measurement(
        config,
        measurement,
        0,
    );
    if decision.allows_new_allocation() {
        decision
            .effective_available_bytes
            .saturating_sub(decision.hard_floor_bytes)
    } else {
        0
    }
}

/// What the drain is doing for a sandbox.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) enum DrainAction {
    /// The node-originated suspend of a started or paused VM.
    Suspend,
    /// The publication of a stopped sandbox's disk.
    PublishDisk,
    /// Finishing a local-only artifact's upload.
    Upload,
}

impl DrainAction {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Suspend => "suspend",
            Self::PublishDisk => "publish_disk",
            Self::Upload => "upload",
        }
    }
}

/// The failures of each kind of the drain's work on each sandbox, with the
/// last one's reason.
pub(super) type DrainFailures = BTreeMap<(String, DrainAction), (u32, String)>;

/// Each row's lifecycle as the drain last saw it, and since when.
type SeenLifecycles = BTreeMap<String, (SandboxState, tokio::time::Instant)>;

/// The drain's work in flight in this process, by sandbox, and what it
/// remembers between calls.
pub(super) struct DrainActions {
    /// How many suspends and disk publications may be in flight at once.
    window: usize,
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
    pub(super) fn with_window(window: usize) -> Self {
        Self {
            window: window.max(1),
            running: Mutex::default(),
            failures: Mutex::default(),
            seen: Mutex::default(),
            disk_waits: Mutex::default(),
        }
    }

    fn running(&self) -> BTreeMap<String, DrainAction> {
        lock(&self.running).clone()
    }

    /// Mark `action` running for `sandbox_id`; false when something already
    /// is.
    fn start(&self, sandbox_id: &str, action: DrainAction) -> bool {
        let mut running = lock(&self.running);
        if running.contains_key(sandbox_id) {
            return false;
        }
        running.insert(sandbox_id.to_owned(), action);
        true
    }

    /// End `sandbox_id`'s running action, started under `epoch`, counting
    /// `failure` if it failed and the counts are still that epoch's.
    fn finish(&self, epoch: u64, sandbox_id: &str, failure: Option<String>) {
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
    fn failures(&self, epoch: u64) -> DrainFailures {
        let mut failures = lock(&self.failures);
        if failures.0 != Some(epoch) {
            *failures = (Some(epoch), DrainFailures::new());
        }
        failures.1.clone()
    }

    /// Forget the failures of every sandbox the drain no longer works on.
    fn keep_failures_of(&self, working: &BTreeSet<String>) {
        lock(&self.failures)
            .1
            .retain(|(sandbox_id, _), _| working.contains(sandbox_id));
    }

    /// Note each row's lifecycle at `now` under `epoch`, and answer the rows
    /// that have been in the same one for [`SETTLE_BOUND`] or longer under
    /// it.
    fn unchanged_since_bound(
        &self,
        epoch: u64,
        rows: &[(String, SandboxState)],
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
    fn disk_waits_past_bound(&self, epoch: u64, now: tokio::time::Instant) -> BTreeSet<String> {
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
    fn note_disk_waits(&self, epoch: u64, waiting: &[String], now: tokio::time::Instant) {
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

/// One row as the planner reads it.
#[derive(Clone, Debug)]
pub(super) struct DrainRow {
    pub(super) sandbox_id: String,
    /// The combined lifecycle, cleanup included.
    pub(super) lifecycle: SandboxState,
    /// An unclaimed warm-pool member.
    pub(super) pool_member: bool,
    pub(super) volume_encryption: bool,
    /// The pinned local artifact and its kind.
    pub(super) pin: Option<(String, pplx_space_common_core::types::ArtifactKind)>,
    /// Why a suspend would be refused, if it would.
    pub(super) suspend_refusal: Option<String>,
    /// What a suspend's snapshot would take of the cache disk: the guest's
    /// memory, for a started or paused row.
    pub(super) snapshot_bytes: u64,
}

/// Something the planner wants started.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum PlannedAction {
    Suspend {
        sandbox_id: String,
    },
    PublishDisk {
        sandbox_id: String,
    },
    Upload {
        sandbox_id: String,
        artifact_id: String,
        kind: pplx_space_common_core::types::ArtifactKind,
    },
}

/// What one call decided, from the rows it read.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct DrainPlan {
    pub(super) evacuated: Vec<(String, DrainPrior, String)>,
    pub(super) pending: Vec<(String, String)>,
    pub(super) blocked: Vec<(Option<String>, String)>,
    pub(super) actions: Vec<PlannedAction>,
    /// The rows whose suspend waits for cache disk, pending or blocked.
    pub(super) disk_waits: Vec<String>,
}

/// Whether a row in `state` may hold a VM.
pub(super) const fn may_hold_vm(state: SandboxState) -> bool {
    !matches!(
        state,
        SandboxState::Stopped
            | SandboxState::Suspended
            | SandboxState::Failed
            | SandboxState::CleanupFailed
    )
}

/// A sandbox's artifacts without a durable copy, by sandbox, from the
/// artifact registry: named snapshots and superseded lifecycle artifacts as
/// well as the current pin.
pub(super) type LocalOnlyArtifacts =
    BTreeMap<String, Vec<(String, pplx_space_common_core::types::ArtifactKind)>>;

/// Everything the planner reads besides the rows.
#[derive(Clone, Debug)]
pub(super) struct PlanInputs {
    /// The pinned artifacts still without a durable copy, by sandbox,
    /// including a pin that names no artifact row.
    pub(super) pending_backups: BTreeMap<String, BTreeSet<String>>,
    /// Every artifact without a durable copy.
    pub(super) local_only: LocalOnlyArtifacts,
    /// The recorded drain suspends, by sandbox.
    pub(super) evacuations: BTreeMap<String, DrainEvacuation>,
    /// The drain's work in flight in this process.
    pub(super) running: BTreeMap<String, DrainAction>,
    /// The failures of the drain's work under this call's epoch.
    pub(super) failures: DrainFailures,
    /// The rows in the same lifecycle for [`SETTLE_BOUND`] or longer.
    pub(super) unchanged: BTreeSet<String>,
    /// How many suspends and disk publications may be in flight at once.
    pub(super) window: usize,
    /// The bytes of cache disk the drain may commit to new snapshots; `None`
    /// when the node admits cache allocations without measuring.
    pub(super) disk_headroom: Option<u64>,
    /// The artifacts whose upload job is still queued or running.
    pub(super) active_uploads: BTreeSet<String>,
    /// The rows that have waited for cache disk for [`SETTLE_BOUND`] or
    /// longer.
    pub(super) disk_waits: BTreeSet<String>,
}

impl Default for PlanInputs {
    fn default() -> Self {
        Self {
            pending_backups: BTreeMap::new(),
            local_only: LocalOnlyArtifacts::new(),
            evacuations: BTreeMap::new(),
            running: BTreeMap::new(),
            failures: DrainFailures::new(),
            unchanged: BTreeSet::new(),
            window: DEFAULT_DRAIN_NODE_WINDOW,
            disk_headroom: None,
            active_uploads: BTreeSet::new(),
            disk_waits: BTreeSet::new(),
        }
    }
}

impl PlanInputs {
    /// Whether `sandbox_id`'s `action` has failed [`MAX_ACTION_FAILURES`]
    /// times under this epoch.
    fn failed_too_often(&self, sandbox_id: &str, action: DrainAction) -> bool {
        self.failures
            .get(&(sandbox_id.to_owned(), action))
            .is_some_and(|(failures, _)| *failures >= MAX_ACTION_FAILURES)
    }

    /// How much of the window is taken: the suspends and disk publications
    /// running, and the rows whose pinned artifact is still uploading, short
    /// of one blocked on its failures.
    fn window_in_flight(&self, rows: &[DrainRow]) -> usize {
        let running = self
            .running
            .values()
            .filter(|action| matches!(action, DrainAction::Suspend | DrainAction::PublishDisk))
            .count();
        let uploading = rows
            .iter()
            .filter(|row| {
                !self.running.contains_key(&row.sandbox_id)
                    && row.pin.as_ref().is_some_and(|(artifact, _)| {
                        self.pending_backups
                            .get(&row.sandbox_id)
                            .is_some_and(|pending| pending.contains(artifact))
                    })
                    && !self.failed_too_often(&row.sandbox_id, DrainAction::Upload)
            })
            .count();
        running + uploading
    }
}

impl DrainPlan {
    /// Wait for `action` on `sandbox_id`, or report it `blocked` once it has
    /// failed [`MAX_ACTION_FAILURES`] times under this epoch.
    fn wait_for(&mut self, inputs: &PlanInputs, sandbox_id: &str, action: DrainAction) {
        self.wait_as(inputs, sandbox_id, action, action.as_str());
    }

    /// [`Self::wait_for`], reporting a pending row with `reason`.
    fn wait_as(
        &mut self,
        inputs: &PlanInputs,
        sandbox_id: &str,
        action: DrainAction,
        reason: &str,
    ) {
        match inputs.failures.get(&(sandbox_id.to_owned(), action)) {
            Some((failures, last)) if *failures >= MAX_ACTION_FAILURES => {
                self.blocked.push((
                    Some(sandbox_id.to_owned()),
                    format!("{} failed {failures} times: {last}", action.as_str()),
                ));
            }
            _ => self
                .pending
                .push((sandbox_id.to_owned(), reason.to_owned())),
        }
    }

    /// Start `action` unless something is already running for its sandbox,
    /// and wait for whichever it is. An upload whose job is still queued or
    /// running is left to that job: enqueueing it again would only ask the
    /// control plane for its location once more.
    fn drive(&mut self, inputs: &PlanInputs, action: PlannedAction) {
        let (sandbox_id, kind) = action.target();
        let sandbox_id = sandbox_id.to_owned();
        match inputs.running.get(&sandbox_id) {
            Some(running) => self.wait_for(inputs, &sandbox_id, *running),
            None => {
                self.wait_for(inputs, &sandbox_id, kind);
                let uploading = matches!(
                    &action,
                    PlannedAction::Upload { artifact_id, .. }
                        if inputs.active_uploads.contains(artifact_id)
                );
                if !uploading {
                    self.actions.push(action);
                }
            }
        }
    }

    /// Start the suspends and disk publications `waiting` names while the
    /// window has room, paused VMs first, then started ones, then stopped
    /// disks, and a suspend only while the cache disk still covers its
    /// snapshot. The rest wait, `queued` for the window or `disk` for the
    /// disk; one that has waited for disk past [`SETTLE_BOUND`] is
    /// blocked.
    fn start_in_window(
        &mut self,
        rows: &[DrainRow],
        inputs: &PlanInputs,
        mut waiting: Vec<(WindowOrder, &DrainRow, PlannedAction)>,
    ) {
        waiting.sort_by(|(left, left_row, _), (right, right_row, _)| {
            (left, &left_row.sandbox_id).cmp(&(right, &right_row.sandbox_id))
        });
        let mut in_flight = inputs.window_in_flight(rows);
        let mut committed: u64 = rows
            .iter()
            .filter(|row| inputs.running.get(&row.sandbox_id) == Some(&DrainAction::Suspend))
            .map(|row| row.snapshot_bytes)
            .sum();
        for (_, row, action) in waiting {
            let id = &row.sandbox_id;
            let kind = action.target().1;
            if in_flight >= inputs.window {
                self.wait_as(inputs, id, kind, "queued");
                continue;
            }
            if kind == DrainAction::Suspend {
                let needed = committed.saturating_add(row.snapshot_bytes);
                if inputs
                    .disk_headroom
                    .is_some_and(|headroom| needed > headroom)
                {
                    self.disk_waits.push(id.clone());
                    if inputs.disk_waits.contains(id) && !inputs.failed_too_often(id, kind) {
                        self.blocked.push((
                            Some(id.clone()),
                            format!(
                                "the cache disk hasn't had room for a {} MiB snapshot for over \
                                 {} minutes",
                                row.snapshot_bytes / (1024 * 1024),
                                SETTLE_BOUND.as_secs() / 60
                            ),
                        ));
                    } else {
                        self.wait_as(inputs, id, kind, "disk");
                    }
                    continue;
                }
                committed = needed;
            }
            in_flight += 1;
            self.drive(inputs, action);
        }
    }
}

/// The order the window starts the drain's work in: a paused VM serves no
/// one, so it goes first; a stopped disk holds no VM, so it goes last.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) enum WindowOrder {
    PausedVm,
    StartedVm,
    StoppedDisk,
}

impl PlannedAction {
    fn target(&self) -> (&str, DrainAction) {
        match self {
            Self::Suspend { sandbox_id } => (sandbox_id, DrainAction::Suspend),
            Self::PublishDisk { sandbox_id } => (sandbox_id, DrainAction::PublishDisk),
            Self::Upload { sandbox_id, .. } => (sandbox_id, DrainAction::Upload),
        }
    }
}

/// Decide what the drain does for every row.
pub(super) fn plan(rows: &[DrainRow], inputs: &PlanInputs) -> DrainPlan {
    use pplx_space_common_core::types::ArtifactKind;
    let mut plan = DrainPlan::default();
    let mut windowed = Vec::new();
    for (sandbox_id, artifacts) in &inputs.local_only {
        if !rows.iter().any(|row| row.sandbox_id == *sandbox_id) {
            for (artifact, _) in artifacts {
                plan.blocked.push((
                    Some(sandbox_id.clone()),
                    format!("local-only artifact {artifact} has no sandbox row"),
                ));
            }
        }
    }
    for row in rows {
        let id = &row.sandbox_id;
        if row.pool_member {
            // Not a resident: the warm-pool reconciler evicts slots on a
            // cordoned node, and until it has, the VM keeps `done` false,
            // and pages once it has waited too long, as an undriven
            // transition does.
            if may_hold_vm(row.lifecycle) {
                if inputs.unchanged.contains(id) {
                    plan.blocked.push((
                        Some(id.clone()),
                        format!(
                            "a warm-pool VM not evicted for over {} minutes",
                            SETTLE_BOUND.as_secs() / 60
                        ),
                    ));
                } else {
                    plan.pending
                        .push((id.clone(), "warm_pool_eviction".to_owned()));
                }
            }
            continue;
        }
        let running = inputs.running.contains_key(id);
        let pending_pin = row.pin.clone().filter(|(artifact, _)| {
            inputs
                .pending_backups
                .get(id)
                .is_some_and(|pending| pending.contains(artifact))
        });
        let durable_pin = row.pin.clone().filter(|_| pending_pin.is_none());
        let upload = |(artifact_id, kind): (String, ArtifactKind)| PlannedAction::Upload {
            sandbox_id: id.clone(),
            artifact_id,
            kind,
        };
        match row.lifecycle {
            SandboxState::Quarantined => {
                plan.blocked.push((Some(id.clone()), "quarantined".into()));
            }
            SandboxState::CleanupFailed => {
                plan.blocked
                    .push((Some(id.clone()), "cleanup failed".into()));
            }
            SandboxState::Started | SandboxState::Paused => {
                let suspend = PlannedAction::Suspend {
                    sandbox_id: id.clone(),
                };
                match &row.suspend_refusal {
                    Some(refusal) if !running => {
                        plan.blocked.push((Some(id.clone()), refusal.clone()));
                    }
                    _ if running => plan.drive(inputs, suspend),
                    _ => windowed.push((
                        if row.lifecycle == SandboxState::Paused {
                            WindowOrder::PausedVm
                        } else {
                            WindowOrder::StartedVm
                        },
                        row,
                        suspend,
                    )),
                }
            }
            SandboxState::Stopped => match (&durable_pin, pending_pin) {
                _ if row.volume_encryption && !running => {
                    plan.blocked.push((
                        Some(id.clone()),
                        "an encrypted stopped sandbox's disk can't be published".into(),
                    ));
                }
                (Some((artifact, ArtifactKind::Disk)), _) if !running => {
                    plan.evacuated
                        .push((id.clone(), DrainPrior::Stopped, artifact.clone()));
                }
                (_, Some(pin @ (_, ArtifactKind::Disk))) => plan.drive(inputs, upload(pin)),
                _ if running => plan.drive(
                    inputs,
                    PlannedAction::PublishDisk {
                        sandbox_id: id.clone(),
                    },
                ),
                _ => windowed.push((
                    WindowOrder::StoppedDisk,
                    row,
                    PlannedAction::PublishDisk {
                        sandbox_id: id.clone(),
                    },
                )),
            },
            SandboxState::Suspended | SandboxState::Failed => {
                if let Some(pin) = pending_pin {
                    plan.drive(inputs, upload(pin));
                } else if let Some(evacuation) = inputs.evacuations.get(id) {
                    // The drain suspended it; its artifact is durable now.
                    match &durable_pin {
                        Some((artifact, ArtifactKind::Full))
                            if row.lifecycle == SandboxState::Suspended =>
                        {
                            plan.evacuated
                                .push((id.clone(), evacuation.prior, artifact.clone()));
                        }
                        _ => plan.blocked.push((
                            Some(id.clone()),
                            format!(
                                "the drain's suspend left the sandbox {} without a durable \
                                 snapshot",
                                row.lifecycle
                            ),
                        )),
                    }
                }
            }
            other => {
                // A transition the drain doesn't drive: wait for it to settle,
                // and page once it has sat there too long.
                if let Some(action) = inputs.running.get(id) {
                    plan.wait_for(inputs, id, *action);
                } else if inputs.unchanged.contains(id) {
                    plan.blocked.push((
                        Some(id.clone()),
                        format!(
                            "{other} for over {} minutes, a transition the drain doesn't drive",
                            SETTLE_BOUND.as_secs() / 60
                        ),
                    ));
                } else {
                    plan.pending.push((id.clone(), format!("settle:{other}")));
                }
            }
        }
        // Every other artifact the sandbox keeps without a durable copy, a
        // named snapshot or one its pin moved on from, is finished the same
        // way. The arm above owns the pin, and a row being deleted takes its
        // artifacts with it.
        if row.lifecycle == SandboxState::Deleting {
            continue;
        }
        let pin = row.pin.as_ref().map(|(artifact, _)| artifact);
        for artifact in inputs.local_only.get(id).into_iter().flatten() {
            if Some(&artifact.0) != pin {
                plan.drive(inputs, upload(artifact.clone()));
            }
        }
    }
    plan.start_in_window(rows, inputs, windowed);
    plan
}

/// What node-manager's orphan inventory found.
#[derive(Clone, Debug, Default)]
pub(super) struct DrainScans {
    pub(super) blocked: Vec<(Option<String>, String)>,
    /// node-manager's sandbox directory candidates, by the sandbox they're
    /// named for. node-manager knows only its slots: a directory whose
    /// sandbox has a row here is the row's, and the plan accounts for it.
    pub(super) orphan_dirs: Vec<(String, String)>,
    /// The bytes of cache disk the drain may commit to new snapshots
    /// ([`drain_disk_headroom`]); `None` when the node admits cache
    /// allocations without measuring.
    pub(super) disk_headroom: Option<u64>,
}

/// node-manager's orphan inventory, read as the drain needs it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct OrphanFindings {
    pub(super) blocked: Vec<String>,
    /// Cache and run directories, by the sandbox they're named for.
    pub(super) dirs: Vec<(String, String)>,
}

/// The sandbox a node-manager directory candidate is named for.
fn dir_sandbox(dir: &str) -> String {
    Path::new(dir).file_name().map_or_else(
        || dir.to_owned(),
        |name| name.to_string_lossy().into_owned(),
    )
}

/// The scanners of node-manager's orphan inventory, as its failure warnings
/// name them (`<scanner>: <error>`), whose findings can hold neither a
/// resident nor an artifact: network namespaces, cgroups, gateway run
/// directories and nft rules. A failure of any other scanner (run and cache
/// directories, processes, or one this build doesn't know) may have hidden
/// a resident.
const HARMLESS_ORPHAN_SCANNERS: [&str; 4] = ["Namespaces", "Cgroups", "GatewayRunDirs", "Nft"];

/// Fold one inventory page into `findings`.
pub(super) fn classify_orphan_page(
    page: &pplx_space_node_mgr_client::proto::ListOrphanCandidatesResponse,
    gateway_run_root: &str,
    findings: &mut OrphanFindings,
) {
    for process in &page.orphaned_processes {
        findings
            .blocked
            .push(format!("untracked runtime process {process}"));
    }
    for dir in &page.orphaned_cache_dirs {
        findings
            .dirs
            .push((dir_sandbox(dir), format!("orphan cache directory {dir}")));
    }
    for dir in &page.orphaned_run_dirs {
        if !Path::new(dir).starts_with(gateway_run_root) {
            findings
                .dirs
                .push((dir_sandbox(dir), format!("orphan run directory {dir}")));
        }
    }
    for warning in &page.warnings {
        // A failure of a scanner this build doesn't know, or one it can't
        // name, may have hidden a resident.
        let harmless = warning
            .split_once(':')
            .is_some_and(|(scanner, _)| HARMLESS_ORPHAN_SCANNERS.contains(&scanner));
        if !harmless {
            findings
                .blocked
                .push(format!("orphan inventory scan failed: {warning}"));
        }
    }
}

impl SandboxManagerService {
    /// Why the seal could not be read at startup; `run` refuses to serve
    /// then, so only a service built some other way can answer with it.
    pub fn activation_seal_load_error(&self) -> Option<String> {
        self.activation_seal.load_error()
    }

    pub(super) async fn drain_node(
        &self,
        request: Request<DrainNodeRequest>,
    ) -> Result<Response<DrainNodeResponse>, Status> {
        self.authorize_peer(&request, "DrainNode", ALLOW_NODE_DRAIN)?;
        let DrainNodeRequest { epoch, cancel } = request.into_inner();
        if self.activation_seal.load_error().is_some() {
            return Err(Status::internal("activation seal unreadable"));
        }
        if cancel {
            self.activation_seal.unseal(&self.db, epoch)?;
            let status = self.activation_seal.status();
            return Ok(Response::new(report(&status, DrainPlan::default(), false)));
        }
        self.activation_seal.seal(&self.db, epoch)?;
        let first = self.activation_seal.status();
        let orphans = self.read_orphan_inventory().await;
        let scans = DrainScans {
            blocked: orphans
                .blocked
                .into_iter()
                .map(|reason| (None, reason))
                .collect(),
            orphan_dirs: orphans.dirs,
            disk_headroom: read_disk_headroom().await,
        };
        Ok(Response::new(self.finish_drain(epoch, &first, scans)))
    }

    /// Plan from the rows, record and start what the plan asks for, and
    /// report. `first` is the seal as the call read it right after sealing,
    /// before its scans; `scans` are what the scans that ran outside the rows
    /// found.
    pub(super) fn finish_drain(
        &self,
        epoch: u64,
        first: &ActivationSealStatus,
        scans: DrainScans,
    ) -> DrainNodeResponse {
        let (mut plan, rows) = match self.read_drain_plan(epoch, scans.disk_headroom) {
            Ok(read) => read,
            Err(reason) => (
                DrainPlan {
                    blocked: vec![(None, reason)],
                    ..DrainPlan::default()
                },
                BTreeSet::new(),
            ),
        };
        self.start_actions(epoch, &mut plan);
        let orphan_dirs = scans
            .orphan_dirs
            .into_iter()
            .filter(|(sandbox_id, _)| !rows.contains(sandbox_id))
            .map(|(sandbox_id, reason)| (Some(sandbox_id), reason));
        plan.blocked
            .splice(0..0, scans.blocked.into_iter().chain(orphan_dirs));
        // An activation admitted before the seal can still launch or resume a
        // VM until it settles.
        plan.pending.splice(
            0..0,
            first
                .in_flight
                .iter()
                .map(|sandbox_id| (sandbox_id.clone(), "activation".to_owned())),
        );

        let last = self.activation_seal.status();
        // A cancelled epoch is never sealed again, so the same epoch at both
        // reads is one continuous seal.
        let continuous = first.sealed_epoch == Some(epoch) && last.sealed_epoch == Some(epoch);
        // Every row that may hold a VM is pending or blocked, so a node with
        // nothing pending or blocked holds none.
        let done = continuous && plan.pending.is_empty() && plan.blocked.is_empty();
        report(first, plan, done)
    }

    /// Read node-manager's whole orphan inventory, one page at a time,
    /// within [`ORPHAN_INVENTORY_DEADLINE`].
    async fn read_orphan_inventory(&self) -> OrphanFindings {
        let mut findings = OrphanFindings::default();
        let gateway_run_root = pplx_space_common_core::host::linux::paths::NET_GATEWAY_RUN_DIR;
        let read = async {
            let mut client =
                self.node_manager.connect().await.map_err(|error| {
                    format!("can't reach node-manager's orphan inventory: {error}")
                })?;
            let mut page_token = String::new();
            loop {
                let page = match client
                    .list_orphan_candidates(
                        pplx_space_node_mgr_client::proto::ListOrphanCandidatesRequest {
                            page_token: page_token.clone(),
                        },
                    )
                    .await
                {
                    Ok(page) => page.into_inner(),
                    // Node-manager serves one inventory at a time and answers
                    // another caller (an overlapping `:terminate` probe, or
                    // node-ctl) busy: ask again within the deadline.
                    Err(status) if status.code() == tonic::Code::ResourceExhausted => {
                        tokio::time::sleep(ORPHAN_INVENTORY_BUSY_RETRY).await;
                        continue;
                    }
                    Err(status) => return Err(format!("orphan inventory failed: {status}")),
                };
                classify_orphan_page(&page, gateway_run_root, &mut findings);
                if page.next_page_token.is_empty() {
                    return Ok(());
                }
                page_token = page.next_page_token;
            }
        };
        let failure = match tokio::time::timeout(ORPHAN_INVENTORY_DEADLINE, read).await {
            Ok(Ok(())) => None,
            Ok(Err(failure)) => Some(failure),
            Err(_) => Some(format!(
                "orphan inventory didn't finish within {}s",
                ORPHAN_INVENTORY_DEADLINE.as_secs()
            )),
        };
        findings.blocked.extend(failure);
        findings
    }

    /// Read every row, the artifacts without a durable copy and the
    /// evacuation record, and plan, with the ids of the rows read. An error
    /// is the scan that failed, which blocks the drain.
    fn read_drain_plan(
        &self,
        epoch: u64,
        disk_headroom: Option<u64>,
    ) -> Result<(DrainPlan, BTreeSet<String>), String> {
        let (sandboxes, local_only_rows, evacuations, pending_rows) = {
            let conn = self
                .db_conn()
                .map_err(|status| status.message().to_owned())?;
            // One lock, so no row or artifact lands between the reads.
            let sandboxes = crate::db::scan::all_sandboxes(&conn, crate::db::scan::StrictRows)
                .map_err(|error| format!("sandbox scan failed: {error}"))?;
            let local_only = crate::db::scan::local_only_artifacts(&conn)
                .map_err(|error| format!("local artifact scan failed: {error}"))?;
            let evacuations = crate::db::read::drain_evacuations(&conn)
                .map_err(|error| format!("evacuation record unreadable: {error}"))?;
            let pending = crate::db::scan::pending_backups(&conn)
                .map_err(|error| format!("pending backup scan failed: {error}"))?;
            (sandboxes, local_only, evacuations, pending)
        };
        let mut local_only = LocalOnlyArtifacts::new();
        for artifact in local_only_rows {
            local_only.entry(artifact.sandbox_id).or_default().push((
                artifact.local_artifact_id.to_string(),
                artifact.artifact_kind,
            ));
        }
        let mut pending_backups: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for pending in pending_rows {
            pending_backups
                .entry(pending.sandbox_id)
                .or_default()
                .insert(pending.local_artifact_id.to_string());
        }
        let active_uploads = pending_backups
            .values()
            .flatten()
            .chain(local_only.values().flatten().map(|(artifact, _)| artifact))
            .filter(|artifact| self.upload_job_active(artifact))
            .cloned()
            .collect();
        // A snapshot without a recorded guest memory is sized as disk
        // admission sizes an allocation it can't measure.
        let unknown_snapshot_bytes =
            pplx_space_common_core::host::disk_admission::CacheDiskAdmissionConfig::from_env()
                .allocation_reservation_bytes;
        let rows: Vec<DrainRow> = sandboxes
            .iter()
            .map(|row| DrainRow {
                sandbox_id: row.id.clone(),
                lifecycle: row.lifecycle,
                pool_member: row.pool_template.is_some(),
                volume_encryption: row.volume_encryption,
                pin: row
                    .current_local_artifact_id
                    .zip(row.current_local_artifact_kind)
                    .map(|(artifact, kind)| (artifact.to_string(), kind)),
                suspend_refusal: matches!(
                    row.lifecycle,
                    SandboxState::Started | SandboxState::Paused
                )
                .then(|| self.drain_suspend_refusal(row))
                .flatten(),
                snapshot_bytes: if matches!(
                    row.lifecycle,
                    SandboxState::Started | SandboxState::Paused
                ) {
                    super::tail_support::sandbox_guest_memory(&row.id)
                        .map_or(unknown_snapshot_bytes, |memory| memory.as_bytes())
                } else {
                    0
                },
            })
            .collect();
        let evacuations = evacuations
            .into_iter()
            .map(|evacuation| (evacuation.sandbox_id.clone(), evacuation))
            .collect();
        let ids = rows.iter().map(|row| row.sandbox_id.clone()).collect();
        let lifecycles: Vec<_> = rows
            .iter()
            .map(|row| (row.sandbox_id.clone(), row.lifecycle))
            .collect();
        let now = tokio::time::Instant::now();
        let inputs = PlanInputs {
            pending_backups,
            local_only,
            evacuations,
            running: self.drain_actions.running(),
            failures: self.drain_actions.failures(epoch),
            unchanged: self
                .drain_actions
                .unchanged_since_bound(epoch, &lifecycles, now),
            window: self.drain_actions.window,
            disk_headroom,
            active_uploads,
            disk_waits: self.drain_actions.disk_waits_past_bound(epoch, now),
        };
        let plan = plan(&rows, &inputs);
        self.drain_actions
            .note_disk_waits(epoch, &plan.disk_waits, now);
        // A failure counts toward the bound only while the drain still works
        // on its sandbox, starting it, waiting for it or reporting it
        // blocked; one that succeeded, or moved on, starts over.
        let working = plan
            .actions
            .iter()
            .map(|action| action.target().0.to_owned())
            .chain(inputs.running.keys().cloned())
            .chain(
                plan.pending
                    .iter()
                    .map(|(sandbox_id, _)| sandbox_id.clone()),
            )
            .chain(
                plan.blocked
                    .iter()
                    .filter_map(|(sandbox_id, _)| sandbox_id.clone()),
            )
            .collect();
        self.drain_actions.keep_failures_of(&working);
        Ok((plan, ids))
    }

    /// Why the Suspend RPC would refuse to suspend `row`, as the drain
    /// reports it.
    fn drain_suspend_refusal(&self, row: &crate::db::SandboxRow) -> Option<String> {
        // Pool members never reach here: the planner leaves them to the
        // warm-pool reconciler.
        self.ensure_memory_state_supported(row, "suspend")
            .and_then(|()| self.ensure_no_plaintext_pool_cow_for_snapshots(row, "suspend"))
            .and_then(|()| self.ensure_snapshot_restore_supported("suspend"))
            .err()
            .map(|status| format!("can't be made durable: {}", status.message()))
    }

    /// Start what the plan asks for that isn't already running.
    fn start_actions(&self, epoch: u64, plan: &mut DrainPlan) {
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

    /// Run one action, unless the node is no longer sealed under `epoch`:
    /// a cancel or a later drain has taken over, and a later call plans
    /// again.
    async fn run_drain_action(&self, epoch: u64, action: PlannedAction) -> Result<(), Status> {
        if self.activation_seal.status().sealed_epoch != Some(epoch) {
            return Ok(());
        }
        match action {
            PlannedAction::Suspend { sandbox_id } => self.drain_suspend(epoch, &sandbox_id).await,
            PlannedAction::PublishDisk { sandbox_id } => self
                .enqueue_artifact_backup_authorized(EnqueueArtifactBackupRequest {
                    sandbox_id,
                    expected_artifact_kind: crate::proto::ArtifactKind::Disk as i32,
                    source: Some(
                        crate::proto::enqueue_artifact_backup_request::Source::CurrentInstance(
                            crate::proto::CurrentInstanceSource::default(),
                        ),
                    ),
                })
                .await
                .map(|_| ()),
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

    /// Whether `artifact_id`'s upload job is still queued or running. A job
    /// that can't be read counts as none, so the upload is enqueued again.
    fn upload_job_active(&self, artifact_id: &str) -> bool {
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
    async fn drain_suspend(&self, epoch: u64, sandbox_id: &str) -> Result<(), Status> {
        let wake_lock = self.auto_lifecycle.wake_lock(sandbox_id);
        let wake_guard = wake_lock.lock_owned().await;
        let sandbox = self.lookup_sandbox(sandbox_id)?;
        let prior = match sandbox.state {
            SandboxState::Started => DrainPrior::Started,
            SandboxState::Paused => DrainPrior::Paused,
            _ => return Ok(()),
        };
        let Some(recorded) =
            self.activation_seal
                .record_drain_suspend(&self.db, epoch, &sandbox.id, prior)?
        else {
            return Ok(());
        };
        let artifact_lock = self.auto_lifecycle.artifact_lock(&sandbox.id);
        let artifact_guard = artifact_lock.lock_owned().await;
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
                .is_ok_and(|row| row.state == recorded_state)
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

/// [`drain_disk_headroom`] of `/space/cache` as disk admission measures it;
/// none when it can't be measured.
async fn read_disk_headroom() -> Option<u64> {
    use pplx_space_common_core::host::disk_admission::{
        CacheDiskAdmissionConfig, read_cache_disk_measurement,
    };
    let config = CacheDiskAdmissionConfig::from_env();
    if !config.enabled {
        return None;
    }
    let measured = tokio::task::spawn_blocking(read_cache_disk_measurement).await;
    Some(match measured {
        Ok(Ok(measurement)) => drain_disk_headroom(config, measurement),
        Ok(Err(error)) => {
            tracing::warn!(%error, "node drain: can't measure the cache disk; no suspend starts");
            0
        }
        Err(error) => {
            tracing::warn!(%error, "node drain: the cache disk probe failed; no suspend starts");
            0
        }
    })
}

fn report(status: &ActivationSealStatus, plan: DrainPlan, done: bool) -> DrainNodeResponse {
    DrainNodeResponse {
        sealed_epoch: status.sealed_epoch,
        evacuated: plan
            .evacuated
            .into_iter()
            .map(|(sandbox_id, prior, artifact_id)| DrainEvacuated {
                sandbox_id,
                prior: prior_to_wire(prior) as i32,
                artifact_id,
            })
            .collect(),
        pending: plan
            .pending
            .into_iter()
            .map(|(sandbox_id, action)| DrainPending { sandbox_id, action })
            .collect(),
        blocked: plan
            .blocked
            .into_iter()
            .map(|(sandbox_id, reason)| DrainBlocked { sandbox_id, reason })
            .collect(),
        done,
    }
}

const fn prior_to_wire(prior: DrainPrior) -> crate::proto::DrainPrior {
    match prior {
        DrainPrior::Started => crate::proto::DrainPrior::Started,
        DrainPrior::Paused => crate::proto::DrainPrior::Paused,
        DrainPrior::Stopped => crate::proto::DrainPrior::Stopped,
    }
}

#[cfg(test)]
mod tests;
