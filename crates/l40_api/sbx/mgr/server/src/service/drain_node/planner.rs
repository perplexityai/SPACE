//! The drain's planner: what one call decides, a pure function of what it
//! read.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use super::actions::DEFAULT_DRAIN_NODE_WINDOW;
use crate::db::activation_seal::{DrainEvacuation, DrainPrior};
use crate::service::*;

/// The pending action of a running sandbox a gentle drain holds.
pub(super) const HOLD: &str = "hold";

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

/// The bytes of cache disk the drain may still commit to snapshots before
/// the cache reaches the floor its disk admission keeps free, the margin
/// every new allocation leaves; none while admission refuses allocations.
pub(super) fn drain_disk_headroom(
    config: pplx_space_node_disk_admission::CacheDiskAdmissionConfig,
    measurement: pplx_space_node_disk_admission::CacheDiskMeasurement,
) -> u64 {
    let decision =
        pplx_space_node_disk_admission::evaluate_cache_disk_measurement(config, measurement, 0);
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
    /// The adoption and upload of the snapshot of a suspended sandbox whose
    /// row pins no artifact, so nothing proves it durable.
    PublishSnapshot,
    /// Finishing a local-only artifact's upload.
    Upload,
}

impl DrainAction {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Suspend => "suspend",
            Self::PublishDisk => "publish_disk",
            Self::PublishSnapshot => "publish_snapshot",
            Self::Upload => "upload",
        }
    }
}

/// The failures of each kind of the drain's work on each sandbox, with the
/// last one's reason.
pub(super) type DrainFailures = BTreeMap<(String, DrainAction), (u32, String)>;

/// One row as the planner reads it.
#[derive(Clone, Debug)]
pub(super) struct DrainRow {
    pub(super) sandbox_id: String,
    /// The lifecycle, and the cleanup that owns the row if one does.
    pub(super) lifecycle: SandboxStatus,
    /// An unclaimed warm-pool member.
    pub(super) pool_member: bool,
    pub(super) volume_encryption: bool,
    /// The pinned local artifact and its kind.
    pub(super) pin: Option<(String, pplx_space_snapshot_model::ArtifactKind)>,
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
        /// The lifecycles it may suspend from, checked again under the wake
        /// lock: a row planned `paused` may be `started` by the time the
        /// suspend runs.
        from: SuspendFrom,
    },
    PublishDisk {
        sandbox_id: String,
    },
    PublishSnapshot {
        sandbox_id: String,
    },
    Upload {
        sandbox_id: String,
        artifact_id: String,
        kind: pplx_space_snapshot_model::ArtifactKind,
    },
}

/// The lifecycles a drain suspend may suspend a VM from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SuspendFrom {
    /// A forced drain's: `started` or `paused`.
    StartedOrPaused,
    /// A gentle drain's: `paused` only, since it holds a started sandbox.
    Paused,
}

impl SuspendFrom {
    fn for_drain(gentle: bool) -> Self {
        if gentle {
            Self::Paused
        } else {
            Self::StartedOrPaused
        }
    }
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
    /// The sandboxes whose running action this plan already waits for, so
    /// that each is reported once however many of its rows' arms and
    /// artifacts would wait for it.
    pub(super) waits_on_running: BTreeSet<String>,
}

/// Whether a row in `status` may hold a VM. A cleanup-owned row may, failed
/// or not: a cleanup that failed can leave its VM running.
pub(super) const fn may_hold_vm(status: SandboxStatus) -> bool {
    match status {
        SandboxStatus::Unowned(lifecycle) => !matches!(
            lifecycle,
            SandboxState::Stopped | SandboxState::Suspended | SandboxState::Failed
        ),
        SandboxStatus::Owned { .. } => true,
    }
}

/// A sandbox's artifacts without a durable copy, by sandbox, from the
/// artifact registry: named snapshots and superseded lifecycle artifacts as
/// well as the current pin.
pub(super) type LocalOnlyArtifacts =
    BTreeMap<String, Vec<(String, pplx_space_snapshot_model::ArtifactKind)>>;

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
    /// A gentle drain: hold every sandbox in `started`.
    pub(super) gentle: bool,
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
            gentle: false,
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

    /// The row's pinned artifact, while it is still without a durable copy.
    fn pending_pin(
        &self,
        row: &DrainRow,
    ) -> Option<(String, pplx_space_snapshot_model::ArtifactKind)> {
        row.pin.clone().filter(|(artifact, _)| {
            self.pending_backups
                .get(&row.sandbox_id)
                .is_some_and(|pending| pending.contains(artifact))
        })
    }

    /// Whether the row's pinned artifact is still uploading as the window
    /// counts it: without a durable copy, nothing running for the row, and
    /// its upload not blocked on its failures. A started or paused row's pin
    /// counts only while its upload job is queued or running: the drain
    /// suspends that row rather than driving the upload, so a capture a
    /// suspend rollback left pinned, its job given up, would otherwise hold a
    /// slot for good.
    fn uploading(&self, row: &DrainRow) -> bool {
        let Some((artifact, _)) = self.pending_pin(row) else {
            return false;
        };
        let vm = matches!(
            row.lifecycle,
            SandboxStatus::Unowned(SandboxState::Started | SandboxState::Paused)
        );
        !self.running.contains_key(&row.sandbox_id)
            && !self.failed_too_often(&row.sandbox_id, DrainAction::Upload)
            && (!vm || self.active_uploads.contains(&artifact))
    }

    /// How much of the window is taken: the suspends and publications
    /// running, and the rows whose pinned artifact is still uploading.
    pub(super) fn window_in_flight(&self, rows: &[DrainRow]) -> usize {
        let running = self
            .running
            .values()
            .filter(|action| {
                matches!(
                    action,
                    DrainAction::Suspend | DrainAction::PublishDisk | DrainAction::PublishSnapshot
                )
            })
            .count();
        running + rows.iter().filter(|row| self.uploading(row)).count()
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

    /// Wait for `running`, the action running for `sandbox_id`, unless this
    /// plan already waits for it: the row's own arm and each of its other
    /// artifacts would otherwise report the same wait again.
    fn wait_for_running(&mut self, inputs: &PlanInputs, sandbox_id: &str, running: DrainAction) {
        if self.waits_on_running.insert(sandbox_id.to_owned()) {
            self.wait_for(inputs, sandbox_id, running);
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
            Some(running) => self.wait_for_running(inputs, &sandbox_id, *running),
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

    /// Start the suspends and publications `waiting` names while the window
    /// has room, paused VMs first, then started ones, then stopped disks,
    /// then unpinned suspend snapshots, and a suspend only while the cache
    /// disk still covers its
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
/// one, so it goes first; a stopped disk and a suspended snapshot hold no
/// VM, so they go last.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) enum WindowOrder {
    PausedVm,
    StartedVm,
    StoppedDisk,
    SuspendedSnapshot,
}

impl PlannedAction {
    pub(super) fn target(&self) -> (&str, DrainAction) {
        match self {
            Self::Suspend { sandbox_id, .. } => (sandbox_id, DrainAction::Suspend),
            Self::PublishDisk { sandbox_id } => (sandbox_id, DrainAction::PublishDisk),
            Self::PublishSnapshot { sandbox_id } => (sandbox_id, DrainAction::PublishSnapshot),
            Self::Upload { sandbox_id, .. } => (sandbox_id, DrainAction::Upload),
        }
    }
}

/// Decide what the drain does for every row.
pub(super) fn plan(rows: &[DrainRow], inputs: &PlanInputs) -> DrainPlan {
    use pplx_space_snapshot_model::ArtifactKind;
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
            // transition does. A failed cleanup blocks as a resident's does,
            // with the same reason.
            if row.lifecycle.cleanup() == Some(CleanupState::Failed) {
                plan.blocked
                    .push((Some(id.clone()), "cleanup failed".into()));
            } else if may_hold_vm(row.lifecycle) {
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
            SandboxStatus::Unowned(SandboxState::Quarantined) => {
                plan.blocked.push((Some(id.clone()), "quarantined".into()));
            }
            SandboxStatus::Owned {
                cleanup: CleanupState::Failed,
                ..
            } => {
                plan.blocked
                    .push((Some(id.clone()), "cleanup failed".into()));
            }
            SandboxStatus::Unowned(SandboxState::Started | SandboxState::Paused) => {
                let suspend = PlannedAction::Suspend {
                    sandbox_id: id.clone(),
                    from: SuspendFrom::for_drain(inputs.gentle),
                };
                match &row.suspend_refusal {
                    Some(refusal) if !running => {
                        plan.blocked.push((Some(id.clone()), refusal.clone()));
                    }
                    _ if running => plan.drive(inputs, suspend),
                    // A gentle drain leaves it running; once it leaves
                    // `started` on its own, the row is planned from there.
                    // Its pin's upload is finished meanwhile, as it is for
                    // any other row: the artifacts below skip the pin, and
                    // no suspend will replace it while the drain holds the
                    // sandbox. The hold keeps the drain pending either way.
                    _ if inputs.gentle && row.lifecycle.is_unowned(SandboxState::Started) => {
                        if let Some(pin) = pending_pin.clone() {
                            plan.drive(inputs, upload(pin));
                        }
                        plan.pending.push((id.clone(), HOLD.to_owned()));
                    }
                    _ => windowed.push((
                        if row.lifecycle.is_unowned(SandboxState::Paused) {
                            WindowOrder::PausedVm
                        } else {
                            WindowOrder::StartedVm
                        },
                        row,
                        suspend,
                    )),
                }
            }
            SandboxStatus::Unowned(SandboxState::Stopped) => match (&durable_pin, pending_pin) {
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
            SandboxStatus::Unowned(SandboxState::Suspended | SandboxState::Failed) => {
                if let Some(pin) = pending_pin {
                    plan.drive(inputs, upload(pin));
                } else if let Some(evacuation) = inputs.evacuations.get(id) {
                    // The drain suspended it; its artifact is durable now.
                    match &durable_pin {
                        Some((artifact, ArtifactKind::Full))
                            if row.lifecycle.is_unowned(SandboxState::Suspended) =>
                        {
                            plan.evacuated
                                .push((id.clone(), evacuation.prior, artifact.clone()));
                        }
                        _ => plan.blocked.push((
                            Some(id.clone()),
                            format!(
                                "the drain's suspend left the sandbox {} without a durable \
                                 snapshot",
                                row.lifecycle.legacy_label()
                            ),
                        )),
                    }
                } else if row.pin.is_none() && row.lifecycle.is_unowned(SandboxState::Suspended) {
                    // A suspended sandbox whose row pins no artifact, from
                    // before the artifact registry or a publication that
                    // never pinned: the registry holds nothing to prove its
                    // snapshot durable. The node's backup adopts and uploads
                    // it, as `EnqueueArtifactBackup` of its full current
                    // instance does; once pinned, its upload is finished
                    // like any other. It keeps its own lifecycle: the drain
                    // didn't suspend it, so it is never reported evacuated.
                    let publish = PlannedAction::PublishSnapshot {
                        sandbox_id: id.clone(),
                    };
                    if running {
                        plan.drive(inputs, publish);
                    } else {
                        windowed.push((WindowOrder::SuspendedSnapshot, row, publish));
                    }
                }
            }
            other => {
                // A transition the drain doesn't drive: wait for it to settle,
                // and page once it has sat there too long.
                if let Some(action) = inputs.running.get(id) {
                    plan.wait_for_running(inputs, id, *action);
                } else if inputs.unchanged.contains(id) {
                    plan.blocked.push((
                        Some(id.clone()),
                        format!(
                            "{} for over {} minutes, a transition the drain doesn't drive",
                            other.legacy_label(),
                            SETTLE_BOUND.as_secs() / 60
                        ),
                    ));
                } else {
                    plan.pending
                        .push((id.clone(), format!("settle:{}", other.legacy_label())));
                }
            }
        }
        // Every other artifact the sandbox keeps without a durable copy, a
        // named snapshot or one its pin moved on from, is finished the same
        // way. The arm above owns the pin, and a row being deleted takes its
        // artifacts with it.
        if row.lifecycle.lifecycle() == SandboxState::Deleting {
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
