//! The drain's planner: what one pass decides, a pure function of what it
//! read, and node-manager's orphan inventory as the planner reads it.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::actions::DEFAULT_DRAIN_NODE_WINDOW;
use crate::service::*;
use pplx_space_snapshot_model::ArtifactKind;

/// Why a started sandbox the drain holds is pending ([`Wait::Hold`]).
pub(super) const HOLD_REASON: &str = "kept running; suspended once the drain evacuates";

/// Why a row whose cleanup failed is pending: nothing on the node moves it on.
const CLEANUP_FAILED_REASON: &str = "cleanup failed";

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

    /// The work as a pending row's reason names it.
    const fn noun(self) -> &'static str {
        match self {
            Self::Suspend => "suspend",
            Self::PublishDisk => "disk publication",
            Self::PublishSnapshot => "snapshot publication",
            Self::Upload => "upload",
        }
    }
}

/// The last failure of each kind of the drain's work on each sandbox.
pub(super) type DrainFailures = BTreeMap<(String, DrainAction), String>;

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
    pub(super) pin: Option<(String, ArtifactKind)>,
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
        kind: ArtifactKind,
    },
}

/// The lifecycles a drain suspend may suspend a VM from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SuspendFrom {
    /// While evacuating: `started` or `paused`.
    StartedOrPaused,
    /// While holding: `paused` only, since the drain keeps a started sandbox
    /// running.
    Paused,
}

impl SuspendFrom {
    fn for_drain(hold_running: bool) -> Self {
        if hold_running {
            Self::Paused
        } else {
            Self::StartedOrPaused
        }
    }

    /// Whether this suspend applies to a row in `state`.
    pub(super) fn allows(self, state: Option<SandboxState>) -> bool {
        matches!(
            (self, state),
            (Self::StartedOrPaused, Some(SandboxState::Started))
                | (
                    Self::StartedOrPaused | Self::Paused,
                    Some(SandboxState::Paused)
                )
        )
    }
}

/// What a pending sandbox waits for. A report gives its kind
/// ([`Self::kind`]) and its detail in the reason ([`DrainWait::reason`]).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Wait {
    /// The drain's work of this kind, running or about to start.
    Work(DrainAction),
    /// Room in the drain's window for its suspend or publication.
    Queued(DrainAction),
    /// Cache disk room for its suspend's snapshot.
    Disk,
    /// An activation admitted before the seal, still in flight.
    Activation,
    /// The warm-pool reconciler's eviction of an unclaimed VM.
    WarmPoolEviction,
    /// A state or transition the drain doesn't drive.
    Settle(SandboxStatus),
    /// A started sandbox the drain keeps running while it holds.
    Hold,
}

impl Wait {
    /// The kind a report gives: held running, being made durable by the
    /// drain, or waiting on work the drain doesn't drive.
    pub(super) const fn kind(self) -> crate::proto::DrainWait {
        match self {
            Self::Hold => crate::proto::DrainWait::Hold,
            Self::Work(_) | Self::Queued(_) | Self::Disk => crate::proto::DrainWait::Moving,
            Self::Activation | Self::WarmPoolEviction | Self::Settle(_) => {
                crate::proto::DrainWait::External
            }
        }
    }

    /// What the row waits for, as a report's reason says it.
    fn detail(self) -> String {
        match self {
            Self::Work(action) => format!("the drain's {}", action.noun()),
            Self::Queued(action) => {
                format!("the drain's {}, waiting for a window slot", action.noun())
            }
            Self::Disk => "the drain's suspend, waiting for cache disk room".to_owned(),
            Self::Activation => "an activation admitted before the seal is in flight".to_owned(),
            Self::WarmPoolEviction => {
                "an unclaimed warm-pool VM, left to the warm-pool reconciler's eviction".to_owned()
            }
            Self::Settle(status) => {
                format!("{}, which the drain doesn't drive", status.legacy_label())
            }
            Self::Hold => HOLD_REASON.to_owned(),
        }
    }

    /// A short name for the wait, which the tests compare.
    #[cfg(test)]
    pub(super) fn label(self) -> String {
        match self {
            Self::Work(action) => action.as_str().to_owned(),
            Self::Queued(_) => "queued".to_owned(),
            Self::Disk => "disk".to_owned(),
            Self::Activation => "activation".to_owned(),
            Self::WarmPoolEviction => "warm_pool_eviction".to_owned(),
            Self::Settle(status) => format!("settle:{}", status.legacy_label()),
            Self::Hold => "hold".to_owned(),
        }
    }
}

/// A sandbox the drain still waits for. One the drain can't move is pending
/// too, with why in `reason`: the drain reports no sandbox blocked.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DrainWait {
    pub(super) sandbox_id: String,
    pub(super) wait: Wait,
    /// Why, when the wait alone doesn't say.
    pub(super) reason: Option<String>,
}

impl DrainWait {
    /// The reason a report gives: what the row waits for, then why when the
    /// wait alone doesn't say. Display text only.
    pub(super) fn reason(&self) -> String {
        let detail = self.wait.detail();
        match &self.reason {
            Some(reason) => format!("{detail}: {reason}"),
            None => detail,
        }
    }
}

/// What one pass decided, from the rows it read.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct DrainPlan {
    /// The sandboxes whose pinned artifact is durable: a suspended one's full
    /// snapshot or a stopped one's disk, with the artifact.
    pub(super) evacuated: Vec<(String, ArtifactKind, String)>,
    pub(super) pending: Vec<DrainWait>,
    /// Node-wide problems, which name no sandbox.
    pub(super) blocked: Vec<String>,
    pub(super) actions: Vec<PlannedAction>,
    /// The sandboxes whose running action this plan already waits for, so
    /// that each is reported once however many of its rows' arms and
    /// artifacts would wait for it.
    pub(super) waits_on_running: BTreeSet<String>,
    /// The sandboxes whose row was a started VM at the read the plan comes
    /// from; a paused one isn't. A report marks each of their pending
    /// entries `running`.
    pub(super) started: BTreeSet<String>,
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
pub(super) type LocalOnlyArtifacts = BTreeMap<String, Vec<(String, ArtifactKind)>>;

/// Everything the planner reads besides the rows.
#[derive(Clone, Debug)]
pub(super) struct PlanInputs {
    /// The pinned artifacts still without a durable copy, by sandbox,
    /// including a pin that names no artifact row.
    pub(super) pending_backups: BTreeMap<String, BTreeSet<String>>,
    /// Every artifact without a durable copy.
    pub(super) local_only: LocalOnlyArtifacts,
    /// The drain's work in flight in this process.
    pub(super) running: BTreeMap<String, DrainAction>,
    /// The last failure of the drain's work under this pass's epoch.
    pub(super) failures: DrainFailures,
    /// How many suspends and disk publications may be in flight at once.
    pub(super) window: usize,
    /// The bytes of cache disk the drain may commit to new snapshots; `None`
    /// when the node admits cache allocations without measuring.
    pub(super) disk_headroom: Option<u64>,
    /// The artifacts whose upload job is still queued or running.
    pub(super) active_uploads: BTreeSet<String>,
    /// The drain holds: keep every sandbox in `started` running.
    pub(super) hold_running: bool,
}

impl Default for PlanInputs {
    fn default() -> Self {
        Self {
            pending_backups: BTreeMap::new(),
            local_only: LocalOnlyArtifacts::new(),
            running: BTreeMap::new(),
            failures: DrainFailures::new(),
            window: DEFAULT_DRAIN_NODE_WINDOW,
            disk_headroom: None,
            active_uploads: BTreeSet::new(),
            hold_running: false,
        }
    }
}

impl PlanInputs {
    /// Why `sandbox_id`'s `action` last failed under this epoch, as a
    /// pending row's reason.
    fn last_failure(&self, sandbox_id: &str, action: DrainAction) -> Option<String> {
        self.failures
            .get(&(sandbox_id.to_owned(), action))
            .map(|last| format!("the last {} failed: {last}", action.noun()))
    }

    /// The row's pinned artifact, while it is still without a durable copy.
    fn pending_pin(&self, row: &DrainRow) -> Option<(String, ArtifactKind)> {
        row.pin.clone().filter(|(artifact, _)| {
            self.pending_backups
                .get(&row.sandbox_id)
                .is_some_and(|pending| pending.contains(artifact))
        })
    }

    /// Whether the row's pinned artifact is still uploading as the window
    /// counts it: without a durable copy, nothing running for the row, and
    /// its upload job queued or running. A job that failed, or a pin whose
    /// upload no job drives, holds no slot: a capture a suspend rollback left
    /// pinned, its job given up, would otherwise hold one for good.
    fn uploading(&self, row: &DrainRow) -> bool {
        self.pending_pin(row).is_some_and(|(artifact, _)| {
            !self.running.contains_key(&row.sandbox_id) && self.active_uploads.contains(&artifact)
        })
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
    fn wait(&mut self, sandbox_id: &str, wait: Wait, reason: Option<String>) {
        self.pending.push(DrainWait {
            sandbox_id: sandbox_id.to_owned(),
            wait,
            reason,
        });
    }

    /// Wait for `action` on `sandbox_id`, with its last failure as the
    /// reason if it has failed under this epoch.
    fn wait_for(&mut self, inputs: &PlanInputs, sandbox_id: &str, action: DrainAction) {
        self.wait_as(inputs, sandbox_id, action, Wait::Work(action));
    }

    /// [`Self::wait_for`], reporting the row as waiting for `wait`.
    fn wait_as(&mut self, inputs: &PlanInputs, sandbox_id: &str, action: DrainAction, wait: Wait) {
        self.wait(sandbox_id, wait, inputs.last_failure(sandbox_id, action));
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
    /// disk still covers its snapshot. The rest wait, queued for the window
    /// or for the disk.
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
                self.wait_as(inputs, id, kind, Wait::Queued(kind));
                continue;
            }
            if kind == DrainAction::Suspend {
                let needed = committed.saturating_add(row.snapshot_bytes);
                if inputs
                    .disk_headroom
                    .is_some_and(|headroom| needed > headroom)
                {
                    self.wait(
                        id,
                        Wait::Disk,
                        Some(format!(
                            "the cache disk has no room yet for a {} MiB snapshot",
                            row.snapshot_bytes / (1024 * 1024)
                        )),
                    );
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

/// Decide what the drain does for every row. What it reports comes from the
/// rows alone: a suspended row whose pinned full snapshot is durable, and a
/// stopped one whose pinned disk is, are evacuated whatever suspended or
/// stopped them. Every reason the drain isn't finished is an entry: each row
/// that may hold a VM, and each artifact still without a durable copy, is
/// pending, and a node-wide problem is blocked, so the drain is finished
/// exactly when the plan has neither.
pub(super) fn plan(rows: &[DrainRow], inputs: &PlanInputs) -> DrainPlan {
    let mut plan = DrainPlan {
        started: rows
            .iter()
            .filter(|row| row.lifecycle.is_unowned(SandboxState::Started))
            .map(|row| row.sandbox_id.clone())
            .collect(),
        ..DrainPlan::default()
    };
    let mut windowed = Vec::new();
    for (sandbox_id, artifacts) in &inputs.local_only {
        if !rows.iter().any(|row| row.sandbox_id == *sandbox_id) {
            for (artifact, _) in artifacts {
                plan.blocked.push(format!(
                    "local-only artifact {artifact} of sandbox {sandbox_id} has no sandbox row"
                ));
            }
        }
    }
    for row in rows {
        let id = &row.sandbox_id;
        if row.pool_member {
            // Not a resident: the warm-pool reconciler evicts slots on a
            // cordoned node, and until it has, the VM keeps the drain pending. A
            // failed cleanup waits as a resident's does, with the same
            // reason.
            if row.lifecycle.cleanup() == Some(CleanupState::Failed) {
                plan.wait(
                    id,
                    Wait::Settle(row.lifecycle),
                    Some(CLEANUP_FAILED_REASON.to_owned()),
                );
            } else if may_hold_vm(row.lifecycle) {
                plan.wait(id, Wait::WarmPoolEviction, None);
            }
            continue;
        }
        let running = inputs.running.contains_key(id);
        let pending_pin = inputs.pending_pin(row);
        let durable_pin = row.pin.clone().filter(|_| pending_pin.is_none());
        let upload = |(artifact_id, kind): (String, ArtifactKind)| PlannedAction::Upload {
            sandbox_id: id.clone(),
            artifact_id,
            kind,
        };
        match row.lifecycle {
            SandboxStatus::Unowned(SandboxState::Quarantined) => plan.wait(
                id,
                Wait::Settle(row.lifecycle),
                Some("nothing on the node moves it on".to_owned()),
            ),
            SandboxStatus::Owned {
                cleanup: CleanupState::Failed,
                ..
            } => plan.wait(
                id,
                Wait::Settle(row.lifecycle),
                Some(CLEANUP_FAILED_REASON.to_owned()),
            ),
            SandboxStatus::Unowned(SandboxState::Started | SandboxState::Paused) => {
                let suspend = PlannedAction::Suspend {
                    sandbox_id: id.clone(),
                    from: SuspendFrom::for_drain(inputs.hold_running),
                };
                match &row.suspend_refusal {
                    Some(refusal) if !running => {
                        plan.wait(id, Wait::Work(DrainAction::Suspend), Some(refusal.clone()));
                    }
                    _ if running => plan.drive(inputs, suspend),
                    // Held while the drain holds; once it leaves `started` on
                    // its own, the row is planned from there. Its pin's
                    // upload is finished meanwhile, as it is for any other
                    // row: the artifacts below skip the pin, and no suspend
                    // will replace it while the drain holds the sandbox. The
                    // hold keeps the drain pending either way.
                    _ if inputs.hold_running && row.lifecycle.is_unowned(SandboxState::Started) => {
                        if let Some(pin) = pending_pin.clone() {
                            plan.drive(inputs, upload(pin));
                        }
                        plan.wait(id, Wait::Hold, None);
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
                _ if row.volume_encryption && !running => plan.wait(
                    id,
                    Wait::Work(DrainAction::PublishDisk),
                    Some("an encrypted stopped sandbox's disk can't be published".to_owned()),
                ),
                (Some((artifact, ArtifactKind::Disk)), _) if !running => {
                    plan.evacuated
                        .push((id.clone(), ArtifactKind::Disk, artifact.clone()));
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
            SandboxStatus::Unowned(SandboxState::Suspended) => match (&durable_pin, pending_pin) {
                (_, Some(pin)) => plan.drive(inputs, upload(pin)),
                (Some((artifact, ArtifactKind::Full)), None) => {
                    plan.evacuated
                        .push((id.clone(), ArtifactKind::Full, artifact.clone()));
                }
                (Some(_), None) => plan.wait(
                    id,
                    Wait::Settle(row.lifecycle),
                    Some("its pinned artifact is not a full snapshot".to_owned()),
                ),
                // A suspended sandbox whose row pins no artifact, from before
                // the artifact registry or a publication that never pinned:
                // the registry holds nothing to prove its snapshot durable.
                // The node's backup adopts and uploads it, as
                // `EnqueueArtifactBackup` of its full current instance does;
                // once pinned, its upload is finished like any other, and
                // once durable it is evacuated.
                (None, None) => {
                    let publish = PlannedAction::PublishSnapshot {
                        sandbox_id: id.clone(),
                    };
                    if running {
                        plan.drive(inputs, publish);
                    } else {
                        windowed.push((WindowOrder::SuspendedSnapshot, row, publish));
                    }
                }
            },
            // Nothing on the node to keep once its artifacts are durable.
            SandboxStatus::Unowned(SandboxState::Failed) => {
                if let Some(pin) = pending_pin {
                    plan.drive(inputs, upload(pin));
                }
            }
            other => {
                // A transition the drain doesn't drive: wait for it to settle.
                if let Some(action) = inputs.running.get(id) {
                    plan.wait_for_running(inputs, id, *action);
                } else {
                    plan.wait(id, Wait::Settle(other), None);
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

/// What a drain call scans outside the rows: node-manager's orphan inventory
/// and the cache disk's headroom.
#[derive(Clone, Debug, Default)]
pub(super) struct DrainScans {
    /// Node-wide problems the scans found.
    pub(super) blocked: Vec<String>,
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
