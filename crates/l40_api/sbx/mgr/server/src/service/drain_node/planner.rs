//! The drain's planner: what one pass decides, a pure function of what it
//! read, and node-manager's orphan inventory as the planner reads it.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::actions::DEFAULT_DRAIN_NODE_WINDOW;
use crate::service::*;
use pplx_space_snapshot_model::ArtifactKind;

/// Why a started sandbox the drain holds is pending ([`Wait::Hold`]).
pub(super) const HOLD_REASON: &str = "kept running; suspended once the drain evacuates";

/// Why a stopped sandbox is pending ([`Wait::Stopped`]): the drain leaves it
/// where it is, and the node is never finished while it holds one.
pub(super) const STOPPED_REASON: &str = "stopped sandbox: drain v2 doesn't move stopped sandboxes";

/// Why a sandbox a node-local artifact producer works on is pending
/// ([`Wait::Snapshot`]).
pub(super) const SNAPSHOT_REASON: &str = "a snapshot is being published";

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
            Self::PublishSnapshot => "publish_snapshot",
            Self::Upload => "upload",
        }
    }

    /// The work as a pending row's reason names it.
    const fn noun(self) -> &'static str {
        match self {
            Self::Suspend => "suspend",
            Self::PublishSnapshot => "snapshot publication",
            Self::Upload => "upload",
        }
    }
}

/// An action of the drain's running in this process, with what it holds
/// while it runs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum RunningAction {
    /// A suspend, holding the cache disk the planner sized for its snapshot
    /// when it started it, until the suspend ends: from then on the snapshot
    /// is on the disk, and the next measurement sees it.
    Suspend {
        reserved_bytes: u64,
    },
    PublishSnapshot,
    /// The upload of `artifact_id`.
    Upload {
        artifact_id: String,
    },
}

impl RunningAction {
    /// The artifact an upload uploads.
    pub(super) fn artifact(&self) -> Option<&str> {
        match self {
            Self::Upload { artifact_id } => Some(artifact_id),
            Self::Suspend { .. } | Self::PublishSnapshot => None,
        }
    }

    pub(super) const fn kind(&self) -> DrainAction {
        match self {
            Self::Suspend { .. } => DrainAction::Suspend,
            Self::PublishSnapshot => DrainAction::PublishSnapshot,
            Self::Upload { .. } => DrainAction::Upload,
        }
    }
}

/// How the last of one kind of the drain's work on a sandbox failed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DrainFailure {
    pub(super) reason: String,
    /// The artifact a failed upload was uploading.
    pub(super) artifact: Option<String>,
}

/// The last failure of each kind of the drain's work on each sandbox.
pub(super) type DrainFailures = BTreeMap<(String, DrainAction), DrainFailure>;

/// One row as the planner reads it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DrainRow {
    pub(super) sandbox_id: String,
    /// The lifecycle, and the cleanup that owns the row if one does.
    pub(super) lifecycle: SandboxStatus,
    /// An unclaimed warm-pool member.
    pub(super) pool_member: bool,
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
        /// The cache disk the plan sized for its snapshot, which the suspend
        /// reserves while it runs ([`RunningAction::Suspend`]).
        snapshot_bytes: u64,
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
    /// Room in the drain's window for its suspend, publication or upload.
    Queued(DrainAction),
    /// Cache disk room for its suspend's snapshot.
    Disk,
    /// A VM whose suspend the node refuses: nothing the drain does moves it
    /// until what refuses it changes.
    Refused,
    /// An activation admitted before the seal, still in flight.
    Activation,
    /// A row the plan found finished whose artifacts a writer holds, or
    /// that changed while the drain read it: planned again on the next
    /// call.
    Recheck,
    /// A node-local artifact producer (a snapshot, an import, a publication)
    /// still in flight, whose artifact the drain makes durable once it is
    /// published.
    Snapshot,
    /// The warm-pool reconciler's eviction of an unclaimed VM.
    WarmPoolEviction,
    /// A state or transition the drain doesn't drive.
    Settle(SandboxStatus),
    /// A stopped sandbox, which the drain doesn't move ([`STOPPED_REASON`]).
    Stopped,
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
            Self::Refused
            | Self::Activation
            | Self::Recheck
            | Self::Snapshot
            | Self::WarmPoolEviction
            | Self::Settle(_)
            | Self::Stopped => crate::proto::DrainWait::External,
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
            Self::Refused => "the node refuses its suspend".to_owned(),
            Self::Activation => "an activation admitted before the seal is in flight".to_owned(),
            Self::Recheck => "checked again on the next call".to_owned(),
            Self::Snapshot => SNAPSHOT_REASON.to_owned(),
            Self::WarmPoolEviction => {
                "an unclaimed warm-pool VM, left to the warm-pool reconciler's eviction".to_owned()
            }
            Self::Settle(status) => {
                format!("{}, which the drain doesn't drive", status.legacy_label())
            }
            Self::Stopped => STOPPED_REASON.to_owned(),
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
            Self::Refused => "refused".to_owned(),
            Self::Activation => "activation".to_owned(),
            Self::Recheck => "recheck".to_owned(),
            Self::Snapshot => "snapshot".to_owned(),
            Self::WarmPoolEviction => "warm_pool_eviction".to_owned(),
            Self::Settle(status) => format!("settle:{}", status.legacy_label()),
            Self::Stopped => "stopped".to_owned(),
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
    /// The suspended sandboxes whose pinned full snapshot is durable, with
    /// the artifact.
    pub(super) evacuated: Vec<(String, ArtifactKind, String)>,
    pub(super) pending: Vec<DrainWait>,
    /// Node-wide problems, which name no sandbox.
    pub(super) blocked: Vec<String>,
    /// At most one per sandbox.
    pub(super) actions: Vec<PlannedAction>,
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
    /// The drain's work in flight in this process, read before the rows.
    pub(super) running: BTreeMap<String, RunningAction>,
    /// The last failure of the drain's work under this pass's generation.
    pub(super) failures: DrainFailures,
    /// How many sandboxes the drain's suspends, publications and the pin
    /// uploads they leave may hold at once.
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
    /// Why `sandbox_id`'s `action` last failed under this generation, as a
    /// pending row's reason.
    fn last_failure(&self, sandbox_id: &str, action: DrainAction) -> Option<String> {
        self.failures
            .get(&(sandbox_id.to_owned(), action))
            .map(|last| format!("the last {} failed: {}", action.noun(), last.reason))
    }

    /// Why the last upload of `sandbox_id`'s pin, `pin`, failed under this
    /// generation, as a pending row's reason; none when the last upload that
    /// failed was another artifact's.
    fn last_pin_upload_failure(&self, sandbox_id: &str, pin: &str) -> Option<String> {
        self.failures
            .get(&(sandbox_id.to_owned(), DrainAction::Upload))
            .filter(|last| last.artifact.as_deref() == Some(pin))
            .and(self.last_failure(sandbox_id, DrainAction::Upload))
    }

    /// The row's pinned artifact, while it is still without a durable copy.
    fn pending_pin(&self, row: &DrainRow) -> Option<(String, ArtifactKind)> {
        row.pin.clone().filter(|(artifact, _)| {
            self.pending_backups
                .get(&row.sandbox_id)
                .is_some_and(|pending| pending.contains(artifact))
        })
    }

    /// `row`'s stage at this pass.
    pub(super) fn stage(&self, row: &DrainRow) -> Stage {
        let pending_pin = self.pending_pin(row);
        let pin_job_active = pending_pin
            .as_ref()
            .is_some_and(|(artifact, _)| self.active_uploads.contains(artifact));
        Stage::of(
            row,
            self.running.get(&row.sandbox_id),
            pending_pin,
            pin_job_active,
            self.hold_running,
        )
    }
}

/// The upload of a sandbox's pinned artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum PinUpload {
    /// None to finish: no pin, or one with a durable copy.
    None,
    /// The drain's upload of this pin runs, or its upload job is queued or
    /// running.
    Uploading(String),
    /// Nothing uploads it: no job of its is queued or running, and no upload
    /// of the drain's runs for it. Whether the drain starts one, and when,
    /// is the stage's ([`Stage::windowed`]).
    Idle(String, ArtifactKind),
}

impl PinUpload {
    /// The pin without a durable copy.
    fn artifact(&self) -> Option<&str> {
        match self {
            Self::None => None,
            Self::Uploading(artifact) | Self::Idle(artifact, _) => Some(artifact),
        }
    }
}

/// Where a sandbox's evacuation is at one pass, apart from its pin's upload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Step {
    /// Nothing left to wait for: no VM, and no pin without a durable copy.
    Finished,
    /// Suspended, its pinned full snapshot durable.
    Evacuated(String),
    /// A started VM the drain keeps running while it holds.
    Held,
    /// A VM whose suspend waits for its window slot and disk room.
    Suspend { from: SuspendFrom, bytes: u64 },
    /// A suspended sandbox whose row pins no artifact, whose publication
    /// waits for its window slot.
    Publish,
    /// The drain's suspend of its VM runs.
    Suspending,
    /// The drain's publication of its snapshot runs.
    Publishing,
    /// An upload is what it waits for: its pin's, without a durable copy, or
    /// one of the drain's running for it, which the rest of what it needs
    /// (a VM's suspend among them) waits behind.
    Uploading,
    /// A VM whose suspend the node refuses, with why.
    Refused(String),
    /// Waiting on what the drain doesn't drive.
    External(Wait, Option<String>),
}

/// One sandbox's stage at one pass ([`Stage::of`]): the window, the disk
/// reservation, the action the pass starts for it and its reported wait all
/// come from it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Stage {
    pub(super) step: Step,
    pub(super) pin: PinUpload,
    /// The drain's action running for it: while one runs, nothing else
    /// starts for the sandbox.
    pub(super) running: Option<RunningAction>,
    /// Whether the drain finishes its other artifacts' uploads
    /// ([`Self::plans_other_uploads`]).
    pub(super) other_uploads: bool,
}

impl Stage {
    /// The stage of the sandbox `row` reads, from its row (its lifecycle,
    /// pin and suspend refusal), the drain's action `running` for it, its
    /// pin while that has no durable copy (`pending_pin`), whether that
    /// pin's upload job is queued or running, and whether the drain holds.
    ///
    /// A warm-pool member and a stopped, quarantined or failed-cleanup row
    /// are what the drain doesn't drive. For any other, a running suspend or
    /// publication is the stage whatever else the row reads, then a refused
    /// suspend, then the hold: a held sandbox is held
    /// whatever its pin's upload does, and that upload is driven meanwhile,
    /// as it is for a VM whose suspend is refused, since no suspend of the
    /// drain's replaces the pin. A VM the drain suspends doesn't upload its
    /// pin: the suspend supersedes it.
    pub(super) fn of(
        row: &DrainRow,
        running: Option<&RunningAction>,
        pending_pin: Option<(String, ArtifactKind)>,
        pin_job_active: bool,
        hold_running: bool,
    ) -> Self {
        let pin = match pending_pin {
            None => PinUpload::None,
            Some((artifact, _))
                if pin_job_active
                    || matches!(
                        running,
                        Some(RunningAction::Upload { artifact_id }) if *artifact_id == artifact
                    ) =>
            {
                PinUpload::Uploading(artifact)
            }
            Some((artifact, kind)) => PinUpload::Idle(artifact, kind),
        };
        Self {
            step: Self::step(row, running, &pin, hold_running),
            pin,
            running: running.cloned(),
            other_uploads: Self::plans_other_uploads(row),
        }
    }

    /// Whether the drain finishes the uploads of the sandbox's other
    /// artifacts without a durable copy, its named snapshots and the ones its
    /// pin moved on from: not a warm-pool member's or a stopped sandbox's,
    /// which it doesn't move, nor those of a row being deleted, which takes
    /// its artifacts with it.
    fn plans_other_uploads(row: &DrainRow) -> bool {
        !row.pool_member
            && !row.lifecycle.is_unowned(SandboxState::Stopped)
            && row.lifecycle.lifecycle() != SandboxState::Deleting
    }

    /// The stage of an action whose row the pass didn't read: it still
    /// holds what it took until it ends.
    fn rowless(running: &RunningAction) -> Self {
        Self {
            step: Step::Finished,
            pin: PinUpload::None,
            running: Some(running.clone()),
            other_uploads: false,
        }
    }

    fn step(
        row: &DrainRow,
        running: Option<&RunningAction>,
        pin: &PinUpload,
        hold_running: bool,
    ) -> Step {
        use SandboxState as S;
        use SandboxStatus::{Owned, Unowned};
        let cleanup_failed = || {
            Step::External(
                Wait::Settle(row.lifecycle),
                Some(CLEANUP_FAILED_REASON.to_owned()),
            )
        };
        if row.pool_member {
            // Not a resident: the warm-pool reconciler evicts slots on a
            // cordoned node, and until it has, the VM keeps the drain
            // pending. A failed cleanup waits as a resident's does.
            return if row.lifecycle.cleanup() == Some(CleanupState::Failed) {
                cleanup_failed()
            } else if may_hold_vm(row.lifecycle) {
                Step::External(Wait::WarmPoolEviction, None)
            } else {
                Step::Finished
            };
        }
        match row.lifecycle {
            // A v2 drain doesn't move a stopped sandbox, whatever it pins or
            // publishes, and starts nothing for it.
            Unowned(S::Stopped) => return Step::External(Wait::Stopped, None),
            Unowned(S::Quarantined) => {
                return Step::External(
                    Wait::Settle(row.lifecycle),
                    Some("nothing on the node moves it on".to_owned()),
                );
            }
            Owned {
                cleanup: CleanupState::Failed,
                ..
            } => return cleanup_failed(),
            _ => {}
        }
        match running {
            Some(RunningAction::Suspend { .. }) => return Step::Suspending,
            Some(RunningAction::PublishSnapshot) => return Step::Publishing,
            Some(RunningAction::Upload { .. }) | None => {}
        }
        let pin_pending = *pin != PinUpload::None;
        match row.lifecycle {
            Unowned(S::Started | S::Paused) => match &row.suspend_refusal {
                Some(refusal) => Step::Refused(refusal.clone()),
                // Held; once it leaves `started` on its own, the row is
                // planned from there.
                None if hold_running && row.lifecycle.is_unowned(S::Started) => Step::Held,
                // The suspend waits for the upload running for it.
                None if running.is_some() => Step::Uploading,
                None => Step::Suspend {
                    from: SuspendFrom::for_drain(hold_running),
                    bytes: row.snapshot_bytes,
                },
            },
            Unowned(S::Suspended) => match &row.pin {
                _ if pin_pending => Step::Uploading,
                Some((artifact, ArtifactKind::Full)) => Step::Evacuated(artifact.clone()),
                Some(_) => Step::External(
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
                None if running.is_some() => Step::Uploading,
                None => Step::Publish,
            },
            // Nothing on the node to keep once its pin is durable.
            Unowned(S::Failed) if pin_pending => Step::Uploading,
            Unowned(S::Failed) => Step::Finished,
            // A transition the drain doesn't drive: wait for it to settle,
            // whatever upload of the drain's runs for it.
            other => Step::External(Wait::Settle(other), None),
        }
    }

    /// Whether it holds one of the window's slots: from its suspend or
    /// publication's start until its pin's upload ends. One whose pin has no
    /// upload job queued or running holds none, so a capture a suspend
    /// rollback left pinned, its job given up, doesn't hold one for good.
    pub(super) fn holds_slot(&self) -> bool {
        matches!(
            self.running,
            Some(RunningAction::Suspend { .. } | RunningAction::PublishSnapshot)
        ) || matches!(self.pin, PinUpload::Uploading(_))
    }

    /// The cache disk its running suspend reserves.
    pub(super) fn reserved_bytes(&self) -> u64 {
        match self.running {
            Some(RunningAction::Suspend { reserved_bytes }) => reserved_bytes,
            _ => 0,
        }
    }

    /// The windowed action it waits to start, if any, and what the pass
    /// starts instead while the disk has no room for that suspend.
    fn windowed(&self, row: &DrainRow) -> Option<Windowed> {
        if self.running.is_some() {
            return None;
        }
        let id = &row.sandbox_id;
        let pin_upload = match &self.pin {
            PinUpload::Idle(artifact_id, kind) => Some(PlannedAction::Upload {
                sandbox_id: id.clone(),
                artifact_id: artifact_id.clone(),
                kind: *kind,
            }),
            PinUpload::None | PinUpload::Uploading(_) => None,
        };
        let windowed = |order, action, reported| Windowed {
            order,
            action,
            reported,
            holds_slot: self.holds_slot(),
            instead: None,
        };
        match &self.step {
            Step::Suspend { from, bytes } => Some(Windowed {
                // While holding, a suspend from `paused` may never come, so
                // a pin whose suspend waits for disk is uploaded meanwhile.
                instead: pin_upload.filter(|_| *from == SuspendFrom::Paused),
                ..windowed(
                    if row.lifecycle.is_unowned(SandboxState::Paused) {
                        WindowOrder::PausedVm
                    } else {
                        WindowOrder::StartedVm
                    },
                    PlannedAction::Suspend {
                        sandbox_id: id.clone(),
                        from: *from,
                        snapshot_bytes: *bytes,
                    },
                    true,
                )
            }),
            Step::Publish => Some(windowed(
                WindowOrder::Artifact,
                PlannedAction::PublishSnapshot {
                    sandbox_id: id.clone(),
                },
                true,
            )),
            Step::Uploading => {
                pin_upload.map(|action| windowed(WindowOrder::Artifact, action, true))
            }
            // Reported held or refused, whatever its upload does.
            Step::Held | Step::Refused(_) => {
                pin_upload.map(|action| windowed(WindowOrder::Artifact, action, false))
            }
            _ => None,
        }
    }

    /// What it waits for, unless the window decides that
    /// ([`Self::windowed`]).
    fn wait(&self, inputs: &PlanInputs, sandbox_id: &str) -> Option<(Wait, Option<String>)> {
        let work = |action| Some((Wait::Work(action), inputs.last_failure(sandbox_id, action)));
        match &self.step {
            Step::Finished | Step::Evacuated(_) | Step::Suspend { .. } | Step::Publish => None,
            Step::Uploading
                if self.running.is_none() && matches!(self.pin, PinUpload::Idle(..)) =>
            {
                None
            }
            Step::Uploading => work(DrainAction::Upload),
            Step::Suspending => work(DrainAction::Suspend),
            Step::Publishing => work(DrainAction::PublishSnapshot),
            // Held, or refused, whatever its pin's upload does, which the
            // reason still says: that it holds a window slot, and for a held
            // sandbox how its last attempt failed.
            Step::Held => Some((
                Wait::Hold,
                self.pin_notes(
                    self.pin
                        .artifact()
                        .and_then(|pin| inputs.last_pin_upload_failure(sandbox_id, pin)),
                ),
            )),
            Step::Refused(refusal) => Some((
                Wait::Refused,
                self.pin_notes(None)
                    .map(|notes| format!("{refusal}; {notes}"))
                    .or_else(|| Some(refusal.clone())),
            )),
            Step::External(wait, reason) => Some((*wait, reason.clone())),
        }
    }
}

impl Stage {
    /// What a held or refused sandbox's reason says of its pin's upload:
    /// that it holds a window slot while it runs, then `failure`.
    fn pin_notes(&self, failure: Option<String>) -> Option<String> {
        let slot = matches!(self.pin, PinUpload::Uploading(_)).then(|| PIN_SLOT_NOTE.to_owned());
        let notes: Vec<String> = slot.into_iter().chain(failure).collect();
        (!notes.is_empty()).then(|| notes.join("; "))
    }

    /// Every wait it is reported with but the one the window decides
    /// ([`Self::wait`]): one per other artifact still without a durable copy
    /// whose upload the drain finishes, the same set on every pass. While a
    /// suspend, publication or upload of the drain's runs for it and is its
    /// own reported wait, the others are folded into it, and a finished or
    /// evacuated sandbox waits for that running action once.
    fn other_waits(&self, inputs: &PlanInputs, row: &DrainRow) -> Vec<(Wait, Option<String>)> {
        let others = self.other_artifacts(inputs, row).count();
        if others == 0 {
            return Vec::new();
        }
        let id = &row.sandbox_id;
        match &self.running {
            Some(_)
                if matches!(
                    self.step,
                    Step::Suspending | Step::Publishing | Step::Uploading
                ) =>
            {
                Vec::new()
            }
            Some(running) if matches!(self.step, Step::Finished | Step::Evacuated(_)) => {
                let kind = running.kind();
                vec![(Wait::Work(kind), inputs.last_failure(id, kind))]
            }
            _ => vec![
                (
                    Wait::Work(DrainAction::Upload),
                    inputs.last_failure(id, DrainAction::Upload),
                );
                others
            ],
        }
    }

    /// The upload of another artifact the pass starts, when nothing of the
    /// drain's runs for the sandbox and `starting` doesn't already start
    /// something for it. An upload whose job is still queued or running is
    /// left to that job: enqueueing it again would only ask the control
    /// plane for its location once more.
    fn other_upload(
        &self,
        inputs: &PlanInputs,
        row: &DrainRow,
        starting: bool,
    ) -> Option<PlannedAction> {
        if self.running.is_some() || starting {
            return None;
        }
        self.other_artifacts(inputs, row)
            .find(|(artifact_id, _)| !inputs.active_uploads.contains(artifact_id))
            .map(|(artifact_id, kind)| PlannedAction::Upload {
                sandbox_id: row.sandbox_id.clone(),
                artifact_id: artifact_id.clone(),
                kind: *kind,
            })
    }

    /// The sandbox's artifacts without a durable copy but its pin, when the
    /// drain finishes their uploads.
    fn other_artifacts<'a>(
        &self,
        inputs: &'a PlanInputs,
        row: &'a DrainRow,
    ) -> impl Iterator<Item = &'a (String, ArtifactKind)> {
        let pin = row.pin.as_ref().map(|(artifact, _)| artifact);
        inputs
            .local_only
            .get(&row.sandbox_id)
            .filter(|_| self.other_uploads)
            .into_iter()
            .flatten()
            .filter(move |(artifact, _)| Some(artifact) != pin)
    }
}

/// What a held or refused sandbox's reason adds while its pin's upload
/// holds a window slot.
pub(super) const PIN_SLOT_NOTE: &str = "its pin's upload holds a window slot";

/// A sandbox's action that needs a window slot, and a suspend disk room too.
struct Windowed {
    order: WindowOrder,
    action: PlannedAction,
    /// Whether its wait is the sandbox's reported one.
    reported: bool,
    /// Whether the sandbox holds a slot already, its pin's upload's: it takes
    /// no second one, though it still starts only while the window has room,
    /// so no more than a window of sandboxes ever has windowed work of the
    /// drain's in flight (other artifacts' uploads take no slot).
    holds_slot: bool,
    /// What starts instead, in the no-VM tier, while the disk has no room for
    /// its suspend.
    instead: Option<PlannedAction>,
}

/// The order the window starts the drain's work in: VMs first, since each
/// holds memory and a VM slot, a paused VM, which serves no one, before a
/// started one; then the work that holds no VM, a suspended sandbox's
/// snapshot publication or a pin's upload retry. Within a pass every VM is
/// admitted before any of them; one already started holds its slot until its
/// upload job ends, failed attempts and backoff included, as any pin upload
/// does.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) enum WindowOrder {
    PausedVm,
    StartedVm,
    Artifact,
}

impl DrainPlan {
    /// Leave `sandbox_id`, a row this plan found finished, pending to be
    /// planned again, with why: it is no longer reported evacuated.
    pub(super) fn recheck(&mut self, sandbox_id: &str, why: &str) {
        self.evacuated.retain(|(id, _, _)| id != sandbox_id);
        self.wait(sandbox_id, Wait::Recheck, Some(why.to_owned()));
    }

    fn wait(&mut self, sandbox_id: &str, wait: Wait, reason: Option<String>) {
        self.pending.push(DrainWait {
            sandbox_id: sandbox_id.to_owned(),
            wait,
            reason,
        });
    }

    /// Start the windowed actions in `waiting` while the window has room,
    /// in [`WindowOrder`], and a suspend only while the cache disk still
    /// covers its snapshot beside what the running suspends reserve.
    /// `in_flight` is how many sandboxes hold a slot and `committed` the
    /// bytes reserved. The rest wait, queued for the window or for the disk; what starts
    /// instead of a suspend waiting for disk waits for the no-VM tier.
    fn start_in_window(
        &mut self,
        inputs: &PlanInputs,
        waiting: Vec<Windowed>,
        mut in_flight: usize,
        mut committed: u64,
    ) {
        let order = |windowed: &Windowed| (windowed.order, windowed.action.target().0.to_owned());
        let (mut vms, mut no_vm): (Vec<_>, Vec<_>) = waiting
            .into_iter()
            .partition(|windowed| windowed.order != WindowOrder::Artifact);
        vms.sort_by_key(order);
        for windowed in vms {
            // What starts instead of a suspend waiting for disk holds no VM:
            // it waits its turn after every VM.
            no_vm.extend(self.admit(inputs, windowed, &mut in_flight, &mut committed));
        }
        no_vm.sort_by_key(order);
        for windowed in no_vm {
            self.admit(inputs, windowed, &mut in_flight, &mut committed);
        }
    }

    /// Start `windowed` if the window has room, and a suspend only while the
    /// cache disk covers it beside `committed`, or leave it waiting. Answers
    /// what starts instead of a suspend waiting for disk, in the no-VM tier.
    fn admit(
        &mut self,
        inputs: &PlanInputs,
        windowed: Windowed,
        in_flight: &mut usize,
        committed: &mut u64,
    ) -> Option<Windowed> {
        let (id, kind) = windowed.action.target();
        let id = id.to_owned();
        if *in_flight >= inputs.window {
            if windowed.reported {
                self.wait(&id, Wait::Queued(kind), inputs.last_failure(&id, kind));
            }
            return None;
        }
        if let PlannedAction::Suspend { snapshot_bytes, .. } = &windowed.action {
            let needed = committed.saturating_add(*snapshot_bytes);
            if inputs
                .disk_headroom
                .is_some_and(|headroom| needed > headroom)
            {
                self.wait(
                    &id,
                    Wait::Disk,
                    Some(format!(
                        "the cache disk has no room yet for a {} MiB snapshot",
                        snapshot_bytes / (1024 * 1024)
                    )),
                );
                return windowed.instead.map(|action| Windowed {
                    order: WindowOrder::Artifact,
                    action,
                    reported: false,
                    holds_slot: windowed.holds_slot,
                    instead: None,
                });
            }
            *committed = needed;
        }
        if !windowed.holds_slot {
            *in_flight += 1;
        }
        if windowed.reported {
            self.wait(&id, Wait::Work(kind), inputs.last_failure(&id, kind));
        }
        self.actions.push(windowed.action);
        None
    }
}

impl PlannedAction {
    pub(super) fn target(&self) -> (&str, DrainAction) {
        match self {
            Self::Suspend { sandbox_id, .. } => (sandbox_id, DrainAction::Suspend),
            Self::PublishSnapshot { sandbox_id } => (sandbox_id, DrainAction::PublishSnapshot),
            Self::Upload { sandbox_id, .. } => (sandbox_id, DrainAction::Upload),
        }
    }

    /// The action as it runs, with what it holds.
    pub(super) fn running(&self) -> RunningAction {
        match self {
            Self::Suspend { snapshot_bytes, .. } => RunningAction::Suspend {
                reserved_bytes: *snapshot_bytes,
            },
            Self::PublishSnapshot { .. } => RunningAction::PublishSnapshot,
            Self::Upload { artifact_id, .. } => RunningAction::Upload {
                artifact_id: artifact_id.clone(),
            },
        }
    }
}

/// Decide what the drain does for every row, from each sandbox's
/// [`Stage`]. What it reports comes from the rows alone: a suspended row
/// whose pinned full snapshot is durable is evacuated whatever suspended it.
/// Every reason the drain isn't finished is an entry: each row that may hold
/// a VM, each stopped row, which the drain doesn't move, and each artifact
/// still without a durable copy, is pending, and a node-wide problem is
/// blocked, so the drain is finished exactly when the plan has neither.
///
/// It starts at most one action per sandbox: the suspend of its VM or the
/// publication of its snapshot first, within the window, then its pin's
/// upload, also within the window, then the upload of another of its
/// artifacts. Those other uploads, of named snapshots or of artifacts its
/// pin moved on from, take no slot.
pub(super) fn plan(rows: &[DrainRow], inputs: &PlanInputs) -> DrainPlan {
    let mut plan = DrainPlan {
        started: rows
            .iter()
            .filter(|row| row.lifecycle.is_unowned(SandboxState::Started))
            .map(|row| row.sandbox_id.clone())
            .collect(),
        ..DrainPlan::default()
    };
    let read: BTreeSet<&str> = rows.iter().map(|row| row.sandbox_id.as_str()).collect();
    for (sandbox_id, artifacts) in &inputs.local_only {
        if !read.contains(sandbox_id.as_str()) {
            for (artifact, _) in artifacts {
                plan.blocked.push(format!(
                    "local-only artifact {artifact} of sandbox {sandbox_id} has no sandbox row"
                ));
            }
        }
    }
    let stages: Vec<(&DrainRow, Stage)> = rows.iter().map(|row| (row, inputs.stage(row))).collect();
    let rowless: Vec<Stage> = inputs
        .running
        .iter()
        .filter(|(sandbox_id, _)| !read.contains(sandbox_id.as_str()))
        .map(|(_, running)| Stage::rowless(running))
        .collect();
    let held = || stages.iter().map(|(_, stage)| stage).chain(&rowless);
    let in_flight = held().filter(|stage| stage.holds_slot()).count();
    let committed = held().map(Stage::reserved_bytes).sum();
    let mut windowed = Vec::new();
    for (row, stage) in &stages {
        let id = &row.sandbox_id;
        if let Step::Evacuated(artifact) = &stage.step {
            plan.evacuated
                .push((id.clone(), ArtifactKind::Full, artifact.clone()));
        }
        for (wait, reason) in stage
            .wait(inputs, id)
            .into_iter()
            .chain(stage.other_waits(inputs, row))
        {
            plan.wait(id, wait, reason);
        }
        windowed.extend(stage.windowed(row));
    }
    plan.start_in_window(inputs, windowed, in_flight, committed);
    let starting: BTreeSet<String> = plan
        .actions
        .iter()
        .map(|action| action.target().0.to_owned())
        .collect();
    for (row, stage) in &stages {
        plan.actions
            .extend(stage.other_upload(inputs, row, starting.contains(&row.sandbox_id)));
    }
    plan
}

/// What a drain call scans outside the rows: node-manager's orphan
/// inventory.
#[derive(Clone, Debug, Default)]
pub(super) struct DrainScans {
    /// Node-wide problems the scans found.
    pub(super) blocked: Vec<String>,
    /// node-manager's sandbox directory candidates, by the sandbox they're
    /// named for. node-manager knows only its slots: a directory whose
    /// sandbox has a row here is the row's, and the plan accounts for it.
    pub(super) orphan_dirs: Vec<(String, String)>,
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
