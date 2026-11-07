//! `DrainNode`: the node's own half of a forced drain.
//!
//! The control plane names an epoch; the node seals itself under it (see
//! [`crate::activation_seal`]) and, on every call, makes each of its
//! residents durable elsewhere and reports what it has done. It acts only
//! through the paths the node already owns: the suspend of a started or paused
//! VM is the idle auto-suspend's (a full snapshot published and uploaded under
//! node-local durability), a stopped sandbox's disk is published through
//! `EnqueueArtifactBackup`, and a local-only artifact's upload is finished the
//! same way. The control plane learns of each artifact as it always does,
//! through the backup it registers; the drain only reports the artifact, the
//! row's durable pin, once it is durable, with the lifecycle the sandbox had.
//! A drain suspend records that lifecycle (`drain_evacuations`) under the
//! sandbox's wake lock, while the node is still sealed under the drain's
//! epoch, so it survives a restart; a stopped sandbox's row keeps its own.
//!
//! A call never waits for that work. It plans from the rows as they are,
//! starts what is not already running, and answers. [`DrainPlan`] is that
//! decision, a pure function of what the call read, and `done` is true only
//! when nothing is left: no VM, nothing pending or blocked, every scan that
//! can reveal a resident or an artifact succeeded, and the seal held, under
//! the call's epoch, from before the walk to its end, with no activation in
//! flight at its first read.

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

/// How many times in a row the drain's work on a sandbox may fail before
/// the drain reports it `blocked` with the last failure. It still retries
/// on every call.
pub(super) const MAX_ACTION_FAILURES: u32 = 3;
/// How long a row may sit in a transition the drain doesn't drive before the
/// drain reports it `blocked`: a start the seal refused leaves a row
/// `created`, and a failed restore's row stays `restoring`, with nothing
/// moving them on.
pub(super) const SETTLE_BOUND: Duration = Duration::from_secs(15 * 60);

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

/// The drain's work in flight in this process, by sandbox, and what it
/// remembers between calls.
#[derive(Default)]
pub(super) struct DrainActions {
    running: Mutex<BTreeMap<String, DrainAction>>,
    /// The consecutive failures of the drain's work on each sandbox, with the
    /// last one's reason.
    failures: Mutex<BTreeMap<String, (u32, String)>>,
    /// Each row's lifecycle as the drain last saw it, and since when.
    seen: Mutex<BTreeMap<String, (SandboxState, tokio::time::Instant)>>,
}

impl DrainActions {
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

    /// End `sandbox_id`'s running action, counting `failure` if it failed.
    fn finish(&self, sandbox_id: &str, failure: Option<String>) {
        if let Some(reason) = failure {
            let mut failures = lock(&self.failures);
            let entry = failures
                .entry(sandbox_id.to_owned())
                .or_insert((0, String::new()));
            entry.0 = entry.0.saturating_add(1);
            entry.1 = reason;
        }
        lock(&self.running).remove(sandbox_id);
    }

    fn failures(&self) -> BTreeMap<String, (u32, String)> {
        lock(&self.failures).clone()
    }

    /// Forget the failures of every sandbox the drain no longer works on.
    fn keep_failures_of(&self, working: &BTreeSet<String>) {
        lock(&self.failures).retain(|sandbox_id, _| working.contains(sandbox_id));
    }

    /// Note each row's lifecycle at `now`, and answer the rows that have
    /// been in the same one for [`SETTLE_BOUND`] or longer.
    fn unchanged_since_bound(
        &self,
        rows: &[(String, SandboxState)],
        now: tokio::time::Instant,
    ) -> BTreeSet<String> {
        let mut seen = lock(&self.seen);
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
    pub(super) vm: bool,
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
#[derive(Clone, Debug, Default)]
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
    /// The consecutive failures of the drain's work, by sandbox.
    pub(super) failures: BTreeMap<String, (u32, String)>,
    /// The rows in the same lifecycle for [`SETTLE_BOUND`] or longer.
    pub(super) unchanged: BTreeSet<String>,
}

impl DrainPlan {
    /// Wait for `action` on `sandbox_id`, or report it `blocked` once it has
    /// failed [`MAX_ACTION_FAILURES`] times in a row.
    fn wait_for(&mut self, inputs: &PlanInputs, sandbox_id: &str, action: &str) {
        match inputs.failures.get(sandbox_id) {
            Some((failures, reason)) if *failures >= MAX_ACTION_FAILURES => {
                self.blocked.push((
                    Some(sandbox_id.to_owned()),
                    format!("{action} failed {failures} times in a row: {reason}"),
                ));
            }
            _ => self
                .pending
                .push((sandbox_id.to_owned(), action.to_owned())),
        }
    }

    /// Start `action` unless something is already running for its sandbox,
    /// and wait for whichever it is.
    fn drive(&mut self, inputs: &PlanInputs, action: PlannedAction) {
        let (sandbox_id, kind) = action.target();
        let sandbox_id = sandbox_id.to_owned();
        match inputs.running.get(&sandbox_id) {
            Some(running) => self.wait_for(inputs, &sandbox_id, running.as_str()),
            None => {
                self.wait_for(inputs, &sandbox_id, kind.as_str());
                self.actions.push(action);
            }
        }
    }
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
            // cordoned node, and until it has, the VM keeps `done` false.
            plan.vm |= may_hold_vm(row.lifecycle);
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
                plan.vm = true;
                plan.blocked.push((Some(id.clone()), "quarantined".into()));
            }
            SandboxState::CleanupFailed => {
                plan.blocked
                    .push((Some(id.clone()), "cleanup failed".into()));
            }
            SandboxState::Started | SandboxState::Paused => {
                plan.vm = true;
                match &row.suspend_refusal {
                    Some(refusal) if !running => {
                        plan.blocked.push((Some(id.clone()), refusal.clone()));
                    }
                    _ => plan.drive(
                        inputs,
                        PlannedAction::Suspend {
                            sandbox_id: id.clone(),
                        },
                    ),
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
                _ => plan.drive(
                    inputs,
                    PlannedAction::PublishDisk {
                        sandbox_id: id.clone(),
                    },
                ),
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
                plan.vm |= may_hold_vm(other);
                if let Some(action) = inputs.running.get(id) {
                    plan.wait_for(inputs, id, action.as_str());
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

/// A scanner of node-manager's orphan inventory, as its failure warnings
/// name it (`<scanner>: <error>`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OrphanScanner {
    Namespaces,
    Cgroups,
    RunDirs,
    GatewayRunDirs,
    CacheDirs,
    Processes,
    Nft,
}

impl OrphanScanner {
    const ALL: [Self; 7] = [
        Self::Namespaces,
        Self::Cgroups,
        Self::RunDirs,
        Self::GatewayRunDirs,
        Self::CacheDirs,
        Self::Processes,
        Self::Nft,
    ];

    const fn as_str(self) -> &'static str {
        match self {
            Self::Namespaces => "Namespaces",
            Self::Cgroups => "Cgroups",
            Self::RunDirs => "RunDirs",
            Self::GatewayRunDirs => "GatewayRunDirs",
            Self::CacheDirs => "CacheDirs",
            Self::Processes => "Processes",
            Self::Nft => "Nft",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|scanner| scanner.as_str() == value)
    }

    /// Whether what this scanner finds can be a resident or an artifact: an
    /// untracked runtime process, or a sandbox cache or run directory. The
    /// others find network namespaces, cgroups, nft rules and gateway run
    /// directories, which hold neither.
    const fn can_hide_a_resident(self) -> bool {
        match self {
            Self::RunDirs | Self::CacheDirs | Self::Processes => true,
            Self::Namespaces | Self::Cgroups | Self::GatewayRunDirs | Self::Nft => false,
        }
    }
}

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
            .and_then(|(scanner, _)| OrphanScanner::parse(scanner))
            .is_some_and(|scanner| !scanner.can_hide_a_resident());
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
        let (mut plan, rows) = match self.read_drain_plan() {
            Ok(read) => read,
            Err(reason) => (
                DrainPlan {
                    blocked: vec![(None, reason)],
                    vm: true,
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
        let done = continuous && !plan.vm && plan.pending.is_empty() && plan.blocked.is_empty();
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
                let page = client
                    .list_orphan_candidates(
                        pplx_space_node_mgr_client::proto::ListOrphanCandidatesRequest {
                            page_token: page_token.clone(),
                        },
                    )
                    .await
                    .map_err(|status| format!("orphan inventory failed: {status}"))?
                    .into_inner();
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
    fn read_drain_plan(&self) -> Result<(DrainPlan, BTreeSet<String>), String> {
        let (sandboxes, local_only_rows, evacuations) = {
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
            (sandboxes, local_only, evacuations)
        };
        let mut local_only = LocalOnlyArtifacts::new();
        for artifact in local_only_rows {
            local_only.entry(artifact.sandbox_id).or_default().push((
                artifact.local_artifact_id.to_string(),
                artifact.artifact_kind,
            ));
        }
        let mut pending_backups: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for pending in self
            .local_artifacts()
            .list_pending_backups()
            .map_err(|error| format!("pending backup scan failed: {error}"))?
        {
            pending_backups
                .entry(pending.sandbox_id)
                .or_default()
                .insert(pending.local_artifact_id.to_string());
        }
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
        let inputs = PlanInputs {
            pending_backups,
            local_only,
            evacuations,
            running: self.drain_actions.running(),
            failures: self.drain_actions.failures(),
            unchanged: self
                .drain_actions
                .unchanged_since_bound(&lifecycles, tokio::time::Instant::now()),
        };
        let plan = plan(&rows, &inputs);
        // A failure counts toward the bound only while the drain still works
        // on its sandbox; one that succeeded, or moved on, starts over.
        let working = plan
            .actions
            .iter()
            .map(|action| action.target().0.to_owned())
            .chain(inputs.running.keys().cloned())
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
                    &task_sandbox_id,
                    result.err().map(|status| status.message().to_owned()),
                );
            });
            if spawned.is_err() {
                // Draining for a restart: the next process's call starts it.
                self.drain_actions.finish(&sandbox_id, None);
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
        if !self
            .activation_seal
            .record_drain_suspend(&self.db, epoch, &sandbox.id, prior)?
        {
            return Ok(());
        }
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
        if suspended.is_err()
            && !self
                .lookup_sandbox(sandbox_id)
                .is_ok_and(|row| row.state == SandboxState::Suspended)
        {
            // The suspend didn't land, so the record would name the lifecycle
            // of a suspend that never happened if a user's own suspend came
            // next. The next call records it again.
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
