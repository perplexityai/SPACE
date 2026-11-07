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

/// How long one page of node-manager's orphan inventory may take.
const ORPHAN_PAGE_TIMEOUT: Duration = Duration::from_secs(10);
/// The most inventory pages one call reads before it reports the scan as
/// incomplete.
const MAX_ORPHAN_PAGES: usize = 64;

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

/// The drain's work in flight in this process, by sandbox.
#[derive(Default)]
pub(super) struct DrainActions {
    running: Mutex<BTreeMap<String, DrainAction>>,
    /// Artifacts this process already asked to upload; the backup worker
    /// retries them on its own.
    enqueued_uploads: Mutex<BTreeSet<String>>,
}

impl DrainActions {
    fn running(&self) -> BTreeMap<String, DrainAction> {
        self.running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Mark `action` running for `sandbox_id`; false when something already
    /// is.
    fn start(&self, sandbox_id: &str, action: DrainAction) -> bool {
        let mut running = self
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if running.contains_key(sandbox_id) {
            return false;
        }
        running.insert(sandbox_id.to_owned(), action);
        true
    }

    fn finish(&self, sandbox_id: &str) {
        self.running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(sandbox_id);
    }

    fn first_upload(&self, artifact_id: &str) -> bool {
        self.enqueued_uploads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(artifact_id.to_owned())
    }
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

/// Decide what the drain does for every row. `pending_backups` maps a
/// sandbox to the pinned artifacts still without a durable copy;
/// `evacuations` are the recorded drain suspends by sandbox; `running` is
/// the drain's work in flight in this process.
pub(super) fn plan(
    rows: &[DrainRow],
    pending_backups: &BTreeMap<String, BTreeSet<String>>,
    evacuations: &BTreeMap<String, DrainEvacuation>,
    running: &BTreeMap<String, DrainAction>,
) -> DrainPlan {
    let mut plan = DrainPlan::default();
    let mut seen = BTreeSet::new();
    for row in rows {
        seen.insert(row.sandbox_id.clone());
        let id = &row.sandbox_id;
        let evacuation = evacuations.get(id);
        let pin_pending = row.pin.as_ref().is_some_and(|(artifact, _)| {
            pending_backups
                .get(id)
                .is_some_and(|pending| pending.contains(artifact))
        });
        let durable_pin = row
            .pin
            .as_ref()
            .filter(|_| !pin_pending)
            .map(|(artifact, kind)| (artifact.clone(), *kind));
        if row.pool_member {
            // Not a resident: the warm-pool reconciler evicts slots on a
            // cordoned node, and until it has, the VM keeps `done` false.
            plan.vm |= may_hold_vm(row.lifecycle);
            continue;
        }
        match row.lifecycle {
            SandboxState::Deleting => {
                plan.vm = true;
                plan.pending.push((id.clone(), "settle:deleting".into()));
            }
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
                if let Some(action) = running.get(id) {
                    plan.pending.push((id.clone(), action.as_str().into()));
                } else if let Some(refusal) = &row.suspend_refusal {
                    plan.blocked.push((Some(id.clone()), refusal.clone()));
                } else {
                    plan.pending
                        .push((id.clone(), DrainAction::Suspend.as_str().into()));
                    plan.actions.push(PlannedAction::Suspend {
                        sandbox_id: id.clone(),
                    });
                }
            }
            SandboxState::Stopped => {
                if let Some(action) = running.get(id) {
                    plan.pending.push((id.clone(), action.as_str().into()));
                } else if row.volume_encryption {
                    plan.blocked.push((
                        Some(id.clone()),
                        "an encrypted stopped sandbox's disk can't be published".into(),
                    ));
                } else if let Some((artifact, pplx_space_common_core::types::ArtifactKind::Disk)) =
                    &durable_pin
                {
                    plan.evacuated
                        .push((id.clone(), DrainPrior::Stopped, artifact.clone()));
                } else if let Some((artifact, kind)) = row.pin.as_ref().filter(|_| pin_pending)
                    && *kind == pplx_space_common_core::types::ArtifactKind::Disk
                {
                    plan.pending
                        .push((id.clone(), DrainAction::Upload.as_str().into()));
                    plan.actions.push(PlannedAction::Upload {
                        sandbox_id: id.clone(),
                        artifact_id: artifact.clone(),
                        kind: *kind,
                    });
                } else {
                    plan.pending
                        .push((id.clone(), DrainAction::PublishDisk.as_str().into()));
                    plan.actions.push(PlannedAction::PublishDisk {
                        sandbox_id: id.clone(),
                    });
                }
            }
            SandboxState::Suspended | SandboxState::Failed => {
                if let Some((artifact, kind)) = row.pin.as_ref().filter(|_| pin_pending) {
                    plan.pending
                        .push((id.clone(), DrainAction::Upload.as_str().into()));
                    plan.actions.push(PlannedAction::Upload {
                        sandbox_id: id.clone(),
                        artifact_id: artifact.clone(),
                        kind: *kind,
                    });
                } else if let Some(evacuation) = evacuation {
                    // The drain suspended it; its artifact is durable now.
                    match &durable_pin {
                        Some((artifact, pplx_space_common_core::types::ArtifactKind::Full))
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
                // A transition the drain doesn't drive: wait for it to settle.
                plan.vm |= may_hold_vm(other);
                let action = running.get(id).map_or_else(
                    || format!("settle:{other}"),
                    |action| action.as_str().into(),
                );
                plan.pending.push((id.clone(), action));
            }
        }
    }
    // A pending backup whose sandbox has no row is still a local-only
    // artifact.
    for (sandbox_id, artifacts) in pending_backups {
        if !seen.contains(sandbox_id) && !artifacts.is_empty() {
            plan.pending
                .push((sandbox_id.clone(), DrainAction::Upload.as_str().into()));
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

/// The inventory's scanners that can reveal a resident or an artifact: an
/// untracked runtime process, or a cache or run directory no row accounts
/// for. Their candidates and failures block the drain; every
/// other scanner's leftovers hold neither, and are ignored.
const BLOCKING_SCANNERS: [&str; 3] = ["RunDirs", "CacheDirs", "Processes"];

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
        let scanner = warning.split_once(':').map_or("", |(scanner, _)| scanner);
        if BLOCKING_SCANNERS.contains(&scanner) {
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

    /// Read node-manager's whole orphan inventory, one page at a time.
    async fn read_orphan_inventory(&self) -> OrphanFindings {
        let mut findings = OrphanFindings::default();
        let gateway_run_root = pplx_space_common_core::host::linux::paths::NET_GATEWAY_RUN_DIR;
        let mut client = match self.node_manager.connect().await {
            Ok(client) => client,
            Err(error) => {
                findings.blocked.push(format!(
                    "can't reach node-manager's orphan inventory: {error}"
                ));
                return findings;
            }
        };
        let mut page_token = String::new();
        for _ in 0..MAX_ORPHAN_PAGES {
            let page = tokio::time::timeout(
                ORPHAN_PAGE_TIMEOUT,
                client.list_orphan_candidates(
                    pplx_space_node_mgr_client::proto::ListOrphanCandidatesRequest {
                        page_token: page_token.clone(),
                    },
                ),
            )
            .await;
            let page = match page {
                Ok(Ok(page)) => page.into_inner(),
                Ok(Err(status)) => {
                    findings
                        .blocked
                        .push(format!("orphan inventory failed: {status}"));
                    return findings;
                }
                Err(_) => {
                    findings
                        .blocked
                        .push("orphan inventory timed out".to_owned());
                    return findings;
                }
            };
            classify_orphan_page(&page, gateway_run_root, &mut findings);
            if page.next_page_token.is_empty() {
                return findings;
            }
            page_token = page.next_page_token;
        }
        findings.blocked.push(format!(
            "orphan inventory ran past {MAX_ORPHAN_PAGES} pages"
        ));
        findings
    }

    /// Read every row, the pending backups and the evacuation record, and
    /// plan, with the ids of the rows read. An error is the scan that failed,
    /// which blocks the drain.
    fn read_drain_plan(&self) -> Result<(DrainPlan, BTreeSet<String>), String> {
        let (sandboxes, evacuations) = {
            let conn = self
                .db_conn()
                .map_err(|status| status.message().to_owned())?;
            let sandboxes = crate::db::scan::all_sandboxes(&conn, crate::db::scan::StrictRows)
                .map_err(|error| format!("sandbox scan failed: {error}"))?;
            let evacuations = crate::db::read::drain_evacuations(&conn)
                .map_err(|error| format!("evacuation record unreadable: {error}"))?;
            (sandboxes, evacuations)
        };
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
        Ok((
            plan(
                &rows,
                &pending_backups,
                &evacuations,
                &self.drain_actions.running(),
            ),
            ids,
        ))
    }

    /// Why the Suspend RPC would refuse to suspend `row`, as the drain
    /// reports it.
    fn drain_suspend_refusal(&self, row: &crate::db::SandboxRow) -> Option<String> {
        self.ensure_memory_state_supported(row, "suspend")
            .and_then(|()| self.ensure_not_pool_sandbox_for_snapshots(row, "suspend"))
            .and_then(|()| self.ensure_no_plaintext_pool_cow_for_snapshots(row, "suspend"))
            .and_then(|()| self.ensure_snapshot_restore_supported("suspend"))
            .err()
            .map(|status| format!("can't be made durable: {}", status.message()))
    }

    /// Start what the plan asks for that isn't already running.
    fn start_actions(&self, epoch: u64, plan: &mut DrainPlan) {
        for action in std::mem::take(&mut plan.actions) {
            let (sandbox_id, kind) = match &action {
                PlannedAction::Suspend { sandbox_id } => (sandbox_id.clone(), DrainAction::Suspend),
                PlannedAction::PublishDisk { sandbox_id } => {
                    (sandbox_id.clone(), DrainAction::PublishDisk)
                }
                PlannedAction::Upload {
                    sandbox_id,
                    artifact_id,
                    ..
                } => {
                    if !self.drain_actions.first_upload(artifact_id) {
                        continue;
                    }
                    (sandbox_id.clone(), DrainAction::Upload)
                }
            };
            if !self.drain_actions.start(&sandbox_id, kind) {
                continue;
            }
            let service = self.clone();
            self.task_tracker.spawn(async move {
                let result = service.run_drain_action(epoch, action).await;
                service.drain_actions.finish(&sandbox_id);
                if let Err(status) = result {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        action = kind.as_str(),
                        error = %status,
                        "node drain action failed; the next drain call plans it again"
                    );
                }
            });
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
            } => self
                .enqueue_artifact_backup_authorized(EnqueueArtifactBackupRequest {
                    sandbox_id,
                    expected_artifact_kind: crate::proto::ArtifactKind::from(kind) as i32,
                    source: Some(
                        crate::proto::enqueue_artifact_backup_request::Source::LocalArtifactId(
                            artifact_id,
                        ),
                    ),
                })
                .await
                .map(|_| ()),
        }
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
        let _state_writer = self.state_writer_lease("NodeDrain")?;
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
