//! The `DrainNode` handler: seal or cancel, read, plan, start and report.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use super::planner::*;
use crate::activation_seal::ActivationSealStatus;
use crate::db::activation_seal::DrainPrior;
use crate::proto::{
    DrainBlocked, DrainEvacuated, DrainNodeRequest, DrainNodeResponse, DrainPending,
    PreviewDrainNodeRequest, PreviewDrainNodeResponse,
};
use crate::service::*;

/// How long one call may spend reading node-manager's whole orphan
/// inventory, so a slow node-manager is reported `blocked` after a bounded
/// wait rather than holding the call. The drain's work goes on between
/// calls.
pub(super) const ORPHAN_INVENTORY_DEADLINE: Duration = Duration::from_secs(20);

/// How long a call waits before asking a busy orphan inventory again.
const ORPHAN_INVENTORY_BUSY_RETRY: Duration = Duration::from_millis(200);

/// The drain call's scan a preview doesn't run, as `unchecked` names it:
/// node-manager's orphan inventory, which every caller on the node shares.
pub(super) const ORPHAN_INVENTORY_SCAN: &str = "orphan_inventory";

impl SandboxManagerService {
    pub(in crate::service) async fn drain_node(
        &self,
        request: Request<DrainNodeRequest>,
    ) -> Result<Response<DrainNodeResponse>, Status> {
        self.authorize_peer(&request, "DrainNode", ALLOW_NODE_DRAIN)?;
        let DrainNodeRequest {
            epoch,
            cancel,
            forget,
            gentle_epoch,
        } = request.into_inner();
        let (epoch, gentle) = drain_epoch(epoch, cancel, gentle_epoch)?;
        if cancel {
            self.activation_seal.unseal(&self.db, epoch)?;
            return Ok(Response::new(self.finish_cancel(epoch, &forget)));
        }
        self.activation_seal.seal(&self.db, epoch)?;
        let first = self.activation_seal.status();
        let scans = self.read_drain_scans().await;
        Ok(Response::new(
            self.finish_drain(epoch, gentle, &first, scans),
        ))
    }

    pub(in crate::service) async fn preview_drain_node(
        &self,
        request: Request<PreviewDrainNodeRequest>,
    ) -> Result<Response<PreviewDrainNodeResponse>, Status> {
        // It changes nothing, but answers what a drain would find on the
        // whole node, so it takes the drain's authority.
        self.authorize_peer(&request, "PreviewDrainNode", ALLOW_NODE_DRAIN)?;
        let PreviewDrainNodeRequest {} = request.into_inner();
        if let Some(error) = self.activation_seal.read_error() {
            return Err(Status::internal(format!(
                "activation seal unreadable: {error}"
            )));
        }
        // Tagged, so what the drain's scans log during a preview says it came
        // from one.
        let span = tracing::info_span!("drain_node_preview", preview = true);
        // Not node-manager's orphan inventory: its scans are shared by every
        // caller on the node and few, so a preview's could evict a drain
        // call's or node-ctl's. The answer names it unchecked.
        let scans = DrainScans {
            disk_headroom: read_disk_headroom().instrument(span.clone()).await,
            ..DrainScans::default()
        };
        Ok(Response::new(span.in_scope(|| self.finish_preview(scans))))
    }

    /// What a drain call scans outside the rows: node-manager's orphan
    /// inventory and the cache disk's headroom. Both only read.
    async fn read_drain_scans(&self) -> DrainScans {
        let orphans = self.read_orphan_inventory().await;
        DrainScans {
            blocked: orphans
                .blocked
                .into_iter()
                .map(|reason| (None, reason))
                .collect(),
            orphan_dirs: orphans.dirs,
            disk_headroom: read_disk_headroom().await,
        }
    }

    /// Plan from the rows, record and start what the plan asks for, and
    /// report. `gentle` holds the started rows. `first` is the seal as the
    /// call read it right after sealing, before its scans; `scans` are what
    /// the scans that ran outside the rows found.
    pub(super) fn finish_drain(
        &self,
        epoch: u64,
        gentle: bool,
        first: &ActivationSealStatus,
        scans: DrainScans,
    ) -> DrainNodeResponse {
        let (mut plan, rows) = self
            .read_drain_plan(epoch, gentle, scans.disk_headroom)
            .unwrap_or_else(read_failed);
        self.start_actions(epoch, &mut plan);
        fold_scans(&mut plan, &rows, scans, first);

        let last = self.activation_seal.status();
        // A cancelled epoch is never sealed again, so the same epoch at both
        // reads is one continuous seal.
        let continuous = first.sealed_epoch == Some(epoch) && last.sealed_epoch == Some(epoch);
        // Every row that may hold a VM is pending or blocked, so a node with
        // nothing pending or blocked holds none.
        let done = continuous && plan.pending.is_empty() && plan.blocked.is_empty();
        DrainNodeResponse {
            gentle_epoch: gentle.then_some(epoch),
            ..report(first, plan, done)
        }
    }

    /// What a forced v2 drain would find on this node now, changing nothing: the
    /// plan [`Self::read_preview_plan`] makes, with what a drain would start
    /// reported `pending` under its action's name and none of it started,
    /// and the node's seal as it stands. `would_finish` when nothing is
    /// pending or blocked: a drain would finish at once, unless the orphan
    /// inventory, which a preview names unchecked, blocks it. A seal isn't
    /// needed for that, since a preview takes none.
    pub(super) fn finish_preview(&self, scans: DrainScans) -> PreviewDrainNodeResponse {
        let seal = self.activation_seal.status();
        let (mut plan, rows) = self
            .read_preview_plan(seal.sealed_epoch, scans.disk_headroom)
            .unwrap_or_else(read_failed);
        fold_scans(&mut plan, &rows, scans, &seal);
        let would_finish = plan.pending.is_empty() && plan.blocked.is_empty();
        let DrainNodeResponse {
            sealed_epoch,
            evacuated,
            pending,
            blocked,
            done: _,
            gentle_epoch: _,
        } = report(&seal, plan, would_finish);
        PreviewDrainNodeResponse {
            sealed_epoch,
            evacuated,
            pending,
            blocked,
            would_finish,
            unchecked: vec![ORPHAN_INVENTORY_SCAN.to_owned()],
        }
    }

    /// Under a cancelled `epoch`: forget the records with nothing left to
    /// hand back and the ones the control plane names in `named` that
    /// nothing is in flight for, while `epoch` is still the node's cancelled
    /// epoch, start the uploads the hand-back waits for, and report. `done`
    /// once nothing is left to hand back.
    pub(super) fn finish_cancel(&self, epoch: u64, named: &[String]) -> DrainNodeResponse {
        let mut plan = match self.read_plan_inputs(epoch, None, tokio::time::Instant::now()) {
            Ok((rows, inputs)) => {
                let (plan, stale) = plan_cancel(&rows, &inputs, named);
                match self
                    .activation_seal
                    .forget_cancelled_drain_suspends(&self.db, epoch, &stale)
                {
                    Ok(true) => plan,
                    // A later drain or cancel took over after this call's
                    // cancel; the records are its own.
                    Ok(false) => DrainPlan {
                        blocked: vec![(
                            None,
                            format!(
                                "epoch {epoch} is no longer the node's current drain epoch; \
                                 this cancel forgot no record"
                            ),
                        )],
                        ..plan
                    },
                    Err(status) => DrainPlan {
                        blocked: vec![(None, status.message().to_owned())],
                        ..plan
                    },
                }
            }
            Err(reason) => DrainPlan {
                blocked: vec![(None, reason)],
                ..DrainPlan::default()
            },
        };
        self.start_actions(epoch, &mut plan);
        let done = plan.evacuated.is_empty() && plan.pending.is_empty() && plan.blocked.is_empty();
        report(&self.activation_seal.status(), plan, done)
    }

    /// Read node-manager's whole orphan inventory, one page at a time,
    /// within [`ORPHAN_INVENTORY_DEADLINE`].
    async fn read_orphan_inventory(&self) -> OrphanFindings {
        let mut findings = OrphanFindings::default();
        let gateway_run_root = pplx_space_host_layout::paths::NET_GATEWAY_RUN_DIR;
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
                    // Node-manager serves one inventory page at a time and
                    // answers a page asked while another caller's (an
                    // overlapping `:terminate` probe, or node-ctl) is served
                    // busy: ask again within the deadline.
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
        gentle: bool,
        disk_headroom: Option<u64>,
    ) -> Result<(DrainPlan, BTreeSet<String>), String> {
        let now = tokio::time::Instant::now();
        let (rows, inputs) = self.read_plan_inputs(epoch, disk_headroom, now)?;
        let inputs = PlanInputs { gentle, ..inputs };
        let ids = row_ids(&rows);
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

    /// [`Self::read_drain_plan`]'s read and plan for a preview, which notes
    /// nothing. The drain's memory is read as a call continuing the drain
    /// the node is sealed under, `sealed`, would plan with it: the failures
    /// it counted, how long it has watched each row in one lifecycle, and the
    /// rows waiting for cache disk past the bound. A node not sealed has no
    /// drain to continue, and a drain call there would start with none.
    ///
    /// Two verdicts turn on how long a row has sat unchanged, which a drain
    /// learns by watching across calls. Where the store knows, a preview
    /// takes it from there instead: the store stamps each write that changes
    /// a row's status, its lifecycle or the cleanup that owns it
    /// (`state_changed_at`), so a row outside the warm pool whose status
    /// last changed [`SETTLE_BOUND`] or longer ago counts as unchanged that
    /// long. A row in a transition the drain doesn't drive is
    /// then blocked, as it would be once a drain had watched it that long,
    /// unless it moves. A warm-pool member is not judged that way: its bound
    /// is on the eviction the pool reconciler starts only once a drain
    /// cordons the node, so its time in its lifecycle says nothing about it.
    /// It is pending (`warm_pool_eviction`) unless the drain the node is
    /// sealed under has watched it past the bound. A row without a stamp
    /// counts as just changed.
    fn read_preview_plan(
        &self,
        sealed: Option<u64>,
        disk_headroom: Option<u64>,
    ) -> Result<(DrainPlan, BTreeSet<String>), String> {
        let now = tokio::time::Instant::now();
        let (rows, mut inputs) = self.read_plan_reads(disk_headroom)?;
        if let Some(epoch) = sealed {
            inputs.failures = self.drain_actions.failures_under(epoch);
            inputs.unchanged =
                self.drain_actions
                    .seen_unchanged_past_bound(epoch, &row_lifecycles(&rows), now);
            inputs.disk_waits = self.drain_actions.disk_waits_past_bound(epoch, now);
        }
        let entered = self
            .db_conn()
            .map_err(|status| status.message().to_owned())
            .and_then(|conn| {
                crate::db::scan::status_entered_at(&conn)
                    .map_err(|error| format!("status stamp scan failed: {error}"))
            })?;
        if let Ok(wall_now) = crate::db::now_epoch_i64() {
            inputs.unchanged.extend(
                rows.iter()
                    .filter(|row| !row.pool_member)
                    .filter(|row| {
                        entered
                            .get(&row.sandbox_id)
                            .copied()
                            .flatten()
                            .is_some_and(|since| {
                                past_settle_bound(Duration::from_secs(
                                    wall_now.cast_unsigned().saturating_sub(since),
                                ))
                            })
                    })
                    .map(|row| row.sandbox_id.clone()),
            );
        }
        Ok((plan(&rows, &inputs), row_ids(&rows)))
    }

    /// Read every row, the artifacts without a durable copy and the
    /// evacuation record, as the planners read them, at `now`, with the
    /// drain's memory of earlier calls under `epoch`, which this notes. An
    /// error is the scan that failed.
    fn read_plan_inputs(
        &self,
        epoch: u64,
        disk_headroom: Option<u64>,
        now: tokio::time::Instant,
    ) -> Result<(Vec<DrainRow>, PlanInputs), String> {
        let (rows, mut inputs) = self.read_plan_reads(disk_headroom)?;
        inputs.failures = self.drain_actions.failures(epoch);
        inputs.unchanged =
            self.drain_actions
                .unchanged_since_bound(epoch, &row_lifecycles(&rows), now);
        inputs.disk_waits = self.drain_actions.disk_waits_past_bound(epoch, now);
        Ok((rows, inputs))
    }

    /// The reads a drain call and a preview share: every row, the artifacts
    /// without a durable copy, the evacuation record and the drain's work in
    /// flight, as the planners read them, with none of the drain's memory of
    /// earlier calls. Only reads. An error is the scan that failed.
    fn read_plan_reads(
        &self,
        disk_headroom: Option<u64>,
    ) -> Result<(Vec<DrainRow>, PlanInputs), String> {
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
            pplx_space_node_disk_admission::CacheDiskAdmissionConfig::from_env()
                .allocation_reservation_bytes;
        let rows: Vec<DrainRow> = sandboxes
            .iter()
            .map(|row| DrainRow {
                sandbox_id: row.id.clone(),
                lifecycle: row.state,
                pool_member: row.pool_template.is_some(),
                volume_encryption: row.volume_encryption,
                pin: row
                    .current_local_artifact_id
                    .zip(row.current_local_artifact_kind)
                    .map(|(artifact, kind)| (artifact.to_string(), kind)),
                suspend_refusal: matches!(
                    row.state,
                    SandboxStatus::Unowned(SandboxState::Started | SandboxState::Paused)
                )
                .then(|| self.drain_suspend_refusal(row))
                .flatten(),
                snapshot_bytes: if matches!(
                    row.state,
                    SandboxStatus::Unowned(SandboxState::Started | SandboxState::Paused)
                ) {
                    crate::service::tail_support::sandbox_guest_memory(&row.id)
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
        let inputs = PlanInputs {
            pending_backups,
            local_only,
            evacuations,
            running: self.drain_actions.running(),
            window: self.drain_actions.window,
            disk_headroom,
            active_uploads,
            ..PlanInputs::default()
        };
        Ok((rows, inputs))
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
}

/// [`drain_disk_headroom`] of `/space/cache` as disk admission measures it;
/// none when it can't be measured.
async fn read_disk_headroom() -> Option<u64> {
    use pplx_space_node_disk_admission::{CacheDiskAdmissionConfig, read_cache_disk_measurement};
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

/// The plan of a call whose read of the rows failed with `reason`.
fn read_failed(reason: String) -> (DrainPlan, BTreeSet<String>) {
    (
        DrainPlan {
            blocked: vec![(None, reason)],
            ..DrainPlan::default()
        },
        BTreeSet::new(),
    )
}

fn row_ids(rows: &[DrainRow]) -> BTreeSet<String> {
    rows.iter().map(|row| row.sandbox_id.clone()).collect()
}

fn row_lifecycles(rows: &[DrainRow]) -> Vec<(String, SandboxStatus)> {
    rows.iter()
        .map(|row| (row.sandbox_id.clone(), row.lifecycle))
        .collect()
}

/// Put what the scans outside the rows found ahead of what the rows gave:
/// each blocks, and an orphan directory only when no row in `rows` accounts
/// for it. The activations in flight at `first` go ahead of the pending.
fn fold_scans(
    plan: &mut DrainPlan,
    rows: &BTreeSet<String>,
    scans: DrainScans,
    first: &ActivationSealStatus,
) {
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
            .map(|(sandbox_id, action)| DrainPending {
                sandbox_id,
                reason: if action == HOLD {
                    HOLD_REASON.to_owned()
                } else {
                    String::new()
                },
                action,
            })
            .collect(),
        blocked: plan
            .blocked
            .into_iter()
            .map(|(sandbox_id, reason)| DrainBlocked { sandbox_id, reason })
            .collect(),
        done,
        gentle_epoch: None,
    }
}

/// The epoch a drain call names, and whether it drains gently: a gentle one
/// names its epoch in `gentle_epoch` alone, with `epoch` 0 (which a node
/// without gentle drains refuses before it seals) and no `cancel`.
pub(super) fn drain_epoch(
    epoch: u64,
    cancel: bool,
    gentle_epoch: u64,
) -> Result<(u64, bool), Status> {
    match (epoch, gentle_epoch) {
        (_, 0) => Ok((epoch, false)),
        (0, gentle_epoch) if !cancel => Ok((gentle_epoch, true)),
        _ => Err(Status::invalid_argument(
            "a gentle drain names its epoch in gentle_epoch alone, with epoch 0 and no cancel",
        )),
    }
}

const fn prior_to_wire(prior: DrainPrior) -> crate::proto::DrainPrior {
    match prior {
        DrainPrior::Started => crate::proto::DrainPrior::Started,
        DrainPrior::Paused => crate::proto::DrainPrior::Paused,
        DrainPrior::Stopped => crate::proto::DrainPrior::Stopped,
    }
}
