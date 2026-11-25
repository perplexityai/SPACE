//! The `DrainNode` handler: seal or cancel, read, plan, start and report.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use super::planner::*;
use crate::activation_seal::ActivationSealStatus;
use crate::proto::drain_node_request::{Cancel, Drain, Op};
use crate::proto::drain_node_response::Result as DrainResult;
use crate::proto::{
    CancelResult, DrainBlocked, DrainEvacuated, DrainNodeRequest, DrainNodeResponse, DrainPending,
    DrainReport, PreviewDrainNodeRequest, PreviewDrainNodeResponse,
};
use crate::service::*;

/// How long one call may spend reading node-manager's whole orphan
/// inventory, so a slow node-manager is reported `blocked` after a bounded
/// wait rather than holding the call. The drain's work goes on between
/// calls.
pub(super) const ORPHAN_INVENTORY_DEADLINE: Duration = Duration::from_secs(20);

/// How long a call waits before asking a busy orphan inventory again.
const ORPHAN_INVENTORY_BUSY_RETRY: Duration = Duration::from_millis(200);

/// What one pass of the drain read and decided.
pub(super) struct DrainPass {
    pub(super) plan: DrainPlan,
    /// The ids of the rows it read.
    pub(super) rows: BTreeSet<String>,
    /// When each row it read entered its status, in Unix seconds: the
    /// `since` of the row's pending entries.
    pub(super) entered: BTreeMap<String, Option<u64>>,
    /// The ids of the rows it read that are warm-pool members: their
    /// pending entries say so.
    pub(super) pool_members: BTreeSet<String>,
}

/// What every pass reads ([`SandboxManagerService::read_plan_reads`]).
pub(super) struct PlanReads {
    pub(super) rows: Vec<DrainRow>,
    /// When each row entered its status, by id, in Unix seconds.
    pub(super) entered: BTreeMap<String, Option<u64>>,
    pub(super) inputs: PlanInputs,
}

impl SandboxManagerService {
    /// One call at a time: a call that arrives while another is in progress
    /// fails at once with `Aborted` and changes nothing. A call ends within
    /// the drain's call timeout; one that runs out answers
    /// `DeadlineExceeded`, and the next call starts over.
    pub(in crate::service) async fn drain_node(
        &self,
        request: Request<DrainNodeRequest>,
    ) -> Result<Response<DrainNodeResponse>, Status> {
        self.authorize_peer(&request, "DrainNode", ALLOW_NODE_DRAIN)?;
        let DrainNodeRequest { generation, op } = request.into_inner();
        let op = op.ok_or_else(|| {
            Status::invalid_argument("a DrainNode call needs an op: drain or cancel")
        })?;
        let _call = self.drain_actions.enter_call()?;
        let timeout = self.drain_actions.call_timeout;
        let answer = tokio::time::timeout(timeout, async {
            match op {
                Op::Drain(Drain { evacuate }) => {
                    let first = self.begin_drain(generation, evacuate)?;
                    let scans = self.read_drain_scans().await;
                    Ok(DrainNodeResponse {
                        generation,
                        result: Some(DrainResult::Drain(
                            self.finish_drain(generation, &first, scans),
                        )),
                    })
                }
                Op::Cancel(Cancel {}) => Ok(DrainNodeResponse {
                    generation: self.cancel_drain(generation)?,
                    result: Some(DrainResult::Cancel(CancelResult {})),
                }),
            }
        })
        .await
        .map_err(|_| {
            Status::deadline_exceeded(format!(
                "the DrainNode call under generation {generation} didn't finish within {}s",
                timeout.as_secs()
            ))
        })?;
        answer.map(Response::new)
    }

    /// A drain call's seal: under `generation`, and evacuating under it from
    /// now on when `evacuate` says so. The seal as it stands then.
    pub(super) fn begin_drain(
        &self,
        generation: u64,
        evacuate: bool,
    ) -> Result<ActivationSealStatus, Status> {
        self.activation_seal.seal(&self.db, generation)?;
        if evacuate {
            self.drain_actions.evacuate(generation);
        }
        Ok(self.activation_seal.status())
    }

    /// Cancel `generation`: unsealed, and the evacuating cleared with it.
    /// Nothing is started, stopped or handed back, so the answer comes at
    /// once: the tombstone, the generation at or below which the node now
    /// refuses seals.
    pub(super) fn cancel_drain(&self, generation: u64) -> Result<u64, Status> {
        let tombstone = self.activation_seal.unseal(&self.db, generation)?;
        self.drain_actions.stop_evacuating();
        Ok(tombstone)
    }

    /// One preview at a time: another that arrives meanwhile fails at once
    /// with `Aborted`. Bounded by the drain's call timeout, as a drain call
    /// is.
    pub(in crate::service) async fn preview_drain_node(
        &self,
        request: Request<PreviewDrainNodeRequest>,
    ) -> Result<Response<PreviewDrainNodeResponse>, Status> {
        // It changes nothing, but answers what a drain would find on the
        // whole node, so it takes the drain's authority.
        self.authorize_peer(&request, "PreviewDrainNode", ALLOW_NODE_DRAIN)?;
        let PreviewDrainNodeRequest {} = request.into_inner();
        self.refuse_preview_while_sealed()?;
        let _preview = self.drain_actions.enter_preview()?;
        let timeout = self.drain_actions.call_timeout;
        // Tagged, so what the drain's scans log during a preview says it came
        // from one.
        let span = tracing::info_span!("drain_node_preview", preview = true);
        let report = tokio::time::timeout(timeout, async {
            // Not node-manager's orphan inventory: its scans are shared by
            // every caller on the node and few, so a preview's could evict a
            // drain call's or node-ctl's.
            let scans = DrainScans {
                disk_headroom: read_disk_headroom().instrument(span.clone()).await,
                ..DrainScans::default()
            };
            span.in_scope(|| self.finish_preview(scans))
        })
        .await
        .map_err(|_| {
            Status::deadline_exceeded(format!(
                "the drain preview didn't finish within {}s",
                timeout.as_secs()
            ))
        })??;
        Ok(Response::new(PreviewDrainNodeResponse {
            report: Some(report),
        }))
    }

    /// What a drain call scans outside the rows: node-manager's orphan
    /// inventory and the cache disk's headroom. Both only read.
    async fn read_drain_scans(&self) -> DrainScans {
        let orphans = self.read_orphan_inventory().await;
        DrainScans {
            blocked: orphans.blocked,
            orphan_dirs: orphans.dirs,
            disk_headroom: read_disk_headroom().await,
        }
    }

    /// Run a pass under the call's `generation` ([`Self::run_drain_pass`])
    /// and report it. `first` is the seal as the call read it right after
    /// sealing, before its scans; `scans` are what the scans that ran
    /// outside the rows found. Every reason the drain isn't finished is an
    /// entry, so it is finished exactly when nothing is pending or blocked.
    pub(super) fn finish_drain(
        &self,
        generation: u64,
        first: &ActivationSealStatus,
        scans: DrainScans,
    ) -> DrainReport {
        let DrainPass {
            mut plan,
            rows,
            entered,
            pool_members,
        } = self
            .run_drain_pass(generation, scans.disk_headroom)
            .unwrap_or_else(read_failed);
        fold_scans(&mut plan, &rows, scans, first);
        // A cancelled generation is never sealed again, so the same
        // generation at both reads is one continuous seal. One call runs at a
        // time, so nothing unseals the node during this one; were it to, the
        // walk proves nothing and the report says so.
        let last = self.activation_seal.status();
        if first.sealed_generation != Some(generation) || last.sealed_generation != Some(generation)
        {
            plan.blocked.insert(
                0,
                format!(
                    "the seal under generation {generation} didn't hold for the whole call: \
                     the node was sealed under {} at its start and {} at its end",
                    seal_name(first.sealed_generation),
                    seal_name(last.sealed_generation),
                ),
            );
        }
        report(plan, &entered, &pool_members)
    }

    /// What an evacuating drain would find on this unsealed node now,
    /// changing nothing: the plan [`Self::read_preview_plan`] makes, with
    /// what a drain would start reported `pending` and none of it started.
    /// A node sealed under a generation refuses it with `FailedPrecondition`
    /// naming the generation, before its reads or after them, since a
    /// preview describes an unsealed node; one whose seal row is unreadable
    /// refuses it with `Internal`.
    pub(super) fn finish_preview(&self, scans: DrainScans) -> Result<DrainReport, Status> {
        self.refuse_preview_while_sealed()?;
        let seal = self.activation_seal.status();
        let DrainPass {
            mut plan,
            rows,
            entered,
            pool_members,
        } = self
            .read_preview_plan(scans.disk_headroom)
            .unwrap_or_else(read_failed);
        fold_scans(&mut plan, &rows, scans, &seal);
        self.refuse_preview_while_sealed()?;
        Ok(report(plan, &entered, &pool_members))
    }

    /// The refusal of a preview on a node whose seal is unreadable, or that
    /// is sealed under a generation.
    fn refuse_preview_while_sealed(&self) -> Result<(), Status> {
        if let Some(error) = self.activation_seal.read_error() {
            return Err(Status::internal(format!(
                "activation seal unreadable: {error}"
            )));
        }
        match self.activation_seal.status().sealed_generation {
            Some(generation) => Err(Status::failed_precondition(format!(
                "{} (generation {generation}); a drain preview reads only an unsealed node",
                pplx_space_rpc_contracts::wake_signals::ACTIVATIONS_SEALED_MARKER
            ))),
            None => Ok(()),
        }
    }

    /// A pass's reads and plan for a preview, which starts and notes
    /// nothing. It plans as an evacuating drain would, so no started
    /// sandbox is held. An unsealed node has no drain to continue, and a
    /// drain call there would start with no failures, so the plan has none.
    fn read_preview_plan(&self, disk_headroom: Option<u64>) -> Result<DrainPass, String> {
        let PlanReads {
            rows,
            entered,
            inputs,
        } = self.read_plan_reads(disk_headroom)?;
        Ok(DrainPass {
            plan: plan(&rows, &inputs),
            rows: row_ids(&rows),
            entered,
            pool_members: pool_member_ids(&rows),
        })
    }

    /// One pass of the drain under a call's `generation`. One call runs at a
    /// time, so a pass plans with the work earlier calls started. It reads
    /// the seal and, while the seal is still under `generation`, reads every
    /// row, plans (holding the started rows unless a call under
    /// `generation` has said to evacuate), and starts what the plan asks for.
    /// A seal no longer under `generation` starts nothing and touches none
    /// of the drain's memory. An error is the scan that failed.
    pub(super) fn run_drain_pass(
        &self,
        generation: u64,
        disk_headroom: Option<u64>,
    ) -> Result<DrainPass, String> {
        let sealed = self.activation_seal.status().sealed_generation == Some(generation);
        let PlanReads {
            rows,
            entered,
            mut inputs,
        } = self.read_plan_reads(disk_headroom)?;
        inputs.hold_running = !(sealed && self.drain_actions.evacuating(generation));
        if sealed {
            inputs.failures = self.drain_actions.failures(generation);
        }
        let mut plan = plan(&rows, &inputs);
        self.confirm_finished_rows(&mut plan, &rows, &inputs, disk_headroom);
        if sealed {
            // A failure counts while the drain still works on its sandbox,
            // starting it or waiting for it; one that succeeded, or moved
            // on, is forgotten.
            let working = plan
                .actions
                .iter()
                .map(|action| action.target().0.to_owned())
                .chain(inputs.running.keys().cloned())
                .chain(plan.pending.iter().map(|wait| wait.sandbox_id.clone()))
                .collect();
            self.drain_actions.keep_failures_of(&working);
            self.start_actions(generation, &mut plan);
        } else {
            plan.actions.clear();
        }
        Ok(DrainPass {
            plan,
            rows: row_ids(&rows),
            entered,
            pool_members: pool_member_ids(&rows),
        })
    }

    /// Confirm, under each sandbox's artifact lock, the rows `plan` read from
    /// `rows` and `inputs` and found finished: evacuated, or with nothing
    /// left to keep (a failed row, a warm-pool member without a VM). Every
    /// writer of a sandbox's artifacts (a publication, a pin, a backup's
    /// registration, a rollback, a suspend, an eviction) holds that lock, so a
    /// row read under it can't move until the lock is dropped. Each lock is
    /// only tried, never waited for, and before the database's: writers take
    /// the wake lock, then the artifact lock, then the database, so the
    /// call never waits on a writer while holding what it waits for. A busy
    /// lock leaves its row pending. With the locks held the rows are read
    /// again, and a row whose own record, artifacts without a durable copy or
    /// pending backups changed since the plan's read is left pending too; the
    /// next call plans both again. The locks are dropped before the plan's
    /// work starts.
    pub(super) fn confirm_finished_rows(
        &self,
        plan: &mut DrainPlan,
        rows: &[DrainRow],
        inputs: &PlanInputs,
        disk_headroom: Option<u64>,
    ) {
        let waiting: BTreeSet<String> = plan
            .pending
            .iter()
            .map(|wait| wait.sandbox_id.clone())
            .collect();
        let mut guards = Vec::new();
        let mut held = Vec::new();
        for row in rows.iter().filter(|row| !waiting.contains(&row.sandbox_id)) {
            match self
                .write_gate
                .try_lock_for_read(&row.sandbox_id, write_gate::Reader::DrainConfirm)
            {
                Some(guard) => {
                    guards.push(guard);
                    held.push(row);
                }
                None => plan.recheck(&row.sandbox_id, "another writer holds its artifacts"),
            }
        }
        if held.is_empty() {
            return;
        }
        let again = match self.read_plan_reads(disk_headroom) {
            Ok(again) => again,
            Err(error) => {
                for row in held {
                    plan.recheck(
                        &row.sandbox_id,
                        &format!("its read under its artifact lock failed: {error}"),
                    );
                }
                return;
            }
        };
        for row in held {
            let id = &row.sandbox_id;
            let unchanged = again.rows.iter().find(|read| read.sandbox_id == *id) == Some(row)
                && again.inputs.local_only.get(id) == inputs.local_only.get(id)
                && again.inputs.pending_backups.get(id) == inputs.pending_backups.get(id);
            if !unchanged {
                plan.recheck(id, "its row changed while the drain read it");
            }
        }
        drop(guards);
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

    /// The reads every pass makes: every row, when each entered its status,
    /// the artifacts without a durable copy and the drain's work in flight,
    /// as the planner reads them, with none of the drain's memory of earlier
    /// passes. Only reads. An error is the scan that failed.
    pub(super) fn read_plan_reads(&self, disk_headroom: Option<u64>) -> Result<PlanReads, String> {
        let (sandboxes, entered, local_only_rows, pending_rows) = {
            let conn = self
                .db_conn()
                .map_err(|status| status.message().to_owned())?;
            // One lock, so no row or artifact lands between the reads.
            let sandboxes = crate::db::scan::all_sandboxes(&conn, crate::db::scan::StrictRows)
                .map_err(|error| format!("sandbox scan failed: {error}"))?;
            let entered = crate::db::scan::status_entered_at(&conn)
                .map_err(|error| format!("status stamp scan failed: {error}"))?;
            let local_only = crate::db::scan::local_only_artifacts(&conn)
                .map_err(|error| format!("local artifact scan failed: {error}"))?;
            let pending = crate::db::scan::pending_backups(&conn)
                .map_err(|error| format!("pending backup scan failed: {error}"))?;
            (sandboxes, entered, local_only, pending)
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
        let inputs = PlanInputs {
            pending_backups,
            local_only,
            running: self.drain_actions.running(),
            window: self.drain_actions.window,
            disk_headroom,
            active_uploads,
            ..PlanInputs::default()
        };
        Ok(PlanReads {
            rows,
            entered,
            inputs,
        })
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

/// The pass of a call whose read of the rows failed with `reason`.
fn read_failed(reason: String) -> DrainPass {
    DrainPass {
        plan: DrainPlan {
            blocked: vec![reason],
            ..DrainPlan::default()
        },
        rows: BTreeSet::new(),
        entered: BTreeMap::new(),
        pool_members: BTreeSet::new(),
    }
}

fn row_ids(rows: &[DrainRow]) -> BTreeSet<String> {
    rows.iter().map(|row| row.sandbox_id.clone()).collect()
}

fn pool_member_ids(rows: &[DrainRow]) -> BTreeSet<String> {
    rows.iter()
        .filter(|row| row.pool_member)
        .map(|row| row.sandbox_id.clone())
        .collect()
}

/// A seal as a report names it.
fn seal_name(sealed: Option<u64>) -> String {
    sealed.map_or_else(
        || "no generation".to_owned(),
        |generation| format!("generation {generation}"),
    )
}

/// Put what the scans outside the rows found ahead of what the rows gave:
/// each blocks, and an orphan directory only when no row in `rows` accounts
/// for it. The activations and the artifact producers in flight at `first`
/// go ahead of the pending: until a producer ends, its artifact isn't there
/// for a pass to see.
pub(super) fn fold_scans(
    plan: &mut DrainPlan,
    rows: &BTreeSet<String>,
    scans: DrainScans,
    first: &ActivationSealStatus,
) {
    let orphan_dirs = scans
        .orphan_dirs
        .into_iter()
        .filter(|(sandbox_id, _)| !rows.contains(sandbox_id))
        .map(|(_, reason)| reason);
    plan.blocked
        .splice(0..0, scans.blocked.into_iter().chain(orphan_dirs));
    // An activation admitted before the seal can still launch or resume a
    // VM until it settles.
    let activations = first.in_flight.iter().map(|sandbox_id| DrainWait {
        sandbox_id: sandbox_id.clone(),
        wait: Wait::Activation,
        reason: None,
    });
    let producers = first
        .producers_in_flight
        .iter()
        .map(|sandbox_id| DrainWait {
            sandbox_id: sandbox_id.clone(),
            wait: Wait::Snapshot,
            reason: None,
        });
    plan.pending.splice(0..0, activations.chain(producers));
}

/// The report of `plan`: each pending entry is `running` when its row was a
/// started VM at the plan's read, a `pool_member` when its row is one of
/// `pool_members`, and its `since` is when its row entered its status, from
/// `entered`.
pub(super) fn report(
    plan: DrainPlan,
    entered: &BTreeMap<String, Option<u64>>,
    pool_members: &BTreeSet<String>,
) -> DrainReport {
    let timestamp = |seconds: i64| prost_types::Timestamp { seconds, nanos: 0 };
    DrainReport {
        evacuated: plan
            .evacuated
            .into_iter()
            .map(|(sandbox_id, kind, artifact_id)| DrainEvacuated {
                sandbox_id,
                artifact_kind: crate::proto::ArtifactKind::from(kind) as i32,
                artifact_id,
            })
            .collect(),
        pending: plan
            .pending
            .into_iter()
            .map(|wait| DrainPending {
                since: entered
                    .get(&wait.sandbox_id)
                    .copied()
                    .flatten()
                    .and_then(|seconds| i64::try_from(seconds).ok())
                    .map(timestamp),
                wait: wait.wait.kind() as i32,
                running: plan.started.contains(&wait.sandbox_id),
                pool_member: pool_members.contains(&wait.sandbox_id),
                reason: wait.reason(),
                sandbox_id: wait.sandbox_id,
            })
            .collect(),
        blocked: plan
            .blocked
            .into_iter()
            .map(|reason| DrainBlocked { reason })
            .collect(),
    }
}
