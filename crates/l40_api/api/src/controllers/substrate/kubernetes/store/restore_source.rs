//! Restore source selection: the source node and the suspend or stopped
//! artifact storage URI a restore reads.

use super::*;
use pplx_space_sandbox_store::restore_source::{
    self, RestoreSourceMode, RestoreSourcePlacement, RestoreSourceSelection,
};

/// Check a selected restore URI the catalog holds. Parsing is validation
/// rather than conversion: the caller wants the wire string, and a value
/// `StorageUri` rejects is a stored-data fault worth failing on rather than
/// handing to a node. `label` names the selector in the error.
fn validated_restore_storage_uri(
    storage_uri: Option<String>,
    label: &str,
) -> Result<Option<String>, SubstrateError> {
    if let Some(storage_uri) = &storage_uri {
        StorageUri::parse(storage_uri).map_err(|err| SubstrateError::Failed {
            message: format!("invalid selected {label} restore storage URI: {err}"),
        })?;
    }
    Ok(storage_uri)
}

/// Which suspend snapshot a restore selects when no existing restore pod
/// already names one, and whether its source node's local copy counts.
/// `pplx_space_sandbox_store::restore_source::select_restore_source` holds the
/// full rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::controllers::substrate::kubernetes) enum RestoreSnapshotChoice {
    /// The source node's local copy, or the remote snapshot the user-restore
    /// rule selects: the current local artifact's, a placement-fresh one, or
    /// on a retry the one the restore started from.
    SourceNodeOrFresh,
    /// A remote snapshot only, never the source node: the restore lands
    /// elsewhere. A row that records its current local artifact restores that
    /// artifact's snapshot; a row without one restores only the newest full
    /// snapshot, once it is ready, so it can't read a stale prior-cycle one.
    NewestRemote,
}

impl K8sStore {
    /// Whether the registered Kubernetes node named `node_name` is ready:
    /// `None` when no registered node has that name.
    pub(in crate::controllers::substrate::kubernetes) async fn restore_source_node_ready(
        &self,
        node_name: &str,
    ) -> Result<Option<bool>, SubstrateError> {
        sqlx::query_scalar(sql!(
            "SELECT n.state = 'ready'
               FROM node_k8s nk
               JOIN nodes n ON n.id = nk.node_id
              WHERE nk.k8s_node_name = $1
                AND n.managed_by = $2
              ORDER BY n.state = 'ready' DESC
              LIMIT 1"
        ))
        .bind(node_name)
        .bind(pplx_space_common_core::types::SubstrateKind::Kubernetes)
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to look up the Kubernetes restore source node")
    }

    pub(in crate::controllers::substrate::kubernetes) async fn restore_source_node_name(
        &self,
        sandbox: &Sandbox,
    ) -> Result<Option<String>, SubstrateError> {
        if let Some(node_id) = sandbox.target_node_id.or(sandbox.active_node_id) {
            if let Some(node_name) = sqlx::query_scalar::<_, String>(sql!(
                "SELECT k8s_node_name FROM node_k8s WHERE node_id = $1"
            ))
            .bind(node_id)
            .fetch_optional(&self.pool)
            .await
            .db_context("failed to query Kubernetes restore node preference")?
            {
                return Ok(Some(node_name));
            }

            tracing::warn!(
                sandbox_id = %sandbox.id,
                node_id = %node_id,
                "sandbox restore node id has no Kubernetes node mapping; falling back to pod tracking"
            );
        }

        // Preserve the node that held the suspended bytes after placement
        // pointers have been cleared.
        let executions_query = sql!(
            "SELECT ske.k8s_node_name
               FROM sandbox_k8s_executions ske
              WHERE ske.sandbox_id = $1
                AND ske.k8s_node_name IS NOT NULL
              ORDER BY ske.generation DESC
              LIMIT 1"
        );
        sqlx::query_scalar::<_, String>(executions_query)
            .bind(sandbox.id)
            .fetch_optional(&self.pool)
            .await
            .db_context("failed to query Kubernetes restore node preference")
    }

    /// The restore source for a `stopped` sandbox: the node name to pin to, and
    /// whether a stopped evacuation has left the row unplaced.
    ///
    /// ## What the evacuation half means
    ///
    /// Only `'stopped'` counts as the marker, and only once `active_node_id`
    /// is clear: `pplx_space_sandbox_store::restore_source::restore_source_plan`
    /// says why both halves are needed. When it holds, the drain has released
    /// the node holding the copy, so the node name beside it is not a restore
    /// source at all — `KubernetesSubstrate::stopped_restore_plan` refuses the
    /// `start`.
    ///
    /// ## Why one statement
    ///
    /// **One statement for both, and it reads placement from the row rather
    /// than from the caller's `Sandbox`.** Both properties are deliberate.
    ///
    /// *One statement*, because asking for the marker separately would cost an
    /// extra round trip on every `start` of a stopped sandbox, in every cluster
    /// — including the ones with the drain feature switched off.
    ///
    /// *From the row*, because the caller's struct was loaded earlier in the
    /// request and a teardown can land in between. Short-circuiting on
    /// `sandbox.active_node_id.is_some()` reads well and is wrong in the
    /// direction that matters: a struct that says placed against a row that says
    /// unplaced would skip the refusal and plan a pod against the drained node.
    /// Reading both facts from one row of one statement also makes them agree
    /// with each other.
    ///
    /// The name resolves the way [`Self::restore_source_node_name`] does: the
    /// placement's Kubernetes mapping first, then the newest execution that
    /// named a node. That fallback is why the evacuation flag cannot be left to
    /// a missing name — retiring an execution marks the row inactive without
    /// deleting it, so it keeps naming the drained node long after the teardown
    /// cleared the placement columns.
    pub(in crate::controllers::substrate::kubernetes) async fn restore_source_plan(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<UnplacedRestoreSource, SubstrateError> {
        // The two name candidates come back *separately* rather than
        // pre-`COALESCE`d, and the placement's node id comes back with them, so
        // the missing-mapping WARN [`Self::restore_source_node_name`] emits can
        // be emitted here too without a second round trip.
        let Some(restore_source::RestoreSourcePlanRow {
            mapped_name,
            execution_name,
            placement_node_id,
            stopped_evacuation_unplaced,
        }) = restore_source::restore_source_plan(&self.pool, sandbox_id)
            .await
            .db_context("failed to query Kubernetes restore node preference")?
        else {
            return Ok(UnplacedRestoreSource::default());
        };
        if let Some(node_id) = placement_node_id
            && mapped_name.is_none()
        {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                node_id = %node_id,
                "sandbox restore node id has no Kubernetes node mapping; falling back to pod tracking"
            );
        }
        Ok(UnplacedRestoreSource {
            node_name: mapped_name.or(execution_name),
            stopped_evacuation_unplaced,
        })
    }

    /// The storage URI of the suspend capture a restore of `sandbox` loads:
    /// `snapshot`, and on a `restoring` sandbox the retry, mapped onto the
    /// shared [`restore_source::select_restore_source`] rule. The node-drain
    /// cohort restore (`NewestRemote`) applies its rule on every attempt.
    pub(in crate::controllers::substrate::kubernetes) async fn selected_suspend_restore_storage_uri(
        &self,
        sandbox: &Sandbox,
        snapshot: RestoreSnapshotChoice,
    ) -> Result<Option<String>, SubstrateError> {
        let restore_retry = sandbox.lifecycle.trim().eq_ignore_ascii_case("restoring");
        let mode = match snapshot {
            RestoreSnapshotChoice::SourceNodeOrFresh => {
                RestoreSourceMode::or_retry(RestoreSourceMode::PlacementFresh, restore_retry)
            }
            RestoreSnapshotChoice::NewestRemote => RestoreSourceMode::NewestFull,
        };
        let storage_uri = match restore_source::select_restore_source(
            &self.pool,
            sandbox.id,
            mode,
            RestoreSourcePlacement::Any,
        )
        .await
        .db_context("failed to select Kubernetes suspend restore URI")?
        {
            RestoreSourceSelection::Selected(selected) => Some(selected.storage_connection_string),
            RestoreSourceSelection::Unavailable => None,
            // The caller falls back to the source node's local copy while
            // that node is registered (`suspend_restore_plan`).
            RestoreSourceSelection::Unordered => {
                return Err(SubstrateError::RestoreContextUnavailable {
                    message: restore_source::unordered_restore_source_message(sandbox.id),
                });
            }
        };
        validated_restore_storage_uri(storage_uri, "suspend")
    }

    /// Run the stopped restore-source selector and validate what it returns.
    ///
    /// `query` arrives already `sql!`-stamped by its caller, so the selector
    /// keeps its own locator token in `pg_stat_activity` and DBM. Do not
    /// re-stamp it here.
    async fn fetch_stopped_restore_storage_uri(
        &self,
        query: String,
        sandbox_id: SandboxId,
    ) -> Result<Option<String>, SubstrateError> {
        let storage_uri: Option<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(query))
            .bind(sandbox_id)
            .fetch_optional(&self.pool)
            .await
            .db_context("failed to select Kubernetes stopped restore URI")?;
        validated_restore_storage_uri(storage_uri, "stopped")
    }

    /// Disk-only sibling of [`Self::selected_suspend_restore_storage_uri`]:
    /// the remote artifact a `stopped` sandbox is restored from once its
    /// rootfs has been evacuated off its node.
    ///
    /// The suspend selector cannot serve this shape. It excludes a stopped
    /// disk row on two independent filters — `s.lifecycle IN ('suspended',
    /// 'restoring')` and `sn.type = 'full'` — and neither is incidental. A
    /// `full` artifact's file set is `vmstate` + `memory` + rootfs
    /// (`snapshot_upload::COMMON_VM_ARTIFACTS`, via
    /// `vm_artifacts_for_format`), where `Disk` contributes the rootfs alone
    /// (`artifact_files_for_kind`; both kinds then append the metadata
    /// document). A stopped sandbox has no VM to have produced the first two.
    /// And `stopped` is outside the lifecycle set that selector serves.
    ///
    /// The guard is `snapshot_is_current_local_artifact` and nothing else.
    /// Composing that shared fragment rather than an inline equivalent is what
    /// keeps the retention sweeper from pruning the row this selector loads;
    /// the fragment's own docs in `pplx_space_sandbox_portability` own that
    /// argument. `SnapshotRetentionDbStore` sweeps `stop` disk rows as well as
    /// full ones, so that guard is the operative exclusion for the pinned disk
    /// row.
    ///
    /// ## Why there is no markerless legacy branch
    ///
    /// The suspend selector carries one, for rollout-skew rows that predate
    /// the artifact-identity marker. A stopped evacuation has no comparable
    /// population to be compatible with, for a blunter reason than the
    /// suspend case: **no control-plane writer sets
    /// `sandboxes.current_local_artifact_kind` to `'disk'` at all.** Three
    /// Postgres writers set it non-NULL, and each is blocked by a *different*
    /// mechanism — worth naming individually, because the obvious answer
    /// ("they all require `ArtifactOrigin::Suspend`") is not what stops the
    /// third one:
    ///
    /// * `finish_suspended_execution` (this file, both the marker-assignment
    ///   and the direct-`UPDATE` positions) takes the kind from a
    ///   `PublishedLocalArtifact`, and `PublishedLocalArtifact::from_published`
    ///   *rejects* an artifact whose kind is not the `expected_kind` its caller
    ///   passes. `Disk` parses off the wire fine; the runtime check is what
    ///   refuses it, and both callers pass `ArtifactKind::Full`.
    /// * `persist_optional_current_local_artifact` (shared `substrate/mod.rs`,
    ///   sole production caller on the Direct substrate) takes `origin` as a
    ///   free parameter, and that caller passes `Suspend` with the same
    ///   `Full`-checked artifact.
    /// * the census-recovery publish accepts a node-published kind of `disk`
    ///   off the wire — nothing rejects it at the boundary. What filters it is
    ///   one SQL conjunct, `current_local_artifact_kind = 'full'` inside
    ///   `node_published_suspend_evidence`, which gates whether the published
    ///   kind is projected at all. Relaxing that conjunct to admit stop-origin
    ///   pins — the natural place to build the missing writer — is what will
    ///   make this selector live.
    ///
    /// (The node-local writer `publish_stopped_artifact` does mint a disk pin,
    /// but in SQLite, in a table this query never reads; the control-plane pin
    /// is a separate value.) One cluster writer does produce a disk pin: node
    /// drain's `record_stopped_evacuation_artifact` writes `('disk', 'stop')` on
    /// a `stopped` row it has claimed for evacuation. The pin stays if the
    /// evacuation is abandoned before its teardown, and a stopped
    /// re-placement's finalize keeps it on the row it settles; a successful
    /// start drops it (`maybe_mark_started_after_start`). Those are the only
    /// rows this selector can return, and the stopped restart plan consults
    /// it only when no node is known.
    ///
    /// That is an enumeration of *current* writers, which is weaker than a
    /// schema guarantee: the artifact-identity migration has permitted
    /// `('disk', 'stop')` since it landed, so a row left by some past version
    /// is not excluded by argument. Such a row with a markerless snapshot
    /// simply selects nothing here and the restart falls back to the
    /// node-only shape, which is the safe direction.
    ///
    /// Requiring the pin is also the only thing that ties the URI to *these*
    /// bytes — `BeginArtifactBackup` accepts `(Disk, Snapshot)`, so a
    /// user-taken disk snapshot is a plausible sibling row, and a
    /// timestamp-only freshness rule would happily load it.
    pub(in crate::controllers::substrate::kubernetes) async fn selected_stopped_restore_storage_uri(
        &self,
        sandbox: &Sandbox,
    ) -> Result<Option<String>, SubstrateError> {
        let pinned = snapshot_is_current_local_artifact("s", "sn");
        // Newest-first because a retried backup of one artifact mints a fresh
        // `snapshots` row per attempt while keeping the same
        // `local_artifact_id`: the pin can match several rows, and the latest
        // registered location is the one to load. `sn.id` breaks a timestamp
        // tie so that claim is actually true — two attempts registering within
        // one statement timestamp would otherwise pick nondeterministically.
        // Same `(timestamp, id)` total order the retention store relies on for
        // the same reason, though it spells it as a row-value comparison in a
        // keep-N probe (`(newer.timestamp, newer.id) > (s.timestamp, s.id)`)
        // rather than an `ORDER BY` — don't go looking for a matching clause
        // there. The suspend sibling, `select_restore_source`, breaks ties the
        // same way.
        let query = sql!(format!(
            "SELECT ssl.storage_connection_string
               FROM sandboxes s
               JOIN snapshots sn ON sn.sandbox_id = s.id
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
              WHERE s.id = $1
                AND s.lifecycle = 'stopped'
                AND ssl.orphaned_at IS NULL
                AND sn.type = 'disk'
                AND sn.state = 'ready'
                AND {pinned}
              ORDER BY sn.timestamp DESC, sn.id DESC
              LIMIT 1"
        ));
        self.fetch_stopped_restore_storage_uri(query, sandbox.id)
            .await
    }

    /// The disk artifact the evacuation restore sweep re-places an evacuated
    /// `stopped` sandbox from, together with the artifact id the node checks it
    /// against.
    ///
    /// Neither existing selector can serve this row. The evacuation claim has
    /// already moved it `stopped → restoring`, so
    /// [`Self::selected_stopped_restore_storage_uri`]'s `lifecycle = 'stopped'`
    /// excludes it, and [`Self::selected_suspend_restore_storage_uri`] requires
    /// a `full` snapshot, which a stopped sandbox never has.
    ///
    /// `pplx_space_sandbox_store::restore_source::stopped_evacuation_restore_source`
    /// selects it: its docs say what scopes it to the stopped evacuation, why
    /// the pin is required, and why it orders newest first.
    pub(in crate::controllers::substrate::kubernetes) async fn selected_stopped_evacuation_restore_source(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<StoppedEvacuationRestoreSource>, SubstrateError> {
        let Some(restore_source::StoppedEvacuationRestoreSourceRow {
            storage_connection_string: storage_uri,
            current_local_artifact_id: local_artifact_id,
        }) = restore_source::stopped_evacuation_restore_source(&self.pool, sandbox_id)
            .await
            .db_context("failed to select Kubernetes stopped evacuation restore source")?
        else {
            return Ok(None);
        };
        StorageUri::parse(&storage_uri).map_err(|err| SubstrateError::Failed {
            message: format!("invalid selected stopped evacuation restore storage URI: {err}"),
        })?;
        Ok(Some(StoppedEvacuationRestoreSource {
            storage_uri,
            local_artifact_id,
        }))
    }

    /// The workers a stopped re-placement must not be placed on: every node an
    /// execution of this sandbox ran on that is no longer the active one.
    ///
    /// A local `stopped` row for the sandbox can only exist where one of its
    /// pods ran: the node it was stopped on, a drained source the
    /// re-placement released, or a destination an earlier attempt staged on
    /// before failing. Each of those executions is inactive by now: the
    /// stopped-on node's was retired by the drain's teardown (a stop alone
    /// leaves it active), and the others were replaced. So the inactive rows
    /// name them, unless an operator has pruned old
    /// generations (`scripts/admin/prune_cluster_artifacts.py` deletes
    /// inactive rows below a sandbox's newest one). sandbox-manager refuses to materialize
    /// over a row pinned to another artifact instead of discarding its bytes,
    /// so a pod placed on one of these nodes never converges. The active
    /// execution is the re-placement's own, and its node holds only what this
    /// re-placement staged.
    ///
    /// The node name is a record of the last node the control plane saw the
    /// execution's pod on, not a history, so it can miss one: a pod whose
    /// binding the control plane never observed leaves it NULL, a
    /// same-generation pod that bound elsewhere overwrites it, and a recovery
    /// that failed with its pod gone clears it. A node missed that way, or
    /// named only by a pruned row, can still be chosen.
    pub(in crate::controllers::substrate::kubernetes) async fn stopped_replacement_excluded_nodes(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Vec<String>, SubstrateError> {
        sqlx::query_scalar(sql!(
            "SELECT DISTINCT BTRIM(k8s_node_name)
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1
                AND NOT active
                AND NULLIF(BTRIM(k8s_node_name), '') IS NOT NULL
              ORDER BY 1"
        ))
        .bind(sandbox_id)
        .fetch_all(&self.pool)
        .await
        .db_context("failed to read the nodes earlier executions of the sandbox ran on")
    }
}
