//! Restore source selection: the source node and the suspend or stopped
//! artifact storage URI a restore reads.

use super::*;

impl K8sStore {
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

    pub(in crate::controllers::substrate::kubernetes) async fn selected_suspend_restore_storage_uri(
        &self,
        sandbox: &Sandbox,
        evacuation: bool,
    ) -> Result<Option<String>, SubstrateError> {
        // Three snapshot-selection modes:
        //
        // * **Suspended user restore** (`evacuation = false`): the placement-
        //   freshness gate — only a snapshot newer than the last placement is
        //   canonical, so a `wake → modify → re-suspend` cycle can't restore
        //   stale bytes.
        // * **Restore retry** (`lifecycle = restoring`): pick the newest ready
        //   snapshot from before the sandbox entered restore. A failed restore
        //   branch can publish a later artifact, but retry pods need the source
        //   state that restore originally started from. If the last placement
        //   predates restore, it still bounds stale prior-cycle snapshots; if
        //   placement moved during restore, it belongs to the retry branch and
        //   must not strand the original source snapshot. When current local
        //   artifact identity exists, legacy markerless snapshots are not
        //   enough proof that the remote row belongs to this restore source.
        // * **Node-drain cohort restore** (`evacuation = true`): require the
        //   selected snapshot to be the **newest** full snapshot for the same
        //   stale-prior-cycle and failed-placement reasons.
        let restore_retry = sandbox.lifecycle.trim().eq_ignore_ascii_case("restoring");
        let snapshot_guard = if restore_retry && !evacuation {
            format!(
                "AND {}",
                restore_retry_source_snapshot_is_current("s", "sn")
            )
        } else {
            let legacy_snapshot_guard = if evacuation {
                no_newer_full_snapshot("s", "sn")
            } else {
                fresh_ready_snapshot_exists("s")
            };
            // The marked branch uses the shared fragment rather than an
            // inline copy: this is the selector that resolves a marked
            // row's restore source, and the retention sweeper's
            // never-prune-the-current-artifact guard is built from the same
            // fragment. Sharing it is what makes "the sweep cannot delete
            // what restore would select" a property instead of a comment.
            let marked_snapshot_guard = snapshot_is_current_local_artifact("s", "sn");
            format!(
                "AND (
                    {marked_snapshot_guard}
                    OR (s.current_local_artifact_id IS NULL AND {legacy_snapshot_guard})
                )"
            )
        };
        // `sql!` is expanded here, not in the shared body: it stamps the
        // statement with *its own* `module_path!()`/`line!()`, which is what
        // attributes an in-flight query in `pg_stat_activity` and DBM back to
        // the code that issued it. Hoisting it into the helper would give both
        // selectors one identical locator and silently move this one.
        let query = sql!(format!(
            "SELECT ssl.storage_connection_string
               FROM sandboxes s
               JOIN snapshots sn ON sn.sandbox_id = s.id
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
              WHERE s.id = $1
                AND s.lifecycle IN ('suspended', 'restoring')
                AND ssl.orphaned_at IS NULL
                AND sn.type = 'full'
                AND sn.state = 'ready'
                {snapshot_guard}
              ORDER BY sn.timestamp DESC
              LIMIT 1"
        ));
        self.fetch_restore_storage_uri(query, sandbox.id, "suspend")
            .await
    }

    /// Run a restore-source selector and validate what it returns.
    ///
    /// Shared by the suspend and stopped selectors, which each keep their own
    /// whole `SELECT`: the storage guidance requires a statement to stay
    /// syntactically whole and reviewable at its query site, so only the
    /// binding and the result processing are factored out here, never a
    /// predicate. The cost is that a filter belonging to both — the
    /// non-orphaned-location requirement, say — has to be fixed in two
    /// places; that is the trade the guidance makes deliberately, in exchange
    /// for each query being readable where it is written.
    ///
    /// `query` arrives already `sql!`-stamped by its caller, so each selector
    /// keeps its own locator token and stays separable in `pg_stat_activity`
    /// and DBM. Do not re-stamp it here.
    ///
    /// `label` names the caller in the two error messages. Parsing the URI is
    /// validation rather than conversion: the caller wants the wire string,
    /// and a value the catalog holds but `StorageUri` rejects is a stored-data
    /// fault worth failing on rather than handing to a node.
    async fn fetch_restore_storage_uri(
        &self,
        query: String,
        sandbox_id: SandboxId,
        label: &str,
    ) -> Result<Option<String>, SubstrateError> {
        let storage_uri: Option<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(query))
            .bind(sandbox_id)
            .fetch_optional(&self.pool)
            .await
            // `db_context_with` rather than `db_context`: the message
            // interpolates the caller's `label`, so it cannot be a
            // `&'static str`.
            .db_context_with(|| format!("failed to select Kubernetes {label} restore URI"))?;
        if let Some(storage_uri) = &storage_uri {
            StorageUri::parse(storage_uri).map_err(|err| SubstrateError::Failed {
                message: format!("invalid selected {label} restore storage URI: {err}"),
            })?;
        }
        Ok(storage_uri)
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
    /// argument. One thing those docs do *not* cover, because it is specific
    /// to disk rows: `SnapshotRetentionDbStore`'s `list_candidates` and
    /// `claim_prune` also filter `type = 'full'`, so a disk row never reaches
    /// the guard at all. The shared spelling is what keeps the property true
    /// if that narrowing is ever lifted — it is not what enforces it now.
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
    /// is a separate value.) So this selector returns `None` for every row that
    /// exists today.
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
        // there. The suspend sibling has no tiebreak and is not changed here:
        // its ordering is load-bearing for a guard that shields every matching
        // row, so narrowing it is a separate decision.
        // Stamped here rather than in the shared body, for the reason the
        // suspend selector's own call site gives.
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
        self.fetch_restore_storage_uri(query, sandbox.id, "stopped")
            .await
    }
}
