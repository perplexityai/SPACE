//! The `sandbox_memory_drops` driver under the reconciler, against a real
//! Postgres and a fake [`SandboxMemoryDropDispatcher`].
//!
//! Pure `retired_memory_plan` matrix coverage lives in `tests/force_restarts.rs`.

use pplx_space_runtime_upgrade_campaign_store::CampaignKind;
use pplx_space_sandbox_store::suspended_environment_pins::{self, SandboxPinState};
use pplx_space_snapshot_model::{ArtifactKind, LocalArtifactId};
use sqlx::PgConnection;

use super::super::retired_memory_edge_plan;
use super::*;

pub(super) enum ScriptedDrop {
    /// Open a Postgres transaction, fence on `lifecycle = 'suspended'`,
    /// `current_local_artifact_id == expected_artifact_id`, source attribution,
    /// and live `source -> target` policy/retirement (`FOR UPDATE`), and
    /// project `expected_artifact_id` onto a `disk` snapshot (`Dropped`),
    /// recording `(campaign_id, sandbox_id)` completion in the same commit.
    CommitDrop,
    /// Commit the fenced `disk` re-pin and `(campaign_id, sandbox_id)`
    /// completion, run any `after_commit` side-effect, and simulate a dropped
    /// call / lost reply by returning `Err(DispatchError::Retryable(reason))`.
    CommitDropThenLoseReply(String),
    /// Project `expected_artifact_id` and insert the completion receipt inside
    /// the transaction, then roll the transaction back and return `Err`, proving
    /// uncommitted work leaves neither a `disk` pin nor a completion receipt.
    ProjectThenRollback(DispatchError),
    AlreadyDropped,
    Sealed(String),
    Held(String),
    Moved(String),
    Ineligible(String),
    Fail(DispatchError),
}

pub(super) type RecordedDropCall = (
    CampaignId,
    Uuid,
    LocalArtifactId,
    EnvironmentReference,
    EnvironmentReference,
);

#[derive(Default)]
struct FakeDropState {
    calls: Vec<RecordedDropCall>,
    committed: Vec<Uuid>,
    cached_campaigns: HashSet<CampaignId>,
    completed_read_errors: HashMap<Uuid, VecDeque<Option<DispatchError>>>,
    pause_completed_false: HashMap<Uuid, (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    script: HashMap<Uuid, VecDeque<ScriptedDrop>>,
    /// Side-effect run inside `drop_memory` before sealing (for example,
    /// rotating the sandbox's pin, changing its lifecycle, or shifting policy).
    before_answer: HashMap<Uuid, PinSideEffect>,
    /// Side-effect run inside `CommitDropThenLoseReply` after committing the
    /// `disk` projection and before returning `Err(DispatchError::Retryable)`.
    after_commit: HashMap<Uuid, PinSideEffect>,
}

#[derive(Clone, Copy)]
pub(super) enum PinSideEffect {
    ProjectToDisk(EnvironmentReference),
    RotateFullPin(EnvironmentReference),
    SetLifecycle(&'static str),
    SetDefaultEnvironment(EnvironmentReference),
    SetLegacyUnboundAllowed(bool),
}

enum SealOutcome {
    Projected,
    AlreadyDropped,
    Moved(&'static str),
    Held(&'static str),
}

/// Fake [`SandboxMemoryDropDispatcher`] that fences on `expected_artifact_id`,
/// `source` attribution, and `source -> target` policy/retirement under
/// `SELECT ... FOR UPDATE`, projects a dropped sandbox's pin onto a `disk`
/// snapshot, and records durable `(campaign_id, sandbox_id)` completion with
/// the commit.
#[derive(Clone)]
pub(super) struct FakeDropDispatcher {
    pool: PgPool,
    retirement: Option<FakeMemoryRetirement>,
    receipts_table: Arc<tokio::sync::OnceCell<()>>,
    state: Arc<Mutex<FakeDropState>>,
}

impl FakeDropDispatcher {
    pub(super) fn new(pool: &PgPool) -> Self {
        Self {
            pool: pool.clone(),
            retirement: None,
            receipts_table: Arc::new(tokio::sync::OnceCell::new()),
            state: Arc::new(Mutex::new(FakeDropState::default())),
        }
    }

    async fn ensure_receipts_table(&self) {
        self.receipts_table
            .get_or_init(|| async {
                sqlx::query(sql!(
                    "CREATE TABLE IF NOT EXISTS test_memory_drop_receipts (
                         campaign_id UUID NOT NULL,
                         sandbox_id UUID NOT NULL,
                         PRIMARY KEY (campaign_id, sandbox_id)
                     )"
                ))
                .execute(&self.pool)
                .await
                .expect("creating test_memory_drop_receipts");
            })
            .await;
    }

    pub(super) fn with_retirement(mut self, retirement: FakeMemoryRetirement) -> Self {
        self.retirement = Some(retirement);
        self
    }

    fn state(&self) -> std::sync::MutexGuard<'_, FakeDropState> {
        self.state.lock().unwrap()
    }

    pub(super) fn script(&self, sandbox: Uuid, results: Vec<ScriptedDrop>) {
        self.state().script.insert(sandbox, results.into());
    }

    pub(super) fn script_completed_errors(
        &self,
        sandbox: Uuid,
        errors: Vec<Option<DispatchError>>,
    ) {
        self.state()
            .completed_read_errors
            .insert(sandbox, errors.into());
    }

    pub(super) fn pause_completed_false_once(
        &self,
        sandbox: Uuid,
    ) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        let reached = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        self.state()
            .pause_completed_false
            .insert(sandbox, (Arc::clone(&reached), Arc::clone(&resume)));
        (reached, resume)
    }

    pub(super) fn before_answer(&self, sandbox: Uuid, effect: PinSideEffect) {
        self.state().before_answer.insert(sandbox, effect);
    }

    pub(super) fn after_commit(&self, sandbox: Uuid, effect: PinSideEffect) {
        self.state().after_commit.insert(sandbox, effect);
    }

    pub(super) fn calls(&self) -> Vec<(Uuid, LocalArtifactId)> {
        self.state()
            .calls
            .iter()
            .map(|(_, id, art, _, _)| (*id, *art))
            .collect()
    }

    pub(super) fn full_calls(&self) -> Vec<RecordedDropCall> {
        self.state().calls.clone()
    }

    pub(super) fn called_sandboxes(&self) -> Vec<Uuid> {
        self.calls().into_iter().map(|(id, _)| id).collect()
    }

    pub(super) fn dropped_sandboxes(&self) -> Vec<Uuid> {
        let mut ids = self.state().committed.clone();
        ids.sort();
        ids.dedup();
        ids
    }

    pub(super) fn committed_count(&self, sandbox: Uuid) -> usize {
        self.state()
            .committed
            .iter()
            .filter(|id| **id == sandbox)
            .count()
    }

    pub(super) fn tracked_completed_campaigns(&self) -> Vec<CampaignId> {
        let mut entries: Vec<_> = self.state().cached_campaigns.iter().copied().collect();
        entries.sort_by_key(ToString::to_string);
        entries
    }

    async fn apply_side_effect(&self, sandbox: Uuid, effect: PinSideEffect) {
        match effect {
            PinSideEffect::ProjectToDisk(env) => {
                repin_to_snapshot(&self.pool, sandbox, "disk", env)
                    .await
                    .expect("re-pinning to disk snapshot");
            }
            PinSideEffect::RotateFullPin(env) => {
                repin_to_snapshot(&self.pool, sandbox, "full", env)
                    .await
                    .expect("rotating full checkpoint pin");
            }
            PinSideEffect::SetLifecycle(lifecycle) => {
                sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
                    .bind(sandbox)
                    .bind(lifecycle)
                    .execute(&self.pool)
                    .await
                    .expect("updating sandbox lifecycle");
            }
            PinSideEffect::SetDefaultEnvironment(env) => {
                let mut tx = self.pool.begin().await.expect("begin policy tx");
                pplx_space_runtime_environment_policy_store::set_default(&mut tx, env, &change())
                    .await
                    .expect("setting default environment");
                tx.commit().await.expect("commit policy tx");
            }
            PinSideEffect::SetLegacyUnboundAllowed(allowed) => {
                let mut tx = self.pool.begin().await.expect("begin policy tx");
                pplx_space_runtime_environment_policy_store::set_legacy_unbound_allowed(
                    &mut tx,
                    allowed,
                    &change(),
                )
                .await
                .expect("setting legacy_unbound_allowed");
                tx.commit().await.expect("commit policy tx");
            }
        }
    }
}

impl SandboxMemoryDropDispatcher for FakeDropDispatcher {
    async fn drop_memory(
        &self,
        campaign_id: CampaignId,
        sandbox_id: SandboxId,
        expected_artifact_id: LocalArtifactId,
        source: EnvironmentReference,
        target: EnvironmentReference,
    ) -> Result<SandboxMemoryDrop, DispatchError> {
        let sandbox = sandbox_id.as_uuid();
        let (scripted, before_effect, after_effect) = {
            let mut state = self.state();
            state
                .calls
                .push((campaign_id, sandbox, expected_artifact_id, source, target));
            let scripted = state
                .script
                .get_mut(&sandbox)
                .and_then(VecDeque::pop_front)
                .unwrap_or(ScriptedDrop::CommitDrop);
            let before_effect = state.before_answer.remove(&sandbox);
            let after_effect = state.after_commit.remove(&sandbox);
            (scripted, before_effect, after_effect)
        };
        if let Some(effect) = before_effect {
            self.apply_side_effect(sandbox, effect).await;
        }
        match scripted {
            ScriptedDrop::CommitDrop => {
                self.ensure_receipts_table().await;
                let mut tx = self.pool.begin().await.expect("begin drop tx");
                match self
                    .seal_and_project_expected_pin(
                        &mut tx,
                        campaign_id,
                        sandbox,
                        expected_artifact_id,
                        source,
                        target,
                    )
                    .await
                    .expect("fenced disk projection")
                {
                    SealOutcome::Projected => {
                        tx.commit().await.expect("commit drop tx");
                        let mut state = self.state();
                        state.committed.push(sandbox);
                        state.cached_campaigns.insert(campaign_id);
                        Ok(SandboxMemoryDrop::Dropped)
                    }
                    SealOutcome::AlreadyDropped => {
                        tx.rollback().await.expect("rollback already-dropped pin");
                        Ok(SandboxMemoryDrop::AlreadyDropped)
                    }
                    SealOutcome::Moved(reason) => {
                        tx.rollback().await.expect("rollback moved pin");
                        Ok(SandboxMemoryDrop::Moved(reason.to_owned()))
                    }
                    SealOutcome::Held(reason) => {
                        tx.rollback().await.expect("rollback held pin");
                        Ok(SandboxMemoryDrop::Held(reason.to_owned()))
                    }
                }
            }
            ScriptedDrop::CommitDropThenLoseReply(reason) => {
                self.ensure_receipts_table().await;
                let mut tx = self.pool.begin().await.expect("begin drop tx");
                assert!(
                    matches!(
                        self.seal_and_project_expected_pin(
                            &mut tx,
                            campaign_id,
                            sandbox,
                            expected_artifact_id,
                            source,
                            target,
                        )
                        .await
                        .expect("fenced disk projection"),
                        SealOutcome::Projected
                    ),
                    "expected_artifact_id still current when CommitDropThenLoseReply commits"
                );
                tx.commit().await.expect("commit drop tx");
                {
                    let mut state = self.state();
                    state.committed.push(sandbox);
                    state.cached_campaigns.insert(campaign_id);
                }
                if let Some(effect) = after_effect {
                    self.apply_side_effect(sandbox, effect).await;
                }
                Err(DispatchError::Retryable(reason))
            }
            ScriptedDrop::ProjectThenRollback(error) => {
                self.ensure_receipts_table().await;
                let mut tx = self.pool.begin().await.expect("begin drop tx");
                assert!(
                    matches!(
                        self.seal_and_project_expected_pin(
                            &mut tx,
                            campaign_id,
                            sandbox,
                            expected_artifact_id,
                            source,
                            target,
                        )
                        .await
                        .expect("fenced disk projection"),
                        SealOutcome::Projected
                    ),
                    "expected_artifact_id still current when ProjectThenRollback stages drop"
                );
                tx.rollback().await.expect("rollback staged drop tx");
                Err(error)
            }
            ScriptedDrop::AlreadyDropped => {
                self.ensure_receipts_table().await;
                sqlx::query(sql!(
                    "INSERT INTO test_memory_drop_receipts (campaign_id, sandbox_id)
                     VALUES ($1, $2)
                     ON CONFLICT DO NOTHING"
                ))
                .bind(campaign_id)
                .bind(sandbox)
                .execute(&self.pool)
                .await
                .expect("recording AlreadyDropped receipt");
                self.state().cached_campaigns.insert(campaign_id);
                Ok(SandboxMemoryDrop::AlreadyDropped)
            }
            ScriptedDrop::Sealed(reason) => Ok(SandboxMemoryDrop::Sealed(reason)),
            ScriptedDrop::Held(reason) => Ok(SandboxMemoryDrop::Held(reason)),
            ScriptedDrop::Moved(reason) => Ok(SandboxMemoryDrop::Moved(reason)),
            ScriptedDrop::Ineligible(reason) => Ok(SandboxMemoryDrop::Ineligible(reason)),
            ScriptedDrop::Fail(error) => Err(error),
        }
    }

    async fn memory_drop_completed(
        &self,
        campaign_id: CampaignId,
        sandbox_id: SandboxId,
    ) -> Result<bool, DispatchError> {
        let sandbox = sandbox_id.as_uuid();
        if let Some(Some(error)) = self
            .state()
            .completed_read_errors
            .get_mut(&sandbox)
            .and_then(VecDeque::pop_front)
        {
            return Err(error);
        }
        self.ensure_receipts_table().await;
        let completed: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS(
                 SELECT 1
                 FROM test_memory_drop_receipts
                 WHERE campaign_id = $1 AND sandbox_id = $2
             )"
        ))
        .bind(campaign_id)
        .bind(sandbox)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| DispatchError::Retryable(format!("reading test receipt: {error}")))?;
        let pause = if !completed {
            self.state().pause_completed_false.remove(&sandbox)
        } else {
            None
        };
        if let Some((reached, resume)) = pause {
            reached.notify_one();
            resume.notified().await;
        }
        Ok(completed)
    }

    fn retain_memory_drop_campaigns(&self, unfinished: &[CampaignId]) {
        self.state()
            .cached_campaigns
            .retain(|campaign_id| unfinished.contains(campaign_id));
    }
}

pub(super) fn memory_drop_seed() -> CampaignSeed {
    CampaignSeed {
        kind: CampaignKind::SandboxMemoryDrops,
        from: source(),
        to: target(),
        per_cluster: 5,
        per_node: 1,
        failure_budget: 5,
        ..CampaignSeed::default()
    }
}

pub(super) fn drop_reconciler(
    pool: &PgPool,
    dispatcher: FakeDropDispatcher,
    retirement: FakeMemoryRetirement,
    page: u32,
) -> Reconciler {
    Reconciler::new(
        pool.clone(),
        Drivers::default().with(SandboxMemoryDropDriver::new(
            pool.clone(),
            K8S,
            dispatcher.with_retirement(retirement.clone()),
            retirement,
        )),
        ReconcilerConfig {
            enabled: true,
            candidate_page: page,
            ..ReconcilerConfig::default()
        },
    )
}

pub(super) async fn insert_suspended_pinned_with_published(
    pool: &PgPool,
    sandbox_id: Uuid,
    kind: &str,
    published: Published,
    observed_node: Option<NodeId>,
) -> TestResult<Uuid> {
    let artifact = LocalArtifactId::new();
    let origin = if kind == "disk" { "stop" } else { "suspend" };
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             current_local_artifact_id, current_local_artifact_kind,
             current_local_artifact_origin,
             placement_changed_at, network_policy_generation
         ) VALUES (
             $1, $2, $3, $4, 'suspended', 'unplaced',
             $5, $6,
             $7,
             NOW() - INTERVAL '1 hour', 1
         )"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(K8S)
    .bind(artifact)
    .bind(kind)
    .bind(origin)
    .execute(pool)
    .await?;

    let (metadata, runtime_version, state_format_version) = match published {
        Published::Bound(environment) => (
            Some(serde_json::json!({ "environment": environment.to_string() })),
            None,
            None,
        ),
        Published::Unbound {
            runtime_version,
            kernel,
            state_format_version,
        } => (
            Some(serde_json::json!({ "components": [{ "kind": "kernel", "version": kernel }] })),
            Some(runtime_version.to_owned()),
            Some(state_format_version.to_owned()),
        ),
        Published::PreMetadata { kernel } => (
            Some(serde_json::json!({ "components": [{ "kind": "kernel", "version": kernel }] })),
            None,
            None,
        ),
        Published::Nothing => (None, None, None),
    };
    let mut conn = pool.acquire().await?;
    insert_pinned_snapshot_row_with_metadata(
        &mut conn,
        sandbox_id,
        artifact,
        kind,
        metadata.clone(),
        Some("firecracker".to_owned()),
        runtime_version,
        Some("firecracker-snapshot".to_owned()),
        state_format_version,
    )
    .await?;

    if let Some(node_id) = observed_node {
        sqlx::query(sql!(
            "INSERT INTO sandbox_observed_states (
                 sandbox_id, node_id, runtime, state_format_name, execution_metadata
             )
             VALUES ($1, $2, 'firecracker', 'firecracker-snapshot', $3)"
        ))
        .bind(sandbox_id)
        .bind(node_id)
        .bind(metadata)
        .execute(pool)
        .await?;
    }
    Ok(sandbox_id)
}

pub(super) async fn insert_suspended_pinned_with_id(
    pool: &PgPool,
    sandbox_id: Uuid,
    kind: &str,
    environment: EnvironmentReference,
    observed_node: Option<NodeId>,
) -> TestResult<Uuid> {
    insert_suspended_pinned_with_published(
        pool,
        sandbox_id,
        kind,
        Published::Bound(environment),
        observed_node,
    )
    .await
}

pub(super) async fn suspended_on_source(pool: &PgPool) -> TestResult<Uuid> {
    insert_suspended_pinned_with_id(pool, Uuid::now_v7(), "full", source(), None).await
}

impl FakeDropDispatcher {
    async fn seal_and_project_expected_pin(
        &self,
        conn: &mut PgConnection,
        campaign_id: CampaignId,
        sandbox_id: Uuid,
        expected_artifact_id: LocalArtifactId,
        source: EnvironmentReference,
        target: EnvironmentReference,
    ) -> TestResult<SealOutcome> {
        let locked: Option<(String, Option<LocalArtifactId>)> = sqlx::query_as(sql!(
            "SELECT lifecycle, current_local_artifact_id
             FROM sandboxes
             WHERE id = $1
             FOR UPDATE"
        ))
        .bind(sandbox_id)
        .fetch_optional(&mut *conn)
        .await?;
        let already_completed: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS(
                 SELECT 1
                 FROM test_memory_drop_receipts
                 WHERE campaign_id = $1 AND sandbox_id = $2
             )"
        ))
        .bind(campaign_id)
        .bind(sandbox_id)
        .fetch_one(&mut *conn)
        .await?;
        if already_completed {
            return Ok(SealOutcome::AlreadyDropped);
        }
        let Some((lifecycle, Some(current_artifact_id))) = locked else {
            return Ok(SealOutcome::Moved("checkpoint pin rotated before sealing"));
        };
        if lifecycle != "suspended" || current_artifact_id != expected_artifact_id {
            return Ok(SealOutcome::Moved("checkpoint pin rotated before sealing"));
        }
        let pin = suspended_environment_pins::pin_state(
            &mut *conn,
            K8S,
            SandboxId::from_uuid(sandbox_id),
        )
        .await?;
        let SandboxPinState::SuspendedPinned {
            kind: ArtifactKind::Full,
            artifact_id,
            execution,
        } = pin
        else {
            return Ok(SealOutcome::Moved("checkpoint pin rotated before sealing"));
        };
        if artifact_id != expected_artifact_id {
            return Ok(SealOutcome::Moved("checkpoint pin rotated before sealing"));
        }
        let memory_retired = match self.retirement.as_ref() {
            Some(retirement) => {
                retirement
                    .memory_retirement(source)
                    .await
                    .expect("reading retirement at seal")
                    .retired
            }
            None => true,
        };
        let inputs = Inputs::read(&self.pool)
            .await
            .expect("reading inputs at seal");
        if inputs.attributed_checkpoint(&execution) != Some(source) {
            return Ok(SealOutcome::Moved(
                "checkpoint no longer attributes to campaign source",
            ));
        }
        if retired_memory_edge_plan(
            &inputs.policy,
            inputs.catalog_entry(source),
            memory_retired,
            source,
            target,
        ) != (RetiredMemoryPlan::Scan { moves: true })
        {
            return Ok(SealOutcome::Held(
                "policy no longer permits dropping memory for campaign target",
            ));
        }
        repin_to_snapshot_in_tx(&mut *conn, sandbox_id, "disk", source).await?;
        sqlx::query(sql!(
            "INSERT INTO test_memory_drop_receipts (campaign_id, sandbox_id)
             VALUES ($1, $2)"
        ))
        .bind(campaign_id)
        .bind(sandbox_id)
        .execute(&mut *conn)
        .await?;
        Ok(SealOutcome::Projected)
    }
}

pub(super) async fn repin_to_snapshot(
    pool: &PgPool,
    sandbox_id: Uuid,
    kind: &str,
    environment: EnvironmentReference,
) -> TestResult<LocalArtifactId> {
    let mut tx = pool.begin().await?;
    let artifact = repin_to_snapshot_in_tx(&mut tx, sandbox_id, kind, environment).await?;
    tx.commit().await?;
    Ok(artifact)
}

async fn repin_to_snapshot_in_tx(
    conn: &mut PgConnection,
    sandbox_id: Uuid,
    kind: &str,
    environment: EnvironmentReference,
) -> TestResult<LocalArtifactId> {
    let artifact = LocalArtifactId::new();
    let origin = if kind == "disk" { "stop" } else { "suspend" };
    sqlx::query(sql!("SELECT 1 FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(sandbox_id)
        .fetch_optional(&mut *conn)
        .await?;
    // When projecting to `"disk"`, preserve the sandbox's existing pinned
    // checkpoint execution columns (including unbound `E0` or pre-metadata rows
    // with `environment: None`), matching offline `MemoryDrop::drop_memory`.
    let preserved = if kind == "disk" {
        sqlx::query_as::<
            _,
            (
                Option<sqlx::types::Json<serde_json::Value>>,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<String>,
            ),
        >(sql!(
            "SELECT sn.execution_metadata, sn.runtime, sn.runtime_version,
                    sn.state_format_name, sn.state_format_version
             FROM sandboxes sb
             JOIN snapshots sn
               ON sn.sandbox_id = sb.id
              AND sn.local_artifact_id = sb.current_local_artifact_id
              AND sn.type = sb.current_local_artifact_kind
             WHERE sb.id = $1
             ORDER BY sn.timestamp DESC, sn.id DESC
             LIMIT 1"
        ))
        .bind(sandbox_id)
        .fetch_optional(&mut *conn)
        .await?
    } else {
        None
    };
    let (metadata, runtime, runtime_version, state_format_name, state_format_version) =
        match preserved {
            Some((meta, runtime, runtime_version, state_format_name, state_format_version)) => (
                meta.map(|json| json.0),
                runtime,
                runtime_version,
                state_format_name,
                state_format_version,
            ),
            None => (
                Some(serde_json::json!({ "environment": environment.to_string() })),
                Some("firecracker".to_owned()),
                None,
                Some("firecracker-snapshot".to_owned()),
                None,
            ),
        };
    insert_pinned_snapshot_row_with_metadata(
        &mut *conn,
        sandbox_id,
        artifact,
        kind,
        metadata,
        runtime,
        runtime_version,
        state_format_name,
        state_format_version,
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET current_local_artifact_id = $2,
             current_local_artifact_kind = $3,
             current_local_artifact_origin = $4
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(artifact)
    .bind(kind)
    .bind(origin)
    .execute(&mut *conn)
    .await?;
    Ok(artifact)
}

#[allow(clippy::too_many_arguments)]
async fn insert_pinned_snapshot_row_with_metadata(
    conn: &mut PgConnection,
    sandbox_id: Uuid,
    artifact: LocalArtifactId,
    kind: &str,
    execution_metadata: Option<serde_json::Value>,
    runtime: Option<String>,
    runtime_version: Option<String>,
    state_format_name: Option<String>,
    state_format_version: Option<String>,
) -> TestResult {
    let origin = if kind == "disk" { "stop" } else { "suspend" };
    let (snapshot_id,): (Uuid,) = sqlx::query_as(sql!(
        "INSERT INTO snapshots (
             sandbox_id, type, state, timestamp,
             local_artifact_id, artifact_kind, artifact_origin, execution_metadata,
             runtime, runtime_version, state_format_name, state_format_version
         ) VALUES (
             $1, $2, 'ready', NOW() - INTERVAL '30 seconds',
             $3, $4, $5, $6,
             $7, $8, $9, $10
         )
         RETURNING id"
    ))
    .bind(sandbox_id)
    .bind(kind)
    .bind(artifact)
    .bind(kind)
    .bind(origin)
    .bind(execution_metadata)
    .bind(runtime)
    .bind(runtime_version)
    .bind(state_format_name)
    .bind(state_format_version)
    .fetch_one(&mut *conn)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, 's3://synthetic-bucket/checkpoint')"
    ))
    .bind(snapshot_id)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn holding_blocks_scans_and_unretiring_mid_pass_resets_the_cursor() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, memory_drop_seed()).await?;

    let mid =
        insert_suspended_pinned_with_id(&db.pool, Uuid::from_u128(200), "full", source(), None)
            .await?;
    let late =
        insert_suspended_pinned_with_id(&db.pool, Uuid::from_u128(300), "full", source(), None)
            .await?;

    let dispatcher = FakeDropDispatcher::new(&db.pool);
    let retirement = FakeMemoryRetirement::default();
    let reconciler = drop_reconciler(&db.pool, dispatcher.clone(), retirement.clone(), 1);

    // Held while memory_retired is false: nothing scanned, nothing leased.
    tick(&reconciler).await;
    assert_eq!(counts(&db.pool, campaign).await?, ItemCounts::default());
    assert!(dispatcher.calls().is_empty());

    // Retire source memory and run one 1-row page: `mid` is dropped and the
    // pass cursor stands at `Some(mid)` with `late` and `tail` still ahead of
    // it.
    let tail =
        insert_suspended_pinned_with_id(&db.pool, Uuid::from_u128(400), "full", source(), None)
            .await?;
    retirement.retire(source());
    tick(&reconciler).await;
    assert_eq!(dispatcher.dropped_sandboxes(), vec![mid]);
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "rolling");

    // Registering a catalog environment (`environment(2)`) changes
    // `Inputs::fingerprint` (since `attribute`'s `only_match` depends on the
    // unretired catalog), so the open pass cursor resets from `Some(mid)` back
    // to `None` and picks up `early` (< `mid`) on the first page.
    let early =
        insert_suspended_pinned_with_id(&db.pool, Uuid::from_u128(100), "full", source(), None)
            .await?;
    sqlx::query(sql!(
        "INSERT INTO runtime_environments (
             environment_ref, runtime, runtime_version, version_sort_key,
             kernel_version, state_format_name, state_format_version, upgrade_edges
         )
         VALUES ($1, 'firecracker', '1.18.0', '{2}', '6.1.155', 'firecracker-snapshot', '13.0.0', $2)"
    ))
    .bind(environment(2).to_string())
    .bind(Vec::<String>::new())
    .execute(&db.pool)
    .await?;
    tick(&reconciler).await;
    assert_eq!(
        dispatcher.called_sandboxes(),
        vec![mid, early],
        "a catalog change alters `Inputs::fingerprint` and resets the open pass cursor back to `early`"
    );

    // The open pass cursor now stands at `Some(early)` (`mid` was already
    // re-pinned to `'disk'`), with `late` and `tail` ahead of it while
    // `earlier` (< `early`) is behind it. Both a mid-tick `SubjectScan::PlanChanged`
    // and a held tick clear that half-scanned pass cursor so re-retiring scans
    // from the start (`earlier`) instead of skipping past it.
    let earlier =
        insert_suspended_pinned_with_id(&db.pool, Uuid::from_u128(75), "full", source(), None)
            .await?;
    let campaign_row = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|row| row.id == campaign)
        .expect("campaign stays unfinished");
    let driver =
        SandboxMemoryDropDriver::new(db.pool.clone(), K8S, dispatcher.clone(), retirement.clone());
    let Plan::Scan(scan_plan) = driver.plan(&campaign_row).await? else {
        panic!("retired source memory plans a scan");
    };
    // A memory-retirement hold or policy shift landing while `candidates_page`
    // is running (after `find_subjects`'s initial `read_plan` matched
    // `scan_plan`) is caught by the post-query `read_plan` check so a short
    // page can't end the pass under stale attribution inputs.
    retirement.script_reads([Ok(true), Ok(false)]);
    assert!(matches!(
        driver
            .find_subjects(&campaign_row, scan_plan, Some(early), 1)
            .await?,
        SubjectScan::PlanChanged
    ));
    let post_query_generation_shift = SandboxMemoryDropDriver::new(
        db.pool.clone(),
        K8S,
        dispatcher.clone(),
        BumpGenerationAfterFirstRead {
            reads: Arc::new(Mutex::new(0)),
            inner: retirement.clone(),
        },
    );
    assert!(matches!(
        post_query_generation_shift
            .find_subjects(&campaign_row, scan_plan, Some(early), 1)
            .await?,
        SubjectScan::PlanChanged
    ));
    let Plan::Scan(scan_plan) = driver.plan(&campaign_row).await? else {
        panic!("updated retirement generation plans a scan");
    };
    retirement.unretire(source());
    assert!(matches!(
        driver
            .find_subjects(&campaign_row, scan_plan, Some(early), 1)
            .await?,
        SubjectScan::PlanChanged
    ));
    tick(&reconciler).await;
    assert_eq!(dispatcher.dropped_sandboxes(), vec![early, mid]);

    retirement.retire(source());
    tick(&reconciler).await;
    assert_eq!(
        dispatcher.called_sandboxes(),
        vec![mid, early, earlier],
        "the held tick reset the pass cursor, so `earlier` (< `early`) is found on the first page"
    );

    // An unretire + re-retire cycle wholly between ticks (no held tick observed)
    // still advances `MemoryRetirement.generation`, changing `ScanPlan` and
    // resetting the open pass cursor from `Some(earlier)` back to `None` so
    // `earliest` (< `earlier`) isn't missed before completion.
    retirement.unretire(source());
    let earliest =
        insert_suspended_pinned_with_id(&db.pool, Uuid::from_u128(50), "full", source(), None)
            .await?;
    retirement.retire(source());
    tick(&reconciler).await;
    assert_eq!(
        dispatcher.called_sandboxes(),
        vec![mid, early, earlier, earliest],
        "advancing `MemoryRetirement.generation` between ticks resets the open pass cursor"
    );

    tick_until_finished(&reconciler, &db.pool, campaign, 6).await?;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert_eq!(
        dispatcher.dropped_sandboxes(),
        vec![earliest, earlier, early, mid, late, tail]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stranded_sandboxes_stay_pending_without_leasing_until_an_edge_is_certified() -> TestResult
{
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, false).await?;
    let campaign = insert_campaign(&db.pool, memory_drop_seed()).await?;
    let first = suspended_on_source(&db.pool).await?;
    let second = suspended_on_source(&db.pool).await?;

    let dispatcher = FakeDropDispatcher::new(&db.pool);
    let retirement = FakeMemoryRetirement::retired(source());
    let reconciler = drop_reconciler(&db.pool, dispatcher.clone(), retirement, 100);

    tick(&reconciler).await;
    tick(&reconciler).await;

    assert_eq!(campaign_phase(&db.pool, campaign).await?, "rolling");
    assert_eq!(
        counts(&db.pool, campaign).await?,
        ItemCounts {
            pending: 2,
            ..ItemCounts::default()
        }
    );
    assert!(dispatcher.calls().is_empty());

    // Certify the upgrade edge to `target()`; the stranded pending rows lease,
    // drop to disk projections, and complete the campaign.
    sqlx::query(sql!(
        "UPDATE runtime_environments SET upgrade_edges = $2 WHERE environment_ref = $1"
    ))
    .bind(source().to_string())
    .bind(vec![target().to_string()])
    .execute(&db.pool)
    .await?;

    tick_until_finished(&reconciler, &db.pool, campaign, 5).await?;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    let mut expected = vec![first, second];
    expected.sort();
    assert_eq!(dispatcher.dropped_sandboxes(), expected);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn upgrade_maps_outcomes_fences_expected_artifact_and_rescans_until_source_memory_is_zero()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    {
        let mut tx = db.pool.begin().await?;
        pplx_space_runtime_environment_policy_store::set_legacy_unbound_allowed(
            &mut tx,
            false,
            &change(),
        )
        .await?;
        tx.commit().await?;
    }
    let mut seed = memory_drop_seed();
    seed.per_cluster = 16;
    seed.failure_budget = 1;
    let campaign = insert_campaign(&db.pool, seed).await?;

    // 1. Lapsed-lease retry: already re-pinned to a `source()` `disk` snapshot
    //    (including an unbound `E0` disk projection) before `upgrade` runs ->
    //    `done` with zero `drop_memory` calls.
    let already_disk_pinned = insert_suspended_pinned_with_published(
        &db.pool,
        Uuid::now_v7(),
        "disk",
        Published::Unbound {
            runtime_version: "1.15.1",
            kernel: "6.1.155",
            state_format_version: "9.0.0",
        },
        None,
    )
    .await?;
    // 2. Pre-dispatch moved off `source()` -> `skipped` with zero `drop_memory` calls.
    let moved_before_dispatch =
        insert_suspended_pinned_with_id(&db.pool, Uuid::now_v7(), "full", target(), None).await?;
    // 2b. Mid-transition (`restoring`) before dispatch -> `Release` (0 attempts
    //     spent, zero `drop_memory` calls), then once it settles back to
    //     `suspended` on `source()` it's dropped cleanly.
    let restoring_before_dispatch = suspended_on_source(&db.pool).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = $1"
    ))
    .bind(restoring_before_dispatch)
    .execute(&db.pool)
    .await?;
    record(
        &db.pool,
        campaign,
        &[
            already_disk_pinned,
            moved_before_dispatch,
            restoring_before_dispatch,
        ],
    )
    .await?;

    // 3. `AlreadyDropped` -> `done` (dispatcher sees concurrent drop; re-pins to disk).
    let already_dropped = suspended_on_source(&db.pool).await?;
    // 4. `Sealed` -> `Release` (0 attempts spent), then `Dropped` on next tick.
    let sealed_then_dropped = suspended_on_source(&db.pool).await?;
    // 5. Pin rotated to a new `source()` `full` checkpoint `B` before sealing ->
    //    `expected_artifact_id` fence returns `Moved` without overwriting `B`,
    //    `inspect_pin` releases (`attempts == 0`), and the next lease passes `B`
    //    and drops it cleanly.
    let rotated_on_source = suspended_on_source(&db.pool).await?;
    let SandboxPinState::SuspendedPinned {
        artifact_id: initial_rotated_source_art,
        ..
    } = suspended_environment_pins::pin_state(
        &db.pool,
        K8S,
        SandboxId::from_uuid(rotated_on_source),
    )
    .await?
    else {
        panic!("rotated_on_source starts full-pinned");
    };
    // 5b. `Moved` while mid-transition (`restoring`) -> `Release` (0 attempts), then `Skipped` once started.
    let moved_into_restoring = suspended_on_source(&db.pool).await?;
    // 6. Pin rotated to a new `target()` `full` checkpoint `B` before sealing ->
    //    `expected_artifact_id` fence leaves `target()`'s memory checkpoint `B`
    //    untouched (`Full`), and `inspect_pin` marks the item `Skipped`.
    let rotated_to_target = suspended_on_source(&db.pool).await?;
    // 7. `Moved` after a concurrent drop projected `disk` on `source()` -> `Done`.
    let moved_to_disk = suspended_on_source(&db.pool).await?;
    // 8. `Retryable` error -> `Retry` (1 attempt), then `Dropped` (`attempts == 2`).
    let transient_error = suspended_on_source(&db.pool).await?;
    // 8b. Dropped call / lost reply on the final allowed lease (`MAX_ATTEMPTS`)
    //     after `CommitDropThenLoseReply` already committed the `disk` re-pin ->
    //     `inspect_pin` on the `Retryable` path sees `ArtifactKind::Disk`
    //     and finishes `Done` (`attempts == MAX_ATTEMPTS`) instead of recording
    //     a terminal `Failed` outcome against `failure_budget`.
    let lost_reply_after_commit = suspended_on_source(&db.pool).await?;
    // 9. Unbound `E0` (`{components}` without `environment`) and pre-metadata `E0`
    //    (`execution_metadata IS NULL` with `legacy_unbound_allowed = Some(false)`)
    //    full checkpoints are discovered and dropped, while an unbound `target()`
    //    checkpoint is omitted without blocking completion.
    let unbound_e0 = insert_suspended_pinned_with_published(
        &db.pool,
        Uuid::now_v7(),
        "full",
        Published::Unbound {
            runtime_version: "1.15.1",
            kernel: "6.1.155",
            state_format_version: "9.0.0",
        },
        None,
    )
    .await?;
    let pre_metadata_e0 = insert_suspended_pinned_with_published(
        &db.pool,
        Uuid::now_v7(),
        "full",
        Published::Nothing,
        None,
    )
    .await?;
    let unbound_target = insert_suspended_pinned_with_published(
        &db.pool,
        Uuid::now_v7(),
        "full",
        Published::Unbound {
            runtime_version: "1.17.0",
            kernel: "6.1.155",
            state_format_version: "12.0.0",
        },
        None,
    )
    .await?;

    let dispatcher = FakeDropDispatcher::new(&db.pool);
    dispatcher.script(already_dropped, vec![ScriptedDrop::AlreadyDropped]);
    dispatcher.before_answer(already_dropped, PinSideEffect::ProjectToDisk(source()));

    dispatcher.script(
        sealed_then_dropped,
        vec![
            ScriptedDrop::Sealed("suspend sealing".to_owned()),
            ScriptedDrop::CommitDrop,
        ],
    );
    dispatcher.before_answer(rotated_on_source, PinSideEffect::RotateFullPin(source()));

    dispatcher.script(
        moved_into_restoring,
        vec![ScriptedDrop::Moved("waking into restoring".to_owned())],
    );
    dispatcher.before_answer(
        moved_into_restoring,
        PinSideEffect::SetLifecycle("restoring"),
    );

    dispatcher.before_answer(rotated_to_target, PinSideEffect::RotateFullPin(target()));

    dispatcher.script(
        moved_to_disk,
        vec![ScriptedDrop::Moved("concurrent drop committed".to_owned())],
    );
    dispatcher.before_answer(moved_to_disk, PinSideEffect::ProjectToDisk(source()));

    dispatcher.script(
        transient_error,
        vec![
            ScriptedDrop::Fail(DispatchError::Retryable("s3 timeout".to_owned())),
            ScriptedDrop::CommitDrop,
        ],
    );
    let mut final_lease_lost_reply: Vec<ScriptedDrop> = (1..MAX_ATTEMPTS)
        .map(|_| ScriptedDrop::Fail(DispatchError::Retryable("s3 timeout".to_owned())))
        .collect();
    final_lease_lost_reply.push(ScriptedDrop::CommitDropThenLoseReply(
        "lost reply after commit".to_owned(),
    ));
    dispatcher.script(lost_reply_after_commit, final_lease_lost_reply);

    let retirement = FakeMemoryRetirement::retired(source());
    let reconciler = drop_reconciler(&db.pool, dispatcher.clone(), retirement.clone(), 100);

    // Tick 1 scans, leases, and runs the first wave.
    tick(&reconciler).await;

    assert!(
        !dispatcher.called_sandboxes().contains(&already_disk_pinned),
        "a row already re-pinned to a source disk projection finishes done without calling drop_memory"
    );
    assert_eq!(
        item(&db.pool, campaign, already_disk_pinned).await?,
        ("done".to_owned(), None, 1)
    );
    assert!(
        !dispatcher
            .called_sandboxes()
            .contains(&moved_before_dispatch)
    );
    assert_eq!(
        item(&db.pool, campaign, moved_before_dispatch).await?.0,
        "skipped"
    );
    assert_eq!(
        item(&db.pool, campaign, restoring_before_dispatch).await?,
        (
            "pending".to_owned(),
            Some("the sandbox is restoring; retrying once it settles".to_owned()),
            0
        ),
        "a mid-transition lifecycle before dispatch releases without spending an attempt"
    );
    assert_eq!(
        item(&db.pool, campaign, moved_into_restoring).await?,
        (
            "pending".to_owned(),
            Some("waking into restoring".to_owned()),
            0
        ),
        "Moved while mid-transition (`restoring`) releases without spending an attempt"
    );
    assert_eq!(
        item(&db.pool, campaign, unbound_e0).await?,
        ("done".to_owned(), None, 1),
        "unbound E0 checkpoint is attributed to `source()` and dropped"
    );
    assert_eq!(
        item(&db.pool, campaign, pre_metadata_e0).await?,
        ("done".to_owned(), None, 1),
        "pre-metadata E0 checkpoint (`legacy_unbound_allowed = Some(false)`) is attributed to `source()` and dropped"
    );
    for legacy_dropped in [unbound_e0, pre_metadata_e0] {
        assert!(
            matches!(
                suspended_environment_pins::pin_state(
                    &db.pool,
                    K8S,
                    SandboxId::from_uuid(legacy_dropped),
                )
                .await?,
                SandboxPinState::SuspendedPinned {
                    kind: ArtifactKind::Disk,
                    ..
                }
            ),
            "legacy/unbound checkpoint remains suspended and is re-pinned to a disk projection"
        );
    }
    assert_eq!(
        maybe_item(&db.pool, campaign, unbound_target).await?,
        None,
        "unbound target checkpoint is excluded from on_source during discovery"
    );
    assert!(
        !dispatcher.called_sandboxes().contains(&unbound_target),
        "unbound target checkpoint is never dispatched"
    );
    assert_eq!(
        item(&db.pool, campaign, already_dropped).await?,
        ("done".to_owned(), None, 1)
    );
    assert_eq!(
        item(&db.pool, campaign, sealed_then_dropped).await?,
        ("pending".to_owned(), Some("suspend sealing".to_owned()), 0),
        "Sealed releases the lease without spending an attempt"
    );
    assert_eq!(
        item(&db.pool, campaign, rotated_on_source).await?,
        (
            "pending".to_owned(),
            Some("checkpoint pin rotated before sealing".to_owned()),
            0
        ),
        "expected_artifact_id fence declines a rotated source pin and releases without spending an attempt"
    );
    assert_eq!(
        item(&db.pool, campaign, rotated_to_target).await?,
        (
            "skipped".to_owned(),
            Some("checkpoint pin rotated before sealing".to_owned()),
            1
        )
    );
    assert!(
        matches!(
            suspended_environment_pins::pin_state(
                &db.pool,
                K8S,
                SandboxId::from_uuid(rotated_to_target),
            )
            .await?,
            SandboxPinState::SuspendedPinned {
                kind: ArtifactKind::Full,
                ..
            }
        ),
        "expected_artifact_id fence leaves the newly suspended target() full checkpoint intact"
    );
    assert_eq!(
        item(&db.pool, campaign, moved_to_disk).await?,
        ("done".to_owned(), None, 1)
    );
    assert_eq!(
        item(&db.pool, campaign, transient_error).await?,
        ("pending".to_owned(), Some("s3 timeout".to_owned()), 1)
    );
    assert_eq!(
        item(&db.pool, campaign, lost_reply_after_commit).await?,
        ("pending".to_owned(), Some("s3 timeout".to_owned()), 1)
    );

    // Settle the two mid-transition rows: `restoring_before_dispatch` settles
    // back to `suspended` on `source()` (so it drops), and `moved_into_restoring`
    // finishes restoring into `started` (so it's skipped and retired).
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'suspended' WHERE id = $1"
    ))
    .bind(restoring_before_dispatch)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET lifecycle = 'started',
             current_local_artifact_id = NULL,
             current_local_artifact_kind = NULL,
             current_local_artifact_origin = NULL
         WHERE id = $1"
    ))
    .bind(moved_into_restoring)
    .execute(&db.pool)
    .await?;

    // While the campaign is still open, another sandbox suspends onto `source()`
    // so completion re-scan must catch it before declaring the campaign done.
    let late_suspender = suspended_on_source(&db.pool).await?;

    tick_until_finished(&reconciler, &db.pool, campaign, 2 * MAX_ATTEMPTS as usize).await?;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert_eq!(
        item(&db.pool, campaign, restoring_before_dispatch).await?,
        ("done".to_owned(), None, 1)
    );
    assert_eq!(
        item(&db.pool, campaign, moved_into_restoring).await?.0,
        "skipped"
    );
    assert_eq!(
        item(&db.pool, campaign, sealed_then_dropped).await?,
        ("done".to_owned(), None, 1)
    );
    assert_eq!(
        item(&db.pool, campaign, rotated_on_source).await?,
        ("done".to_owned(), None, 1)
    );
    let rotated_calls: Vec<LocalArtifactId> = dispatcher
        .calls()
        .into_iter()
        .filter(|(id, _)| *id == rotated_on_source)
        .map(|(_, art)| art)
        .collect();
    assert_eq!(rotated_calls.len(), 2);
    assert_eq!(rotated_calls[0], initial_rotated_source_art);
    assert_ne!(
        rotated_calls[1], initial_rotated_source_art,
        "second lease inspects and passes the newly rotated LocalArtifactId"
    );
    assert_eq!(
        item(&db.pool, campaign, transient_error).await?,
        ("done".to_owned(), None, 2)
    );
    assert_eq!(
        item(&db.pool, campaign, lost_reply_after_commit).await?,
        ("done".to_owned(), None, MAX_ATTEMPTS as i64),
        "a committed drop whose reply is lost on MAX_ATTEMPTS finishes done instead of failing"
    );
    assert_eq!(
        dispatcher
            .calls()
            .into_iter()
            .filter(|(id, _)| *id == lost_reply_after_commit)
            .count(),
        MAX_ATTEMPTS as usize,
        "lost-reply commit on MAX_ATTEMPTS finishes done after re-inspecting the disk pin"
    );
    assert_eq!(
        item(&db.pool, campaign, late_suspender).await?,
        ("done".to_owned(), None, 1)
    );
    assert_eq!(
        maybe_item(&db.pool, campaign, unbound_target).await?,
        None,
        "unbound target checkpoint still has no campaign item after completion"
    );
    Ok(())
}

async fn maybe_item(
    pool: &PgPool,
    campaign: CampaignId,
    sandbox: Uuid,
) -> TestResult<Option<(String, Option<String>, i64)>> {
    Ok(sqlx::query_as::<_, (String, Option<String>, i64)>(sql!(
        "SELECT phase, last_error, attempts FROM runtime_upgrade_items
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(campaign)
    .bind(sandbox)
    .fetch_optional(pool)
    .await?)
}

#[derive(Clone)]
struct BumpGenerationAfterFirstRead {
    reads: Arc<Mutex<usize>>,
    inner: FakeMemoryRetirement,
}

impl MemoryRetirementSource for BumpGenerationAfterFirstRead {
    async fn memory_retirement(
        &self,
        environment: EnvironmentReference,
    ) -> Result<MemoryRetirement, String> {
        let read_index = {
            let mut reads = self.reads.lock().unwrap();
            *reads += 1;
            *reads
        };
        if read_index == 2 {
            self.inner.unretire(environment);
            self.inner.retire(environment);
        }
        self.inner.memory_retirement(environment).await
    }
}
