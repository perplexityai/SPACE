//! Seeds and whole-database readers shared by the store's tests and
//! sbx-mgr's other test suites, exposed through the `test-support` feature.
use super::*;

pub mod state_write_fixtures;

/// [`complete_cleanup_episode_for_job`] outside a cleanup job.
pub fn complete_cleanup_episode(
    conn: &Connection,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
) -> std::result::Result<Option<()>, CompleteCleanupEpisodeError> {
    complete_cleanup_episode_for_job(conn, sandbox_id, cleanup_id, None)
}

/// [`quarantine_cleanup_episode_for_job`] outside a cleanup job.
pub fn quarantine_cleanup_episode(
    conn: &Connection,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    unsafe_evidence: &str,
) -> std::result::Result<bool, MarkQuarantinedError> {
    quarantine_cleanup_episode_for_job(conn, sandbox_id, cleanup_id, unsafe_evidence, None)
}

/// [`update_runtime_fields_for_activation`] with no activation owner.
pub fn update_runtime_fields(
    conn: &Connection,
    sandbox_id: SandboxId,
    fields: RuntimeFields<'_>,
) -> Result<SupersededTeardownGeneration> {
    sandboxes::update_runtime_fields_with_activation_owner(conn, sandbox_id, None, fields)
}

/// Fence a runtime upgrade of a stopped or suspended `sandbox_id` to
/// `target`, under a test authorization that accepts memory loss.
pub fn fence_runtime_upgrade(
    conn: &Connection,
    sandbox_id: &str,
    target: pplx_space_common_core::types::EnvironmentReference,
    checkpoint_dir_identity: Option<&str>,
) -> RuntimeUpgradeRecord {
    let sandbox = get_sandbox(conn, sandbox_id).unwrap().unwrap();
    match begin_runtime_upgrade(
        conn,
        NewRuntimeUpgrade {
            operation_id: RuntimeUpgradeOperationId::from_uuid(uuid::Uuid::now_v7()),
            sandbox_id,
            observed: ObservedSandbox::of(&sandbox),
            target_environment: target,
            target_state: RuntimeUpgradeTargetState::Started,
            authorization: &RuntimeUpgradeAuthorization {
                id: "block-e0".into(),
                reason: "runtime advisory".into(),
                memory_loss: true,
            },
            checkpoint_dir_identity,
            previous_execution: sandbox
                .execution_metadata
                .as_ref()
                .expect("the fixture's sandbox has a saved consumer"),
        },
    )
    .unwrap()
    {
        BeginRuntimeUpgrade::Fenced(record) => *record,
        other => panic!("expected a fenced runtime upgrade, got {other:?}"),
    }
}

/// [`begin_activation_with_route_evidence`] with no route evidence.
pub fn begin_activation(
    conn: &Connection,
    id: &str,
    expected_state: SandboxState,
    kind: ActivationKind,
    execution_generation: Option<ExecutionGeneration>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
) -> std::result::Result<Option<CleanupEpisode>, BeginActivationError> {
    begin_activation_with_route_evidence(
        conn,
        id,
        expected_state,
        kind,
        execution_generation,
        activation_attempt_id,
        ActivationRouteAdmission::default(),
    )
}

/// Build a quarantine-recovery episode for the loop's mechanics to act on.
///
/// For any reason other than `orphaned_sandbox_row_missing` this both names a
/// full/suspend artifact on the sandbox row and registers it in
/// `local_artifacts`. Production reaches the same *loop-visible* state by a
/// different route: the registry row is reaped while the sandbox row keeps
/// naming the artifact, a restore then fails, and its cleanup quarantines
/// because it cannot account for the artifact. The scan and the claim read only
/// the sandbox row's columns, which are identical either way, and the adoption
/// upserts the registry row rather than requiring it. What no writer produces is
/// the divergence that would let a *failed restore* arrive in this shape — see
/// `a_failed_restore_recovers_only_if_the_two_artifact_stores_diverge`. So tests
/// built on this helper cover the fence, the backoff and the adoption, which are
/// reason-agnostic; do not cite one as evidence that a failed restore recovers.
pub fn setup_quarantine_recovery_episode(
    conn: &Connection,
    reason_code: SandboxQuarantineReasonCode,
) -> SandboxId {
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(
        conn,
        TestSandboxInsert::new(
            &sandbox_id_string,
            SandboxRuntimeLabel::from_sandbox_id(sandbox_id).to_string(),
            SandboxState::Restoring,
        ),
    );
    if reason_code != SandboxQuarantineReasonCode::OrphanedSandboxRowMissing {
        let local_artifact_id = LocalArtifactId::new();
        insert_local_artifact(
            conn,
            &sandbox_id_string,
            local_artifact_id,
            ArtifactKind::Full,
            ArtifactOrigin::Suspend,
            None,
            "/space/cache/sandboxes/test/quarantine-recovery",
        )
        .unwrap();
        conn.execute(
            "UPDATE sandboxes
                SET current_local_artifact_id = ?1,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend',
                    space_api_route_execution_generation = 7
              WHERE id = ?2",
            params![local_artifact_id.to_string(), sandbox_id_string],
        )
        .unwrap();
    }
    assert!(
        mark_restoring_quarantined(
            conn,
            &sandbox_id_string,
            reason_code,
            "restore rollout interrupted",
        )
        .unwrap()
    );
    sandbox_id
}

/// `(next_action_owner, attempt_count, next_action_after, claim_token, claim_expires_at)`
/// for one quarantine episode — the fields legacy recovery leases against.
pub fn quarantine_claim_state(
    conn: &Connection,
    sandbox_id: SandboxId,
) -> (
    Option<String>,
    i64,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    conn.query_row(
        "SELECT next_action_owner, attempt_count, next_action_after,
                claim_token, claim_expires_at
           FROM sandbox_quarantine_state
          WHERE sandbox_id = ?1",
        params![sandbox_id.to_string()],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        },
    )
    .unwrap()
}

pub fn setup_quarantine_recovery_candidate(
    conn: &Connection,
) -> (SandboxId, QuarantineRecoveryCandidate) {
    let sandbox_id = setup_quarantine_recovery_episode(
        conn,
        SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
    );
    let [candidate] = scan_quarantine_recovery_candidates(conn)
        .unwrap()
        .candidates
        .try_into()
        .unwrap();
    (sandbox_id, candidate)
}

/// Every row of every table, order-insensitive.
pub fn database_rows(conn: &Connection) -> std::collections::BTreeMap<String, Vec<String>> {
    let tables: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    tables
        .into_iter()
        .map(|table| {
            let mut statement = conn.prepare(&format!("SELECT * FROM \"{table}\"")).unwrap();
            let width = statement.column_count();
            let mut rows: Vec<String> = statement
                .query_map([], |row| {
                    (0..width)
                        .map(|column| row.get::<_, rusqlite::types::Value>(column))
                        .collect::<rusqlite::Result<Vec<_>>>()
                        .map(|values| format!("{values:?}"))
                })
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            rows.sort();
            (table, rows)
        })
        .collect()
}

pub struct TestSandboxInsert {
    pub(crate) id: String,
    pub(crate) uuid8: String,
    pub(crate) state: SandboxState,
    pub(crate) user: String,
    pub(crate) rootfs_size_mb: Option<u32>,
    pub(crate) disk_io_limits: pplx_space_common_core::types::DiskIoLimits,
    pub(crate) pool_template: Option<String>,
    pub(crate) volume_encryption: bool,
    pub(crate) process_env: BTreeMap<String, String>,
    pub(crate) auto_lifecycle: AutoLifecyclePolicy,
    pub(crate) network_policy_override: Option<NetworkPolicy>,
    pub(crate) network_policy_generation: u64,
    pub(crate) network_policy_authority: PolicyAuthority,
    pub(crate) auto_lifecycle_authority: PolicyAuthority,
}

impl TestSandboxInsert {
    pub(crate) fn new(
        id: impl Into<String>,
        uuid8: impl Into<String>,
        state: SandboxState,
    ) -> Self {
        Self {
            id: id.into(),
            uuid8: uuid8.into(),
            state,
            user: "user".to_string(),
            rootfs_size_mb: None,
            disk_io_limits: Default::default(),
            pool_template: None,
            volume_encryption: false,
            process_env: BTreeMap::new(),
            auto_lifecycle: AutoLifecyclePolicy::disabled(),
            network_policy_override: None,
            network_policy_generation: 0,
            network_policy_authority: PolicyAuthority::Cluster,
            auto_lifecycle_authority: PolicyAuthority::Cluster,
        }
    }

    pub(crate) fn insert_params<'a>(
        &'a self,
        uuid8: &'a SandboxRuntimeLabel,
        user: &'a SandboxOwner,
    ) -> InsertSandboxParams<'a> {
        InsertSandboxParams {
            guest_image: Default::default(),
            id: &self.id,
            uuid8,
            state: self.state,
            template: "seed",
            template_hash: Some("hash"),
            user,
            runtime: "fc",
            snapshots_enabled: false,
            rootfs_size_mb: self.rootfs_size_mb,
            disk_io_limits: self.disk_io_limits,
            pool_template: self.pool_template.as_deref(),
            volume_encryption: self.volume_encryption,
            process_env: &self.process_env,
            auto_lifecycle: &self.auto_lifecycle,
            network_policy_override: self.network_policy_override.as_ref(),
            network_policy_generation: self.network_policy_generation,
            network_policy_authority: self.network_policy_authority,
            auto_lifecycle_authority: self.auto_lifecycle_authority,
        }
    }
}

pub fn insert_test_sandbox(conn: &Connection, params: TestSandboxInsert) {
    let uuid8 = SandboxRuntimeLabel::parse(params.uuid8.clone()).unwrap();
    let user = SandboxOwner::parse(params.user.clone()).unwrap();
    insert_sandbox(conn, params.insert_params(&uuid8, &user)).unwrap();
}

/// Claim the episode's due candidate and record `reason` against the claim,
/// as one recovery pass does: the claim carries the reason's effect on the run
/// of plan rejections, and the no-progress write records the reason.
pub fn record_quarantine_recovery_attempt(
    conn: &Connection,
    sandbox_id: SandboxId,
    reason: QuarantineRecoveryNoProgressReason,
) -> QuarantineRecoveryNoProgressOutcome {
    conn.execute(
        "UPDATE sandbox_quarantine_state SET next_action_after = NULL WHERE sandbox_id = ?1",
        params![sandbox_id.to_string()],
    )
    .unwrap();
    let [candidate] = scan_quarantine_recovery_candidates(conn)
        .unwrap()
        .candidates
        .try_into()
        .expect("a due episode is a candidate");
    let claimed =
        claim_quarantine_recovery_candidate(conn, &candidate, PlanRejectionRunEffect::of(reason))
            .unwrap()
            .expect("a due candidate is claimable");
    record_quarantine_recovery_no_progress(conn, &claimed, reason).unwrap()
}
