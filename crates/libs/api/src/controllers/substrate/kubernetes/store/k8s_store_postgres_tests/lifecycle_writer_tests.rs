//! Characterization of two Kubernetes store writes to Postgres
//! `sandboxes.lifecycle`: the failed-recovery revert to `suspended` (through
//! `complete_started_recovery`), and the finalization of a `suspending`
//! sandbox whose pod disappeared after publishing a current snapshot. Each
//! sweeps every stored lifecycle and pins the columns the write moves,
//! including the current local artifact marker it keeps or clears; the
//! finalization also sets its other guards beside the accepting control. The
//! revert's execution-proof and snapshot guards are pinned by the
//! `failed_started_recovery_*` behaviours in the parent module.

use pplx_space_placement::StoredSandboxLifecycle;

use super::*;

/// The Postgres `sandboxes` columns both writes move, and the state of the
/// execution they retire.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
struct Row {
    lifecycle: String,
    placement: String,
    active_node_id: Option<NodeId>,
    target_node_id: Option<NodeId>,
    placement_changed: bool,
    command_ready_pending: bool,
    current_local_artifact_id: Option<LocalArtifactId>,
    current_local_artifact_kind: Option<String>,
    current_local_artifact_origin: Option<String>,
    execution_state: String,
    execution_active: bool,
}

async fn row(pool: &PgPool, sandbox_id: SandboxId, generation: i64) -> TestResult<Row> {
    Ok(sqlx::query_as::<_, Row>(sql!(
        "SELECT s.lifecycle, s.placement, s.active_node_id, s.target_node_id,
                s.placement_changed_at IS NOT NULL AS placement_changed,
                s.command_ready_pending, s.current_local_artifact_id,
                s.current_local_artifact_kind, s.current_local_artifact_origin,
                e.state AS execution_state, e.active AS execution_active
           FROM sandboxes s
           JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
          WHERE s.id = $1 AND e.generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_one(pool)
    .await?)
}

async fn set_lifecycle(pool: &PgPool, sandbox_id: SandboxId, lifecycle: &str) -> TestResult {
    sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
        .bind(sandbox_id)
        .bind(lifecycle)
        .execute(pool)
        .await?;
    Ok(())
}

/// A terminal started-recovery failure reverts the row to `suspended`,
/// unplaced and with no node or placement clock, and retires the failed
/// execution, only when the row is still `started`. From every other stored
/// lifecycle the row stays as it was and the failed execution stays active.
#[tokio::test(flavor = "current_thread")]
async fn failed_recovery_reverts_only_a_started_row() -> TestResult {
    let fixture = postgres_store().await?;
    let pool = &fixture.pool;

    let mut mismatches = Vec::new();
    for lifecycle in StoredSandboxLifecycle::all_as_str() {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, "started").await?;
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET target_node_id = $2,
                    placement_changed_at = NOW() - INTERVAL '1 hour'
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(NodeId::new())
        .execute(pool)
        .await?;
        insert_ready_full_snapshot_relative_to_lifecycle(
            pool,
            sandbox_id,
            "s3://space-test/failed-recovery-sweep?region=us-east-1",
            -1,
            None,
        )
        .await?;
        let generation = match fixture.store.claim_started_recovery(sandbox_id).await? {
            StartedRecoveryClaim::CreatedExecution { generation } => generation,
            other => return Err(format!("{lifecycle}: expected a new claim, got {other:?}").into()),
        };
        set_lifecycle(pool, sandbox_id, lifecycle).await?;
        let before = row(pool, sandbox_id, generation).await?;

        fixture
            .store
            .complete_started_recovery(
                sandbox_id,
                generation,
                None,
                StartedRecoveryCompletion::Failed,
            )
            .await?;

        let expected = if lifecycle == "started" {
            Row {
                lifecycle: "suspended".to_owned(),
                placement: "unplaced".to_owned(),
                active_node_id: None,
                target_node_id: None,
                placement_changed: false,
                command_ready_pending: false,
                current_local_artifact_id: None,
                current_local_artifact_kind: None,
                current_local_artifact_origin: None,
                execution_state: "recovery_failed".to_owned(),
                execution_active: false,
            }
        } else {
            Row {
                execution_state: "recovery_failed".to_owned(),
                ..before
            }
        };
        let actual = row(pool, sandbox_id, generation).await?;
        if actual != expected {
            mismatches.push(format!("{lifecycle}: row {actual:?}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// The failed-recovery revert keeps a marker the snapshot it reverts to
/// backs, and clears one no snapshot backs, so the restore selector picks the
/// newest snapshot instead.
#[tokio::test(flavor = "current_thread")]
async fn failed_recovery_revert_keeps_only_a_backed_marker() -> TestResult {
    let fixture = postgres_store().await?;
    let pool = &fixture.pool;

    // (case, the snapshot holds the marked artifact, the revert keeps the marker)
    let cases = [
        ("backed marker", true, true),
        ("stale marker", false, false),
    ];
    for (case, backed, keeps) in cases {
        let sandbox_id = SandboxId::new();
        let artifact = LocalArtifactId::new();
        insert_sandbox(pool, sandbox_id, "started").await?;
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(artifact)
        .execute(pool)
        .await?;
        insert_ready_full_snapshot_relative_to_lifecycle(
            pool,
            sandbox_id,
            "s3://space-test/failed-recovery-marker?region=us-east-1",
            -1,
            backed.then_some(artifact),
        )
        .await?;
        let generation = match fixture.store.claim_started_recovery(sandbox_id).await? {
            StartedRecoveryClaim::CreatedExecution { generation } => generation,
            other => return Err(format!("{case}: expected a new claim, got {other:?}").into()),
        };

        fixture
            .store
            .complete_started_recovery(
                sandbox_id,
                generation,
                None,
                StartedRecoveryCompletion::Failed,
            )
            .await?;

        let actual = row(pool, sandbox_id, generation).await?;
        assert_eq!(actual.lifecycle, "suspended", "{case}");
        let marker = (
            actual.current_local_artifact_id,
            actual.current_local_artifact_kind.as_deref(),
            actual.current_local_artifact_origin.as_deref(),
        );
        let expected = if keeps {
            (Some(artifact), Some("full"), Some("suspend"))
        } else {
            (None, None, None)
        };
        assert_eq!(marker, expected, "{case}");
    }
    Ok(())
}

const POD_UID: &str = "missing-suspend-pod-uid";

/// One way to set up a `suspending` sandbox whose generation-1 pod
/// disappeared, differing from the accepting control in one respect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Variation {
    Control,
    SuspendedExecution,
    NoActiveNode,
    InactiveExecution,
    OtherPodUid,
    ExecutionNotRunning,
    /// The caller names generation 2, not the execution's generation 1.
    OtherRequestedGeneration,
    SuccessorGeneration,
    /// The row's marker names an artifact no snapshot holds.
    StaleMarker,
    /// The row's marker names the snapshot's artifact.
    CurrentMarker,
    SnapshotFromOtherNode,
    SnapshotBeforeLifecycleChange,
    /// Another node published a later ready snapshot.
    NewerSnapshotFromOtherNode,
    /// A later attempt failed; the selector does not pick it.
    NewerFailedSnapshot,
    /// A later ready snapshot whose location was orphaned.
    NewerOrphanedSnapshot,
    /// A later ready disk snapshot.
    NewerDiskSnapshot,
    CommandAfterSnapshot,
    AccessAfterSnapshot,
    OrphanedSnapshotLocation,
    /// The resident node's snapshot is still uploading.
    SnapshotNotReady,
    /// The resident node's snapshot is a disk snapshot.
    DiskSnapshot,
    /// The resident node's snapshot came from a stop, not a suspend.
    StopOriginSnapshot,
}

impl Variation {
    fn requested_generation(self) -> i64 {
        if self == Self::OtherRequestedGeneration {
            2
        } else {
            1
        }
    }
}

/// A `snapshots` row to register: by default a ready full snapshot with a
/// durable location and no artifact identity.
#[derive(Debug, Clone, Copy)]
struct SnapshotSeed {
    source_node_id: NodeId,
    /// Minutes after the row's last lifecycle change (negative: before it).
    offset_minutes: i32,
    snapshot_type: &'static str,
    state: &'static str,
    /// The local artifact the snapshot holds, a full one of `origin`.
    artifact: Option<(LocalArtifactId, &'static str)>,
    orphaned: bool,
}

impl SnapshotSeed {
    fn ready_full(source_node_id: NodeId, offset_minutes: i32) -> Self {
        Self {
            source_node_id,
            offset_minutes,
            snapshot_type: "full",
            state: "ready",
            artifact: None,
            orphaned: false,
        }
    }
}

async fn insert_suspend_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    seed: SnapshotSeed,
) -> TestResult {
    let snapshot_id = SnapshotId::new();
    let (artifact, origin) = seed.artifact.unzip();
    sqlx::query(sql!(
        "INSERT INTO snapshots
             (id, sandbox_id, type, state, source_node_id, timestamp, org_id,
              local_artifact_id, artifact_kind, artifact_origin)
         VALUES ($1, $2, $3, $4, $5,
                 (SELECT lifecycle_changed_at + ($6 * INTERVAL '1 minute')
                    FROM sandboxes WHERE id = $2),
                 '00000000-0000-0000-0000-000000000001',
                 $7,
                 CASE WHEN $7::uuid IS NULL THEN NULL ELSE 'full' END,
                 $8)"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(seed.snapshot_type)
    .bind(seed.state)
    .bind(seed.source_node_id)
    .bind(seed.offset_minutes)
    .bind(artifact)
    .bind(origin)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id, orphaned_at)
         VALUES ($1, 's3://space-test/missing-suspend-pod?region=us-east-1',
                 '00000000-0000-0000-0000-000000000001',
                 CASE WHEN $2 THEN NOW() END)"
    ))
    .bind(snapshot_id)
    .bind(seed.orphaned)
    .execute(pool)
    .await?;
    Ok(())
}

/// A `k8s` sandbox in `lifecycle`, resident on a node, whose generation-1
/// execution holds [`POD_UID`], with a ready full snapshot from that node
/// taken a minute after the row's last lifecycle change, shaped by
/// `variation`.
async fn insert_missing_suspend_pod(
    pool: &PgPool,
    lifecycle: &str,
    variation: Variation,
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let artifact = LocalArtifactId::new();
    insert_sandbox(pool, sandbox_id, lifecycle).await?;
    let active_node_id = (variation != Variation::NoActiveNode).then_some(node_id);
    let marker =
        matches!(variation, Variation::StaleMarker | Variation::CurrentMarker).then_some(artifact);
    // Two minutes after the last lifecycle change: after the snapshot.
    let command_late = variation == Variation::CommandAfterSnapshot;
    let access_late = variation == Variation::AccessAfterSnapshot;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET placement = 'on_node',
                active_node_id = $2,
                target_node_id = $3,
                placement_changed_at = NOW() - INTERVAL '1 hour',
                current_local_artifact_id = $4,
                current_local_artifact_kind = CASE WHEN $4::uuid IS NULL THEN NULL ELSE 'full' END,
                current_local_artifact_origin =
                    CASE WHEN $4::uuid IS NULL THEN NULL ELSE 'suspend' END,
                last_command_at = CASE WHEN $5 THEN lifecycle_changed_at + INTERVAL '2 minutes' END,
                last_access_at = CASE WHEN $6 THEN lifecycle_changed_at + INTERVAL '2 minutes' END
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(active_node_id)
    .bind(node_id)
    .bind(marker)
    .bind(command_late)
    .bind(access_late)
    .execute(pool)
    .await?;
    let state = match variation {
        Variation::SuspendedExecution => "suspended",
        Variation::ExecutionNotRunning => "recovery_failed",
        _ => "running",
    };
    let pod_uid = match variation {
        Variation::OtherPodUid => "a-replacement-pod-uid",
        _ => POD_UID,
    };
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active)
         VALUES ($1, 1, $2, 'space', $3, $4, $5)"
    ))
    .bind(sandbox_id)
    .bind(format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple()))
    .bind(pod_uid)
    .bind(state)
    .bind(variation != Variation::InactiveExecution)
    .execute(pool)
    .await?;
    if variation == Variation::SuccessorGeneration {
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active)
             VALUES ($1, 2, $2, 'space', 'forgotten', FALSE)"
        ))
        .bind(sandbox_id)
        .bind(format!("space-sbx-{}-g2", sandbox_id.as_uuid().simple()))
        .execute(pool)
        .await?;
    }
    let source_node_id = match variation {
        Variation::SnapshotFromOtherNode => NodeId::new(),
        _ => node_id,
    };
    let offset_minutes = match variation {
        Variation::SnapshotBeforeLifecycleChange => -1,
        _ => 1,
    };
    let mut snapshot = SnapshotSeed::ready_full(source_node_id, offset_minutes);
    match variation {
        Variation::CurrentMarker => snapshot.artifact = Some((artifact, "suspend")),
        Variation::StopOriginSnapshot => snapshot.artifact = Some((artifact, "stop")),
        Variation::OrphanedSnapshotLocation => snapshot.orphaned = true,
        Variation::SnapshotNotReady => snapshot.state = "creating",
        Variation::DiskSnapshot => snapshot.snapshot_type = "disk",
        _ => {}
    }
    insert_suspend_snapshot(pool, sandbox_id, snapshot).await?;
    let newer = SnapshotSeed::ready_full(NodeId::new(), 2);
    let newer = match variation {
        Variation::NewerSnapshotFromOtherNode => Some(newer),
        Variation::NewerFailedSnapshot => Some(SnapshotSeed {
            source_node_id: node_id,
            state: "failed",
            ..newer
        }),
        Variation::NewerOrphanedSnapshot => Some(SnapshotSeed {
            orphaned: true,
            ..newer
        }),
        Variation::NewerDiskSnapshot => Some(SnapshotSeed {
            snapshot_type: "disk",
            ..newer
        }),
        _ => None,
    };
    if let Some(newer) = newer {
        insert_suspend_snapshot(pool, sandbox_id, newer).await?;
    }
    Ok(sandbox_id)
}

/// What the finalization does to a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Finalization {
    Refused,
    Applied,
    /// Applied, clearing a marker no current snapshot backs.
    AppliedClearingMarker,
}

/// The row the finalization leaves: when it applies, `suspended` and
/// unplaced with its restore target kept, and the execution retired
/// `suspended`.
fn finalized(before: &Row, finalization: Finalization) -> Row {
    let mut after = Row {
        lifecycle: "suspended".to_owned(),
        placement: "unplaced".to_owned(),
        active_node_id: None,
        command_ready_pending: false,
        execution_state: "suspended".to_owned(),
        execution_active: false,
        ..before.clone()
    };
    match finalization {
        Finalization::Refused => before.clone(),
        Finalization::Applied => after,
        Finalization::AppliedClearingMarker => {
            after.current_local_artifact_id = None;
            after.current_local_artifact_kind = None;
            after.current_local_artifact_origin = None;
            after
        }
    }
}

/// The missing-pod finalization applies only to a `suspending` row; from
/// every other stored lifecycle it refuses and writes nothing.
#[tokio::test(flavor = "current_thread")]
async fn missing_suspend_pod_finalizes_only_a_suspending_row() -> TestResult {
    let fixture = postgres_store().await?;
    let pool = &fixture.pool;

    let mut mismatches = Vec::new();
    for lifecycle in StoredSandboxLifecycle::all_as_str() {
        let sandbox_id = insert_missing_suspend_pod(pool, lifecycle, Variation::Control).await?;
        let before = row(pool, sandbox_id, 1).await?;

        let applied = fixture
            .store
            .finalize_missing_suspending_pod_with_current_snapshot(sandbox_id, 1, POD_UID)
            .await?;

        let finalization = if lifecycle == "suspending" {
            Finalization::Applied
        } else {
            Finalization::Refused
        };
        let actual = row(pool, sandbox_id, 1).await?;
        if applied != (finalization != Finalization::Refused)
            || actual != finalized(&before, finalization)
        {
            mismatches.push(format!("{lifecycle}: applied {applied}, row {actual:?}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// The finalization's other guards, each beside the accepting control, which
/// differs from it in that one respect: the execution must be the active one,
/// at the requested generation, still holding the observed pod in `running`
/// or `suspended`, with no later generation; and the snapshot must be a ready
/// full suspend snapshot from the resident node, taken after the row's last
/// lifecycle change and its recorded command and access, durably located, and
/// the newest durable ready full snapshot. A later failed, orphaned or disk
/// snapshot does not displace it. A marker the snapshot does not back is
/// cleared, and one it does is kept.
///
/// A row with no active node fails the snapshot's source-node test too, so
/// its refusal does not rest on the residency guard alone. The marker
/// branch's own snapshot guards are not set one at a time: with a marker the
/// markerless branch accepts the same snapshot.
#[tokio::test(flavor = "current_thread")]
async fn missing_suspend_pod_finalization_requires_each_guard() -> TestResult {
    use Finalization::{Applied, AppliedClearingMarker, Refused};

    let fixture = postgres_store().await?;
    let pool = &fixture.pool;
    let cases = [
        (Variation::Control, Applied),
        (Variation::SuspendedExecution, Applied),
        (Variation::NoActiveNode, Refused),
        (Variation::InactiveExecution, Refused),
        (Variation::OtherPodUid, Refused),
        (Variation::ExecutionNotRunning, Refused),
        (Variation::OtherRequestedGeneration, Refused),
        (Variation::SuccessorGeneration, Refused),
        (Variation::StaleMarker, AppliedClearingMarker),
        (Variation::CurrentMarker, Applied),
        (Variation::SnapshotFromOtherNode, Refused),
        (Variation::SnapshotBeforeLifecycleChange, Refused),
        (Variation::NewerSnapshotFromOtherNode, Refused),
        (Variation::NewerFailedSnapshot, Applied),
        (Variation::NewerOrphanedSnapshot, Applied),
        (Variation::NewerDiskSnapshot, Applied),
        (Variation::CommandAfterSnapshot, Refused),
        (Variation::AccessAfterSnapshot, Refused),
        (Variation::OrphanedSnapshotLocation, Refused),
        (Variation::SnapshotNotReady, Refused),
        (Variation::DiskSnapshot, Refused),
        (Variation::StopOriginSnapshot, Refused),
    ];

    let mut mismatches = Vec::new();
    for (variation, finalization) in cases {
        let sandbox_id = insert_missing_suspend_pod(pool, "suspending", variation).await?;
        let before = row(pool, sandbox_id, 1).await?;

        let applied = fixture
            .store
            .finalize_missing_suspending_pod_with_current_snapshot(
                sandbox_id,
                variation.requested_generation(),
                POD_UID,
            )
            .await?;

        let actual = row(pool, sandbox_id, 1).await?;
        if applied != (finalization != Refused) || actual != finalized(&before, finalization) {
            mismatches.push(format!("{variation:?}: applied {applied}, row {actual:?}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}
