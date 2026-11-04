//! `BeginArtifactBackup`'s execution-generation fence: a teardown suspend of a
//! runtime the control plane has retired must not register a snapshot.

use super::*;

use pplx_space_common_core::types::{
    AbandonedRestoreRetirement, ExecutionGeneration, SandboxQuarantineReasonCode,
};
use pplx_space_node_api_client::cluster::backup_execution_superseded_generation;

fn teardown_backup_request(
    sandbox_id: SandboxId,
    local_artifact_id: LocalArtifactId,
    backup_attempt_id: SnapshotId,
    execution_generation: i64,
) -> BeginArtifactBackupRequest {
    BeginArtifactBackupRequest {
        sandbox_id: sandbox_id.to_string(),
        local_artifact_id: local_artifact_id.to_string(),
        artifact_kind: pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32,
        backup_attempt_id: backup_attempt_id.to_string(),
        storage_uri: test_storage_uri(sandbox_id, backup_attempt_id),
        artifact_origin: pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend as i32,
        snapshot_name: None,
        execution: Some(test_snapshot_execution()),
        capture_host_cpu: None,
        execution_generation: Some(execution_generation),
        fence_verdict_only: false,
    }
}

async fn begin_teardown_backup(
    service: &ClusterApiService,
    node_ip: std::net::IpAddr,
    sandbox_id: SandboxId,
    backup_attempt_id: SnapshotId,
    execution_generation: i64,
) -> Result<(), tonic::Status> {
    begin_teardown_backup_of(
        service,
        node_ip,
        sandbox_id,
        LocalArtifactId::new(),
        backup_attempt_id,
        execution_generation,
    )
    .await
}

/// [`begin_teardown_backup`] of the given local artifact.
async fn begin_teardown_backup_of(
    service: &ClusterApiService,
    node_ip: std::net::IpAddr,
    sandbox_id: SandboxId,
    local_artifact_id: LocalArtifactId,
    backup_attempt_id: SnapshotId,
    execution_generation: i64,
) -> Result<(), tonic::Status> {
    service
        .begin_artifact_backup(request_from_node(
            teardown_backup_request(
                sandbox_id,
                local_artifact_id,
                backup_attempt_id,
                execution_generation,
            ),
            node_ip,
        ))
        .await
        .map(|_| ())
}

async fn insert_execution(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    state: &str,
    active: bool,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
                 sandbox_id, generation, pod_name, pod_namespace, state, active
             )
             VALUES ($1, $2, $3, 'space', $4, $5)"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(format!(
        "space-sbx-{}-g{generation}",
        sandbox_id.as_uuid().simple()
    ))
    .bind(state)
    .bind(active)
    .execute(executor)
    .await?;
    Ok(())
}

/// The suspended row a rolled-back restore leaves: the VM's source node is
/// still `target_node_id`, so the node check alone admits it.
async fn suspend_on_source_node(pool: &sqlx::PgPool, sandbox_id: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'suspended', active_node_id = NULL
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn retire_execution(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = FALSE, state = 'replaced'
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .execute(executor)
    .await?;
    Ok(())
}

fn assert_superseded_refusal(error: &tonic::Status, generation: i64) {
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert_eq!(
        backup_execution_superseded_generation(error).map(ExecutionGeneration::get),
        Some(generation),
        "the refusal must name the retired generation: {error:?}"
    );
}

/// A restore claimed generation 2, was rolled back (the claim retired as
/// `replaced`, the row reverted to `suspended`), and its stale pod later bound
/// on the source node. The GC reaps that pod, and the teardown suspend tries to
/// register the orphan VM's state. It must not become a snapshot the next
/// restore of this markerless row would select.
#[tokio::test(flavor = "current_thread")]
async fn row_reverted_reap_cannot_register_a_teardown_snapshot() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let (sandbox_id, source_ip, _) = seed_sandbox_on_source_node(pool).await?;
    insert_execution(pool, sandbox_id, 1, "suspended", false).await?;
    insert_execution(pool, sandbox_id, 2, "replaced", false).await?;
    suspend_on_source_node(pool, sandbox_id).await?;

    let backup_attempt_id = SnapshotId::new();
    let error = begin_teardown_backup(
        &fixture.service,
        source_ip,
        sandbox_id,
        backup_attempt_id,
        2,
    )
    .await
    .expect_err("a retired execution's teardown suspend must not register a snapshot");
    assert_superseded_refusal(&error, 2);
    assert_eq!(snapshot_row_count(pool, backup_attempt_id).await?, 0);
    Ok(())
}

/// An older generation's teardown is refused once a newer execution is active,
/// even before that one has a pod and on the node that runs it. The older row
/// is retired as `forgotten`, so only the newer execution can refuse it.
#[tokio::test(flavor = "current_thread")]
async fn teardown_backup_below_the_active_generation_is_refused() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let (sandbox_id, source_ip, _) = seed_sandbox_on_source_node(pool).await?;
    insert_execution(pool, sandbox_id, 1, "forgotten", false).await?;
    insert_execution(pool, sandbox_id, 2, "running", true).await?;

    let stale_attempt = SnapshotId::new();
    let error = begin_teardown_backup(&fixture.service, source_ip, sandbox_id, stale_attempt, 1)
        .await
        .expect_err("a superseded generation must not register a snapshot");
    assert_superseded_refusal(&error, 1);
    assert_eq!(snapshot_row_count(pool, stale_attempt).await?, 0);

    let current_attempt = SnapshotId::new();
    begin_teardown_backup(&fixture.service, source_ip, sandbox_id, current_attempt, 2).await?;
    assert_eq!(snapshot_state(pool, current_attempt).await?, "creating");
    Ok(())
}

/// The fence admits the API's own generation-bound suspend, which keeps its
/// execution active until the snapshot is durable, and a replay of that attempt
/// after the execution retires: the registered row is already the record.
#[tokio::test(flavor = "current_thread")]
async fn teardown_backup_of_the_active_generation_registers_and_replays() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let (sandbox_id, source_ip, _) = seed_sandbox_on_source_node(pool).await?;
    insert_execution(pool, sandbox_id, 3, "running", true).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'suspending' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;

    let backup_attempt_id = SnapshotId::new();
    let request = teardown_backup_request(sandbox_id, LocalArtifactId::new(), backup_attempt_id, 3);
    fixture
        .service
        .begin_artifact_backup(request_from_node(request.clone(), source_ip))
        .await?;
    assert_eq!(snapshot_state(pool, backup_attempt_id).await?, "creating");

    retire_execution(pool, sandbox_id, 3).await?;
    fixture
        .service
        .begin_artifact_backup(request_from_node(request, source_ip))
        .await?;
    finalize_artifact_backup_from_node(&fixture.service, source_ip, backup_attempt_id).await?;
    assert_eq!(snapshot_state(pool, backup_attempt_id).await?, "ready");
    Ok(())
}

/// A retire that commits while Begin waits on the execution row wins: Begin
/// re-reads the retired row and refuses rather than registering against the
/// version it first saw.
#[tokio::test(flavor = "current_thread")]
async fn teardown_backup_racing_a_retire_sees_the_retired_execution() -> TestResult {
    let Some(fixture) = cluster_fixture_with_max_connections(5).await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let (sandbox_id, source_ip, _) = seed_sandbox_on_source_node(pool).await?;
    insert_execution(pool, sandbox_id, 4, "creating", true).await?;

    let mut retire = pool.begin().await?;
    retire_execution(&mut *retire, sandbox_id, 4).await?;

    let backup_attempt_id = SnapshotId::new();
    let begin = begin_teardown_backup(
        &fixture.service,
        source_ip,
        sandbox_id,
        backup_attempt_id,
        4,
    );
    let commit = async {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let waiting: i64 = sqlx::query_scalar(sql!(
                "SELECT COUNT(*) FROM pg_stat_activity
                  WHERE wait_event_type = 'Lock'
                    AND query LIKE '%FROM sandbox_k8s_executions%'"
            ))
            .fetch_one(pool)
            .await?;
            if waiting > 0 {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                return Err("Begin never blocked on the execution row".into());
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        retire.commit().await?;
        TestResult::Ok(())
    };
    let (begin, commit) = tokio::join!(begin, commit);
    commit?;

    let error = begin.expect_err("Begin must observe the committed retire");
    assert_superseded_refusal(&error, 4);
    assert_eq!(snapshot_row_count(pool, backup_attempt_id).await?, 0);
    Ok(())
}

/// A newer execution inserted while Begin is registering can't slip past the
/// fence. The inserter holds the sandbox row `FOR UPDATE`, as every execution
/// insert does, so the snapshot insert's foreign-key check waits on it, and
/// the fence then reads the committed newer execution.
#[tokio::test(flavor = "current_thread")]
async fn teardown_backup_racing_a_newer_execution_sees_it() -> TestResult {
    let Some(fixture) = cluster_fixture_with_max_connections(5).await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let (sandbox_id, source_ip, _) = seed_sandbox_on_source_node(pool).await?;
    insert_execution(pool, sandbox_id, 4, "creating", true).await?;

    // The substrate's claim of a newer execution: lock the sandbox row, retire
    // the current execution for a reason the own-row check doesn't refuse, and
    // insert its successor.
    let mut claim = pool.begin().await?;
    sqlx::query(sql!("SELECT 1 FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(sandbox_id)
        .execute(&mut *claim)
        .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = FALSE, state = 'suspended'
          WHERE sandbox_id = $1 AND generation = 4"
    ))
    .bind(sandbox_id)
    .execute(&mut *claim)
    .await?;
    insert_execution(&mut *claim, sandbox_id, 5, "creating", true).await?;

    let backup_attempt_id = SnapshotId::new();
    let begin = begin_teardown_backup(
        &fixture.service,
        source_ip,
        sandbox_id,
        backup_attempt_id,
        4,
    );
    let commit = async {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let waiting: i64 = sqlx::query_scalar(sql!(
                "SELECT COUNT(*) FROM pg_stat_activity
                  WHERE wait_event_type = 'Lock'
                    AND query LIKE '%INSERT INTO snapshots%'"
            ))
            .fetch_one(pool)
            .await?;
            if waiting > 0 {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                return Err("Begin never blocked on the sandbox row".into());
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        claim.commit().await?;
        TestResult::Ok(())
    };
    let (begin, commit) = tokio::join!(begin, commit);
    commit?;

    let error = begin.expect_err("Begin must observe the committed newer execution");
    assert_superseded_refusal(&error, 4);
    assert_eq!(snapshot_row_count(pool, backup_attempt_id).await?, 0);
    Ok(())
}

/// A fresh attempt for bytes the control plane already admitted is a retry of
/// a capture taken while its execution was current, which started recovery
/// may be restoring from: it registers even after that execution is
/// superseded. A capture never admitted is still refused.
#[tokio::test(flavor = "current_thread")]
async fn retry_of_an_admitted_capture_registers_after_its_execution_is_superseded() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let (sandbox_id, source_ip, _) = seed_sandbox_on_source_node(pool).await?;
    insert_execution(pool, sandbox_id, 4, "running", true).await?;

    let captured = LocalArtifactId::new();
    let admitted = SnapshotId::new();
    begin_teardown_backup_of(
        &fixture.service,
        source_ip,
        sandbox_id,
        captured,
        admitted,
        4,
    )
    .await?;
    sqlx::query(sql!("UPDATE snapshots SET state = 'failed' WHERE id = $1"))
        .bind(admitted)
        .execute(pool)
        .await?;
    // Started recovery's handoff: the current execution is replaced and its
    // successor claimed.
    retire_execution(pool, sandbox_id, 4).await?;
    insert_execution(pool, sandbox_id, 5, "recovering", true).await?;

    let retry = SnapshotId::new();
    begin_teardown_backup_of(&fixture.service, source_ip, sandbox_id, captured, retry, 4).await?;
    assert_eq!(snapshot_state(pool, retry).await?, "creating");

    let never_admitted = SnapshotId::new();
    let error = begin_teardown_backup(&fixture.service, source_ip, sandbox_id, never_admitted, 4)
        .await
        .expect_err("a capture never admitted stays refused");
    assert_superseded_refusal(&error, 4);
    assert_eq!(snapshot_row_count(pool, never_admitted).await?, 0);
    Ok(())
}

/// A registration without a generation, as an sbx-mgr that predates the
/// fence sends, isn't fenced: the same capture that registers without one is
/// refused when it names its superseded generation.
#[tokio::test(flavor = "current_thread")]
async fn registration_without_a_generation_is_not_fenced() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let (sandbox_id, source_ip, _) = seed_sandbox_on_source_node(pool).await?;
    insert_execution(pool, sandbox_id, 1, "replaced", false).await?;
    insert_execution(pool, sandbox_id, 2, "running", true).await?;

    let fenced = SnapshotId::new();
    let error = begin_teardown_backup(&fixture.service, source_ip, sandbox_id, fenced, 1)
        .await
        .expect_err("the superseded generation is fenced when named");
    assert_superseded_refusal(&error, 1);

    let backup_attempt_id = SnapshotId::new();
    let mut request =
        teardown_backup_request(sandbox_id, LocalArtifactId::new(), backup_attempt_id, 1);
    request.execution_generation = None;
    fixture
        .service
        .begin_artifact_backup(request_from_node(request, source_ip))
        .await?;
    assert_eq!(snapshot_state(pool, backup_attempt_id).await?, "creating");
    Ok(())
}

/// A generation that isn't positive names no execution, so the request is
/// malformed rather than fenced.
#[tokio::test(flavor = "current_thread")]
async fn non_positive_generation_is_invalid() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let (sandbox_id, source_ip, _) = seed_sandbox_on_source_node(pool).await?;
    insert_execution(pool, sandbox_id, 1, "running", true).await?;

    for generation in [0, -1] {
        let backup_attempt_id = SnapshotId::new();
        let error = fixture
            .service
            .begin_artifact_backup(request_from_node(
                teardown_backup_request(
                    sandbox_id,
                    LocalArtifactId::new(),
                    backup_attempt_id,
                    generation,
                ),
                source_ip,
            ))
            .await
            .expect_err("a non-positive generation is refused");
        assert_eq!(error.code(), Code::InvalidArgument, "{generation}: {error}");
        assert!(error.message().contains("execution_generation"), "{error}");
        assert_eq!(snapshot_row_count(pool, backup_attempt_id).await?, 0);
    }
    Ok(())
}

/// A stale runtime can sit on a node the sandbox has since left. Its retired
/// generation still gets the superseded refusal, not a permission refusal, so
/// that node releases it; an active generation still needs the source node.
#[tokio::test(flavor = "current_thread")]
async fn retired_teardown_off_the_source_node_is_told_it_was_superseded() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let (sandbox_id, _, other_ip) = seed_sandbox_on_source_node(pool).await?;
    insert_execution(pool, sandbox_id, 1, "forgotten", false).await?;
    insert_execution(pool, sandbox_id, 2, "running", true).await?;

    let stale_attempt = SnapshotId::new();
    let error = begin_teardown_backup(&fixture.service, other_ip, sandbox_id, stale_attempt, 1)
        .await
        .expect_err("a retired generation must not register from any node");
    assert_superseded_refusal(&error, 1);
    assert_eq!(snapshot_row_count(pool, stale_attempt).await?, 0);

    let active_attempt = SnapshotId::new();
    let error = begin_teardown_backup(&fixture.service, other_ip, sandbox_id, active_attempt, 2)
        .await
        .expect_err("the active generation registers only from the source node");
    assert_eq!(error.code(), Code::PermissionDenied, "{error}");
    assert_eq!(snapshot_row_count(pool, active_attempt).await?, 0);
    Ok(())
}

/// Only a retirement that proves the runtime superseded refuses the backup. An
/// execution the control plane gave up on for another reason, with nothing
/// newer, still registers its capture: it may be the sandbox's latest state.
#[tokio::test(flavor = "current_thread")]
async fn retired_execution_without_a_successor_still_registers() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let (sandbox_id, source_ip, _) = seed_sandbox_on_source_node(pool).await?;
    insert_execution(pool, sandbox_id, 5, "forgotten", false).await?;

    let backup_attempt_id = SnapshotId::new();
    begin_teardown_backup(
        &fixture.service,
        source_ip,
        sandbox_id,
        backup_attempt_id,
        5,
    )
    .await?;
    assert_eq!(snapshot_state(pool, backup_attempt_id).await?, "creating");
    Ok(())
}

/// A newer claim abandoned before it materialized a pod never ran, so it
/// doesn't supersede the execution below it. One that materialized does, even
/// after it retired: by its `pod_uid` alone while the materialization record
/// is unset (a row from before the record, until the backfill stamps it), and
/// by the record once its `pod_uid` is cleared, as pod re-materialization and
/// failed-recovery pod absence clear it after the pod may have run.
#[tokio::test(flavor = "current_thread")]
async fn only_a_newer_execution_that_ran_supersedes() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let (sandbox_id, source_ip, _) = seed_sandbox_on_source_node(pool).await?;
    insert_execution(pool, sandbox_id, 5, "forgotten", false).await?;
    insert_execution(pool, sandbox_id, 6, "replaced", false).await?;

    let admitted = SnapshotId::new();
    begin_teardown_backup(&fixture.service, source_ip, sandbox_id, admitted, 5).await?;
    assert_eq!(snapshot_state(pool, admitted).await?, "creating");

    for (case, update) in [
        (
            "a recorded pod UID with no materialization record",
            sql!(
                "UPDATE sandbox_k8s_executions SET pod_uid = 'pod-uid-6'
                  WHERE sandbox_id = $1 AND generation = 6"
            ),
        ),
        (
            "a materialization record whose pod UID was cleared",
            sql!(
                "UPDATE sandbox_k8s_executions
                    SET pod_uid = NULL, pod_materialized_at = NOW()
                  WHERE sandbox_id = $1 AND generation = 6"
            ),
        ),
    ] {
        sqlx::query(update).bind(sandbox_id).execute(pool).await?;
        let refused = SnapshotId::new();
        let error = begin_teardown_backup(&fixture.service, source_ip, sandbox_id, refused, 5)
            .await
            .expect_err(case);
        assert_superseded_refusal(&error, 5);
        assert_eq!(snapshot_row_count(pool, refused).await?, 0, "{case}");
    }
    Ok(())
}

/// The last execution of a sandbox, retired as `forgotten` with nothing
/// newer, left the row `suspended` on no node. Its capture can't register,
/// but it isn't superseded either: it may hold writes newer than the ready
/// snapshot the sandbox is durably restorable from. So it gets the plain
/// source-node refusal, which keeps it on the node, and not the verdict that
/// would have the node discard it.
#[tokio::test(flavor = "current_thread")]
async fn last_execution_on_no_node_keeps_its_capture() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let (sandbox_id, source_ip, _) = seed_sandbox_on_source_node(pool).await?;
    insert_execution(pool, sandbox_id, 1, "running", true).await?;
    let earlier = SnapshotId::new();
    begin_teardown_backup(&fixture.service, source_ip, sandbox_id, earlier, 1).await?;
    finalize_artifact_backup_from_node(&fixture.service, source_ip, earlier).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET active = FALSE, state = 'forgotten'
          WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'suspended', placement = 'unplaced',
                active_node_id = NULL, target_node_id = NULL, placement_changed_at = NULL
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    assert!(
        pplx_space_sandbox_store::durably_restorable(pool, sandbox_id).await?,
        "the earlier snapshot makes the sandbox restorable without the capture"
    );

    let later = SnapshotId::new();
    let error = begin_teardown_backup(&fixture.service, source_ip, sandbox_id, later, 1)
        .await
        .expect_err("a capture with no source node can't register");
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert_eq!(
        backup_execution_superseded_generation(&error),
        None,
        "the last execution's capture isn't declared superseded: {error:?}"
    );
    assert_eq!(snapshot_row_count(pool, later).await?, 0);
    Ok(())
}

/// A verdict-only registration answers as the registration would and persists
/// nothing. Admitted, it leaves no row, so a sandbox durably restorable from
/// its ready snapshot stays so, where the same registration made for real
/// would add a newer full row that holds a release. Refused, it still names
/// the superseded generation.
#[tokio::test(flavor = "current_thread")]
async fn verdict_only_registration_persists_nothing() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let (sandbox_id, source_ip, _) = seed_sandbox_on_source_node(pool).await?;
    insert_execution(pool, sandbox_id, 1, "running", true).await?;
    let ready_attempt = SnapshotId::new();
    begin_teardown_backup(&fixture.service, source_ip, sandbox_id, ready_attempt, 1).await?;
    finalize_artifact_backup_from_node(&fixture.service, source_ip, ready_attempt).await?;
    assert!(pplx_space_sandbox_store::durably_restorable(pool, sandbox_id).await?);

    let verdict_only = |backup_attempt_id, generation| BeginArtifactBackupRequest {
        fence_verdict_only: true,
        ..teardown_backup_request(
            sandbox_id,
            LocalArtifactId::new(),
            backup_attempt_id,
            generation,
        )
    };
    let admitted = SnapshotId::new();
    let reply = fixture
        .service
        .begin_artifact_backup(request_from_node(verdict_only(admitted, 1), source_ip))
        .await?
        .into_inner();
    assert!(
        reply.fence_verdict_only,
        "the reply says nothing was persisted"
    );
    assert_eq!(snapshot_row_count(pool, admitted).await?, 0);
    assert!(
        pplx_space_sandbox_store::durably_restorable(pool, sandbox_id).await?,
        "an admitted probe leaves no newer row"
    );

    retire_execution(pool, sandbox_id, 1).await?;
    insert_execution(pool, sandbox_id, 2, "running", true).await?;
    let refused = SnapshotId::new();
    let error = fixture
        .service
        .begin_artifact_backup(request_from_node(verdict_only(refused, 1), source_ip))
        .await
        .expect_err("a verdict-only request is fenced like the registration");
    assert_superseded_refusal(&error, 1);
    assert_eq!(snapshot_row_count(pool, refused).await?, 0);

    // The same admitted registration made for real persists a newer full row,
    // which the release's durability check refuses to look past.
    let registered = SnapshotId::new();
    let reply = fixture
        .service
        .begin_artifact_backup(request_from_node(
            teardown_backup_request(sandbox_id, LocalArtifactId::new(), registered, 2),
            source_ip,
        ))
        .await?
        .into_inner();
    assert!(!reply.fence_verdict_only);
    assert_eq!(snapshot_state(pool, registered).await?, "creating");
    assert!(!pplx_space_sandbox_store::durably_restorable(pool, sandbox_id).await?);
    Ok(())
}

/// The reply says whether the generation fence ran and admitted the
/// registration: only a fresh attempt bound to a generation, with no other
/// attempt's row for its bytes, is fenced. A replay, a retry of bytes another
/// attempt registered and an unbound registration are admitted without it.
#[tokio::test(flavor = "current_thread")]
async fn the_reply_says_whether_the_fence_admitted_the_registration() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let (sandbox_id, source_ip, _) = seed_sandbox_on_source_node(pool).await?;
    insert_execution(pool, sandbox_id, 1, "running", true).await?;
    let begin = |request: BeginArtifactBackupRequest| {
        fixture
            .service
            .begin_artifact_backup(request_from_node(request, source_ip))
    };

    let captured = LocalArtifactId::new();
    let first = teardown_backup_request(sandbox_id, captured, SnapshotId::new(), 1);
    let verdict_only = BeginArtifactBackupRequest {
        fence_verdict_only: true,
        ..first.clone()
    };
    assert!(
        begin(verdict_only)
            .await?
            .into_inner()
            .generation_fence_admitted,
        "a verdict-only request is fenced like the registration"
    );
    assert!(
        begin(first.clone())
            .await?
            .into_inner()
            .generation_fence_admitted,
        "a capture's first registration is fenced"
    );
    assert!(
        !begin(first).await?.into_inner().generation_fence_admitted,
        "a replay isn't"
    );
    let retry = teardown_backup_request(sandbox_id, captured, SnapshotId::new(), 1);
    assert!(
        !begin(retry).await?.into_inner().generation_fence_admitted,
        "a retry of bytes another attempt registered isn't"
    );
    let mut unbound =
        teardown_backup_request(sandbox_id, LocalArtifactId::new(), SnapshotId::new(), 1);
    unbound.execution_generation = None;
    assert!(
        !begin(unbound).await?.into_inner().generation_fence_admitted,
        "an unbound registration isn't"
    );
    Ok(())
}

/// The node rolled back a restore of generation 3 (its prepare failed) and
/// published the rollback receipt, which the control plane applies under
/// `retirement`. The sandbox has a durable pre-restore snapshot. Returns the
/// sandbox, its source node's address, and the state the rollback retired
/// generation 3's execution as.
async fn apply_rollback_of_a_restore_with_a_durable_source(
    fixture: &ClusterFixture,
    retirement: AbandonedRestoreRetirement,
) -> TestResult<(SandboxId, std::net::IpAddr, String)> {
    let pool = &fixture.service.pool;
    let (sandbox_id, source_ip, _) = seed_sandbox_on_source_node(pool).await?;
    let node_id: NodeId = sqlx::query_scalar(sql!("SELECT id FROM nodes WHERE host(address) = $1"))
        .bind(source_ip.to_string())
        .fetch_one(pool)
        .await?;
    insert_execution(pool, sandbox_id, 2, "suspended", false).await?;
    // The pre-restore snapshot generation 2's suspend made durable, taken
    // before the wake moved the row to `restoring`.
    let pre_restore = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, source_node_id, org_id,
             local_artifact_id, artifact_kind, artifact_origin, timestamp
         )
         VALUES (
             $1, $2, 'full', 'ready', $3, $4::uuid, $5, 'full', 'suspend',
             NOW() - INTERVAL '1 minute'
         )"
    ))
    .bind(pre_restore)
    .bind(sandbox_id)
    .bind(node_id)
    .bind(SEEDED_ORG_ID)
    .bind(LocalArtifactId::new())
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, 's3://space-test/pre-restore', $2::uuid)"
    ))
    .bind(pre_restore)
    .bind(SEEDED_ORG_ID)
    .execute(pool)
    .await?;
    // The wake claims the row, creates generation 3's execution, and its pod
    // is placed, which moves the placement clock past the pre-restore
    // snapshot.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'restoring', lifecycle_changed_at = NOW()
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    insert_execution(pool, sandbox_id, 3, "creating", true).await?;
    let activation_attempt_id = Uuid::now_v7();
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = $2
          WHERE sandbox_id = $1 AND generation = 3"
    ))
    .bind(sandbox_id)
    .bind(activation_attempt_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET placement_changed_at = NOW() WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;

    let rollback = PublishedSandboxState {
        transition_receipt: Some(pplx_space_sbx_mgr_client::proto::TransitionReceipt {
            id: Uuid::now_v7().to_string(),
            kind: "activation_rollback".to_string(),
            target_state: "suspended".to_string(),
            execution_generation: 3,
            reason_code: SandboxQuarantineReasonCode::RestorePrepareFailed.to_string(),
            activation_attempt_id: Some(activation_attempt_id.to_string()),
        }),
        ..published_sandbox_state(sandbox_id, "suspended")
    };
    let mut conn = pool.acquire().await?;
    let outcome =
        publish_sandbox_states_with_retirement(&mut conn, node_id, &[rollback], retirement).await?;
    drop(conn);
    assert_eq!(outcome.post_commit_events.len(), 1, "the rollback applies");
    let (lifecycle, state): (String, String) = sqlx::query_as(sql!(
        "SELECT s.lifecycle, e.state
           FROM sandboxes s
           JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id AND e.generation = 3
          WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?;
    assert_eq!(lifecycle, "suspended");
    Ok((sandbox_id, source_ip, state))
}

/// Kubelet's retry of [`apply_rollback_of_a_restore_with_a_durable_source`]'s
/// pod brought a gen-3 runtime up anyway, and when the pod is reaped its
/// teardown suspend registers that runtime's capture. With retirement
/// enabled the rollback retired the execution as `replaced`, so the capture
/// is refused rather than becoming the snapshot the next restore selects, and
/// the next restore still selects the pre-restore snapshot.
#[tokio::test(flavor = "current_thread")]
async fn a_rolled_back_restore_cannot_register_a_teardown_snapshot() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let (sandbox_id, source_ip, state) = apply_rollback_of_a_restore_with_a_durable_source(
        &fixture,
        AbandonedRestoreRetirement::Enabled,
    )
    .await?;
    assert_eq!(state, "replaced");
    // The retirement cleared the placement clock the restore's pod moved, so
    // the suspended selector still takes the pre-restore snapshot.
    let restorable: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(
        format!(
            "SELECT {} FROM sandboxes s WHERE s.id = $1",
            pplx_space_sandbox_portability::fresh_ready_snapshot_exists("s")
        )
        .as_str()
    )))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?;
    assert!(
        restorable,
        "the next wake must restore from the pre-restore snapshot"
    );

    let relaunched_capture = SnapshotId::new();
    let error = begin_teardown_backup(
        &fixture.service,
        source_ip,
        sandbox_id,
        relaunched_capture,
        3,
    )
    .await
    .expect_err("a rolled-back restore's runtime must not register a snapshot");
    assert_superseded_refusal(&error, 3);
    assert_eq!(snapshot_row_count(pool, relaunched_capture).await?, 0);
    Ok(())
}

/// With retirement disabled, a routing replica from before the restoring gate
/// may have served the relaunched runtime, so the rollback retires the
/// execution as `suspended` and the same capture registers.
#[tokio::test(flavor = "current_thread")]
async fn a_rolled_back_restore_keeps_its_capture_with_retirement_disabled() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let (sandbox_id, source_ip, state) = apply_rollback_of_a_restore_with_a_durable_source(
        &fixture,
        AbandonedRestoreRetirement::Disabled,
    )
    .await?;
    assert_eq!(state, "suspended");

    let relaunched_capture = SnapshotId::new();
    begin_teardown_backup(
        &fixture.service,
        source_ip,
        sandbox_id,
        relaunched_capture,
        3,
    )
    .await?;
    assert_eq!(
        snapshot_state(&fixture.service.pool, relaunched_capture).await?,
        "creating"
    );
    Ok(())
}
