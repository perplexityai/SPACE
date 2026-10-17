//! `BeginArtifactBackup`'s execution-generation fence: a teardown suspend of a
//! runtime the control plane has retired must not register a snapshot.

use super::*;

use pplx_space_common_core::types::ExecutionGeneration;
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
        execution_generation: Some(execution_generation),
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
/// after it retired.
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

    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET pod_uid = 'pod-uid-6'
          WHERE sandbox_id = $1 AND generation = 6"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    let refused = SnapshotId::new();
    let error = begin_teardown_backup(&fixture.service, source_ip, sandbox_id, refused, 5)
        .await
        .expect_err("a newer execution that materialized a pod supersedes this one");
    assert_superseded_refusal(&error, 5);
    assert_eq!(snapshot_row_count(pool, refused).await?, 0);
    Ok(())
}
