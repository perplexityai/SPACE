//! The `*_reads` modules decode what their callers decoded before the
//! statements moved here: the port-exposure list both of its callers share,
//! the execution reads that alias or trim columns, and the ownership probe
//! two public routes share. With them, the other reads that moved here with
//! callers in other crates: the Kubernetes node attribution's target,
//! fork-copy and latest-execution legs and its UID filter, and the quarantine
//! and tombstone counts.

use pplx_space_db_test_fixtures::insert_ready_worker_node;
use pplx_space_snapshot_model::SnapshotId;

use super::*;
use crate::{
    active_quarantines, debug_reads, deleted_rows, k8s_executions, k8s_node_attribution,
    port_exposures, row_reads,
};

async fn insert_port_exposure(
    pool: &PgPool,
    sandbox_id: Uuid,
    guest_port: i32,
    desired_state: &str,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_port_exposures
            (sandbox_id, protocol, guest_port, visibility, source, desired_state, apply_state)
         VALUES ($1, 'tcp', $2, 'private', 'api', $3, 'pending')"
    ))
    .bind(sandbox_id)
    .bind(guest_port)
    .bind(desired_state)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_execution(
    pool: &PgPool,
    sandbox_id: Uuid,
    generation: i64,
    pod_uid: &str,
    k8s_node_name: Option<&str>,
    state: &str,
    active: bool,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip,
             k8s_node_name, state, active)
         VALUES ($1, $2, 'pod-' || $2::TEXT, 'sandboxes', $3, '10.1.0.7', $4, $5, $6)"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(pod_uid)
    .bind(k8s_node_name)
    .bind(state)
    .bind(active)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn port_exposures_filter_and_order_one_sandboxs_rows() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = insert_sandbox(&db.pool, None, "started").await?;
    let other = insert_sandbox(&db.pool, None, "started").await?;
    insert_port_exposure(&db.pool, id, 8080, "closed").await?;
    insert_port_exposure(&db.pool, id, 3000, "open").await?;
    insert_port_exposure(&db.pool, other, 22, "open").await?;
    let sandbox_id = SandboxId::from_uuid(id);

    let ports = |rows: Vec<port_exposures::PortExposureRecord>| {
        rows.iter().map(|row| row.guest_port).collect::<Vec<_>>()
    };
    assert_eq!(
        ports(port_exposures::list(&db.pool, sandbox_id).await?),
        [3000, 8080]
    );
    assert_eq!(
        ports(port_exposures::list_open(&db.pool, sandbox_id).await?),
        [3000]
    );
    let closed = port_exposures::get_tcp(&db.pool, sandbox_id, 8080)
        .await?
        .expect("the closed exposure is still a row");
    assert_eq!(closed.desired_state, "closed");
    assert!(port_exposures::any_open(&db.pool, sandbox_id).await?);
    assert!(!port_exposures::any_open(&db.pool, SandboxId::new()).await?);
    assert!(port_exposures::open_exists(&db.pool, sandbox_id, "tcp", 3000).await?);
    assert!(!port_exposures::open_exists(&db.pool, sandbox_id, "tcp", 8080).await?);
    assert_eq!(
        port_exposures::count_other_open(&db.pool, sandbox_id, "tcp", 3000).await?,
        0
    );
    assert_eq!(
        port_exposures::count_other_open(&db.pool, sandbox_id, "tcp", 8080).await?,
        1
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn k8s_executions_decode_the_columns_they_alias_or_trim() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = insert_sandbox(&db.pool, None, "started").await?;
    insert_execution(&db.pool, id, 1, " ", Some(" node-a "), "forgotten", false).await?;
    insert_execution(&db.pool, id, 2, "uid-2", None, "running", true).await?;
    let sandbox_id = SandboxId::from_uuid(id);

    assert_eq!(
        k8s_executions::latest_k8s_node_name(&db.pool, sandbox_id).await?,
        Some(" node-a ".to_string())
    );
    assert_eq!(
        k8s_executions::inactive_k8s_node_names(&db.pool, sandbox_id).await?,
        ["node-a"]
    );
    assert!(k8s_executions::successor_exists(&db.pool, sandbox_id, 1).await?);
    assert_eq!(
        k8s_executions::unclaimed_generation(&db.pool, sandbox_id).await?,
        None,
        "an active execution is reused, not replaced"
    );
    assert_eq!(
        k8s_executions::unclaimed_generation(&db.pool, SandboxId::new()).await?,
        Some(1)
    );
    assert!(k8s_executions::active_is_generation(&db.pool, sandbox_id, 2).await?);
    assert!(!k8s_executions::active_is_generation(&db.pool, sandbox_id, 1).await?);
    assert_eq!(
        k8s_executions::active_in_state(&db.pool, sandbox_id, 2, "running").await?,
        Some(true)
    );
    assert_eq!(
        k8s_executions::active_in_state(&db.pool, sandbox_id, 1, "forgotten").await?,
        None
    );
    assert!(k8s_executions::generation_retired(&db.pool, sandbox_id, 1, "pod-1").await?);

    let recovering = k8s_executions::recovering_execution(
        &db.pool,
        sandbox_id,
        1,
        ["creating", "recovering", "running"],
    )
    .await?
    .expect("the running active execution counts");
    assert_eq!(recovering.generation, 2);
    assert_eq!(recovering.pod_ip.as_deref(), Some("10.1.0.7"));
    assert_eq!(recovering.lifecycle, "started");

    let status = k8s_executions::active_execution_status(&db.pool, sandbox_id, 3600)
        .await?
        .expect("generation 2 is active");
    assert_eq!(
        (
            status.generation,
            status.state.as_str(),
            status.lifecycle.as_str()
        ),
        (2, "running", "started")
    );
    assert_eq!(
        status.pod_uid.as_ref().map(|uid| uid.as_str()),
        Some("uid-2")
    );
    assert!(
        !status.create_age_expired,
        "an hour has not passed since the insert"
    );
    assert_eq!(
        row_reads::restore_lease_remaining_secs(&db.pool, sandbox_id).await?,
        0.0,
        "a row with no lease has none left"
    );

    let executions = debug_reads::executions(&db.pool, sandbox_id).await?;
    let pod_uids = executions
        .iter()
        .map(|row| {
            (
                row.generation,
                row.pod_uid.as_ref().map(|uid| uid.as_str().to_string()),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(pod_uids, [(2, Some("uid-2".to_string())), (1, None)]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn org_owns_requires_the_row_and_its_organization() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = SandboxId::from_uuid(insert_sandbox(&db.pool, None, "deleted").await?);

    assert!(row_reads::org_owns(&db.pool, id, OrgId::from_uuid(SEED_ORG)).await?);
    assert!(!row_reads::org_owns(&db.pool, id, OrgId::from_uuid(Uuid::now_v7())).await?);
    assert!(!row_reads::org_owns(&db.pool, SandboxId::new(), OrgId::from_uuid(SEED_ORG)).await?);
    Ok(())
}

/// A Kubernetes worker registered with UID `uid`.
async fn insert_k8s_node(pool: &PgPool, uid: &str) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    insert_ready_worker_node(pool, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, $2, $3)"
    ))
    .bind(node_id)
    .bind(format!("ip-{node_id}.ec2.internal"))
    .bind(uid)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// The UIDs of the nodes attributed to `sandbox_id` and `snapshot_id`,
/// sorted.
async fn attributed_uids(
    pool: &PgPool,
    sandbox_id: Uuid,
    snapshot_id: SnapshotId,
) -> TestResult<Vec<String>> {
    let mut uids = k8s_node_attribution::attributed_k8s_nodes(
        pool,
        SandboxId::from_uuid(sandbox_id),
        snapshot_id,
    )
    .await?
    .into_iter()
    .map(|node| node.k8s_node_uid.as_str().to_owned())
    .collect::<Vec<_>>();
    uids.sort();
    Ok(uids)
}

#[tokio::test(flavor = "current_thread")]
async fn attributed_k8s_nodes_include_the_target_placement() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let target = insert_k8s_node(pool, "uid-target").await?;
    // No active node, execution or snapshot: only the target leg can
    // attribute a node.
    let id = insert_sandbox(pool, None, "started").await?;
    let snapshot_id = SnapshotId::new();
    assert_eq!(
        attributed_uids(pool, id, snapshot_id).await?,
        Vec::<String>::new()
    );

    sqlx::query(sql!(
        "UPDATE sandboxes SET target_node_id = $2 WHERE id = $1"
    ))
    .bind(id)
    .bind(target)
    .execute(pool)
    .await?;
    assert_eq!(
        attributed_uids(pool, id, snapshot_id).await?,
        ["uid-target"]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn attributed_k8s_nodes_include_a_same_org_fork_copys_source_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let source_node = insert_k8s_node(pool, "uid-source").await?;
    let subject = insert_sandbox(pool, None, "started").await?;
    let same_org_owner = insert_sandbox(pool, None, "started").await?;
    let other_org = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug) VALUES ($1, 'Other Org', $2)"
    ))
    .bind(other_org)
    .bind(format!("other-org-{other_org}"))
    .execute(pool)
    .await?;
    let other_org_owner = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         VALUES ($1, $2, $3, 'k8s', 'started', 1)"
    ))
    .bind(other_org_owner)
    .bind(SEED_USER)
    .bind(other_org)
    .execute(pool)
    .await?;

    // Each snapshot belongs to another sandbox, so only the fork-copy leg
    // can attribute its source node to `subject`.
    for (owner, org_id, origin, expected, why) in [
        (
            same_org_owner,
            SEED_ORG,
            "fork",
            &["uid-source"][..],
            "a same-org fork copy",
        ),
        (
            same_org_owner,
            SEED_ORG,
            "suspend",
            &[],
            "another sandbox's own snapshot",
        ),
        (
            other_org_owner,
            other_org,
            "fork",
            &[],
            "another org's fork copy",
        ),
    ] {
        let snapshot_id = SnapshotId::new();
        sqlx::query(sql!(
            "INSERT INTO snapshots (
                 id, sandbox_id, type, state, source_node_id, org_id,
                 local_artifact_id, artifact_kind, artifact_origin
             )
             VALUES ($1, $2, 'full', 'ready', $3, $4, $5, 'full', $6)"
        ))
        .bind(snapshot_id)
        .bind(owner)
        .bind(source_node)
        .bind(org_id)
        .bind(Uuid::now_v7())
        .bind(origin)
        .execute(pool)
        .await?;
        assert_eq!(
            attributed_uids(pool, subject, snapshot_id).await?,
            expected,
            "{why}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn attributed_k8s_nodes_take_only_the_latest_execution_and_need_a_uid() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let older = insert_k8s_node(pool, "uid-older").await?;
    let newer = insert_k8s_node(pool, "uid-newer").await?;
    let without_uid = insert_k8s_node(pool, "uid-cleared").await?;
    sqlx::query(sql!(
        "UPDATE node_k8s SET k8s_node_uid = NULL WHERE node_id = $1"
    ))
    .bind(without_uid)
    .execute(pool)
    .await?;
    // The active node has no UID, so it is skipped.
    let id = insert_sandbox(pool, None, "started").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
    ))
    .bind(id)
    .bind(without_uid)
    .execute(pool)
    .await?;
    // Two executions on different nodes: only the newer one's node counts.
    let older_name = format!("ip-{older}.ec2.internal");
    let newer_name = format!("ip-{newer}.ec2.internal");
    insert_execution(
        pool,
        id,
        1,
        "pod-uid-1",
        Some(&older_name),
        "forgotten",
        false,
    )
    .await?;
    insert_execution(pool, id, 2, "pod-uid-2", Some(&newer_name), "running", true).await?;

    assert_eq!(
        attributed_uids(pool, id, SnapshotId::new()).await?,
        ["uid-newer"]
    );
    Ok(())
}

async fn insert_quarantine_episode(pool: &PgPool, sandbox_id: Uuid) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_quarantine_state
             (sandbox_id, quarantine_id, entered_from_lifecycle, source, reason_code,
              reason_message, next_action)
         VALUES ($1, $2, 'paused', 'sbx_manager', 'pause_failed', 'pause timed out',
                 'requires_operator_attention')"
    ))
    .bind(sandbox_id)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn active_quarantines_count_only_rows_still_quarantined() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let before = active_quarantines::count(pool).await?;
    let quarantined = insert_sandbox(pool, None, "quarantined").await?;
    let moved_on = insert_sandbox(pool, None, "suspended").await?;
    for id in [quarantined, moved_on] {
        insert_quarantine_episode(pool, id).await?;
    }
    // Quarantined, but with no episode to count.
    insert_sandbox(pool, None, "quarantined").await?;

    assert_eq!(active_quarantines::count(pool).await?, before + 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn deleted_rows_count_each_tombstone_once() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let before = deleted_rows::count(pool).await?;
    insert_sandbox(pool, None, "deleted").await?;
    insert_sandbox(pool, None, "deleting").await?;
    insert_sandbox(pool, None, "started").await?;

    assert_eq!(deleted_rows::count(pool).await?, before + 1);
    Ok(())
}
