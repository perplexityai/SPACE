//! The `*_reads` modules decode what their callers decoded before the
//! statements moved here: the port-exposure list both of its callers share,
//! the execution reads that alias or trim columns, and the ownership probe
//! two public routes share.

use super::*;
use crate::{debug_reads, k8s_executions, port_exposures, row_reads};

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
