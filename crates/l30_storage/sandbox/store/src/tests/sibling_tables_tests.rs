//! Characterization of the sibling-table verbs at the store layer: the
//! port-exposure upserts, apply outcomes, invalidations and node apply
//! reports, the ECW credential epoch advance, the runtime-access read and the
//! published snapshot upsert.

use super::*;
use crate::{ecw_credential_epochs, port_exposures, runtime_access, snapshot_inventory};

async fn insert_node(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.10', 'worker', 'k8s', 'ready')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(node_id)
}

async fn place_on(pool: &PgPool, id: Uuid, node_id: NodeId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
    ))
    .bind(id)
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// `apply_state`, `last_applied_node_id`, `last_error`, `last_bridge_status`,
/// `last_observed_at IS NOT NULL`, as stored.
type Observation = (String, Option<Uuid>, Option<String>, Option<String>, bool);

async fn read_observation(pool: &PgPool, id: Uuid, port: i32) -> TestResult<Observation> {
    Ok(sqlx::query_as::<_, Observation>(sql!(
        "SELECT apply_state, last_applied_node_id, last_error, last_bridge_status,
                last_observed_at IS NOT NULL
           FROM sandbox_port_exposures
          WHERE sandbox_id = $1 AND protocol = 'tcp' AND guest_port = $2"
    ))
    .bind(id)
    .bind(port)
    .fetch_one(pool)
    .await?)
}

async fn mark_applied(pool: &PgPool, id: Uuid, node: Uuid) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandbox_port_exposures
            SET apply_state = 'applied', last_applied_node_id = $2,
                last_bridge_status = 'direct', last_observed_at = NOW()
          WHERE sandbox_id = $1"
    ))
    .bind(id)
    .bind(node)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn port_exposure_upserts_record_the_intent_and_keep_the_last_observation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = insert_sandbox(&db.pool, None, "started").await?;
    let sandbox = SandboxId::from_uuid(id);
    let node = Uuid::now_v7();

    let opened =
        port_exposures::upsert_open(&db.pool, sandbox, "tcp", 8080, "public", "api").await?;
    assert_eq!(
        (
            opened.sandbox_id,
            opened.guest_port,
            opened.visibility.as_str(),
            opened.desired_state.as_str(),
            opened.apply_state.as_str(),
        ),
        (sandbox, 8080, "public", "open", "pending")
    );

    mark_applied(&db.pool, id, node).await?;
    let closed = port_exposures::upsert_closed(&db.pool, sandbox, "tcp", 8080).await?;
    assert_eq!(
        (
            closed.visibility.as_str(),
            closed.desired_state.as_str(),
            closed.apply_state.as_str(),
            closed.last_applied_node_id,
            closed.last_bridge_status.as_deref(),
        ),
        (
            "public",
            "closed",
            "pending",
            Some(NodeId::from_uuid(node)),
            Some("direct")
        ),
        "a close keeps the visibility and what the node last applied"
    );
    sqlx::query(sql!(
        "UPDATE sandbox_port_exposures SET last_error = 'stale'
          WHERE sandbox_id = $1 AND protocol = 'tcp' AND guest_port = 8080"
    ))
    .bind(id)
    .execute(&db.pool)
    .await?;
    let reopened =
        port_exposures::upsert_open(&db.pool, sandbox, "tcp", 8080, "private", "api").await?;
    assert_eq!(
        (
            reopened.visibility.as_str(),
            reopened.desired_state.as_str(),
            reopened.apply_state.as_str(),
            reopened.last_applied_node_id,
            reopened.last_bridge_status.as_deref(),
            reopened.last_error.as_deref(),
        ),
        (
            "private",
            "open",
            "pending",
            Some(NodeId::from_uuid(node)),
            Some("direct"),
            None
        ),
        "a re-open takes the new visibility, clears the error and keeps what the node last applied"
    );

    let never_opened = port_exposures::upsert_closed(&db.pool, sandbox, "tcp", 9090).await?;
    assert_eq!(
        (
            never_opened.visibility.as_str(),
            never_opened.source.as_str(),
            never_opened.desired_state.as_str(),
        ),
        ("private", "api", "closed")
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn port_exposure_invalidations_reset_only_open_exposures() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = insert_sandbox(&db.pool, None, "started").await?;
    let sandbox = SandboxId::from_uuid(id);
    let node = Uuid::now_v7();
    port_exposures::upsert_open(&db.pool, sandbox, "tcp", 8080, "private", "api").await?;
    port_exposures::upsert_open(&db.pool, sandbox, "tcp", 8081, "private", "api").await?;
    port_exposures::upsert_closed(&db.pool, sandbox, "tcp", 9090).await?;
    mark_applied(&db.pool, id, node).await?;
    let applied = (
        "applied".to_string(),
        Some(node),
        None,
        Some("direct".to_string()),
        true,
    );
    let reset = ("pending".to_string(), None, None, None, false);

    let invalidated = port_exposures::invalidate_open_tcp_exposure_after_proxy_failure(
        &db.pool,
        sandbox,
        8080,
        "proxy could not connect",
    )
    .await?;
    assert!(invalidated);
    assert_eq!(
        read_observation(&db.pool, id, 8080).await?,
        (
            "pending".to_string(),
            None,
            Some("proxy could not connect".to_string()),
            None,
            false
        )
    );
    assert_eq!(read_observation(&db.pool, id, 8081).await?, applied);
    assert!(
        !port_exposures::invalidate_open_tcp_exposure_after_proxy_failure(
            &db.pool, sandbox, 9090, "closed",
        )
        .await?,
        "a closed exposure is not invalidated"
    );

    let mut tx = db.pool.begin().await?;
    port_exposures::invalidate_open_observations(&mut *tx, sandbox).await?;
    tx.commit().await?;
    assert_eq!(read_observation(&db.pool, id, 8080).await?, reset);
    assert_eq!(read_observation(&db.pool, id, 8081).await?, reset);
    assert_eq!(read_observation(&db.pool, id, 9090).await?, applied);
    Ok(())
}

/// Every column of one TCP exposure, as stored.
async fn read_exposure(
    pool: &PgPool,
    id: Uuid,
    port: i32,
) -> TestResult<port_exposures::PortExposureRecord> {
    Ok(
        port_exposures::get_tcp(pool, SandboxId::from_uuid(id), port)
            .await?
            .expect("exposure exists"),
    )
}

#[tokio::test(flavor = "current_thread")]
async fn port_exposure_mark_applied_records_the_observation_only_while_open() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = insert_sandbox(&db.pool, None, "started").await?;
    let sandbox = SandboxId::from_uuid(id);
    let node = NodeId::new();
    let observation = port_exposures::BridgeObservation {
        bridge_status: "direct",
        service_generation: Some("gen-1"),
    };
    port_exposures::upsert_open(&db.pool, sandbox, "tcp", 8080, "private", "api").await?;

    let applied = port_exposures::mark_applied(&db.pool, sandbox, 8080, node, observation)
        .await?
        .expect("an open exposure is marked applied");
    assert_eq!(applied, read_exposure(&db.pool, id, 8080).await?);
    assert_eq!(
        (
            applied.apply_state.as_str(),
            applied.last_applied_node_id,
            applied.last_bridge_status.as_deref(),
            applied.last_applied_bridge_generation.as_deref(),
            applied.last_observed_at.is_some(),
        ),
        ("applied", Some(node), Some("direct"), Some("gen-1"), true)
    );

    port_exposures::upsert_closed(&db.pool, sandbox, "tcp", 8080).await?;
    let closed = read_exposure(&db.pool, id, 8080).await?;
    assert_eq!(
        port_exposures::mark_applied(&db.pool, sandbox, 8080, NodeId::new(), observation).await?,
        None,
        "a close that landed first wins"
    );
    assert_eq!(read_exposure(&db.pool, id, 8080).await?, closed);
    assert_eq!(
        port_exposures::mark_applied(&db.pool, sandbox, 9090, node, observation).await?,
        None,
        "an exposure that never existed is not created"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn port_exposure_mark_failed_loses_to_a_fresh_success_on_the_same_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = insert_sandbox(&db.pool, None, "started").await?;
    let sandbox = SandboxId::from_uuid(id);
    let node = NodeId::new();
    let observation = port_exposures::BridgeObservation {
        bridge_status: "loopback_forwarded",
        service_generation: Some("gen-1"),
    };
    port_exposures::upsert_open(&db.pool, sandbox, "tcp", 8080, "private", "api").await?;
    port_exposures::mark_applied(&db.pool, sandbox, 8080, node, observation).await?;
    let applied = read_exposure(&db.pool, id, 8080).await?;

    assert!(
        !port_exposures::mark_failed(&db.pool, sandbox, 8080, node, "late failure", None, 60)
            .await?,
        "a fresh success on the same node wins"
    );
    assert_eq!(read_exposure(&db.pool, id, 8080).await?, applied);

    let failure = port_exposures::BridgeObservation {
        bridge_status: "bind_conflict",
        service_generation: None,
    };
    let other_node = NodeId::new();
    assert!(
        port_exposures::mark_failed(
            &db.pool,
            sandbox,
            8080,
            other_node,
            "bind conflict",
            Some(failure),
            60,
        )
        .await?,
        "a failure on another node replaces the success"
    );
    let failed = read_exposure(&db.pool, id, 8080).await?;
    assert_eq!(
        (
            failed.apply_state.as_str(),
            failed.last_applied_node_id,
            failed.last_error.as_deref(),
            failed.last_bridge_status.as_deref(),
            failed.last_applied_bridge_generation.as_deref(),
        ),
        (
            "failed",
            Some(node),
            Some("bind conflict"),
            Some("bind_conflict"),
            Some("gen-1")
        ),
        "the failure keeps the last applied node and generation it did not report"
    );

    port_exposures::mark_applied(&db.pool, sandbox, 8080, node, observation).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_port_exposures
            SET last_observed_at = NOW() - INTERVAL '120 seconds'
          WHERE sandbox_id = $1 AND protocol = 'tcp' AND guest_port = 8080"
    ))
    .bind(id)
    .execute(&db.pool)
    .await?;
    assert!(
        port_exposures::mark_failed(&db.pool, sandbox, 8080, node, "stale", None, 60).await?,
        "a success on the same node older than the freshness window loses"
    );
    assert_eq!(
        read_exposure(&db.pool, id, 8080).await?.apply_state,
        "failed"
    );

    port_exposures::upsert_closed(&db.pool, sandbox, "tcp", 8080).await?;
    let closed = read_exposure(&db.pool, id, 8080).await?;
    assert!(
        !port_exposures::mark_failed(&db.pool, sandbox, 8080, node, "closed", None, 60).await?,
        "a closed exposure is not marked failed"
    );
    assert_eq!(read_exposure(&db.pool, id, 8080).await?, closed);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn ecw_credential_epoch_advances_from_the_implicit_first_epoch() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = SandboxId::from_uuid(insert_sandbox(&db.pool, None, "started").await?);

    let advanced = [
        ecw_credential_epochs::advance(&db.pool, id).await?,
        ecw_credential_epochs::advance(&db.pool, id).await?,
    ]
    .map(|epoch| epoch.map(|epoch| epoch.get()));
    assert_eq!(advanced, [Some(2), Some(3)]);
    assert_eq!(
        ecw_credential_epochs::advance(&db.pool, SandboxId::new()).await?,
        None
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn runtime_access_reports_a_recent_lifecycle_change_by_the_grace_window() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = SandboxId::from_uuid(insert_sandbox(&db.pool, None, "started").await?);

    let recent = runtime_access::runtime_access(&db.pool, id, 3600.0).await?;
    let stale = runtime_access::runtime_access(&db.pool, id, 0.0).await?;
    assert_eq!(
        [recent, stale].map(|row| row.map(|row| (row.lifecycle, row.lifecycle_changed_recently))),
        [
            Some(("started".to_string(), true)),
            Some(("started".to_string(), false))
        ]
    );
    assert_eq!(
        runtime_access::runtime_access(&db.pool, SandboxId::new(), 3600.0).await?,
        None
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn published_snapshots_upsert_in_scope_and_skip_unchanged_rows() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let in_scope = SandboxId::from_uuid(insert_sandbox(&db.pool, None, "started").await?);
    let out_of_scope = SandboxId::from_uuid(insert_sandbox(&db.pool, None, "started").await?);
    // The inventory's foreign key is to the observed state, not the row.
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.10', 'worker', 'k8s', 'ready')"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    for sandbox_id in [in_scope, out_of_scope] {
        sqlx::query(sql!(
            "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status)
             VALUES ($1, $2, 'running')"
        ))
        .bind(sandbox_id)
        .bind(node_id)
        .execute(&db.pool)
        .await?;
    }
    let snapshot = |sandbox_id: SandboxId, state: &str| {
        serde_json::json!({
            "sandbox_id": sandbox_id.to_string(),
            "snapshot_id": "snap-1",
            "type": "full",
            "state": state,
            "size_bytes": null,
            "locations": null,
        })
    };
    let read = |sandbox_id: SandboxId| {
        let pool = db.pool.clone();
        async move {
            sqlx::query_as::<_, (String, i64, serde_json::Value, time::OffsetDateTime)>(sql!(
                "SELECT state, size_bytes, locations, updated_at
                   FROM sandbox_snapshot_inventory
                  WHERE sandbox_id = $1 AND snapshot_id = 'snap-1'"
            ))
            .bind(sandbox_id)
            .fetch_optional(&pool)
            .await
        }
    };

    snapshot_inventory::upsert_published(
        &db.pool,
        &[in_scope],
        serde_json::json!([snapshot(in_scope, "ready"), snapshot(out_of_scope, "ready")]),
    )
    .await?;
    let (state, size_bytes, locations, first_write) = read(in_scope)
        .await?
        .expect("the in-scope snapshot is written");
    assert_eq!(
        (state.as_str(), size_bytes, locations),
        ("ready", 0, serde_json::json!([]))
    );
    assert!(
        read(out_of_scope).await?.is_none(),
        "a sandbox outside the scope is not written"
    );

    snapshot_inventory::upsert_published(
        &db.pool,
        &[in_scope],
        serde_json::json!([snapshot(in_scope, "ready")]),
    )
    .await?;
    let unchanged = read(in_scope).await?.expect("still present");
    assert_eq!(
        unchanged.3, first_write,
        "an unchanged row is not rewritten"
    );

    snapshot_inventory::upsert_published(
        &db.pool,
        &[in_scope],
        serde_json::json!([snapshot(in_scope, "deleting")]),
    )
    .await?;
    assert_eq!(
        read(in_scope).await?.map(|row| row.0),
        Some("deleting".to_string())
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn apply_state_reports_update_only_sandboxes_active_on_the_reporting_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let reporter = insert_node(&db.pool).await?;
    let other = insert_node(&db.pool).await?;
    let here = insert_sandbox(&db.pool, None, "started").await?;
    let elsewhere = insert_sandbox(&db.pool, None, "started").await?;
    place_on(&db.pool, here, reporter).await?;
    place_on(&db.pool, elsewhere, other).await?;
    for id in [here, elsewhere] {
        port_exposures::upsert_open(
            &db.pool,
            SandboxId::from_uuid(id),
            "tcp",
            8080,
            "private",
            "api",
        )
        .await?;
    }
    let report = |id: Uuid, guest_port: i32| port_exposures::ApplyStateReport {
        sandbox_id: SandboxId::from_uuid(id),
        protocol: "tcp",
        guest_port,
        apply_state: "applied",
        error: None,
        bridge_status: Some("direct"),
        service_generation: Some("gen-1"),
    };

    let missed = port_exposures::record_apply_states(
        &db.pool,
        reporter,
        &[
            report(here, 8080),
            report(elsewhere, 8080),
            report(here, 9090),
        ],
    )
    .await?;

    let missed = missed
        .into_iter()
        .map(|row| (row.sandbox_id, row.protocol, row.guest_port))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        missed,
        [
            (SandboxId::from_uuid(elsewhere), "tcp".to_string(), 8080),
            (SandboxId::from_uuid(here), "tcp".to_string(), 9090),
        ]
        .into_iter()
        .collect(),
        "a sandbox placed on another node and an exposure that doesn't exist come back missed"
    );
    assert_eq!(
        read_observation(&db.pool, here, 8080).await?,
        (
            "applied".to_string(),
            Some(reporter.as_uuid()),
            None,
            Some("direct".to_string()),
            true
        )
    );
    assert_eq!(
        read_observation(&db.pool, elsewhere, 8080).await?,
        ("pending".to_string(), None, None, None, false),
        "the other node's exposure is left as it was"
    );
    Ok(())
}
