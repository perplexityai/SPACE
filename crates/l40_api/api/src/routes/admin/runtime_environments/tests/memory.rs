//! Route tests for `PUT /runtime-environments/{environment_ref}/memory`:
//! Direct-substrate `501` refusal, feature-flag gating, certified-edge checks,
//! lifecycle refusals, restore-time memory-drop coverage, rollback when
//! `SPACE_MEMORY_DROP_ENABLED` is turned back off, `If-Match` optimistic
//! concurrency, audit logging, and reconcile trigger enqueueing.

use axum::http::StatusCode;

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn retire_returns_501_on_direct_substrate_while_unretire_clears_existing_flag() -> TestResult
{
    let Some(h) = harness_with_memory_drop_direct().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    assert_eq!(h.set_default(e1).await.0, StatusCode::OK);
    assert_eq!(
        h.set_state(e0, json!({"state": "deprecated", "reason": "drain E0"}))
            .await
            .0,
        StatusCode::OK
    );

    let (status, body) = h
        .set_memory(
            e0,
            json!({"action": "retire", "reason": "direct substrate cannot enforce"}),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{body}");
    assert_error_contains(&body, "Direct substrate");

    // Seed an existing retired flag in Postgres and verify `unretire` still clears
    // it on Direct so an operator can always recover policy state.
    let change = policy_store::PolicyChange {
        actor: Some(h.user_id),
        reason: policy_store::PolicyChangeReason::new("seed retired")?,
    };
    let mut tx = h.db.pool.begin().await?;
    policy_store::set_environment_memory_retired(&mut tx, e0, true, &change).await?;
    tx.commit().await?;

    let (status, body) = h
        .set_memory(
            e0,
            json!({"action": "unretire", "reason": "clear retired flag on direct"}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["memory_retired"], false);
    Ok(())
}

/// Kubernetes-only because `retire` requires
/// `Substrate::check_environment_memory_retirement()` (see
/// [`crate::controllers::substrate::direct`] `# Gaps`:
/// `Runtime-environment memory retirement`).
#[cfg(feature = "kubernetes")]
#[tokio::test(flavor = "current_thread")]
async fn retire_requires_memory_drop_enabled_while_unretire_rolls_back_without_it() -> TestResult {
    let Some(h) = harness_with_memory_drop().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    insert_catalog_environment(&h.db.pool, e0, "1.15.1", "9.0.0", &[e1], &[]).await?;
    insert_catalog_environment(&h.db.pool, e1, "1.17.0", "12.0.0", &[], &[]).await?;
    assert_eq!(h.set_default(e1).await.0, StatusCode::OK);
    assert_eq!(
        h.set_state(e0, json!({"state": "deprecated", "reason": "drain E0"}))
            .await
            .0,
        StatusCode::OK
    );

    // Build a second Kubernetes router over the same database with
    // SPACE_MEMORY_DROP_ENABLED=false.
    let state_disabled = state_with_kubernetes_substrate(h.db.pool.clone()).await?;
    assert!(!state_disabled.memory_drop_enabled);
    let app_disabled =
        crate::routes::admin::shared_host_router(state_disabled.clone()).with_state(state_disabled);

    // Retiring is refused with 503 when SPACE_MEMORY_DROP_ENABLED is off.
    let (status, body) = call(
        &app_disabled,
        &h.token,
        Method::PUT,
        &memory_path(e0),
        Some(json!({"action": "retire", "reason": "retire E0 memory"})),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_error_contains(&body, "memory drop is disabled");

    // Retiring succeeds on the enabled router.
    let (status, body) = h
        .set_memory(
            e0,
            json!({"action": "retire", "reason": "retire E0 memory"}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({
            "generation": 3,
            "environment_ref": e0.to_string(),
            "memory_retired": true,
        })
    );

    // Unretiring succeeds even on the router where SPACE_MEMORY_DROP_ENABLED is off,
    // so flipping the flag off never strands a retired environment.
    let (status, body) = call(
        &app_disabled,
        &h.token,
        Method::PUT,
        &memory_path(e0),
        Some(json!({"action": "unretire", "reason": "roll back flag"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({
            "generation": 4,
            "environment_ref": e0.to_string(),
            "memory_retired": false,
        })
    );
    Ok(())
}

/// Kubernetes-only because `retire` requires
/// `Substrate::check_environment_memory_retirement()` (see
/// [`crate::controllers::substrate::direct`] `# Gaps`:
/// `Runtime-environment memory retirement`).
#[cfg(feature = "kubernetes")]
#[tokio::test(flavor = "current_thread")]
async fn lifecycle_guards_and_read_projection_preserve_state_and_memory_history() -> TestResult {
    let Some(h) = harness_with_memory_drop().await? else {
        return Ok(());
    };
    let (e0, e1, e2) = (environment(0), environment(1), environment(2));
    insert_catalog_environment(&h.db.pool, e1, "1.17.0", "12.0.0", &[], &[]).await?;
    insert_catalog_environment(&h.db.pool, e2, "1.18.0", "13.0.0", &[], &[]).await?;

    // Retiring before a cluster default exists is refused with 409.
    let (status, body) = h
        .set_memory(e0, json!({"action": "retire", "reason": "too early"}))
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error_contains(&body, "cluster default");

    assert_eq!(h.set_default(e1).await.0, StatusCode::OK);

    // Retiring an allowed environment is refused with 409.
    let (status, body) = h
        .set_memory(e1, json!({"action": "retire", "reason": "still allowed"}))
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error_contains(&body, "deprecated");

    assert_eq!(
        h.set_state(e0, json!({"state": "deprecated", "reason": "superseded"}))
            .await
            .0,
        StatusCode::OK
    );

    // Retiring without a certified upgrade edge to `e1` is refused with 409 even
    // when `strand: true` is supplied, without advancing generation.
    let (status, body) = h
        .set_memory(
            e0,
            json!({"action": "retire", "reason": "missing upgrade edge", "strand": true}),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error_contains(&body, "no certified upgrade edge");

    // Register `e0 -> e1` certified upgrade edge so retirement can convert restores.
    insert_catalog_environment(&h.db.pool, e0, "1.15.1", "9.0.0", &[e1], &[]).await?;

    // If-Match mismatch returns 412 without advancing generation or queuing reconcile.
    let worker = NodeId::new();
    insert_ready_worker_node(&h.db.pool, worker).await?;
    let (status, body) = h
        .put_if_match(
            &memory_path(e0),
            json!({"action": "retire", "reason": "stale generation"}),
            "\"99\"",
        )
        .await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED, "{body}");
    let refused_triggers: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM node_reconcile_triggers WHERE node_id = $1"
    ))
    .bind(worker)
    .fetch_one(&h.db.pool)
    .await?;
    assert_eq!(refused_triggers, 0);

    // Matching If-Match succeeds, records the audit event, and enqueues a reconcile trigger.
    let (status, body) = h
        .put_if_match(
            &memory_path(e0),
            json!({"action": "retire", "reason": "retire E0 checkpoints"}),
            "\"2\"",
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["generation"], 3);
    assert_eq!(body["memory_retired"], true);

    let (status, get_body) = h.get("/admin/api/v1/runtime-environments").await;
    assert_eq!(status, StatusCode::OK, "{get_body}");
    assert_eq!(get_body["generation"], 3);
    assert_eq!(get_body["last_change"]["reason"], "retire E0 checkpoints");
    let e0_entry = get_body["environments"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["environment_ref"] == e0.to_string())
        .expect("e0 entry");
    assert_eq!(e0_entry["state"], "deprecated");
    assert_eq!(e0_entry["last_change"]["reason"], "superseded");
    assert_eq!(e0_entry["memory_retired"], true);
    assert_eq!(
        e0_entry["memory_last_change"]["reason"],
        "retire E0 checkpoints"
    );

    // Audit event and node reconcile trigger + queue entry are persisted.
    let audit_count: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM admin_audit_events
          WHERE path LIKE '%/runtime-environments/%/memory'
            AND method = 'PUT'
            AND success = TRUE"
    ))
    .fetch_one(&h.db.pool)
    .await?;
    assert_eq!(audit_count, 1);
    let triggers: Vec<(NodeId, Vec<String>)> = sqlx::query_as(sql!(
        "SELECT node_id, reasons::TEXT[] FROM node_reconcile_triggers WHERE node_id = $1"
    ))
    .bind(worker)
    .fetch_all(&h.db.pool)
    .await?;
    assert_eq!(triggers, vec![(worker, vec!["node_updated".to_owned()])]);
    let refs: Vec<(NodeId, String, String)> = sqlx::query_as(sql!(
        "SELECT node_id, facet, element FROM node_reconcile_queue WHERE node_id = $1"
    ))
    .bind(worker)
    .fetch_all(&h.db.pool)
    .await?;
    assert_eq!(
        refs,
        vec![(worker, "runtime-provisioning".to_owned(), String::new())]
    );

    // Moving the default to `e2` while `e0` (`deprecated` + `memory_retired`) has no
    // certified upgrade edge to `e2` is refused with 409 unless `strand: true` is supplied.
    let (status, body) = h.set_default(e2).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error_contains(&body, "certified upgrade edge");

    // Promoting a memory-retired environment back to allowed or default is refused with 409.
    let (status, body) = h
        .set_state(e0, json!({"state": "allowed", "reason": "re-enable"}))
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error_contains(&body, "unretire");

    let (status, body) = h.set_default(e0).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error_contains(&body, "unretire");

    // Blocking succeeds while memory_retired is true, and unretiring after blocked is refused.
    let (status, body) = h
        .set_state(e0, json!({"state": "blocked", "reason": "retire E0"}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = h
        .set_memory(e0, json!({"action": "unretire", "reason": "too late"}))
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error_contains(&body, "blocked");

    Ok(())
}

/// Kubernetes-only because `retire` requires
/// `Substrate::check_environment_memory_retirement()` (see
/// [`crate::controllers::substrate::direct`] `# Gaps`:
/// `Runtime-environment memory retirement`).
#[cfg(feature = "kubernetes")]
#[tokio::test(flavor = "current_thread")]
async fn retire_enforces_coverage_gate_and_reports_structured_uncovered_counts() -> TestResult {
    let Some(h) = harness_with_memory_drop().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    insert_catalog_environment(&h.db.pool, e0, "1.15.1", "9.0.0", &[e1], &[]).await?;
    insert_catalog_environment(&h.db.pool, e1, "1.17.0", "12.0.0", &[], &[]).await?;
    assert_eq!(h.set_default(e1).await.0, StatusCode::OK);
    assert_eq!(
        h.set_state(e0, json!({"state": "deprecated", "reason": "drain E0"}))
            .await
            .0,
        StatusCode::OK
    );
    let node = NodeId::new();
    insert_ready_worker_node(&h.db.pool, node).await?;

    // Running E1 sandbox should not block retiring E0.
    let live_e1 = insert_sandbox(&h.db.pool, "started").await?;
    observe_sandbox(&h.db.pool, live_e1, node, Some(metadata(Some(e1)))).await?;

    // Stopped sandbox upgraded to an E1 disk artifact (even with a stale E0
    // observation) and already-dropped E0 sandbox are covered.
    let stopped_upgraded = insert_sandbox(&h.db.pool, "stopped").await?;
    observe_sandbox(&h.db.pool, stopped_upgraded, node, Some(metadata(Some(e0)))).await?;
    let upgraded_disk = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots
             (sandbox_id, type, state, artifact_origin, artifact_kind,
              local_artifact_id, timestamp, execution_metadata)
         VALUES ($1, 'disk', 'ready', 'stop', 'disk', $2, NOW(), $3)"
    ))
    .bind(stopped_upgraded)
    .bind(upgraded_disk)
    .bind(sqlx::types::Json(metadata(Some(e1))))
    .execute(&h.db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'disk',
                current_local_artifact_origin = 'stop'
          WHERE id = $1"
    ))
    .bind(stopped_upgraded)
    .bind(upgraded_disk)
    .execute(&h.db.pool)
    .await?;

    let droppable_e0 = insert_sandbox(&h.db.pool, "suspended").await?;
    insert_terminated_execution_on_gone_node(&h.db.pool, droppable_e0).await?;
    let artifact = insert_snapshot(
        &h.db.pool,
        droppable_e0,
        ("ready", Some("suspend"), 10, true),
        metadata(Some(e0)),
    )
    .await?;
    mark_current_artifact(&h.db.pool, droppable_e0, artifact).await?;

    let already_dropped_e0 = insert_sandbox(&h.db.pool, "suspended").await?;
    insert_terminated_execution_on_gone_node(&h.db.pool, already_dropped_e0).await?;
    let dropped_artifact = insert_snapshot(
        &h.db.pool,
        already_dropped_e0,
        ("ready", Some("suspend"), 10, true),
        metadata(Some(e0)),
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'disk',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(already_dropped_e0)
    .bind(dropped_artifact)
    .execute(&h.db.pool)
    .await?;

    // Add uncovered rows across every bucket:
    // 1. Direct-managed suspended E0 sandbox -> not_kubernetes.
    let direct_e0 = insert_sandbox_on(&h.db.pool, SubstrateKind::Space, "suspended").await?;
    insert_snapshot(
        &h.db.pool,
        direct_e0,
        ("ready", Some("suspend"), 10, true),
        metadata(Some(e0)),
    )
    .await?;

    // 2. Encrypted E0 sandbox on a gone node -> encrypted.
    let encrypted_e0 = insert_sandbox(&h.db.pool, "suspended").await?;
    insert_terminated_execution_on_gone_node(&h.db.pool, encrypted_e0).await?;
    let enc_artifact = insert_snapshot(
        &h.db.pool,
        encrypted_e0,
        ("ready", Some("suspend"), 10, true),
        metadata(Some(e0)),
    )
    .await?;
    mark_current_artifact(&h.db.pool, encrypted_e0, enc_artifact).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET volume_encryption = TRUE WHERE id = $1"
    ))
    .bind(encrypted_e0)
    .execute(&h.db.pool)
    .await?;

    // 3. Running E0 sandbox and un-upgraded stopped E0 sandbox -> node_held (2).
    let running_e0 = insert_sandbox(&h.db.pool, "started").await?;
    observe_sandbox(&h.db.pool, running_e0, node, Some(metadata(Some(e0)))).await?;
    let stopped_e0 = insert_sandbox(&h.db.pool, "stopped").await?;
    observe_sandbox(&h.db.pool, stopped_e0, node, Some(metadata(Some(e0)))).await?;

    // 4. Unmarked suspended E0 sandbox and non-per-attempt storage URI E0
    // sandbox on a gone node -> unpinned_or_not_ready (2).
    let unmarked_e0 = insert_sandbox(&h.db.pool, "suspended").await?;
    insert_terminated_execution_on_gone_node(&h.db.pool, unmarked_e0).await?;
    insert_snapshot(
        &h.db.pool,
        unmarked_e0,
        ("ready", Some("suspend"), 10, true),
        metadata(Some(e0)),
    )
    .await?;

    let bad_uri_e0 = insert_sandbox(&h.db.pool, "suspended").await?;
    insert_terminated_execution_on_gone_node(&h.db.pool, bad_uri_e0).await?;
    let bad_uri_artifact = insert_snapshot(
        &h.db.pool,
        bad_uri_e0,
        ("ready", Some("suspend"), 10, true),
        metadata(Some(e0)),
    )
    .await?;
    mark_current_artifact(&h.db.pool, bad_uri_e0, bad_uri_artifact).await?;
    sqlx::query(sql!(
        "UPDATE snapshot_storage_locations ssl
            SET storage_connection_string = 's3://synthetic-bucket/legacy?region=us-east-1'
           FROM snapshots sn
          WHERE ssl.snapshot_id = sn.id
            AND sn.sandbox_id = $1"
    ))
    .bind(bad_uri_e0)
    .execute(&h.db.pool)
    .await?;

    // 5. Unrecorded creating sandbox -> unattributed.
    let unrecorded = insert_sandbox(&h.db.pool, "creating").await?;

    let (status, body) = h
        .set_memory(e0, json!({"action": "retire", "reason": "check coverage"}))
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["uncovered"],
        json!({
            "not_kubernetes": 1,
            "encrypted": 1,
            "node_held": 2,
            "unpinned_or_not_ready": 2,
            "unattributed": 1,
        })
    );

    // strand=true with unretire is refused with 400.
    let (status, body) = h
        .set_memory(
            e0,
            json!({"action": "unretire", "reason": "bad flag", "strand": true}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    // strand=true never waives attributed uncovered rows.
    let (status, body) = h
        .set_memory(
            e0,
            json!({"action": "retire", "reason": "still attributed", "strand": true}),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");

    // Transition attributed uncovered rows to deleting, leaving `unrecorded`
    // (`uncovered.unattributed == 1`). Retiring without strand still refuses
    // with 409, while `strand: true` waives `unattributed` and succeeds.
    for id in [
        direct_e0,
        encrypted_e0,
        running_e0,
        stopped_e0,
        unmarked_e0,
        bad_uri_e0,
    ] {
        sqlx::query(sql!(
            "UPDATE sandboxes SET lifecycle = 'deleting' WHERE id = $1"
        ))
        .bind(id)
        .execute(&h.db.pool)
        .await?;
    }

    let (status, body) = h
        .set_memory(
            e0,
            json!({"action": "retire", "reason": "unattributed remains"}),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["uncovered"],
        json!({
            "not_kubernetes": 0,
            "encrypted": 0,
            "node_held": 0,
            "unpinned_or_not_ready": 0,
            "unattributed": 1,
        })
    );

    let (status, body) = h
        .set_memory(
            e0,
            json!({"action": "retire", "reason": "waive unattributed", "strand": true}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["memory_retired"], true);

    let _ = unrecorded;
    Ok(())
}

/// Kubernetes-only because `retire` requires
/// `Substrate::check_environment_memory_retirement()` (see
/// [`crate::controllers::substrate::direct`] `# Gaps`:
/// `Runtime-environment memory retirement`).
#[cfg(feature = "kubernetes")]
#[tokio::test(flavor = "current_thread")]
async fn pre_migration_schema_keeps_reads_and_state_writes_working_and_returns_503_on_memory_write()
-> TestResult {
    let Some(h) = harness_with_memory_drop_unmigrated().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    assert_eq!(h.set_default(e1).await.0, StatusCode::OK);
    assert_eq!(
        h.set_state(e0, json!({"state": "deprecated", "reason": "drain E0"}))
            .await
            .0,
        StatusCode::OK
    );

    let (status, list_body) = h.get("/admin/api/v1/runtime-environments").await;
    assert_eq!(status, StatusCode::OK, "{list_body}");
    let e0_entry = list_body["environments"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["environment_ref"] == e0.to_string())
        .expect("e0 entry");
    assert_eq!(e0_entry["memory_retired"], false);

    let (status, body) = h
        .set_memory(e0, json!({"action": "retire", "reason": "schema pending"}))
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_error_contains(&body, "memory-retirement migration has not been applied");
    Ok(())
}

/// Kubernetes-only because `retire` requires
/// `Substrate::check_environment_memory_retirement()` (see
/// [`crate::controllers::substrate::direct`] `# Gaps`:
/// `Runtime-environment memory retirement`).
#[cfg(feature = "kubernetes")]
#[tokio::test(flavor = "current_thread")]
async fn retire_blocks_on_in_use_template_releases_until_tags_and_pools_move_to_default()
-> TestResult {
    let Some(h) = harness_booting_twins_with_memory_drop().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    insert_catalog_environment(&h.db.pool, e0, "1.15.1", "9.0.0", &[e1], &[]).await?;
    insert_catalog_environment(&h.db.pool, e1, "1.17.0", "12.0.0", &[], &[]).await?;
    assert_eq!(h.set_default(e1).await.0, StatusCode::OK);
    assert_eq!(
        h.set_state(e0, json!({"state": "deprecated", "reason": "drain E0"}))
            .await
            .0,
        StatusCode::OK
    );

    // Unused E0 release never blocks retirement.
    let _unused = insert_template_release(&h.db.pool, "unused", Some(metadata(Some(e0)))).await?;

    // Tagged E0 release with no E1 twin blocks retirement even with `strand: true`.
    let tagged = insert_template_release(&h.db.pool, "tagged", Some(metadata(Some(e0)))).await?;
    tag_release(&h.db.pool, tagged, "tagged", "latest").await?;
    let (status, body) = h
        .set_memory(
            e0,
            json!({"action": "retire", "reason": "tagged E0 release", "strand": true}),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["template_releases_in_use"],
        json!({ "bound": 1, "unattributed": 0 })
    );

    // Once an E1 twin exists, creates by tag resolve to the E1 twin, while an
    // enabled pool config on the E0 release still blocks retirement.
    sqlx::query(sql!(
        "DROP INDEX IF EXISTS idx_template_releases_template_name_hash_unique"
    ))
    .execute(&h.db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_releases
             (template_name, type, hash, org_id, execution_metadata, created_at)
         SELECT template_name, type, hash, org_id,
                jsonb_build_object('environment', $2::TEXT), clock_timestamp()
           FROM template_releases
          WHERE id = $1"
    ))
    .bind(tagged)
    .bind(e1.to_string())
    .execute(&h.db.pool)
    .await?;

    let pool_cfg = pool_release(&h.db.pool, tagged, true).await?;
    let (status, body) = h
        .set_memory(
            e0,
            json!({"action": "retire", "reason": "pooled E0 release", "strand": true}),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["template_releases_in_use"],
        json!({ "bound": 1, "unattributed": 0 })
    );

    // Disabling the pool config removes the bound blocker; an unbound in-use
    // release blocks without `strand` and is waived by `strand: true`.
    sqlx::query(sql!(
        "UPDATE kubernetes_pool_configs SET enabled = FALSE WHERE id = $1"
    ))
    .bind(pool_cfg)
    .execute(&h.db.pool)
    .await?;
    let unbound = insert_template_release(&h.db.pool, "legacy-unbound", None).await?;
    tag_release(&h.db.pool, unbound, "legacy-unbound", "latest").await?;

    let (status, body) = h
        .set_memory(
            e0,
            json!({"action": "retire", "reason": "unbound in-use release"}),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["template_releases_in_use"],
        json!({ "bound": 0, "unattributed": 1 })
    );

    let (status, body) = h
        .set_memory(
            e0,
            json!({"action": "retire", "reason": "waive unbound release", "strand": true}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["memory_retired"], true);
    Ok(())
}

#[cfg(feature = "kubernetes")]
async fn insert_catalog_environment(
    pool: &PgPool,
    reference: EnvironmentReference,
    runtime_version: &str,
    state_format_version: &str,
    upgrade_edges: &[EnvironmentReference],
    memory_edges: &[EnvironmentReference],
) -> TestResult {
    let upgrade_edges: Vec<String> = upgrade_edges.iter().map(ToString::to_string).collect();
    let memory_edges: Vec<String> = memory_edges.iter().map(ToString::to_string).collect();
    sqlx::query(sql!(
        "INSERT INTO runtime_environments
             (environment_ref, runtime, runtime_version, kernel_version,
              state_format_name, state_format_version, upgrade_edges, memory_edges)
         VALUES ($1, 'firecracker', $2, '6.1.155', 'firecracker-snapshot', $3, $4, $5)"
    ))
    .bind(reference.to_string())
    .bind(runtime_version)
    .bind(state_format_version)
    .bind(upgrade_edges)
    .bind(memory_edges)
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(feature = "kubernetes")]
async fn insert_terminated_execution_on_gone_node(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, k8s_node_name,
             state, active, terminated_at)
         VALUES ($1, 1, $2, 'synthetic', 'worker-gone', 'terminated', FALSE,
                 NOW() - INTERVAL '1 hour')"
    ))
    .bind(sandbox_id)
    .bind(format!("sbx-{sandbox_id}"))
    .execute(pool)
    .await?;
    Ok(())
}
