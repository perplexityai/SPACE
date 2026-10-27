use super::*;
use crate::ClusterApiService;
use crate::proto::cluster_api_server::ClusterApi;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{LocalArtifactId, NodeId, SandboxId};
use pplx_space_db_test_fixtures::{insert_ready_worker_node, pg_test_db, try_pg_test_db};
use pplx_space_placement::StoredSandboxLifecycle;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

#[path = "../tests/support/published_state.rs"]
mod support;
use support::*;

#[test]
fn published_fork_artifact_origin_maps_to_the_core_vocabulary() {
    assert_eq!(
        published_artifact_origin(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Fork as i32)
            .unwrap(),
        ArtifactOrigin::Fork
    );
}

async fn register_inventory_template(pool: &PgPool) -> TestResult {
    ClusterApiService::new(pool.clone())
        .register_template(crate::tests::publisher_request(
            crate::RegisterTemplateRequest {
                system_publication_grant_id: None,
                tenant_publication_grant_id: None,
                name: "code-runtime".to_string(),
                description: None,
                org_id: None,
            },
        ))
        .await?;
    Ok(())
}

fn template_state() -> PublishedTemplateState {
    PublishedTemplateState {
        template: "code-runtime".to_string(),
        present: true,
        content_hash: Some("sha256:abc".to_string()),
        runtime: Some("fc".to_string()),
        arch: Some("x86_64".to_string()),
        built_at: Some("2026-05-07T00:00:00Z".to_string()),
        artifact_size_bytes: Some(1234),
        cache_size_bytes: Some(2345),
        has_base_snapshot: true,
        base_snapshot_size_bytes: Some(5678),
        pool_target: Some(3),
        pool_current: 2,
        pool_pending: 1,
        ensure_base_snapshot: Some(true),
        snapshot_vcpu: Some(2),
        snapshot_memory_mib: Some(1024),
        vcpu: Some(4),
        memory_mib: Some(4096),
        disk_size_mb: Some(20480),
        ..PublishedTemplateState::default()
    }
}

#[test]
fn unknown_recovery_owner_and_future_schema_preserve_known_fields() {
    let local_quarantine_id = Uuid::now_v7();
    let state = PublishedSandboxState {
        quarantine_recovery: Some(
            pplx_space_sbx_mgr_client::proto::QuarantineRecoveryObservation {
                schema_version: 2,
                local_quarantine_id: local_quarantine_id.to_string(),
                observed_owner: i32::MAX,
                attempt_count: 7,
                next_attempt_at: Some(timestamp(1_786_800_000)),
                degraded: false,
            },
        ),
        ..PublishedSandboxState::default()
    };

    let QuarantineRecoveryProjectionState::Observed(observation) =
        quarantine_recovery_projection(&state).unwrap()
    else {
        panic!("an unknown owner must not degrade otherwise valid evidence");
    };
    assert_eq!(observation.local_quarantine_id, local_quarantine_id);
    assert_eq!(observation.schema_version, 2);
    assert_eq!(
        observation.observed_owner,
        QuarantineRecoveryObservedOwner::Unspecified
    );
    assert_eq!(observation.attempt_count, 7);
    assert_eq!(
        observation.next_attempt_at.as_deref(),
        Some("2026-08-15T13:20:00Z")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn execution_metadata_and_node_capabilities_are_queryable() -> TestResult {
    use pplx_space_execution_common::proto::{
        ArtifactComponentInstallation, NodeExecutionInventory, RuntimeInstallation,
        StateFormatCapability,
    };

    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    register_inventory_template(&db.pool).await?;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "started").await?;
    let execution = test_execution();
    let inventory = NodeExecutionInventory {
        policy: None,
        runtimes: vec![RuntimeInstallation {
            runtime: Some(execution.runtime().into()),
            supported_guests: vec![execution.guest().into()],
            state_formats: execution
                .state_format()
                .into_iter()
                .map(|identity| StateFormatCapability {
                    identity: Some(identity.into()),
                    can_create: true,
                    can_load: true,
                })
                .collect(),
            version_sort_key: Vec::new(),
        }],
        components: execution
            .components()
            .iter()
            .map(|component| ArtifactComponentInstallation {
                component: Some(component.into()),
                default_for_new: true,
                legacy_default: true,
            })
            .collect(),
        environments: None,
    };
    let mut template = template_state();
    template.execution = Some((&execution).into());
    let mut sandbox = published_sandbox_state(sandbox_id, "started");
    sandbox.execution = Some((&execution).into());

    let mut tx = db.pool.begin().await?;
    let observation =
        crate::execution_observation::prepare(&mut tx, node_id, None, Some(&inventory)).await?;
    crate::execution_observation::publish(&mut tx, observation).await?;
    tx.commit().await?;
    let mut conn = db.pool.acquire().await?;
    publish_template_states(&mut conn, node_id, NodeRole::Builder, &[template]).await?;
    publish_sandbox_states(&mut conn, node_id, &[sandbox]).await?;
    drop(conn);

    let runtime_version: String = sqlx::query_scalar(
        "SELECT runtime_version FROM node_runtime_installations WHERE node_id = $1",
    )
    .bind(node_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(runtime_version, "1.15.1");
    let template_component_role: String =
        sqlx::query_scalar("SELECT role_key FROM node_template_components WHERE node_id = $1")
            .bind(node_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(template_component_role, "");
    let sandbox_state_format: String = sqlx::query_scalar(
        "SELECT state_format_version FROM sandbox_observed_states WHERE sandbox_id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(sandbox_state_format, "9.0.0");
    let restore_candidates: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT runtime.node_id)
         FROM node_runtime_installations runtime
         JOIN node_runtime_state_formats state_format
           ON state_format.node_id = runtime.node_id
          AND state_format.runtime = runtime.runtime
          AND state_format.runtime_version = runtime.runtime_version
         WHERE runtime.runtime = 'fc'
           AND state_format.state_format_name = 'firecracker-snapshot'
           AND state_format.can_load IS TRUE",
    )
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(restore_candidates, 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn repeated_template_publish_does_not_rewrite_node_template_inventory() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    register_inventory_template(&db.pool).await?;
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;

    let mut state = template_state();
    state.execution = Some((&test_execution()).into());
    let mut conn = db.pool.acquire().await?;
    let accepted =
        publish_template_states(&mut conn, node_id, NodeRole::Builder, &[state.clone()]).await?;
    assert_eq!(accepted, 1);
    drop(conn);

    let component_ctid: String = sqlx::query_scalar(
        "SELECT ctid::TEXT
         FROM node_template_components
         WHERE node_id = $1
           AND template_name = 'code-runtime'
           AND content_hash = 'sha256:abc'
         ORDER BY kind, role_key",
    )
    .bind(node_id)
    .fetch_one(&db.pool)
    .await?;

    sqlx::query(
        "UPDATE node_template_inventory
         SET updated_at = '2026-01-01 00:00:00+00'::timestamptz
         WHERE node_id = $1 AND template_name = 'code-runtime'",
    )
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    let mut conn = db.pool.acquire().await?;
    let accepted = publish_template_states(&mut conn, node_id, NodeRole::Builder, &[state]).await?;
    assert_eq!(accepted, 1);
    drop(conn);

    let updated_at: String = sqlx::query_scalar(
        "SELECT updated_at::TEXT
         FROM node_template_inventory
         WHERE node_id = $1 AND template_name = 'code-runtime'",
    )
    .bind(node_id)
    .fetch_one(&db.pool)
    .await?;
    assert!(
        updated_at.starts_with("2026-01-01 00:00:00"),
        "unchanged publish should not touch updated_at, got {updated_at}"
    );

    let current_component_ctid: String = sqlx::query_scalar(
        "SELECT ctid::TEXT
         FROM node_template_components
         WHERE node_id = $1
           AND template_name = 'code-runtime'
           AND content_hash = 'sha256:abc'
         ORDER BY kind, role_key",
    )
    .bind(node_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        current_component_ctid, component_ctid,
        "unchanged publish should not rewrite component rows"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn template_publish_does_not_reactivate_deleted_template() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;

    sqlx::query(
        "INSERT INTO templates (id, name, org_id, desired_state)
         VALUES ($1, 'code-runtime', '00000000-0000-0000-0000-000000000001', 'deleted')",
    )
    .bind(Uuid::now_v7())
    .execute(&db.pool)
    .await?;

    let mut conn = db.pool.acquire().await?;
    let accepted =
        publish_template_states(&mut conn, node_id, NodeRole::Builder, &[template_state()]).await?;
    assert_eq!(accepted, 1);
    drop(conn);

    let desired_state: String =
        sqlx::query_scalar("SELECT desired_state FROM templates WHERE name = 'code-runtime'")
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(desired_state, "deleted");

    let inventory_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT
         FROM node_template_inventory
         WHERE node_id = $1 AND template_name = 'code-runtime'",
    )
    .bind(node_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(inventory_count, 1);

    Ok(())
}

/// Node publication must never write `template_releases`: the rows are
/// fleet-shared, and any per-node write to them holds row locks until the
/// whole publish transaction commits, convoying other publishers.
/// `RegisterTemplateRelease` is the only writer of release execution
/// metadata. Covers both row states a publish could have touched: a release
/// with no metadata (the removed backfill would have filled it) and one whose
/// metadata disagrees with the publish (the old projection would have
/// rewritten it). Both must keep their values and their physical rows (ctid).
#[tokio::test(flavor = "current_thread")]
async fn template_publish_never_writes_template_releases() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    sqlx::query(
        "INSERT INTO templates (id, name, org_id)
         VALUES ($1, 'code-runtime', '00000000-0000-0000-0000-000000000001')",
    )
    .bind(Uuid::now_v7())
    .execute(&db.pool)
    .await?;
    // One release with no metadata, one whose sentinel metadata disagrees
    // with what the node publishes.
    sqlx::query(
        "INSERT INTO template_releases (id, template_name, type, hash)
         VALUES ($1, 'code-runtime', 'fc', 'sha256:abc')",
    )
    .bind(Uuid::now_v7())
    .execute(&db.pool)
    .await?;
    sqlx::query(
        "INSERT INTO template_releases (id, template_name, type, hash, execution_metadata)
         VALUES ($1, 'code-runtime', 'fc', 'sha256:def',
                 '{\"guest\": {\"architecture\": \"aarch64\"}, \"sentinel\": true}'::jsonb)",
    )
    .bind(Uuid::now_v7())
    .execute(&db.pool)
    .await?;
    let before: Vec<(String, Option<serde_json::Value>, String)> = sqlx::query_as(
        "SELECT hash, execution_metadata, ctid::TEXT FROM template_releases
         WHERE template_name = 'code-runtime' ORDER BY hash",
    )
    .fetch_all(&db.pool)
    .await?;

    let mut state = template_state();
    state.execution = Some((&test_execution()).into());
    let mut second = template_state();
    second.content_hash = Some("sha256:def".to_string());
    second.execution = Some((&test_execution()).into());
    let mut conn = db.pool.acquire().await?;
    publish_template_states(&mut conn, node_id, NodeRole::Builder, &[state, second]).await?;
    drop(conn);

    let after: Vec<(String, Option<serde_json::Value>, String)> = sqlx::query_as(
        "SELECT hash, execution_metadata, ctid::TEXT FROM template_releases
         WHERE template_name = 'code-runtime' ORDER BY hash",
    )
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(
        before, after,
        "publish must leave template_releases untouched, values and physical rows alike"
    );
    assert_eq!(after[0].1, None, "the metadata-less release must stay NULL");
    assert_eq!(
        after[1].1.as_ref().map(|m| m["sentinel"].clone()),
        Some(serde_json::json!(true))
    );
    Ok(())
}

#[test]
fn worker_template_publish_strips_template_metadata() {
    let row = template_state_row(&template_state(), false).unwrap();

    assert_eq!(row["template_name"], "code-runtime");
    assert_eq!(row["content_hash"], "sha256:abc");
    assert_eq!(row["has_base_snapshot"], true);
    assert_eq!(row["base_snapshot_size_bytes"], 5678);
    assert_eq!(row["pool_target"], 3);
    assert_eq!(row["pool_current"], 2);
    assert_eq!(row["pool_pending"], 1);

    assert!(row["runtime"].is_null());
    assert!(row["arch"].is_null());
    assert!(row["built_at"].is_null());
    assert!(row["artifact_size_bytes"].is_null());
    assert_eq!(row["cache_size_bytes"], 2345);
    assert!(row["default_auto_lifecycle_json"].is_null());
    assert!(row["ensure_base_snapshot"].is_null());
    assert!(row["snapshot_vcpu"].is_null());
    assert!(row["snapshot_memory_mib"].is_null());
    assert!(row["vcpu"].is_null());
    assert!(row["memory_mib"].is_null());
    assert!(row["disk_size_mb"].is_null());
}

#[test]
fn builder_template_publish_includes_template_metadata() {
    let row = template_state_row(&template_state(), true).unwrap();

    assert_eq!(row["runtime"], "fc");
    assert_eq!(row["arch"], "x86_64");
    assert_eq!(row["built_at"], "2026-05-07T00:00:00Z");
    assert_eq!(row["artifact_size_bytes"], 1234);
    assert_eq!(row["cache_size_bytes"], 2345);
    assert_eq!(row["ensure_base_snapshot"], true);
    assert_eq!(row["snapshot_vcpu"], 2);
    assert_eq!(row["snapshot_memory_mib"], 1024);
    assert_eq!(row["vcpu"], 4);
    assert_eq!(row["memory_mib"], 4096);
    assert_eq!(row["disk_size_mb"], 20480);
}

#[test]
fn template_first_seen_refs_cover_template_policy_facets() {
    let refs = template_policy_reconcile_refs_for_new_templates(&[
        "code-runtime".to_string(),
        "code-runtime".to_string(),
    ]);

    assert_eq!(
        refs,
        vec![
            (FACET_TEMPLATE_DESIRED_STATE, "code-runtime".to_string()),
            (
                FACET_TEMPLATE_BASE_SNAPSHOT_POLICY,
                "code-runtime".to_string()
            ),
            (FACET_NETWORK_POLICY, "template:code-runtime".to_string()),
        ]
    );
}

#[test]
fn sandbox_first_seen_refs_cover_sandbox_policy_facets() {
    let sandbox_id = Uuid::parse_str("018f6d61-181a-7a40-8f62-6a70a61fb630").unwrap();
    let refs = sandbox_policy_reconcile_refs_for_new_sandboxes(&[
        SandboxId::from_uuid(sandbox_id),
        SandboxId::from_uuid(sandbox_id),
    ]);

    assert_eq!(
        refs,
        vec![
            (
                FACET_SANDBOX_DESIRED_STATE,
                "018f6d61-181a-7a40-8f62-6a70a61fb630".to_string()
            ),
            (
                FACET_NETWORK_POLICY,
                "sandbox:018f6d61-181a-7a40-8f62-6a70a61fb630".to_string()
            ),
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn inventory_cannot_register_unknown_template() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    let mut state = template_state();
    state.execution = Some((&test_execution()).into());
    state.network_policy_generation = 1;
    state.network_policy = Some(pplx_space_net_common::proto::NetworkPolicyV1 {
        version: pplx_space_net_common::proto::NetworkPolicyVersion::V1 as i32,
        egress_default_action: pplx_space_net_common::proto::EgressDefaultAction::Deny as i32,
        ..pplx_space_net_common::proto::NetworkPolicyV1::default()
    });

    let mut conn = db.pool.acquire().await?;
    let accepted = publish_template_states(
        &mut conn,
        node_id,
        NodeRole::Builder,
        std::slice::from_ref(&state),
    )
    .await?;
    drop(conn);

    assert_eq!(accepted, 0);
    let before = sqlx::query_as::<_, (i64, i64, i64, i64)>(sql!(
        "SELECT
             (SELECT COUNT(*) FROM templates WHERE name = 'code-runtime'),
             (SELECT COUNT(*) FROM node_template_inventory
              WHERE node_id = $1 AND template_name = 'code-runtime'),
             (SELECT COUNT(*) FROM node_template_components
              WHERE node_id = $1 AND template_name = 'code-runtime'),
             (SELECT COUNT(*) FROM node_facet_observed_status
              WHERE node_id = $1 AND element = 'template:code-runtime')"
    ))
    .bind(node_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(before, (0, 0, 0, 0));

    register_inventory_template(&db.pool).await?;

    let mut conn = db.pool.acquire().await?;
    let accepted = publish_template_states(&mut conn, node_id, NodeRole::Builder, &[state]).await?;
    drop(conn);

    assert_eq!(accepted, 1);
    let after = sqlx::query_as::<_, (Option<Uuid>, i64, i64, i64)>(sql!(
        "SELECT org_id,
             (SELECT COUNT(*) FROM node_template_inventory
              WHERE node_id = $1 AND template_name = 'code-runtime'),
             (SELECT COUNT(*) FROM node_template_components
              WHERE node_id = $1 AND template_name = 'code-runtime'),
             (SELECT COUNT(*) FROM node_facet_observed_status
              WHERE node_id = $1 AND element = 'template:code-runtime')
         FROM templates WHERE name = 'code-runtime'"
    ))
    .bind(node_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(after, (None, 1, 1, 1));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn template_publish_does_not_adopt_unowned_template() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    sqlx::query("INSERT INTO templates (id, name) VALUES ($1, 'code-runtime')")
        .bind(Uuid::now_v7())
        .execute(&db.pool)
        .await?;

    let mut conn = db.pool.acquire().await?;
    let accepted =
        publish_template_states(&mut conn, node_id, NodeRole::Builder, &[template_state()]).await?;
    drop(conn);

    assert_eq!(accepted, 1);
    let org_id = sqlx::query_scalar::<_, Option<Uuid>>(
        "SELECT org_id FROM templates WHERE name = 'code-runtime'",
    )
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(org_id, None);
    Ok(())
}

#[tokio::test]
async fn registered_inventory_does_not_require_cluster_singleton() -> TestResult {
    let db = pg_test_db().await?;
    register_inventory_template(&db.pool).await?;
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    sqlx::query(sql!("DELETE FROM clusters WHERE singleton"))
        .execute(&db.pool)
        .await?;
    let mut conn = db.pool.acquire().await?;

    let accepted =
        publish_template_states(&mut conn, node_id, NodeRole::Builder, &[template_state()]).await?;

    assert_eq!(accepted, 1);
    Ok(())
}

/// Node publication moves a row's lifecycle only out of its convergence
/// sources, swept over every stored lifecycle, both reports and both
/// substrates. A `stopped` report converges `started`, `paused`, `suspended`,
/// `stopping` and `restoring` to `stopped`; a `started` report (without an
/// activation receipt) promotes only `restoring`, so it never moves a
/// `stopped` row. A control-plane write of `started` over a sandbox that is
/// in fact stopped is therefore corrected by the next publication, while a
/// `stopped` row whose sandbox runs is not. Both compare the lifecycle
/// trimmed and lower-cased. `space` rows have no Kubernetes
/// execution to route; `k8s` rows carry a routable one, so routability does
/// not mask the lifecycle filter for either.
#[tokio::test(flavor = "current_thread")]
async fn node_publication_converges_only_from_its_source_lifecycles() -> TestResult {
    use pplx_space_common_core::types::SandboxLifecycle as L;

    /// What a `started` and a `stopped` report leave in a row stored as `stored`.
    fn after_reports(stored: L) -> (L, L) {
        match stored {
            L::Restoring => (L::Started, L::Stopped),
            L::Started | L::Paused | L::Suspended | L::Stopping => (stored, L::Stopped),
            L::Created
            | L::Creating
            | L::Starting
            | L::Pausing
            | L::Resuming
            | L::Suspending
            | L::Stopped
            | L::Deleting
            | L::Checkpointing
            | L::Quarantined
            | L::Failed => (stored, stored),
        }
    }

    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    let cases = StoredSandboxLifecycle::all()
        .iter()
        .map(|&stored| match stored {
            StoredSandboxLifecycle::Live(lifecycle) => {
                let (after_started, after_stopped) = after_reports(lifecycle);
                (
                    stored.as_str(),
                    after_started.as_str(),
                    after_stopped.as_str(),
                )
            }
            // The stored tombstones; no report moves them.
            StoredSandboxLifecycle::Deleted | StoredSandboxLifecycle::CleanupRequired => {
                (stored.as_str(), stored.as_str(), stored.as_str())
            }
        })
        // Values outside the vocabulary. Both convergences compare
        // `LOWER(TRIM(lifecycle))`, so a padded member of their source sets
        // converges like the canonical one; an unknown value is in neither.
        .chain([
            (" Restoring ", "started", "stopped"),
            (" Suspended ", " Suspended ", "stopped"),
            ("bogus", "bogus", "bogus"),
        ]);
    let cases = ["space", "k8s"]
        .into_iter()
        .flat_map(|managed_by| cases.clone().map(move |case| (managed_by, case)));
    let mut mismatches = Vec::new();
    for (managed_by, (stored, after_started, after_stopped)) in cases {
        for (published, expected) in [("started", after_started), ("stopped", after_stopped)] {
            let sandbox_id = SandboxId::new();
            insert_active_sandbox_with_managed_by(
                &db.pool, sandbox_id, node_id, stored, managed_by,
            )
            .await?;
            if managed_by == "k8s" {
                insert_k8s_execution(&db.pool, sandbox_id, 1, "running", Some("100.64.0.1"))
                    .await?;
            }

            let mut tx = db.pool.begin().await?;
            let outcome = publish_sandbox_states(
                &mut tx,
                node_id,
                &[published_sandbox_state(sandbox_id, published)],
            )
            .await?;
            tx.commit().await?;

            let lifecycle: String =
                sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
                    .bind(sandbox_id)
                    .fetch_one(&db.pool)
                    .await?;
            if (outcome.accepted, lifecycle.as_str()) != (1, expected) {
                mismatches.push(format!(
                    "{managed_by} {stored} published as {published}: expected {expected}, \
                     observed {lifecycle} (accepted {})",
                    outcome.accepted
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_suspended_node_state_finalizes_driver_crashed_suspend() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "suspending").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 1, "running", Some("100.64.0.30")).await?;
    let snapshot_id = insert_ready_suspend_snapshot(&db.pool, sandbox_id, node_id).await?;
    let local_artifact_id = snapshot_local_artifact_id(&db.pool, snapshot_id).await?;
    let mut published = published_sandbox_state(sandbox_id, "suspended");
    published.execution_generation = Some(1);
    published.pod_uid = Some("pod-uid-1".to_string());
    published.current_local_artifact_id = Some(local_artifact_id.to_string());
    published.current_local_artifact_kind =
        Some(pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32);
    published.current_local_artifact_origin =
        Some(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend as i32);
    published.state_changed_at = Some(timestamp(1_234_567));

    let mut conn = db.pool.acquire().await?;
    let outcome = publish_sandbox_states(&mut conn, node_id, &[published]).await?;
    drop(conn);

    // The driver-crashed suspend must finalize: `suspended`/inactive
    // execution, with the current local artifact pinned to exactly the
    // published one (a run-varying UUID, asserted inline via redaction).
    insta::assert_yaml_snapshot!(
        sandbox_execution_artifact_row(&db.pool, sandbox_id).await?,
        {
            ".current_local_artifact_id" => insta::dynamic_redaction(move |value, _path| {
                assert_eq!(
                    value.as_str(),
                    Some(local_artifact_id.to_string().as_str()),
                    "finalized suspend must pin the published local artifact"
                );
                "[artifact_id]"
            }),
        }
    );
    assert_eq!(
        outcome.post_commit_state_convergence,
        vec![crate::published_state::StateConvergenceSample {
            state: "suspended".to_string(),
            changed_at: time::OffsetDateTime::from_unix_timestamp(1_234_567)?,
        }]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn post_selection_execution_identity_change_blocks_exact_finalization() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_active_sandbox(&db.pool, sandbox_id, node_id, "suspending").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 1, "running", Some("100.64.0.32")).await?;

    let mut tx = db.pool.begin().await?;
    sqlx::query(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(sandbox_id)
        .fetch_one(&mut *tx)
        .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET pod_uid = NULL
          WHERE sandbox_id = $1 AND generation = 1 AND active"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let projected = apply_suspended_k8s_finalization(
        &mut tx,
        &[SuspendedFinalizationCandidate {
            sandbox_id,
            clear_marker: false,
            published_artifact: Some(sandbox_quarantine_store::PublishedSuspendArtifact {
                id: LocalArtifactId::new(),
                kind: ArtifactKind::Full,
                origin: ArtifactOrigin::Suspend,
            }),
            published_execution_identity: Some(
                sandbox_quarantine_store::PublishedExecutionIdentity {
                    generation: ExecutionGeneration::new(1)?,
                    pod_uid: PodUid::parse("pod-uid-1")?,
                    pod_uid_was_missing: false,
                },
            ),
        }],
    )
    .await?;
    assert!(projected.is_empty());
    tx.commit().await?;

    // A post-selection identity change must block exact finalization:
    // lifecycle stays `suspending` and the original execution stays
    // `active`/`running`.
    insta::assert_yaml_snapshot!(sandbox_execution_row(&db.pool, sandbox_id).await?);
    Ok(())
}

// This file is loaded through `#[path]`, so a bare `mod` would resolve beside
// it in `src/` rather than under `published_state_tests/`.
#[path = "published_state_tests/lifecycle_writers.rs"]
mod lifecycle_writers;
