//! Postgres behaviors for the evidence the resume- and start-time runtime
//! upgrade is decided from (`store/runtime_upgrade.rs`).

use pplx_space_common_core::types::{LocalArtifactId, NodeId, SandboxId, SnapshotId};

use super::*;
use pplx_space_db_test_fixtures::SEED_ORG_ID;

const E0: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const E1: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";

/// A `full` snapshot recording `execution_metadata` and the legacy columns,
/// stored at `uri`, `age_seconds` old.
async fn insert_full_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    uri: &str,
    age_seconds: i32,
    local_artifact_id: Option<LocalArtifactId>,
    execution_metadata: serde_json::Value,
) -> TestResult<SnapshotId> {
    let snapshot_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots
             (id, sandbox_id, type, state, timestamp, org_id, local_artifact_id,
              artifact_kind, artifact_origin, runtime, runtime_version,
              state_format_name, state_format_version, execution_metadata)
         VALUES ($1, $2, 'full', 'ready', NOW() - ($3 * INTERVAL '1 second'), $4, $5,
                 CASE WHEN $5::uuid IS NULL THEN NULL ELSE 'full' END,
                 CASE WHEN $5::uuid IS NULL THEN NULL ELSE 'suspend' END,
                 'firecracker', '1.15.1', 'firecracker-snapshot', '9.0.0', $6)"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(age_seconds)
    .bind(SEED_ORG_ID)
    .bind(local_artifact_id)
    .bind(sqlx::types::Json(execution_metadata))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, $3)"
    ))
    .bind(snapshot_id)
    .bind(uri)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    Ok(snapshot_id)
}

fn bound(environment: &str, kernel: &str) -> serde_json::Value {
    serde_json::json!({
        "environment": environment,
        "components": [
            {"kind": "rootfs", "version": "1"},
            {"kind": "kernel", "version": kernel},
        ],
    })
}

#[tokio::test]
async fn a_restore_reads_the_checkpoint_it_loads() -> TestResult {
    let fixture = postgres_store().await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "suspended").await?;
    let pinned = LocalArtifactId::new();
    let older = insert_full_snapshot(
        &fixture.pool,
        sandbox_id,
        "s3://bucket/older",
        60,
        Some(pinned),
        bound(E0, "6.1.155"),
    )
    .await?;
    let newer = insert_full_snapshot(
        &fixture.pool,
        sandbox_id,
        "s3://bucket/newer",
        10,
        None,
        serde_json::json!({"components": []}),
    )
    .await?;

    // The restore's selected URI names its snapshot.
    let selected = fixture
        .store
        .restore_checkpoint_execution(sandbox_id, Some("s3://bucket/older"))
        .await?
        .unwrap();
    assert_eq!(selected.subject, older.to_string());
    assert_eq!(selected.local_artifact_id, Some(pinned));
    assert_eq!(selected.execution.environment.as_deref(), Some(E0));
    assert_eq!(
        selected.execution.kernel_version.as_deref(),
        Some("6.1.155")
    );
    assert_eq!(
        selected.execution.runtime_version.as_deref(),
        Some("1.15.1")
    );
    assert_eq!(
        selected.execution.state_format_version.as_deref(),
        Some("9.0.0")
    );

    let unbound = fixture
        .store
        .restore_checkpoint_execution(sandbox_id, Some("s3://bucket/newer"))
        .await?
        .unwrap();
    assert_eq!(unbound.subject, newer.to_string());
    assert_eq!(unbound.execution.environment, None);
    assert_eq!(unbound.execution.kernel_version, None);

    // Without a URI the restore loads the local checkpoint, which is the
    // sandbox's current local artifact; with none pinned there's no record.
    assert_eq!(
        fixture
            .store
            .restore_checkpoint_execution(sandbox_id, None)
            .await?,
        None
    );
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(pinned)
    .execute(&fixture.pool)
    .await?;
    // A disk artifact holds no checkpoint, even one claiming the same
    // identity.
    sqlx::query(sql!(
        "INSERT INTO snapshots
             (id, sandbox_id, type, state, timestamp, org_id, local_artifact_id,
              artifact_kind, artifact_origin)
         VALUES ($1, $2, 'disk', 'ready', NOW(), $3, $4, 'disk', 'stop')"
    ))
    .bind(SnapshotId::new())
    .bind(sandbox_id)
    .bind(SEED_ORG_ID)
    .bind(pinned)
    .execute(&fixture.pool)
    .await?;
    let pinned_read = fixture
        .store
        .restore_checkpoint_execution(sandbox_id, None)
        .await?
        .unwrap();
    assert_eq!(pinned_read.subject, older.to_string());

    // A URI no snapshot of this sandbox has, and a sandbox with no snapshot,
    // record nothing.
    assert_eq!(
        fixture
            .store
            .restore_checkpoint_execution(sandbox_id, Some("s3://bucket/other"))
            .await?,
        None
    );
    let empty = SandboxId::new();
    insert_sandbox(&fixture.pool, empty, "suspended").await?;
    assert_eq!(
        fixture
            .store
            .restore_checkpoint_execution(empty, None)
            .await?,
        None
    );
    Ok(())
}

#[tokio::test]
async fn a_start_reads_the_published_execution_and_names_it_by_what_it_records() -> TestResult {
    let fixture = postgres_store().await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "stopped").await?;
    assert_eq!(
        fixture.store.last_published_execution(sandbox_id).await?,
        None
    );

    let node_id = NodeId::new();
    pplx_space_db_test_fixtures::insert_ready_worker_node(&fixture.pool, node_id).await?;
    // A publish that carried no execution is no evidence, rather than a
    // record from before execution metadata.
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status)
         VALUES ($1, $2, 'stopped')"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(&fixture.pool)
    .await?;
    assert_eq!(
        fixture.store.last_published_execution(sandbox_id).await?,
        None
    );
    let publish = |metadata: serde_json::Value| {
        let pool = fixture.pool.clone();
        async move {
            sqlx::query(sql!(
                "INSERT INTO sandbox_observed_states
                     (sandbox_id, node_id, status, runtime, runtime_version, execution_metadata)
                 VALUES ($1, $2, 'stopped', 'firecracker', '1.15.1', $3)
                 ON CONFLICT (sandbox_id) DO UPDATE
                    SET execution_metadata = EXCLUDED.execution_metadata,
                        runtime = EXCLUDED.runtime,
                        runtime_version = EXCLUDED.runtime_version,
                        observed_at = NOW()"
            ))
            .bind(sandbox_id)
            .bind(node_id)
            .bind(sqlx::types::Json(metadata))
            .execute(&pool)
            .await
        }
    };
    publish(bound(E0, "6.1.155")).await?;
    let first = fixture
        .store
        .last_published_execution(sandbox_id)
        .await?
        .unwrap();
    assert_eq!(first.execution.environment.as_deref(), Some(E0));
    assert_eq!(first.execution.kernel_version.as_deref(), Some("6.1.155"));
    assert_eq!(first.execution.runtime_version.as_deref(), Some("1.15.1"));
    assert_eq!(first.execution.state_format_version, None);

    // A republish of the same execution names the same subject, so a retry
    // reuses its upgrade; another execution names a new one.
    publish(bound(E0, "6.1.155")).await?;
    assert_eq!(
        fixture
            .store
            .last_published_execution(sandbox_id)
            .await?
            .unwrap()
            .subject,
        first.subject
    );
    // A later stop names a new upgrade even with the same execution, so a
    // sandbox that returns to an execution it was once upgraded from isn't
    // taken for a replay of that upgrade.
    for lifecycle in ["started", "stopped"] {
        sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
            .bind(sandbox_id)
            .bind(lifecycle)
            .execute(&fixture.pool)
            .await?;
    }
    let restopped = fixture
        .store
        .last_published_execution(sandbox_id)
        .await?
        .unwrap();
    assert_ne!(restopped.subject, first.subject);
    assert_eq!(restopped.execution, first.execution);

    publish(bound(E1, "6.1.155")).await?;
    let second = fixture
        .store
        .last_published_execution(sandbox_id)
        .await?
        .unwrap();
    assert_ne!(second.subject, restopped.subject);
    assert_eq!(second.execution.environment.as_deref(), Some(E1));
    Ok(())
}

#[tokio::test]
async fn the_catalog_lists_unretired_environments_with_their_edges() -> TestResult {
    let fixture = postgres_store().await?;
    let e2 = "sha256:2222222222222222222222222222222222222222222222222222222222222222";
    for (reference, version, format, upgrade, memory, retired) in [
        (E0, "1.15.1", "9.0.0", vec![E1], vec![], false),
        (E1, "1.17.0", "12.0.0", vec![E0], vec![], false),
        (e2, "1.17.1", "12.0.0", vec![E1], vec![E1], true),
    ] {
        sqlx::query(sql!(
            "INSERT INTO runtime_environments
                 (environment_ref, runtime, runtime_version, version_sort_key, kernel_version,
                  state_format_name, state_format_version, upgrade_edges, memory_edges,
                  retired_at)
             VALUES ($1, 'firecracker', $2, NULL, '6.1.155', 'firecracker-snapshot', $3,
                     $4, $5, CASE WHEN $6 THEN NOW() END)"
        ))
        .bind(reference)
        .bind(version)
        .bind(format)
        .bind(upgrade)
        .bind(memory)
        .bind(retired)
        .execute(&fixture.pool)
        .await?;
    }
    let catalog = fixture.store.unretired_runtime_environments().await?;
    let e0: pplx_space_common_core::types::EnvironmentReference = E0.parse().unwrap();
    let e1: pplx_space_common_core::types::EnvironmentReference = E1.parse().unwrap();
    assert_eq!(
        catalog
            .iter()
            .map(|environment| environment.reference)
            .collect::<Vec<_>>(),
        [e0, e1]
    );
    assert_eq!(catalog[0].runtime_version, "1.15.1");
    assert_eq!(catalog[0].kernel_version, "6.1.155");
    assert_eq!(catalog[0].state_format_version, "9.0.0");
    assert_eq!(
        catalog[0].upgrade_edges,
        std::collections::BTreeSet::from([e1])
    );
    assert!(catalog[0].memory_edges.is_empty());
    assert_eq!(
        catalog[1].upgrade_edges,
        std::collections::BTreeSet::from([e0])
    );
    Ok(())
}

#[tokio::test]
async fn the_policy_reads_through_the_policy_store() -> TestResult {
    let fixture = postgres_store().await?;
    // The migration's bootstrap: generation 0 with no default.
    let policy = fixture.store.runtime_environment_policy().await?;
    assert_eq!(policy.default_environment, None);
    assert!(policy.environments.is_empty());
    Ok(())
}
