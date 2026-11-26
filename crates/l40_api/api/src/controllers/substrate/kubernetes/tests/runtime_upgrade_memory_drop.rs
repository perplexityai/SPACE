//! Restore-time memory drop and campaign-seal waiting on Kubernetes.

use std::sync::Arc;
use std::time::Duration;

use pplx_space_artifact_model::{SnapshotArtifact as SnapshotFile, StorageUri};
use pplx_space_cpu_arch::Arch;
use pplx_space_execution_spec::{ExecutionMetadata, HostCpu, SnapshotExecutionMetadata};
use pplx_space_runtime_environment_policy_store::{
    PolicyChange, PolicyChangeReason, RuntimeEnvironmentState, set_default,
    set_environment_memory_retired, set_environment_state,
};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::{LocalArtifactId, SnapshotId};
use pplx_space_sql_locator::sql;
use serde_json::{Value, json};
use uuid::Uuid;

use super::*;
use crate::controllers::sandbox::memory_drop::{MapObjects, MemoryDrop};
use crate::controllers::substrate::kubernetes::runtime_upgrade::ActivationRuntimePlan;
use crate::controllers::substrate::runtime_upgrade::{
    EnvironmentMove, UpgradeActivation, UpgradeCause,
};
use crate::controllers::substrate::{ActivationOptions, RecoveryCharge, SubstrateAdapter};
use crate::metrics::sandbox::api::SandboxApiMetricIdentity;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const E0: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const E1: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const BUCKET: &str = "space-test";
const PARENT: &str = "suspend-snapshots";

fn e0_snapshot_execution() -> SnapshotExecutionMetadata {
    SnapshotExecutionMetadata::new(
        ExecutionMetadata::initial_firecracker(Arch::Aarch64, vec![])
            .with_environment(E0.parse().expect("E0")),
    )
    .expect("snapshot execution metadata")
    .with_host_cpu(Some(HostCpu::parse("cpu-a").expect("host cpu")))
}

fn full_metadata(sandbox_id: SandboxId, local_artifact_id: LocalArtifactId) -> Value {
    let execution = e0_snapshot_execution();
    json!({
        "version": 2,
        "execution": execution,
        "sandbox_id": sandbox_id.to_string(),
        "size_bytes": 9000,
        "rootfs_size_bytes": 4,
        "created_at": "2026-11-24T00:00:00Z",
        "source": {"runtime": "firecracker", "template_id": "seed"},
        "rootfs_diff_format": "btrfs-send-v1",
        "rootfs_send_source_id": Uuid::now_v7().to_string(),
        "local_artifact_id": local_artifact_id.to_string(),
        "artifact_kind": "full",
        "artifact_origin": "suspend",
    })
}

async fn seed_catalog_and_policy(
    pool: &PgPool,
    e0_state: RuntimeEnvironmentState,
    e0_memory_retired: bool,
) -> Result<(), BoxError> {
    pplx_space_runtime_environment_policy_store::test_support::apply_memory_retirement_schema(pool)
        .await?;
    for (reference, version, format, upgrade_edges, memory_edges) in [
        (E0, "1.15.1", "9.0.0", vec![E1], Vec::<&str>::new()),
        (E1, "1.17.0", "12.0.0", Vec::new(), Vec::new()),
    ] {
        sqlx::query(sql!(
            "INSERT INTO runtime_environments
                 (environment_ref, runtime, runtime_version, kernel_version,
                  state_format_name, state_format_version, upgrade_edges, memory_edges)
             VALUES ($1, 'firecracker', $2, '6.1.155', 'firecracker-snapshot', $3, $4, $5)"
        ))
        .bind(reference)
        .bind(version)
        .bind(format)
        .bind(upgrade_edges)
        .bind(memory_edges)
        .execute(pool)
        .await?;
    }

    let change = PolicyChange {
        actor: None,
        reason: PolicyChangeReason::new("test policy")?,
    };
    let mut tx = pool.begin().await?;
    set_default(&mut tx, E1.parse()?, &change).await?;
    set_environment_state(&mut tx, E0.parse()?, e0_state, &change).await?;
    if e0_memory_retired {
        set_environment_memory_retired(&mut tx, E0.parse()?, true, &change).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Unplaced suspended Kubernetes sandbox on gone node `gone-e0-node`, pinned to
/// a ready `full` `suspend` snapshot attributed to `E0`, with its checkpoint
/// objects populated in `objects`.
async fn unplaced_e0_restore_subject(
    pool: &PgPool,
    sandbox_id: SandboxId,
    objects: &MapObjects,
) -> Result<(KubernetesSubstrate, SnapshotId, LocalArtifactId), BoxError> {
    insert_test_sandbox_row(pool, sandbox_id, "suspended").await?;
    let artifact_uuid = Uuid::now_v7();
    let local_artifact_id = LocalArtifactId::from_uuid(artifact_uuid);
    let snapshot_id = SnapshotId::from_uuid(Uuid::now_v7());
    let uri = format!("s3://{BUCKET}/{PARENT}/{sandbox_id}/{snapshot_id}?region=us-east-1");
    let parsed_uri = StorageUri::parse(&uri)?;

    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, size_bytes, timestamp,
             local_artifact_id, artifact_kind, artifact_origin,
             runtime, runtime_version, state_format_name, state_format_version,
             execution_metadata
         )
         VALUES (
             $1, $2, 'full', 'ready', 9000, NOW() - INTERVAL '10 minutes',
             $3, 'full', 'suspend',
             'firecracker', '1.15.1', 'firecracker-snapshot', '9.0.0',
             $4
         )"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(artifact_uuid)
    .bind(sqlx::types::Json(e0_snapshot_execution()))
    .execute(pool)
    .await?;

    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, $2)"
    ))
    .bind(snapshot_id)
    .bind(&uri)
    .execute(pool)
    .await?;

    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend',
                active_node_id = NULL,
                target_node_id = NULL,
                evacuation_restore_target = NULL,
                placement_changed_at = NOW() - INTERVAL '1 hour',
                lifecycle_changed_at = NOW() - INTERVAL '10 minutes'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(artifact_uuid)
    .execute(pool)
    .await?;

    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, pod_uid,
             k8s_node_name, state, active, updated_at
         )
         VALUES (
             $1, 1, 'space-sbx-e0-g1', 'space', 'pod-uid-e0-g1',
             'gone-e0-node', 'terminated', FALSE, NOW() - INTERVAL '1 hour'
         )"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;

    objects.put(
        &parsed_uri,
        SnapshotFile::Metadata,
        serde_json::to_vec(&full_metadata(sandbox_id, local_artifact_id))?,
    );
    objects.put(&parsed_uri, SnapshotFile::RootfsBtrfsSend, b"disk".to_vec());

    // Leave `runtime_upgrade_at_activation` at its default (`Disabled`, i.e.
    // `SPACE_RUNTIME_UPGRADE_AT_ACTIVATION_ENABLED=false`) so every test also
    // proves restore-time memory drop runs independently of that flag.
    let substrate = substrate_with_store(pool.clone());
    Ok((substrate, snapshot_id, local_artifact_id))
}

async fn restore(
    substrate: &KubernetesSubstrate,
    sandbox_id: SandboxId,
) -> Result<crate::controllers::substrate::ActivationCompletion, SubstrateError> {
    substrate
        .restore_execution(
            SandboxApiMetricIdentity::native("restore"),
            sandbox_id,
            ActivationOptions {
                blocking: true,
                ready_timeout: Duration::from_secs(2),
            },
            RecoveryCharge::exempt_for_tests(),
        )
        .await
}

#[tokio::test(flavor = "current_thread")]
async fn restore_of_retired_memory_environment_drops_memory_without_placing_a_pod()
-> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let objects = Arc::new(MapObjects::default());
    let (substrate, full_snapshot_id, full_artifact_id) =
        unplaced_e0_restore_subject(&db.pool, sandbox_id, &objects).await?;
    seed_catalog_and_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let substrate = substrate.with_memory_drop(MemoryDrop::new(objects));

    // Inert Kubernetes client rejects every pod operation; completing the inline
    // drop without hitting it proves no pod was placed or queried.
    let err = restore(&substrate, sandbox_id)
        .await
        .expect_err("disk cold-boot handoff returns retryable 503 until disk boot lands");
    assert!(
        matches!(&err, SubstrateError::Timeout { message, .. } if message.contains("its memory was dropped, its disk is durable")),
        "expected memory-dropped Timeout after inline memory drop, got {err:?}"
    );

    let (lifecycle, pin_id, pin_kind, pin_origin, lease_token): (
        String,
        Option<Uuid>,
        Option<String>,
        Option<String>,
        Option<Uuid>,
    ) = sqlx::query_as(sql!(
        "SELECT lifecycle, current_local_artifact_id, current_local_artifact_kind,
                current_local_artifact_origin, restore_lease_token
           FROM sandboxes
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(lifecycle, "suspended");
    assert_ne!(pin_id, Some(full_artifact_id.as_uuid()));
    assert_eq!(pin_kind.as_deref(), Some("disk"));
    assert_eq!(pin_origin.as_deref(), Some("stop"));
    assert_eq!(lease_token, None);

    // Retained full snapshot remains ready and unorphaned for rollback.
    let (full_state, orphaned): (String, Option<time::OffsetDateTime>) = sqlx::query_as(sql!(
        "SELECT sn.state, ssl.orphaned_at
           FROM snapshots sn
           JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
          WHERE sn.id = $1"
    ))
    .bind(full_snapshot_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(full_state, "ready");
    assert!(orphaned.is_none());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn pre_pod_memory_drop_failure_parks_row_suspended_without_booting_retired_memory()
-> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let populated_objects = MapObjects::default();
    let (substrate, _full_snapshot_id, full_artifact_id) =
        unplaced_e0_restore_subject(&db.pool, sandbox_id, &populated_objects).await?;
    seed_catalog_and_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;

    // Case 1: memory_drop disabled on this replica (`None`).
    let err = restore(&substrate, sandbox_id)
        .await
        .expect_err("disabled memory_drop must refuse with retryable 503");
    assert!(
        matches!(&err, SubstrateError::Timeout { message, .. } if message.contains("disabled on this API replica")),
        "got {err:?}"
    );

    // Case 2: object-store failure during `drop_for_restore` (empty object store).
    let substrate = substrate.with_memory_drop(MemoryDrop::new(Arc::new(MapObjects::default())));
    let err = restore(&substrate, sandbox_id)
        .await
        .expect_err("S3 failure during memory drop must refuse with retryable 503");
    assert!(
        matches!(&err, SubstrateError::Timeout { message, .. } if message.contains("writing the disk artifact failed")),
        "got {err:?}"
    );

    // Case 3: permanent structural refusal (encrypted volume) returns 412
    // FailedPrecondition rather than retryable 503 Timeout.
    sqlx::query(sql!(
        "UPDATE sandboxes SET volume_encryption = TRUE WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let err = restore(&substrate, sandbox_id)
        .await
        .expect_err("encrypted retired-memory restore must fail precondition");
    assert!(
        matches!(&err, SubstrateError::FailedPrecondition { message } if message.contains("encrypted")),
        "got {err:?}"
    );

    let (lifecycle, pin_id, pin_kind, lease_token): (
        String,
        Option<Uuid>,
        Option<String>,
        Option<Uuid>,
    ) = sqlx::query_as(sql!(
        "SELECT lifecycle, current_local_artifact_id, current_local_artifact_kind,
                restore_lease_token
           FROM sandboxes
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(lifecycle, "suspended");
    assert_eq!(pin_id, Some(full_artifact_id.as_uuid()));
    assert_eq!(pin_kind.as_deref(), Some("full"));
    assert_eq!(lease_token, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn policy_cache_refreshes_immediately_across_memory_retire_and_unretire()
-> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let objects = Arc::new(MapObjects::default());
    let (substrate, _snapshot_id, _artifact_id) =
        unplaced_e0_restore_subject(&db.pool, sandbox_id, &objects).await?;
    seed_catalog_and_policy(&db.pool, RuntimeEnvironmentState::Allowed, false).await?;

    // Populate the policy cache while E0 is Allowed: hits the bound-Allowed
    // fast path (NoWakeUpgrade).
    let initial = substrate
        .decide_activation_runtime(sandbox_id, UpgradeActivation::Restore, None)
        .await?;
    assert_eq!(initial, ActivationRuntimePlan::NoWakeUpgrade);

    // Deprecate and retire E0's memory directly in Postgres (as if on another
    // API replica, without invalidating this replica's cache) within the
    // 5-second cache TTL: next Restore refreshes up front and drops memory
    // instead of taking the stale Allowed fast path.
    let deprecate = PolicyChange {
        actor: None,
        reason: PolicyChangeReason::new("deprecate e0")?,
    };
    let change = PolicyChange {
        actor: None,
        reason: PolicyChangeReason::new("retire e0 memory")?,
    };
    let mut tx = db.pool.begin().await?;
    set_environment_state(
        &mut tx,
        E0.parse()?,
        RuntimeEnvironmentState::Deprecated,
        &deprecate,
    )
    .await?;
    set_environment_memory_retired(&mut tx, E0.parse()?, true, &change).await?;
    tx.commit().await?;
    let after_retire = substrate
        .decide_activation_runtime(sandbox_id, UpgradeActivation::Restore, None)
        .await?;
    assert_eq!(
        after_retire,
        ActivationRuntimePlan::DropMemoryAndColdBoot(EnvironmentMove {
            from: E0.parse()?,
            to: E1.parse()?,
            cause: UpgradeCause::MemoryRetired,
        })
    );

    // Unretire E0's memory within the cache TTL: next Restore refreshes before
    // discarding memory and returns to Stay (NoWakeUpgrade).
    let unretire = PolicyChange {
        actor: None,
        reason: PolicyChangeReason::new("unretire e0 memory")?,
    };
    let mut tx = db.pool.begin().await?;
    set_environment_memory_retired(&mut tx, E0.parse()?, false, &unretire).await?;
    tx.commit().await?;
    let after_unretire = substrate
        .decide_activation_runtime(sandbox_id, UpgradeActivation::Restore, None)
        .await?;
    assert_eq!(after_unretire, ActivationRuntimePlan::NoWakeUpgrade);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restore_waits_for_an_active_campaign_memory_drop_seal() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let objects = Arc::new(MapObjects::default());
    let (substrate, _snapshot_id, _artifact_id) =
        unplaced_e0_restore_subject(&db.pool, sandbox_id, &objects).await?;
    seed_catalog_and_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let substrate = substrate.with_memory_drop(MemoryDrop::new(objects));

    // Simulate a background memory-drop campaign holding the row's seal while
    // keeping `lifecycle = 'suspended'`, then releasing the seal after 150ms.
    let seal_token = Uuid::now_v7();
    let mut tx = db.pool.begin().await?;
    let sealed = pplx_space_sandbox_store::memory_drop::seal(
        &mut tx,
        sandbox_id,
        seal_token,
        Duration::from_secs(30),
    )
    .await?;
    tx.commit().await?;
    assert!(sealed.is_some(), "campaign seal must succeed");

    let release_pool = db.pool.clone();
    let releaser = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        pplx_space_sandbox_store::memory_drop::release(&release_pool, sandbox_id, seal_token)
            .await
            .expect("release campaign seal");
    });

    let err = restore(&substrate, sandbox_id)
        .await
        .expect_err("after waiting for seal release, inline drop parks disk and returns 503");
    releaser.await?;
    assert!(
        matches!(&err, SubstrateError::Timeout { message, .. } if message.contains("its memory was dropped, its disk is durable")),
        "got {err:?}"
    );

    let pin_kind: Option<String> = sqlx::query_scalar(sql!(
        "SELECT current_local_artifact_kind FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(pin_kind.as_deref(), Some("disk"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unattributed_and_same_node_markerless_restores_refuse_412_while_deprecated_memory_is_retired()
-> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let objects = Arc::new(MapObjects::default());
    let (substrate, snapshot_id, _artifact_id) =
        unplaced_e0_restore_subject(&db.pool, sandbox_id, &objects).await?;
    seed_catalog_and_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;

    // Path 1: same-node markerless restore (`storage_uri = None` while
    // `current_local_artifact_id IS NULL`) resolves `recorded = None` and must
    // return 412 rather than booting potentially retired E0 memory on a worker
    // node that still allows `deprecated` environments.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = NULL,
                current_local_artifact_kind = NULL,
                current_local_artifact_origin = NULL
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let err = substrate
        .decide_activation_runtime(sandbox_id, UpgradeActivation::Restore, None)
        .await
        .expect_err("markerless same-node restore must refuse 412 while E0 is deprecated+retired");
    assert!(
        matches!(&err, SubstrateError::FailedPrecondition { message } if message.contains("unretire")),
        "got {err:?}"
    );

    // Path 2: unattributed checkpoint (for example waived via `strand: true`)
    // resolves `attribute(...) = None` and must also return 412 while E0 is
    // `deprecated` with retired memory.
    sqlx::query(sql!(
        "UPDATE snapshots
            SET execution_metadata = NULL,
                runtime = NULL,
                runtime_version = NULL,
                state_format_name = NULL,
                state_format_version = NULL
          WHERE id = $1"
    ))
    .bind(snapshot_id)
    .execute(&db.pool)
    .await?;
    let uri: String = sqlx::query_scalar(sql!(
        "SELECT storage_connection_string
           FROM snapshot_storage_locations
          WHERE snapshot_id = $1"
    ))
    .bind(snapshot_id)
    .fetch_one(&db.pool)
    .await?;
    let err = substrate
        .decide_activation_runtime(sandbox_id, UpgradeActivation::Restore, Some(&uri))
        .await
        .expect_err(
            "unattributed checkpoint restore must refuse 412 while E0 is deprecated+retired",
        );
    assert!(
        matches!(&err, SubstrateError::FailedPrecondition { message } if message.contains("unretire")),
        "got {err:?}"
    );

    // Once E0 moves to `blocked` (so worker nodes already block E0 themselves),
    // E0's permanent `memory_retired = true` flag no longer bricks unrelated
    // unattributed restores forever.
    let block = PolicyChange {
        actor: None,
        reason: PolicyChangeReason::new("block e0")?,
    };
    let mut tx = db.pool.begin().await?;
    set_environment_state(
        &mut tx,
        E0.parse()?,
        RuntimeEnvironmentState::Blocked,
        &block,
    )
    .await?;
    tx.commit().await?;
    let after_block = substrate
        .decide_activation_runtime(sandbox_id, UpgradeActivation::Restore, Some(&uri))
        .await?;
    assert_eq!(after_block, ActivationRuntimePlan::NoWakeUpgrade);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn non_retired_pre_pod_restore_failures_preserve_existing_claim_and_lease_invariants()
-> Result<(), BoxError> {
    use crate::controllers::substrate::runtime_upgrade::RuntimeUpgradeAtActivation;

    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let objects = Arc::new(MapObjects::default());
    let (mut substrate, snapshot_id, _artifact_id) =
        unplaced_e0_restore_subject(&db.pool, sandbox_id, &objects).await?;
    seed_catalog_and_policy(&db.pool, RuntimeEnvironmentState::Allowed, false).await?;

    let saved_uri: String = sqlx::query_scalar(sql!(
        "SELECT storage_connection_string
           FROM snapshot_storage_locations
          WHERE snapshot_id = $1"
    ))
    .bind(snapshot_id)
    .fetch_one(&db.pool)
    .await?;

    // 1. Pending upload (`suspend_restore_plan` returning `Timeout` before
    // `ensure_pod_exists` when `memory_drop_entered` is false) must leave the
    // row in `restoring` and keep its user restore lease intact.
    sqlx::query(sql!(
        "DELETE FROM sandbox_k8s_executions WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "DELETE FROM snapshot_storage_locations WHERE snapshot_id = $1"
    ))
    .bind(snapshot_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE snapshots SET state = 'creating' WHERE id = $1"
    ))
    .bind(snapshot_id)
    .execute(&db.pool)
    .await?;

    let err = restore(&substrate, sandbox_id)
        .await
        .expect_err("uploading snapshot must return retryable Timeout");
    assert!(matches!(err, SubstrateError::Timeout { .. }), "got {err:?}");
    let (lifecycle, lease_active): (String, bool) = sqlx::query_as(sql!(
        "SELECT lifecycle,
                restore_lease_token IS NOT NULL AND restore_lease_expires_at > NOW()
           FROM sandboxes
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(lifecycle, "restoring");
    assert!(
        lease_active,
        "non-retired pending-upload Timeout must keep its unexpired restore lease"
    );

    // 2. Restore the ready remote snapshot and block E0 without an upgrade
    // edge. Re-driving the lapsed `restoring` row (`RedroveRestoring`) fails
    // with `FailedPrecondition` (`Stranded`) before `ensure_pod_exists`: it
    // must keep `lifecycle = 'restoring'` while clearing `restore_lease_token`.
    sqlx::query(sql!("UPDATE snapshots SET state = 'ready' WHERE id = $1"))
        .bind(snapshot_id)
        .execute(&db.pool)
        .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, $2)"
    ))
    .bind(snapshot_id)
    .bind(&saved_uri)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE runtime_environments
            SET upgrade_edges = '{}'::text[]
          WHERE environment_ref = $1"
    ))
    .bind(E0)
    .execute(&db.pool)
    .await?;
    let block = PolicyChange {
        actor: None,
        reason: PolicyChangeReason::new("block e0 without upgrade edge")?,
    };
    let mut tx = db.pool.begin().await?;
    set_environment_state(
        &mut tx,
        E0.parse()?,
        RuntimeEnvironmentState::Blocked,
        &block,
    )
    .await?;
    tx.commit().await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() - INTERVAL '5 minutes'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;
    let err = restore(&substrate, sandbox_id)
        .await
        .expect_err("stranded RedroveRestoring must fail precondition");
    assert!(
        matches!(err, SubstrateError::FailedPrecondition { .. }),
        "got {err:?}"
    );
    let (lifecycle, lease_expired): (String, bool) = sqlx::query_as(sql!(
        "SELECT lifecycle,
                restore_lease_expires_at IS NOT NULL AND restore_lease_expires_at <= NOW()
           FROM sandboxes
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        lifecycle, "restoring",
        "non-retired RedroveRestoring failure must not revert lifecycle to suspended"
    );
    assert!(
        lease_expired,
        "non-retired RedroveRestoring FailedPrecondition must expire restore_lease_expires_at immediately"
    );

    // 3. Fresh claim from `suspended` (`FreshFromSuspended`) under the same
    // pre-pod `Stranded` failure reverts `lifecycle` to `suspended` and clears
    // `restore_lease_token`.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'suspended',
                restore_lease_token = NULL,
                restore_lease_expires_at = NULL
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let err = restore(&substrate, sandbox_id)
        .await
        .expect_err("stranded FreshFromSuspended must fail precondition");
    assert!(
        matches!(err, SubstrateError::FailedPrecondition { .. }),
        "got {err:?}"
    );
    let (lifecycle, lease_token): (String, Option<Uuid>) = sqlx::query_as(sql!(
        "SELECT lifecycle, restore_lease_token FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(lifecycle, "suspended");
    assert_eq!(lease_token, None);
    Ok(())
}
