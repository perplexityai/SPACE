use pplx_space_common_core::sql;
use pplx_space_common_core::types::{LocalArtifactId, OrgId, UserId};
use pplx_space_common_core::types::{NodeId, SandboxId};
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    AutoLifecycleExecution, FieldPatch, SandboxDesiredStatePatch, SandboxMetadata,
    SandboxMetadataValue, any_undescribed_artifact_owner, durably_restorable, get,
    update_desired_state,
};

/// The three desired-state columns, read back raw.
async fn read_desired_state(
    pool: &PgPool,
    id: Uuid,
) -> TestResult<(serde_json::Value, Option<i32>, Option<i32>)> {
    let row = sqlx::query_as::<_, (serde_json::Value, Option<i32>, Option<i32>)>(sql!(
        "SELECT metadata, vcpu, memory_mib FROM sandboxes WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?;
    Ok(row)
}

/// The live row version's inserting transaction id. Changes iff Postgres
/// wrote a new tuple for the row.
async fn read_row_version(pool: &PgPool, id: Uuid) -> TestResult<String> {
    let (xmin,) =
        sqlx::query_as::<_, (String,)>(sql!("SELECT xmin::TEXT FROM sandboxes WHERE id = $1"))
            .bind(id)
            .fetch_one(pool)
            .await?;
    Ok(xmin)
}

async fn read_updated_at(pool: &PgPool, id: Uuid) -> TestResult<time::OffsetDateTime> {
    let (updated_at,) = sqlx::query_as::<_, (time::OffsetDateTime,)>(sql!(
        "SELECT updated_at FROM sandboxes WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?;
    Ok(updated_at)
}

fn metadata_of(pairs: &[(&str, &str)]) -> SandboxMetadata {
    pairs
        .iter()
        .map(|(key, value)| {
            (
                (*key).to_string(),
                SandboxMetadataValue::String((*value).to_string()),
            )
        })
        .collect()
}

/// Seeded by the PostgreSQL migrations.
const SEED_USER: Uuid = Uuid::from_u128(0x10);
const SEED_ORG: Uuid = Uuid::from_u128(0x1);

async fn insert_sandbox(pool: &PgPool, name: Option<&str>, lifecycle: &str) -> TestResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, name, user_id, org_id, managed_by, lifecycle, template_id,
             network_policy_generation)
         VALUES ($1, $2, $3, $4, 'k8s', $5, 'tmpl-store-test', 1)"
    ))
    .bind(id)
    .bind(name)
    .bind(SEED_USER)
    .bind(SEED_ORG)
    .bind(lifecycle)
    .execute(pool)
    .await?;
    Ok(id)
}

/// `FieldPatch::default()` must be available for every `T`, not just the
/// `Default` ones — `Unchanged` carries no value. This only compiles if the
/// impl is unbounded, so it guards the derive from creeping back in.
#[test]
fn field_patch_default_does_not_require_t_default() {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct NotDefault;

    assert_eq!(FieldPatch::<NotDefault>::default(), FieldPatch::Unchanged);
    // …and the struct's own derive stays usable through it.
    assert_eq!(
        SandboxDesiredStatePatch::default().vcpu,
        FieldPatch::Unchanged
    );
}

/// Only a Kubernetes-managed row can prove there is no execution; a Direct or
/// unrecognized `managed_by` leaves the outcome uncertain.
#[test]
fn proves_no_execution_only_for_kubernetes_rows() {
    for (managed_by, proves) in [
        ("k8s", true),
        ("space", false),
        ("kubernetes", false),
        ("", false),
    ] {
        let execution = AutoLifecycleExecution {
            managed_by: managed_by.to_owned(),
            lifecycle: "started".to_owned(),
            placement: "unplaced".to_owned(),
            active_node_id: None,
            target_node_id: None,
            execution_generation: None,
            has_direct_route: false,
            has_activation_lease: false,
            policy: sqlx::types::Json(Default::default()),
        };
        assert_eq!(execution.proves_no_execution(), proves, "{managed_by:?}");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn get_returns_the_inserted_row() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = insert_sandbox(&db.pool, Some("store-get"), "suspended").await?;

    let record = get(&db.pool, SandboxId::from_uuid(id))
        .await?
        .expect("inserted row must be found");

    assert_eq!(record.id, SandboxId::from_uuid(id));
    assert_eq!(record.name.as_deref(), Some("store-get"));
    assert_eq!(record.user_id, UserId::from_uuid(SEED_USER));
    assert_eq!(record.org_id, OrgId::from_uuid(SEED_ORG));
    assert_eq!(record.managed_by, "k8s");
    assert_eq!(record.lifecycle, "suspended");
    // Column defaults flow through the record untouched.
    assert_eq!(record.placement, "unplaced");
    assert_eq!(record.active_node_id, None);
    assert_eq!(record.target_node_id, None);
    assert_eq!(record.template_id.as_deref(), Some("tmpl-store-test"));
    assert_eq!(record.current_local_artifact_id, None);
    assert!(record.created_at <= record.updated_at);

    // A populated artifact marker decodes back out (the drain-promotion
    // caller reads exactly this). The marker is a three-column identity —
    // `sandboxes_current_local_artifact_identity_check` forbids setting the
    // id without its kind and origin — so the seed writes a valid triple even
    // though the record currently exposes only the id.
    let artifact_id = Uuid::now_v7();
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(id)
    .bind(artifact_id)
    .execute(&db.pool)
    .await?;
    let record = get(&db.pool, SandboxId::from_uuid(id))
        .await?
        .expect("row still present");
    assert_eq!(
        record.current_local_artifact_id,
        Some(LocalArtifactId::from_uuid(artifact_id))
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn get_returns_none_for_a_missing_row() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let missing = get(&db.pool, SandboxId::new()).await?;
    assert!(missing.is_none());
    Ok(())
}

/// Each field moves independently, and `Unchanged` leaves its column alone.
#[tokio::test(flavor = "current_thread")]
async fn update_desired_state_applies_only_the_named_fields() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = insert_sandbox(&db.pool, Some("patch-fields"), "started").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET vcpu = 2, memory_mib = 512 WHERE id = $1"
    ))
    .bind(id)
    .execute(&db.pool)
    .await?;

    // Metadata alone: the resource columns must not move.
    let metadata = metadata_of(&[("team", "core")]);
    assert!(
        update_desired_state(
            &db.pool,
            SandboxId::from_uuid(id),
            SandboxDesiredStatePatch {
                metadata: Some(&metadata),
                ..Default::default()
            },
        )
        .await?
    );
    let (stored_metadata, vcpu, memory_mib) = read_desired_state(&db.pool, id).await?;
    assert_eq!(stored_metadata, serde_json::json!({"team": "core"}));
    assert_eq!((vcpu, memory_mib), (Some(2), Some(512)));

    // One resource column alone: the other and the metadata must not move.
    assert!(
        update_desired_state(
            &db.pool,
            SandboxId::from_uuid(id),
            SandboxDesiredStatePatch {
                vcpu: FieldPatch::Set(8),
                ..Default::default()
            },
        )
        .await?
    );
    let (stored_metadata, vcpu, memory_mib) = read_desired_state(&db.pool, id).await?;
    assert_eq!(stored_metadata, serde_json::json!({"team": "core"}));
    assert_eq!((vcpu, memory_mib), (Some(8), Some(512)));
    Ok(())
}

/// `Clear` is the state `Option` could not express: it writes NULL, which
/// on these columns means "inherit the template default". `Unchanged` on the
/// same column in the same call must not.
#[tokio::test(flavor = "current_thread")]
async fn update_desired_state_clear_writes_null_and_unchanged_does_not() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = insert_sandbox(&db.pool, Some("patch-clear"), "started").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET vcpu = 4, memory_mib = 1024 WHERE id = $1"
    ))
    .bind(id)
    .execute(&db.pool)
    .await?;

    assert!(
        update_desired_state(
            &db.pool,
            SandboxId::from_uuid(id),
            SandboxDesiredStatePatch {
                vcpu: FieldPatch::Clear,
                memory_mib: FieldPatch::Unchanged,
                ..Default::default()
            },
        )
        .await?
    );

    let (_, vcpu, memory_mib) = read_desired_state(&db.pool, id).await?;
    assert_eq!(vcpu, None, "Clear writes NULL");
    assert_eq!(memory_mib, Some(1024), "Unchanged leaves the column alone");
    Ok(())
}

/// Every field moves in one statement, and `Set` works on both resource
/// columns — `vcpu` alone carries `Set` in the tests above.
#[tokio::test(flavor = "current_thread")]
async fn update_desired_state_applies_all_fields_in_one_call() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = insert_sandbox(&db.pool, Some("patch-combined"), "started").await?;

    let metadata = metadata_of(&[("team", "core")]);
    assert!(
        update_desired_state(
            &db.pool,
            SandboxId::from_uuid(id),
            SandboxDesiredStatePatch {
                metadata: Some(&metadata),
                vcpu: FieldPatch::Set(4),
                memory_mib: FieldPatch::Set(2048),
            },
        )
        .await?
    );

    let (stored_metadata, vcpu, memory_mib) = read_desired_state(&db.pool, id).await?;
    assert_eq!(stored_metadata, serde_json::json!({"team": "core"}));
    assert_eq!((vcpu, memory_mib), (Some(4), Some(2048)));
    Ok(())
}

/// The metadata document is an untagged enum: every variant must survive the
/// round-trip as its own JSON type rather than collapsing into a string.
#[tokio::test(flavor = "current_thread")]
async fn update_desired_state_round_trips_every_metadata_value_type() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = insert_sandbox(&db.pool, Some("patch-metadata-types"), "started").await?;

    let metadata: SandboxMetadata = [
        (
            "text".to_string(),
            SandboxMetadataValue::String("value".to_string()),
        ),
        ("count".to_string(), SandboxMetadataValue::Number(7)),
        ("enabled".to_string(), SandboxMetadataValue::Bool(true)),
    ]
    .into_iter()
    .collect();

    update_desired_state(
        &db.pool,
        SandboxId::from_uuid(id),
        SandboxDesiredStatePatch {
            metadata: Some(&metadata),
            ..Default::default()
        },
    )
    .await?;

    let (stored, _, _) = read_desired_state(&db.pool, id).await?;
    assert_eq!(
        stored,
        serde_json::json!({"text": "value", "count": 7, "enabled": true}),
        "numbers and booleans must not decode as strings"
    );
    Ok(())
}

/// An empty document is a real value, distinct from "no metadata supplied".
#[tokio::test(flavor = "current_thread")]
async fn update_desired_state_writes_an_empty_metadata_document() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = insert_sandbox(&db.pool, Some("patch-metadata-empty"), "started").await?;
    let seeded = metadata_of(&[("team", "core")]);
    update_desired_state(
        &db.pool,
        SandboxId::from_uuid(id),
        SandboxDesiredStatePatch {
            metadata: Some(&seeded),
            ..Default::default()
        },
    )
    .await?;

    // `Some(&{})` clears the document; `None` would have left it alone.
    let empty = SandboxMetadata::new();
    update_desired_state(
        &db.pool,
        SandboxId::from_uuid(id),
        SandboxDesiredStatePatch {
            metadata: Some(&empty),
            ..Default::default()
        },
    )
    .await?;
    let (stored, _, _) = read_desired_state(&db.pool, id).await?;
    assert_eq!(stored, serde_json::json!({}));

    // …and `None` really does leave it alone.
    update_desired_state(
        &db.pool,
        SandboxId::from_uuid(id),
        SandboxDesiredStatePatch {
            vcpu: FieldPatch::Set(1),
            ..Default::default()
        },
    )
    .await?;
    let (stored, _, _) = read_desired_state(&db.pool, id).await?;
    assert_eq!(stored, serde_json::json!({}));
    Ok(())
}

/// Pins the hazard the verb documents: an all-`Unchanged` patch names no
/// column, but it is still an `UPDATE`, so the row's guardless `BEFORE
/// UPDATE` triggers fire. `sandboxes_clear_restore_lease` nulls the lease
/// columns whenever `lifecycle <> 'restoring'` — with no check that the
/// lifecycle changed, or that this statement touched them.
///
/// This is the `pids_max`-only request path: the runtime call carries the
/// change and nothing here needs writing, yet the write still happens.
#[tokio::test(flavor = "current_thread")]
async fn update_desired_state_empty_patch_still_fires_the_row_triggers() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = insert_sandbox(&db.pool, Some("patch-empty"), "started").await?;
    let before = read_row_version(&db.pool, id).await?;

    let applied = update_desired_state(
        &db.pool,
        SandboxId::from_uuid(id),
        SandboxDesiredStatePatch::default(),
    )
    .await?;
    assert!(
        applied,
        "an all-Unchanged patch still matches and writes the row"
    );

    // `xmin` is the inserting transaction of the live row version. A new
    // value means Postgres wrote a fresh tuple and therefore ran the row's
    // triggers — the self-assignments are not optimised away.
    assert_ne!(
        read_row_version(&db.pool, id).await?,
        before,
        "an all-Unchanged patch still rewrites the row"
    );

    // …and it is genuinely a no-op for the columns, `updated_at` included.
    let (metadata, vcpu, memory_mib) = read_desired_state(&db.pool, id).await?;
    assert_eq!(metadata, serde_json::json!({}));
    assert_eq!((vcpu, memory_mib), (None, None));
    Ok(())
}

/// A patch against a row that does not exist is an outcome, not an error.
#[tokio::test(flavor = "current_thread")]
async fn update_desired_state_reports_a_missing_row() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let applied = update_desired_state(
        &db.pool,
        SandboxId::new(),
        SandboxDesiredStatePatch {
            vcpu: FieldPatch::Set(1),
            ..Default::default()
        },
    )
    .await?;
    assert!(!applied);
    Ok(())
}

/// Pins the behaviour change this verb makes deliberately. The call sites it
/// replaces wrote `updated_at = NOW()` inline, which makes
/// `sandboxes_set_updated_at` return early and stamp the row even when
/// nothing changed. Leaving the column to the trigger means a patch that
/// rewrites a value to itself no longer bumps `updated_at`, while one that
/// genuinely changes a column still does.
#[tokio::test(flavor = "current_thread")]
async fn update_desired_state_leaves_updated_at_to_the_trigger() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = insert_sandbox(&db.pool, Some("patch-updated-at"), "started").await?;
    let metadata = metadata_of(&[("team", "core")]);
    update_desired_state(
        &db.pool,
        SandboxId::from_uuid(id),
        SandboxDesiredStatePatch {
            metadata: Some(&metadata),
            ..Default::default()
        },
    )
    .await?;
    let after_first = read_updated_at(&db.pool, id).await?;

    // Same value again: the trigger sees no meaningful change and holds.
    update_desired_state(
        &db.pool,
        SandboxId::from_uuid(id),
        SandboxDesiredStatePatch {
            metadata: Some(&metadata),
            ..Default::default()
        },
    )
    .await?;
    assert_eq!(
        read_updated_at(&db.pool, id).await?,
        after_first,
        "an idempotent patch must not bump updated_at"
    );

    // A real change still stamps it.
    let changed = metadata_of(&[("team", "platform")]);
    update_desired_state(
        &db.pool,
        SandboxId::from_uuid(id),
        SandboxDesiredStatePatch {
            metadata: Some(&changed),
            ..Default::default()
        },
    )
    .await?;
    assert!(
        read_updated_at(&db.pool, id).await? > after_first,
        "a real change must bump updated_at"
    );
    Ok(())
}

/// The write must compose into a caller's open transaction — the controller
/// paths that will adopt it write and enqueue their reconcile in one tx.
#[tokio::test(flavor = "current_thread")]
async fn update_desired_state_composes_into_the_callers_transaction() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = insert_sandbox(&db.pool, Some("patch-tx"), "started").await?;

    let mut tx = db.pool.begin().await?;
    assert!(
        update_desired_state(
            &mut *tx,
            SandboxId::from_uuid(id),
            SandboxDesiredStatePatch {
                vcpu: FieldPatch::Set(16),
                ..Default::default()
            },
        )
        .await?
    );
    tx.rollback().await?;

    let (_, vcpu, _) = read_desired_state(&db.pool, id).await?;
    assert_eq!(vcpu, None, "rolled-back write must not persist");
    Ok(())
}

/// The verb must compose into a caller's open transaction — that is the
/// reason it takes an executor instead of owning a pool.
#[tokio::test(flavor = "current_thread")]
async fn get_reads_uncommitted_rows_inside_the_callers_transaction() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = SandboxId::new();
    let mut tx = db.pool.begin().await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, managed_by, lifecycle,
                                network_policy_generation)
         VALUES ($1, $2, $3, 'k8s', 'creating', 1)"
    ))
    .bind(id)
    .bind(SEED_USER)
    .bind(SEED_ORG)
    .execute(&mut *tx)
    .await?;

    let inside = get(&mut *tx, id).await?;
    assert!(inside.is_some(), "uncommitted row visible inside the tx");

    tx.rollback().await?;
    let outside = get(&db.pool, id).await?;
    assert!(outside.is_none(), "rolled-back row invisible outside");
    Ok(())
}

/// Pin a local artifact on the row and place it, the way a suspend does.
async fn pin_and_place(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: Option<NodeId>,
) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend',
                active_node_id = $3
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(Uuid::now_v7())
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// The verb's own contract, at the layer that owns it. The behavioural matrix
/// over durability and residency lives with the census that consumes this —
/// what is asserted here is the API's own shape: the short-circuit, the marker
/// conjunct, and the two residency outcomes that need no `snapshots` fixture.
#[tokio::test(flavor = "current_thread")]
async fn any_undescribed_artifact_owner_answers_its_own_contract() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let this_node = NodeId::new();
    let other_node = NodeId::new();

    // A sandbox with no marker is not holding anything to describe.
    let markerless = insert_sandbox(&db.pool, Some("store-markerless"), "suspended").await?;
    assert_eq!(
        any_undescribed_artifact_owner(&db.pool, this_node, &[SandboxId::from_uuid(markerless)])
            .await?,
        None,
        "`current_local_artifact_id IS NULL` means there is nothing on this disk to account for"
    );

    // Marker, no `snapshots` row at all, and placed here: the whole point.
    let held = insert_sandbox(&db.pool, Some("store-held"), "suspended").await?;
    pin_and_place(&db.pool, SandboxId::from_uuid(held), Some(this_node)).await?;
    assert_eq!(
        any_undescribed_artifact_owner(&db.pool, this_node, &[SandboxId::from_uuid(held)]).await?,
        Some(SandboxId::from_uuid(held)),
        "a pinned artifact with no durable copy, on this node, is exactly what the caller \
         must not certify away"
    );

    // Same row, asked about from a node it was never on: demonstrably departed.
    assert_eq!(
        any_undescribed_artifact_owner(&db.pool, other_node, &[SandboxId::from_uuid(held)]).await?,
        None,
        "a signal names some node and none names the asking node, so it has left"
    );

    // The empty-candidate short circuit, asserted against an executor that
    // cannot answer. `id = ANY('{}')` selects nothing on its own, so a pool
    // that still works would return `None` whether or not the early return
    // exists — the assertion has to be unsatisfiable without it. Last in the
    // test because the pool is shared with every case above.
    db.pool.close().await;
    assert_eq!(
        any_undescribed_artifact_owner(&db.pool, this_node, &[]).await?,
        None,
        "no candidates is answered without touching the database at all"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restore_observation_uses_database_lease_expiration() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = insert_sandbox(&db.pool, None, "restoring").await?;
    let id = SandboxId::from_uuid(id);
    assert!(!super::restore_owner_active(&db.pool, id).await?);
    for (seconds, expected) in [(-60_i64, false), (60, true)] {
        sqlx::query(sql!("UPDATE sandboxes SET restore_lease_token = space_uuid_v7(), restore_lease_expires_at = NOW() + ($2 * INTERVAL '1 second') WHERE id = $1"))
            .bind(id).bind(seconds).execute(&db.pool).await?;
        assert_eq!(super::restore_owner_active(&db.pool, id).await?, expected);
    }
    assert!(!super::restore_owner_active(&db.pool, SandboxId::new()).await?);
    Ok(())
}

/// Inserts a full suspend snapshot of `sandbox`, `state`, at `timestamp`, for
/// local artifact `artifact`, with a storage location orphaned when asked.
async fn insert_full_snapshot(
    pool: &PgPool,
    sandbox: Uuid,
    artifact: Uuid,
    state: &str,
    timestamp: &str,
    orphaned: bool,
) -> TestResult {
    let snapshot = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, timestamp, local_artifact_id, artifact_kind,
             artifact_origin, org_id
         )
         VALUES ($1, $2, 'full', $3, $4::TIMESTAMPTZ, $5, 'full', 'suspend', $6)"
    ))
    .bind(snapshot)
    .bind(sandbox)
    .bind(state)
    .bind(timestamp)
    .bind(artifact)
    .bind(SEED_ORG)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (
             id, snapshot_id, storage_connection_string, org_id, orphaned_at
         ) VALUES (
             $1, $2, 's3://test-bucket/snapshots/restorable?region=us-east-1', $3,
             CASE WHEN $4 THEN NOW() END
         )"
    ))
    .bind(Uuid::now_v7())
    .bind(snapshot)
    .bind(SEED_ORG)
    .bind(orphaned)
    .execute(pool)
    .await?;
    Ok(())
}

/// A sandbox marked to a local artifact is durably restorable only through
/// that artifact's ready backup with a live location: a stale ready snapshot
/// of another artifact doesn't count, since the restore wouldn't select it. An
/// unmarked sandbox needs a ready full snapshot at least as fresh as its last
/// placement change, with no newer full row in any state; an evacuation-marked
/// one only a ready newest full snapshot. A restoring row needs a source taken
/// before the restore started. A missing sandbox isn't restorable.
#[tokio::test(flavor = "current_thread")]
async fn durably_restorable_follows_the_marker_and_placement() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    let marked = insert_sandbox(pool, None, "suspended").await?;
    let pinned = Uuid::now_v7();
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(marked)
    .bind(pinned)
    .execute(pool)
    .await?;
    let marked_id = SandboxId::from_uuid(marked);
    insert_full_snapshot(
        pool,
        marked,
        Uuid::now_v7(),
        "ready",
        "2026-01-01T00:00:00Z",
        false,
    )
    .await?;
    assert!(
        !durably_restorable(pool, marked_id).await?,
        "another artifact's snapshot"
    );
    insert_full_snapshot(pool, marked, pinned, "ready", "2026-01-02T00:00:00Z", true).await?;
    assert!(
        !durably_restorable(pool, marked_id).await?,
        "an orphaned location"
    );
    insert_full_snapshot(pool, marked, pinned, "ready", "2026-01-03T00:00:00Z", false).await?;
    assert!(
        durably_restorable(pool, marked_id).await?,
        "the marked artifact's backup"
    );
    insert_full_snapshot(
        pool,
        marked,
        Uuid::now_v7(),
        "failed",
        "2026-01-04T00:00:00Z",
        false,
    )
    .await?;
    assert!(
        durably_restorable(pool, marked_id).await?,
        "a newer failed row of another artifact doesn't displace the marked one"
    );

    let unmarked = insert_sandbox(pool, None, "suspended").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET placement_changed_at = '2026-02-01T00:00:00Z' WHERE id = $1"
    ))
    .bind(unmarked)
    .execute(pool)
    .await?;
    let unmarked_id = SandboxId::from_uuid(unmarked);
    insert_full_snapshot(
        pool,
        unmarked,
        Uuid::now_v7(),
        "ready",
        "2026-01-15T00:00:00Z",
        false,
    )
    .await?;
    assert!(
        !durably_restorable(pool, unmarked_id).await?,
        "older than the placement"
    );
    insert_full_snapshot(
        pool,
        unmarked,
        Uuid::now_v7(),
        "ready",
        "2026-02-02T00:00:00Z",
        false,
    )
    .await?;
    assert!(
        durably_restorable(pool, unmarked_id).await?,
        "fresh after the placement"
    );
    insert_full_snapshot(
        pool,
        unmarked,
        Uuid::now_v7(),
        "failed",
        "2026-02-03T00:00:00Z",
        false,
    )
    .await?;
    assert!(
        !durably_restorable(pool, unmarked_id).await?,
        "a newer failed full row"
    );

    // An evacuation-marked row needs only its newest full snapshot ready; its
    // placement freshness doesn't apply.
    let evacuating = insert_sandbox(pool, None, "suspended").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET evacuation_restore_target = 'started',
                placement_changed_at = '2026-05-01T00:00:00Z'
          WHERE id = $1"
    ))
    .bind(evacuating)
    .execute(pool)
    .await?;
    insert_full_snapshot(
        pool,
        evacuating,
        Uuid::now_v7(),
        "ready",
        "2026-04-01T00:00:00Z",
        false,
    )
    .await?;
    assert!(
        durably_restorable(pool, SandboxId::from_uuid(evacuating)).await?,
        "an evacuation's newest ready snapshot"
    );

    // A restoring row needs a source in its retry window: a snapshot taken
    // before the restore started.
    let restoring = insert_sandbox(pool, None, "restoring").await?;
    let restoring_id = SandboxId::from_uuid(restoring);
    insert_full_snapshot(
        pool,
        restoring,
        Uuid::now_v7(),
        "ready",
        "2026-03-01T00:00:00Z",
        false,
    )
    .await?;
    assert!(
        durably_restorable(pool, restoring_id).await?,
        "a pre-restore source"
    );
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle_changed_at = '2026-01-01T00:00:00Z' WHERE id = $1"
    ))
    .bind(restoring)
    .execute(pool)
    .await?;
    assert!(
        !durably_restorable(pool, restoring_id).await?,
        "a snapshot from after the restore started isn't its source"
    );

    assert!(!durably_restorable(pool, SandboxId::new()).await?);
    Ok(())
}
