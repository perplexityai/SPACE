//! End-to-end coverage for the PATCH update path: an `UpdateSandboxRequest`
//! through `SandboxController::update`, into the sandbox store's
//! `update_desired_state`, and back out through the api's read path.
//!
//! Scope note: only the `metadata` branch is reachable here. The resources
//! branch calls `control_runtime_client` and drives the guest before it
//! persists, so it needs a live runtime or a substrate mock — its column
//! behaviour is covered at the store verb instead. `auto_lifecycle` and
//! `network_policy` are untouched by this migration and keep their own SQL.

use pplx_space_common_core::types::SandboxId;
use pplx_space_common_core::types::{OrgId, UserId};

use super::*;
use crate::models::{SandboxMetadata, SandboxMetadataValue, User};
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_test_env::TestResult;

async fn insert_actor(pool: &PgPool) -> TestResult<User> {
    let org_id = OrgId::new();
    let user_id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug)
         VALUES ($1, 'Sandbox Update Test', $2)"
    ))
    .bind(org_id)
    .bind(format!("sandbox-update-{org_id}"))
    .execute(pool)
    .await?;
    Ok(sqlx::query_as::<_, User>(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'unused', 'Sandbox Update Test', $3, 'member')
         RETURNING id, email, password_hash, display_name, org_id, role, owner_source,
                   created_at, updated_at"
    ))
    .bind(user_id)
    .bind(format!("sandbox-update-{user_id}@example.com"))
    .bind(org_id)
    .fetch_one(pool)
    .await?)
}

/// Seed a sandbox and return it as the controller sees it.
async fn seed_sandbox(pool: &PgPool, actor: &User) -> TestResult<Sandbox> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
             (id, name, user_id, org_id, managed_by, lifecycle, placement,
              vcpu, memory_mib, network_policy_generation)
         VALUES ($1, 'update-path', $2, $3, 'k8s', 'started', 'unplaced', 2, 512, 1)"
    ))
    .bind(id)
    .bind(actor.id)
    .bind(actor.org_id)
    .execute(pool)
    .await?;
    Ok(fetch_sandbox(pool, id)
        .await?
        .expect("seeded sandbox must be readable"))
}

fn metadata_request(metadata: Option<SandboxMetadata>) -> UpdateSandboxRequest {
    UpdateSandboxRequest {
        metadata,
        vcpu: None,
        memory_mib: None,
        pids_max: None,
        auto_lifecycle: None,
        network_policy: None,
    }
}

// Direct is only a placeholder: the tests using this never reach the substrate.
fn direct_sandbox_controller(pool: &PgPool) -> SandboxController {
    SandboxController::new(crate::controllers::substrate::Substrate::Direct(Box::new(
        crate::controllers::substrate::direct::DirectSubstrate::new(pool.clone()),
    )))
}

/// The whole path: request in, row written, and the value visible through
/// the api's own read. Every metadata value variant is exercised, because
/// the document is an untagged enum and this is the layer where a number or
/// boolean would silently become a string.
#[tokio::test(flavor = "current_thread")]
async fn update_persists_metadata_and_reads_it_back() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let sandbox = seed_sandbox(&db.pool, &actor).await?;
    let controller = direct_sandbox_controller(&db.pool);

    let metadata: SandboxMetadata = [
        (
            "team".to_string(),
            SandboxMetadataValue::String("core".to_string()),
        ),
        ("retries".to_string(), SandboxMetadataValue::Number(3)),
        ("pinned".to_string(), SandboxMetadataValue::Bool(true)),
    ]
    .into_iter()
    .collect();

    let generation = controller
        .update(
            &db.pool,
            &sandbox,
            sandbox.id,
            metadata_request(Some(metadata.clone())),
            None,
        )
        .await?;
    assert!(
        generation.is_none(),
        "a metadata-only patch must not touch the network policy generation"
    );

    let reread = fetch_sandbox(&db.pool, sandbox.id)
        .await?
        .expect("sandbox still present");
    assert_eq!(reread.metadata.0, metadata);
    // The patch names metadata only; the seeded resources must not move.
    assert_eq!(reread.vcpu, Some(2));
    assert_eq!(reread.memory_mib, Some(512));
    Ok(())
}

/// An empty request writes nothing at all — no branch of `update` runs.
#[tokio::test(flavor = "current_thread")]
async fn update_with_no_fields_leaves_the_row_alone() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let sandbox = seed_sandbox(&db.pool, &actor).await?;
    let controller = direct_sandbox_controller(&db.pool);

    let before = fetch_sandbox(&db.pool, sandbox.id).await?.expect("present");
    controller
        .update(&db.pool, &sandbox, sandbox.id, metadata_request(None), None)
        .await?;

    let after = fetch_sandbox(&db.pool, sandbox.id).await?.expect("present");
    assert_eq!(after.metadata.0, before.metadata.0);
    assert_eq!(after.vcpu, before.vcpu);
    assert_eq!(after.memory_mib, before.memory_mib);
    assert_eq!(
        after.updated_at, before.updated_at,
        "a request with no fields must not stamp updated_at"
    );
    Ok(())
}

/// The `updated_at` behaviour change, asserted at the layer callers see.
/// The replaced inline SQL wrote `updated_at = NOW()`, which made
/// `sandboxes_set_updated_at` return early and stamp the row even when the
/// document was identical. Leaving the column to the trigger means an
/// idempotent PATCH no longer moves it, while a real edit still does.
#[tokio::test(flavor = "current_thread")]
async fn update_metadata_bumps_updated_at_only_on_a_real_change() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let sandbox = seed_sandbox(&db.pool, &actor).await?;
    let controller = direct_sandbox_controller(&db.pool);

    let metadata: SandboxMetadata = [(
        "team".to_string(),
        SandboxMetadataValue::String("core".to_string()),
    )]
    .into_iter()
    .collect();

    controller
        .update(
            &db.pool,
            &sandbox,
            sandbox.id,
            metadata_request(Some(metadata.clone())),
            None,
        )
        .await?;
    let first = fetch_sandbox(&db.pool, sandbox.id).await?.expect("present");

    // Same document again.
    controller
        .update(
            &db.pool,
            &sandbox,
            sandbox.id,
            metadata_request(Some(metadata)),
            None,
        )
        .await?;
    let repeated = fetch_sandbox(&db.pool, sandbox.id).await?.expect("present");
    assert_eq!(
        repeated.updated_at, first.updated_at,
        "an idempotent PATCH must not bump updated_at"
    );

    // A genuine edit.
    let changed: SandboxMetadata = [(
        "team".to_string(),
        SandboxMetadataValue::String("platform".to_string()),
    )]
    .into_iter()
    .collect();
    controller
        .update(
            &db.pool,
            &sandbox,
            sandbox.id,
            metadata_request(Some(changed)),
            None,
        )
        .await?;
    let edited = fetch_sandbox(&db.pool, sandbox.id).await?.expect("present");
    assert!(
        edited.updated_at > first.updated_at,
        "a real change must bump updated_at"
    );
    Ok(())
}

/// Metadata replaces wholesale rather than merging, and an empty document is
/// a value a caller can actually write.
#[tokio::test(flavor = "current_thread")]
async fn update_metadata_replaces_the_whole_document() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let sandbox = seed_sandbox(&db.pool, &actor).await?;
    let controller = direct_sandbox_controller(&db.pool);

    let seeded: SandboxMetadata = [
        (
            "team".to_string(),
            SandboxMetadataValue::String("core".to_string()),
        ),
        (
            "env".to_string(),
            SandboxMetadataValue::String("prod".to_string()),
        ),
    ]
    .into_iter()
    .collect();
    controller
        .update(
            &db.pool,
            &sandbox,
            sandbox.id,
            metadata_request(Some(seeded)),
            None,
        )
        .await?;

    let replacement: SandboxMetadata = [(
        "team".to_string(),
        SandboxMetadataValue::String("platform".to_string()),
    )]
    .into_iter()
    .collect();
    controller
        .update(
            &db.pool,
            &sandbox,
            sandbox.id,
            metadata_request(Some(replacement.clone())),
            None,
        )
        .await?;
    let after = fetch_sandbox(&db.pool, sandbox.id).await?.expect("present");
    assert_eq!(
        after.metadata.0, replacement,
        "the patch replaces the document; `env` must be gone, not merged"
    );

    controller
        .update(
            &db.pool,
            &sandbox,
            sandbox.id,
            metadata_request(Some(SandboxMetadata::new())),
            None,
        )
        .await?;
    let cleared = fetch_sandbox(&db.pool, sandbox.id).await?.expect("present");
    assert!(cleared.metadata.0.is_empty());
    Ok(())
}
