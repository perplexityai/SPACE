use pplx_space_common_core::types::{NodeId, OrgId};

use sqlx::PgPool;
use uuid::Uuid;

use super::*;

use crate::routes::admin::test_fixtures::{TestResult, postgres_fixture};

/// Orgs seeded by the baseline migration's fixture data.
const ORG_A: Uuid = Uuid::from_u128(0x1);
const ORG_B: Uuid = Uuid::from_u128(0x3);
const SEEDED_USER: Uuid = Uuid::from_u128(0x10);

async fn insert_template(
    pool: &PgPool,
    name: &str,
    visibility: &str,
    org_id: Option<OrgId>,
) -> TestResult {
    sqlx::query("INSERT INTO templates (id, name, org_id, visibility) VALUES ($1, $2, $3, $4)")
        .bind(Uuid::now_v7())
        .bind(name)
        .bind(org_id)
        .bind(visibility)
        .execute(pool)
        .await?;
    Ok(())
}

async fn insert_sandbox(pool: &PgPool, template: &str, lifecycle: &str) -> TestResult {
    sqlx::query(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             template_id, network_policy_generation
         )
         VALUES ($1, $2, $3, $4, 'unplaced', 'k8s', $5, 1)",
    )
    .bind(Uuid::now_v7())
    .bind(SEEDED_USER)
    .bind(ORG_A)
    .bind(lifecycle)
    .bind(template)
    .execute(pool)
    .await?;
    Ok(())
}

/// The in-use guard counts live and restorable rows: `started`,
/// `suspended` (restorable), and `failed` rows that carry a durable,
/// repair-eligible suspend snapshot (the failed-restorable repair loop can
/// revert those to `suspended`). Rows that are genuinely gone —
/// `cleanup_required`, `deleted`, and a `failed` row with no restorable
/// snapshot — do not count. The `failed` rows seeded here have no snapshot,
/// so this case pins the exclusion of unrecoverable-failed; the positive
/// failed+snapshot path is covered by the shared
/// `failed_repair_revert_eligibility` predicate's own unit tests, seeding a
/// durable snapshot fixture at this layer being a heavier follow-up.
#[tokio::test(flavor = "current_thread")]
async fn in_use_counts_live_and_restorable_only() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    insert_template(pool, "flip-target", "generic", None).await?;
    insert_sandbox(pool, "flip-target", "started").await?;
    insert_sandbox(pool, "flip-target", "suspended").await?;
    // `failed` with no restorable snapshot → not recoverable → excluded.
    insert_sandbox(pool, "flip-target", "failed").await?;
    insert_sandbox(pool, "flip-target", "deleted").await?;

    let mut tx = pool.begin().await?;
    assert_eq!(
        template_sandboxes_in_use_in_tx(&mut tx, "flip-target").await?,
        2
    );
    assert_eq!(template_sandboxes_in_use_in_tx(&mut tx, "other").await?, 0);
    tx.commit().await?;
    Ok(())
}

/// The narrowing guard sees any cached rootfs row.
#[tokio::test(flavor = "current_thread")]
async fn cached_rootfs_counts_inventory_rows() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    insert_template(pool, "cached", "generic", None).await?;
    let node_id = NodeId::new();
    sqlx::query(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.1'::INET, 'worker', 'k8s', 'ready')",
    )
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query("INSERT INTO node_template_inventory (node_id, template_name) VALUES ($1, $2)")
        .bind(node_id)
        .bind("cached")
        .execute(pool)
        .await?;

    let mut tx = pool.begin().await?;
    assert_eq!(template_cached_rootfs_in_tx(&mut tx, "cached").await?, 1);
    assert_eq!(template_cached_rootfs_in_tx(&mut tx, "uncached").await?, 0);
    tx.commit().await?;
    Ok(())
}

/// The visibility write assigns an owner in the same statement when one
/// is supplied and otherwise leaves ownership untouched; the
/// `org_specific_requires_owner` CHECK backstops ownerless narrows.
#[tokio::test(flavor = "current_thread")]
async fn visibility_write_and_owner_check() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    insert_template(pool, "ownerless", "generic", None).await?;

    // Ownerless narrow without an owner in the same write → CHECK.
    let mut tx = pool.begin().await?;
    let err = update_template_visibility_in_tx(
        &mut tx,
        "ownerless",
        TemplateVisibility::OrgSpecific,
        None,
    )
    .await
    .unwrap_err();
    drop(tx);
    let constraint = match &err {
        sqlx::Error::Database(db) => db.constraint().map(str::to_string),
        _ => None,
    };
    assert_eq!(constraint.as_deref(), Some("org_specific_requires_owner"));

    // Narrow with the owner supplied in the same write → row carries
    // both the new visibility and the owner.
    let mut tx = pool.begin().await?;
    update_template_visibility_in_tx(
        &mut tx,
        "ownerless",
        TemplateVisibility::OrgSpecific,
        Some(OrgId::from_uuid(ORG_A)),
    )
    .await?;
    let row = lock_template_for_update(&mut tx, "ownerless")
        .await?
        .expect("row");
    tx.commit().await?;
    assert_eq!(row.visibility, TemplateVisibility::OrgSpecific);
    assert_eq!(row.org_id, Some(OrgId::from_uuid(ORG_A)));

    // Widening back leaves the owner in place (org_id is don't-care for
    // generic templates).
    let mut tx = pool.begin().await?;
    update_template_visibility_in_tx(&mut tx, "ownerless", TemplateVisibility::Generic, None)
        .await?;
    let row = lock_template_for_update(&mut tx, "ownerless")
        .await?
        .expect("row");
    tx.commit().await?;
    assert_eq!(row.visibility, TemplateVisibility::Generic);
    assert_eq!(row.org_id, Some(OrgId::from_uuid(ORG_A)));
    Ok(())
}

/// A narrowing flip prunes the template's allowlist rows from every
/// group not owned by the new owner, and keeps the owner's own rows.
#[tokio::test(flavor = "current_thread")]
async fn narrowing_prunes_cross_org_compat_rows() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    insert_template(pool, "narrowed", "generic", Some(OrgId::from_uuid(ORG_A))).await?;
    let group_a = Uuid::now_v7();
    let group_b = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO node_groups (id, name, org_id, template_policy)
         VALUES ($1, 'org-a-restricted', $2, 'restricted'),
                ($3, 'org-b-restricted', $4, 'restricted')",
    )
    .bind(group_a)
    .bind(ORG_A)
    .bind(group_b)
    .bind(ORG_B)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO node_group_template_compat (node_group_id, template_name)
         VALUES ($1, 'narrowed'), ($2, 'narrowed')",
    )
    .bind(group_a)
    .bind(group_b)
    .execute(pool)
    .await?;

    let mut tx = pool.begin().await?;
    let pruned = prune_cross_org_compat_in_tx(&mut tx, "narrowed", OrgId::from_uuid(ORG_A)).await?;
    tx.commit().await?;
    assert_eq!(pruned, 1, "only the other org's row is pruned");

    let remaining: Vec<Uuid> = sqlx::query_scalar(
        "SELECT node_group_id FROM node_group_template_compat WHERE template_name = $1",
    )
    .bind("narrowed")
    .fetch_all(pool)
    .await?;
    assert_eq!(remaining, vec![group_a], "the owner's row survives");
    Ok(())
}

/// Widening an org-specific template whose content is wrapped under its
/// owner's key to `generic` would make it launchable-but-undecryptable for
/// every other org — the RFC forbids it and defers re-keying. The guard
/// must report the per-template encryption sidecar so the handler refuses
/// the widen; a template without a sidecar is safe to widen.
#[tokio::test(flavor = "current_thread")]
async fn widen_guard_detects_owner_encryption() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    insert_template(
        pool,
        "plain-owned",
        "org_specific",
        Some(OrgId::from_uuid(ORG_A)),
    )
    .await?;
    insert_template(
        pool,
        "encrypted-owned",
        "org_specific",
        Some(OrgId::from_uuid(ORG_A)),
    )
    .await?;

    let key_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO org_master_keys (id, org_id, provider, key_identifier)
         VALUES ($1, $2, 'test', 'k')",
    )
    .bind(key_id)
    .bind(ORG_A)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO template_encrypted_fields
             (template_name, org_id, master_key_id, wrapped_dek, network_policy_json)
         VALUES ('encrypted-owned', $1, $2, '\\x00'::BYTEA, '\\x00'::BYTEA)",
    )
    .bind(ORG_A)
    .bind(key_id)
    .execute(pool)
    .await?;

    assert!(
        !template_has_owner_encryption(pool, "plain-owned").await?,
        "a template with no encryption sidecar is safe to widen"
    );
    assert!(
        template_has_owner_encryption(pool, "encrypted-owned").await?,
        "a per-template encryption sidecar must block the widen"
    );
    assert!(
        !template_has_owner_encryption(pool, "absent").await?,
        "an unknown template has no sidecar"
    );
    Ok(())
}
