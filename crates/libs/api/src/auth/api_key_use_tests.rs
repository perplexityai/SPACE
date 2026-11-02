//! What a successful API key lookup records and whom the key acts as.

use pplx_space_common_core::{
    sql,
    types::{OrgId, UserId},
};
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::api_surface::ApiSurface;
use crate::auth::{API_KEY_LAST_USED_GRANULARITY, get_api_key_org, hash_token};

const SEED_ORG_ID: &str = "00000000-0000-0000-0000-000000000002";

async fn seed_key(
    pool: &PgPool,
    org_id: OrgId,
    created_by: Option<UserId>,
    raw_key: &str,
) -> TestResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO api_keys (id, org_id, name, key_hash, created_by, allowed_surfaces)
         VALUES ($1, $2, $3, $4, $5, ARRAY['native'])"
    ))
    .bind(id)
    .bind(org_id)
    .bind(raw_key)
    .bind(hash_token(raw_key))
    .bind(created_by)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn last_used_at(pool: &PgPool, id: Uuid) -> TestResult<Option<OffsetDateTime>> {
    Ok(
        sqlx::query_scalar(sql!("SELECT last_used_at FROM api_keys WHERE id = $1"))
            .bind(id)
            .fetch_one(pool)
            .await?,
    )
}

async fn insert_user(pool: &PgPool, org_id: OrgId, created_at: &str) -> TestResult<UserId> {
    let id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role, created_at)
         VALUES ($1, $2, '!', 'api key use test', $3, 'member', $4::timestamptz)"
    ))
    .bind(id)
    .bind(format!("{id}@example.invalid"))
    .bind(org_id)
    .bind(created_at)
    .execute(pool)
    .await?;
    Ok(id)
}

#[tokio::test]
async fn a_use_records_last_used_at_once_per_window() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let raw_key = "key_last_used";
    let id = seed_key(
        pool,
        OrgId::from_uuid(Uuid::parse_str(SEED_ORG_ID)?),
        None,
        raw_key,
    )
    .await?;
    assert_eq!(last_used_at(pool, id).await?, None);

    get_api_key_org(pool, raw_key, ApiSurface::Native)
        .await?
        .expect("key resolves");
    let first = last_used_at(pool, id)
        .await?
        .expect("the first use is recorded");

    get_api_key_org(pool, raw_key, ApiSurface::Native)
        .await?
        .expect("key resolves");
    assert_eq!(
        last_used_at(pool, id).await?,
        Some(first),
        "a use inside the window writes nothing"
    );

    let stale = first - API_KEY_LAST_USED_GRANULARITY - time::Duration::seconds(1);
    sqlx::query(sql!("UPDATE api_keys SET last_used_at = $2 WHERE id = $1"))
        .bind(id)
        .bind(stale)
        .execute(pool)
        .await?;
    get_api_key_org(pool, raw_key, ApiSurface::Native)
        .await?
        .expect("key resolves");
    let advanced = last_used_at(pool, id).await?.expect("still recorded");
    assert!(advanced > stale, "a use after the window advances the time");

    assert!(
        get_api_key_org(pool, raw_key, ApiSurface::Ecw)
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn a_key_acts_as_its_creator_only_inside_the_keys_organization() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let operator_org = OrgId::from_uuid(Uuid::parse_str(SEED_ORG_ID)?);
    let operator = insert_user(pool, operator_org, "2026-01-01T00:00:00Z").await?;
    let tenant_org = OrgId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug) VALUES ($1, 'Tenant', $2)"
    ))
    .bind(tenant_org)
    .bind(format!("tenant-{tenant_org}"))
    .execute(pool)
    .await?;
    let oldest = insert_user(pool, tenant_org, "2026-01-01T00:00:00Z").await?;
    let newer = insert_user(pool, tenant_org, "2026-02-01T00:00:00Z").await?;

    for (raw_key, created_by, expected_actor) in [
        ("key_tenant_creator", Some(newer), newer),
        ("key_operator_creator", Some(operator), oldest),
        ("key_no_creator", None, oldest),
    ] {
        seed_key(pool, tenant_org, created_by, raw_key).await?;
        let context = get_api_key_org(pool, raw_key, ApiSurface::Native)
            .await?
            .expect("key resolves");
        assert_eq!(context.org.id, tenant_org, "{raw_key} keeps its org");
        let actor = context.actor.expect("key resolves an actor");
        assert_eq!(actor.id, expected_actor, "{raw_key} actor");
        assert_eq!(actor.org_id, tenant_org, "{raw_key} actor org");
    }
    Ok(())
}
