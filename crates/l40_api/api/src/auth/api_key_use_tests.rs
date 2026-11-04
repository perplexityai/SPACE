//! What a successful API key lookup records.

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
use crate::auth::{API_KEY_LAST_USED_GRANULARITY, get_api_key_org, hash_token, record_api_key_use};
use crate::models::ApiKey;

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

/// A key revoked between its lookup and the recording of its use keeps its
/// `last_used_at`, so a revoked key never shows a use after its revocation.
#[tokio::test]
async fn a_key_revoked_after_its_lookup_keeps_its_last_used_at() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let id = seed_key(
        pool,
        OrgId::from_uuid(Uuid::parse_str(SEED_ORG_ID)?),
        None,
        "key_revoked_after_lookup",
    )
    .await?;
    // The key as its lookup read it: never used, so a use would be recorded.
    let looked_up = sqlx::query_as::<_, ApiKey>(sql!("SELECT * FROM api_keys WHERE id = $1"))
        .bind(id)
        .fetch_one(pool)
        .await?;
    // Revoked as the admin key store revokes, before the use is recorded.
    sqlx::query(sql!(
        "UPDATE api_keys
            SET revoked_at = NOW(),
                updated_at = NOW(),
                key_hash = 'revoked:' || id::text
          WHERE id = $1"
    ))
    .bind(id)
    .execute(pool)
    .await?;

    record_api_key_use(pool, &looked_up).await;
    assert_eq!(last_used_at(pool, id).await?, None);
    Ok(())
}
