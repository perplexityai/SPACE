//! A service credential authenticates only on the surfaces it is scoped to.

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use crate::api_surface::ApiSurface;
use crate::auth::{get_api_key_org, hash_token};

const SEED_ORG_ID: &str = "00000000-0000-0000-0000-000000000002";

async fn seed_key(pool: &PgPool, raw_key: &str, surfaces: &[&str]) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO api_keys (id, org_id, name, key_hash, allowed_surfaces)
         VALUES ($1, $2, $3, $4, $5)"
    ))
    .bind(Uuid::now_v7())
    .bind(Uuid::parse_str(SEED_ORG_ID)?)
    .bind(raw_key)
    .bind(hash_token(raw_key))
    .bind(surfaces)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn a_key_resolves_only_on_a_surface_it_is_scoped_to() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    seed_key(pool, "key_ecw_only", &["ecw"]).await?;
    seed_key(pool, "key_native_only", &["native"]).await?;
    seed_key(pool, "key_both", &["ecw", "native"]).await?;

    for (raw_key, surface, expected) in [
        ("key_ecw_only", ApiSurface::Ecw, true),
        ("key_ecw_only", ApiSurface::Native, false),
        ("key_native_only", ApiSurface::Ecw, false),
        ("key_native_only", ApiSurface::Native, true),
        ("key_both", ApiSurface::Ecw, true),
        ("key_both", ApiSurface::Native, true),
    ] {
        let resolved = get_api_key_org(pool, raw_key, surface).await?.is_some();
        assert_eq!(
            resolved, expected,
            "{raw_key} on {surface} should resolve={expected}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_key_written_without_surfaces_is_ecw_only() -> TestResult {
    // Every credential that predates the column was issued for ECW, so the
    // default must not widen anything on upgrade.
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let raw_key = "key_legacy";
    sqlx::query(sql!(
        "INSERT INTO api_keys (id, org_id, name, key_hash)
         VALUES ($1, $2, $3, $4)"
    ))
    .bind(Uuid::now_v7())
    .bind(Uuid::parse_str(SEED_ORG_ID)?)
    .bind(raw_key)
    .bind(hash_token(raw_key))
    .execute(pool)
    .await?;

    assert!(
        get_api_key_org(pool, raw_key, ApiSurface::Ecw)
            .await?
            .is_some()
    );
    assert!(
        get_api_key_org(pool, raw_key, ApiSurface::Native)
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn the_database_rejects_an_unknown_or_empty_surface() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;

    for surfaces in [vec!["admin"], vec![]] {
        let result = sqlx::query(sql!(
            "INSERT INTO api_keys (id, org_id, name, key_hash, allowed_surfaces)
             VALUES ($1, $2, $3, $4, $5)"
        ))
        .bind(Uuid::now_v7())
        .bind(Uuid::parse_str(SEED_ORG_ID)?)
        .bind("key_bad_surface")
        .bind(hash_token(&format!("key_bad_{surfaces:?}")))
        .bind(&surfaces)
        .execute(pool)
        .await;

        assert!(
            result.is_err(),
            "expected {surfaces:?} to violate the check"
        );
    }
    Ok(())
}
