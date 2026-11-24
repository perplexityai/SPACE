//! The store's guarded writes and its organization scoping, against Postgres.

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, TestDbResult, pg_test_db};
use sqlx::PgPool;

use super::*;

async fn seed_org(pool: &PgPool) -> TestDbResult<OrgId> {
    let org = OrgId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug) VALUES ($1, $2, $2)"
    ))
    .bind(org.as_uuid())
    .bind(org.as_uuid().to_string())
    .execute(pool)
    .await?;
    Ok(org)
}

#[tokio::test]
async fn a_second_active_master_key_is_an_outcome_and_keys_stay_in_their_org() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let org = seed_org(&db.pool).await?;
    let other = seed_org(&db.pool).await?;

    let cmk_id = insert_active_master_key(
        &db.pool,
        org,
        KekProvider::InMemory,
        &MasterKeyIdentifier::parse("first")?,
    )
    .await?
    .expect("the first active key is inserted");
    assert_eq!(
        insert_active_master_key(
            &db.pool,
            org,
            KekProvider::InMemory,
            &MasterKeyIdentifier::parse("second")?
        )
        .await?,
        None
    );
    let active = active_master_key(&db.pool, org)
        .await?
        .expect("the first key is active");
    assert_eq!(
        (active.id, active.org_id, active.provider.as_str()),
        (cmk_id, org, "in-memory")
    );
    assert_eq!(
        (active.key_identifier.as_str(), active.retired_at),
        ("first", None)
    );
    assert!(org_master_key(&db.pool, cmk_id, other).await?.is_none());
    assert!(active_master_key(&db.pool, other).await?.is_none());

    sqlx::query(sql!(
        "UPDATE org_master_keys SET retired_at = NOW() WHERE id = $1"
    ))
    .bind(cmk_id)
    .execute(&db.pool)
    .await?;
    assert!(active_master_key(&db.pool, org).await?.is_none());
    assert_eq!(
        org_master_key(&db.pool, cmk_id, org).await?,
        Some(OrgMasterKey {
            provider: "in-memory".to_owned(),
            key_identifier: "first".to_owned(),
            retired: true,
        })
    );
    Ok(())
}

#[tokio::test]
async fn the_first_volume_wrap_stands() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let org = OrgId::from_uuid(SEED_ORG_ID);
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, $2, $3, 1)"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    let cmk_id = insert_active_master_key(
        &db.pool,
        org,
        KekProvider::InMemory,
        &MasterKeyIdentifier::parse("volume")?,
    )
    .await?
    .expect("the first active key is inserted");

    assert!(volume_wrap(&db.pool, sandbox_id).await?.is_none());
    assert!(insert_volume_wrap(&db.pool, sandbox_id, org, cmk_id, b"first").await?);
    assert!(!insert_volume_wrap(&db.pool, sandbox_id, org, cmk_id, b"second").await?);
    assert_eq!(
        volume_wrap(&db.pool, sandbox_id).await?,
        Some(VolumeWrapRecord {
            org_id: org,
            master_key_id: cmk_id,
            wrapped_dek: b"first".to_vec(),
        })
    );
    Ok(())
}
