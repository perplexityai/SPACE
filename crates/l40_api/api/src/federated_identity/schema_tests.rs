use pplx_space_common_core::types::{OrgId, SandboxId, UserId};

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{UpstreamIssuer, UpstreamOrgId, UpstreamSubjectId};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID as LOCAL_ORG_ID, SEED_USER_ID as LOCAL_USER_ID, pg_test_db,
};
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

use super::{
    DEFAULT_FEDERATED_GRANT_AUDIENCE, FEDERATED_OWNER_GRANT_PURPOSE, FederatedGrantStoreError,
    FederatedOwner, FederatedServiceGrant, upsert_sandbox_federated_service_grant,
    upsert_sandbox_federated_service_grant_tx,
};

use pplx_space_test_env::TestResult;

const UPSTREAM_ORG_ID: &str = "example-org-not-a-space-uuid";
const UPSTREAM_SUBJECT_ID: &str = "123";

async fn insert_federated_org(pool: &PgPool, upstream_org_id: &str) -> TestResult<OrgId> {
    let org_id = OrgId::new();
    sqlx::query(
        "INSERT INTO organizations (
             id, name, slug, owner_source, upstream_issuer, upstream_org_id
         )
         VALUES ($1, $2, $3, 'federated_identity', 'example-issuer', $4)",
    )
    .bind(org_id)
    .bind(format!("Federated Org {upstream_org_id}"))
    .bind(format!("federated-{org_id}"))
    .bind(upstream_org_id)
    .execute(pool)
    .await?;
    Ok(org_id)
}

async fn insert_federated_user(
    pool: &PgPool,
    org_id: OrgId,
    upstream_subject_id: &str,
) -> TestResult<UserId> {
    let user_id = UserId::new();
    sqlx::query(
        "INSERT INTO users (
             id, email, password_hash, display_name, org_id, role,
             owner_source, upstream_issuer, upstream_subject_id
         )
         VALUES (
             $1, $2, 'federated-user-no-local-password', $3, $4, 'member',
             'federated_identity', 'example-issuer', $5
         )",
    )
    .bind(user_id)
    .bind(format!(
        "federated-{org_id}-{upstream_subject_id}-{user_id}@space.invalid"
    ))
    .bind(format!("Federated User {upstream_subject_id}"))
    .bind(org_id)
    .bind(upstream_subject_id)
    .execute(pool)
    .await?;
    Ok(user_id)
}

async fn insert_federated_sandbox(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<(UserId, OrgId)> {
    let org_id = insert_federated_org(pool, UPSTREAM_ORG_ID).await?;
    let user_id = insert_federated_user(pool, org_id, UPSTREAM_SUBJECT_ID).await?;
    sqlx::query(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, $2, $3, 1)",
    )
    .bind(sandbox_id)
    .bind(user_id)
    .bind(org_id)
    .execute(pool)
    .await?;
    Ok((user_id, org_id))
}

fn federated_owner(subject_id: &str) -> TestResult<FederatedOwner> {
    Ok(FederatedOwner {
        issuer: UpstreamIssuer::parse("example-issuer")?,
        org_id: UpstreamOrgId::parse(UPSTREAM_ORG_ID)?,
        subject_id: UpstreamSubjectId::parse(subject_id)?,
    })
}

#[tokio::test]
async fn local_org_and_user_owner_identity_defaults_to_local() -> TestResult {
    let db = pg_test_db().await?;

    let (org_source, org_issuer, upstream_org_id): (String, Option<String>, Option<String>) =
        sqlx::query_as(
            "SELECT owner_source, upstream_issuer, upstream_org_id
             FROM organizations
             WHERE id = $1",
        )
        .bind(LOCAL_ORG_ID)
        .fetch_one(&db.pool)
        .await?;
    let (user_source, user_issuer, upstream_subject_id): (String, Option<String>, Option<String>) =
        sqlx::query_as(
            "SELECT owner_source, upstream_issuer, upstream_subject_id
         FROM users
         WHERE id = $1",
        )
        .bind(LOCAL_USER_ID)
        .fetch_one(&db.pool)
        .await?;

    assert_eq!(org_source, "local");
    assert_eq!(org_issuer, None);
    assert_eq!(upstream_org_id, None);
    assert_eq!(user_source, "local");
    assert_eq!(user_issuer, None);
    assert_eq!(upstream_subject_id, None);
    Ok(())
}

#[tokio::test]
async fn federated_org_and_user_persist_opaque_upstream_ids() -> TestResult {
    let db = pg_test_db().await?;

    let org_id = insert_federated_org(&db.pool, UPSTREAM_ORG_ID).await?;
    let user_id = insert_federated_user(&db.pool, org_id, UPSTREAM_SUBJECT_ID).await?;

    let (org_source, org_issuer, stored_upstream_org_id): (String, String, String) =
        sqlx::query_as(
            "SELECT owner_source, upstream_issuer, upstream_org_id
             FROM organizations
             WHERE id = $1",
        )
        .bind(org_id)
        .fetch_one(&db.pool)
        .await?;
    let (user_source, user_issuer, stored_upstream_subject_id): (String, String, String) =
        sqlx::query_as(
            "SELECT owner_source, upstream_issuer, upstream_subject_id
             FROM users
             WHERE id = $1",
        )
        .bind(user_id)
        .fetch_one(&db.pool)
        .await?;

    assert_eq!(org_source, "federated_identity");
    assert_eq!(org_issuer, "example-issuer");
    assert_eq!(stored_upstream_org_id, UPSTREAM_ORG_ID);
    assert_eq!(user_source, "federated_identity");
    assert_eq!(user_issuer, "example-issuer");
    assert_eq!(stored_upstream_subject_id, UPSTREAM_SUBJECT_ID);
    Ok(())
}

#[tokio::test]
async fn federated_org_and_user_can_own_normal_sandbox_row() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = insert_federated_org(&db.pool, UPSTREAM_ORG_ID).await?;
    let user_id = insert_federated_user(&db.pool, org_id, UPSTREAM_SUBJECT_ID).await?;
    let sandbox_id = SandboxId::new();

    sqlx::query(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, $2, $3, 1)",
    )
    .bind(sandbox_id)
    .bind(user_id)
    .bind(org_id)
    .execute(&db.pool)
    .await?;

    let (stored_user_id, stored_org_id): (UserId, OrgId) = sqlx::query_as(
        "SELECT user_id, org_id
         FROM sandboxes
         WHERE id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(stored_user_id, user_id);
    assert_eq!(stored_org_id, org_id);
    Ok(())
}

#[tokio::test]
async fn owner_identity_mapping_fields_are_immutable() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = insert_federated_org(&db.pool, UPSTREAM_ORG_ID).await?;
    let user_id = insert_federated_user(&db.pool, org_id, UPSTREAM_SUBJECT_ID).await?;

    let org_err = sqlx::query("UPDATE organizations SET upstream_org_id = 'other' WHERE id = $1")
        .bind(org_id)
        .execute(&db.pool)
        .await
        .unwrap_err();
    let user_err = sqlx::query("UPDATE users SET upstream_subject_id = '456' WHERE id = $1")
        .bind(user_id)
        .execute(&db.pool)
        .await
        .unwrap_err();

    assert!(
        org_err
            .to_string()
            .contains("organization owner identity fields are immutable")
    );
    assert!(
        user_err
            .to_string()
            .contains("user owner identity fields are immutable")
    );
    Ok(())
}

#[tokio::test]
async fn local_org_rejects_upstream_owner_mapping() -> TestResult {
    let db = pg_test_db().await?;

    let err = sqlx::query(
        "INSERT INTO organizations (id, name, slug, upstream_issuer, upstream_org_id)
         VALUES ($1, 'bad local org', $2, 'example-issuer', $3)",
    )
    .bind(Uuid::now_v7())
    .bind(format!("bad-local-org-{}", Uuid::now_v7()))
    .bind(UPSTREAM_ORG_ID)
    .execute(&db.pool)
    .await
    .unwrap_err();

    assert!(
        err.to_string()
            .contains("organizations_owner_identity_shape_check")
    );
    Ok(())
}

#[tokio::test]
async fn local_user_rejects_upstream_owner_mapping() -> TestResult {
    let db = pg_test_db().await?;

    let err = sqlx::query(
        "INSERT INTO users (
             id, email, password_hash, display_name, org_id, role,
             upstream_issuer, upstream_subject_id
         )
         VALUES (
             $1, $2, 'password', 'bad local user', $3, 'member',
             'example-issuer', $4
         )",
    )
    .bind(Uuid::now_v7())
    .bind(format!("bad-local-user-{}@space.invalid", Uuid::now_v7()))
    .bind(LOCAL_ORG_ID)
    .bind(UPSTREAM_SUBJECT_ID)
    .execute(&db.pool)
    .await
    .unwrap_err();

    assert!(err.to_string().contains("users_owner_identity_shape_check"));
    Ok(())
}

#[tokio::test]
async fn federated_org_rejects_empty_upstream_org() -> TestResult {
    let db = pg_test_db().await?;

    let err = sqlx::query(
        "INSERT INTO organizations (
             id, name, slug, owner_source, upstream_issuer, upstream_org_id
         )
         VALUES ($1, 'bad federated org', $2, 'federated_identity', 'example-issuer', '')",
    )
    .bind(Uuid::now_v7())
    .bind(format!("bad-federated-org-{}", Uuid::now_v7()))
    .execute(&db.pool)
    .await
    .unwrap_err();

    assert!(
        err.to_string()
            .contains("organizations_owner_identity_shape_check")
    );
    Ok(())
}

#[tokio::test]
async fn federated_user_rejects_empty_upstream_subject() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = insert_federated_org(&db.pool, UPSTREAM_ORG_ID).await?;

    let err = sqlx::query(
        "INSERT INTO users (
             id, email, password_hash, display_name, org_id, role,
             owner_source, upstream_issuer, upstream_subject_id
         )
         VALUES (
             $1, $2, 'password', 'bad federated user', $3, 'member',
             'federated_identity', 'example-issuer', ''
         )",
    )
    .bind(Uuid::now_v7())
    .bind(format!("bad-federated-user-{org_id}@space.invalid"))
    .bind(org_id)
    .execute(&db.pool)
    .await
    .unwrap_err();

    assert!(err.to_string().contains("users_owner_identity_shape_check"));
    Ok(())
}

#[tokio::test]
async fn federated_org_mapping_is_unique() -> TestResult {
    let db = pg_test_db().await?;
    insert_federated_org(&db.pool, UPSTREAM_ORG_ID).await?;

    let err = insert_federated_org(&db.pool, UPSTREAM_ORG_ID)
        .await
        .unwrap_err();

    assert!(
        err.to_string()
            .contains("idx_organizations_federated_owner")
    );
    Ok(())
}

#[tokio::test]
async fn federated_user_mapping_is_unique_per_org() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = insert_federated_org(&db.pool, UPSTREAM_ORG_ID).await?;
    insert_federated_user(&db.pool, org_id, UPSTREAM_SUBJECT_ID).await?;

    let err = insert_federated_user(&db.pool, org_id, UPSTREAM_SUBJECT_ID)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("idx_users_federated_subject"));
    Ok(())
}

#[tokio::test]
async fn sandbox_still_requires_existing_local_user() -> TestResult {
    let db = pg_test_db().await?;

    let err = sqlx::query(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, $2, $3, 1)",
    )
    .bind(Uuid::now_v7())
    .bind(Uuid::now_v7())
    .bind(LOCAL_ORG_ID)
    .execute(&db.pool)
    .await
    .unwrap_err();

    assert!(err.to_string().contains("sandboxes_user_id_fkey"));
    Ok(())
}

async fn insert_grant(
    pool: &PgPool,
    sandbox_id: SandboxId,
    audience: &str,
    purpose: &str,
    grant_blob: &[u8],
    expires_sql: &str,
) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
    let sql = format!(
        "INSERT INTO sandbox_federated_service_grants (
             sandbox_id, grant_audience, grant_purpose, grant_blob, grant_expires_at
         )
         VALUES ($1, $2, $3, $4, {expires_sql})"
    );
    sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
        .bind(sandbox_id)
        .bind(audience)
        .bind(purpose)
        .bind(grant_blob)
        .execute(pool)
        .await
}

#[tokio::test]
async fn federated_service_grant_persists_opaque_blob() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_federated_sandbox(&db.pool, sandbox_id).await?;

    let blob = b"opaque-federated-service-grant-bytes".to_vec();
    insert_grant(
        &db.pool,
        sandbox_id,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        "federated_owner",
        &blob,
        "NOW() + INTERVAL '90 seconds'",
    )
    .await?;

    let (audience, purpose, stored_blob): (String, String, Vec<u8>) = sqlx::query_as(
        "SELECT grant_audience, grant_purpose, grant_blob
         FROM sandbox_federated_service_grants
         WHERE sandbox_id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;

    assert_eq!(audience, DEFAULT_FEDERATED_GRANT_AUDIENCE);
    assert_eq!(purpose, "federated_owner");
    assert_eq!(stored_blob, blob);
    Ok(())
}

/// Whole seconds because `TIMESTAMPTZ` keeps microseconds and the assertions
/// below compare the stored expiry for equality; relative to now because the
/// `grant_expires_at > updated_at` check would eventually reject a hardcoded
/// instant.
fn expiry_in_days(days: i64) -> TestResult<OffsetDateTime> {
    Ok(OffsetDateTime::now_utc().replace_nanosecond(0)? + time::Duration::days(days))
}

async fn stored_grant(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<(Vec<u8>, OffsetDateTime, OffsetDateTime, i64)> {
    Ok(sqlx::query_as(
        "SELECT grant_blob, grant_expires_at, updated_at, COUNT(*) OVER ()
         FROM sandbox_federated_service_grants
         WHERE sandbox_id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// Renewal is monotone in the credential provider expiry: a delayed older renewal is
/// skipped atomically — same blob, same expiry, and (the load-bearing part) the same
/// `updated_at`, because a guarded DO UPDATE that matches nothing fires no trigger. The
/// skip reports success, never `OwnerMismatch`. Equal expiry also skips: the stored
/// grant is already at least as good.
#[tokio::test]
async fn federated_service_grant_upsert_skips_stale_renewals() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let owner = federated_owner(UPSTREAM_SUBJECT_ID)?;
    insert_federated_sandbox(&db.pool, sandbox_id).await?;

    let stored_expiry = expiry_in_days(6)?;
    upsert_sandbox_federated_service_grant(
        &db.pool,
        sandbox_id,
        &owner,
        &FederatedServiceGrant::new(b"fresh-grant".to_vec(), stored_expiry)?,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        FEDERATED_OWNER_GRANT_PURPOSE,
    )
    .await?;
    let (_, _, fresh_updated_at, _) = stored_grant(&db.pool, sandbox_id).await?;

    for (blob, expiry) in [
        (b"stale-grant".to_vec(), expiry_in_days(1)?),
        (b"equal-expiry-grant".to_vec(), stored_expiry),
    ] {
        upsert_sandbox_federated_service_grant(
            &db.pool,
            sandbox_id,
            &owner,
            &FederatedServiceGrant::new(blob, expiry)?,
            DEFAULT_FEDERATED_GRANT_AUDIENCE,
            FEDERATED_OWNER_GRANT_PURPOSE,
        )
        .await
        .expect("a skipped stale renewal is success, not OwnerMismatch");
    }

    let (stored_blob, stored_expires_at, updated_at, count) =
        stored_grant(&db.pool, sandbox_id).await?;
    assert_eq!(stored_blob, b"fresh-grant");
    assert_eq!(stored_expires_at, stored_expiry);
    assert_eq!(
        updated_at, fresh_updated_at,
        "a skipped renewal must not touch updated_at"
    );
    assert_eq!(count, 1);
    Ok(())
}

/// Producer order beats lifetime: when both sides carry issued-at, a newer
/// mint replaces the stored grant even with a SHORTER remaining life — Data
/// Engine alone chooses and may tighten TTLs, and the fresh credential is
/// the newer mint, not the longer-lived blob.
#[tokio::test]
async fn federated_service_grant_upsert_orders_by_issued_at_when_present() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let owner = federated_owner(UPSTREAM_SUBJECT_ID)?;
    insert_federated_sandbox(&db.pool, sandbox_id).await?;
    let now_unix = OffsetDateTime::now_utc().unix_timestamp();

    let long_lived = FederatedServiceGrant::new(b"old-long-grant".to_vec(), expiry_in_days(8)?)?
        .with_issued_at_unix(now_unix - 600)?;
    let newer_shorter_expiry = expiry_in_days(2)?;
    let newer_shorter =
        FederatedServiceGrant::new(b"new-short-grant".to_vec(), newer_shorter_expiry)?
            .with_issued_at_unix(now_unix)?;

    for grant in [&long_lived, &newer_shorter] {
        upsert_sandbox_federated_service_grant(
            &db.pool,
            sandbox_id,
            &owner,
            grant,
            DEFAULT_FEDERATED_GRANT_AUDIENCE,
            FEDERATED_OWNER_GRANT_PURPOSE,
        )
        .await?;
    }

    let (stored_blob, stored_expires_at, _, count) = stored_grant(&db.pool, sandbox_id).await?;
    assert_eq!(stored_blob, b"new-short-grant");
    assert_eq!(stored_expires_at, newer_shorter_expiry);
    assert_eq!(count, 1);

    // And the reverse: an older mint arriving late is skipped even though it
    // outlives the stored grant.
    upsert_sandbox_federated_service_grant(
        &db.pool,
        sandbox_id,
        &owner,
        &long_lived,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        FEDERATED_OWNER_GRANT_PURPOSE,
    )
    .await
    .expect("a skipped older mint is success while the stored grant is live");
    let (stored_blob, _, _, _) = stored_grant(&db.pool, sandbox_id).await?;
    assert_eq!(stored_blob, b"new-short-grant");

    Ok(())
}

/// The legacy transition is one-way, ascending: a stamped offer replaces an
/// unstamped stored row unconditionally — even with a SHORTER remaining life,
/// because the stamped side is a known-newer mint and the unstamped side
/// predates issued-at entirely.
#[tokio::test]
async fn federated_service_grant_stamped_offer_replaces_an_unstamped_row() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let owner = federated_owner(UPSTREAM_SUBJECT_ID)?;
    insert_federated_sandbox(&db.pool, sandbox_id).await?;

    upsert_sandbox_federated_service_grant(
        &db.pool,
        sandbox_id,
        &owner,
        &FederatedServiceGrant::new(b"legacy-unstamped".to_vec(), expiry_in_days(8)?)?,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        FEDERATED_OWNER_GRANT_PURPOSE,
    )
    .await?;

    let stamped_shorter_expiry = expiry_in_days(2)?;
    let stamped_shorter =
        FederatedServiceGrant::new(b"stamped-offer".to_vec(), stamped_shorter_expiry)?
            .with_issued_at_unix(OffsetDateTime::now_utc().unix_timestamp())?;
    upsert_sandbox_federated_service_grant(
        &db.pool,
        sandbox_id,
        &owner,
        &stamped_shorter,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        FEDERATED_OWNER_GRANT_PURPOSE,
    )
    .await?;

    let (stored_blob, stored_expires_at, _, count) = stored_grant(&db.pool, sandbox_id).await?;
    assert_eq!(stored_blob, b"stamped-offer");
    assert_eq!(stored_expires_at, stamped_shorter_expiry);
    assert_eq!(count, 1);
    Ok(())
}

/// And never descending: once a stamped grant is stored, an unstamped offer
/// is skipped even with a LONGER expiry — a delayed or replayed legacy
/// envelope must not roll the bearer blob back. The skip is atomic (no
/// trigger fires) and reports success while the stored grant is live.
#[tokio::test]
async fn federated_service_grant_unstamped_offer_never_displaces_a_stamped_row() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let owner = federated_owner(UPSTREAM_SUBJECT_ID)?;
    insert_federated_sandbox(&db.pool, sandbox_id).await?;

    let stamped_expiry = expiry_in_days(2)?;
    let stamped = FederatedServiceGrant::new(b"stamped-stored".to_vec(), stamped_expiry)?
        .with_issued_at_unix(OffsetDateTime::now_utc().unix_timestamp())?;
    upsert_sandbox_federated_service_grant(
        &db.pool,
        sandbox_id,
        &owner,
        &stamped,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        FEDERATED_OWNER_GRANT_PURPOSE,
    )
    .await?;
    let (_, _, stamped_updated_at, _) = stored_grant(&db.pool, sandbox_id).await?;

    upsert_sandbox_federated_service_grant(
        &db.pool,
        sandbox_id,
        &owner,
        &FederatedServiceGrant::new(b"unstamped-longer".to_vec(), expiry_in_days(8)?)?,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        FEDERATED_OWNER_GRANT_PURPOSE,
    )
    .await
    .expect("a skipped legacy envelope is success while the stored grant is live");

    let (stored_blob, stored_expires_at, updated_at, count) =
        stored_grant(&db.pool, sandbox_id).await?;
    assert_eq!(stored_blob, b"stamped-stored");
    assert_eq!(stored_expires_at, stamped_expiry);
    assert_eq!(
        updated_at, stamped_updated_at,
        "a skipped legacy envelope must not touch updated_at"
    );
    assert_eq!(count, 1);
    Ok(())
}

/// A skip only reports success while the stored grant is unexpired by the
/// database clock: skipping onto an expired stored grant surfaces as a
/// renewal failure, because "success" would leave the later lookup with
/// nothing usable.
#[tokio::test]
async fn federated_service_grant_skip_onto_expired_stored_grant_is_an_error() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let owner = federated_owner(UPSTREAM_SUBJECT_ID)?;
    insert_federated_sandbox(&db.pool, sandbox_id).await?;

    upsert_sandbox_federated_service_grant(
        &db.pool,
        sandbox_id,
        &owner,
        &FederatedServiceGrant::new(b"stored-grant".to_vec(), expiry_in_days(8)?)?,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        FEDERATED_OWNER_GRANT_PURPOSE,
    )
    .await?;
    // Age the stored row into expiry. The BEFORE UPDATE trigger would stamp
    // updated_at past the new expiry and trip the row's CHECK, so it is
    // disabled for this surgical backdating only.
    sqlx::query(sql!(
        "ALTER TABLE sandbox_federated_service_grants DISABLE TRIGGER USER"
    ))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_federated_service_grants
         SET grant_expires_at = NOW() - INTERVAL '1 hour',
             updated_at = NOW() - INTERVAL '2 hours'
         WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "ALTER TABLE sandbox_federated_service_grants ENABLE TRIGGER USER"
    ))
    .execute(&db.pool)
    .await?;

    // An offer even older than the expired stored grant: the guard skips, and
    // the skip must surface as a failure, not a success over unusable state.
    let older_offer = FederatedServiceGrant::new(
        b"older-offer".to_vec(),
        OffsetDateTime::now_utc() - time::Duration::hours(2),
    )?;
    let err = upsert_sandbox_federated_service_grant(
        &db.pool,
        sandbox_id,
        &owner,
        &older_offer,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        FEDERATED_OWNER_GRANT_PURPOSE,
    )
    .await
    .expect_err("skipping onto an expired stored grant must fail");
    assert!(matches!(
        err,
        FederatedGrantStoreError::StaleRenewalOfExpiredGrant
    ));

    Ok(())
}

/// An expired offer is never storable — the table's `expires_after_update`
/// CHECK fires on the candidate tuple before conflict arbitration — but it
/// must not disturb a valid stored grant or masquerade as a store failure:
/// with a usable grant in place the renewal is a skip-success.
#[tokio::test]
async fn federated_service_grant_expired_offer_skips_over_a_valid_grant() -> TestResult {
    let db = pg_test_db().await?;
    let owner = federated_owner(UPSTREAM_SUBJECT_ID)?;
    let sandbox_id = SandboxId::new();
    insert_federated_sandbox(&db.pool, sandbox_id).await?;
    upsert_sandbox_federated_service_grant(
        &db.pool,
        sandbox_id,
        &owner,
        &FederatedServiceGrant::new(b"valid-stored".to_vec(), expiry_in_days(8)?)?,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        FEDERATED_OWNER_GRANT_PURPOSE,
    )
    .await?;
    upsert_sandbox_federated_service_grant(
        &db.pool,
        sandbox_id,
        &owner,
        &FederatedServiceGrant::new(
            b"expired-offer".to_vec(),
            OffsetDateTime::now_utc() - time::Duration::hours(2),
        )?,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        FEDERATED_OWNER_GRANT_PURPOSE,
    )
    .await
    .expect("an expired offer onto a valid stored grant is a skip-success");
    let (stored_blob, _, _, _) = stored_grant(&db.pool, sandbox_id).await?;
    assert_eq!(stored_blob, b"valid-stored");

    Ok(())
}

/// An expired offer with no stored grant at all delivered nothing usable:
/// that is the stale-renewal failure, not a spurious owner mismatch (the
/// probe distinguishes an absent grant row from an absent owner).
#[tokio::test]
async fn federated_service_grant_expired_offer_with_nothing_stored_fails() -> TestResult {
    let db = pg_test_db().await?;
    let owner = federated_owner(UPSTREAM_SUBJECT_ID)?;
    let sandbox_id = SandboxId::new();
    insert_federated_sandbox(&db.pool, sandbox_id).await?;
    let err = upsert_sandbox_federated_service_grant(
        &db.pool,
        sandbox_id,
        &owner,
        &FederatedServiceGrant::new(
            b"expired-offer".to_vec(),
            OffsetDateTime::now_utc() - time::Duration::hours(2),
        )?,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        FEDERATED_OWNER_GRANT_PURPOSE,
    )
    .await
    .expect_err("an expired offer with nothing stored must fail");
    assert!(matches!(
        err,
        FederatedGrantStoreError::StaleRenewalOfExpiredGrant
    ));

    Ok(())
}

/// The create-path caller runs the upsert inside its own transaction: an
/// expired offer there trips the row CHECK, and the savepoint recovery must
/// leave that transaction usable — the documented stale-renewal error comes
/// back, and later work on the same transaction still commits (no 25P02).
#[tokio::test]
async fn federated_service_grant_tx_survives_an_expired_offer() -> TestResult {
    let db = pg_test_db().await?;
    let owner = federated_owner(UPSTREAM_SUBJECT_ID)?;
    let sandbox_id = SandboxId::new();
    insert_federated_sandbox(&db.pool, sandbox_id).await?;

    let mut tx = db.pool.begin().await?;
    let err = upsert_sandbox_federated_service_grant_tx(
        &mut tx,
        sandbox_id,
        &owner,
        &FederatedServiceGrant::new(
            b"expired-offer".to_vec(),
            OffsetDateTime::now_utc() - time::Duration::hours(2),
        )?,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        FEDERATED_OWNER_GRANT_PURPOSE,
    )
    .await
    .expect_err("an expired offer with nothing stored must fail in a transaction too");
    assert!(matches!(
        err,
        FederatedGrantStoreError::StaleRenewalOfExpiredGrant
    ));

    // The transaction is still live after the caught CHECK violation: a valid
    // renewal on the same transaction succeeds and commits.
    upsert_sandbox_federated_service_grant_tx(
        &mut tx,
        sandbox_id,
        &owner,
        &FederatedServiceGrant::new(b"valid-after-failure".to_vec(), expiry_in_days(8)?)?,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        FEDERATED_OWNER_GRANT_PURPOSE,
    )
    .await?;
    tx.commit().await?;

    let (stored_blob, _, _, count) = stored_grant(&db.pool, sandbox_id).await?;
    assert_eq!(stored_blob, b"valid-after-failure");
    assert_eq!(count, 1);
    Ok(())
}

/// SPACE stores the lifetime the credential provider minted the grant with, not one of
/// its own: the old write path hardcoded `NOW() + INTERVAL '24 hours'`, so a grant good
/// for eight days was silently filed as expiring tomorrow.
#[tokio::test]
async fn federated_service_grant_upsert_stores_the_provider_expiry() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let owner = federated_owner(UPSTREAM_SUBJECT_ID)?;
    insert_federated_sandbox(&db.pool, sandbox_id).await?;

    let expires_at = expiry_in_days(8)?;
    upsert_sandbox_federated_service_grant(
        &db.pool,
        sandbox_id,
        &owner,
        &FederatedServiceGrant::new(b"grant".to_vec(), expires_at)?,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        FEDERATED_OWNER_GRANT_PURPOSE,
    )
    .await?;

    let (_, stored_expires_at, _, _) = stored_grant(&db.pool, sandbox_id).await?;
    assert_eq!(stored_expires_at, expires_at);
    Ok(())
}

#[tokio::test]
async fn federated_service_grant_upsert_matches_request_owner_and_renews_grant() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let owner = federated_owner(UPSTREAM_SUBJECT_ID)?;
    insert_federated_sandbox(&db.pool, sandbox_id).await?;

    let first = FederatedServiceGrant::new(b"first-grant".to_vec(), expiry_in_days(2)?)?;
    let renewed_expires_at = expiry_in_days(8)?;
    let second = FederatedServiceGrant::new(b"second-grant".to_vec(), renewed_expires_at)?;

    upsert_sandbox_federated_service_grant(
        &db.pool,
        sandbox_id,
        &owner,
        &first,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        FEDERATED_OWNER_GRANT_PURPOSE,
    )
    .await?;
    let (_, _, first_updated_at, _) = stored_grant(&db.pool, sandbox_id).await?;
    upsert_sandbox_federated_service_grant(
        &db.pool,
        sandbox_id,
        &owner,
        &second,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        FEDERATED_OWNER_GRANT_PURPOSE,
    )
    .await?;

    let (stored_blob, stored_expires_at, updated_at, count) =
        stored_grant(&db.pool, sandbox_id).await?;

    assert_eq!(stored_blob, b"second-grant");
    assert_eq!(stored_expires_at, renewed_expires_at);
    // Advanced by the table's BEFORE UPDATE trigger, which is what keeps the
    // `grant_expires_at > updated_at` check meaningful across renewals.
    assert!(updated_at > first_updated_at);
    assert_eq!(count, 1);
    Ok(())
}

#[tokio::test]
async fn federated_service_grant_upsert_rejects_wrong_request_owner() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let wrong_owner = federated_owner("456")?;
    let grant = FederatedServiceGrant::new(b"grant".to_vec(), expiry_in_days(1)?)?;
    insert_federated_sandbox(&db.pool, sandbox_id).await?;

    let err = upsert_sandbox_federated_service_grant(
        &db.pool,
        sandbox_id,
        &wrong_owner,
        &grant,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        FEDERATED_OWNER_GRANT_PURPOSE,
    )
    .await
    .unwrap_err();

    assert!(matches!(err, FederatedGrantStoreError::OwnerMismatch));
    Ok(())
}

#[tokio::test]
async fn federated_service_grant_cascades_on_sandbox_delete() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_federated_sandbox(&db.pool, sandbox_id).await?;
    insert_grant(
        &db.pool,
        sandbox_id,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        "federated_owner",
        b"blob",
        "NOW() + INTERVAL '90 seconds'",
    )
    .await?;

    sqlx::query("DELETE FROM sandboxes WHERE id = $1")
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;

    let remaining: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sandbox_federated_service_grants WHERE sandbox_id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(remaining, 0);
    Ok(())
}

#[tokio::test]
async fn federated_service_grant_rejects_non_positive_lifetime() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_federated_sandbox(&db.pool, sandbox_id).await?;

    let err = insert_grant(
        &db.pool,
        sandbox_id,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        "federated_owner",
        b"blob",
        "NOW() - INTERVAL '1 second'",
    )
    .await
    .unwrap_err();

    assert!(
        err.to_string()
            .contains("sandbox_federated_service_grants_expires_after_update")
    );
    Ok(())
}

// Grant lifetime is chosen by the credential provider when it mints and travels with the
// grant, so the schema does not police it: a ceiling here would have to be
// migrated every time that number is tuned. This keeps the coverage that
// matters — a grant with a future expiry is storable — without asserting any
// particular ceiling, so it holds both before and after the cap is dropped.
#[tokio::test]
async fn federated_service_grant_accepts_future_expiry() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_federated_sandbox(&db.pool, sandbox_id).await?;

    insert_grant(
        &db.pool,
        sandbox_id,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        "federated_owner",
        b"blob",
        "NOW() + INTERVAL '23 hours'",
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn federated_service_grant_rejects_empty_blob() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_federated_sandbox(&db.pool, sandbox_id).await?;

    let err = insert_grant(
        &db.pool,
        sandbox_id,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        "federated_owner",
        b"",
        "NOW() + INTERVAL '90 seconds'",
    )
    .await
    .unwrap_err();

    assert!(
        err.to_string()
            .contains("sandbox_federated_service_grants_blob_size")
    );
    Ok(())
}

#[tokio::test]
async fn federated_service_grant_rejects_oversized_blob() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_federated_sandbox(&db.pool, sandbox_id).await?;

    let oversized = vec![0u8; 65537];
    let err = insert_grant(
        &db.pool,
        sandbox_id,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        "federated_owner",
        &oversized,
        "NOW() + INTERVAL '90 seconds'",
    )
    .await
    .unwrap_err();

    assert!(
        err.to_string()
            .contains("sandbox_federated_service_grants_blob_size")
    );
    Ok(())
}

#[tokio::test]
async fn federated_service_grant_rejects_empty_audience() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_federated_sandbox(&db.pool, sandbox_id).await?;

    let err = insert_grant(
        &db.pool,
        sandbox_id,
        "",
        "federated_owner",
        b"blob",
        "NOW() + INTERVAL '90 seconds'",
    )
    .await
    .unwrap_err();

    assert!(
        err.to_string()
            .contains("sandbox_federated_service_grants_audience_nonempty")
    );
    Ok(())
}

#[tokio::test]
async fn federated_service_grant_requires_an_existing_sandbox() -> TestResult {
    let db = pg_test_db().await?;

    // No sandbox row inserted: the FK must reject the orphan grant.
    let err = insert_grant(
        &db.pool,
        SandboxId::new(),
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        "federated_owner",
        b"blob",
        "NOW() + INTERVAL '90 seconds'",
    )
    .await
    .unwrap_err();

    assert!(
        err.to_string()
            .contains("sandbox_federated_service_grants_sandbox_id_fkey")
    );
    Ok(())
}
