use pplx_space_common_core::types::{SandboxId, UserId};

use pplx_space_db_test_fixtures::try_pg_test_db;
use uuid::Uuid;

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const ISSUER: &str = "https://identity.example";
const TEST_GRANT_AUDIENCE: &str = "test-provider";

async fn seed_local_org(pool: &sqlx::PgPool) -> TestResult<OrgId> {
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

async fn seed_federated_org(pool: &sqlx::PgPool, upstream_org_id: &str) -> TestResult<OrgId> {
    let org = OrgId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug, owner_source, upstream_issuer, upstream_org_id) \
         VALUES ($1, $2, $2, 'federated_identity', $3, $4)"
    ))
    .bind(org.as_uuid())
    .bind(org.as_uuid().to_string())
    .bind(ISSUER)
    .bind(upstream_org_id)
    .execute(pool)
    .await?;
    Ok(org)
}

/// A federated sandbox with its owner, returning the sandbox id.
async fn seed_sandbox(
    pool: &sqlx::PgPool,
    org: OrgId,
    upstream_subject_id: &str,
) -> TestResult<SandboxId> {
    let user_id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role, \
                            owner_source, upstream_issuer, upstream_subject_id) \
         VALUES ($1, $2, 'x', $2, $3, 'member', 'federated_identity', $4, $5)"
    ))
    .bind(user_id)
    .bind(format!("{user_id}@space.invalid"))
    .bind(org.as_uuid())
    .bind(ISSUER)
    .bind(upstream_subject_id)
    .execute(pool)
    .await?;

    // Neither `id` nor `network_policy_generation` carries a default; the same
    // columns sandbox/org/owner_scope_tests.rs has to supply.
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation) \
         VALUES ($1, $2, $3, 1)"
    ))
    .bind(sandbox_id)
    .bind(user_id)
    .bind(org.as_uuid())
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

/// `expires_in_secs` is relative to now, so a negative value stores a lapsed
/// grant. The table's `grant_expires_at > updated_at` check permits it only
/// because `updated_at` is set here too.
async fn seed_grant(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    blob: &[u8],
    expires_in_secs: i64,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_federated_service_grants \
             (sandbox_id, grant_audience, grant_purpose, grant_blob, grant_expires_at, updated_at) \
         VALUES ($1, $2, $3, $4, now() + make_interval(secs => $5::double precision), \
                 now() + make_interval(secs => $5::double precision) - interval '1 second')"
    ))
    .bind(sandbox_id)
    .bind(TEST_GRANT_AUDIENCE)
    .bind(FEDERATED_OWNER_GRANT_PURPOSE)
    .bind(blob)
    .bind(expires_in_secs as f64)
    .execute(pool)
    .await?;
    Ok(())
}

/// A local org has no upstream id, and the branch key it already owns is scoped
/// by this uuid — sending anything else would strand it.
#[tokio::test]
async fn local_org_claims_the_space_uuid_and_no_grant() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org = seed_local_org(&db.pool).await?;

    let lookup = PgIntermediateKeyIdentityLookup::new(
        db.pool.clone(),
        FederatedGrantAudience::parse(TEST_GRANT_AUDIENCE)?,
    );
    let identity = lookup.identity(org).await?;

    assert_eq!(identity.wire_org_id, org.as_uuid().to_string());
    assert_eq!(identity.grant, None);

    let identity = lookup.identity_for_sandbox(org, SandboxId::new()).await?;
    assert_eq!(identity.wire_org_id, org.as_uuid().to_string());
    assert_eq!(identity.grant, None);
    Ok(())
}

/// The upstream id travels, not SPACE's own: the credential provider derives the key
/// scope from it and the grant is sealed for it.
#[tokio::test]
async fn federated_org_claims_the_upstream_id() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let upstream = Uuid::now_v7().to_string();
    let org = seed_federated_org(&db.pool, &upstream).await?;

    let identity = PgIntermediateKeyIdentityLookup::new(
        db.pool.clone(),
        FederatedGrantAudience::parse(TEST_GRANT_AUDIENCE)?,
    )
    .identity(org)
    .await?;

    assert_eq!(identity.wire_org_id, upstream);
    assert_ne!(identity.wire_org_id, org.as_uuid().to_string());
    assert_eq!(identity.grant, None, "no sandbox, so no grant to present");
    Ok(())
}

/// Two live grants: the later expiry wins, so a renewed grant is preferred over
/// one about to lapse mid-flight.
#[tokio::test]
async fn presents_the_freshest_live_grant_with_its_user() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let upstream = Uuid::now_v7().to_string();
    let org = seed_federated_org(&db.pool, &upstream).await?;
    let soon = seed_sandbox(&db.pool, org, "user-soon").await?;
    let later = seed_sandbox(&db.pool, org, "user-later").await?;
    seed_grant(&db.pool, soon, b"about-to-lapse", 120).await?;
    seed_grant(&db.pool, later, b"renewed", 3600).await?;
    // Pin a fractional expiry so this test proves both the epoch conversion
    // and the ceil policy used to agree with `grant_expires_at > now()`.
    const FIXED_EXPIRY_SECOND: i64 = 2_000_000_000;
    sqlx::query(sql!(
        "UPDATE sandbox_federated_service_grants
         SET grant_expires_at = to_timestamp($2::double precision) + interval '400 milliseconds'
         WHERE sandbox_id = $1"
    ))
    .bind(later)
    .bind(FIXED_EXPIRY_SECOND as f64)
    .execute(&db.pool)
    .await?;

    let identity = PgIntermediateKeyIdentityLookup::new(
        db.pool.clone(),
        FederatedGrantAudience::parse(TEST_GRANT_AUDIENCE)?,
    )
    .identity(org)
    .await?;

    assert_eq!(identity.wire_org_id, upstream);
    let presented = identity.grant.expect("the freshest live grant");
    assert_eq!(presented.user_id, "user-later");
    assert_eq!(presented.grant, "renewed");
    assert_eq!(
        presented.expires_at,
        jiff::Timestamp::from_second(FIXED_EXPIRY_SECOND + 1)?
    );
    Ok(())
}

/// A lapsed grant is not presented. The provider would refuse it, and sending
/// it would spend the call to learn what the expiry already says.
#[tokio::test]
async fn skips_expired_grants_but_still_claims_the_upstream_id() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let upstream = Uuid::now_v7().to_string();
    let org = seed_federated_org(&db.pool, &upstream).await?;
    let sandbox = seed_sandbox(&db.pool, org, "user-lapsed").await?;
    seed_grant(&db.pool, sandbox, b"lapsed", -60).await?;

    let identity = PgIntermediateKeyIdentityLookup::new(
        db.pool.clone(),
        FederatedGrantAudience::parse(TEST_GRANT_AUDIENCE)?,
    )
    .identity(org)
    .await?;

    assert_eq!(identity.wire_org_id, upstream);
    assert_eq!(identity.grant, None);
    Ok(())
}

/// Another org's live grant is not borrowed: the grant names an org and the
/// provider checks it against the one on the request.
#[tokio::test]
async fn does_not_present_another_orgs_grant() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let mine = seed_federated_org(&db.pool, &Uuid::now_v7().to_string()).await?;
    let theirs = seed_federated_org(&db.pool, &Uuid::now_v7().to_string()).await?;
    let their_sandbox = seed_sandbox(&db.pool, theirs, "their-user").await?;
    seed_grant(&db.pool, their_sandbox, b"theirs", 3600).await?;

    let identity = PgIntermediateKeyIdentityLookup::new(
        db.pool.clone(),
        FederatedGrantAudience::parse(TEST_GRANT_AUDIENCE)?,
    )
    .identity(mine)
    .await?;

    assert_eq!(identity.grant, None);
    Ok(())
}

/// A grant written under a different audience or purpose belongs to another consumer;
/// picking it up would present the credential provider a grant sealed for someone
/// else's use.
#[tokio::test]
async fn ignores_grants_for_another_audience_or_purpose() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let upstream = Uuid::now_v7().to_string();
    let org = seed_federated_org(&db.pool, &upstream).await?;
    let sandbox = seed_sandbox(&db.pool, org, "user-1").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_federated_service_grants \
             (sandbox_id, grant_audience, grant_purpose, grant_blob, grant_expires_at) \
         VALUES ($1, 'somewhere-else', $2, 'other', now() + interval '1 hour')"
    ))
    .bind(sandbox)
    .bind(FEDERATED_OWNER_GRANT_PURPOSE)
    .execute(&db.pool)
    .await?;

    let identity = PgIntermediateKeyIdentityLookup::new(
        db.pool.clone(),
        FederatedGrantAudience::parse(TEST_GRANT_AUDIENCE)?,
    )
    .identity(org)
    .await?;

    assert_eq!(identity.grant, None);
    Ok(())
}

/// An org row that is not there at all is the provider's failure to report, not
/// this lookup's: claim the uuid and let the RPC answer.
#[tokio::test]
async fn unknown_org_claims_the_space_uuid() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org = OrgId::new();

    let identity = PgIntermediateKeyIdentityLookup::new(
        db.pool.clone(),
        FederatedGrantAudience::parse(TEST_GRANT_AUDIENCE)?,
    )
    .identity(org)
    .await?;

    assert_eq!(identity.wire_org_id, org.as_uuid().to_string());
    assert_eq!(identity.grant, None);
    Ok(())
}

#[tokio::test]
async fn sandbox_lookup_prefers_its_own_grant_over_a_fresher_siblings() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let upstream = Uuid::now_v7().to_string();
    let org = seed_federated_org(&db.pool, &upstream).await?;
    let own = seed_sandbox(&db.pool, org, "own-user").await?;
    let sibling = seed_sandbox(&db.pool, org, "sibling-user").await?;
    seed_grant(&db.pool, own, b"own-grant", 120).await?;
    seed_grant(&db.pool, sibling, b"sibling-fresher", 3600).await?;

    let identity = PgIntermediateKeyIdentityLookup::new(
        db.pool.clone(),
        FederatedGrantAudience::parse(TEST_GRANT_AUDIENCE)?,
    )
    .identity_for_sandbox(org, own)
    .await?;

    assert_eq!(identity.wire_org_id, upstream);
    let grant = identity.grant.expect("requesting sandbox's grant");
    assert_eq!(grant.user_id, "own-user");
    assert_eq!(grant.grant, "own-grant");
    Ok(())
}

#[tokio::test]
async fn sandbox_lookup_falls_back_when_its_own_grant_lapsed() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org = seed_federated_org(&db.pool, &Uuid::now_v7().to_string()).await?;
    let own = seed_sandbox(&db.pool, org, "own-user").await?;
    let sibling = seed_sandbox(&db.pool, org, "sibling-user").await?;
    seed_grant(&db.pool, own, b"own-lapsed", -60).await?;
    seed_grant(&db.pool, sibling, b"sibling-live", 3600).await?;

    let identity = PgIntermediateKeyIdentityLookup::new(
        db.pool.clone(),
        FederatedGrantAudience::parse(TEST_GRANT_AUDIENCE)?,
    )
    .identity_for_sandbox(org, own)
    .await?;

    let grant = identity.grant.expect("org fallback grant");
    assert_eq!(grant.user_id, "sibling-user");
    assert_eq!(grant.grant, "sibling-live");
    Ok(())
}

#[tokio::test]
async fn sandbox_lookup_with_no_live_grant_presents_none() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let upstream = Uuid::now_v7().to_string();
    let org = seed_federated_org(&db.pool, &upstream).await?;
    let sandbox = seed_sandbox(&db.pool, org, "own-user").await?;
    seed_grant(&db.pool, sandbox, b"own-lapsed", -60).await?;

    let identity = PgIntermediateKeyIdentityLookup::new(
        db.pool.clone(),
        FederatedGrantAudience::parse(TEST_GRANT_AUDIENCE)?,
    )
    .identity_for_sandbox(org, sandbox)
    .await?;

    assert_eq!(identity.wire_org_id, upstream);
    assert_eq!(identity.grant, None);
    Ok(())
}
