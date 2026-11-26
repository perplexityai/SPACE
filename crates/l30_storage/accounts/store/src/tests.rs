//! The accounts statements against Postgres: each row type decodes from its
//! explicit column list, `user_in_organization` answers what the admin
//! create's former `SELECT id ... WHERE id = $1 AND org_id = $2` did, the
//! locking reads hold their locks for the caller's transaction, and each
//! guarded write leaves a row its guard does not match unchanged.

use std::time::Duration;

use pplx_space_accounts::{ApiKeyId, OrgId, SandboxOwnerSource, UserId};
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, TestDbResult, pg_test_db};
use pplx_space_postgres_pool::DbErrorKind;
use pplx_space_sql_locator::sql;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

use super::*;

fn seed_org() -> OrgId {
    OrgId::from_uuid(SEED_ORG_ID)
}

fn seed_user() -> UserId {
    UserId::from_uuid(SEED_USER_ID)
}

async fn new_org(
    pool: &PgPool,
    slug: &str,
    parent_id: Option<OrgId>,
) -> TestDbResult<Organization> {
    Ok(organizations::create_organization(
        pool,
        NewOrganization {
            id: OrgId::new(),
            name: slug.to_owned(),
            slug: slug.to_owned(),
            parent_id: parent_id.map(|id| id.as_uuid()),
            theme_primary: None,
            theme_secondary: None,
            logo_url: None,
        },
    )
    .await?)
}

async fn new_user(pool: &PgPool, org_id: OrgId, email: &str) -> TestDbResult<User> {
    Ok(users::create_user(
        pool,
        NewUser {
            id: UserId::new(),
            email: email.to_owned(),
            password_hash: "!".to_owned(),
            display_name: email.to_owned(),
            org_id,
            role: UserRole::Member,
        },
    )
    .await?)
}

/// The statement `user_in_organization` replaced at the admin create.
async fn former_owner_lookup(
    pool: &PgPool,
    org_id: OrgId,
    user_id: UserId,
) -> TestDbResult<Option<UserId>> {
    Ok(
        sqlx::query_scalar(sql!("SELECT id FROM users WHERE id = $1 AND org_id = $2"))
            .bind(user_id)
            .bind(org_id.as_uuid())
            .fetch_optional(pool)
            .await?,
    )
}

#[tokio::test]
async fn user_in_organization_matches_the_former_owner_lookup() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let other = new_org(&db.pool, "accounts-other", None).await?;
    let outsider = new_user(&db.pool, other.id, "outsider@example.com").await?;
    for (org_id, user_id) in [
        (seed_org(), seed_user()),
        (seed_org(), outsider.id),
        (other.id, outsider.id),
        (other.id, seed_user()),
        (seed_org(), UserId::new()),
    ] {
        let former = former_owner_lookup(&db.pool, org_id, user_id).await?;
        let now = users::user_in_organization(&db.pool, org_id, user_id).await?;
        assert_eq!(now.then_some(user_id), former, "{org_id} / {user_id}");
    }
    Ok(())
}

#[tokio::test]
async fn organizations_round_trip_through_their_columns() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let parent = new_org(&db.pool, "accounts-parent", None).await?;
    let child = new_org(&db.pool, "accounts-child", Some(parent.id)).await?;

    let read = organizations::get_organization(&db.pool, child.id)
        .await?
        .expect("created org reads back");
    assert_eq!(read.parent_id, Some(parent.id.as_uuid()));
    assert!(
        organizations::get_organization(&db.pool, OrgId::new())
            .await?
            .is_none()
    );
    assert!(
        organizations::list_organizations(&db.pool)
            .await?
            .iter()
            .any(|org| org.id == seed_org())
    );
    assert!(
        organizations::organization_ids(&db.pool)
            .await?
            .contains(&child.id)
    );

    let subtree = organizations::organization_subtree_ids(&db.pool, parent.id).await?;
    assert_eq!(subtree.len(), 2);
    assert!(organizations::organization_subtree_contains(&db.pool, parent.id, child.id).await?);
    assert!(!organizations::organization_subtree_contains(&db.pool, child.id, parent.id).await?);
    assert!(organizations::organization_exists(&db.pool, child.id).await?);
    assert!(organizations::organization_has_local_owner(&db.pool, child.id).await?);
    assert_eq!(
        organizations::organization_byok_enabled(&db.pool, child.id).await?,
        Some(false)
    );
    assert_eq!(
        organizations::organization_byok_enabled(&db.pool, OrgId::new()).await?,
        None
    );

    let patched = organizations::update_organization(
        &db.pool,
        child.id,
        OrganizationPatch {
            name: Some("renamed".to_owned()),
            slug: None,
            theme_primary: Some(Some("#112233".to_owned())),
            theme_secondary: None,
            logo_url: Some(None),
            snapshot_base_object_storage_uri: None,
            byok_enabled: Some(true),
        },
    )
    .await?
    .expect("existing org updates");
    assert_eq!(patched.name, "renamed");
    assert_eq!(patched.slug, "accounts-child");
    assert_eq!(patched.theme_primary.as_deref(), Some("#112233"));
    assert!(patched.byok_enabled);

    assert!(organizations::delete_organization(&db.pool, child.id).await?);
    assert!(!organizations::delete_organization(&db.pool, child.id).await?);

    assert_eq!(
        organizations::federated_organization_id(&db.pool, "issuer", "tenant").await?,
        None
    );
    let mut ids = Vec::new();
    for slug in ["federated-a", "federated-b"] {
        ids.push(
            organizations::upsert_federated_organization(
                &db.pool,
                FederatedOrganization {
                    proposed_id: OrgId::new(),
                    name: slug,
                    slug,
                    upstream_issuer: "issuer",
                    upstream_org_id: "tenant",
                },
            )
            .await?,
        );
    }
    assert_eq!(
        ids[0], ids[1],
        "a second upsert finds the first organization"
    );
    assert_eq!(
        organizations::federated_organization_id(&db.pool, "issuer", "tenant").await?,
        Some(ids[0])
    );
    assert!(!organizations::organization_has_local_owner(&db.pool, ids[0]).await?);
    Ok(())
}

#[tokio::test]
async fn users_sessions_and_keys_round_trip_through_their_columns() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let org = new_org(&db.pool, "accounts-users", None).await?;
    let user = new_user(&db.pool, org.id, "member@example.com").await?;

    let by_id = users::get_user(&db.pool, user.id)
        .await?
        .expect("user reads back");
    assert_eq!(by_id.org_id, org.id);
    assert_eq!(
        users::user_by_email(&db.pool, "member@example.com")
            .await?
            .map(|user| user.id),
        Some(user.id)
    );
    assert!(
        users::organization_user(&db.pool, org.id, user.id)
            .await?
            .is_some()
    );
    assert!(
        users::organization_user(&db.pool, seed_org(), user.id)
            .await?
            .is_none()
    );
    assert_eq!(
        users::first_organization_user_id(&db.pool, org.id).await?,
        Some(user.id)
    );
    assert_eq!(
        users::list_organization_users(&db.pool, org.id)
            .await?
            .len(),
        1
    );
    let promoted = users::set_user_role(&db.pool, user.id, UserRole::OrganizationAdmin)
        .await?
        .expect("existing user updates");
    assert_eq!(promoted.role, UserRole::OrganizationAdmin);
    let renamed = users::update_user_profile(
        &db.pool,
        user.id,
        UserProfilePatch {
            email: None,
            password_hash: None,
            display_name: Some("Renamed".to_owned()),
        },
    )
    .await?
    .expect("existing user updates");
    assert_eq!(renamed.display_name, "Renamed");
    assert_eq!(renamed.email, "member@example.com");
    users::set_user_password_hash(&db.pool, user.id, "new-hash").await?;
    assert_eq!(
        users::get_user(&db.pool, user.id)
            .await?
            .map(|user| user.password_hash),
        Some("new-hash".to_owned())
    );

    sessions::create_session(
        &db.pool,
        NewSession {
            id: Uuid::now_v7(),
            user_id: user.id,
            token_hash: "accounts-session",
            ttl_seconds: 60,
        },
    )
    .await?;
    sessions::delete_session_by_token_hash(&db.pool, "accounts-session").await?;
    sessions::delete_user_sessions(&db.pool, user.id).await?;

    let key_id = ApiKeyId::new();
    let created = api_keys::create_api_key(
        &db.pool,
        CreateApiKey {
            id: key_id,
            org_id: org.id,
            name: "accounts key",
            key_hash: "accounts-key-hash",
            created_by: user.id,
            allowed_surfaces: &[ApiSurface::Ecw],
        },
    )
    .await?;
    assert_eq!(
        created.created_by_email.as_deref(),
        Some("member@example.com")
    );
    let auth = api_keys::api_key_auth_by_hash(&db.pool, "accounts-key-hash", ApiSurface::Ecw)
        .await?
        .expect("key authenticates on its surface");
    assert_eq!(auth.org.id, org.id);
    assert_eq!(auth.actor.map(|actor| actor.id), Some(user.id));
    assert!(
        api_keys::api_key_auth_by_hash(&db.pool, "accounts-key-hash", ApiSurface::Native)
            .await?
            .is_none()
    );
    assert!(api_keys::ecw_api_key_authorizes_org(&db.pool, "accounts-key-hash", org.id).await?);
    assert_eq!(
        api_keys::list_organization_api_keys(&db.pool, org.id, false)
            .await?
            .len(),
        1
    );

    let cluster = clusters::get_cluster(&db.pool)
        .await?
        .expect("seeded cluster");
    assert_eq!(cluster.managing_org_id, seed_org());
    assert_eq!(
        clusters::cluster_managing_org_id(&db.pool).await?,
        seed_org()
    );
    let moved = clusters::set_cluster_managing_org(&db.pool, org.id)
        .await?
        .expect("seeded cluster updates");
    assert_eq!(moved.managing_org_id, org.id);
    Ok(())
}

const ISSUER: &str = "https://identity.example";
const UPSTREAM_ORG: &str = "upstream-org";
const UPSTREAM_SUBJECT: &str = "upstream-subject";

/// A federated organization and a user in it whose columns each hold a value
/// no other column of the user and no default shares, so a read that takes a
/// field from the wrong column (another table's, or a swapped alias) decodes a
/// different user, and one that drops a column fails to decode. The
/// organization's columns are distinct and non-default in the same way, for
/// the joined reads.
async fn distinct_federated_owner(pool: &PgPool) -> TestDbResult<(User, Organization)> {
    let parent = new_org(pool, "distinct-parent", None).await?;
    let org_id = organizations::upsert_federated_organization(
        pool,
        FederatedOrganization {
            proposed_id: OrgId::new(),
            name: "Distinct Org Name",
            slug: "distinct-org-slug",
            upstream_issuer: ISSUER,
            upstream_org_id: UPSTREAM_ORG,
        },
    )
    .await?;
    let org = Organization {
        id: org_id,
        name: "Distinct Org Name".to_owned(),
        slug: "distinct-org-slug".to_owned(),
        parent_id: Some(parent.id.as_uuid()),
        theme_primary: Some("#123456".to_owned()),
        theme_secondary: Some("#654321".to_owned()),
        logo_url: Some("https://logo.example/distinct.png".to_owned()),
        byok_enabled: true,
        snapshot_base_object_storage_uri: Some("s3://distinct-bucket/prefix".to_owned()),
        created_at: OffsetDateTime::from_unix_timestamp(1_400_000_000)?,
        updated_at: OffsetDateTime::from_unix_timestamp(1_500_000_000)?,
    };
    sqlx::query(sql!(
        "UPDATE organizations
         SET parent_id = $2, theme_primary = $3, theme_secondary = $4, logo_url = $5,
             byok_enabled = $6, snapshot_base_object_storage_uri = $7,
             created_at = $8, updated_at = $9
         WHERE id = $1"
    ))
    .bind(org.id)
    .bind(org.parent_id)
    .bind(&org.theme_primary)
    .bind(&org.theme_secondary)
    .bind(&org.logo_url)
    .bind(org.byok_enabled)
    .bind(&org.snapshot_base_object_storage_uri)
    .bind(org.created_at)
    .bind(org.updated_at)
    .execute(pool)
    .await?;
    let user = User {
        id: UserId::new(),
        email: "distinct@example.com".to_owned(),
        password_hash: "distinct-password-hash".to_owned(),
        display_name: "Distinct Name".to_owned(),
        org_id,
        role: UserRole::OrganizationAdmin,
        owner_source: SandboxOwnerSource::FederatedIdentity,
        created_at: OffsetDateTime::from_unix_timestamp(1_600_000_000)?,
        updated_at: OffsetDateTime::from_unix_timestamp(1_700_000_000)?,
    };
    sqlx::query(sql!(
        "INSERT INTO users (
             id, email, password_hash, display_name, org_id, role, owner_source,
             upstream_issuer, upstream_subject_id, created_at, updated_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
    ))
    .bind(user.id)
    .bind(&user.email)
    .bind(&user.password_hash)
    .bind(&user.display_name)
    .bind(user.org_id)
    .bind(user.role)
    .bind(user.owner_source)
    .bind(ISSUER)
    .bind(UPSTREAM_SUBJECT)
    .bind(user.created_at)
    .bind(user.updated_at)
    .execute(pool)
    .await?;
    Ok((user, org))
}

async fn distinct_federated_user(pool: &PgPool) -> TestDbResult<User> {
    Ok(distinct_federated_owner(pool).await?.0)
}

fn assert_same_organization(read: &Organization, expected: &Organization) {
    assert_eq!(read.id, expected.id);
    assert_eq!(read.name, expected.name);
    assert_eq!(read.slug, expected.slug);
    assert_eq!(read.parent_id, expected.parent_id);
    assert_eq!(read.theme_primary, expected.theme_primary);
    assert_eq!(read.theme_secondary, expected.theme_secondary);
    assert_eq!(read.logo_url, expected.logo_url);
    assert_eq!(read.byok_enabled, expected.byok_enabled);
    assert_eq!(
        read.snapshot_base_object_storage_uri,
        expected.snapshot_base_object_storage_uri
    );
    assert_eq!(read.created_at, expected.created_at);
    assert_eq!(read.updated_at, expected.updated_at);
}

fn assert_same_user(read: &User, expected: &User) {
    assert_eq!(read.id, expected.id);
    assert_eq!(read.email, expected.email);
    assert_eq!(read.password_hash, expected.password_hash);
    assert_eq!(read.display_name, expected.display_name);
    assert_eq!(read.org_id, expected.org_id);
    assert_eq!(read.role, expected.role);
    assert_eq!(read.owner_source, expected.owner_source);
    assert_eq!(read.created_at, expected.created_at);
    assert_eq!(read.updated_at, expected.updated_at);
}

async fn insert_session(
    pool: &PgPool,
    user_id: UserId,
    token_hash: &str,
    expires_in_seconds: i64,
) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO sessions (id, user_id, token_hash, expires_at)
         VALUES ($1, $2, $3, NOW() + ($4::bigint * INTERVAL '1 second'))"
    ))
    .bind(Uuid::now_v7())
    .bind(user_id)
    .bind(token_hash)
    .bind(expires_in_seconds)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn session_user_decodes_every_column_while_the_session_is_live() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let user = distinct_federated_user(&db.pool).await?;
    insert_session(&db.pool, user.id, "live-session", 60).await?;
    insert_session(&db.pool, user.id, "expired-session", -60).await?;

    let read = users::user_by_session_token_hash(&db.pool, "live-session")
        .await?
        .expect("a live session finds its user");
    assert_same_user(&read, &user);
    assert!(
        users::user_by_session_token_hash(&db.pool, "expired-session")
            .await?
            .is_none(),
        "an expired session finds no user"
    );
    assert!(
        users::user_by_session_token_hash(&db.pool, "no-such-session")
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn share_locked_user_decodes_every_column_and_holds_its_lock() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let user = distinct_federated_user(&db.pool).await?;

    let mut tx = db.pool.begin().await?;
    let read = users::share_lock_user_by_email(&mut *tx, &user.email)
        .await?
        .expect("the user is found by email");
    assert_same_user(&read, &user);
    assert!(
        users::share_lock_user_by_email(&mut *tx, "nobody@example.com")
            .await?
            .is_none()
    );

    let mut other = db.pool.begin().await?;
    let conflict = sqlx::query(sql!("SELECT id FROM users WHERE id = $1 FOR UPDATE NOWAIT"))
        .bind(user.id)
        .execute(&mut *other)
        .await
        .expect_err("the share lock outlasts the statement");
    assert_eq!(
        conflict
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("55P03")
    );
    other.rollback().await?;
    // A password-only change's `UPDATE` takes `FOR NO KEY UPDATE`, which
    // `FOR SHARE` refuses and `FOR KEY SHARE` would admit.
    let mut writer = db.pool.begin().await?;
    let conflict = sqlx::query(sql!(
        "SELECT id FROM users WHERE id = $1 FOR NO KEY UPDATE NOWAIT"
    ))
    .bind(user.id)
    .execute(&mut *writer)
    .await
    .expect_err("the share lock holds off a plain update");
    assert_eq!(
        conflict
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("55P03")
    );
    writer.rollback().await?;
    tx.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn joined_user_and_organization_reads_decode_both_rows() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let (user, org) = distinct_federated_owner(&db.pool).await?;

    let found = users::user_with_organization(&db.pool, user.id).await?;
    assert_same_user(&found.user, &user);
    assert_same_organization(&found.org, &org);
    let missing = users::user_with_organization(&db.pool, UserId::new())
        .await
        .expect_err("an unknown user has no row");
    assert_eq!(missing.kind(), DbErrorKind::NotFound);

    let federated = organizations::federated_organization_user(
        &db.pool,
        ISSUER,
        UPSTREAM_ORG,
        UPSTREAM_SUBJECT,
    )
    .await?
    .expect("the federated owner is materialized");
    assert_same_user(&federated.user, &user);
    assert_same_organization(&federated.org, &org);
    // An organization of the same issuer whose users do not include the
    // subject, which is a user of another organization.
    organizations::upsert_federated_organization(
        &db.pool,
        FederatedOrganization {
            proposed_id: OrgId::new(),
            name: "subjectless",
            slug: "subjectless",
            upstream_issuer: ISSUER,
            upstream_org_id: "subjectless-upstream-org",
        },
    )
    .await?;
    for (issuer, upstream_org, subject) in [
        ("https://other.example", UPSTREAM_ORG, UPSTREAM_SUBJECT),
        (ISSUER, "other-upstream-org", UPSTREAM_SUBJECT),
        (ISSUER, "subjectless-upstream-org", UPSTREAM_SUBJECT),
        (ISSUER, UPSTREAM_ORG, "other-subject"),
    ] {
        assert!(
            organizations::federated_organization_user(&db.pool, issuer, upstream_org, subject)
                .await?
                .is_none(),
            "{issuer} / {upstream_org} / {subject}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn organization_owner_and_branding_reads() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let user = distinct_federated_user(&db.pool).await?;

    let federated = organizations::organization_owner(&db.pool, user.org_id)
        .await?
        .expect("the organization exists");
    assert_eq!(federated.owner_source, "federated_identity");
    assert_eq!(federated.upstream_org_id.as_deref(), Some(UPSTREAM_ORG));
    let local = organizations::organization_owner(&db.pool, seed_org())
        .await?
        .expect("the seeded organization exists");
    assert_eq!(local.owner_source, "local");
    assert_eq!(local.upstream_org_id, None);
    assert!(
        organizations::organization_owner(&db.pool, OrgId::new())
            .await?
            .is_none()
    );

    let branded = organizations::create_organization(
        &db.pool,
        NewOrganization {
            id: OrgId::new(),
            name: "branded".to_owned(),
            slug: "branded".to_owned(),
            parent_id: None,
            theme_primary: Some("#111111".to_owned()),
            theme_secondary: Some("#222222".to_owned()),
            logo_url: Some("https://logo.example/branded.png".to_owned()),
        },
    )
    .await?;
    let mut tx = db.pool.begin().await?;
    let branding = organizations::lock_organization_branding(&mut *tx, branded.id)
        .await?
        .expect("the organization exists");
    assert_eq!(branding.theme_primary.as_deref(), Some("#111111"));
    assert_eq!(branding.theme_secondary.as_deref(), Some("#222222"));
    assert_eq!(
        branding.logo_url.as_deref(),
        Some("https://logo.example/branded.png")
    );
    assert!(
        organizations::lock_organization_branding(&mut *tx, OrgId::new())
            .await?
            .is_none()
    );
    let mut other = db.pool.begin().await?;
    let conflict = sqlx::query(sql!(
        "SELECT id FROM organizations WHERE id = $1 FOR SHARE NOWAIT"
    ))
    .bind(branded.id)
    .execute(&mut *other)
    .await
    .expect_err("the branding lock outlasts the statement");
    assert_eq!(
        conflict
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("55P03")
    );
    other.rollback().await?;
    // Only `FOR UPDATE` refuses `FOR KEY SHARE`.
    let mut sharer = db.pool.begin().await?;
    let conflict = sqlx::query(sql!(
        "SELECT id FROM organizations WHERE id = $1 FOR KEY SHARE NOWAIT"
    ))
    .bind(branded.id)
    .execute(&mut *sharer)
    .await
    .expect_err("the branding lock is FOR UPDATE");
    assert_eq!(
        conflict
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("55P03")
    );
    sharer.rollback().await?;
    tx.rollback().await?;
    Ok(())
}

async fn insert_key(pool: &PgPool, org_id: OrgId, key_hash: &str) -> TestDbResult<ApiKeyId> {
    let id = ApiKeyId::new();
    sqlx::query(sql!(
        "INSERT INTO api_keys (id, org_id, name, key_hash, created_by)
         VALUES ($1, $2, $3, $3, NULL)"
    ))
    .bind(id)
    .bind(org_id)
    .bind(key_hash)
    .execute(pool)
    .await?;
    Ok(id)
}

#[derive(Debug, PartialEq, sqlx::FromRow)]
struct KeyState {
    key_hash: String,
    last_used_at: Option<OffsetDateTime>,
    revoked_at: Option<OffsetDateTime>,
    updated_at: OffsetDateTime,
}

async fn key_state(pool: &PgPool, id: ApiKeyId) -> TestDbResult<KeyState> {
    Ok(sqlx::query_as::<_, KeyState>(sql!(
        "SELECT key_hash, last_used_at, revoked_at, updated_at FROM api_keys WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

const GRANULARITY: Duration = Duration::from_secs(300);

#[tokio::test]
async fn advancing_last_used_at_writes_only_an_unrevoked_stale_key() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let key = insert_key(&db.pool, seed_org(), "advance-key").await?;

    assert!(
        api_keys::advance_api_key_last_used_at(&db.pool, key.as_uuid(), GRANULARITY).await?,
        "a never-used key records its use"
    );
    let first = key_state(&db.pool, key).await?;
    assert!(first.last_used_at.is_some());

    // Losing case: a use inside the window leaves the key as it is.
    assert!(!api_keys::advance_api_key_last_used_at(&db.pool, key.as_uuid(), GRANULARITY).await?);
    assert_eq!(key_state(&db.pool, key).await?, first);

    // A use after the window advances it again.
    sqlx::query(sql!(
        "UPDATE api_keys SET last_used_at = NOW() - INTERVAL '1 hour' WHERE id = $1"
    ))
    .bind(key)
    .execute(&db.pool)
    .await?;
    let stale = key_state(&db.pool, key).await?.last_used_at;
    assert!(api_keys::advance_api_key_last_used_at(&db.pool, key.as_uuid(), GRANULARITY).await?);
    assert!(key_state(&db.pool, key).await?.last_used_at > stale);

    // Losing case: a revoked key, never used, keeps `last_used_at` unset.
    let revoked = insert_key(&db.pool, seed_org(), "advance-revoked-key").await?;
    assert_eq!(
        api_keys::revoke_organization_api_key(&db.pool, seed_org(), revoked).await?,
        ApiKeyRevocation::Revoked
    );
    let before = key_state(&db.pool, revoked).await?;
    assert!(
        !api_keys::advance_api_key_last_used_at(&db.pool, revoked.as_uuid(), GRANULARITY).await?
    );
    assert_eq!(key_state(&db.pool, revoked).await?, before);
    assert_eq!(before.last_used_at, None);

    assert!(!api_keys::advance_api_key_last_used_at(&db.pool, Uuid::now_v7(), GRANULARITY).await?);
    Ok(())
}

#[tokio::test]
async fn revoking_a_key_leaves_revoked_and_other_organizations_keys_unchanged() -> TestDbResult<()>
{
    let db = pg_test_db().await?;
    let other_org = new_org(&db.pool, "revoke-other", None).await?;
    let key = insert_key(&db.pool, seed_org(), "revoke-key").await?;
    let other_key = insert_key(&db.pool, other_org.id, "revoke-other-key").await?;

    // Losing case: another organization's key is not found and unchanged.
    let other_before = key_state(&db.pool, other_key).await?;
    assert_eq!(
        api_keys::revoke_organization_api_key(&db.pool, seed_org(), other_key).await?,
        ApiKeyRevocation::NotFound
    );
    assert_eq!(key_state(&db.pool, other_key).await?, other_before);

    assert_eq!(
        api_keys::revoke_organization_api_key(&db.pool, seed_org(), key).await?,
        ApiKeyRevocation::Revoked
    );
    let revoked = key_state(&db.pool, key).await?;
    assert_eq!(revoked.key_hash, format!("revoked:{key}"));
    assert!(revoked.revoked_at.is_some());

    // Losing case: an already revoked key keeps its first revocation.
    assert_eq!(
        api_keys::revoke_organization_api_key(&db.pool, seed_org(), key).await?,
        ApiKeyRevocation::AlreadyRevoked
    );
    assert_eq!(key_state(&db.pool, key).await?, revoked);

    assert_eq!(
        api_keys::revoke_organization_api_key(&db.pool, seed_org(), ApiKeyId::new()).await?,
        ApiKeyRevocation::NotFound
    );
    Ok(())
}

#[tokio::test]
async fn create_list_and_org_scoped_revoke_preserve_key_metadata() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let (org_id, user_id) = (seed_org(), seed_user());
    let key_id = ApiKeyId::new();
    let key_hash = "store-test-key-hash";

    let created = api_keys::create_api_key(
        &db.pool,
        CreateApiKey {
            id: key_id,
            org_id,
            name: "store test",
            key_hash,
            created_by: user_id,
            allowed_surfaces: &[ApiSurface::Ecw, ApiSurface::Native],
        },
    )
    .await?;
    assert_eq!(created.id, key_id);
    assert_eq!(created.created_by, Some(user_id));
    assert_eq!(
        created.allowed_surfaces,
        vec![ApiSurface::Ecw, ApiSurface::Native]
    );
    for surface in [ApiSurface::Ecw, ApiSurface::Native] {
        assert!(
            api_keys::api_key_auth_by_hash(&db.pool, key_hash, surface)
                .await?
                .is_some(),
            "new key must authenticate on its {surface} surface"
        );
    }
    assert!(
        api_keys::advance_api_key_last_used_at(&db.pool, key_id.as_uuid(), GRANULARITY).await?,
        "the key's use is recorded"
    );
    let used_at = key_state(&db.pool, key_id).await?.last_used_at;

    let legacy_key_id = insert_key(&db.pool, org_id, "legacy-unassigned-key-hash").await?;
    let listed = api_keys::list_organization_api_keys(&db.pool, org_id, false).await?;
    assert!(listed.iter().any(|key| key.id == key_id));
    assert!(
        listed
            .iter()
            .any(|key| key.id == legacy_key_id && key.created_by.is_none()),
        "legacy keys without a surviving creator remain visible"
    );

    assert_eq!(
        api_keys::revoke_organization_api_key(&db.pool, OrgId::new(), key_id).await?,
        ApiKeyRevocation::NotFound,
        "an organization cannot revoke another organization's key"
    );
    // Revocation composes into the caller's transaction: rolled back, the key
    // is still live.
    let mut tx = db.pool.begin().await?;
    assert_eq!(
        api_keys::revoke_organization_api_key(&mut *tx, org_id, key_id).await?,
        ApiKeyRevocation::Revoked
    );
    tx.rollback().await?;
    assert!(
        api_keys::api_key_auth_by_hash(&db.pool, key_hash, ApiSurface::Native)
            .await?
            .is_some(),
        "a rolled-back revocation leaves the key live"
    );
    assert_eq!(
        api_keys::revoke_organization_api_key(&db.pool, org_id, key_id).await?,
        ApiKeyRevocation::Revoked
    );
    for surface in [ApiSurface::Ecw, ApiSurface::Native] {
        assert!(
            api_keys::api_key_auth_by_hash(&db.pool, key_hash, surface)
                .await?
                .is_none(),
            "a revoked key must stop authenticating on its {surface} surface"
        );
    }
    assert!(
        !api_keys::list_organization_api_keys(&db.pool, org_id, false)
            .await?
            .iter()
            .any(|key| key.id == key_id),
        "a revoked key is listed only on request"
    );
    let revoked = api_keys::list_organization_api_keys(&db.pool, org_id, true)
        .await?
        .into_iter()
        .find(|key| key.id == key_id)
        .expect("a revoked key is listed on request");
    assert!(
        revoked.revoked_at.is_some(),
        "a revoked key lists as revoked"
    );
    assert!(
        revoked.last_used_at.is_some(),
        "the key's use before revocation is recorded"
    );
    assert_eq!(
        revoked.last_used_at, used_at,
        "revocation keeps the recorded use"
    );
    Ok(())
}

/// The text between a statement's first `SELECT` and its first `FROM` outside
/// parentheses: its select list.
fn select_list(statement: &str) -> &str {
    let (_, rest) = statement
        .split_once("SELECT")
        .expect("the statement selects");
    let mut depth = 0usize;
    for (index, character) in rest.char_indices() {
        match character {
            '(' => depth += 1,
            ')' => depth -= 1,
            _ if depth == 0
                && let (Some(list), Some(tail)) = (rest.get(..index), rest.get(index..))
                && list.ends_with(char::is_whitespace)
                && tail
                    .strip_prefix("FROM")
                    .is_some_and(|after| after.starts_with(char::is_whitespace)) =>
            {
                return list;
            }
            _ => {}
        }
    }
    panic!("the statement has no top-level FROM: {statement}")
}

#[test]
fn joined_user_and_organization_reads_share_one_select_list() {
    let joined = select_list(users::USER_WITH_ORGANIZATION_SQL);
    let federated = select_list(organizations::FEDERATED_ORGANIZATION_USER_SQL);
    assert!(joined.contains("u.id AS user_id"), "{joined}");
    assert!(
        joined.contains("o.updated_at AS org_updated_at"),
        "{joined}"
    );
    assert_eq!(
        joined, federated,
        "both statements decode `UserOrganizationRow`, so their select lists change together"
    );
}
