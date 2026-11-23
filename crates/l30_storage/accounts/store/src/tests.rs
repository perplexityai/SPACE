//! The accounts statements against Postgres: each row type decodes from its
//! explicit column list, and `user_in_organization` answers what the admin
//! create's former `SELECT id ... WHERE id = $1 AND org_id = $2` did.

use pplx_space_accounts::{ApiKeyId, OrgId, UserId};
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, TestDbResult, pg_test_db};
use pplx_space_sql_locator::sql;
use sqlx::PgPool;
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
