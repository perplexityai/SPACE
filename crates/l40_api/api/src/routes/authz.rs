use pplx_space_common_core::types::OrgId;

use pplx_space_common_core::sql;
use sqlx::PgPool;

use crate::{models::User, user_role::UserRole};

pub(crate) async fn cluster_managing_org_id(pool: &PgPool) -> Result<OrgId, sqlx::Error> {
    sqlx::query_scalar::<_, OrgId>(sql!("SELECT managing_org_id FROM clusters WHERE singleton"))
        .fetch_one(pool)
        .await
}

pub(crate) async fn is_managing_org_admin(pool: &PgPool, user: &User) -> Result<bool, sqlx::Error> {
    if user.role != UserRole::OrganizationAdmin {
        return Ok(false);
    }

    Ok(user.org_id == cluster_managing_org_id(pool).await?)
}

async fn org_subtree_ids(pool: &PgPool, org_id: OrgId) -> Result<Vec<OrgId>, sqlx::Error> {
    sqlx::query_scalar::<_, OrgId>(sql!(
        "WITH RECURSIVE org_tree AS (
            SELECT id FROM organizations WHERE id = $1
            UNION ALL
            SELECT o.id FROM organizations o JOIN org_tree t ON o.parent_id = t.id
        )
        SELECT id FROM org_tree"
    ))
    .bind(org_id)
    .fetch_all(pool)
    .await
}

async fn org_subtree_contains(
    pool: &PgPool,
    root_org_id: OrgId,
    target_org_id: OrgId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(sql!(
        "WITH RECURSIVE org_tree AS (
            SELECT id FROM organizations WHERE id = $1
            UNION ALL
            SELECT o.id FROM organizations o JOIN org_tree t ON o.parent_id = t.id
        )
        SELECT EXISTS(SELECT 1 FROM org_tree WHERE id = $2)"
    ))
    .bind(root_org_id)
    .bind(target_org_id)
    .fetch_one(pool)
    .await
}

pub(crate) async fn visible_org_ids(pool: &PgPool, user: &User) -> Result<Vec<OrgId>, sqlx::Error> {
    match user.role {
        UserRole::OrganizationAdmin => {
            if is_managing_org_admin(pool, user).await? {
                sqlx::query_scalar::<_, OrgId>(sql!("SELECT id FROM organizations"))
                    .fetch_all(pool)
                    .await
            } else {
                org_subtree_ids(pool, user.org_id).await
            }
        }
        UserRole::Member => Ok(vec![user.org_id]),
    }
}

pub(crate) async fn user_can_access_org(
    pool: &PgPool,
    user: &User,
    org_id: OrgId,
) -> Result<bool, sqlx::Error> {
    match user.role {
        UserRole::OrganizationAdmin => {
            if is_managing_org_admin(pool, user).await? {
                Ok(true)
            } else {
                org_subtree_contains(pool, user.org_id, org_id).await
            }
        }
        UserRole::Member => Ok(user.org_id == org_id),
    }
}

pub(crate) async fn can_manage_users_in_org(
    pool: &PgPool,
    actor: &User,
    target_org_id: OrgId,
) -> Result<bool, sqlx::Error> {
    match actor.role {
        UserRole::OrganizationAdmin => {
            if is_managing_org_admin(pool, actor).await? {
                Ok(true)
            } else {
                user_can_access_org(pool, actor, target_org_id).await
            }
        }
        UserRole::Member => Ok(false),
    }
}

pub(crate) async fn can_create_user_with_role(
    pool: &PgPool,
    actor: &User,
    target_org_id: OrgId,
    role: UserRole,
) -> Result<bool, sqlx::Error> {
    if !can_manage_users_in_org(pool, actor, target_org_id).await? {
        return Ok(false);
    }

    Ok(actor.role.can_assign_role(role))
}

pub(crate) async fn can_update_user_profile(
    pool: &PgPool,
    actor: &User,
    target: &User,
) -> Result<bool, sqlx::Error> {
    if actor.id == target.id {
        return Ok(true);
    }

    if actor.role == UserRole::Member {
        return Ok(false);
    }

    if !actor.role.can_mutate_role_holder(target.role) {
        return Ok(false);
    }

    can_manage_users_in_org(pool, actor, target.org_id).await
}

pub(crate) async fn can_update_user_role(
    pool: &PgPool,
    actor: &User,
    target: &User,
    role: UserRole,
) -> Result<bool, sqlx::Error> {
    if !can_update_user_profile(pool, actor, target).await? {
        return Ok(false);
    }

    Ok(actor.role.can_assign_role(role))
}
