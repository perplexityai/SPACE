use pplx_space_accounts::OrgId;

use sqlx::PgPool;

use crate::{
    models::User,
    user_role::{UserRole, UserRolePolicy},
};
use pplx_space_accounts_store::{clusters, organizations};

pub(crate) async fn is_managing_org_admin(pool: &PgPool, user: &User) -> Result<bool, sqlx::Error> {
    if user.role != UserRole::OrganizationAdmin {
        return Ok(false);
    }

    Ok(user.org_id == clusters::cluster_managing_org_id(pool).await?)
}

pub(crate) async fn visible_org_ids(pool: &PgPool, user: &User) -> Result<Vec<OrgId>, sqlx::Error> {
    match user.role {
        UserRole::OrganizationAdmin => {
            if is_managing_org_admin(pool, user).await? {
                organizations::organization_ids(pool)
                    .await
                    .map_err(sqlx::Error::from)
            } else {
                organizations::organization_subtree_ids(pool, user.org_id)
                    .await
                    .map_err(sqlx::Error::from)
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
                organizations::organization_subtree_contains(pool, user.org_id, org_id)
                    .await
                    .map_err(sqlx::Error::from)
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
