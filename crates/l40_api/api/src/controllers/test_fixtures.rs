use pplx_space_common_core::{
    sql,
    types::{TemplateId, TemplateVisibility},
};
use pplx_space_db_test_fixtures::TestDbResult;
use sqlx::PgPool;
use uuid::Uuid;

pub(super) async fn insert_org_specific_template(
    pool: &PgPool,
    name: &str,
    owner_org: Uuid,
) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, description, org_id, visibility, network_policy_generation)
         VALUES ($1, $2, NULL, $3, $4, 1)"
    ))
    .bind(TemplateId::new().as_uuid())
    .bind(name)
    .bind(owner_org)
    .bind(TemplateVisibility::OrgSpecific.as_str())
    .execute(pool)
    .await?;
    Ok(())
}
