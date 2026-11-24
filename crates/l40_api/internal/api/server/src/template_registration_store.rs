use super::{NetworkPolicyV1, TemplateRow};
use pplx_space_accounts::OrgId;
use pplx_space_template_model::{TemplateDesiredState, TemplateId, TemplateVisibility};
use pplx_space_template_store::templates::{self, NewTemplate, RegisteredTemplate};
use sqlx::PgExecutor;

pub(super) async fn insert(
    executor: impl PgExecutor<'_>,
    name: &str,
    description: Option<&str>,
    owner: Option<OrgId>,
) -> Result<Option<TemplateRow>, sqlx::Error> {
    // Launch scope follows ownership: a tenant's registration is private to
    // that org, while an ownerless system publication is usable everywhere.
    // The build API advertises exactly this (`public: false` for owned
    // builds); widening an owned template is an operator action through the
    // admin visibility route, never a registration default.
    let visibility = if owner.is_some() {
        TemplateVisibility::OrgSpecific
    } else {
        TemplateVisibility::Generic
    };
    // Both the name and the name/owner constraints can conflict concurrently.
    // The caller rechecks the winning row under a lock in a fresh statement.
    let inserted = templates::insert_if_absent(
        executor,
        NewTemplate {
            id: TemplateId::new(),
            name,
            description,
            org_id: owner,
            visibility,
            network_policy: &NetworkPolicyV1::deny_all(),
            desired_state: TemplateDesiredState::Present,
        },
    )
    .await?;
    Ok(inserted.map(TemplateRow::from))
}

pub(super) async fn fill_description(
    executor: impl PgExecutor<'_>,
    name: &str,
    description: Option<&str>,
) -> Result<TemplateRow, sqlx::Error> {
    templates::fill_description(executor, name, description)
        .await
        .map(TemplateRow::from)
}

impl From<RegisteredTemplate> for TemplateRow {
    fn from(row: RegisteredTemplate) -> Self {
        let RegisteredTemplate {
            name,
            description,
            created_at,
            network_policy_json,
            network_policy_generation,
        } = row;
        Self {
            name,
            description,
            created_at,
            network_policy_json,
            network_policy_generation,
        }
    }
}
