use super::{ClusterApiService, adapter::TemplateOrg};
use pplx_space_common_core::{
    sql,
    types::{OrgId, SystemPublicationGrantId, TemplateDesiredState, TemplateName},
};
use std::collections::BTreeSet;
use tonic::Status;

#[derive(Clone)]
pub(super) struct ReservedTemplateNames(BTreeSet<String>);

impl Default for ReservedTemplateNames {
    fn default() -> Self {
        Self(["seed".to_string(), "code-runtime".to_string()].into())
    }
}

pub(super) fn parse_template_name(value: &str) -> Result<TemplateName, Status> {
    TemplateName::parse(value.trim().to_string())
        .map_err(|err| Status::invalid_argument(format!("invalid template name: {err}")))
}

impl ReservedTemplateNames {
    pub(super) fn from_env() -> Result<Self, Status> {
        let mut names = Self::default();
        let additional = match std::env::var("SPACE_TEMPLATE_PUBLISH_RESERVED_NAMES") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => return Ok(names),
            Err(error) => {
                return Err(Status::internal(format!(
                    "invalid reserved template names: {error}"
                )));
            }
        };
        for name in additional
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
        {
            names.0.insert(parse_template_name(name)?.to_string());
        }
        Ok(names)
    }

    pub(super) fn authorize(&self, name: &TemplateName, owner: TemplateOrg) -> Result<(), Status> {
        if matches!(owner, TemplateOrg::Owned { .. }) && self.0.contains(name.as_str()) {
            return Err(Status::permission_denied(
                "template name is reserved for operator publication",
            ));
        }
        Ok(())
    }
}

impl ClusterApiService {
    pub(super) async fn lock_template_for_publication(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        name: &TemplateName,
        publisher: TemplateOrg,
    ) -> Result<Option<OrgId>, Status> {
        // Hold the ownership check through registration and tag writes, including
        // against concurrent administrative ownership or lifecycle changes.
        let (owner, desired_state, managing_org) =
            sqlx::query_as::<_, (Option<OrgId>, String, Option<OrgId>)>(sql!(
                "SELECT org_id, desired_state,
                    (SELECT managing_org_id FROM clusters WHERE singleton)
             FROM templates WHERE name = $1 FOR UPDATE"
            ))
            .bind(name.as_str())
            .fetch_optional(&mut **tx)
            .await
            .map_err(|err| Status::internal(format!("lock publication owner: {err}")))?
            .ok_or_else(|| Status::not_found("template does not exist"))?;
        let allowed = match publisher {
            TemplateOrg::Owned { org_id } => owner == Some(org_id),
            // Reserved legacy templates may belong to the managing org. That
            // association is compatibility data, never operator authority.
            TemplateOrg::System => {
                owner.is_none()
                    || (self.reserved_template_names.0.contains(name.as_str())
                        && owner == managing_org)
            }
        };
        if !allowed || desired_state != TemplateDesiredState::Present.as_str() {
            return Err(Status::permission_denied(
                "template ownership or lifecycle does not permit publication",
            ));
        }
        Ok(owner)
    }
}

/// Publication authority is distinct from tenant ownership. A grant is never
/// inferred from the target name or borrowed from another pending build.
pub(super) fn system_publication_grant(
    value: Option<&str>,
    owner: TemplateOrg,
    requires_grant: bool,
) -> Result<Option<SystemPublicationGrantId>, Status> {
    match (owner, value) {
        (TemplateOrg::Owned { .. }, Some(_)) => Err(Status::invalid_argument(
            "tenant publication cannot carry a system publication grant",
        )),
        (_, Some(value)) => value
            .parse()
            .map(Some)
            .map_err(|_| Status::invalid_argument("invalid system publication grant ID")),
        (TemplateOrg::System, None) if requires_grant => Err(missing_system_publication_grant()),
        (_, None) => Ok(None),
    }
}

pub(super) fn missing_system_publication_grant() -> Status {
    Status::permission_denied("system template publication requires a build publication grant")
}
