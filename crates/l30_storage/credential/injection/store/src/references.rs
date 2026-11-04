use pplx_space_common_core::{
    sql,
    types::{CredentialReferenceId, OrgId},
};
use pplx_space_credential_injection_core::validation::ValidatedCredentialReference;
use serde::{Deserialize, Serialize};
use sqlx::types::Json;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "upstream_provider",
    content = "upstream_provider_config",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Config {
    AwsSecretsManager { secret_arn: String },
}

impl From<&ValidatedCredentialReference> for Config {
    fn from(reference: &ValidatedCredentialReference) -> Self {
        match reference {
            ValidatedCredentialReference::AwsSecretsManager(secret_arn) => {
                Self::AwsSecretsManager {
                    secret_arn: secret_arn.as_str().to_owned(),
                }
            }
        }
    }
}

impl Config {
    pub fn to_validated_reference(
        &self,
    ) -> Result<
        ValidatedCredentialReference,
        pplx_space_credential_injection_core::validation::ReferenceValidationError,
    > {
        match self {
            Self::AwsSecretsManager { secret_arn } => {
                ValidatedCredentialReference::aws_secrets_manager(secret_arn.clone())
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
pub enum ReferenceState {
    Active,
    Retired,
}

#[derive(Clone, Debug, sqlx::FromRow)]
pub struct Reference {
    pub id: CredentialReferenceId,
    pub config: Json<Config>,
    pub state: ReferenceState,
}

pub async fn create(
    executor: impl sqlx::PgExecutor<'_>,
    organization: OrgId,
    reference: &ValidatedCredentialReference,
) -> Result<CredentialReferenceId, sqlx::Error> {
    let id = CredentialReferenceId::new();
    sqlx::query(sql!(
        "INSERT INTO credential_references (id, org_id, upstream_provider, upstream_provider_config)
         VALUES ($1, $2, $3::jsonb ->> 'upstream_provider', $3::jsonb -> 'upstream_provider_config')"
    ))
    .bind(id)
    .bind(organization)
    .bind(Json(Config::from(reference)))
    .execute(executor)
    .await?;
    Ok(id)
}

/// Lock references in UUID order before using them in a transaction.
pub async fn lock(
    executor: impl sqlx::PgExecutor<'_>,
    organization: OrgId,
    ids: &[CredentialReferenceId],
) -> Result<Vec<Reference>, sqlx::Error> {
    let ids: Vec<_> = ids.iter().map(|id| id.as_uuid()).collect();
    sqlx::query_as(sql!(
        "SELECT id, jsonb_build_object('upstream_provider', upstream_provider,
                  'upstream_provider_config', upstream_provider_config) AS config, state
         FROM credential_references WHERE org_id = $1 AND id = ANY($2)
         ORDER BY id FOR UPDATE"
    ))
    .bind(organization)
    .bind(ids)
    .fetch_all(executor)
    .await
}

pub async fn retire(
    executor: impl sqlx::PgExecutor<'_>,
    organization: OrgId,
    id: CredentialReferenceId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE credential_references SET state = 'retired'
         WHERE org_id = $1 AND id = $2 AND state = 'active'"
    ))
    .bind(organization)
    .bind(id)
    .execute(executor)
    .await?;
    Ok(())
}
