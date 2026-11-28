use pplx_space_accounts::{CredentialReferenceId, OrgId};
use pplx_space_credential_injection::validation::ValidatedCredentialReference;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
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
        pplx_space_credential_injection::validation::ReferenceValidationError,
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
    #[sqlx(json)]
    pub config: Config,
    pub state: ReferenceState,
}

pub async fn create(
    executor: impl Executor<'_>,
    organization: OrgId,
    reference: &ValidatedCredentialReference,
) -> Result<CredentialReferenceId, DbError> {
    let id = CredentialReferenceId::new();
    sqlx::query(sql!(
        "INSERT INTO credential_references (id, org_id, upstream_provider, upstream_provider_config)
         VALUES ($1, $2, $3::jsonb ->> 'upstream_provider', $3::jsonb -> 'upstream_provider_config')"
    ))
    .bind(id)
    .bind(organization)
    .bind(Json(Config::from(reference)))
    .execute(executor.into_sqlx())
    .await?;
    Ok(id)
}

/// Lock and return the references in `ids` that belong to `organization`,
/// in UUID order (`SELECT ... FOR UPDATE`). The locks last until the
/// caller's transaction ends, so the caller must pass its open transaction.
pub async fn lock<C>(
    tx: &mut C,
    organization: OrgId,
    ids: &[CredentialReferenceId],
) -> Result<Vec<Reference>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    let ids: Vec<_> = ids.iter().map(|id| id.as_uuid()).collect();
    Ok(sqlx::query_as(sql!(
        "SELECT id, jsonb_build_object('upstream_provider', upstream_provider,
                  'upstream_provider_config', upstream_provider_config) AS config, state
         FROM credential_references WHERE org_id = $1 AND id = ANY($2)
         ORDER BY id FOR UPDATE"
    ))
    .bind(organization)
    .bind(ids)
    .fetch_all(tx.into_sqlx())
    .await?)
}

pub async fn retire(
    executor: impl Executor<'_>,
    organization: OrgId,
    id: CredentialReferenceId,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "UPDATE credential_references SET state = 'retired'
         WHERE org_id = $1 AND id = $2 AND state = 'active'"
    ))
    .bind(organization)
    .bind(id)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}
