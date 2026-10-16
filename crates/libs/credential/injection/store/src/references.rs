use pplx_space_common_core::{
    sql,
    types::{CredentialReferenceId, OrgId},
};
use pplx_space_credential_injection_core::validation::ValidatedCredentialReference;
use pplx_space_net_common::NetworkPolicyV1;
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

/// Share-lock the references a network-policy write binds until the caller's
/// transaction ends.
///
/// Retirement holds the reference row exclusively while it checks
/// [`attached`], so a concurrent bind either commits first and blocks
/// retirement, or waits and then binds a retired reference that resolution
/// never authorizes. Templates can serve multiple organizations, so all
/// existing references are locked. Sandbox writes use
/// [`lock_for_sandbox_binding`] to lock only their organization's references.
pub async fn lock_for_binding(
    executor: impl sqlx::PgExecutor<'_>,
    policy: &NetworkPolicyV1,
) -> Result<(), sqlx::Error> {
    lock_policy_references(executor, None, policy).await
}

async fn lock_policy_references(
    executor: impl sqlx::PgExecutor<'_>,
    organization: Option<OrgId>,
    policy: &NetworkPolicyV1,
) -> Result<(), sqlx::Error> {
    let mut ids: Vec<_> = policy
        .network_credential_rules
        .iter()
        .flat_map(|rule| &rule.credential_header_bindings)
        .map(|binding| binding.credential_reference_id)
        .collect();
    if ids.is_empty() {
        return Ok(());
    }
    ids.sort_unstable();
    ids.dedup();
    sqlx::query(sql!(
        "SELECT id FROM credential_references
         WHERE id = ANY($1) AND ($2::uuid IS NULL OR org_id = $2)
         ORDER BY id FOR SHARE"
    ))
    .bind(ids)
    .bind(organization)
    .fetch_all(executor)
    .await?;
    Ok(())
}

/// Share-lock what a sandbox policy write makes effective until the caller's
/// transaction ends: the override's references or, when the sandbox inherits,
/// its template row and the references of that row's current policy.
/// Missing and foreign references cannot resolve for the sandbox and are not
/// locked, so one organization's writes cannot lock another's credentials.
///
/// Resolution reads the template row live, even after template deletion, so a
/// policy resolved before the transaction is not what the sandbox inherits.
/// The row lock makes a concurrent template policy write or deletion wait for
/// this sandbox write; a template write that wins instead is read here.
pub async fn lock_for_sandbox_binding(
    connection: &mut sqlx::PgConnection,
    organization: OrgId,
    policy_override: Option<&NetworkPolicyV1>,
    template: Option<&str>,
) -> Result<(), sqlx::Error> {
    if let Some(policy) = policy_override {
        return lock_policy_references(connection, Some(organization), policy).await;
    }
    let Some(template) = template else {
        return Ok(());
    };
    let inherited: Option<Json<NetworkPolicyV1>> = sqlx::query_scalar(sql!(
        "SELECT network_policy_json FROM templates WHERE name = $1 FOR SHARE"
    ))
    .bind(template)
    .fetch_optional(&mut *connection)
    .await?;
    match inherited {
        Some(policy) => lock_policy_references(connection, Some(organization), &policy.0).await,
        None => Ok(()),
    }
}

/// Whether a policy the organization can run still binds its reference.
///
/// A sandbox binds through its override, or through its template's policy
/// when it has none; like resolution, that inheritance survives template
/// deletion. Deleted sandbox tombstones never run again. A template that is
/// not deleted and that the organization can use binds its future sandboxes.
/// Other organizations' policies never resolve the reference.
pub async fn attached(
    executor: impl sqlx::PgExecutor<'_>,
    organization: OrgId,
    id: CredentialReferenceId,
) -> Result<bool, sqlx::Error> {
    // JSONB containment matches any rule with any binding naming the
    // reference, using resolution's text comparison of the identifier.
    sqlx::query_scalar(sql!(
        "WITH binding AS (
             SELECT jsonb_build_object('network_credential_rules', jsonb_build_array(
                 jsonb_build_object('credential_header_bindings', jsonb_build_array(
                     jsonb_build_object('credential_reference_id', $2::uuid::TEXT))))) AS policy
         )
         SELECT EXISTS (
             SELECT 1
             FROM sandboxes s
             LEFT JOIN templates t ON t.name = s.template_id
             CROSS JOIN binding
             WHERE s.org_id = $1 AND s.lifecycle <> 'deleted'
               AND COALESCE(s.network_policy_override_json, t.network_policy_json)
                   @> binding.policy
         ) OR EXISTS (
             SELECT 1
             FROM templates t
             CROSS JOIN binding
             WHERE t.desired_state <> 'deleted'
               AND (t.visibility = 'generic' OR t.org_id = $1)
               AND t.network_policy_json @> binding.policy
         )"
    ))
    .bind(organization)
    .bind(id)
    .fetch_one(executor)
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
