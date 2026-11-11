//! Durable replies for credential reference registration.
use pplx_space_common_core::{
    sql,
    types::{IdempotencyKey, OrgId},
};
use pplx_space_credential_injection_core::validation::ValidatedCredentialReference;
use pplx_space_public_api::RequestFingerprint;
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Debug)]
pub(super) struct Scope<'a> {
    organization: OrgId,
    key: String,
    reference: &'a ValidatedCredentialReference,
}
impl<'a> Scope<'a> {
    pub(super) fn register(
        organization: OrgId,
        request_id: &IdempotencyKey,
        reference: &'a ValidatedCredentialReference,
    ) -> Self {
        Self {
            organization,
            key: hex::encode(Sha256::digest(request_id.as_str().as_bytes())),
            reference,
        }
    }

    pub(super) fn fingerprint(&self) -> RequestFingerprint {
        let mut hash = Sha256::new();
        hash.update(b"credential-reference-register-v2\0");
        match self.reference {
            ValidatedCredentialReference::AwsSecretsManager(secret_arn) => {
                hash.update(b"aws_secrets_manager\0");
                hash.update(secret_arn.as_str().as_bytes());
            }
        }
        RequestFingerprint::parse(hex::encode(hash.finalize()))
            .expect("SHA-256 hex is a valid request fingerprint")
    }

    fn method(&self) -> &'static str {
        "POST"
    }
    fn path(&self) -> &'static str {
        pplx_space_public_api::routes::REGISTER_CREDENTIAL_REFERENCE.path()
    }
}

#[derive(Debug, sqlx::FromRow)]
pub(super) struct Reply<T> {
    pub(super) fingerprint: String,
    pub(super) response: sqlx::types::Json<T>,
}

pub(super) async fn get<T: DeserializeOwned + Send + Unpin + 'static>(
    executor: impl sqlx::PgExecutor<'_>,
    scope: &Scope<'_>,
) -> Result<Option<Reply<T>>, sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT request_fingerprint AS fingerprint, response_json AS response
         FROM public_idempotency_records
         WHERE org_id = $1 AND api_product = 'native' AND method = $4
           AND path_template = $2 AND idempotency_key = $3"
    ))
    .bind(scope.organization.as_uuid())
    .bind(scope.path())
    .bind(&scope.key)
    .bind(scope.method())
    .fetch_optional(executor)
    .await
}

pub(super) async fn insert<T: Serialize + Sync>(
    executor: impl sqlx::PgExecutor<'_>,
    scope: &Scope<'_>,
    fingerprint: &RequestFingerprint,
    response: &T,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(sql!(
        "INSERT INTO public_idempotency_records (id, org_id, api_product, method, path_template, idempotency_key, request_fingerprint, response_json, expires_at)
         VALUES ($1, $2, 'native', $7, $3, $4, $5, $6, 'infinity')
         ON CONFLICT (org_id, api_product, method, path_template, idempotency_key) DO NOTHING"
    )).bind(Uuid::now_v7()).bind(scope.organization.as_uuid()).bind(scope.path()).bind(&scope.key).bind(fingerprint.as_str()).bind(sqlx::types::Json(response)).bind(scope.method()).execute(executor).await?;
    Ok(result.rows_affected() == 1)
}
