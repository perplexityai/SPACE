//! Durable replies for credential reference registration.
use pplx_space_accounts::OrgId;
use pplx_space_credential_injection::validation::ValidatedCredentialReference;
use pplx_space_guest_process::IdempotencyKey;
use pplx_space_public_api::RequestFingerprint;
use pplx_space_public_operation_store::idempotency as idempotency_store;
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
    fn key(&self) -> idempotency_store::IdempotencyKeyRef<'_> {
        idempotency_store::IdempotencyKeyRef {
            org_id: self.organization,
            method: self.method(),
            path_template: self.path(),
            idempotency_key: &self.key,
        }
    }
}

pub(super) use pplx_space_public_operation_store::idempotency::RecordedReply as Reply;

pub(super) async fn get<T: DeserializeOwned + Send + Unpin + 'static>(
    executor: impl pplx_space_postgres_pool::Executor<'_>,
    scope: &Scope<'_>,
) -> Result<Option<Reply<T>>, sqlx::Error> {
    idempotency_store::find_native_reply(executor, scope.key())
        .await
        .map_err(sqlx::Error::from)
}

pub(super) async fn insert<T: Serialize + Sync>(
    executor: impl pplx_space_postgres_pool::Executor<'_>,
    scope: &Scope<'_>,
    fingerprint: &RequestFingerprint,
    response: &T,
) -> Result<bool, sqlx::Error> {
    idempotency_store::insert_permanent_native_reply(
        executor,
        Uuid::now_v7(),
        scope.key(),
        fingerprint.as_str(),
        response,
    )
    .await
    .map_err(sqlx::Error::from)
}
