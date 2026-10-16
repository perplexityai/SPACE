use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use pplx_space_credential_injection_core::secret_arn::SecretArn;
use zeroize::Zeroizing;

use super::*;
use crate::aws_secrets_manager::ResolvedAwsSecret;
use crate::credential_provider::{
    CredentialFetch, CredentialProviderResolver, PreparedCredential, ResolutionContext,
};

struct TestAwsProvider {
    value: &'static str,
    reads: Arc<AtomicUsize>,
}

#[async_trait]
impl CredentialProviderResolver for TestAwsProvider {
    type Reference = SecretArn;
    type Material = ResolvedAwsSecret;

    async fn prepare(
        &self,
        _context: ResolutionContext,
        _reference: &SecretArn,
    ) -> Result<PreparedCredential<Self::Material>, ResolutionError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(PreparedCredential::new(TestFetch(self.value)))
    }
}

struct TestFetch(&'static str);

#[async_trait]
impl CredentialFetch<ResolvedAwsSecret> for TestFetch {
    async fn fetch(self: Box<Self>) -> Result<ResolvedAwsSecret, ResolutionError> {
        Ok(ResolvedAwsSecret::new(Zeroizing::new(self.0.to_string())))
    }
}

fn materializer_returning(
    value: &'static str,
) -> (NetworkCredentialMaterializer, Arc<AtomicUsize>) {
    let reads = Arc::new(AtomicUsize::new(0));
    let providers = CredentialProviderDispatcher::default().with_aws_secrets_manager(Arc::new(
        TestAwsProvider {
            value,
            reads: reads.clone(),
        },
    ));
    (NetworkCredentialMaterializer::new(providers), reads)
}

fn reference(destinations: Vec<DestinationMatcher>) -> NetworkCredentialMaterialization {
    NetworkCredentialMaterialization {
        reference_id: CredentialReferenceId::new(),
        reference: ValidatedCredentialReference::aws_secrets_manager(
            "arn:aws:secretsmanager:us-west-2:123456789012:secret:openai-AbCdEf",
        )
        .unwrap(),
        destinations,
    }
}

fn destination() -> DestinationMatcher {
    DestinationMatcher {
        exact_host: "api.openai.com".to_string(),
        port: 443,
    }
}

#[tokio::test]
async fn materializes_the_provider_value_keyed_by_reference() {
    let (materializer, reads) = materializer_returning("Bearer custom-secret");
    let input = reference(vec![destination()]);
    let reference_id = input.reference_id.to_string();

    let credentials = materializer
        .materialize_network_credentials(Uuid::now_v7(), vec![input])
        .await
        .unwrap();

    assert_eq!(reads.load(Ordering::SeqCst), 1);
    assert_eq!(credentials.len(), 1);
    assert_eq!(credentials[0].credential_reference_id, reference_id);
    assert_eq!(credentials[0].value, b"Bearer custom-secret");
}

#[tokio::test]
async fn omits_permanently_unusable_credentials() {
    // A value that cannot be an HTTP header is a permanent denial for that
    // reference, as is a reference without an authorized destination.
    let (materializer, _) = materializer_returning("line\nbreak");
    assert!(
        materializer
            .materialize_network_credentials(Uuid::now_v7(), vec![reference(vec![destination()])])
            .await
            .unwrap()
            .is_empty()
    );

    let (materializer, _) = materializer_returning("Bearer custom-secret");
    assert!(
        materializer
            .materialize_network_credentials(Uuid::now_v7(), vec![reference(Vec::new())])
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn unconfigured_provider_omits_the_credential() {
    let materializer = NetworkCredentialMaterializer::new(CredentialProviderDispatcher::default());

    assert!(
        materializer
            .materialize_network_credentials(Uuid::now_v7(), vec![reference(vec![destination()])])
            .await
            .unwrap()
            .is_empty()
    );
}
