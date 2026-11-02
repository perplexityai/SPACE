//! Storage exchanges reuse one AWS configuration, so the base identity is
//! resolved once and STS is reached over a pooled connection (INC-834).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use aws_credential_types::Credentials;
use aws_credential_types::credential_fn::provide_credentials_fn;
use aws_credential_types::provider::SharedCredentialsProvider;
use pplx_space_common_svc::core::types::{ContentHash, TemplateName};
use pplx_space_credmgr_client::{
    STORAGE_SESSION_TTL_SECONDS, STORAGE_TOKEN_AUDIENCE, STORAGE_WRITE_SCOPE,
    SUBJECT_ID_TOKEN_TYPE, TOKEN_EXCHANGE_GRANT_TYPE,
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};

use super::*;
use crate::storage_vend_store::{SandboxStorageContext, StorageVendIdentity};

const ROLE_ARN: &str = "arn:aws:iam::123456789012:role/space-storage-vend";

/// Every template is an unpublished system template.
struct SystemTemplates;

#[tonic::async_trait]
impl StorageVendStore for SystemTemplates {
    async fn sandbox_context(
        &self,
        _sandbox_id: SandboxId,
        _snapshot_id: pplx_space_common_core::types::SnapshotId,
    ) -> Result<Option<SandboxStorageContext>, StorageVendStoreError> {
        Ok(None)
    }

    async fn template_owner(
        &self,
        _template: &TemplateName,
    ) -> Result<Option<Option<OrgId>>, StorageVendStoreError> {
        Ok(Some(None))
    }

    async fn template_release_published(
        &self,
        _template: &TemplateName,
        _hash: &ContentHash,
    ) -> Result<bool, StorageVendStoreError> {
        Ok(false)
    }

    async fn org_vend_identity(
        &self,
        _organization_id: OrgId,
    ) -> Result<Option<StorageVendIdentity>, StorageVendStoreError> {
        Ok(None)
    }
}

/// Fake STS that serves `AssumeRole` over its first connection, kept alive,
/// and resets every later connection, as the affected hosts' connect step
/// failed while established connections kept working.
async fn first_connection_only_sts(accepted: Arc<AtomicUsize>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            if accepted.fetch_add(1, Ordering::SeqCst) == 0 {
                tokio::spawn(serve_assume_role(stream));
            }
        }
    });
    url
}

async fn serve_assume_role(mut stream: TcpStream) {
    let expiration = jiff::Timestamp::now() + jiff::SignedDuration::from_hours(1);
    let body = format!(
        "<AssumeRoleResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\"><AssumeRoleResult>\
         <Credentials><AccessKeyId>ASIAVENDED</AccessKeyId><SecretAccessKey>secret</SecretAccessKey>\
         <SessionToken>token</SessionToken><Expiration>{expiration}</Expiration></Credentials>\
         <AssumedRoleUser><AssumedRoleId>AROA:space-storage-exchange</AssumedRoleId>\
         <Arn>arn:aws:sts::123456789012:assumed-role/space-storage-vend/space-storage-exchange</Arn>\
         </AssumedRoleUser></AssumeRoleResult></AssumeRoleResponse>",
        expiration = expiration.strftime("%Y-%m-%dT%H:%M:%SZ"),
    );
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/xml\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    );
    let mut pending = Vec::new();
    let mut buf = [0_u8; 4096];
    loop {
        // One response per complete request; the connection stays open.
        while let Some(end) = request_end(&pending) {
            pending.drain(..end);
            if stream.write_all(response.as_bytes()).await.is_err() {
                return;
            }
        }
        match stream.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(read) => pending.extend_from_slice(&buf[..read]),
        }
    }
}

/// Length of the first complete `content-length` request in `bytes`.
fn request_end(bytes: &[u8]) -> Option<usize> {
    let header_end = bytes.windows(4).position(|w| w == b"\r\n\r\n")? + 4;
    let content_length = String::from_utf8_lossy(&bytes[..header_end])
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())?
        })
        .unwrap_or(0);
    (bytes.len() >= header_end + content_length).then_some(header_end + content_length)
}

fn exchange(hash: usize) -> Request<ExchangeTokenRequest> {
    let mut request = Request::new(ExchangeTokenRequest {
        grant_type: TOKEN_EXCHANGE_GRANT_TYPE.to_string(),
        resource: format!("arn:aws:s3:::space-snapshots/templates/seed/sha256:{hash:064x}/*"),
        audience: Some(STORAGE_TOKEN_AUDIENCE.to_string()),
        scope: Some(STORAGE_WRITE_SCOPE.to_string()),
        requested_token_type: AWS_CREDENTIALS_TOKEN_TYPE.to_string(),
        subject_token: Some("template:seed".to_string()),
        subject_token_type: Some(SUBJECT_ID_TOKEN_TYPE.to_string()),
        actor_token: None,
        actor_token_type: None,
        requested_expires_in: Some(STORAGE_SESSION_TTL_SECONDS),
        copy_source_resource: None,
    });
    request
        .extensions_mut()
        .insert(crate::tcp_auth::TcpCallerIdentity::K8sJwt(
            test_k8s_identity(ClientService::BuildManager),
        ));
    request
}

#[tokio::test]
async fn storage_exchanges_reuse_base_credentials_and_sts_connections() {
    let accepted = Arc::new(AtomicUsize::new(0));
    let endpoint = first_connection_only_sts(Arc::clone(&accepted)).await;
    let base_resolutions = Arc::new(AtomicUsize::new(0));
    let base = {
        let base_resolutions = Arc::clone(&base_resolutions);
        provide_credentials_fn(move || {
            base_resolutions.fetch_add(1, Ordering::SeqCst);
            async {
                Ok(Credentials::new(
                    "base-access-key",
                    "base-secret-key",
                    None,
                    Some(SystemTime::now() + Duration::from_secs(3600)),
                    "sts-reuse-test",
                ))
            }
        })
    };
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .credentials_provider(SharedCredentialsProvider::new(base))
        .load()
        .await;
    let service = CredentialManagerService::new(
        Arc::new(InMemoryKeyWrapper::new()),
        PeerProcessCheckMode::Off,
    )
    .with_storage_vend_store(Arc::new(SystemTemplates))
    .with_storage_vend_role(VendRoleArn::parse(ROLE_ARN).unwrap())
    .with_storage_sts_config(&config);

    for hash in 0..5 {
        // Handlers run on clones of the service.
        let response = service
            .clone()
            .exchange_token(exchange(hash))
            .await
            .unwrap_or_else(|status| panic!("exchange {hash}: {status}"));
        assert!(response.into_inner().payload.is_some());
    }
    assert_eq!(base_resolutions.load(Ordering::SeqCst), 1);
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
}
