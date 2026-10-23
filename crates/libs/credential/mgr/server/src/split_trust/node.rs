//! Node-local sandbox key cache and gateway signing.

use crate::proto::{
    GatewayCaIssuer, IssueIntermediateCertificateRequest, IssueIntermediateCertificateResponse,
    ReleaseSandboxAnchorRequest, ReleaseSandboxAnchorResponse,
};
use pplx_space_credmgr_client::CredentialManagerGrpcClient;
use pplx_space_sandbox_anchor::Anchor;
use rcgen::PublicKeyData;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tonic::Status;
use x509_parser::prelude::{FromDer, X509Certificate};

struct Cached {
    anchor: Anchor,
    acquired: tokio::time::Instant,
}

type AnchorCache = HashMap<(String, Option<i64>), Cached>;

#[derive(Clone)]
pub(crate) struct NodeAnchors {
    upstream: CredentialManagerGrpcClient,
    // Bounded, generation-keyed, memory-only. TTL is a backstop for eviction
    // lost during teardown. Nothing here writes keys to durable storage.
    cache: Arc<Mutex<AnchorCache>>,
}

impl NodeAnchors {
    pub fn new(upstream: CredentialManagerGrpcClient) -> Self {
        Self {
            upstream,
            cache: Arc::default(),
        }
    }

    pub async fn forget(&self, request: ReleaseSandboxAnchorRequest) {
        self.cache.lock().await.retain(|(id, generation), _| {
            id != &request.sandbox_id
                || request
                    .execution_generation
                    .is_some_and(|requested| Some(requested) != *generation)
        });
    }

    pub fn start_sweeper(
        &self,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> tokio_util::task::AbortOnDropHandle<()> {
        let cache = self.cache.clone();
        tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = interval.tick() => {
                        cache.lock().await.retain(|_, entry| entry.acquired.elapsed() < Duration::from_secs(60));
                    }
                }
            }
        }))
    }

    async fn with_anchor<T>(
        &self,
        request: ReleaseSandboxAnchorRequest,
        use_anchor: impl FnOnce(&Anchor) -> Result<T, Status>,
    ) -> Result<T, Status> {
        let id = uuid::Uuid::parse_str(&request.sandbox_id)
            .map_err(|_| Status::invalid_argument("invalid sandbox UUID"))?;
        pplx_space_sandbox_anchor::sandbox_timestamp_ms(id)
            .map_err(|_| Status::invalid_argument("anchor requires UUIDv7"))?;
        let key = (request.sandbox_id.clone(), request.execution_generation);
        let mut cache = self.cache.lock().await;
        cache.retain(|_, entry| entry.acquired.elapsed() < Duration::from_secs(60));
        if !cache.contains_key(&key) {
            if cache.len() >= 4096 {
                return Err(Status::resource_exhausted(
                    "node anchor cache capacity exceeded",
                ));
            }
            let response = tokio::time::timeout(
                Duration::from_secs(10),
                self.upstream.clone().release_sandbox_anchor(request),
            )
            .await
            .map_err(|_| Status::unavailable("anchor authority timed out"))??
            .into_inner();
            if response.private_key_der.is_empty()
                || response.private_key_der.len() > 1024
                || response.certificate_der.is_empty()
                || response.certificate_der.len() > 4096
            {
                return Err(Status::failed_precondition(
                    "authority returned an incomplete anchor",
                ));
            }
            let pair = rcgen::KeyPair::try_from(response.private_key_der.as_slice())
                .map_err(|_| Status::failed_precondition("invalid anchor key"))?;
            let (_, certificate) = X509Certificate::from_der(&response.certificate_der)
                .map_err(|_| Status::failed_precondition("invalid anchor certificate"))?;
            if certificate.public_key().raw != pair.subject_public_key_info() {
                return Err(Status::failed_precondition(
                    "anchor certificate and key differ",
                ));
            }
            certificate
                .verify_signature(None)
                .map_err(|_| Status::failed_precondition("invalid anchor self-signature"))?;
            cache.insert(
                key.clone(),
                Cached {
                    anchor: Anchor {
                        version: response.version,
                        certificate_der: response.certificate_der.clone(),
                        private_key_der: zeroize::Zeroizing::new(response.private_key_der.clone()),
                    },
                    acquired: tokio::time::Instant::now(),
                },
            );
        }
        use_anchor(&cache.get(&key).expect("inserted above").anchor)
    }

    pub async fn public_anchor(
        &self,
        request: ReleaseSandboxAnchorRequest,
    ) -> Result<ReleaseSandboxAnchorResponse, Status> {
        self.with_anchor(request, |anchor| {
            Ok(ReleaseSandboxAnchorResponse {
                version: anchor.version,
                certificate_der: anchor.certificate_der.clone(),
                private_key_der: Vec::new(),
            })
        })
        .await
    }

    pub async fn issue(
        &self,
        request: IssueIntermediateCertificateRequest,
    ) -> Result<IssueIntermediateCertificateResponse, Status> {
        let csr = crate::intermediate_ca::csr::NodeCsr::parse(&request.csr)
            .map_err(|_| Status::invalid_argument("invalid gateway CSR"))?;
        let public = rcgen::SubjectPublicKeyInfo::from_der(csr.public_key_der())
            .map_err(|_| Status::invalid_argument("invalid gateway public key"))?;
        if request.execution_generation <= 0 {
            return Err(Status::invalid_argument(
                "anchor issuance requires execution generation",
            ));
        }
        self.with_anchor(
            ReleaseSandboxAnchorRequest {
                sandbox_id: request.sandbox_id,
                execution_generation: Some(request.execution_generation),
            },
            |anchor| {
                Ok(IssueIntermediateCertificateResponse {
                    certificate_der: anchor
                        .sign_gateway(&public, time::OffsetDateTime::now_utc())
                        .map_err(|_| Status::internal("anchor signing failed"))?,
                    issuer_certificate_der: anchor.certificate_der.clone(),
                    issuer: GatewayCaIssuer::Anchor as i32,
                })
            },
        )
        .await
    }
}
