//! Bounded, memory-only anchor cache over the X.509 token-exchange profile.
use crate::proto::{
    ExchangeTokenResponse, IssueIntermediateCertificateRequest,
    IssueIntermediateCertificateResponse, exchange_token_response::Payload,
};
use pplx_space_common_core::types::SandboxId;
use pplx_space_credmgr_client::{CredentialManagerGrpcClient, x509::CredentialKind};
use pplx_space_sandbox_anchor::Anchor;
use rcgen::PublicKeyData;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{OnceCell, Semaphore};
use tonic::Status;
use x509_parser::prelude::{FromDer, X509Certificate};

#[cfg(test)]
mod tests;

#[derive(Clone)]
pub(crate) struct NodeAnchors {
    upstream: CredentialManagerGrpcClient,
    cache: Arc<Mutex<HashMap<SandboxId, Arc<Entry>>>>,
    misses: Arc<Semaphore>,
}

struct Entry {
    value: OnceCell<Cached>,
    created: tokio::time::Instant,
}

struct Cached {
    anchor: Anchor,
    acquired: tokio::time::Instant,
}

impl NodeAnchors {
    pub fn new(upstream: CredentialManagerGrpcClient) -> Self {
        Self {
            upstream,
            cache: Arc::default(),
            misses: Arc::new(Semaphore::new(32)),
        }
    }

    fn expire(cache: &mut HashMap<SandboxId, Arc<Entry>>) {
        cache.retain(|_, item| match item.value.get() {
            Some(cached) => cached.acquired.elapsed() < Duration::from_secs(60),
            None => item.created.elapsed() < Duration::from_secs(20),
        });
    }

    fn entry(&self, sandbox_id: SandboxId) -> Result<Arc<Entry>, Status> {
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| Status::internal("anchor cache poisoned"))?;
        Self::expire(&mut cache);
        if let Some(entry) = cache.get(&sandbox_id) {
            return Ok(entry.clone());
        }
        if cache.len() >= 4096 {
            return Err(Status::resource_exhausted("anchor cache capacity exceeded"));
        }
        let entry = Arc::new(Entry {
            value: OnceCell::new(),
            created: tokio::time::Instant::now(),
        });
        cache.insert(sandbox_id, entry.clone());
        Ok(entry)
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
                    () = shutdown.cancelled() => break,
                    _ = interval.tick() => match cache.lock() {
                        Ok(mut cache) => Self::expire(&mut cache),
                        Err(_) => {
                            tracing::error!("anchor cache poisoned; sweeper stopped");
                            break;
                        }
                    },
                }
            }
        }))
    }

    async fn with_anchor<R>(
        &self,
        sandbox_id: SandboxId,
        use_anchor: impl FnOnce(&Anchor) -> Result<R, Status>,
    ) -> Result<R, Status> {
        let operation = async {
            // The map lock never crosses an await. Only requests for this
            // sandbox share initialization; another slow miss cannot block hits.
            let entry = self.entry(sandbox_id)?;
            let cached = entry
                .value
                .get_or_try_init(|| async {
                    let _permit = self
                        .misses
                        .acquire()
                        .await
                        .map_err(|_| Status::unavailable("anchor acquisition closed"))?;
                    let mut response = self
                        .upstream
                        .clone()
                        .exchange_token(CredentialKind::SigningCredentials.request(sandbox_id))
                        .await?
                        .into_inner();
                    if response.issued_token_type != CredentialKind::SigningCredentials.as_str()
                        || response.token_type != "N_A"
                        || response.expires_in.is_some()
                        || response.scope.is_some()
                        || response.refresh_token.is_some()
                    {
                        return Err(Status::failed_precondition(
                            "invalid signing credential profile",
                        ));
                    }
                    let Some(Payload::X509SigningCredentials(credentials)) =
                        response.payload.as_mut()
                    else {
                        return Err(Status::failed_precondition(
                            "authority did not return signing credentials",
                        ));
                    };
                    if credentials.private_key_pkcs8_der.is_empty()
                        || credentials.private_key_pkcs8_der.len() > 1024
                        || credentials.certificate_der.is_empty()
                        || credentials.certificate_der.len() > 4096
                    {
                        return Err(Status::failed_precondition(
                            "authority returned incomplete signing credentials",
                        ));
                    }
                    let pair =
                        rcgen::KeyPair::try_from(credentials.private_key_pkcs8_der.as_slice())
                            .map_err(|_| Status::failed_precondition("invalid anchor key"))?;
                    let (_, certificate) = X509Certificate::from_der(&credentials.certificate_der)
                        .map_err(|_| Status::failed_precondition("invalid anchor certificate"))?;
                    if certificate.public_key().raw != pair.subject_public_key_info() {
                        return Err(Status::failed_precondition(
                            "anchor certificate and key differ",
                        ));
                    }
                    certificate.verify_signature(None).map_err(|_| {
                        Status::failed_precondition("invalid anchor self-signature")
                    })?;
                    Ok(Cached {
                        anchor: Anchor {
                            certificate_der: std::mem::take(&mut credentials.certificate_der),
                            private_key_der: zeroize::Zeroizing::new(std::mem::take(
                                &mut credentials.private_key_pkcs8_der,
                            )),
                        },
                        acquired: tokio::time::Instant::now(),
                    })
                })
                .await?;
            use_anchor(&cached.anchor)
        };
        tokio::time::timeout(Duration::from_secs(20), operation)
            .await
            .map_err(|_| Status::deadline_exceeded("sandbox anchor acquisition timed out"))?
    }

    pub async fn public_anchor(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<ExchangeTokenResponse, Status> {
        self.with_anchor(sandbox_id, |anchor| {
            Ok(pplx_space_credmgr_client::x509::certificate_response(
                anchor.certificate_der.clone(),
            ))
        })
        .await
    }

    pub async fn issue(
        &self,
        request: IssueIntermediateCertificateRequest,
    ) -> Result<IssueIntermediateCertificateResponse, Status> {
        let sandbox_id = pplx_space_credmgr_client::x509::parse_sandbox_id(
            request
                .sandbox_id
                .as_deref()
                .ok_or_else(|| Status::invalid_argument("sandbox identity is required"))?,
        )
        .map_err(|_| Status::invalid_argument("invalid sandbox identity"))?;
        let csr = crate::intermediate_ca::csr::NodeCsr::parse(&request.csr)
            .map_err(|_| Status::invalid_argument("invalid gateway CSR"))?;
        let public = rcgen::SubjectPublicKeyInfo::from_der(csr.public_key_der())
            .map_err(|_| Status::invalid_argument("invalid gateway public key"))?;
        self.with_anchor(sandbox_id, |anchor| {
            Ok(IssueIntermediateCertificateResponse {
                certificate_der: anchor
                    .sign_gateway(&public, time::OffsetDateTime::now_utc())
                    .map_err(|_| Status::internal("anchor signing failed"))?,
                issuer_certificate_der: anchor.certificate_der.clone(),
            })
        })
        .await
    }
}
