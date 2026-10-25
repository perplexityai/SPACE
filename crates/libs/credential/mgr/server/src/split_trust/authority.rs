use super::kms::{HardwareDeriver, HardwareError};
use crate::proto::{IssueIntermediateCertificateRequest, IssueIntermediateCertificateResponse};
use pplx_space_common_core::types::SandboxId;
use pplx_space_credmgr_client::x509::{AnchorCertificate, AnchorKeyId, ScopedIssuer};
use pplx_space_sandbox_anchor::{Anchor, AnchorEnvironment, derive_anchor, sandbox_timestamp_ms};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tonic::Status;

mod seeds;
use seeds::{Seed, SeedKeyring};

#[derive(Debug, thiserror::Error)]
pub enum AuthorityError {
    #[error("hardware derivation: {0}")]
    Hardware(#[from] HardwareError),
    #[error("anchor environment configuration: {0}")]
    Environment(#[from] std::env::VarError),
    #[error("anchor derivation configuration: {0}")]
    Anchor(#[from] pplx_space_sandbox_anchor::AnchorError),
}

#[derive(Clone)]
pub(crate) struct Authority {
    environment: AnchorEnvironment,
    hardware: HardwareDeriver,
    // Never evicted: new sandbox creation must not become a cold KMS operation
    // because historical restores churn the bounded secondary cache.
    active_seed: Seed,
    historical_seeds: SeedKeyring,
    signing: Arc<Semaphore>,
}

impl Authority {
    pub async fn load() -> Result<Self, AuthorityError> {
        let environment: AnchorEnvironment = std::env::var("SPACE_ANCHOR_ENVIRONMENT")?.parse()?;
        let hardware = HardwareDeriver::from_env().await?;
        let active_seed = Arc::new(
            hardware
                .derive(hardware.active_key_id(), environment.seed_input())
                .await?,
        );
        Ok(Self {
            environment,
            hardware,
            active_seed,
            historical_seeds: SeedKeyring::default(),
            signing: Arc::new(Semaphore::new(32)),
        })
    }

    async fn seed(&self, key_id: &AnchorKeyId) -> Result<Seed, Status> {
        if key_id == self.hardware.active_key_id() {
            return Ok(self.active_seed.clone());
        }
        self.historical_seeds
            .get(
                key_id.clone(),
                self.hardware.derive(key_id, self.environment.seed_input()),
            )
            .await
            .map_err(|error| {
                tracing::warn!(%error, %key_id, "historical authority seed unavailable");
                Status::unavailable("historical authority seed unavailable")
            })
    }

    /// Entire operation is bounded to 8s, including admission, an optional
    /// historical seed load and the blocking signing pool. No metadata lookup.
    async fn with_anchor<R: Send + 'static>(
        &self,
        sandbox_id: SandboxId,
        key_id: AnchorKeyId,
        use_anchor: impl FnOnce(Anchor) -> Result<R, Status> + Send + 'static,
    ) -> Result<R, Status> {
        tokio::time::timeout(Duration::from_secs(8), async {
            let id = sandbox_id.as_uuid();
            let timestamp = sandbox_timestamp_ms(id)
                .map_err(|_| Status::invalid_argument("sandbox anchor requires UUIDv7"))?;
            let now = u64::try_from(jiff::Timestamp::now().as_millisecond())
                .map_err(|_| Status::unavailable("invalid system clock"))?;
            if timestamp > now.saturating_add(60_000) {
                return Err(Status::invalid_argument(
                    "sandbox UUID timestamp is in the future",
                ));
            }
            let permit = self
                .signing
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| Status::unavailable("anchor signing is shutting down"))?;
            let seed = self.seed(&key_id).await?;
            tokio::task::spawn_blocking(move || {
                // Cancellation must not release capacity while CPU work runs.
                let _permit = permit;
                let anchor = derive_anchor(id, &seed).map_err(|_| {
                    Status::failed_precondition("sandbox anchor derivation unavailable")
                })?;
                use_anchor(anchor)
            })
            .await
            .map_err(|_| Status::internal("anchor signing task failed"))?
        })
        .await
        .map_err(|_| Status::deadline_exceeded("sandbox anchor operation timed out"))?
    }

    pub async fn public_anchor(
        &self,
        sandbox_id: SandboxId,
        key_id: Option<AnchorKeyId>,
    ) -> Result<AnchorCertificate, Status> {
        // A persisted pin wins. Only new enrollment asks for the active key.
        let key_id = key_id.unwrap_or_else(|| self.hardware.active_key_id().clone());
        self.with_anchor(sandbox_id, key_id.clone(), move |anchor| {
            Ok(AnchorCertificate {
                key_id,
                certificate_der: anchor.certificate_der,
            })
        })
        .await
    }

    pub async fn issue(
        &self,
        request: IssueIntermediateCertificateRequest,
    ) -> Result<IssueIntermediateCertificateResponse, Status> {
        let identity = ScopedIssuer::parse(&request)
            .map_err(|error| Status::invalid_argument(error.to_string()))?
            .ok_or_else(|| Status::invalid_argument("scoped issuer is required"))?;
        let csr = crate::intermediate_ca::csr::NodeCsr::parse(&request.csr)
            .map_err(|_| Status::invalid_argument("invalid gateway CSR"))?;
        let public = rcgen::SubjectPublicKeyInfo::from_der(csr.public_key_der())
            .map_err(|_| Status::invalid_argument("invalid gateway public key"))?;
        self.with_anchor(identity.sandbox_id, identity.key_id, move |anchor| {
            Ok(IssueIntermediateCertificateResponse {
                certificate_der: anchor
                    .sign_gateway(&public, time::OffsetDateTime::now_utc())
                    .map_err(|_| Status::internal("anchor signing failed"))?,
                issuer_certificate_der: anchor.certificate_der,
            })
        })
        .await
    }
}
