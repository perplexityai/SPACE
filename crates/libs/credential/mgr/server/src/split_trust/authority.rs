use super::kms::{HardwareDeriver, HardwareError};
use crate::proto::X509SigningCredentials;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::Api;
use pplx_space_common_core::types::SandboxId;
use pplx_space_sandbox_anchor::{EpochCatalog, derive_anchor, sandbox_timestamp_ms};
use std::{sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tonic::Status;

pub(crate) const CATALOG: &str = "space-anchor-epochs";

#[derive(Debug, thiserror::Error)]
pub enum AuthorityError {
    #[error("hardware derivation: {0}")]
    Hardware(#[from] HardwareError),
    #[error("anchor namespace configuration: {0}")]
    Environment(#[from] std::env::VarError),
    #[error("anchor Kubernetes operation: {0}")]
    Kubernetes(#[from] kube::Error),
    #[error("anchor Kubernetes configuration: {0}")]
    KubeConfig(#[from] kube::config::InferConfigError),
    #[error("anchor catalog: {0}")]
    Catalog(#[from] pplx_space_sandbox_anchor::AnchorError),
    #[error("missing epoch catalog; provision or restore the explicit catalog first")]
    MissingCatalog,
    #[error("anchor catalog lookup timed out")]
    Timeout,
}

#[derive(Clone)]
pub(crate) struct Authority {
    catalog: Arc<RwLock<EpochCatalog>>,
    api: Api<ConfigMap>,
    hardware: HardwareDeriver,
}

impl Authority {
    pub async fn load() -> Result<Self, AuthorityError> {
        let namespace = std::env::var("SPACE_ANCHOR_NAMESPACE")?;
        let kube = pplx_space_common_svc::kube_client::traced_kube_client_from_env().await?;
        let api = Api::namespaced(kube, &namespace);
        let catalog = Self::read_catalog(&api).await?;
        let hardware = HardwareDeriver::from_env().await?;
        hardware.validate_catalog(&catalog)?;
        Ok(Self {
            catalog: Arc::new(RwLock::new(catalog)),
            api,
            hardware,
        })
    }

    async fn read_catalog(api: &Api<ConfigMap>) -> Result<EpochCatalog, AuthorityError> {
        let map = tokio::time::timeout(Duration::from_secs(2), api.get(CATALOG))
            .await
            .map_err(|_| AuthorityError::Timeout)??;
        let json = map
            .data
            .as_ref()
            .and_then(|data| data.get("epochs.json"))
            .ok_or(AuthorityError::MissingCatalog)?;
        Ok(EpochCatalog::decode(json.as_bytes())?)
    }

    async fn current_catalog(&self) -> Result<EpochCatalog, AuthorityError> {
        // A fresh read on a node-cache miss avoids stale epoch selection after
        // rotation. There is no cloud backup, master creation, or mutation here.
        let current = Self::read_catalog(&self.api).await?;
        self.hardware.validate_catalog(&current)?;
        let mut loaded = self.catalog.write().await;
        // Concurrent reads can finish out of order. An older, compatible prefix
        // must not roll back the in-memory view already adopted by another call.
        if current.epochs().len() < loaded.epochs().len() {
            current.validate_successor(&loaded)?;
            return Ok(loaded.clone());
        }
        loaded.validate_successor(&current)?;
        *loaded = current.clone();
        Ok(current)
    }

    /// Node cache misses have a 2s catalog budget and at most two 1s hardware
    /// attempts. No master key or epoch is generated as a side effect.
    pub async fn credentials(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<X509SigningCredentials, Status> {
        let id = sandbox_id.as_uuid();
        let timestamp = sandbox_timestamp_ms(id)
            .map_err(|_| Status::invalid_argument("sandbox anchor requires UUIDv7"))?;
        let now_ms = u64::try_from(jiff::Timestamp::now().as_millisecond())
            .map_err(|_| Status::unavailable("system clock is outside the sandbox UUID epoch"))?;
        if timestamp > now_ms.saturating_add(60_000) {
            return Err(Status::invalid_argument(
                "sandbox UUID timestamp is in the future",
            ));
        }
        let catalog = self.current_catalog().await.map_err(|error| {
            tracing::warn!(%error, "sandbox anchor catalog unavailable");
            Status::unavailable("anchor catalog unavailable")
        })?;
        let epoch = catalog
            .epoch(id)
            .map_err(|_| Status::failed_precondition("sandbox anchor epoch unavailable"))?;
        let input = catalog
            .derivation_input(id)
            .map_err(|_| Status::failed_precondition("sandbox anchor input unavailable"))?;
        let material = self
            .hardware
            .derive(&epoch.key_id, input)
            .await
            .map_err(|error| {
                tracing::warn!(%error, "sandbox hardware derivation unavailable");
                Status::unavailable("sandbox hardware derivation unavailable")
            })?;
        let anchor = tokio::task::spawn_blocking(move || derive_anchor(&catalog, id, &material))
            .await
            .map_err(|_| Status::internal("anchor derivation task failed"))?
            .map_err(|_| Status::failed_precondition("sandbox anchor derivation unavailable"))?;
        Ok(X509SigningCredentials {
            certificate_der: anchor.certificate_der,
            private_key_pkcs8_der: anchor.private_key_der.to_vec(),
        })
    }
}
