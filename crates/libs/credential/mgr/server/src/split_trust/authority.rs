use super::backup::{Backup, BackupError};
use crate::proto::{ReleaseSandboxAnchorRequest, ReleaseSandboxAnchorResponse};
use k8s_openapi::{ByteString, api::core::v1::Secret};
use kube::{Api, api::PostParams};
use pplx_space_sandbox_anchor::{Keyring, SeedVersion};
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::Status;
use zeroize::Zeroizing;

const SECRET: &str = "space-anchor-keyring";

#[derive(Debug, thiserror::Error)]
pub enum AuthorityError {
    #[error("anchor backup: {0}")]
    Backup(#[from] BackupError),
    #[error("anchor namespace configuration: {0}")]
    Environment(#[from] std::env::VarError),
    #[error("anchor Kubernetes operation: {0}")]
    Kubernetes(#[from] kube::Error),
    #[error("anchor Kubernetes configuration: {0}")]
    KubeConfig(#[from] kube::config::InferConfigError),
    #[error("anchor keyring: {0}")]
    Keyring(#[from] pplx_space_sandbox_anchor::AnchorError),
    #[error("missing anchor keyring; explicit bootstrap or backup recovery required")]
    MissingKeyring,
    #[error("anchor seed RNG failed")]
    Random,
    #[error("system clock cannot be represented as unsigned epoch milliseconds")]
    InvalidClock,
    #[error(
        "anchor rotation interval and activation delay must be positive; delay is at least 120 seconds"
    )]
    InvalidRotation,
}

#[derive(Clone)]
pub(crate) struct Authority {
    ring: Arc<RwLock<Keyring>>,
    api: Api<Secret>,
    backup: Backup,
    reconcile_lock: Arc<tokio::sync::Mutex<()>>,
    rotation_ms: u64,
    activation_ms: u64,
}

impl Authority {
    pub async fn load() -> Result<Self, AuthorityError> {
        fn seconds(name: &str, default: u64) -> Result<u64, AuthorityError> {
            std::env::var(name).map_or(Ok(default), |value| {
                value.parse().map_err(|_| AuthorityError::InvalidRotation)
            })
        }
        let rotation = seconds("SPACE_ANCHOR_ROTATION_INTERVAL_SECONDS", 30 * 24 * 60 * 60)?;
        let activation = seconds("SPACE_ANCHOR_ACTIVATION_DELAY_SECONDS", 60 * 60)?;
        if activation < 120 || rotation < activation || rotation > 365 * 24 * 60 * 60 {
            return Err(AuthorityError::InvalidRotation);
        }
        let namespace = std::env::var("SPACE_ANCHOR_NAMESPACE")?;
        let kube = pplx_space_common_svc::kube_client::traced_kube_client_from_env().await?;
        let api: Api<Secret> = Api::namespaced(kube, &namespace);
        let backup = Backup::from_env().await?;
        let secret = match api.get_opt(SECRET).await? {
            Some(secret) => secret,
            None => {
                // A missing existing keyring is never permission to generate
                // another. Bootstrap is an explicit empty-deployment action.
                let recovery = std::env::var("SPACE_ANCHOR_RECOVER_OBJECT").ok();
                if recovery.is_none()
                    && (std::env::var("SPACE_ANCHOR_BOOTSTRAP").as_deref() != Ok("true")
                        || backup.has_existing_backup().await?)
                {
                    return Err(AuthorityError::MissingKeyring);
                }
                use aws_lc_rs::rand::{SecureRandom, SystemRandom};
                let mut seed = Zeroizing::new([0u8; 32]);
                SystemRandom::new()
                    .fill(seed.as_mut())
                    .map_err(|_| AuthorityError::Random)?;
                let ring = if let Some(object) = recovery.as_deref() {
                    backup.recover(object).await?
                } else {
                    Keyring::new(vec![SeedVersion::new(1, 0, *seed)])?
                };
                let object = backup.persist(&ring).await?;
                tracing::info!(version = ring.versions().last().map(|version| version.0), backup_object = %object, "sandbox anchor keyring backed up before bootstrap");
                let desired = Secret {
                    metadata: kube::api::ObjectMeta {
                        name: Some(SECRET.into()),
                        annotations: Some({
                            let mut annotations = std::collections::BTreeMap::from([
                                ("space.pplx.net/anchor-backup-object".into(), object.clone()),
                                ("space.pplx.net/anchor-keyring-hash".into(), ring.hash()?),
                            ]);
                            if let Some(recovery) = recovery {
                                annotations.insert(
                                    "space.pplx.net/anchor-recovered-from".into(),
                                    recovery,
                                );
                            }
                            annotations
                        }),
                        ..Default::default()
                    },
                    data: Some(
                        [("keyring.json".into(), ByteString(ring.encode()?.to_vec()))].into(),
                    ),
                    ..Default::default()
                };
                match api.create(&PostParams::default(), &desired).await {
                    Ok(secret) => secret,
                    Err(kube::Error::Api(error)) if error.code == 409 => api.get(SECRET).await?,
                    Err(error) => return Err(error.into()),
                }
            }
        };
        let bytes = secret
            .data
            .as_ref()
            .and_then(|data| data.get("keyring.json"))
            .ok_or(AuthorityError::MissingKeyring)?;
        let ring = Keyring::decode(&bytes.0)?;
        backup.persist(&ring).await?;
        Ok(Self {
            ring: Arc::new(RwLock::new(ring)),
            api,
            backup,
            reconcile_lock: Arc::default(),
            rotation_ms: rotation * 1000,
            activation_ms: activation * 1000,
        })
    }

    pub fn start_reconciler(
        &self,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> tokio_util::task::AbortOnDropHandle<()> {
        let authority = self.clone();
        tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = interval.tick() => {
                        if let Err(error) = authority.reconcile().await {
                            tracing::error!(%error, "sandbox anchor keyring reconcile failed");
                        }
                    }
                }
            }
        }))
    }

    async fn reconcile(&self) -> Result<(), AuthorityError> {
        let _guard = self.reconcile_lock.lock().await;
        let mut secret = self.api.get(SECRET).await?;
        let bytes = secret
            .data
            .as_ref()
            .and_then(|data| data.get("keyring.json"))
            .ok_or(AuthorityError::MissingKeyring)?;
        let current = Keyring::decode(&bytes.0)?;
        {
            let mut loaded = self.ring.write().await;
            loaded.validate_successor(&current)?;
            *loaded = current.clone();
        }
        let now = u64::try_from(time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000)
            .map_err(|_| AuthorityError::InvalidClock)?;
        let tail = current
            .versions()
            .last()
            .ok_or(AuthorityError::MissingKeyring)?;
        // A one-hour activation delay leaves time for replica propagation.
        // Version 1 covers pre-bootstrap identities; rotation stages version 2.
        // No lifecycle inventory is consulted by this prototype. Retain every
        // seed rather than infer that an identity can never be restored.
        let mut next = current.clone();
        if now.saturating_sub(tail.1) >= self.rotation_ms {
            next = next.rotate(now, self.activation_ms)?;
        }
        let hash = next.hash()?;
        let tail_version = next
            .versions()
            .last()
            .ok_or(AuthorityError::MissingKeyring)?
            .0;
        let expected_object = format!("sandbox-anchor/v{tail_version}/{hash}.kms");
        let published = secret
            .metadata
            .annotations
            .as_ref()
            .is_some_and(|annotations| {
                annotations.get("space.pplx.net/anchor-keyring-hash") == Some(&hash)
                    && annotations.get("space.pplx.net/anchor-backup-object")
                        == Some(&expected_object)
            });
        if hash == current.hash()? && published {
            return Ok(());
        }
        let object = self.backup.persist(&next).await?;
        secret.data = Some([("keyring.json".into(), ByteString(next.encode()?.to_vec()))].into());
        let annotations = secret.metadata.annotations.get_or_insert_default();
        annotations.insert("space.pplx.net/anchor-backup-object".into(), object.clone());
        annotations.insert("space.pplx.net/anchor-keyring-hash".into(), next.hash()?);
        // resourceVersion from GET is the CAS token. Concurrent writers lose
        // with 409 and adopt the winner on their next reconcile.
        match self
            .api
            .replace(SECRET, &PostParams::default(), &secret)
            .await
        {
            Ok(_) => {
                *self.ring.write().await = next;
                tracing::info!(backup_object = %object, latest_version = tail_version, keyring_hash = %hash, "sandbox anchor keyring update committed");
            }
            Err(kube::Error::Api(error)) if error.code == 409 => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    pub async fn release(
        &self,
        request: &ReleaseSandboxAnchorRequest,
    ) -> Result<ReleaseSandboxAnchorResponse, Status> {
        let uuid = uuid::Uuid::parse_str(&request.sandbox_id)
            .map_err(|_| Status::invalid_argument("invalid sandbox UUID"))?;
        let timestamp = pplx_space_sandbox_anchor::sandbox_timestamp_ms(uuid)
            .map_err(|_| Status::invalid_argument("sandbox anchor requires UUIDv7"))?;
        let now_ms =
            u64::try_from(time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000)
                .map_err(|_| {
                    Status::unavailable("system clock is outside the sandbox UUID epoch")
                })?;
        if timestamp > now_ms.saturating_add(60_000) {
            return Err(Status::invalid_argument(
                "sandbox UUID timestamp is in the future",
            ));
        }
        // Refresh before release as well as periodically. A stale replica must
        // never issue from an obsolete seed-selection view.
        self.reconcile()
            .await
            .map_err(|_| Status::unavailable("anchor keyring refresh failed"))?;
        // The service handler applies the existing credential-agent RPC
        // authentication/allow-list. Sandbox placement is intentionally not
        // authorized here; generation is only a node-local cache identity.
        let anchor =
            self.ring.read().await.derive(uuid).map_err(|_| {
                Status::failed_precondition("sandbox anchor derivation unavailable")
            })?;
        Ok(ReleaseSandboxAnchorResponse {
            version: anchor.version,
            certificate_der: anchor.certificate_der,
            private_key_der: anchor.private_key_der.to_vec(),
        })
    }
}
