use super::backup::{Backup, BackupError};
use crate::{
    auth::K8sNodeIdentity,
    proto::{ReleaseSandboxAnchorRequest, ReleaseSandboxAnchorResponse},
};
use k8s_openapi::{
    ByteString,
    api::core::v1::{Pod, Secret},
};
use kube::{Api, api::PostParams};
use pplx_space_common_core::types::SandboxId;
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
    #[error("anchor retirement inventory: {0}")]
    Inventory(#[from] pplx_space_sandbox_store::SandboxStoreError),
    #[error(
        "anchor rotation interval and activation delay must be positive; delay is at least 120 seconds"
    )]
    InvalidRotation,
}

#[derive(Clone)]
pub(crate) struct Authority {
    ring: Arc<RwLock<Keyring>>,
    api: Api<Secret>,
    kube: kube::Client,
    backup: Backup,
    reconcile_lock: Arc<tokio::sync::Mutex<()>>,
    pool: sqlx::PgPool,
    rotation_ms: u64,
    activation_ms: u64,
}

impl Authority {
    pub async fn load(pool: sqlx::PgPool) -> Result<Self, AuthorityError> {
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
        let api: Api<Secret> = Api::namespaced(kube.clone(), &namespace);
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
            kube,
            backup,
            reconcile_lock: Arc::default(),
            pool,
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
        let oldest =
            pplx_space_sandbox_store::anchor_placement::oldest_identity_ms(&self.pool).await?;
        let mut next = current.retire_before(oldest);
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
        pool: &sqlx::PgPool,
        node: &K8sNodeIdentity,
        request: &ReleaseSandboxAnchorRequest,
    ) -> Result<ReleaseSandboxAnchorResponse, Status> {
        // Refresh before release as well as periodically. A stale replica must
        // never issue from an obsolete seed-selection view.
        self.reconcile()
            .await
            .map_err(|_| Status::unavailable("anchor keyring refresh failed"))?;
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
        let id = SandboxId::from_uuid(uuid);
        let placement = self
            .placement(pool, id, node, request.execution_generation)
            .await?;
        let anchor =
            self.ring.read().await.derive(uuid).map_err(|_| {
                Status::failed_precondition("sandbox anchor derivation unavailable")
            })?;
        if self
            .placement(pool, id, node, request.execution_generation)
            .await?
            != placement
        {
            return Err(Status::permission_denied(
                "sandbox placement changed during key release",
            ));
        }
        Ok(ReleaseSandboxAnchorResponse {
            version: anchor.version,
            certificate_der: anchor.certificate_der,
            private_key_der: anchor.private_key_der.to_vec(),
        })
    }

    async fn placement(
        &self,
        pool: &sqlx::PgPool,
        id: SandboxId,
        node: &K8sNodeIdentity,
        generation: Option<i64>,
    ) -> Result<Binding, Status> {
        use pplx_space_sandbox_store::anchor_placement as store;
        if let Some(active) = store::load(pool, id)
            .await
            .map_err(|_| Status::unavailable("sandbox placement lookup failed"))?
        {
            if active.node_name == node.node_name().as_str()
                && active.node_uid == node.node_uid().as_str()
                && active.generation == generation
            {
                return Ok(Binding::Active(active));
            }
            return Err(Status::permission_denied(
                "sandbox is not assigned to this node execution",
            ));
        }
        let reservation = store::load_reservation(pool, id)
            .await
            .map_err(|_| Status::unavailable("warm-pool placement lookup failed"))?
            .ok_or_else(|| Status::permission_denied("sandbox has no warm-pool reservation"))?;
        let pods: Api<Pod> = Api::namespaced(self.kube.clone(), &reservation.pod_namespace);
        let pod = pods
            .get(&reservation.pod_name)
            .await
            .map_err(|_| Status::unavailable("warm-pool pod lookup failed"))?;
        let uid = pod
            .metadata
            .uid
            .as_deref()
            .ok_or_else(|| Status::permission_denied("warm-pool pod has no UID"))?;
        let label = pod.metadata.annotations.as_ref().and_then(|labels| {
            labels.get(pplx_space_common_core::kubernetes::SANDBOX_POOL_SLOT_ID_KEY)
        });
        let pod_generation = pod
            .metadata
            .annotations
            .as_ref()
            .and_then(|annotations| {
                annotations.get(
                    pplx_space_common_core::kubernetes::SANDBOX_EXECUTION_GENERATION_ANNOTATION,
                )
            })
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|value| *value > 0)
            .ok_or_else(|| {
                Status::permission_denied("warm-pool pod has no execution generation")
            })?;
        if pod.metadata.deletion_timestamp.is_some()
            || generation.is_some_and(|requested| requested != pod_generation)
            || pod.spec.as_ref().and_then(|spec| spec.node_name.as_deref())
                != Some(node.node_name().as_str())
            || reservation
                .pod_uid
                .as_deref()
                .is_some_and(|expected| expected != uid)
            || label.map(String::as_str) != Some(reservation.slot_id.to_string().as_str())
            || !store::node_is_current(pool, node.node_name().as_str(), node.node_uid().as_str())
                .await
                .map_err(|_| Status::unavailable("warm-pool node identity lookup failed"))?
        {
            return Err(Status::permission_denied(
                "warm-pool pod is not assigned to this node",
            ));
        }
        Ok(Binding::Warm(reservation, uid.to_owned(), pod_generation))
    }
}

#[derive(PartialEq, Eq)]
enum Binding {
    Active(pplx_space_sandbox_store::anchor_placement::AnchorPlacement),
    Warm(
        pplx_space_sandbox_store::anchor_placement::WarmAnchorReservation,
        String,
        i64,
    ),
}
