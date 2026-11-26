//! Persistence of the per-sandbox volume DEK wrap for
//! `GetOrIssueSandboxVolumeDek`.
//!
//! One row per sandbox in `sandbox_volume_encryption`: the wrapped DEK plus
//! the `org_master_keys.id` (cmk_id) of the KEK that wrapped it. Presence of
//! the row IS the "this sandbox has a volume DEK" invariant; the same DEK
//! covers every block volume the sandbox owns (live rootfs COW, suspend
//! snapshot volume). The wrap moved here from the cluster-api sidecar so that
//! the entire KEK→DEK chain lives behind the credential service — callers
//! never see a KEK or a wrap, only the resolved plaintext.

use pplx_space_accounts::OrgId;
use pplx_space_byok_store::VolumeWrapRecord;
use pplx_space_sandbox_model::SandboxId;
use uuid::Uuid;

/// A resolved `sandbox_volume_encryption` row, narrowed to what the unwrap
/// step needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeWrap {
    /// Organization the wrap was minted under. The wrap's AAD binds
    /// (org, sandbox), so a wrap whose org no longer matches the sandbox's
    /// resolved owner can never be unwrapped again — the service detects
    /// that state from this field and refuses with a permanent error
    /// instead of failing deep in KEK resolution.
    pub org_id: OrgId,
    /// `org_master_keys.id` of the KEK that wrapped this DEK. Used to look the
    /// KEK row back up at unwrap time so the caller never names a key.
    pub cmk_id: Uuid,
    /// The DEK wrapped under that KEK. Opaque ciphertext.
    pub wrapped_dek: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum VolumeWrapStoreError {
    #[error("sandbox volume wrap query failed: {0}")]
    Query(#[from] sqlx::Error),
}

impl From<pplx_space_postgres_pool::DbError> for VolumeWrapStoreError {
    fn from(error: pplx_space_postgres_pool::DbError) -> Self {
        Self::from(sqlx::Error::from(error))
    }
}

/// Read and first-writer-wins persistence of per-sandbox volume DEK wraps. A
/// trait so the service handler can be exercised without a live Postgres.
#[tonic::async_trait]
pub trait VolumeWrapStore: Send + Sync + 'static {
    /// The sandbox's stored volume wrap, or `None` when none has been issued
    /// yet.
    async fn get(&self, sandbox_id: SandboxId) -> Result<Option<VolumeWrap>, VolumeWrapStoreError>;

    /// Persist a freshly issued wrap, first-writer-wins, and return the
    /// canonical stored row. If a concurrent caller already inserted one, the
    /// supplied values are discarded and the existing row is returned — so the
    /// loser of a race unwraps the winner's DEK and every volume for the
    /// sandbox shares one DEK.
    async fn insert_if_absent(
        &self,
        sandbox_id: SandboxId,
        org_id: OrgId,
        cmk_id: Uuid,
        wrapped_dek: &[u8],
    ) -> Result<VolumeWrap, VolumeWrapStoreError>;
}

pub struct PgVolumeWrapStore {
    pool: sqlx::PgPool,
}

impl PgVolumeWrapStore {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[tonic::async_trait]
impl VolumeWrapStore for PgVolumeWrapStore {
    async fn get(&self, sandbox_id: SandboxId) -> Result<Option<VolumeWrap>, VolumeWrapStoreError> {
        Ok(pplx_space_byok_store::volume_wrap(&self.pool, sandbox_id)
            .await?
            .map(VolumeWrap::from))
    }

    async fn insert_if_absent(
        &self,
        sandbox_id: SandboxId,
        org_id: OrgId,
        cmk_id: Uuid,
        wrapped_dek: &[u8],
    ) -> Result<VolumeWrap, VolumeWrapStoreError> {
        // First-writer-wins: the unique constraint on sandbox_id makes the
        // insert a no-op for a concurrent loser, after which the SELECT reads
        // back whichever row won. The two statements run in one transaction so
        // the read cannot observe a state between a different writer's insert
        // and its commit.
        let mut tx = self.pool.begin().await?;
        pplx_space_byok_store::insert_volume_wrap(
            &mut *tx,
            sandbox_id,
            org_id,
            cmk_id,
            wrapped_dek,
        )
        .await?;
        let stored = pplx_space_byok_store::volume_wrap(&mut *tx, sandbox_id)
            .await?
            .ok_or(sqlx::Error::RowNotFound)?;
        tx.commit().await?;
        Ok(stored.into())
    }
}

impl From<VolumeWrapRecord> for VolumeWrap {
    fn from(record: VolumeWrapRecord) -> Self {
        Self {
            org_id: record.org_id,
            cmk_id: record.master_key_id,
            wrapped_dek: record.wrapped_dek,
        }
    }
}
