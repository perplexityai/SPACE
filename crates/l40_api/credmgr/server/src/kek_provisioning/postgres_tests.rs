use std::sync::atomic::{AtomicUsize, Ordering};

use pplx_space_common_svc::core::types::MasterKeyIdentifier;
use pplx_space_db_test_fixtures::{try_pg_test_db, try_pg_test_db_with_max_connections};
use zeroize::Zeroizing;

use super::*;
use crate::intermediate_key_broker::{
    IssuedIntermediateKey, UnwrappedIntermediateKey, WrappedIntermediateKeyStore,
};
use crate::intermediate_key_store::PgWrappedIntermediateKeyStore;

use pplx_space_test_env::TestResult;

/// Issues deterministic per-call key ids and counts issue calls; unwrap
/// is not exercised by these tests.
struct FakeIssuer {
    issues: AtomicUsize,
    deny: bool,
    /// When set, every issue call blocks here until all expected racers
    /// arrive — forcing them past their initial active-KEK lookup so the
    /// ON CONFLICT / adopt-winner path is genuinely contended.
    barrier: Option<tokio::sync::Barrier>,
}

impl FakeIssuer {
    fn new(deny: bool) -> Self {
        Self {
            issues: AtomicUsize::new(0),
            deny,
            barrier: None,
        }
    }

    fn with_barrier(racers: usize) -> Self {
        Self {
            issues: AtomicUsize::new(0),
            deny: false,
            barrier: Some(tokio::sync::Barrier::new(racers)),
        }
    }
}

#[tonic::async_trait]
impl IntermediateKeyBrokerApi for FakeIssuer {
    async fn issue_intermediate_key(
        &self,
        org: OrgId,
        _requesting_sandbox: Option<SandboxId>,
    ) -> Result<IssuedIntermediateKey, ProviderError> {
        let n = self.issues.fetch_add(1, Ordering::SeqCst);
        if let Some(barrier) = &self.barrier {
            barrier.wait().await;
        }
        if self.deny {
            return Err(ProviderError::AccessRevoked("org offboarded".into()));
        }
        Ok(IssuedIntermediateKey {
            key_id: MasterKeyIdentifier::parse(format!("bk/{}/v{n}", org.as_uuid()))
                .expect("uuid-derived key id is valid"),
            wrapped_key: format!("wrapped:{}:{n}", org.as_uuid()).into_bytes(),
        })
    }

    async fn unwrap_intermediate_key(
        &self,
        _org: OrgId,
        _requesting_sandbox: Option<SandboxId>,
        _key_id: &MasterKeyIdentifier,
        _wrapped_key: &[u8],
    ) -> Result<UnwrappedIntermediateKey, ProviderError> {
        Ok(UnwrappedIntermediateKey {
            key: Zeroizing::new([0u8; 32]),
            max_cache_ttl: std::time::Duration::from_secs(900),
        })
    }
}

async fn seed_org(pool: &sqlx::PgPool) -> TestResult<OrgId> {
    let org = OrgId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug) VALUES ($1, $2, $2)"
    ))
    .bind(org.as_uuid())
    .bind(org.as_uuid().to_string())
    .execute(pool)
    .await?;
    Ok(org)
}

#[tokio::test]
async fn provisions_kek_row_and_readable_blob() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org = seed_org(&db.pool).await?;
    let issuer = Arc::new(FakeIssuer::new(false));
    let provisioner = IntermediateKeyProvisioner::new(db.pool.clone(), issuer.clone());

    let record = provisioner.provision_active_kek(org, None).await?;
    assert_eq!(record.provider, KekProvider::IntermediateKeyBroker);
    assert!(!record.retired);

    // The blob round-trips through the read path the unwrap flow uses.
    let store = PgWrappedIntermediateKeyStore::new(db.pool.clone());
    let blob = store
        .wrapped_key(org, &record.key_identifier)
        .await?
        .expect("blob persisted");
    assert!(blob.starts_with(b"wrapped:"));

    // Cross-org claims read as absent.
    let other = seed_org(&db.pool).await?;
    assert!(
        store
            .wrapped_key(other, &record.key_identifier)
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn second_call_adopts_the_existing_kek_without_issuing() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org = seed_org(&db.pool).await?;
    let issuer = Arc::new(FakeIssuer::new(false));
    let provisioner = IntermediateKeyProvisioner::new(db.pool.clone(), issuer.clone());

    let first = provisioner.provision_active_kek(org, None).await?;
    let second = provisioner.provision_active_kek(org, None).await?;
    assert_eq!(first.cmk_id, second.cmk_id);
    assert_eq!(issuer.issues.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn concurrent_provisioners_converge_on_one_active_kek() -> TestResult {
    // Every racer holds a transaction connection at the same time, so
    // the pool must fit all of them plus slack — the default 2-conn
    // test pool starves the racers under parallel suite load.
    let Some(db) = try_pg_test_db_with_max_connections(10).await? else {
        return Ok(());
    };
    let org = seed_org(&db.pool).await?;
    // The issuer's barrier holds every racer inside its issue call, so
    // all of them necessarily pass the initial active-KEK lookup before
    // any row exists — the insert conflict and adopt-winner paths are
    // genuinely contended, not scheduling-dependent.
    const RACERS: usize = 8;
    let issuer = Arc::new(FakeIssuer::with_barrier(RACERS));
    let provisioner = Arc::new(IntermediateKeyProvisioner::new(
        db.pool.clone(),
        issuer.clone(),
    ));

    let mut handles = Vec::new();
    for _ in 0..RACERS {
        let provisioner = provisioner.clone();
        handles.push(tokio::spawn(async move {
            provisioner.provision_active_kek(org, None).await
        }));
    }
    let mut cmk_ids = Vec::new();
    for handle in handles {
        cmk_ids.push(handle.await??.cmk_id);
    }
    cmk_ids.dedup();
    assert_eq!(cmk_ids.len(), 1, "all racers must adopt one KEK");
    assert_eq!(
        issuer.issues.load(Ordering::SeqCst),
        RACERS,
        "every racer must have issued before any could commit"
    );

    let active: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM org_master_keys WHERE org_id = $1 AND retired_at IS NULL"
    ))
    .bind(org.as_uuid())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(active, 1);
    // Losers' issued keys are orphaned at the broker, never persisted.
    let blobs: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM org_branching_keys WHERE org_id = $1"
    ))
    .bind(org.as_uuid())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(blobs, 1);
    Ok(())
}

#[tokio::test]
async fn issue_denial_provisions_nothing() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org = seed_org(&db.pool).await?;
    let issuer = Arc::new(FakeIssuer::new(true));
    let provisioner = IntermediateKeyProvisioner::new(db.pool.clone(), issuer);

    let err = provisioner
        .provision_active_kek(org, None)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            ProvisionError::Provider(ProviderError::AccessRevoked(_))
        ),
        "got {err:?}"
    );
    let rows: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM org_master_keys WHERE org_id = $1"
    ))
    .bind(org.as_uuid())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(rows, 0, "denied issue must persist nothing");
    Ok(())
}

#[tokio::test]
async fn in_memory_provisioning_is_authority_owned_and_idempotent() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org = seed_org(&db.pool).await?;
    let identifier = MasterKeyIdentifier::parse("local-development-key")?;
    let provisioner = InMemoryKekProvisioner::new(db.pool.clone(), identifier.clone());

    let first = provisioner.provision_active_kek(org, None).await?;
    let second = provisioner.provision_active_kek(org, None).await?;

    assert_eq!(first.cmk_id, second.cmk_id);
    assert_eq!(first.provider, KekProvider::InMemory);
    assert_eq!(first.key_identifier, identifier);
    let rows: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM org_master_keys WHERE org_id = $1 AND retired_at IS NULL"
    ))
    .bind(org.as_uuid())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(rows, 1);
    Ok(())
}
