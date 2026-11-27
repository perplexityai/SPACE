//! The template driver against a real Postgres, the compiled catalog's
//! initial environment (E0) and its certified upgrade target (E1), and an
//! in-memory object store.

use pplx_space_db_test_fixtures::{TestDb, try_pg_test_db_with_max_connections};
use pplx_space_execution_catalog::catalog::initial_environment_reference;
use pplx_space_postgres_pool::Db;
use pplx_space_runtime_environment_policy_store::{PolicyChange, PolicyChangeReason, set_default};
use pplx_space_runtime_upgrade_campaign_store::test_support::{CampaignSeed, insert_campaign};
use pplx_space_runtime_upgrade_campaign_store::{
    self as store, CampaignId, CampaignPhase, ItemCounts, PhaseChange,
};
use pplx_space_template_manifest::TemplateMetadata;
use pplx_space_template_release_store::test_support::MemoryReleaseObjects;
use pplx_space_test_env::TestResult;

use super::super::{Drivers, Reconciler, ReconcilerConfig};
use super::*;

const BUCKET: &str = "templates-bucket";

/// The compiled catalog's initial environment and the target of its
/// certified upgrade edge.
fn environments() -> (
    Vec<CompiledEnvironment>,
    EnvironmentReference,
    EnvironmentReference,
) {
    let catalog = CompiledEnvironment::cached_for_architecture(Arch::X86_64)
        .expect("compiled catalog")
        .to_vec();
    let e0 = initial_environment_reference();
    let e1 = *catalog
        .iter()
        .find(|entry| entry.definition().reference() == e0)
        .expect("E0 is compiled")
        .upgrade_targets()
        .first()
        .expect("E0 has a certified upgrade target");
    (catalog, e0, e1)
}

fn requirements(
    catalog: &[CompiledEnvironment],
    environment: EnvironmentReference,
) -> ExecutionMetadata {
    catalog
        .iter()
        .find(|entry| entry.definition().reference() == environment)
        .expect("compiled")
        .definition()
        .execution_requirements()
        .expect("requirements")
}

struct Fixture {
    db: TestDb,
    objects: Arc<MemoryReleaseObjects>,
    catalog: Vec<CompiledEnvironment>,
    e0: EnvironmentReference,
    e1: EnvironmentReference,
}

impl Fixture {
    async fn new() -> TestResult<Option<Self>> {
        let Some(db) = try_pg_test_db_with_max_connections(8).await? else {
            return Ok(None);
        };
        // Twins need the `(template_name, hash)` index gone, as the contract
        // migration leaves the schema; a test that needs it recreates it.
        sqlx::query("DROP INDEX IF EXISTS idx_template_releases_template_name_hash_unique")
            .execute(&db.pool)
            .await?;
        // A worker that reads twin metadata, without which every campaign
        // holds (`nodes_dont_read_twins`).
        let worker = pplx_space_accounts::NodeId::new();
        pplx_space_db_test_fixtures::insert_ready_worker_node(&db.pool, worker).await?;
        let (catalog, e0, e1) = environments();
        worker_snapshot(
            &db.pool,
            worker,
            &[e0, e1],
            r#"{"reachable": true, "reads_twin_metadata": true}"#,
        )
        .await?;
        Ok(Some(Self {
            db,
            objects: Arc::new(MemoryReleaseObjects::default()),
            catalog,
            e0,
            e1,
        }))
    }

    fn pool(&self) -> &PgPool {
        &self.db.pool
    }

    fn driver(&self) -> TemplateDriver {
        TemplateDriver::for_test(
            self.pool().clone(),
            Arc::clone(&self.objects) as Arc<dyn ReleaseObjects>,
            self.catalog.clone(),
        )
    }

    fn reconciler(&self) -> Reconciler {
        self.reconciler_with(Arc::clone(&self.objects) as Arc<dyn ReleaseObjects>)
    }

    /// A reconciler whose driver goes through `objects`.
    fn reconciler_with(&self, objects: Arc<dyn ReleaseObjects>) -> Reconciler {
        Reconciler::new(
            self.pool().clone(),
            Drivers::default().with(TemplateDriver::for_test(
                self.pool().clone(),
                objects,
                self.catalog.clone(),
            )),
            ReconcilerConfig {
                enabled: true,
                ..ReconcilerConfig::default()
            },
        )
    }

    async fn set_default(&self, environment: EnvironmentReference) -> TestResult {
        let mut tx = self.pool().begin().await?;
        set_default(
            &mut *tx,
            environment,
            &PolicyChange {
                actor: None,
                reason: PolicyChangeReason::new("test").expect("reason"),
            },
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn template(&self, name: &str) -> TestResult {
        sqlx::query("INSERT INTO templates (id, name) VALUES ($1, $2)")
            .bind(Uuid::now_v7())
            .bind(name)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    /// A release of `name` at a hash made from `byte`, bound to E0, with a
    /// storage location and its `metadata.json`, made from `base` when given.
    async fn release(
        &self,
        name: &str,
        byte: u8,
        base: Option<(&str, &str)>,
    ) -> TestResult<(Uuid, String)> {
        let hash = format!("sha256:{}", format!("{byte:02x}").repeat(32));
        self.release_at(name, hash, base).await
    }

    /// [`Self::release`] at `hash` as spelled, bare or `sha256:`.
    async fn release_at(
        &self,
        name: &str,
        hash: String,
        base: Option<(&str, &str)>,
    ) -> TestResult<(Uuid, String)> {
        let execution = requirements(&self.catalog, self.e0);
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO template_releases (template_name, type, hash, execution_metadata, created_at)
             VALUES ($1, 'fc', $2, $3, clock_timestamp())
             RETURNING id",
        )
        .bind(name)
        .bind(&hash)
        .bind(serde_json::to_value(&execution)?)
        .fetch_one(self.pool())
        .await?;
        let prefix = format!("space/templates/{name}/{hash}");
        sqlx::query(
            "INSERT INTO template_release_storage_locations
                 (template_release_id, storage_connection_string)
             VALUES ($1, $2)",
        )
        .bind(id)
        .bind(format!("s3://{BUCKET}/{prefix}?region=us-east-1"))
        .execute(self.pool())
        .await?;
        let mut document = TemplateMetadata {
            id: name.to_owned(),
            name: name.to_owned(),
            base: base.map_or_else(|| "scratch".to_owned(), |(base, _)| base.to_owned()),
            base_resolved: base.map(|(base, hash)| format!("{base}@{hash}")),
            ..TemplateMetadata::default()
        };
        document.set_execution_metadata(execution.clone());
        // Every pull reads the build's manifest.json too.
        let mut manifest = pplx_space_template_manifest::TemplateManifest::default();
        manifest.set_execution_metadata(execution);
        self.objects.put(
            BUCKET,
            &format!("{prefix}/manifest.json"),
            serde_json::to_vec(&manifest)?,
        );
        // A field this build doesn't model, as a newer builder might write.
        let mut document = serde_json::to_value(&document)?;
        document["carried_over"] = serde_json::json!({"by": "a newer builder"});
        self.objects.put(
            BUCKET,
            &format!("{prefix}/metadata.json"),
            serde_json::to_vec(&document)?,
        );
        Ok((id, hash))
    }

    async fn manual_pool(&self, release: Uuid) -> TestResult {
        sqlx::query(
            "INSERT INTO kubernetes_pool_configs (
                 template_release_id, vcpu, memory_mib, target_ready_slots, refill_per_tick,
                 max_warming_slots, namespace, enabled, managed_by
             )
             VALUES ($1, 2, 1024, 2, 1, 1, 'space', TRUE, 'manual')",
        )
        .bind(release)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn campaign(&self, phase: CampaignPhase, per_cluster: i64) -> TestResult<CampaignId> {
        Ok(insert_campaign(
            self.pool(),
            CampaignSeed {
                from: self.e0,
                to: self.e1,
                kind: CampaignKind::Templates,
                phase,
                per_node: 1,
                per_cluster,
                failure_budget: 2,
                include_running: false,
            },
        )
        .await?)
    }

    /// The twin of `name` at `hash` bound to E1, with its created_at.
    async fn twin(
        &self,
        name: &str,
        hash: &str,
    ) -> TestResult<Option<(Uuid, time::OffsetDateTime)>> {
        Ok(sqlx::query_as(
            "SELECT id, created_at FROM template_releases
             WHERE template_name = $1 AND hash = $2
               AND execution_metadata->>'environment' = $3",
        )
        .bind(name)
        .bind(hash)
        .bind(self.e1.to_string())
        .fetch_optional(self.pool())
        .await?)
    }

    async fn phase(&self, campaign: CampaignId) -> TestResult<String> {
        Ok(
            sqlx::query_scalar("SELECT phase FROM runtime_upgrade_campaigns WHERE id = $1")
                .bind(campaign)
                .fetch_one(self.pool())
                .await?,
        )
    }

    async fn counts(&self, campaign: CampaignId) -> TestResult<ItemCounts> {
        Ok(store::item_counts(self.pool(), campaign).await?)
    }

    /// Ticks until every dispatch has finished.
    async fn tick(&self, reconciler: &Reconciler) {
        reconciler.tick().await;
        reconciler.settle().await;
    }
}

/// What [`HookedObjects`] does on a twin-metadata write of its template's
/// build.
enum OnWrite {
    /// Mark the template deleting, then write.
    MarkDeleting,
    /// Move the cluster default to this environment, then fail the write.
    MoveDefaultAndFail(EnvironmentReference),
    /// Register a worker that reports no twin support, then write.
    AddWorkerThatDoesntReadTwins,
}

/// The fixture's objects, with a race injected into the twin-metadata writes
/// of one template's build: the step between a twin's preparation and its
/// commit.
struct HookedObjects {
    inner: Arc<MemoryReleaseObjects>,
    pool: PgPool,
    template: &'static str,
    on_write: OnWrite,
}

#[async_trait]
impl ReleaseObjects for HookedObjects {
    async fn read_metadata(
        &self,
        target: &TwinMetadataTarget,
    ) -> Result<Vec<u8>, pplx_space_template_release_store::ObjectAccessError> {
        self.inner.read_metadata(target).await
    }

    async fn read_manifest(
        &self,
        target: &TwinMetadataTarget,
    ) -> Result<Vec<u8>, pplx_space_template_release_store::ObjectAccessError> {
        self.inner.read_manifest(target).await
    }

    async fn write_twin_metadata(
        &self,
        target: &TwinMetadataTarget,
        environment: EnvironmentReference,
        document: bytes::Bytes,
    ) -> Result<(), pplx_space_template_release_store::ObjectAccessError> {
        if !target
            .shared_metadata_key()
            .contains(&format!("/templates/{}/", self.template))
        {
            return self
                .inner
                .write_twin_metadata(target, environment, document)
                .await;
        }
        match self.on_write {
            OnWrite::MarkDeleting => {
                sqlx::query("UPDATE templates SET desired_state = 'deleting' WHERE name = $1")
                    .bind(self.template)
                    .execute(&self.pool)
                    .await
                    .expect("mark the template deleting");
                self.inner
                    .write_twin_metadata(target, environment, document)
                    .await
            }
            OnWrite::MoveDefaultAndFail(to) => {
                let mut tx = self.pool.begin().await.expect("begin");
                set_default(
                    &mut *tx,
                    to,
                    &PolicyChange {
                        actor: None,
                        reason: PolicyChangeReason::new("roll back").expect("reason"),
                    },
                )
                .await
                .expect("move the default");
                tx.commit().await.expect("commit");
                Err(
                    pplx_space_template_release_store::ObjectAccessError::NotReleaseDirectory {
                        bucket: target.bucket().to_owned(),
                    },
                )
            }
            OnWrite::AddWorkerThatDoesntReadTwins => {
                pplx_space_db_test_fixtures::insert_ready_worker_node(
                    &self.pool,
                    pplx_space_accounts::NodeId::new(),
                )
                .await
                .expect("register a worker");
                self.inner
                    .write_twin_metadata(target, environment, document)
                    .await
            }
        }
    }
}

mod commit;
mod lineage;
mod storage;

/// Through the loop, a campaign gives each E0 release an E1 twin: the canary
/// moves one release and waits for an operator; resumed, it rolls through the
/// rest and completes. Each twin has its own E1 metadata.json, shares its
/// original's storage location, and has copies of its manual pools.
#[tokio::test(flavor = "multi_thread")]
async fn a_campaign_twins_every_release_canary_first() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("seed").await?;
    let mut releases = Vec::new();
    for byte in 1..=3u8 {
        let release = fixture.release("seed", byte, None).await?;
        fixture.manual_pool(release.0).await?;
        releases.push(release);
    }
    let campaign = fixture.campaign(CampaignPhase::Canary, 2).await?;
    let reconciler = fixture.reconciler();

    // Held until E1 is the cluster default.
    fixture.tick(&reconciler).await;
    assert_eq!(fixture.counts(campaign).await?, ItemCounts::default());

    fixture.set_default(fixture.e1).await?;
    for _ in 0..3 {
        fixture.tick(&reconciler).await;
    }
    let counts = fixture.counts(campaign).await?;
    assert_eq!(
        (counts.done, counts.pending),
        (1, 2),
        "the canary moves one release"
    );
    assert_eq!(fixture.phase(campaign).await?, "canary");

    store::resume_campaign(
        &Db::from(fixture.pool().clone()),
        campaign,
        &PhaseChange::default(),
        false,
    )
    .await?;
    for _ in 0..4 {
        fixture.tick(&reconciler).await;
    }
    assert_eq!(fixture.counts(campaign).await?.done, 3);
    assert_eq!(fixture.phase(campaign).await?, "completed");

    for (original, hash) in &releases {
        let (twin, _) = fixture.twin("seed", hash).await?.expect("twin");
        let prefix = format!("space/templates/seed/{hash}");
        let document = fixture
            .objects
            .get(
                BUCKET,
                &format!("{prefix}/environments/{}/metadata.json", fixture.e1),
            )
            .expect("the twin's metadata.json");
        let raw: serde_json::Value = serde_json::from_slice(&document)?;
        assert_eq!(
            raw["carried_over"],
            serde_json::json!({"by": "a newer builder"}),
            "fields this build doesn't model carry over"
        );
        let document: TemplateMetadata = serde_json::from_slice(&document)?;
        assert_eq!(document.environment()?, Some(fixture.e1));
        assert_eq!(
            releases::storage_locations(fixture.pool(), twin).await?,
            releases::storage_locations(fixture.pool(), *original).await?
        );
        let pools: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM kubernetes_pool_configs
             WHERE template_release_id = $1 AND enabled AND managed_by = 'manual'",
        )
        .bind(twin)
        .fetch_one(fixture.pool())
        .await?;
        assert_eq!(pools, 1, "the twin has its original's manual pool");
    }
    assert!(
        fixture
            .objects
            .writes()
            .iter()
            .all(|key| key.contains("/environments/")),
        "an original's metadata.json is never written"
    );
    Ok(())
}

/// A build registered under both hash spellings, each original with storage
/// and its own manual pools, gets one twin carrying each original's shapes,
/// with the newer original's settings for a shape both configure, whichever
/// original's item makes the twin.
#[tokio::test(flavor = "multi_thread")]
async fn a_twin_takes_the_pools_of_both_hash_spellings() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("seed").await?;
    let hex = "07".repeat(32);
    // The older original, under the bare spelling, is leased first and makes
    // the twin.
    let (bare, _) = fixture.release_at("seed", hex.clone(), None).await?;
    let (canonical, _) = fixture
        .release_at("seed", format!("sha256:{hex}"), None)
        .await?;
    let pool = |release: Uuid, vcpu: i32, memory_mib: i32, target: i32| {
        sqlx::query(
            "INSERT INTO kubernetes_pool_configs (
                 template_release_id, vcpu, memory_mib, target_ready_slots, refill_per_tick,
                 max_warming_slots, namespace, enabled, managed_by
             )
             VALUES ($1, $2, $3, $4, 1, 1, 'space', TRUE, 'manual')",
        )
        .bind(release)
        .bind(vcpu)
        .bind(memory_mib)
        .bind(target)
        .execute(fixture.pool())
    };
    pool(bare, 2, 1024, 2).await?;
    pool(bare, 1, 512, 4).await?;
    pool(canonical, 2, 1024, 5).await?;
    pool(canonical, 4, 2048, 3).await?;
    let campaign = fixture.campaign(CampaignPhase::Rolling, 1).await?;
    fixture.set_default(fixture.e1).await?;
    let reconciler = fixture.reconciler();
    for _ in 0..4 {
        fixture.tick(&reconciler).await;
    }
    assert_eq!(fixture.counts(campaign).await?.done, 2);
    assert_eq!(fixture.phase(campaign).await?, "completed");

    let twins: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id FROM template_releases
         WHERE template_name = 'seed' AND execution_metadata->>'environment' = $1",
    )
    .bind(fixture.e1.to_string())
    .fetch_all(fixture.pool())
    .await?;
    assert_eq!(twins.len(), 1, "one twin serves both spellings");
    let pools: Vec<(i32, i32, i32)> = sqlx::query_as(
        "SELECT vcpu, memory_mib, target_ready_slots FROM kubernetes_pool_configs
         WHERE template_release_id = $1 AND managed_by = 'manual'
         ORDER BY vcpu",
    )
    .bind(twins[0])
    .fetch_all(fixture.pool())
    .await?;
    assert_eq!(
        pools,
        vec![(1, 512, 4), (2, 1024, 5), (4, 2048, 3)],
        "each original's own shapes, and the newer one's settings where they share a shape"
    );
    Ok(())
}

/// A campaign paused mid-wave leases nothing more, however many ticks pass;
/// what was already done stays done.
#[tokio::test(flavor = "multi_thread")]
async fn a_paused_campaign_stays_paused() -> TestResult {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.template("seed").await?;
    for byte in 1..=4u8 {
        fixture.release("seed", byte, None).await?;
    }
    fixture.set_default(fixture.e1).await?;
    let campaign = fixture.campaign(CampaignPhase::Rolling, 1).await?;
    let reconciler = fixture.reconciler();
    fixture.tick(&reconciler).await;
    fixture.tick(&reconciler).await;
    let before = fixture.counts(campaign).await?;
    assert!(before.done >= 1 && before.pending >= 1, "{before:?}");

    store::pause_campaign(
        &Db::from(fixture.pool().clone()),
        campaign,
        &PhaseChange::default(),
    )
    .await?;
    for _ in 0..4 {
        fixture.tick(&reconciler).await;
    }
    assert_eq!(fixture.phase(campaign).await?, "paused");
    assert_eq!(fixture.counts(campaign).await?, before);
    Ok(())
}

/// A fresh, complete snapshot for `node` listing `usable`, with `node_ctl`
/// as its node-ctl document.
async fn worker_snapshot(
    pool: &PgPool,
    node: pplx_space_accounts::NodeId,
    usable: &[EnvironmentReference],
    node_ctl: &str,
) -> TestResult {
    let usable: Vec<String> = usable.iter().map(ToString::to_string).collect();
    sqlx::query(
        "INSERT INTO node_status_snapshots (
             node_id, observed_at, stale_after, reachable, services, updated_at,
             execution_inventory_status, execution_inventory_received_at,
             usable_environment_refs, known_environment_refs, environment_refs_received_at,
             node_ctl
         )
         VALUES ($1, now(), now() + interval '1 minute', TRUE, '[]'::jsonb, now(),
                 'complete', now(), $2, $2, now(), $3::jsonb)
         ON CONFLICT (node_id) DO UPDATE SET
             usable_environment_refs = EXCLUDED.usable_environment_refs,
             node_ctl = EXCLUDED.node_ctl",
    )
    .bind(node)
    .bind(&usable)
    .bind(node_ctl)
    .execute(pool)
    .await?;
    Ok(())
}
