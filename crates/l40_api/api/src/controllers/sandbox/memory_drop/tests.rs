use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use pplx_space_accounts::NodeId;
use pplx_space_artifact_model::{SnapshotArtifact as SnapshotFile, StorageUri};
use pplx_space_cpu_arch::Arch;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_execution_spec::{ExecutionMetadata, HostCpu, SnapshotExecutionMetadata};
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_store::memory_drop::{
    self, CandidateRead, MemoryDropCandidate, MemoryDropRefusal,
};
use pplx_space_snapshot_model::{LocalArtifactId, SnapshotId, SnapshotKind, SnapshotState};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use serde_json::{Value, json};
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

use super::{
    CommitOutcome, CopyPlan, MemoryDrop, MemoryDropIneligible, MemoryDropObjects,
    MemoryDropObjectsError, MemoryDropOutcome, Sealed, object_key, project_disk_metadata,
};

const BUCKET: &str = "test-bucket";
const PARENT: &str = "suspend_snapshots/00";

fn candidate_at(storage_connection_string: String) -> MemoryDropCandidate {
    MemoryDropCandidate {
        org_id: pplx_space_accounts::OrgId::from_uuid(SEED_ORG_ID),
        pinned_artifact_id: LocalArtifactId::from_uuid(Uuid::now_v7()),
        full_snapshot_id: SnapshotId::from_uuid(Uuid::now_v7()),
        storage_connection_string,
    }
}

/// A candidate whose location is `<parent>/<sandbox>/<snapshot>`, with
/// `parent` empty for a bare prefix.
fn candidate_under(sandbox_id: SandboxId, parent: &str) -> MemoryDropCandidate {
    let mut candidate = candidate_at(String::new());
    let suffix = format!("{sandbox_id}/{}", candidate.full_snapshot_id);
    let key = if parent.is_empty() {
        suffix
    } else {
        format!("{parent}/{suffix}")
    };
    candidate.storage_connection_string = format!("s3://{BUCKET}/{key}?region=us-east-1");
    candidate
}

#[test]
fn the_disk_prefix_replaces_the_attempt_segment_and_keeps_the_rest() {
    let sandbox_id = SandboxId::new();
    for parent in [PARENT, ""] {
        let candidate = candidate_under(sandbox_id, parent);
        let plan = CopyPlan::new(sandbox_id, &candidate).expect("per-attempt prefix");

        let dest = plan.dest.parsed();
        let source = plan.source.parsed();
        assert_eq!(dest.bucket, source.bucket);
        assert_eq!(dest.region, source.region);
        let disk_suffix = format!("{sandbox_id}/{}", plan.disk_snapshot_id);
        let expected = if parent.is_empty() {
            disk_suffix
        } else {
            format!("{parent}/{disk_suffix}")
        };
        assert_eq!(dest.key_prefix, expected);
        assert_ne!(plan.disk_snapshot_id, candidate.full_snapshot_id);
    }
}

#[test]
fn a_location_of_another_shape_has_no_plan() {
    let sandbox_id = SandboxId::new();
    let mut candidate = candidate_under(sandbox_id, PARENT);
    for key in [
        format!("{PARENT}/{sandbox_id}"),
        format!("{PARENT}/{}", candidate.full_snapshot_id),
        format!(
            "{PARENT}/{}/{}",
            SandboxId::new(),
            candidate.full_snapshot_id
        ),
        format!("{PARENT}x{sandbox_id}/{}", candidate.full_snapshot_id),
    ] {
        candidate.storage_connection_string = format!("s3://{BUCKET}/{key}?region=us-east-1");
        assert_eq!(CopyPlan::new(sandbox_id, &candidate), None, "{key}");
    }
    candidate.storage_connection_string = "not a uri".to_string();
    assert_eq!(CopyPlan::new(sandbox_id, &candidate), None);
}

#[test]
fn the_disk_ids_are_fixed_by_the_pinned_artifact() {
    let sandbox_id = SandboxId::new();
    let candidate = candidate_under(sandbox_id, PARENT);
    let first = CopyPlan::new(sandbox_id, &candidate).expect("plan");
    let again = CopyPlan::new(sandbox_id, &candidate).expect("plan");
    assert_eq!(first, again);
    assert_ne!(
        first.disk_snapshot_id.as_uuid(),
        first.disk_artifact_id.as_uuid()
    );

    let mut repinned = candidate.clone();
    repinned.pinned_artifact_id = LocalArtifactId::from_uuid(Uuid::now_v7());
    let next = CopyPlan::new(sandbox_id, &repinned).expect("plan");
    assert_ne!(next.disk_snapshot_id, first.disk_snapshot_id);
    assert_ne!(next.disk_artifact_id, first.disk_artifact_id);

    // A newer backup of the same pin lands on the same prefix, so an attempt
    // that read the older backup leaves no stray copy.
    let mut newer_backup = candidate.clone();
    newer_backup.full_snapshot_id = SnapshotId::from_uuid(Uuid::now_v7());
    newer_backup.storage_connection_string = format!(
        "s3://{BUCKET}/{PARENT}/{sandbox_id}/{}?region=us-east-1",
        newer_backup.full_snapshot_id
    );
    let reread = CopyPlan::new(sandbox_id, &newer_backup).expect("plan");
    assert_ne!(reread.source, first.source);
    assert_eq!(reread.dest, first.dest);
    assert_eq!(reread.disk_snapshot_id, first.disk_snapshot_id);
    assert_eq!(reread.disk_artifact_id, first.disk_artifact_id);
}

fn execution() -> SnapshotExecutionMetadata {
    SnapshotExecutionMetadata::new(ExecutionMetadata::initial_firecracker(
        Arch::Aarch64,
        vec![],
    ))
    .expect("snapshot execution metadata")
}

/// A version 2 `metadata.json` of the full artifact `candidate` pins.
fn full_metadata(sandbox_id: SandboxId, candidate: &MemoryDropCandidate) -> Value {
    let execution = execution().with_host_cpu(Some(HostCpu::parse("cpu-a").expect("host cpu")));
    json!({
        "version": 2,
        "execution": execution,
        "sandbox_id": sandbox_id.to_string(),
        "size_bytes": 9000,
        "rootfs_size_bytes": 4,
        "created_at": "2026-11-20T00:00:00Z",
        "source": {"runtime": "fc", "template_id": "seed"},
        "rootfs_diff_format": "btrfs-send-v1",
        "rootfs_send_source_id": Uuid::now_v7().to_string(),
        "local_artifact_id": candidate.pinned_artifact_id.to_string(),
        "artifact_kind": "full",
        "artifact_origin": "suspend",
        "artifact_snapshot_name": "snap-1",
        "field_from_a_newer_node": "kept",
    })
}

fn project(
    metadata: &Value,
    sandbox_id: SandboxId,
    candidate: &MemoryDropCandidate,
) -> Result<(Value, SnapshotFile, u64), &'static str> {
    let disk = LocalArtifactId::from_uuid(Uuid::now_v7());
    let bytes = serde_json::to_vec(metadata).map_err(|_| "unserializable test metadata")?;
    let projection = project_disk_metadata(&bytes, sandbox_id, candidate, disk)?;
    let projected: Value = serde_json::from_slice(&projection.metadata)
        .map_err(|_| "projected metadata isn't JSON")?;
    assert_eq!(projected["local_artifact_id"], disk.to_string());
    Ok((projected, projection.rootfs, projection.rootfs_size))
}

#[test]
fn a_full_artifact_projects_to_a_stop_disk() {
    let sandbox_id = SandboxId::new();
    let candidate = candidate_under(sandbox_id, PARENT);
    let metadata = full_metadata(sandbox_id, &candidate);

    let (projected, rootfs, size) = project(&metadata, sandbox_id, &candidate).expect("projects");

    assert_eq!(rootfs, SnapshotFile::RootfsBtrfsSend);
    assert_eq!(size, 4);
    assert_eq!(projected["artifact_kind"], "disk");
    assert_eq!(projected["artifact_origin"], "stop");
    assert_eq!(projected["size_bytes"], 4);
    assert_eq!(projected["rootfs_size_bytes"], 4);
    assert_eq!(projected["rootfs_diff_format"], "btrfs-send-v1");
    assert_eq!(projected["field_from_a_newer_node"], "kept");
    assert_eq!(projected["source"], metadata["source"]);
    assert!(projected.get("rootfs_send_source_id").is_none());
    assert!(projected.get("artifact_snapshot_name").is_none());
    let execution: SnapshotExecutionMetadata =
        serde_json::from_value(projected["execution"].clone()).expect("execution");
    assert_eq!(execution, self::execution());
}

#[test]
fn a_full_rootfs_artifact_projects_to_the_rootfs_object() {
    let sandbox_id = SandboxId::new();
    let candidate = candidate_under(sandbox_id, PARENT);
    let mut metadata = full_metadata(sandbox_id, &candidate);
    metadata
        .as_object_mut()
        .expect("object")
        .remove("rootfs_diff_format");

    let (_, rootfs, _) = project(&metadata, sandbox_id, &candidate).expect("projects");

    assert_eq!(rootfs, SnapshotFile::Rootfs);
}

#[test]
fn an_artifact_the_projection_cant_vouch_for_is_refused() {
    let sandbox_id = SandboxId::new();
    let candidate = candidate_under(sandbox_id, PARENT);
    let edits: [(&str, Value); 8] = [
        ("version", json!(1)),
        ("sandbox_id", json!(SandboxId::new().to_string())),
        ("encrypted", json!(true)),
        ("artifact_kind", json!("disk")),
        ("local_artifact_id", json!(Uuid::now_v7().to_string())),
        ("rootfs_size_bytes", Value::Null),
        ("rootfs_diff_format", json!("zfs-send-v9")),
        ("execution", Value::Null),
    ];
    for (field, value) in edits {
        let mut metadata = full_metadata(sandbox_id, &candidate);
        metadata[field] = value;
        assert!(
            project(&metadata, sandbox_id, &candidate).is_err(),
            "{field} should refuse"
        );
    }
    project_disk_metadata(b"not json", sandbox_id, &candidate, LocalArtifactId::new())
        .expect_err("not JSON should refuse");
}

/// Objects in a map keyed by `bucket/key`. `on_write` runs before the
/// metadata write lands, standing in for whatever happens to the row while
/// the copy runs.
#[derive(Default)]
pub(crate) struct MapObjects {
    objects: Mutex<HashMap<String, Vec<u8>>>,
    /// How many object copies the drops made.
    copies: AtomicUsize,
    on_write: Option<Box<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>>,
}

impl MapObjects {
    fn path(prefix: &StorageUri, file: SnapshotFile) -> String {
        format!("{}/{}", prefix.bucket, object_key(prefix, file))
    }

    pub(crate) fn put(&self, prefix: &StorageUri, file: SnapshotFile, body: Vec<u8>) {
        self.objects
            .lock()
            .expect("objects")
            .insert(Self::path(prefix, file), body);
    }

    fn get(&self, prefix: &StorageUri, file: SnapshotFile) -> Option<Vec<u8>> {
        self.objects
            .lock()
            .expect("objects")
            .get(&Self::path(prefix, file))
            .cloned()
    }
}

impl MemoryDropObjects for MapObjects {
    fn size<'a>(
        &'a self,
        prefix: &'a StorageUri,
        file: SnapshotFile,
    ) -> BoxFuture<'a, Result<Option<u64>, MemoryDropObjectsError>> {
        let size = self.get(prefix, file).map(|body| body.len() as u64);
        async move { Ok(size) }.boxed()
    }

    fn read<'a>(
        &'a self,
        prefix: &'a StorageUri,
        file: SnapshotFile,
    ) -> BoxFuture<'a, Result<Vec<u8>, MemoryDropObjectsError>> {
        let body = self
            .get(prefix, file)
            .ok_or_else(|| MemoryDropObjectsError(format!("no {file}")));
        async move { body }.boxed()
    }

    fn copy<'a>(
        &'a self,
        source: &'a StorageUri,
        dest: &'a StorageUri,
        file: SnapshotFile,
        size: u64,
    ) -> BoxFuture<'a, Result<(), MemoryDropObjectsError>> {
        async move {
            let body = self
                .get(source, file)
                .ok_or_else(|| MemoryDropObjectsError(format!("no {file}")))?;
            assert_eq!(body.len() as u64, size);
            self.copies.fetch_add(1, Ordering::SeqCst);
            self.put(dest, file, body);
            Ok(())
        }
        .boxed()
    }

    fn write<'a>(
        &'a self,
        prefix: &'a StorageUri,
        file: SnapshotFile,
        body: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), MemoryDropObjectsError>> {
        async move {
            if let Some(on_write) = &self.on_write {
                on_write().await;
            }
            self.put(prefix, file, body);
            Ok(())
        }
        .boxed()
    }
}

/// An eligible row: a Kubernetes sandbox suspended off a node that's gone,
/// pinned to a full suspend artifact whose objects are in the map.
struct Fixture {
    sandbox_id: SandboxId,
    artifact_id: LocalArtifactId,
    snapshot_id: SnapshotId,
}

impl Fixture {
    async fn insert(pool: &PgPool, objects: &MapObjects) -> TestResult<Self> {
        let sandbox_id = SandboxId::new();
        let artifact_id = LocalArtifactId::from_uuid(Uuid::now_v7());
        sqlx::query(sql!(
            "INSERT INTO sandboxes
                (id, user_id, org_id, managed_by, lifecycle, placement,
                 target_node_id, network_policy_generation,
                 current_local_artifact_id, current_local_artifact_kind,
                 current_local_artifact_origin)
             VALUES ($1, $2::UUID, $3::UUID, $4, 'suspended', 'unplaced',
                     $5, 1, $6, 'full', 'suspend')"
        ))
        .bind(sandbox_id)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(SubstrateKind::Kubernetes)
        .bind(NodeId::new())
        .bind(artifact_id)
        .execute(pool)
        .await?;
        // The last run's node, which has no `node_k8s` mapping: gone.
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                (sandbox_id, generation, pod_name, pod_namespace, k8s_node_name,
                 state, active, terminated_at)
             VALUES ($1, 1, $2, 'space-sandboxes', 'worker-gone', 'terminated',
                     FALSE, NOW() - INTERVAL '1 hour')"
        ))
        .bind(sandbox_id)
        .bind(format!("sbx-{sandbox_id}"))
        .execute(pool)
        .await?;
        let snapshot_id = insert_full_snapshot(pool, sandbox_id, artifact_id, "1 hour").await?;
        let fixture = Self {
            sandbox_id,
            artifact_id,
            snapshot_id,
        };
        let source = fixture.source();
        let candidate = MemoryDropCandidate {
            org_id: pplx_space_accounts::OrgId::from_uuid(SEED_ORG_ID),
            pinned_artifact_id: artifact_id,
            full_snapshot_id: snapshot_id,
            storage_connection_string: String::new(),
        };
        let metadata = full_metadata(sandbox_id, &candidate);
        objects.put(
            &source,
            SnapshotFile::Metadata,
            serde_json::to_vec(&metadata)?,
        );
        objects.put(&source, SnapshotFile::RootfsBtrfsSend, b"disk".to_vec());
        objects.put(&source, SnapshotFile::Memory, b"memory".to_vec());
        Ok(fixture)
    }

    fn source(&self) -> StorageUri {
        StorageUri::parse(&storage_uri(self.sandbox_id, self.snapshot_id)).expect("uri")
    }

    async fn lifecycle(&self, pool: &PgPool) -> TestResult<(String, LocalArtifactId, String)> {
        Ok(sqlx::query_as(sql!(
            "SELECT lifecycle, current_local_artifact_id, current_local_artifact_kind
               FROM sandboxes WHERE id = $1"
        ))
        .bind(self.sandbox_id)
        .fetch_one(pool)
        .await?)
    }

    async fn lifecycle_changed_at(&self, pool: &PgPool) -> TestResult<OffsetDateTime> {
        Ok(sqlx::query_scalar(sql!(
            "SELECT lifecycle_changed_at FROM sandboxes WHERE id = $1"
        ))
        .bind(self.sandbox_id)
        .fetch_one(pool)
        .await?)
    }

    /// The `(snapshot id, orphaned)` locations of this sandbox's snapshots
    /// and of orphans named after it, by connection string.
    async fn locations(&self, pool: &PgPool) -> TestResult<HashMap<String, Option<SnapshotId>>> {
        let rows: Vec<(String, Option<SnapshotId>)> = sqlx::query_as(sql!(
            "SELECT storage_connection_string, snapshot_id
               FROM snapshot_storage_locations
              WHERE storage_connection_string LIKE '%' || $1::TEXT || '%'"
        ))
        .bind(self.sandbox_id.to_string())
        .fetch_all(pool)
        .await?;
        Ok(rows.into_iter().collect())
    }
}

async fn insert_full_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    artifact_id: LocalArtifactId,
    age: &str,
) -> TestResult<SnapshotId> {
    let snapshot_id = SnapshotId::from_uuid(Uuid::now_v7());
    sqlx::query(sql!(
        "INSERT INTO snapshots
            (id, sandbox_id, type, state, timestamp, local_artifact_id,
             artifact_kind, artifact_origin, org_id, vcpu, memory_mib)
         VALUES ($1, $2, 'full', 'ready', NOW() - $3::INTERVAL, $4, 'full',
                 'suspend', $5::UUID, 2, 512)"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(age)
    .bind(artifact_id)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations
            (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, $3::UUID)"
    ))
    .bind(snapshot_id)
    .bind(storage_uri(sandbox_id, snapshot_id))
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    Ok(snapshot_id)
}

fn storage_uri(sandbox_id: SandboxId, snapshot_id: SnapshotId) -> String {
    format!("s3://{BUCKET}/{PARENT}/{sandbox_id}/{snapshot_id}?region=us-east-1")
}

#[tokio::test]
async fn a_suspended_sandbox_off_a_gone_node_keeps_its_disk_and_retains_its_full_artifact()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let objects = Arc::new(MapObjects::default());
    let fixture = Fixture::insert(&db.pool, &objects).await?;
    let before = fixture.lifecycle_changed_at(&db.pool).await?;
    let drop = MemoryDrop::new(objects.clone());
    assert_eq!(
        MemoryDrop::eligibility(&db.pool, fixture.sandbox_id).await?,
        None
    );

    let outcome = drop.drop_memory(&db.pool, fixture.sandbox_id).await?;

    let MemoryDropOutcome::Dropped(dropped) = outcome else {
        panic!("expected a drop, got {outcome:?}");
    };
    assert_eq!(dropped.retained_snapshot_id, fixture.snapshot_id);
    assert_eq!(dropped.retained_artifact_id, fixture.artifact_id);
    let (lifecycle, pin, kind) = fixture.lifecycle(&db.pool).await?;
    assert_eq!(
        (lifecycle.as_str(), pin, kind.as_str()),
        ("suspended", dropped.disk_artifact_id, "disk")
    );
    assert_eq!(
        fixture.lifecycle_changed_at(&db.pool).await?,
        before,
        "the drop doesn't move the lifecycle clock"
    );
    let lease: Option<Uuid> = sqlx::query_scalar(sql!(
        "SELECT restore_lease_token FROM sandboxes WHERE id = $1"
    ))
    .bind(fixture.sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(lease, None, "the commit lifted the seal");

    type DiskRow = (
        SnapshotId,
        String,
        String,
        String,
        Option<i64>,
        Option<i32>,
        bool,
    );
    let (disk_id, kind, origin, state, size, vcpu, written_off_node): DiskRow =
        sqlx::query_as(sql!(
            "SELECT id, artifact_kind, artifact_origin, state, size_bytes, vcpu, written_off_node
               FROM snapshots
              WHERE sandbox_id = $1 AND local_artifact_id = $2"
        ))
        .bind(fixture.sandbox_id)
        .bind(pin)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(disk_id, dropped.disk_snapshot_id);
    assert_eq!(
        (kind.as_str(), origin.as_str(), state.as_str()),
        ("disk", "stop", "ready")
    );
    assert_eq!((size, vcpu), (Some(4), Some(2)));
    assert!(written_off_node, "the control plane wrote this disk");
    // The disk carries its source full snapshot's timestamp: the same bytes,
    // so the rows order together.
    let (disk_at, full_at): (OffsetDateTime, OffsetDateTime) = sqlx::query_as(sql!(
        "SELECT d.timestamp, f.timestamp
           FROM snapshots d, snapshots f
          WHERE d.id = $1 AND f.id = $2"
    ))
    .bind(disk_id)
    .bind(fixture.snapshot_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(disk_at, full_at);

    // The full artifact is retained: its row still ready, its prefix still
    // attached, nothing handed to the sweeper.
    let full_state: String = sqlx::query_scalar(sql!("SELECT state FROM snapshots WHERE id = $1"))
        .bind(fixture.snapshot_id)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(full_state, "ready");
    let disk_uri = storage_uri(fixture.sandbox_id, disk_id);
    let locations = fixture.locations(&db.pool).await?;
    assert_eq!(locations.get(&disk_uri), Some(&Some(disk_id)));
    assert_eq!(
        locations.get(&storage_uri(fixture.sandbox_id, fixture.snapshot_id)),
        Some(&Some(fixture.snapshot_id))
    );
    assert_eq!(locations.len(), 2, "no orphan was registered");
    assert_eq!(
        objects.get(&fixture.source(), SnapshotFile::Memory),
        Some(b"memory".to_vec()),
        "the full artifact's memory is untouched"
    );

    let dest = StorageUri::parse(&disk_uri)?;
    assert_eq!(
        objects.get(&dest, SnapshotFile::RootfsBtrfsSend),
        Some(b"disk".to_vec())
    );
    assert_eq!(objects.get(&dest, SnapshotFile::Memory), None);
    let metadata: Value = serde_json::from_slice(
        &objects
            .get(&dest, SnapshotFile::Metadata)
            .expect("disk metadata"),
    )?;
    assert_eq!(metadata["local_artifact_id"], pin.to_string());
    assert_eq!(metadata["artifact_kind"], "disk");
    assert!(
        metadata["execution"].is_object(),
        "the producer stays recorded"
    );
    Ok(())
}

#[tokio::test]
async fn a_retry_after_the_commit_reports_the_drop_already_done() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let objects = Arc::new(MapObjects::default());
    let fixture = Fixture::insert(&db.pool, &objects).await?;
    let drop = MemoryDrop::new(objects.clone());
    assert!(matches!(
        drop.drop_memory(&db.pool, fixture.sandbox_id).await?,
        MemoryDropOutcome::Dropped(_)
    ));
    let after_first = fixture.lifecycle(&db.pool).await?;
    let object_count = objects.objects.lock().expect("objects").len();

    assert_eq!(
        drop.drop_memory(&db.pool, fixture.sandbox_id).await?,
        MemoryDropOutcome::AlreadyDropped
    );
    assert_eq!(fixture.lifecycle(&db.pool).await?, after_first);
    assert_eq!(objects.objects.lock().expect("objects").len(), object_count);
    assert_eq!(
        MemoryDrop::eligibility(&db.pool, fixture.sandbox_id).await?,
        Some(MemoryDropIneligible::Row(MemoryDropRefusal::AlreadyDropped))
    );
    Ok(())
}

/// An operator repoints a dropped row back to its retained full artifact,
/// retention starts deleting the now-unpinned disk, and the operator drops
/// again. The ids are derived from the pin, so the second drop finds the
/// first one's disk row: it must not re-pin to bytes being deleted.
#[tokio::test]
async fn a_redrop_onto_a_disk_retention_is_deleting_is_retryable() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let objects = Arc::new(MapObjects::default());
    let fixture = Fixture::insert(&db.pool, &objects).await?;
    let drop = MemoryDrop::new(objects.clone());
    let MemoryDropOutcome::Dropped(dropped) =
        drop.drop_memory(&db.pool, fixture.sandbox_id).await?
    else {
        panic!("the first drop commits");
    };
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET current_local_artifact_id = $2,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend'
         WHERE id = $1"
    ))
    .bind(fixture.sandbox_id)
    .bind(fixture.artifact_id.as_uuid())
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE snapshots SET state = 'deleting' WHERE id = $1"
    ))
    .bind(dropped.disk_snapshot_id.as_uuid())
    .execute(&db.pool)
    .await?;

    let copies = objects.copies.load(Ordering::SeqCst);

    let error = drop
        .drop_memory(&db.pool, fixture.sandbox_id)
        .await
        .expect_err("a disk being deleted isn't re-pinned");
    assert_eq!(
        objects.copies.load(Ordering::SeqCst),
        copies,
        "nothing is copied into a prefix retention is deleting"
    );
    assert_eq!(
        error.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "{error}"
    );
    let (lifecycle, pin, kind): (String, Uuid, String) = sqlx::query_as(sql!(
        "SELECT lifecycle, current_local_artifact_id, current_local_artifact_kind
         FROM sandboxes WHERE id = $1"
    ))
    .bind(fixture.sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        (lifecycle.as_str(), pin, kind.as_str()),
        ("suspended", fixture.artifact_id.as_uuid(), "full")
    );
    assert!(
        !pplx_space_sandbox_store::memory_drop::sealed(&db.pool, fixture.sandbox_id).await?,
        "the refused drop lifts its seal"
    );
    Ok(())
}

/// Point a dropped row back at its retained full artifact, as an operator
/// does before a rollback.
async fn repoint_to_full(pool: &PgPool, fixture: &Fixture) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET current_local_artifact_id = $2,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend'
         WHERE id = $1"
    ))
    .bind(fixture.sandbox_id)
    .bind(fixture.artifact_id.as_uuid())
    .execute(pool)
    .await?;
    Ok(())
}

/// The sandbox's `(lifecycle, pin, pin kind)`.
async fn pin_of(pool: &PgPool, fixture: &Fixture) -> TestResult<(String, Uuid, String)> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, current_local_artifact_id, current_local_artifact_kind
         FROM sandboxes WHERE id = $1"
    ))
    .bind(fixture.sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// After a repoint, the first drop's disk is unpinned and retention may claim
/// it at any time: it carries the full snapshot's timestamp, so the age gate
/// doesn't hold it. A re-drop that copied into the same prefix could have
/// objects deleted under it. The second drop re-pins to the registered disk
/// and copies nothing.
#[tokio::test]
async fn a_redrop_onto_a_ready_earlier_disk_reuses_it_without_copying() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let objects = Arc::new(MapObjects::default());
    let fixture = Fixture::insert(&db.pool, &objects).await?;
    let drop = MemoryDrop::new(objects.clone());
    let MemoryDropOutcome::Dropped(first) = drop.drop_memory(&db.pool, fixture.sandbox_id).await?
    else {
        panic!("the first drop commits");
    };
    repoint_to_full(&db.pool, &fixture).await?;
    let copies = objects.copies.load(Ordering::SeqCst);

    let MemoryDropOutcome::Dropped(second) = drop.drop_memory(&db.pool, fixture.sandbox_id).await?
    else {
        panic!("the re-drop commits");
    };
    assert_eq!(second, first);
    assert_eq!(objects.copies.load(Ordering::SeqCst), copies);
    assert_eq!(
        pin_of(&db.pool, &fixture).await?,
        (
            "suspended".to_string(),
            first.disk_artifact_id.as_uuid(),
            "disk".to_string()
        )
    );
    assert!(!memory_drop::sealed(&db.pool, fixture.sandbox_id).await?);
    Ok(())
}

/// Once retention has deleted the first drop's disk, its rows are gone too
/// (it deletes the prefix before the rows), so the prefix is free and the
/// re-drop copies again.
#[tokio::test]
async fn a_redrop_after_retention_deleted_the_earlier_disk_copies_again() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let objects = Arc::new(MapObjects::default());
    let fixture = Fixture::insert(&db.pool, &objects).await?;
    let drop = MemoryDrop::new(objects.clone());
    let MemoryDropOutcome::Dropped(first) = drop.drop_memory(&db.pool, fixture.sandbox_id).await?
    else {
        panic!("the first drop commits");
    };
    repoint_to_full(&db.pool, &fixture).await?;
    sqlx::query(sql!(
        "DELETE FROM snapshot_storage_locations WHERE snapshot_id = $1"
    ))
    .bind(first.disk_snapshot_id.as_uuid())
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!("DELETE FROM snapshots WHERE id = $1"))
        .bind(first.disk_snapshot_id.as_uuid())
        .execute(&db.pool)
        .await?;
    let copies = objects.copies.load(Ordering::SeqCst);

    let MemoryDropOutcome::Dropped(second) = drop.drop_memory(&db.pool, fixture.sandbox_id).await?
    else {
        panic!("the re-drop commits");
    };
    assert_eq!(second, first);
    assert_eq!(objects.copies.load(Ordering::SeqCst), copies + 1);
    Ok(())
}

/// A drop that lost after its pin moved handed the prefix to the storage
/// sweeper. If the row is pointed back at that artifact, a re-drop derives
/// the same prefix, so it waits for the sweeper rather than copy into bytes
/// being deleted.
#[tokio::test]
async fn a_redrop_onto_an_unswept_orphan_prefix_is_retryable() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let objects = Arc::new(MapObjects::default());
    let fixture = Fixture::insert(&db.pool, &objects).await?;
    let drop = MemoryDrop::new(objects.clone());
    let candidate = memory_drop::candidate(&db.pool, fixture.sandbox_id, CandidateRead::Plain)
        .await?
        .expect("eligible");
    let plan = CopyPlan::new(fixture.sandbox_id, &candidate).expect("plan");
    assert!(
        pplx_space_snapshot_store::orphan::orphan_unowned_location(
            &db.pool,
            plan.disk_snapshot_id,
            &plan.dest,
            candidate.org_id,
        )
        .await?
    );

    let error = drop
        .drop_memory(&db.pool, fixture.sandbox_id)
        .await
        .expect_err("an unswept prefix isn't copied into");
    assert_eq!(
        error.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "{error}"
    );
    assert_eq!(objects.copies.load(Ordering::SeqCst), 0);
    assert_eq!(
        pin_of(&db.pool, &fixture).await?,
        (
            "suspended".to_string(),
            fixture.artifact_id.as_uuid(),
            "full".to_string()
        )
    );
    assert!(!memory_drop::sealed(&db.pool, fixture.sandbox_id).await?);
    Ok(())
}

/// The commit re-checks the earlier disk under the fork-source lock, which
/// retention's claim also takes: a claim that lands after the drop decided to
/// reuse the disk, and before it commits, still stops the re-pin.
#[tokio::test]
async fn the_commit_refuses_a_disk_retention_claimed_after_the_drop_read_it() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let objects = Arc::new(MapObjects::default());
    let fixture = Fixture::insert(&db.pool, &objects).await?;
    let drop = MemoryDrop::new(objects.clone());
    let MemoryDropOutcome::Dropped(first) = drop.drop_memory(&db.pool, fixture.sandbox_id).await?
    else {
        panic!("the first drop commits");
    };
    repoint_to_full(&db.pool, &fixture).await?;
    let token = Uuid::now_v7();
    let Sealed::Candidate(candidate) = drop.seal(&db.pool, fixture.sandbox_id, token).await? else {
        panic!("the repointed row seals");
    };
    let plan = CopyPlan::new(fixture.sandbox_id, &candidate).expect("plan");
    sqlx::query(sql!(
        "UPDATE snapshots SET state = 'deleting' WHERE id = $1"
    ))
    .bind(first.disk_snapshot_id.as_uuid())
    .execute(&db.pool)
    .await?;

    assert_eq!(
        drop.commit(&db.pool, fixture.sandbox_id, token, &candidate, &plan, None)
            .await?,
        CommitOutcome::DiskRetiring
    );
    assert_eq!(
        pin_of(&db.pool, &fixture).await?,
        (
            "suspended".to_string(),
            fixture.artifact_id.as_uuid(),
            "full".to_string()
        )
    );
    Ok(())
}

#[tokio::test]
async fn a_row_whose_node_still_has_a_mapping_is_refused() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let objects = Arc::new(MapObjects::default());
    let fixture = Fixture::insert(&db.pool, &objects).await?;
    // The node may still hold a local copy, which its restore would prefer
    // over any URI, memory included.
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         SELECT target_node_id, '10.0.0.41'::INET, 'worker', 'k8s', 'ready'
           FROM sandboxes WHERE id = $1"
    ))
    .bind(fixture.sandbox_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name)
         SELECT target_node_id, 'worker-live' FROM sandboxes WHERE id = $1"
    ))
    .bind(fixture.sandbox_id)
    .execute(&db.pool)
    .await?;
    let before = objects.objects.lock().expect("objects").len();

    let outcome = MemoryDrop::new(objects.clone())
        .drop_memory(&db.pool, fixture.sandbox_id)
        .await?;

    let refused = MemoryDropIneligible::Row(MemoryDropRefusal::NodeNotGone);
    assert_eq!(outcome, MemoryDropOutcome::NotEligible(refused));
    assert_eq!(
        MemoryDrop::eligibility(&db.pool, fixture.sandbox_id).await?,
        Some(refused)
    );
    assert_eq!(fixture.lifecycle(&db.pool).await?.0, "suspended");
    assert_eq!(objects.objects.lock().expect("objects").len(), before);
    Ok(())
}

/// A drop that loses its commit to `race`, which runs while the disk's
/// metadata is being written. Returns the fixture and the disk prefix's
/// location row: `None` when there's none, `Some(None)` for an orphan.
async fn lose_the_commit(
    pool: &PgPool,
    race: impl Fn(PgPool, SandboxId, LocalArtifactId) -> BoxFuture<'static, ()> + Send + Sync + 'static,
) -> TestResult<(Fixture, Option<Option<SnapshotId>>)> {
    // The fixture's ids exist only after the insert, and the hook needs them,
    // so the hook reads them from a slot filled in afterwards.
    let slot: Arc<Mutex<Option<(SandboxId, LocalArtifactId)>>> = Arc::default();
    let hook_slot = slot.clone();
    let hook_pool = pool.clone();
    let race = Arc::new(race);
    let objects = Arc::new(MapObjects {
        objects: Mutex::default(),
        copies: AtomicUsize::new(0),
        on_write: Some(Box::new(move || {
            let (sandbox_id, artifact_id) = hook_slot.lock().expect("slot").expect("filled");
            race(hook_pool.clone(), sandbox_id, artifact_id)
        })),
    });
    let fixture = Fixture::insert(pool, &objects).await?;
    *slot.lock().expect("slot") = Some((fixture.sandbox_id, fixture.artifact_id));
    let candidate = memory_drop::candidate(pool, fixture.sandbox_id, CandidateRead::Plain)
        .await?
        .expect("eligible");
    let dest = CopyPlan::new(fixture.sandbox_id, &candidate)
        .expect("plan")
        .dest
        .as_wire()
        .to_string();

    let lost = MemoryDrop::new(objects.clone())
        .drop_memory(pool, fixture.sandbox_id)
        .await;

    let error = lost.expect_err("the commit loses");
    assert_eq!(error.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    let (lifecycle, _, kind) = fixture.lifecycle(pool).await?;
    assert_eq!((lifecycle.as_str(), kind.as_str()), ("suspended", "full"));
    assert!(
        !memory_drop::sealed(pool, fixture.sandbox_id).await?,
        "the loser left no seal"
    );
    let disk_rows: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM snapshots WHERE sandbox_id = $1 AND type = 'disk'"
    ))
    .bind(fixture.sandbox_id)
    .fetch_one(pool)
    .await?;
    assert_eq!(disk_rows, 0);
    let disk = fixture.locations(pool).await?.get(&dest).copied();
    Ok((fixture, disk))
}

#[tokio::test]
async fn a_drop_whose_pin_moved_hands_its_prefix_to_the_sweeper() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    // Something re-pinned the row while it stayed `suspended`, so the seal
    // survived and the commit's pin check is what refuses.
    let (_, disk) = lose_the_commit(&db.pool, |pool, sandbox_id, _| {
        async move {
            sqlx::query(sql!(
                "UPDATE sandboxes SET current_local_artifact_id = $2 WHERE id = $1"
            ))
            .bind(sandbox_id)
            .bind(LocalArtifactId::from_uuid(Uuid::now_v7()))
            .execute(&pool)
            .await
            .expect("re-pin the row");
        }
        .boxed()
    })
    .await?;

    assert_eq!(disk, Some(None), "the disk prefix is an orphan");
    Ok(())
}

#[tokio::test]
async fn a_drop_that_lost_its_seal_on_the_same_pin_keeps_its_prefix() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    // A wake claimed the row and a failed restore put it back on the same pin:
    // leaving `restoring` cleared the seal.
    let (fixture, disk) = lose_the_commit(&db.pool, |pool, sandbox_id, _| {
        async move {
            for lifecycle in ["restoring", "suspended"] {
                sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
                    .bind(sandbox_id)
                    .bind(lifecycle)
                    .execute(&pool)
                    .await
                    .expect("move the row");
            }
        }
        .boxed()
    })
    .await?;

    // A retry derives the same prefix, so it stays unregistered.
    assert_eq!(disk, None);
    assert_eq!(
        MemoryDrop::eligibility(&db.pool, fixture.sandbox_id).await?,
        None
    );
    Ok(())
}

#[tokio::test]
async fn a_newer_backup_of_the_pin_during_the_copy_doesnt_stop_the_commit() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let slot: Arc<Mutex<Option<(SandboxId, LocalArtifactId)>>> = Arc::default();
    let hook_slot = slot.clone();
    let hook_pool = db.pool.clone();
    let objects = Arc::new(MapObjects {
        objects: Mutex::default(),
        copies: AtomicUsize::new(0),
        on_write: Some(Box::new(move || {
            let (sandbox_id, artifact_id) = hook_slot.lock().expect("slot").expect("filled");
            let pool = hook_pool.clone();
            async move {
                insert_full_snapshot(&pool, sandbox_id, artifact_id, "0 seconds")
                    .await
                    .expect("newer snapshot");
            }
            .boxed()
        })),
    });
    let fixture = Fixture::insert(&db.pool, &objects).await?;
    *slot.lock().expect("slot") = Some((fixture.sandbox_id, fixture.artifact_id));

    let outcome = MemoryDrop::new(objects.clone())
        .drop_memory(&db.pool, fixture.sandbox_id)
        .await?;

    // The same artifact's bytes, so the commit's pin check still holds.
    assert!(
        matches!(outcome, MemoryDropOutcome::Dropped(_)),
        "{outcome:?}"
    );
    Ok(())
}

/// Objects whose metadata write waits `delay` first, then reports through
/// `sealed_after` whether the row was still sealed.
fn slow_objects(
    pool: PgPool,
    slot: Arc<Mutex<Option<SandboxId>>>,
    delay: std::time::Duration,
    during: impl Fn(PgPool, SandboxId) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    sealed_after: Arc<Mutex<Option<bool>>>,
) -> Arc<MapObjects> {
    let during = Arc::new(during);
    Arc::new(MapObjects {
        objects: Mutex::default(),
        copies: AtomicUsize::new(0),
        on_write: Some(Box::new(move || {
            let sandbox_id = slot.lock().expect("slot").expect("filled");
            let pool = pool.clone();
            let during = during.clone();
            let sealed_after = sealed_after.clone();
            async move {
                during(pool.clone(), sandbox_id).await;
                tokio::time::sleep(delay).await;
                let sealed = memory_drop::sealed(&pool, sandbox_id)
                    .await
                    .expect("read the seal");
                *sealed_after.lock().expect("sealed") = Some(sealed);
            }
            .boxed()
        })),
    })
}

#[tokio::test]
async fn a_copy_longer_than_the_seal_ttl_keeps_its_seal_by_renewing_it() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let slot: Arc<Mutex<Option<SandboxId>>> = Arc::default();
    let sealed_after: Arc<Mutex<Option<bool>>> = Arc::default();
    // The copy runs four TTLs; only renewals keep the seal live.
    let objects = slow_objects(
        db.pool.clone(),
        slot.clone(),
        std::time::Duration::from_millis(1200),
        |_, _| async {}.boxed(),
        sealed_after.clone(),
    );
    let fixture = Fixture::insert(&db.pool, &objects).await?;
    *slot.lock().expect("slot") = Some(fixture.sandbox_id);

    let outcome = MemoryDrop::new(objects.clone())
        .with_seal_timing(
            std::time::Duration::from_millis(300),
            std::time::Duration::from_millis(50),
        )
        .drop_memory(&db.pool, fixture.sandbox_id)
        .await?;

    assert!(
        matches!(outcome, MemoryDropOutcome::Dropped(_)),
        "{outcome:?}"
    );
    assert_eq!(*sealed_after.lock().expect("sealed"), Some(true));
    Ok(())
}

#[tokio::test]
async fn a_copy_whose_seal_was_taken_stops_at_the_next_renewal() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let slot: Arc<Mutex<Option<SandboxId>>> = Arc::default();
    let sealed_after: Arc<Mutex<Option<bool>>> = Arc::default();
    // Another holder replaced the token mid-copy, as a retry does once a seal
    // lapses. The copy outlasts a renewal, which finds the token gone and
    // cancels the copy before the metadata lands.
    let objects = slow_objects(
        db.pool.clone(),
        slot.clone(),
        std::time::Duration::from_millis(500),
        |pool, sandbox_id| {
            async move {
                sqlx::query(sql!(
                    "UPDATE sandboxes SET restore_lease_token = $2 WHERE id = $1"
                ))
                .bind(sandbox_id)
                .bind(Uuid::now_v7())
                .execute(&pool)
                .await
                .expect("take the seal");
            }
            .boxed()
        },
        sealed_after.clone(),
    );
    let fixture = Fixture::insert(&db.pool, &objects).await?;
    *slot.lock().expect("slot") = Some(fixture.sandbox_id);
    let dest = CopyPlan::new(
        fixture.sandbox_id,
        &memory_drop::candidate(&db.pool, fixture.sandbox_id, CandidateRead::Plain)
            .await?
            .expect("eligible"),
    )
    .expect("plan")
    .dest;

    let error = MemoryDrop::new(objects.clone())
        .with_seal_timing(
            std::time::Duration::from_secs(60),
            std::time::Duration::from_millis(50),
        )
        .drop_memory(&db.pool, fixture.sandbox_id)
        .await
        .expect_err("the seal was lost");

    assert_eq!(error.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        *sealed_after.lock().expect("sealed"),
        None,
        "the copy was cancelled mid-write"
    );
    assert_eq!(
        objects.get(dest.parsed(), SnapshotFile::Metadata),
        None,
        "no publication marker"
    );
    let (lifecycle, pin, kind) = fixture.lifecycle(&db.pool).await?;
    assert_eq!(
        (lifecycle.as_str(), pin, kind.as_str()),
        ("suspended", fixture.artifact_id, "full")
    );
    // Still on the same pin, so the prefix stays for a retry.
    assert_eq!(fixture.locations(&db.pool).await?.get(dest.as_wire()), None);
    Ok(())
}

/// A queued fork whose source is `sandbox_id`.
async fn queue_fork(pool: &PgPool, sandbox_id: SandboxId) {
    sqlx::query(sql!(
        "INSERT INTO public_operations
             (operation_id, org_id, kind, target_type, target_id, state)
         VALUES ($1, $2::UUID, 'fork_sandbox', 'sandbox', $3, 'queued')"
    ))
    .bind(Uuid::now_v7())
    .bind(SEED_ORG_ID)
    .bind(sandbox_id.to_string())
    .execute(pool)
    .await
    .expect("queue the fork");
}

#[tokio::test]
async fn a_drop_under_a_queued_fork_is_refused_without_copying() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let objects = Arc::new(MapObjects::default());
    let fixture = Fixture::insert(&db.pool, &objects).await?;
    queue_fork(&db.pool, fixture.sandbox_id).await;
    let before = objects.objects.lock().expect("objects").len();

    let outcome = MemoryDrop::new(objects.clone())
        .drop_memory(&db.pool, fixture.sandbox_id)
        .await?;

    assert_eq!(
        outcome,
        MemoryDropOutcome::NotEligible(MemoryDropIneligible::ForkActive)
    );
    assert_eq!(
        MemoryDrop::eligibility(&db.pool, fixture.sandbox_id).await?,
        Some(MemoryDropIneligible::ForkActive)
    );
    assert_eq!(fixture.lifecycle(&db.pool).await?.0, "suspended");
    assert_eq!(objects.objects.lock().expect("objects").len(), before);
    Ok(())
}

#[tokio::test]
async fn a_fork_queued_during_the_copy_keeps_the_suspend_artifact() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let slot: Arc<Mutex<Option<SandboxId>>> = Arc::default();
    let hook_slot = slot.clone();
    let hook_pool = db.pool.clone();
    let objects = Arc::new(MapObjects {
        objects: Mutex::default(),
        copies: AtomicUsize::new(0),
        on_write: Some(Box::new(move || {
            let sandbox_id = hook_slot.lock().expect("slot").expect("filled");
            let pool = hook_pool.clone();
            async move { queue_fork(&pool, sandbox_id).await }.boxed()
        })),
    });
    let fixture = Fixture::insert(&db.pool, &objects).await?;
    *slot.lock().expect("slot") = Some(fixture.sandbox_id);

    let outcome = MemoryDrop::new(objects.clone())
        .drop_memory(&db.pool, fixture.sandbox_id)
        .await?;

    assert_eq!(
        outcome,
        MemoryDropOutcome::NotEligible(MemoryDropIneligible::ForkActive)
    );
    let (lifecycle, pin, _) = fixture.lifecycle(&db.pool).await?;
    assert_eq!(
        (lifecycle.as_str(), pin),
        ("suspended", fixture.artifact_id)
    );
    // The snapshot row and location the fork froze are both still live.
    let source = storage_uri(fixture.sandbox_id, fixture.snapshot_id);
    assert_eq!(
        fixture.locations(&db.pool).await?.get(&source).copied(),
        Some(Some(fixture.snapshot_id))
    );
    Ok(())
}

#[tokio::test]
async fn an_unusable_object_store_fails_retryably_and_leaves_the_row() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let objects = Arc::new(MapObjects::default());
    let fixture = Fixture::insert(&db.pool, &objects).await?;
    objects.objects.lock().expect("objects").clear();

    let error = MemoryDrop::new(objects.clone())
        .drop_memory(&db.pool, fixture.sandbox_id)
        .await
        .expect_err("the object store failed");

    assert_eq!(error.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    let (lifecycle, pin, _) = fixture.lifecycle(&db.pool).await?;
    assert_eq!(
        (lifecycle.as_str(), pin),
        ("suspended", fixture.artifact_id)
    );
    Ok(())
}

/// A wake of a dropped row answers the retryable 503 before any pod exists:
/// no restore path boots a disk into a sandbox from `suspended` yet.
#[cfg(feature = "kubernetes")]
#[tokio::test(flavor = "current_thread")]
async fn a_wake_of_a_dropped_sandbox_is_a_retryable_503_without_a_pod() -> TestResult {
    use crate::controllers::sandbox::SandboxController;
    use crate::controllers::sandbox::lifecycle::SandboxAction;
    use crate::controllers::substrate::kubernetes::tests::{
        mock_kubernetes_client, substrate_with_store_and_client,
    };
    use crate::metrics::sandbox::api::SandboxApiMetricIdentity;

    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let objects = Arc::new(MapObjects::default());
    let fixture = Fixture::insert(&db.pool, &objects).await?;
    assert!(matches!(
        MemoryDrop::new(objects.clone())
            .drop_memory(&db.pool, fixture.sandbox_id)
            .await?,
        MemoryDropOutcome::Dropped(_)
    ));
    let pod_creates = Arc::new(Mutex::new(Vec::new()));
    let substrate = substrate_with_store_and_client(
        db.pool.clone(),
        mock_kubernetes_client(pod_creates.clone()),
    );
    let controller = SandboxController::new(
        crate::controllers::substrate::Substrate::from_kubernetes(substrate),
    );

    for action in [
        SandboxAction::Restore,
        SandboxAction::Start,
        SandboxAction::Wake,
    ] {
        let sandbox = crate::controllers::sandbox::fetch_sandbox(&db.pool, fixture.sandbox_id)
            .await?
            .expect("sandbox row exists");
        let error = controller
            .action(
                &db.pool,
                fixture.sandbox_id,
                Some(&sandbox),
                SandboxApiMetricIdentity::native(action.as_str()),
                action,
                None,
                crate::controllers::substrate::ActivationClient::Native,
            )
            .await
            .expect_err("a dropped row can't restore yet");
        assert_eq!(
            error.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "{action:?}: {error:?}"
        );
        let (lifecycle, _, kind) = fixture.lifecycle(&db.pool).await?;
        assert_eq!((lifecycle.as_str(), kind.as_str()), ("suspended", "disk"));
    }
    assert!(pod_creates.lock().expect("pod creates").is_empty());
    Ok(())
}

/// A wake of a row a memory drop holds sealed waits up to the caller's budget,
/// then answers a retryable 503 (`RestoreInFlight`) before any pod exists and
/// leaves the seal for the drop to commit.
#[cfg(feature = "kubernetes")]
#[tokio::test(flavor = "current_thread")]
async fn a_wake_of_a_sealed_sandbox_is_a_retryable_503_without_a_pod() -> TestResult {
    use crate::controllers::ControllerRetryAfterCause;
    use crate::controllers::sandbox::SandboxController;
    use crate::controllers::sandbox::lifecycle::SandboxAction;
    use crate::controllers::substrate::SubstrateAdapter;
    use crate::controllers::substrate::kubernetes::tests::{
        mock_kubernetes_client, substrate_with_store_and_client,
    };
    use crate::metrics::sandbox::api::SandboxApiMetricIdentity;

    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let objects = Arc::new(MapObjects::default());
    let fixture = Fixture::insert(&db.pool, &objects).await?;
    let token = Uuid::now_v7();
    let mut tx = db.pool.begin().await?;
    memory_drop::seal(
        &mut tx,
        fixture.sandbox_id,
        token,
        std::time::Duration::from_secs(60),
    )
    .await?
    .expect("eligible");
    tx.commit().await?;
    let pod_creates = Arc::new(Mutex::new(Vec::new()));
    let substrate =
        crate::controllers::substrate::Substrate::from_kubernetes(substrate_with_store_and_client(
            db.pool.clone(),
            mock_kubernetes_client(pod_creates.clone()),
        ));
    let controller = SandboxController::new(substrate.clone());

    let sandbox = crate::controllers::sandbox::fetch_sandbox(&db.pool, fixture.sandbox_id)
        .await?
        .expect("sandbox row exists");
    let restore_error = controller
        .action(
            &db.pool,
            fixture.sandbox_id,
            Some(&sandbox),
            SandboxApiMetricIdentity::native("restore"),
            SandboxAction::Restore,
            Some(crate::models::SandboxActivationRequest {
                blocking: Some(true),
                ready_timeout_secs: Some(1),
            }),
            crate::controllers::substrate::ActivationClient::Native,
        )
        .await
        .expect_err("a sealed row can't restore while sealed");
    assert_eq!(
        restore_error.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "Restore: {restore_error:?}"
    );
    assert_eq!(
        restore_error.retry_after_cause(),
        Some(ControllerRetryAfterCause::RestoreInFlight),
        "Restore: {restore_error:?}"
    );
    assert_eq!(fixture.lifecycle(&db.pool).await?.0, "suspended");

    // `SandboxAction::Wake` uses `DEFAULT_BLOCKING_WAIT_TIMEOUT` (60s); test
    // `wake_execution` directly with a bounded readiness budget so the test
    // exercises the wake path without waiting 60 real wall-clock seconds.
    let wake_error = crate::controllers::ControllerError::from(
        substrate
            .wake_execution(
                SandboxApiMetricIdentity::native("wake"),
                &sandbox,
                std::time::Duration::from_millis(200),
                None,
                crate::controllers::substrate::RecoveryCharge::exempt_for_tests(),
            )
            .await
            .expect_err("a sealed row can't wake while sealed"),
    );
    assert_eq!(
        wake_error.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "Wake: {wake_error:?}"
    );
    assert_eq!(
        wake_error.retry_after_cause(),
        Some(ControllerRetryAfterCause::RestoreInFlight),
        "Wake: {wake_error:?}"
    );
    assert_eq!(fixture.lifecycle(&db.pool).await?.0, "suspended");
    assert!(pod_creates.lock().expect("pod creates").is_empty());
    assert!(memory_drop::sealed(&db.pool, fixture.sandbox_id).await?);
    Ok(())
}

/// When a campaign holds a seal on a suspended row and commits within the
/// wake's budget, the wake waits out the seal through `RestoreInFlight` and
/// re-reads the committed disk pin on the next lap instead of failing as soon
/// as it sees the seal.
#[cfg(feature = "kubernetes")]
#[tokio::test(flavor = "current_thread")]
async fn a_wake_waits_for_a_campaign_seal_to_commit_within_budget() -> TestResult {
    use crate::controllers::ControllerRetryAfterCause;
    use crate::controllers::sandbox::SandboxController;
    use crate::controllers::sandbox::lifecycle::SandboxAction;
    use crate::controllers::substrate::kubernetes::tests::{
        mock_kubernetes_client, substrate_with_store_and_client,
    };
    use crate::metrics::sandbox::api::SandboxApiMetricIdentity;

    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let objects = Arc::new(MapObjects::default());
    let fixture = Fixture::insert(&db.pool, &objects).await?;
    let token = Uuid::now_v7();
    let mut tx = db.pool.begin().await?;
    let candidate = memory_drop::seal(
        &mut tx,
        fixture.sandbox_id,
        token,
        std::time::Duration::from_secs(60),
    )
    .await?
    .expect("eligible");
    tx.commit().await?;

    let pool_for_commit = db.pool.clone();
    let sandbox_id = fixture.sandbox_id;
    let commit_task = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        memory_drop::commit(
            &pool_for_commit,
            sandbox_id,
            token,
            &candidate,
            LocalArtifactId::from_uuid(Uuid::now_v7()),
        )
        .await
    });

    let pod_creates = Arc::new(Mutex::new(Vec::new()));
    let substrate = substrate_with_store_and_client(
        db.pool.clone(),
        mock_kubernetes_client(pod_creates.clone()),
    );
    let controller = SandboxController::new(
        crate::controllers::substrate::Substrate::from_kubernetes(substrate),
    );
    let sandbox = crate::controllers::sandbox::fetch_sandbox(&db.pool, fixture.sandbox_id)
        .await?
        .expect("sandbox row exists");
    let error = controller
        .action(
            &db.pool,
            fixture.sandbox_id,
            Some(&sandbox),
            SandboxApiMetricIdentity::native("restore"),
            SandboxAction::Restore,
            Some(crate::models::SandboxActivationRequest {
                blocking: Some(true),
                ready_timeout_secs: Some(5),
            }),
            crate::controllers::substrate::ActivationClient::Native,
        )
        .await
        .expect_err("disk-pinned row awaits disk cold-boot wiring");

    assert!(commit_task.await??);
    assert_eq!(error.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert_ne!(
        error.retry_after_cause(),
        Some(ControllerRetryAfterCause::RestoreInFlight),
        "wake must wait past the seal and observe the committed disk pin: {error:?}"
    );
    assert!(pod_creates.lock().expect("pod creates").is_empty());
    Ok(())
}

#[tokio::test]
async fn drop_for_restore_rewrites_disk_and_parks_suspended_under_restore_lease() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let objects = Arc::new(MapObjects::default());
    let fixture = Fixture::insert(&db.pool, &objects).await?;
    let token = Uuid::now_v7();
    pplx_space_sandbox_store::restore_lease::claim_user_restore(
        &db.pool,
        fixture.sandbox_id,
        300,
        token,
        60,
        None,
    )
    .await?
    .expect("claim user restore");

    let drop = MemoryDrop::new(objects.clone());
    let outcome = drop
        .drop_for_restore(&db.pool, fixture.sandbox_id, token)
        .await?;
    let MemoryDropOutcome::Dropped(dropped) = outcome else {
        panic!("expected Dropped, got {outcome:?}");
    };
    assert_eq!(dropped.retained_snapshot_id, fixture.snapshot_id);
    assert_eq!(dropped.retained_artifact_id, fixture.artifact_id);

    let (lifecycle, _artifact_id, kind) = fixture.lifecycle(&db.pool).await?;
    assert_eq!((lifecycle.as_str(), kind.as_str()), ("suspended", "disk"));
    assert!(memory_drop::dropped(&db.pool, fixture.sandbox_id).await?);
    assert!(!memory_drop::sealed(&db.pool, fixture.sandbox_id).await?);

    let disk_snapshot = pplx_space_snapshot_store::get(&db.pool, dropped.disk_snapshot_id)
        .await?
        .expect("disk snapshot registered");
    assert_eq!(disk_snapshot.kind(), SnapshotKind::Disk);
    assert_eq!(disk_snapshot.state(), SnapshotState::Ready);
    assert_eq!(
        disk_snapshot
            .execution_metadata()
            .and_then(|meta| meta.host_cpu()),
        None
    );
    Ok(())
}

#[tokio::test]
async fn drop_for_restore_reuses_ready_earlier_disk_and_waits_for_deleting_or_orphaned_prefix()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let objects = Arc::new(MapObjects::default());
    let fixture = Fixture::insert(&db.pool, &objects).await?;
    let drop = MemoryDrop::new(objects.clone());

    let MemoryDropOutcome::Dropped(first) = drop.drop_memory(&db.pool, fixture.sandbox_id).await?
    else {
        panic!("the first drop commits");
    };

    // 1. Re-pointing back to the retained full artifact and running
    // `drop_for_restore` reuses the registered `Ready` disk without copying.
    repoint_to_full(&db.pool, &fixture).await?;
    let copies = objects.copies.load(Ordering::SeqCst);
    let token = Uuid::now_v7();
    pplx_space_sandbox_store::restore_lease::claim_user_restore(
        &db.pool,
        fixture.sandbox_id,
        300,
        token,
        60,
        None,
    )
    .await?
    .expect("claim user restore");

    let MemoryDropOutcome::Dropped(second) = drop
        .drop_for_restore(&db.pool, fixture.sandbox_id, token)
        .await?
    else {
        panic!("restore re-drop commits");
    };
    assert_eq!(second, first);
    assert_eq!(objects.copies.load(Ordering::SeqCst), copies);
    assert_eq!(
        pin_of(&db.pool, &fixture).await?,
        (
            "suspended".to_string(),
            first.disk_artifact_id.as_uuid(),
            "disk".to_string()
        )
    );

    // 2. While retention is deleting the earlier disk (`state = 'deleting'`),
    // `drop_for_restore` returns 503 without copying into the deleting prefix.
    repoint_to_full(&db.pool, &fixture).await?;
    sqlx::query(sql!(
        "UPDATE snapshots SET state = 'deleting' WHERE id = $1"
    ))
    .bind(first.disk_snapshot_id.as_uuid())
    .execute(&db.pool)
    .await?;
    let token_deleting = Uuid::now_v7();
    pplx_space_sandbox_store::restore_lease::claim_user_restore(
        &db.pool,
        fixture.sandbox_id,
        300,
        token_deleting,
        60,
        None,
    )
    .await?
    .expect("claim user restore while disk deleting");
    let err_deleting = drop
        .drop_for_restore(&db.pool, fixture.sandbox_id, token_deleting)
        .await
        .expect_err("deleting disk must refuse retryably without copying");
    assert_eq!(
        err_deleting.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(objects.copies.load(Ordering::SeqCst), copies);
    assert!(
        pplx_space_sandbox_store::memory_drop::restore::park_unplaced_restore_as_suspended(
            &db.pool,
            fixture.sandbox_id,
            token_deleting,
        )
        .await?
    );

    // 3. Once retention finishes deleting the disk row, an unswept orphan row
    // for the same prefix also refuses retryably without copying.
    sqlx::query(sql!(
        "DELETE FROM snapshot_storage_locations WHERE snapshot_id = $1"
    ))
    .bind(first.disk_snapshot_id.as_uuid())
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!("DELETE FROM snapshots WHERE id = $1"))
        .bind(first.disk_snapshot_id.as_uuid())
        .execute(&db.pool)
        .await?;
    let candidate = memory_drop::candidate(&db.pool, fixture.sandbox_id, CandidateRead::Plain)
        .await?
        .expect("eligible");
    let plan = CopyPlan::new(fixture.sandbox_id, &candidate).expect("plan");
    assert!(
        pplx_space_snapshot_store::orphan::orphan_unowned_location(
            &db.pool,
            plan.disk_snapshot_id,
            &plan.dest,
            candidate.org_id,
        )
        .await?
    );
    let token_orphan = Uuid::now_v7();
    pplx_space_sandbox_store::restore_lease::claim_user_restore(
        &db.pool,
        fixture.sandbox_id,
        300,
        token_orphan,
        60,
        None,
    )
    .await?
    .expect("claim user restore with pending orphan");
    let err_orphan = drop
        .drop_for_restore(&db.pool, fixture.sandbox_id, token_orphan)
        .await
        .expect_err("unswept orphan prefix must refuse retryably without copying");
    assert_eq!(
        err_orphan.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(objects.copies.load(Ordering::SeqCst), copies);
    Ok(())
}
