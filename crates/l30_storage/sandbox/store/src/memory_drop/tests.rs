use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::{LocalArtifactId, SnapshotId};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

use std::time::Duration;

use super::{
    CandidateRead, MemoryDropCandidate, MemoryDropRefusal, candidate, commit, dropped, pin_held,
    refusal, release, renew, seal,
};

const GONE_NODE_NAME: &str = "worker-gone";

/// An eligible row: a Kubernetes sandbox suspended off its node, pinned to a
/// full suspend artifact with one ready snapshot, whose last execution ran on
/// a node that no longer has a `node_k8s` mapping.
struct Fixture {
    sandbox_id: SandboxId,
    artifact_id: LocalArtifactId,
    snapshot_id: SnapshotId,
    target_node_id: NodeId,
}

impl Fixture {
    async fn insert(pool: &PgPool) -> TestResult<Self> {
        let sandbox_id = SandboxId::new();
        let artifact_id = LocalArtifactId::from_uuid(Uuid::now_v7());
        let target_node_id = NodeId::new();
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
        .bind(target_node_id)
        .bind(artifact_id)
        .execute(pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                (sandbox_id, generation, pod_name, pod_namespace, k8s_node_name,
                 state, active, terminated_at)
             VALUES ($1, 1, $2, 'space-sandboxes', $3, 'terminated', FALSE,
                     NOW() - INTERVAL '1 hour')"
        ))
        .bind(sandbox_id)
        .bind(format!("sbx-{sandbox_id}"))
        .bind(GONE_NODE_NAME)
        .execute(pool)
        .await?;
        let snapshot_id = insert_snapshot(pool, sandbox_id, artifact_id, "1 hour").await?;
        Ok(Self {
            sandbox_id,
            artifact_id,
            snapshot_id,
            target_node_id,
        })
    }

    async fn read(&self, pool: &PgPool) -> TestResult<Option<MemoryDropCandidate>> {
        Ok(candidate(pool, self.sandbox_id, CandidateRead::Plain).await?)
    }

    async fn refusal(&self, pool: &PgPool) -> TestResult<Option<MemoryDropRefusal>> {
        Ok(refusal(pool, self.sandbox_id).await?)
    }
}

/// A ready full suspend snapshot of `artifact_id`, taken `age` ago, with one
/// unorphaned storage location named after it.
async fn insert_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    artifact_id: LocalArtifactId,
    age: &str,
) -> TestResult<SnapshotId> {
    insert_snapshot_in(pool, sandbox_id, artifact_id, age, "ready").await
}

/// [`insert_snapshot`] in `state`.
async fn insert_snapshot_in(
    pool: &PgPool,
    sandbox_id: SandboxId,
    artifact_id: LocalArtifactId,
    age: &str,
    state: &str,
) -> TestResult<SnapshotId> {
    let snapshot_id = SnapshotId::from_uuid(Uuid::now_v7());
    sqlx::query(sql!(
        "INSERT INTO snapshots
            (id, sandbox_id, type, state, timestamp, local_artifact_id,
             artifact_kind, artifact_origin, org_id)
         VALUES ($1, $2, 'full', $6, NOW() - $3::INTERVAL, $4, 'full',
                 'suspend', $5::UUID)"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(age)
    .bind(artifact_id)
    .bind(SEED_ORG_ID)
    .bind(state)
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
    format!("s3://test-bucket/suspend_snapshots/00/{sandbox_id}/{snapshot_id}?region=us-east-1")
}

async fn insert_node(pool: &PgPool, node_id: NodeId, k8s_node_name: &str) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.40'::INET, 'worker', 'k8s', 'ready')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
    ))
    .bind(node_id)
    .bind(k8s_node_name)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn eligible_row_names_its_pinned_snapshot() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let fixture = Fixture::insert(&db.pool).await?;

    let read = fixture.read(&db.pool).await?.expect("eligible");

    assert_eq!(read.pinned_artifact_id, fixture.artifact_id);
    assert_eq!(read.full_snapshot_id, fixture.snapshot_id);
    assert_eq!(
        read.storage_connection_string,
        storage_uri(fixture.sandbox_id, fixture.snapshot_id)
    );
    Ok(())
}

#[tokio::test]
async fn newest_snapshot_of_the_pin_wins() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let fixture = Fixture::insert(&db.pool).await?;
    let newer = insert_snapshot(
        &db.pool,
        fixture.sandbox_id,
        fixture.artifact_id,
        "1 minute",
    )
    .await?;
    // A failed retry of the same artifact, newer still, doesn't count.
    insert_snapshot_in(
        &db.pool,
        fixture.sandbox_id,
        fixture.artifact_id,
        "1 second",
        "failed",
    )
    .await?;

    let read = fixture.read(&db.pool).await?.expect("eligible");

    assert_eq!(read.full_snapshot_id, newer);
    assert_eq!(fixture.refusal(&db.pool).await?, None);
    Ok(())
}

#[tokio::test]
async fn a_newer_full_snapshot_of_another_artifact_refuses_the_drop() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let fixture = Fixture::insert(&db.pool).await?;
    // Retention keeps the newest full snapshot, which wouldn't be the pinned
    // one, so the drop can't promise to retain it.
    insert_snapshot(
        &db.pool,
        fixture.sandbox_id,
        LocalArtifactId::from_uuid(Uuid::now_v7()),
        "1 second",
    )
    .await?;

    assert_eq!(fixture.read(&db.pool).await?, None);
    assert_eq!(
        fixture.refusal(&db.pool).await?,
        Some(MemoryDropRefusal::NewerFullSnapshot)
    );
    Ok(())
}

/// Each statement takes the eligible fixture out of eligibility one way.
/// `$1` is the sandbox id, `$2` its pinned snapshot id. The middle value is
/// the reason [`refusal`] gives for it.
const DISQUALIFIERS: &[(&str, MemoryDropRefusal, &str)] = &[
    (
        "not kubernetes",
        MemoryDropRefusal::NotKubernetes,
        "UPDATE sandboxes SET managed_by = 'space' WHERE id = $1 AND $2::UUID IS NOT NULL",
    ),
    (
        "not suspended",
        MemoryDropRefusal::NotSuspended,
        "UPDATE sandboxes SET lifecycle = 'stopped' WHERE id = $1 AND $2::UUID IS NOT NULL",
    ),
    (
        "restore lease",
        MemoryDropRefusal::NotSuspended,
        "UPDATE sandboxes
            SET lifecycle = 'restoring', restore_lease_token = gen_random_uuid(),
                restore_lease_expires_at = NOW() + INTERVAL '1 minute'
          WHERE id = $1 AND $2::UUID IS NOT NULL",
    ),
    (
        "evacuation claim",
        MemoryDropRefusal::Placed,
        "UPDATE sandboxes SET evacuation_restore_target = 'started'
          WHERE id = $1 AND $2::UUID IS NOT NULL",
    ),
    (
        "volume encrypted",
        MemoryDropRefusal::Encrypted,
        "UPDATE sandboxes SET volume_encryption = TRUE WHERE id = $1 AND $2::UUID IS NOT NULL",
    ),
    (
        "pinned to a disk artifact",
        MemoryDropRefusal::AlreadyDropped,
        "UPDATE sandboxes
            SET current_local_artifact_kind = 'disk', current_local_artifact_origin = 'stop'
          WHERE id = $1 AND $2::UUID IS NOT NULL",
    ),
    (
        "unpinned",
        MemoryDropRefusal::NotFullSuspendPin,
        "UPDATE sandboxes
            SET current_local_artifact_id = NULL, current_local_artifact_kind = NULL,
                current_local_artifact_origin = NULL
          WHERE id = $1 AND $2::UUID IS NOT NULL",
    ),
    (
        "snapshot not ready",
        MemoryDropRefusal::NoDurableCopy,
        "UPDATE snapshots SET state = 'creating' WHERE id = $2 AND $1::UUID IS NOT NULL",
    ),
    (
        "location orphaned",
        MemoryDropRefusal::NoDurableCopy,
        "UPDATE snapshot_storage_locations SET snapshot_id = NULL, orphaned_at = NOW()
          WHERE snapshot_id = $2 AND $1::UUID IS NOT NULL",
    ),
    (
        "active execution",
        MemoryDropRefusal::Placed,
        "UPDATE sandbox_k8s_executions SET active = TRUE, terminated_at = NULL
          WHERE sandbox_id = $1 AND $2::UUID IS NOT NULL",
    ),
];

#[tokio::test]
async fn each_disqualifier_makes_the_row_ineligible() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    assert_eq!(
        refusal(&db.pool, SandboxId::new()).await?,
        Some(MemoryDropRefusal::NotFound)
    );
    for (name, reason, statement) in DISQUALIFIERS {
        let fixture = Fixture::insert(&db.pool).await?;
        sqlx::query(sqlx::AssertSqlSafe(sql!(*statement)))
            .bind(fixture.sandbox_id)
            .bind(fixture.snapshot_id)
            .execute(&db.pool)
            .await?;
        assert_eq!(fixture.read(&db.pool).await?, None, "{name}");
        assert_eq!(fixture.refusal(&db.pool).await?, Some(*reason), "{name}");
    }
    Ok(())
}

#[tokio::test]
async fn active_node_makes_the_row_ineligible() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let fixture = Fixture::insert(&db.pool).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = target_node_id WHERE id = $1"
    ))
    .bind(fixture.sandbox_id)
    .execute(&db.pool)
    .await?;

    assert_eq!(fixture.read(&db.pool).await?, None);
    assert_eq!(
        fixture.refusal(&db.pool).await?,
        Some(MemoryDropRefusal::Placed)
    );
    Ok(())
}

#[tokio::test]
async fn a_live_target_node_makes_the_row_ineligible() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let fixture = Fixture::insert(&db.pool).await?;
    insert_node(&db.pool, fixture.target_node_id, "worker-target").await?;

    assert_eq!(fixture.read(&db.pool).await?, None);
    assert_eq!(
        fixture.refusal(&db.pool).await?,
        Some(MemoryDropRefusal::NodeNotGone)
    );
    Ok(())
}

#[tokio::test]
async fn a_live_last_execution_node_makes_the_row_ineligible() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let fixture = Fixture::insert(&db.pool).await?;
    insert_node(&db.pool, NodeId::new(), GONE_NODE_NAME).await?;

    assert_eq!(fixture.read(&db.pool).await?, None);
    assert_eq!(
        fixture.refusal(&db.pool).await?,
        Some(MemoryDropRefusal::NodeNotGone)
    );
    Ok(())
}

#[tokio::test]
async fn a_row_whose_executions_name_no_node_is_ineligible() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let fixture = Fixture::insert(&db.pool).await?;
    assert!(fixture.read(&db.pool).await?.is_some());
    // No node in the history means nothing to prove gone.
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET k8s_node_name = NULL WHERE sandbox_id = $1"
    ))
    .bind(fixture.sandbox_id)
    .execute(&db.pool)
    .await?;

    assert_eq!(fixture.read(&db.pool).await?, None);
    assert_eq!(
        fixture.refusal(&db.pool).await?,
        Some(MemoryDropRefusal::NodeNotGone)
    );
    Ok(())
}

#[tokio::test]
async fn only_the_newest_execution_node_counts() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let fixture = Fixture::insert(&db.pool).await?;
    // An older execution ran on a node that's still up; the newest didn't.
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, k8s_node_name,
             state, active, terminated_at)
         VALUES ($1, 0, 'sbx-old', 'space-sandboxes', 'worker-live',
                 'terminated', FALSE, NOW() - INTERVAL '1 day')"
    ))
    .bind(fixture.sandbox_id)
    .execute(&db.pool)
    .await?;
    insert_node(&db.pool, NodeId::new(), "worker-live").await?;

    assert!(fixture.read(&db.pool).await?.is_some());
    Ok(())
}

/// Seal the row with `token` the way the drop does: in a transaction that
/// commits before the copy. Returns the candidate, or `None` when the row
/// can't be sealed.
async fn seal_row(
    pool: &PgPool,
    sandbox_id: SandboxId,
    token: Uuid,
    ttl: Duration,
) -> TestResult<Option<MemoryDropCandidate>> {
    let mut tx = pool.begin().await?;
    let sealed = seal(&mut tx, sandbox_id, token, ttl).await?;
    tx.commit().await?;
    Ok(sealed)
}

/// The row's lifecycle, lifecycle clock and restore lease.
async fn lease_row(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<(String, OffsetDateTime, Option<Uuid>, Option<OffsetDateTime>)> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, lifecycle_changed_at, restore_lease_token,
                restore_lease_expires_at
           FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

const TTL: Duration = Duration::from_secs(60);

#[tokio::test]
async fn a_seal_takes_the_restore_lease_and_leaves_the_lifecycle_alone() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let fixture = Fixture::insert(&db.pool).await?;
    let (_, changed_before, _, _) = lease_row(&db.pool, fixture.sandbox_id).await?;
    let token = Uuid::now_v7();

    let sealed = seal_row(&db.pool, fixture.sandbox_id, token, TTL).await?;

    assert_eq!(
        sealed.map(|c| c.full_snapshot_id),
        Some(fixture.snapshot_id)
    );
    let (lifecycle, changed_at, lease, expires) = lease_row(&db.pool, fixture.sandbox_id).await?;
    assert_eq!(lifecycle, "suspended");
    assert_eq!(
        changed_at, changed_before,
        "the lifecycle clock didn't move"
    );
    assert_eq!(lease, Some(token));
    assert!(expires.expect("expiry") > OffsetDateTime::now_utc());
    assert!(super::sealed(&db.pool, fixture.sandbox_id).await?);
    assert_eq!(
        fixture.refusal(&db.pool).await?,
        Some(MemoryDropRefusal::Sealed)
    );
    assert_eq!(fixture.read(&db.pool).await?, None);
    // A second drop can't take a live seal.
    assert_eq!(
        seal_row(&db.pool, fixture.sandbox_id, Uuid::now_v7(), TTL).await?,
        None
    );
    assert_eq!(
        lease_row(&db.pool, fixture.sandbox_id).await?.2,
        Some(token)
    );
    Ok(())
}

#[tokio::test]
async fn an_expired_seal_is_taken_over_and_only_the_new_token_renews() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let fixture = Fixture::insert(&db.pool).await?;
    let dead = Uuid::now_v7();
    // A zero TTL expires at once: a drop that died mid-copy.
    assert!(
        seal_row(&db.pool, fixture.sandbox_id, dead, Duration::ZERO)
            .await?
            .is_some()
    );
    assert!(!super::sealed(&db.pool, fixture.sandbox_id).await?);
    assert_eq!(fixture.refusal(&db.pool).await?, None);

    let retry = Uuid::now_v7();
    assert!(
        seal_row(&db.pool, fixture.sandbox_id, retry, TTL)
            .await?
            .is_some()
    );

    assert!(!renew(&db.pool, fixture.sandbox_id, dead, TTL).await?);
    assert!(renew(&db.pool, fixture.sandbox_id, retry, TTL).await?);
    assert!(!release(&db.pool, fixture.sandbox_id, dead).await?);
    assert_eq!(
        lease_row(&db.pool, fixture.sandbox_id).await?.2,
        Some(retry)
    );
    Ok(())
}

#[tokio::test]
async fn an_expired_seal_nothing_replaced_still_renews() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let fixture = Fixture::insert(&db.pool).await?;
    let token = Uuid::now_v7();
    seal_row(&db.pool, fixture.sandbox_id, token, Duration::ZERO).await?;

    assert!(renew(&db.pool, fixture.sandbox_id, token, TTL).await?);
    assert!(super::sealed(&db.pool, fixture.sandbox_id).await?);
    Ok(())
}

#[tokio::test]
async fn release_lifts_the_seal_and_leaves_the_row_eligible() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let fixture = Fixture::insert(&db.pool).await?;
    let (_, changed_before, _, _) = lease_row(&db.pool, fixture.sandbox_id).await?;
    let token = Uuid::now_v7();
    seal_row(&db.pool, fixture.sandbox_id, token, TTL).await?;

    assert!(release(&db.pool, fixture.sandbox_id, token).await?);

    let (lifecycle, changed_at, lease, expires) = lease_row(&db.pool, fixture.sandbox_id).await?;
    assert_eq!(
        (lifecycle.as_str(), changed_at, lease, expires),
        ("suspended", changed_before, None, None)
    );
    assert!(fixture.read(&db.pool).await?.is_some());
    assert!(!release(&db.pool, fixture.sandbox_id, token).await?);
    Ok(())
}

#[tokio::test]
async fn commit_repins_the_row_lifts_the_seal_and_keeps_the_clock() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let fixture = Fixture::insert(&db.pool).await?;
    assert!(!dropped(&db.pool, fixture.sandbox_id).await?);
    let (_, changed_before, _, _) = lease_row(&db.pool, fixture.sandbox_id).await?;
    let token = Uuid::now_v7();
    let read = seal_row(&db.pool, fixture.sandbox_id, token, TTL)
        .await?
        .expect("eligible");
    let disk = LocalArtifactId::from_uuid(Uuid::now_v7());

    assert!(commit(&db.pool, fixture.sandbox_id, token, &read, disk).await?);

    let row: (String, String, Uuid, String, String) = sqlx::query_as(sql!(
        "SELECT lifecycle, placement, current_local_artifact_id,
                current_local_artifact_kind, current_local_artifact_origin
           FROM sandboxes WHERE id = $1"
    ))
    .bind(fixture.sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    let (lifecycle, placement, pin, kind, origin) = row;
    assert_eq!(
        (
            lifecycle.as_str(),
            placement.as_str(),
            pin,
            kind.as_str(),
            origin.as_str()
        ),
        ("suspended", "unplaced", disk.as_uuid(), "disk", "stop")
    );
    let (_, changed_at, lease, expires) = lease_row(&db.pool, fixture.sandbox_id).await?;
    assert_eq!(
        changed_at, changed_before,
        "the lifecycle clock didn't move"
    );
    assert_eq!((lease, expires), (None, None), "the seal is lifted");
    assert!(!pin_held(&db.pool, fixture.sandbox_id, &read).await?);
    assert!(dropped(&db.pool, fixture.sandbox_id).await?);
    assert_eq!(
        fixture.refusal(&db.pool).await?,
        Some(MemoryDropRefusal::AlreadyDropped)
    );

    // The full artifact is retained: still ready, its prefix still attached
    // and not handed to the sweeper.
    let retained: (String, Option<Uuid>, bool) = sqlx::query_as(sql!(
        "SELECT sn.state, ssl.snapshot_id, ssl.orphaned_at IS NOT NULL
           FROM snapshots sn
           JOIN snapshot_storage_locations ssl
             ON ssl.storage_connection_string = $2
          WHERE sn.id = $1"
    ))
    .bind(fixture.snapshot_id)
    .bind(storage_uri(fixture.sandbox_id, fixture.snapshot_id))
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        retained,
        (
            "ready".to_string(),
            Some(fixture.snapshot_id.as_uuid()),
            false
        )
    );
    Ok(())
}

#[tokio::test]
async fn commit_requires_the_seal_token() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let fixture = Fixture::insert(&db.pool).await?;
    let read = fixture.read(&db.pool).await?.expect("eligible");
    let disk = LocalArtifactId::from_uuid(Uuid::now_v7());
    // Unsealed: no token matches.
    assert!(!commit(&db.pool, fixture.sandbox_id, Uuid::now_v7(), &read, disk).await?);

    let token = Uuid::now_v7();
    seal_row(&db.pool, fixture.sandbox_id, token, TTL).await?;
    assert!(!commit(&db.pool, fixture.sandbox_id, Uuid::now_v7(), &read, disk).await?);
    assert!(!dropped(&db.pool, fixture.sandbox_id).await?);
    assert!(pin_held(&db.pool, fixture.sandbox_id, &read).await?);
    Ok(())
}

#[tokio::test]
async fn commit_loses_to_a_wake_that_reverted() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let fixture = Fixture::insert(&db.pool).await?;
    let token = Uuid::now_v7();
    let read = seal_row(&db.pool, fixture.sandbox_id, token, TTL)
        .await?
        .expect("eligible");
    // A wake claimed the row and a failed restore put it back: same lifecycle
    // and pin, and the write that left `restoring` cleared the lease.
    for lifecycle in ["restoring", "suspended"] {
        sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
            .bind(fixture.sandbox_id)
            .bind(lifecycle)
            .execute(&db.pool)
            .await?;
    }
    assert_eq!(lease_row(&db.pool, fixture.sandbox_id).await?.2, None);

    let disk = LocalArtifactId::from_uuid(Uuid::now_v7());
    assert!(!commit(&db.pool, fixture.sandbox_id, token, &read, disk).await?);
    assert!(!renew(&db.pool, fixture.sandbox_id, token, TTL).await?);
    // Still pinned to the same artifact, so a retry reuses the prefix.
    assert!(pin_held(&db.pool, fixture.sandbox_id, &read).await?);
    assert!(fixture.read(&db.pool).await?.is_some());
    Ok(())
}

#[tokio::test]
async fn a_write_that_keeps_the_row_suspended_keeps_the_seal() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let fixture = Fixture::insert(&db.pool).await?;
    let token = Uuid::now_v7();
    seal_row(&db.pool, fixture.sandbox_id, token, TTL).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET preserve_local_artifact_on_restore_failure = TRUE
          WHERE id = $1"
    ))
    .bind(fixture.sandbox_id)
    .execute(&db.pool)
    .await?;

    let (lease, preserve): (Option<Uuid>, bool) = sqlx::query_as(sql!(
        "SELECT restore_lease_token, preserve_local_artifact_on_restore_failure
           FROM sandboxes WHERE id = $1"
    ))
    .bind(fixture.sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(lease, Some(token));
    assert!(!preserve, "the trigger still resets the restore-only flag");
    Ok(())
}

#[tokio::test]
async fn no_restore_claim_takes_a_dropped_row() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let fixture = Fixture::insert(&db.pool).await?;
    let token = Uuid::now_v7();
    let read = seal_row(&db.pool, fixture.sandbox_id, token, TTL)
        .await?
        .expect("eligible");
    let disk = LocalArtifactId::from_uuid(Uuid::now_v7());
    assert!(commit(&db.pool, fixture.sandbox_id, token, &read, disk).await?);

    let user = crate::restore_lease::claim_user_restore(
        &db.pool,
        fixture.sandbox_id,
        60,
        Uuid::now_v7(),
        60,
        None,
    )
    .await?;
    assert!(user.is_none(), "user claim");

    // A drain marker doesn't open the evacuation claim either.
    set_drain_marker(&db.pool, fixture.sandbox_id).await?;
    let evacuation =
        crate::restore_lease::claim_evacuation_restore(&db.pool, fixture.sandbox_id, 60, 60, None)
            .await?;
    assert!(evacuation.is_none(), "evacuation claim");

    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(fixture.sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(lifecycle, "suspended");
    Ok(())
}

async fn set_drain_marker(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes SET evacuation_restore_target = 'started' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn no_restore_claim_takes_a_sealed_row_until_the_seal_expires() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let fixture = Fixture::insert(&db.pool).await?;
    let token = Uuid::now_v7();
    let read = seal_row(&db.pool, fixture.sandbox_id, token, TTL)
        .await?
        .expect("eligible");

    let user = crate::restore_lease::claim_user_restore(
        &db.pool,
        fixture.sandbox_id,
        60,
        Uuid::now_v7(),
        60,
        None,
    )
    .await?;
    assert!(user.is_none(), "user claim");
    // The drain marker sits on placed rows only, but the claim refuses a
    // sealed row regardless.
    set_drain_marker(&db.pool, fixture.sandbox_id).await?;
    let evacuation =
        crate::restore_lease::claim_evacuation_restore(&db.pool, fixture.sandbox_id, 60, 60, None)
            .await?;
    assert!(evacuation.is_none(), "evacuation claim");
    sqlx::query(sql!(
        "UPDATE sandboxes SET evacuation_restore_target = NULL WHERE id = $1"
    ))
    .bind(fixture.sandbox_id)
    .execute(&db.pool)
    .await?;

    // Once the seal lapses, a user claim takes the row, and the drop's commit
    // loses.
    sqlx::query(sql!(
        "UPDATE sandboxes SET restore_lease_expires_at = NOW() - INTERVAL '1 second'
          WHERE id = $1"
    ))
    .bind(fixture.sandbox_id)
    .execute(&db.pool)
    .await?;
    let user = crate::restore_lease::claim_user_restore(
        &db.pool,
        fixture.sandbox_id,
        60,
        Uuid::now_v7(),
        60,
        None,
    )
    .await?;
    assert!(user.is_some(), "user claim after the seal lapsed");
    let disk = LocalArtifactId::from_uuid(Uuid::now_v7());
    assert!(!commit(&db.pool, fixture.sandbox_id, token, &read, disk).await?);
    assert!(!dropped(&db.pool, fixture.sandbox_id).await?);
    Ok(())
}
