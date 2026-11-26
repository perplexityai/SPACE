//! The node row locks against Postgres: what each reports, which other
//! locks it holds off while the caller's transaction is open, and that it
//! ends with that transaction.

use pplx_space_db_test_fixtures::{TestDbResult, insert_ready_worker_node, pg_test_db};
use sqlx::PgPool;

use super::*;

/// A row lock another transaction asks for without waiting.
#[derive(Clone, Copy, Debug)]
enum Probe {
    NodeUpdate,
    NodeNoKeyUpdate,
    NodeShare,
    NodeKeyShare,
    RegistrationUpdate,
    RegistrationNoKeyUpdate,
    RegistrationShare,
}

/// Whether another transaction can take `probe` on `node_id`'s row now.
async fn acquirable(pool: &PgPool, probe: Probe, node_id: NodeId) -> TestDbResult<bool> {
    let mut other = pool.begin().await?;
    let statement = match probe {
        Probe::NodeUpdate => sql!("SELECT 1 FROM nodes WHERE id = $1 FOR UPDATE NOWAIT"),
        Probe::NodeNoKeyUpdate => {
            sql!("SELECT 1 FROM nodes WHERE id = $1 FOR NO KEY UPDATE NOWAIT")
        }
        Probe::NodeShare => sql!("SELECT 1 FROM nodes WHERE id = $1 FOR SHARE NOWAIT"),
        Probe::NodeKeyShare => sql!("SELECT 1 FROM nodes WHERE id = $1 FOR KEY SHARE NOWAIT"),
        Probe::RegistrationUpdate => {
            sql!("SELECT 1 FROM node_k8s WHERE node_id = $1 FOR UPDATE NOWAIT")
        }
        Probe::RegistrationNoKeyUpdate => {
            sql!("SELECT 1 FROM node_k8s WHERE node_id = $1 FOR NO KEY UPDATE NOWAIT")
        }
        Probe::RegistrationShare => {
            sql!("SELECT 1 FROM node_k8s WHERE node_id = $1 FOR SHARE NOWAIT")
        }
    };
    let result = sqlx::query(statement)
        .bind(node_id)
        .execute(&mut *other)
        .await;
    other.rollback().await?;
    match result {
        Ok(_) => Ok(true),
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("55P03") => Ok(false),
        Err(error) => Err(error.into()),
    }
}

async fn node(pool: &PgPool) -> TestDbResult<NodeId> {
    let node_id = NodeId::new();
    insert_ready_worker_node(pool, node_id).await?;
    Ok(node_id)
}

async fn register(pool: &PgPool, node_id: NodeId, uid: Option<&str>) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, $2, $3)"
    ))
    .bind(node_id)
    .bind(format!("space-test-lock-{}", node_id.as_uuid().simple()))
    .bind(uid)
    .execute(pool)
    .await?;
    Ok(())
}

/// The shared lock reports whether the row exists, admits other shared
/// locks, holds off the lock a plain UPDATE takes and an update lock until
/// the transaction ends, and on a missing row locks nothing.
#[tokio::test]
async fn share_lock_node_holds_off_writers_until_the_transaction_ends() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let node_id = node(&db.pool).await?;

    let mut tx = db.pool.begin().await?;
    assert!(!share_lock_node(&mut tx, NodeId::new()).await?);
    assert!(share_lock_node(&mut tx, node_id).await?);
    assert!(acquirable(&db.pool, Probe::NodeShare, node_id).await?);
    assert!(!acquirable(&db.pool, Probe::NodeUpdate, node_id).await?);
    assert!(!acquirable(&db.pool, Probe::NodeNoKeyUpdate, node_id).await?);
    tx.commit().await?;

    assert!(acquirable(&db.pool, Probe::NodeUpdate, node_id).await?);
    assert!(acquirable(&db.pool, Probe::NodeNoKeyUpdate, node_id).await?);
    Ok(())
}

/// The update lock reports whether the row exists and holds off even a
/// key-share lock until the transaction ends; on a missing row it locks
/// nothing.
#[tokio::test]
async fn lock_node_for_update_holds_until_the_transaction_ends() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let node_id = node(&db.pool).await?;

    let mut tx = db.pool.begin().await?;
    assert!(!lock_node_for_update(&mut tx, NodeId::new()).await?);
    assert!(lock_node_for_update(&mut tx, node_id).await?);
    assert!(!acquirable(&db.pool, Probe::NodeKeyShare, node_id).await?);
    tx.rollback().await?;

    assert!(acquirable(&db.pool, Probe::NodeKeyShare, node_id).await?);
    Ok(())
}

/// The no-key-update lock reports whether the row exists and holds off a
/// shared lock until the transaction ends, but admits the key-share lock a
/// foreign key takes.
#[tokio::test]
async fn lock_node_for_no_key_update_admits_key_share_locks() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let node_id = node(&db.pool).await?;

    let mut tx = db.pool.begin().await?;
    assert!(!lock_node_for_no_key_update(&mut tx, NodeId::new()).await?);
    assert!(lock_node_for_no_key_update(&mut tx, node_id).await?);
    assert!(acquirable(&db.pool, Probe::NodeKeyShare, node_id).await?);
    assert!(!acquirable(&db.pool, Probe::NodeShare, node_id).await?);
    tx.commit().await?;

    assert!(acquirable(&db.pool, Probe::NodeShare, node_id).await?);
    Ok(())
}

/// The drain-phase lock matches only a Kubernetes row in the given phase
/// (`None` = outside any drain), and locks the row only when it matches.
#[tokio::test]
async fn share_lock_k8s_node_in_drain_phase_matches_the_phase_and_substrate() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let undrained = node(&db.pool).await?;
    let draining = node(&db.pool).await?;
    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = 'draining', drain_started_at = NOW() WHERE id = $1"
    ))
    .bind(draining)
    .execute(&db.pool)
    .await?;
    let direct = node(&db.pool).await?;
    sqlx::query(sql!("UPDATE nodes SET managed_by = 'space' WHERE id = $1"))
        .bind(direct)
        .execute(&db.pool)
        .await?;

    for (node_id, phase, expected) in [
        (undrained, None, true),
        (undrained, Some("draining"), false),
        (draining, Some("draining"), true),
        (draining, None, false),
        (direct, None, false),
    ] {
        let case = format!("{node_id} {phase:?}");
        let mut tx = db.pool.begin().await?;
        assert_eq!(
            share_lock_k8s_node_in_drain_phase(&mut tx, node_id, phase).await?,
            expected,
            "{case}"
        );
        assert!(
            acquirable(&db.pool, Probe::NodeShare, node_id).await?,
            "{case}"
        );
        assert_eq!(
            acquirable(&db.pool, Probe::NodeUpdate, node_id).await?,
            !expected,
            "{case}"
        );
        assert_eq!(
            acquirable(&db.pool, Probe::NodeNoKeyUpdate, node_id).await?,
            !expected,
            "{case}"
        );
        tx.commit().await?;
        assert!(
            acquirable(&db.pool, Probe::NodeUpdate, node_id).await?,
            "{case}"
        );
    }
    Ok(())
}

/// The registration lock matches only the given UID (`None` = no UID), and
/// locks the `node_k8s` row only when it matches.
#[tokio::test]
async fn share_lock_k8s_registration_with_uid_matches_the_uid() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let with_uid = node(&db.pool).await?;
    register(&db.pool, with_uid, Some("uid-1")).await?;
    let without_uid = node(&db.pool).await?;
    register(&db.pool, without_uid, None).await?;
    let unregistered = node(&db.pool).await?;

    for (node_id, uid, expected) in [
        (with_uid, Some("uid-1"), true),
        (with_uid, Some("uid-2"), false),
        (with_uid, None, false),
        (without_uid, None, true),
        (without_uid, Some("uid-1"), false),
        (unregistered, None, false),
    ] {
        let case = format!("{node_id} {uid:?}");
        let mut tx = db.pool.begin().await?;
        assert_eq!(
            share_lock_k8s_registration_with_uid(&mut tx, node_id, uid).await?,
            expected,
            "{case}"
        );
        assert_eq!(
            acquirable(&db.pool, Probe::RegistrationUpdate, node_id).await?,
            !expected,
            "{case}"
        );
        assert_eq!(
            acquirable(&db.pool, Probe::RegistrationNoKeyUpdate, node_id).await?,
            !expected,
            "{case}"
        );
        tx.commit().await?;
        assert!(
            acquirable(&db.pool, Probe::RegistrationUpdate, node_id).await?,
            "{case}"
        );
    }
    Ok(())
}

/// The identity lock reads the registration's uid and instance (`None`
/// without a registration), admits other shared locks, and holds off the
/// locks a registration's update takes until the transaction ends.
#[tokio::test]
async fn share_lock_k8s_identity_reads_and_holds_the_registration() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let registered = node(&db.pool).await?;
    register(&db.pool, registered, Some("uid-1")).await?;
    sqlx::query(sql!(
        "UPDATE node_k8s SET instance_id = 'i-0000000000000a0a0' WHERE node_id = $1"
    ))
    .bind(registered)
    .execute(&db.pool)
    .await?;
    let unregistered = node(&db.pool).await?;

    let mut tx = db.pool.begin().await?;
    assert_eq!(share_lock_k8s_identity(&mut tx, unregistered).await?, None);
    assert_eq!(
        share_lock_k8s_identity(&mut tx, registered).await?,
        Some((
            Some("uid-1".to_owned()),
            Some("i-0000000000000a0a0".to_owned())
        ))
    );
    assert!(acquirable(&db.pool, Probe::RegistrationShare, registered).await?);
    assert!(!acquirable(&db.pool, Probe::RegistrationUpdate, registered).await?);
    assert!(!acquirable(&db.pool, Probe::RegistrationNoKeyUpdate, registered).await?);
    assert!(acquirable(&db.pool, Probe::NodeUpdate, registered).await?);
    tx.commit().await?;

    assert!(acquirable(&db.pool, Probe::RegistrationUpdate, registered).await?);
    assert!(acquirable(&db.pool, Probe::RegistrationNoKeyUpdate, registered).await?);
    Ok(())
}
