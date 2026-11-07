//! Recording a node's EC2 instance, through registration and through direct
//! calls of the store function, against Postgres: when a superseded instance
//! is kept, with which UID, and that concurrent writers keep each replacement
//! once.

use std::time::Duration;

use pplx_space_common_core::types::{NodeId, NodeRole};
use pplx_space_db_test_fixtures::{TestDb, TestDbResult, pg_test_db_with_max_connections};
use pplx_space_sql_locator::sql;
use sqlx::{PgConnection, PgPool};

use super::*;
use crate::instance::{InstanceRecorded, record_k8s_node_instance};

const NAME: &str = "space-test-instance-node";

fn registration<'a>(
    node_id: NodeId,
    uid: &'a str,
    instance_id: Option<&'a str>,
) -> K8sNodeRegistration<'a> {
    K8sNodeRegistration {
        node_id,
        name: NAME,
        uid,
        role: NodeRole::Worker,
        address: Some("10.0.0.10"),
        state: "ready",
        manage_state: true,
        instance_id,
    }
}

/// Register in a transaction of its own, committed only on success.
async fn register(
    pool: &PgPool,
    registration: K8sNodeRegistration<'_>,
) -> TestDbResult<Result<bool, K8sNodeRegistrationError>> {
    let mut tx = pool.begin().await?;
    let result = register_k8s_node(&mut tx, registration, || {}).await;
    if result.is_ok() {
        tx.commit().await?;
    }
    Ok(result)
}

async fn registered(pool: &PgPool, registration: K8sNodeRegistration<'_>) -> TestDbResult<bool> {
    Ok(register(pool, registration).await??)
}

#[derive(Debug, PartialEq, Eq)]
struct NodeK8s {
    k8s_node_uid: Option<String>,
    instance_id: Option<String>,
    /// The row version: unchanged means no statement wrote the row.
    version: String,
}

async fn node_k8s(executor: &mut PgConnection, node_id: NodeId) -> TestDbResult<Option<NodeK8s>> {
    let row: Option<(Option<String>, Option<String>)> = sqlx::query_as(sql!(
        "SELECT k8s_node_uid, instance_id FROM node_k8s WHERE node_id = $1"
    ))
    .bind(node_id)
    .fetch_optional(&mut *executor)
    .await?;
    let Some((k8s_node_uid, instance_id)) = row else {
        return Ok(None);
    };
    // A schema variant can make `node_k8s` a view, which has no `xmin`.
    let version = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(format!(
        "SELECT xmin::TEXT FROM {} WHERE node_id = $1",
        pplx_space_db_test_fixtures::physical_table("node_k8s")?
    ))))
    .bind(node_id)
    .fetch_one(&mut *executor)
    .await?;
    Ok(Some(NodeK8s {
        k8s_node_uid,
        instance_id,
        version,
    }))
}

/// `(instance_id, k8s_node_uid)` of each superseded instance, oldest first.
async fn superseded(
    executor: &mut PgConnection,
    node_id: NodeId,
) -> TestDbResult<Vec<(String, Option<String>)>> {
    Ok(sqlx::query_as(sql!(
        "SELECT instance_id, k8s_node_uid FROM node_k8s_superseded_instances
         WHERE node_id = $1 ORDER BY superseded_at, id"
    ))
    .bind(node_id)
    .fetch_all(executor)
    .await?)
}

fn kept(rows: &[(&str, &str)]) -> Vec<(String, Option<String>)> {
    rows.iter()
        .map(|(instance, uid)| ((*instance).to_string(), Some((*uid).to_string())))
        .collect()
}

async fn db() -> TestDbResult<TestDb> {
    // Two writers and the connection that probes for a wait.
    pg_test_db_with_max_connections(4).await
}

/// A node's first instance, on a new row or on a row registered before
/// instances were recorded, is recorded and supersedes nothing.
#[tokio::test]
async fn a_first_instance_is_recorded_without_a_superseded_row() -> TestDbResult<()> {
    let db = db().await?;
    let mut conn = db.pool.acquire().await?;

    let new_node = NodeId::new();
    assert!(registered(&db.pool, registration(new_node, "uid-1", Some("i-0a"))).await?);
    assert_eq!(
        node_k8s(&mut conn, new_node)
            .await?
            .unwrap()
            .instance_id
            .as_deref(),
        Some("i-0a")
    );
    assert_eq!(superseded(&mut conn, new_node).await?, vec![]);

    let older_row = NodeId::new();
    assert!(registered(&db.pool, registration(older_row, "uid-1", None)).await?);
    assert_eq!(
        node_k8s(&mut conn, older_row).await?.unwrap().instance_id,
        None
    );
    assert!(registered(&db.pool, registration(older_row, "uid-1", Some("i-0a"))).await?);
    assert_eq!(
        node_k8s(&mut conn, older_row)
            .await?
            .unwrap()
            .instance_id
            .as_deref(),
        Some("i-0a")
    );
    assert_eq!(superseded(&mut conn, older_row).await?, vec![]);
    Ok(())
}

/// The recorded instance again, or no instance (a caller that has none),
/// writes nothing: not the row, not a superseded instance.
#[tokio::test]
async fn the_same_instance_or_none_writes_nothing() -> TestDbResult<()> {
    let db = db().await?;
    let mut conn = db.pool.acquire().await?;
    let node_id = NodeId::new();
    registered(&db.pool, registration(node_id, "uid-1", Some("i-0a"))).await?;
    let before = node_k8s(&mut conn, node_id).await?.unwrap();

    for instance_id in [Some("i-0a"), None] {
        assert!(
            !registered(&db.pool, registration(node_id, "uid-1", instance_id)).await?,
            "{instance_id:?}"
        );
        assert_eq!(
            node_k8s(&mut conn, node_id).await?.unwrap(),
            before,
            "{instance_id:?}"
        );
        assert_eq!(
            superseded(&mut conn, node_id).await?,
            vec![],
            "{instance_id:?}"
        );

        assert_eq!(
            record_k8s_node_instance(&mut conn, node_id, "i-0a").await?,
            InstanceRecorded::Unchanged
        );
        assert_eq!(node_k8s(&mut conn, node_id).await?.unwrap(), before);
    }

    // No instance keeps the recorded one through a UID change too.
    registered(&db.pool, registration(node_id, "uid-2", None)).await?;
    let after = node_k8s(&mut conn, node_id).await?.unwrap();
    assert_eq!(after.k8s_node_uid.as_deref(), Some("uid-2"));
    assert_eq!(after.instance_id.as_deref(), Some("i-0a"));
    assert_eq!(superseded(&mut conn, node_id).await?, vec![]);
    Ok(())
}

/// Another instance keeps the old one with the UID the row held then, in
/// the registration's transaction: inside it both writes are visible, and
/// outside it neither is until it commits.
#[tokio::test]
async fn another_instance_keeps_the_old_one_with_its_uid() -> TestDbResult<()> {
    let db = db().await?;
    let mut conn = db.pool.acquire().await?;
    let node_id = NodeId::new();
    registered(&db.pool, registration(node_id, "uid-1", Some("i-0a"))).await?;

    let started: f64 = sqlx::query_scalar(sql!("SELECT EXTRACT(EPOCH FROM NOW())::FLOAT8"))
        .fetch_one(&mut *conn)
        .await?;
    let mut tx = db.pool.begin().await?;
    assert!(register_k8s_node(&mut tx, registration(node_id, "uid-2", Some("i-0b")), || {}).await?);
    let inside = node_k8s(&mut tx, node_id).await?.unwrap();
    assert_eq!(
        (
            inside.k8s_node_uid.as_deref(),
            inside.instance_id.as_deref()
        ),
        (Some("uid-2"), Some("i-0b"))
    );
    assert_eq!(
        superseded(&mut tx, node_id).await?,
        kept(&[("i-0a", "uid-1")])
    );
    assert_eq!(
        node_k8s(&mut conn, node_id)
            .await?
            .unwrap()
            .instance_id
            .as_deref(),
        Some("i-0a")
    );
    assert_eq!(superseded(&mut conn, node_id).await?, vec![]);
    tx.commit().await?;

    assert_eq!(
        superseded(&mut conn, node_id).await?,
        kept(&[("i-0a", "uid-1")])
    );
    let superseded_at: f64 = sqlx::query_scalar(sql!(
        "SELECT EXTRACT(EPOCH FROM superseded_at)::FLOAT8
         FROM node_k8s_superseded_instances WHERE node_id = $1"
    ))
    .bind(node_id)
    .fetch_one(&mut *conn)
    .await?;
    assert!(superseded_at >= started, "{superseded_at} < {started}");

    // And again: each replacement is kept, the latest last.
    registered(&db.pool, registration(node_id, "uid-3", Some("i-0c"))).await?;
    assert_eq!(
        superseded(&mut conn, node_id).await?,
        kept(&[("i-0a", "uid-1"), ("i-0b", "uid-2")])
    );
    Ok(())
}

/// A tombstoned node refuses before anything is written, an instance
/// included.
#[tokio::test]
async fn a_tombstoned_node_refuses_without_recording_an_instance() -> TestDbResult<()> {
    let db = db().await?;
    let mut conn = db.pool.acquire().await?;
    let node_id = NodeId::new();
    registered(&db.pool, registration(node_id, "uid-1", Some("i-0a"))).await?;
    let before = node_k8s(&mut conn, node_id).await?.unwrap();
    sqlx::query(sql!(
        "INSERT INTO node_k8s_tombstones (node_id, k8s_node_name, role, reason)
         VALUES ($1, $2, 'worker', 'admin-terminate')"
    ))
    .bind(node_id)
    .bind(NAME)
    .execute(&mut *conn)
    .await?;

    let result = register(&db.pool, registration(node_id, "uid-2", Some("i-0b"))).await?;
    assert!(
        matches!(result, Err(K8sNodeRegistrationError::Tombstoned)),
        "{result:?}"
    );
    assert_eq!(node_k8s(&mut conn, node_id).await?.unwrap(), before);
    assert_eq!(superseded(&mut conn, node_id).await?, vec![]);
    Ok(())
}

/// A UID change clears drain state and an instance change keeps the old
/// instance, each without the other.
#[tokio::test]
async fn the_drain_clear_and_a_superseded_instance_are_independent() -> TestDbResult<()> {
    let db = db().await?;
    let mut conn = db.pool.acquire().await?;
    for (uid, instance_id) in [
        ("uid-1", "i-0a"),
        ("uid-2", "i-0a"),
        ("uid-1", "i-0b"),
        ("uid-2", "i-0b"),
    ] {
        let node_id = NodeId::new();
        registered(&db.pool, registration(node_id, "uid-1", Some("i-0a"))).await?;
        sqlx::query(sql!(
            "UPDATE nodes
             SET drain_phase = 'draining', drain_mode = 'gentle',
                 drain_started_at = NOW(), drain_last_error = 'seeded'
             WHERE id = $1"
        ))
        .bind(node_id)
        .execute(&mut *conn)
        .await?;

        let mut cleared = false;
        let mut tx = db.pool.begin().await?;
        register_k8s_node(
            &mut tx,
            registration(node_id, uid, Some(instance_id)),
            || cleared = true,
        )
        .await?;
        tx.commit().await?;

        let drain: (Option<String>, Option<String>) = sqlx::query_as(sql!(
            "SELECT drain_phase, drain_mode FROM nodes WHERE id = $1"
        ))
        .bind(node_id)
        .fetch_one(&mut *conn)
        .await?;
        let uid_changed = uid != "uid-1";
        assert_eq!(cleared, uid_changed, "{uid} {instance_id}");
        assert_eq!(
            drain,
            if uid_changed {
                (None, None)
            } else {
                (Some("draining".to_string()), Some("gentle".to_string()))
            },
            "{uid} {instance_id}"
        );
        assert_eq!(
            superseded(&mut conn, node_id).await?,
            if instance_id == "i-0a" {
                vec![]
            } else {
                kept(&[("i-0a", "uid-1")])
            },
            "{uid} {instance_id}"
        );
    }
    Ok(())
}

/// A direct call of the store function records the instance on the node's
/// existing row, and on no row creates nothing.
#[tokio::test]
async fn a_direct_record_writes_an_existing_row_only() -> TestDbResult<()> {
    let db = db().await?;
    let mut conn = db.pool.acquire().await?;

    let unregistered = NodeId::new();
    assert_eq!(
        record_k8s_node_instance(&mut conn, unregistered, "i-0a").await?,
        InstanceRecorded::Unchanged
    );
    assert_eq!(node_k8s(&mut conn, unregistered).await?, None);

    let node_id = NodeId::new();
    registered(&db.pool, registration(node_id, "uid-1", None)).await?;
    let registered_at = updated_at(&mut conn, node_id).await?;
    assert_eq!(
        record_k8s_node_instance(&mut conn, node_id, "i-0a").await?,
        InstanceRecorded::First
    );
    assert_eq!(
        record_k8s_node_instance(&mut conn, node_id, "i-0b").await?,
        InstanceRecorded::Superseded
    );
    let row = node_k8s(&mut conn, node_id).await?.unwrap();
    assert_eq!(row.instance_id.as_deref(), Some("i-0b"));
    assert_eq!(
        superseded(&mut conn, node_id).await?,
        kept(&[("i-0a", "uid-1")])
    );
    // An instance report doesn't date the row's Kubernetes metadata, which
    // absence demotion reads.
    assert_eq!(updated_at(&mut conn, node_id).await?, registered_at);
    Ok(())
}

async fn updated_at(conn: &mut PgConnection, node_id: NodeId) -> TestDbResult<String> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT updated_at::TEXT FROM node_k8s WHERE node_id = $1"
    ))
    .bind(node_id)
    .fetch_one(conn)
    .await?)
}

/// The backend pid of `holder`'s session, for [`wait_until_blocked_by`].
async fn backend_pid(holder: &mut PgConnection) -> TestDbResult<i32> {
    Ok(sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(holder)
        .await?)
}

/// Wait until some backend waits on a lock the session `holder_pid` holds.
///
/// Probe from a connection outside the holder's transaction. Within one
/// transaction Postgres keeps the session list it read first, so a waiter that
/// connects after the first probe would never be listed.
async fn wait_until_blocked_by(probe: &mut PgConnection, holder_pid: i32) -> TestDbResult<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let blocked: bool = sqlx::query_scalar(sql!(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_stat_activity
                      WHERE datname = current_database()
                        AND $1 = ANY(pg_blocking_pids(pid))
                 )"
            ))
            .bind(holder_pid)
            .fetch_one(&mut *probe)
            .await?;
            if blocked {
                return Ok::<(), sqlx::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum Writer {
    Registration(&'static str),
    /// A direct call of the store function, outside registration.
    Direct,
}

impl Writer {
    async fn write(
        self,
        conn: &mut PgConnection,
        node_id: NodeId,
        instance_id: &str,
    ) -> Result<(), String> {
        match self {
            Self::Registration(uid) => {
                register_k8s_node(conn, registration(node_id, uid, Some(instance_id)), || {})
                    .await
                    .map(|_| ())
                    .map_err(|error| error.to_string())
            }
            Self::Direct => record_k8s_node_instance(conn, node_id, instance_id)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
        }
    }
}

/// Two writers of one node's instance, the second waiting on the first's
/// open transaction. Whichever kinds they are, each replacement is kept
/// exactly once, with the UID of the row it replaced, and the second writer
/// supersedes what the first committed rather than what it read before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_writers_keep_each_replacement_once() -> TestDbResult<()> {
    let db = db().await?;
    let mut conn = db.pool.acquire().await?;
    #[allow(clippy::type_complexity)]
    let cases: [(Option<&str>, Writer, &str, Writer, &str, &[(&str, &str)]); 7] = [
        // Two replacements, by registration: the first is kept with its UID.
        (
            Some("i-0a"),
            Writer::Registration("uid-b"),
            "i-0b",
            Writer::Registration("uid-c"),
            "i-0c",
            &[("i-0a", "uid-a"), ("i-0b", "uid-b")],
        ),
        // The same replacement twice is kept once.
        (
            Some("i-0a"),
            Writer::Registration("uid-b"),
            "i-0b",
            Writer::Registration("uid-b"),
            "i-0b",
            &[("i-0a", "uid-a")],
        ),
        // A new node's two first registrations: the first one's instance is
        // a first sighting, the second replaces it.
        (
            None,
            Writer::Registration("uid-b"),
            "i-0b",
            Writer::Registration("uid-c"),
            "i-0c",
            &[("i-0b", "uid-b")],
        ),
        // A direct call and a registration, either one first. Two
        // registrations already serialize on the `nodes` row; these cases, and
        // the two below, are the ones that reach the `FOR UPDATE` re-read.
        (
            Some("i-0a"),
            Writer::Direct,
            "i-0b",
            Writer::Registration("uid-c"),
            "i-0c",
            &[("i-0a", "uid-a"), ("i-0b", "uid-a")],
        ),
        (
            Some("i-0a"),
            Writer::Registration("uid-b"),
            "i-0b",
            Writer::Direct,
            "i-0c",
            &[("i-0a", "uid-a"), ("i-0b", "uid-b")],
        ),
        // Two direct calls.
        (
            Some("i-0a"),
            Writer::Direct,
            "i-0b",
            Writer::Direct,
            "i-0c",
            &[("i-0a", "uid-a"), ("i-0b", "uid-a")],
        ),
        (
            Some("i-0a"),
            Writer::Direct,
            "i-0b",
            Writer::Direct,
            "i-0b",
            &[("i-0a", "uid-a")],
        ),
    ];
    for (seed, first, first_instance, second, second_instance, expected) in cases {
        let case = format!("{seed:?} {first:?} {first_instance} {second:?} {second_instance}");
        let node_id = NodeId::new();
        if let Some(seed) = seed {
            registered(&db.pool, registration(node_id, "uid-a", Some(seed))).await?;
        }

        let mut holder = db.pool.begin().await?;
        let holder_pid = backend_pid(&mut holder).await?;
        first.write(&mut holder, node_id, first_instance).await?;
        let pool = db.pool.clone();
        let second_instance_owned = second_instance.to_string();
        let waiter = tokio::spawn(async move {
            let mut tx = pool.begin().await.map_err(|error| error.to_string())?;
            second
                .write(&mut tx, node_id, &second_instance_owned)
                .await?;
            tx.commit().await.map_err(|error| error.to_string())
        });
        wait_until_blocked_by(&mut conn, holder_pid).await?;
        holder.commit().await?;
        tokio::time::timeout(Duration::from_secs(10), waiter).await???;

        assert_eq!(
            superseded(&mut conn, node_id).await?,
            kept(expected),
            "{case}"
        );
        assert_eq!(
            node_k8s(&mut conn, node_id)
                .await?
                .unwrap()
                .instance_id
                .as_deref(),
            Some(second_instance),
            "{case}"
        );
    }
    Ok(())
}

/// A replacement is dated when it is written, not when its transaction
/// began. The waiter's transaction begins first and the holder's replacement
/// commits first: the waiter's superseded row still dates after the
/// holder's. Both writers are direct calls of the store function, so the
/// waiter blocks on the `node_k8s` row, not on `nodes`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replacement_is_dated_when_it_is_written() -> TestDbResult<()> {
    let db = db().await?;
    let mut conn = db.pool.acquire().await?;
    let node_id = NodeId::new();
    registered(&db.pool, registration(node_id, "uid-1", Some("i-0a"))).await?;

    let mut waiter = db.pool.begin().await?;
    let waiter_began: f64 = sqlx::query_scalar(sql!("SELECT EXTRACT(EPOCH FROM NOW())::FLOAT8"))
        .fetch_one(&mut *waiter)
        .await?;
    let mut holder = db.pool.begin().await?;
    let holder_pid = backend_pid(&mut holder).await?;
    let holder_began: f64 = sqlx::query_scalar(sql!("SELECT EXTRACT(EPOCH FROM NOW())::FLOAT8"))
        .fetch_one(&mut *holder)
        .await?;
    assert!(
        waiter_began < holder_began,
        "{waiter_began} >= {holder_began}"
    );
    assert_eq!(
        record_k8s_node_instance(&mut holder, node_id, "i-0b").await?,
        InstanceRecorded::Superseded
    );

    let waiting = tokio::spawn(async move {
        let recorded = record_k8s_node_instance(&mut waiter, node_id, "i-0c").await?;
        waiter.commit().await?;
        Ok::<_, sqlx::Error>(recorded)
    });
    wait_until_blocked_by(&mut conn, holder_pid).await?;
    holder.commit().await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), waiting).await???,
        InstanceRecorded::Superseded
    );

    let by_date: Vec<String> = sqlx::query_scalar(sql!(
        "SELECT instance_id FROM node_k8s_superseded_instances
         WHERE node_id = $1 ORDER BY superseded_at"
    ))
    .bind(node_id)
    .fetch_all(&mut *conn)
    .await?;
    assert_eq!(by_date, ["i-0a", "i-0b"]);
    assert_eq!(
        node_k8s(&mut conn, node_id)
            .await?
            .unwrap()
            .instance_id
            .as_deref(),
        Some("i-0c")
    );
    Ok(())
}
