//! Which worker nodes hold a template campaign toward a target.

use super::*;

/// `node`'s snapshot is `seed`, lists `usable` as its usable environments,
/// and carries `node_ctl`, a JSON document or none.
async fn snapshot(
    pool: &PgPool,
    node: NodeId,
    seed: SnapshotSeed,
    usable: &[EnvironmentReference],
    node_ctl: Option<&str>,
) -> TestResult {
    upsert_snapshot(pool, node, seed).await?;
    let usable: Vec<String> = usable.iter().map(ToString::to_string).collect();
    sqlx::query(
        "UPDATE node_status_snapshots
         SET usable_environment_refs = $2, node_ctl = $3::jsonb
         WHERE node_id = $1",
    )
    .bind(node)
    .bind(&usable)
    .bind(node_ctl)
    .execute(pool)
    .await?;
    Ok(())
}

const READS: &str = r#"{"reachable": true, "reads_twin_metadata": true}"#;
const PREDATES: &str = r#"{"reachable": true, "reconciliations": []}"#;

/// Every worker of the substrate holds the campaign until its
/// template-manager reports reading twin metadata, whatever environments its
/// snapshot lists or its drain phase: a create can land wherever node-ctl's
/// labels allow, and a draining node can still admit pods. One with a stale
/// or missing snapshot holds it too, and so does having no worker at all. A
/// builder, another substrate's worker and a deleted worker's row don't.
#[tokio::test(flavor = "current_thread")]
async fn every_worker_holds_until_it_reads_twins() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (target, other) = (environment(1), environment(2));
    let count = || async {
        workers_not_reading_twins(&db.pool, "k8s")
            .await
            .map(|readers| readers.holding)
    };
    let none = workers_not_reading_twins(&db.pool, "k8s").await?;
    assert_eq!((none.workers, none.holding), (0, 0));
    assert!(none.holds(), "no worker is no evidence");

    let worker = NodeId::new();
    insert_ready_worker_node(&db.pool, worker).await?;
    assert_eq!(count().await?, 1, "a worker with no snapshot");
    let fresh = SnapshotSeed::default();
    snapshot(&db.pool, worker, fresh, &[target], Some(PREDATES)).await?;
    assert_eq!(count().await?, 1, "a node that predates the report");
    snapshot(&db.pool, worker, fresh, &[target], None).await?;
    assert_eq!(count().await?, 1, "no node-ctl document");
    snapshot(&db.pool, worker, fresh, &[other], Some(PREDATES)).await?;
    assert_eq!(
        count().await?,
        1,
        "its snapshot doesn't list the target yet"
    );
    snapshot(&db.pool, worker, fresh, &[target], Some(READS)).await?;
    assert_eq!(count().await?, 0, "it reads twins");
    snapshot(&db.pool, worker, fresh, &[other], Some(READS)).await?;
    assert_eq!(
        count().await?,
        0,
        "a reader holds nothing, whatever environments it lists"
    );
    snapshot(&db.pool, worker, fresh, &[target], Some(READS)).await?;
    let readers = workers_not_reading_twins(&db.pool, "k8s").await?;
    assert!(!readers.holds());
    assert_eq!((readers.workers, readers.sample.len()), (1, 0));
    let stale = SnapshotSeed {
        stale_in_secs: -1.0,
        ..fresh
    };
    snapshot(&db.pool, worker, stale, &[target], Some(READS)).await?;
    assert_eq!(
        count().await?,
        1,
        "a stale snapshot holds, whatever it says"
    );
    let readers = workers_not_reading_twins(&db.pool, "k8s").await?;
    assert_eq!(readers.sample, vec![worker.as_uuid()], "names the holder");

    snapshot(&db.pool, worker, fresh, &[target], Some(PREDATES)).await?;
    sqlx::query("UPDATE nodes SET drain_phase = 'draining' WHERE id = $1")
        .bind(worker)
        .execute(&db.pool)
        .await?;
    assert_eq!(count().await?, 1, "a draining worker can still admit pods");
    for (column, value) in [
        ("role", "builder"),
        ("managed_by", "space"),
        ("state", "deleted"),
    ] {
        let set = format!("UPDATE nodes SET {column} = $2 WHERE id = $1");
        sqlx::query(sqlx::AssertSqlSafe(set))
            .bind(worker)
            .bind(value)
            .execute(&db.pool)
            .await?;
        assert_eq!(count().await?, 0, "{column} = {value}");
        sqlx::query(
            "UPDATE nodes SET role = 'worker', managed_by = 'k8s', state = 'ready' WHERE id = $1",
        )
        .bind(worker)
        .execute(&db.pool)
        .await?;
        assert_eq!(count().await?, 1);
    }
    Ok(())
}
