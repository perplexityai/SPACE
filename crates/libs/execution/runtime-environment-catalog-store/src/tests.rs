use std::collections::BTreeSet;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{Digest, EnvironmentReference, NodeId, VersionSortKey};
use pplx_space_db_test_fixtures::{
    insert_ready_worker_node, try_pg_test_db, try_pg_test_db_with_max_connections,
};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::*;

fn environment(byte: u8) -> EnvironmentReference {
    EnvironmentReference::new(Digest::sha256([byte; 32]))
}

/// Insert a catalog row as a node publish would, retired or not.
async fn insert(pool: &PgPool, environment: &str, retired: bool) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO runtime_environments (
             environment_ref, runtime, runtime_version, version_sort_key,
             kernel_version, state_format_name, state_format_version, retired_at
         )
         VALUES ($1, 'firecracker', '1.17.0', '{1,17,0}', '6.1.155',
                 'firecracker-snapshot', '12.0.0',
                 CASE WHEN $2 THEN now() END)"
    ))
    .bind(environment)
    .bind(retired)
    .execute(pool)
    .await?;
    Ok(())
}

async fn retire(pool: &PgPool, environment: EnvironmentReference) -> TestResult {
    sqlx::query(sql!(
        "UPDATE runtime_environments SET retired_at = now() WHERE environment_ref = $1"
    ))
    .bind(environment.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

/// A fresh status row as a publication leaves it, with `known` as the node's
/// known set (NULL when `None`).
async fn insert_status(
    pool: &PgPool,
    node_id: NodeId,
    known: Option<&[EnvironmentReference]>,
) -> TestResult {
    let known: Option<Vec<String>> =
        known.map(|known| known.iter().map(ToString::to_string).collect());
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots (
             node_id, observed_at, updated_at, stale_after, reachable, services, execution_inventory_status,
             execution_inventory_received_at, usable_environment_refs,
             known_environment_refs, environment_refs_received_at
         )
         VALUES ($1, now(), now(), now() + interval '150 seconds', TRUE, '[]', 'complete', now(),
                 CASE WHEN $2::TEXT[] IS NULL THEN NULL ELSE '{}'::TEXT[] END,
                 $2::TEXT[], CASE WHEN $2::TEXT[] IS NULL THEN NULL ELSE now() END)"
    ))
    .bind(node_id)
    .bind(known)
    .execute(pool)
    .await?;
    Ok(())
}

async fn node(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    insert_ready_worker_node(pool, node_id).await?;
    Ok(node_id)
}

#[tokio::test(flavor = "current_thread")]
async fn a_node_is_assigned_the_unretired_environments_it_knows() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    insert(&db.pool, &environment(2).to_string(), false).await?;
    insert(&db.pool, &environment(1).to_string(), true).await?;
    insert(&db.pool, &environment(0).to_string(), false).await?;
    insert(&db.pool, &environment(3).to_string(), false).await?;

    // No status row and a NULL known set both mean no report: the whole
    // unretired catalog, as before nodes reported.
    let unreported = node(&db.pool).await?;
    let all = vec![environment(0), environment(2), environment(3)];
    assert_eq!(list_unretired_for_node(&db.pool, unreported).await?, all);
    insert_status(&db.pool, unreported, None).await?;
    assert_eq!(list_unretired_for_node(&db.pool, unreported).await?, all);

    // A known set narrows it, and a known retired environment stays out.
    let reporting = node(&db.pool).await?;
    insert_status(
        &db.pool,
        reporting,
        Some(&[environment(1), environment(2), environment(9)]),
    )
    .await?;
    assert_eq!(
        list_unretired_for_node(&db.pool, reporting).await?,
        vec![environment(2)]
    );

    // An empty known set is a build that certifies nothing.
    let empty = node(&db.pool).await?;
    insert_status(&db.pool, empty, Some(&[])).await?;
    assert!(list_unretired_for_node(&db.pool, empty).await?.is_empty());

    assert_eq!(list_unretired_within(&db.pool, None).await?, all);
    assert_eq!(
        retired_among(
            &db.pool,
            &[environment(1), environment(2), environment(9)].map(|e| e.to_string())
        )
        .await?,
        BTreeSet::from([environment(1).to_string()])
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_unparseable_reference_fails_the_read() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    // The schema refuses a malformed reference, so drop its CHECK in this
    // isolated database to stand in for a row the parser rejects.
    sqlx::query(sql!(
        "ALTER TABLE runtime_environments
         DROP CONSTRAINT runtime_environments_environment_ref_check"
    ))
    .execute(&db.pool)
    .await?;
    insert(&db.pool, &environment(0).to_string(), false).await?;
    insert(&db.pool, "sha256:not-a-digest", false).await?;

    let node_id = node(&db.pool).await?;
    let error = list_unretired_for_node(&db.pool, node_id)
        .await
        .expect_err("a malformed reference must fail the whole read");
    assert!(
        matches!(
            &error,
            ListUnretiredError::InvalidEnvironmentReference { value, .. }
                if value == "sha256:not-a-digest"
        ),
        "{error:?}"
    );
    Ok(())
}

fn identity(runtime_version: &str) -> CatalogIdentity {
    CatalogIdentity {
        runtime: "fc".into(),
        runtime_version: runtime_version.into(),
        kernel_version: "6.1.155".into(),
        state_format_name: "firecracker-snapshot".into(),
        state_format_version: "12.0.0".into(),
    }
}

fn certified(
    reference: EnvironmentReference,
    upgrade: &[EnvironmentReference],
    memory: &[EnvironmentReference],
) -> CertifiedEnvironment {
    CertifiedEnvironment {
        reference,
        identity: Some(identity("1.17.0")),
        version_sort_key: Some(VersionSortKey::new(vec![1, 17, 0]).unwrap()),
        upgrade_edges: upgrade.iter().copied().collect(),
        memory_edges: memory.iter().copied().collect(),
    }
}

async fn record(
    pool: &PgPool,
    node_id: NodeId,
    certified: &[CertifiedEnvironment],
) -> TestResult<CertificationOutcome> {
    let mut tx = pool.begin().await?;
    let changed = record_node_certifications(&mut tx, node_id, certified).await?;
    tx.commit().await?;
    Ok(changed)
}

type Edges = (
    BTreeSet<EnvironmentReference>,
    BTreeSet<EnvironmentReference>,
);

async fn edges(pool: &PgPool, environment: EnvironmentReference) -> TestResult<Edges> {
    let (upgrade, memory): (Vec<String>, Vec<String>) = sqlx::query_as(sql!(
        "SELECT upgrade_edges, memory_edges FROM runtime_environments
         WHERE environment_ref = $1"
    ))
    .bind(environment.to_string())
    .fetch_one(pool)
    .await?;
    let parse = |edges: Vec<String>| edges.iter().map(|edge| edge.parse().unwrap()).collect();
    Ok((parse(upgrade), parse(memory)))
}

fn set(edges: &[EnvironmentReference]) -> BTreeSet<EnvironmentReference> {
    edges.iter().copied().collect()
}

#[tokio::test(flavor = "current_thread")]
async fn builds_that_disagree_keep_only_the_edges_every_judging_build_certifies() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (e0, e1, e2, e3) = (
        environment(0),
        environment(1),
        environment(2),
        environment(3),
    );
    let (a, b, c, d) = (
        node(&db.pool).await?,
        node(&db.pool).await?,
        node(&db.pool).await?,
        node(&db.pool).await?,
    );
    // A certifies E0 to E1 (with memory) and to E2.
    assert_eq!(
        record(
            &db.pool,
            a,
            &[
                certified(e0, &[e1, e2], &[e1]),
                certified(e1, &[], &[]),
                certified(e2, &[], &[]),
            ],
        )
        .await?,
        CertificationOutcome::Recorded
    );
    assert_eq!(edges(&db.pool, e0).await?, (set(&[e1, e2]), set(&[e1])));
    // B knows all three but certifies only E0 to E1, without memory: E2 and
    // the memory edge are vetoed.
    record(
        &db.pool,
        b,
        &[
            certified(e0, &[e1], &[]),
            certified(e1, &[], &[]),
            certified(e2, &[], &[]),
        ],
    )
    .await?;
    assert_eq!(edges(&db.pool, e0).await?, (set(&[e1]), set(&[])));
    // C doesn't know E1 at all, so it abstains on E0 to E1 rather than
    // vetoing it, and adds E0 to E3, which only it can judge.
    record(
        &db.pool,
        c,
        &[
            certified(e0, &[e2, e3], &[]),
            certified(e2, &[], &[]),
            certified(e3, &[], &[]),
        ],
    )
    .await?;
    assert_eq!(edges(&db.pool, e0).await?, (set(&[e1, e3]), set(&[])));

    // B upgrades to a build that certifies E2 and the memory edge: the
    // aggregate is recomputed from every node's rows, so the edges A and C
    // already certify come back.
    record(
        &db.pool,
        b,
        &[
            certified(e0, &[e1, e2], &[e1]),
            certified(e1, &[], &[]),
            certified(e2, &[], &[]),
        ],
    )
    .await?;
    assert_eq!(edges(&db.pool, e0).await?, (set(&[e1, e2, e3]), set(&[e1])));

    // D's build knows E0 and E3 and certifies no edge: its veto holds until it
    // withdraws, and withdrawing restores the edge.
    record(
        &db.pool,
        d,
        &[certified(e0, &[], &[]), certified(e3, &[], &[])],
    )
    .await?;
    assert_eq!(edges(&db.pool, e0).await?, (set(&[e1, e2]), set(&[e1])));
    assert_eq!(
        record(&db.pool, d, &[]).await?,
        CertificationOutcome::Recorded
    );
    assert_eq!(edges(&db.pool, e0).await?, (set(&[e1, e2, e3]), set(&[e1])));
    let rows: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM node_environment_certifications WHERE node_id = $1"
    ))
    .bind(d)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(rows, 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_environment_no_node_certifies_keeps_its_last_edges() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    let node_id = node(&db.pool).await?;
    record(
        &db.pool,
        node_id,
        &[certified(e0, &[e1], &[]), certified(e1, &[], &[])],
    )
    .await?;
    // A build that dropped E0 from its catalog: E0's sandboxes keep their
    // upgrade path rather than losing it with the last reporter.
    record(&db.pool, node_id, &[certified(e1, &[], &[])]).await?;
    assert_eq!(edges(&db.pool, e0).await?, (set(&[e1]), set(&[])));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_unchanged_report_writes_nothing() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    let node_id = node(&db.pool).await?;
    let report = [certified(e0, &[e1], &[e1]), certified(e1, &[], &[])];
    assert_eq!(
        record(&db.pool, node_id, &report).await?,
        CertificationOutcome::Recorded
    );
    let updated = || {
        sqlx::query_scalar::<_, String>(sql!(
            "SELECT string_agg(updated_at::text, ',' ORDER BY environment_ref)
             FROM runtime_environments"
        ))
        .fetch_one(&db.pool)
    };
    let before = updated().await?;
    assert_eq!(
        record(&db.pool, node_id, &report).await?,
        CertificationOutcome::Unchanged
    );
    assert_eq!(updated().await?, before);
    // Edge order isn't a change: the stored form is sorted.
    let mut reordered = report.clone();
    reordered.reverse();
    assert_eq!(
        record(&db.pool, node_id, &reordered).await?,
        CertificationOutcome::Unchanged
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn the_catalog_row_takes_the_definition_and_the_newest_sort_key() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    let node_id = node(&db.pool).await?;
    let mut without_identity = certified(e1, &[], &[]);
    without_identity.identity = None;
    record(
        &db.pool,
        node_id,
        &[certified(e0, &[e1], &[]), without_identity.clone()],
    )
    .await?;
    let row = || {
        sqlx::query_as::<_, (String, String, String, String, String, Option<Vec<i32>>)>(sql!(
            "SELECT runtime, runtime_version, kernel_version, state_format_name,
                    state_format_version, version_sort_key
             FROM runtime_environments WHERE environment_ref = $1"
        ))
        .bind(e0.to_string())
        .fetch_one(&db.pool)
    };
    assert_eq!(
        row().await?,
        (
            "fc".into(),
            "1.17.0".into(),
            "6.1.155".into(),
            "firecracker-snapshot".into(),
            "12.0.0".into(),
            Some(vec![1, 17, 0])
        )
    );
    // No identity, no catalog row; the edge to it still counts.
    let present: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS (SELECT 1 FROM runtime_environments WHERE environment_ref = $1)"
    ))
    .bind(e1.to_string())
    .fetch_one(&db.pool)
    .await?;
    assert!(!present);
    assert_eq!(edges(&db.pool, e0).await?, (set(&[e1]), set(&[])));

    let mut rekeyed = certified(e0, &[e1], &[]);
    rekeyed.version_sort_key = None;
    record(&db.pool, node_id, &[rekeyed, without_identity]).await?;
    assert_eq!(row().await?.5, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_change_while_another_holds_the_aggregate_waits_for_a_later_publication() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    let (x, y) = (environment(0), environment(1));
    let (a, b, c) = (
        node(&db.pool).await?,
        node(&db.pool).await?,
        node(&db.pool).await?,
    );
    // C certifies X to Y and B vetoes it.
    record(
        &db.pool,
        c,
        &[certified(x, &[y], &[]), certified(y, &[], &[])],
    )
    .await?;
    let vetoes = [certified(x, &[], &[]), certified(y, &[], &[])];
    record(&db.pool, b, &vetoes).await?;
    assert_eq!(edges(&db.pool, x).await?, (set(&[]), set(&[])));

    // A's build vetoes it too, uncommitted, holding the aggregate lock.
    let mut veto = db.pool.begin().await?;
    assert_eq!(
        record_node_certifications(&mut veto, a, &vetoes).await?,
        CertificationOutcome::Recorded
    );
    // B withdraws meanwhile. It doesn't wait: it writes nothing, so it can't
    // aggregate without A's veto, and its rows stay for the next try. The lock
    // timeout turns a wait into a failure rather than a hang.
    let mut withdrawal = db.pool.begin().await?;
    sqlx::query(sql!("SET LOCAL lock_timeout = '1s'"))
        .execute(&mut *withdrawal)
        .await?;
    assert_eq!(
        record_node_certifications(&mut withdrawal, b, &[]).await?,
        CertificationOutcome::Deferred
    );
    withdrawal.commit().await?;
    let rows: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM node_environment_certifications WHERE node_id = $1"
    ))
    .bind(b)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(rows, 2);
    veto.commit().await?;
    // B's next publication lands, and counts A's committed veto.
    assert_eq!(
        record(&db.pool, b, &[]).await?,
        CertificationOutcome::Recorded
    );
    assert_eq!(edges(&db.pool, x).await?, (set(&[]), set(&[])));
    Ok(())
}

/// The evidence function, judged against rows whose clock columns are set
/// relative to now.
#[tokio::test(flavor = "current_thread")]
async fn evidence_needs_a_complete_inventory_a_fresh_node_and_a_fresh_receipt() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    // (status, observed seconds ago, window seconds, array receipt seconds
    // ago or no arrays, evidence)
    let cases: [(&str, i32, i32, Option<i32>, bool); 7] = [
        ("complete", 0, 150, Some(0), true),
        // Still within the window from the receipt.
        ("complete", 0, 150, Some(140), true),
        // The receipt is older than the window: an older writer refreshed
        // the inventory columns but not the arrays.
        ("complete", 0, 150, Some(160), false),
        // The node itself is stale.
        ("complete", 200, 150, Some(0), false),
        ("unavailable", 0, 150, Some(0), false),
        ("legacy", 0, 150, Some(0), false),
        // No report at all.
        ("complete", 0, 150, None, false),
    ];
    for (status, observed_ago, window, receipt_ago, expected) in cases {
        let node_id = node(&db.pool).await?;
        sqlx::query(sql!(
            "INSERT INTO node_status_snapshots (
                 node_id, observed_at, updated_at, stale_after, reachable, services, execution_inventory_status,
                 execution_inventory_received_at, usable_environment_refs,
                 known_environment_refs, environment_refs_received_at
             )
             SELECT $1, at, at, at + make_interval(secs => $3), TRUE, '[]', $4, at,
                    CASE WHEN $5::INT IS NULL THEN NULL ELSE '{}'::TEXT[] END,
                    CASE WHEN $5::INT IS NULL THEN NULL ELSE '{}'::TEXT[] END,
                    now() - make_interval(secs => $5)
             FROM (SELECT now() - make_interval(secs => $2) AS at) observed"
        ))
        .bind(node_id)
        .bind(observed_ago)
        .bind(window)
        .bind(status)
        .bind(receipt_ago)
        .execute(&db.pool)
        .await?;
        let evidence: bool = sqlx::query_scalar(sql!(
            "SELECT node_environment_evidence(nss) FROM node_status_snapshots nss
             WHERE nss.node_id = $1"
        ))
        .bind(node_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(
            evidence, expected,
            "{status} observed {observed_ago}s ago, window {window}s, receipt {receipt_ago:?}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_retired_environment_keeps_its_certifications() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    let node_id = node(&db.pool).await?;
    let report = [certified(e0, &[e1], &[]), certified(e1, &[], &[])];
    record(&db.pool, node_id, &report).await?;
    retire(&db.pool, e1).await?;
    // Retirement is the operator's; a publication never clears it.
    let mut rekeyed = report.clone();
    rekeyed[1].version_sort_key = Some(VersionSortKey::new(vec![1, 17, 1]).unwrap());
    record(&db.pool, node_id, &rekeyed).await?;
    assert_eq!(
        retired_among(&db.pool, &[e1.to_string()]).await?,
        BTreeSet::from([e1.to_string()])
    );
    Ok(())
}
