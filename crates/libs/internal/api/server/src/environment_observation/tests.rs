use std::collections::BTreeSet;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{
    Digest, EnvironmentReference, ExecutionCompatibilityProfile, NodeId, RuntimeBinding,
    StateFormatIdentity, VersionSortKey,
};
use pplx_space_db_test_fixtures::{insert_ready_worker_node, try_pg_test_db};
use pplx_space_execution_common::environment::{
    EnvironmentDefinition, EnvironmentObservation, FIRECRACKER_MACHINE_CONTRACT, KnownEnvironment,
};
use pplx_space_execution_common::proto::NodeExecutionInventory;
use pplx_space_test_env::TestResult;

use crate::proto::{
    ExecutionInventoryObservation, ExecutionInventoryUnavailable,
    execution_inventory_observation::Outcome,
};

fn e0() -> EnvironmentDefinition {
    EnvironmentDefinition::from_document(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../../../proto/execution/v1/fixtures/e0.json"
    )))
    .unwrap()
    .into_runtime_neutral()
    .unwrap()
}

/// Another Firecracker release over E0's guest and kernel, as E1 is.
fn release(version: &str) -> EnvironmentDefinition {
    let mut spec = e0().descriptor().spec().clone();
    let RuntimeBinding::ManagedRelease(release) = &mut spec.runtime;
    release.runtime.version = version.parse().unwrap();
    release.digest = Digest::sha256([version.len() as u8; 32]);
    spec.compatibility = ExecutionCompatibilityProfile::RuntimeV1 {
        contract: FIRECRACKER_MACHINE_CONTRACT.parse().unwrap(),
        state_format: Some(StateFormatIdentity {
            format: "firecracker-snapshot".parse().unwrap(),
            version: "12.0.0".parse().unwrap(),
        }),
    };
    EnvironmentDefinition::new(spec.try_into().unwrap())
}

fn key(components: &[u32]) -> Option<VersionSortKey> {
    Some(VersionSortKey::new(components.to_vec()).unwrap())
}

/// A node build that knows E0 and E1, certifies E0 to E1, and can boot both.
fn observation() -> EnvironmentObservation {
    let (e0, e1) = (e0(), release("1.17.0"));
    EnvironmentObservation::new(
        [
            KnownEnvironment::new(e0.clone(), key(&[1, 15, 1]), [e1.reference()], []).unwrap(),
            KnownEnvironment::new(e1.clone(), key(&[1, 17, 0]), [], []).unwrap(),
        ],
        [e0.reference(), e1.reference()],
    )
    .unwrap()
}

fn inventory(observation: Option<&EnvironmentObservation>) -> NodeExecutionInventory {
    NodeExecutionInventory {
        environments: observation.map(Into::into),
        ..Default::default()
    }
}

fn complete(inventory: NodeExecutionInventory) -> ExecutionInventoryObservation {
    ExecutionInventoryObservation {
        outcome: Some(Outcome::Complete(inventory)),
    }
}

/// Commit one publication as `PublishNodeState` does, through the status
/// upsert. `None` is a publication that carries no execution observation.
async fn publish(
    pool: &sqlx::PgPool,
    node_id: NodeId,
    observation: Option<ExecutionInventoryObservation>,
) -> TestResult {
    let mut tx = pool.begin().await?;
    let prepared =
        crate::execution_observation::prepare(&mut tx, node_id, observation.as_ref(), None).await?;
    let outcome = crate::execution_observation::publish(&mut tx, prepared).await?;
    crate::status::upsert_node_status_from_publish(
        &mut tx,
        node_id,
        None,
        None,
        crate::status::SandboxInventoryPrune::Skipped,
        crate::status::LocalBackupInventoryReport::Absent,
        outcome,
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
struct Stored {
    usable: Option<Vec<String>>,
    known: Option<Vec<String>>,
    received_at: Option<String>,
    evidence: bool,
    certifications: i64,
}

async fn stored(pool: &sqlx::PgPool, node_id: NodeId) -> TestResult<Stored> {
    let (usable, known, received_at, evidence, certifications) = sqlx::query_as(sql!(
        "SELECT usable_environment_refs, known_environment_refs,
                environment_refs_received_at::text, node_environment_evidence(nss),
                (SELECT COUNT(*) FROM node_environment_certifications c
                 WHERE c.node_id = nss.node_id)
         FROM node_status_snapshots nss WHERE nss.node_id = $1"
    ))
    .bind(node_id)
    .fetch_one(pool)
    .await?;
    Ok(Stored {
        usable,
        known,
        received_at,
        evidence,
        certifications,
    })
}

fn texts(references: &[EnvironmentReference]) -> Vec<String> {
    let mut texts: Vec<String> = references.iter().map(ToString::to_string).collect();
    texts.sort();
    texts
}

async fn node(pool: &sqlx::PgPool) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    insert_ready_worker_node(pool, node_id).await?;
    Ok(node_id)
}

/// A `runtime_environments` row: the reference, the five identity columns,
/// the sort key and the upgrade edges.
type CatalogRow = (
    String,
    String,
    String,
    String,
    String,
    String,
    Option<Vec<i32>>,
    Vec<String>,
);

#[tokio::test]
async fn a_complete_inventory_records_the_sets_and_the_catalog() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = node(&db.pool).await?;
    let (e0, e1) = (e0().reference(), release("1.17.0").reference());
    publish(
        &db.pool,
        node_id,
        Some(complete(inventory(Some(&observation())))),
    )
    .await?;

    let row = stored(&db.pool, node_id).await?;
    assert_eq!(row.usable, Some(texts(&[e0, e1])));
    assert_eq!(row.known, Some(texts(&[e0, e1])));
    assert!(row.received_at.is_some());
    assert!(row.evidence);
    assert_eq!(row.certifications, 2);

    // The catalog row is spelled as a published execution records it, so
    // the upgrade decision can match unbound records against it.
    let catalog: Vec<CatalogRow> = sqlx::query_as(sql!(
        "SELECT environment_ref, runtime, runtime_version, kernel_version,
                    state_format_name, state_format_version, version_sort_key, upgrade_edges
             FROM runtime_environments ORDER BY runtime_version"
    ))
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(
        catalog,
        vec![
            (
                e0.to_string(),
                "fc".into(),
                "1.15.1".into(),
                "6.1.155".into(),
                "firecracker-snapshot".into(),
                "9.0.0".into(),
                Some(vec![1, 15, 1]),
                vec![e1.to_string()],
            ),
            (
                e1.to_string(),
                "fc".into(),
                "1.17.0".into(),
                "6.1.155".into(),
                "firecracker-snapshot".into(),
                "12.0.0".into(),
                Some(vec![1, 17, 0]),
                vec![],
            ),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn a_retired_environment_is_known_but_never_usable() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = node(&db.pool).await?;
    let (e0, e1) = (e0().reference(), release("1.17.0").reference());
    let report = || Some(complete(inventory(Some(&observation()))));
    publish(&db.pool, node_id, report()).await?;
    sqlx::query(sql!(
        "UPDATE runtime_environments SET retired_at = now() WHERE environment_ref = $1"
    ))
    .bind(e0.to_string())
    .execute(&db.pool)
    .await?;
    // An AMI-baked copy still verifies on the node, but retirement wins.
    publish(&db.pool, node_id, report()).await?;
    let row = stored(&db.pool, node_id).await?;
    assert_eq!(row.usable, Some(texts(&[e1])));
    assert_eq!(row.known, Some(texts(&[e0, e1])));
    Ok(())
}

#[tokio::test]
async fn an_old_build_or_an_invalid_observation_leaves_the_node_without_evidence() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let none = Stored {
        usable: None,
        known: None,
        received_at: None,
        evidence: false,
        certifications: 0,
    };
    // A node that has only ever run an old build.
    let old = node(&db.pool).await?;
    publish(&db.pool, old, Some(complete(inventory(None)))).await?;
    assert_eq!(stored(&db.pool, old).await?, none);

    // A node downgraded to an old build withdraws what it reported, and its
    // votes, rather than leaving them to age.
    let downgraded = node(&db.pool).await?;
    publish(
        &db.pool,
        downgraded,
        Some(complete(inventory(Some(&observation())))),
    )
    .await?;
    publish(&db.pool, downgraded, Some(complete(inventory(None)))).await?;
    assert_eq!(stored(&db.pool, downgraded).await?, none);

    // An observation this reader can't decode, here a future schema, is no
    // evidence either.
    let invalid = node(&db.pool).await?;
    publish(
        &db.pool,
        invalid,
        Some(complete(inventory(Some(&observation())))),
    )
    .await?;
    let mut future = inventory(Some(&observation()));
    future
        .environments
        .as_mut()
        .expect("the inventory carries an observation")
        .schema_version += 1;
    publish(&db.pool, invalid, Some(complete(future))).await?;
    assert_eq!(stored(&db.pool, invalid).await?, none);
    Ok(())
}

#[tokio::test]
async fn reports_without_a_complete_inventory_leave_the_sets_to_age() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = node(&db.pool).await?;
    publish(
        &db.pool,
        node_id,
        Some(complete(inventory(Some(&observation())))),
    )
    .await?;
    let recorded = stored(&db.pool, node_id).await?;

    // A heartbeat and an unavailable collection say nothing about
    // environments: the arrays and their receipt stay as they were.
    publish(&db.pool, node_id, None).await?;
    assert_eq!(stored(&db.pool, node_id).await?, recorded);
    publish(
        &db.pool,
        node_id,
        Some(ExecutionInventoryObservation {
            outcome: Some(Outcome::Unavailable(ExecutionInventoryUnavailable {})),
        }),
    )
    .await?;
    let unavailable = stored(&db.pool, node_id).await?;
    assert_eq!(unavailable.usable, recorded.usable);
    assert_eq!(unavailable.received_at, recorded.received_at);
    assert_eq!(unavailable.certifications, 2);
    // The latest inventory isn't complete, so they aren't evidence now.
    assert!(!unavailable.evidence);
    Ok(())
}

#[tokio::test]
async fn nodes_on_three_builds_keep_the_edges_every_judging_build_certifies() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (e0, e1, e2) = (e0(), release("1.17.0"), release("1.17.10"));
    let build = |e0_edges: &[&EnvironmentDefinition], knows_e2: bool| {
        let mut known = vec![
            KnownEnvironment::new(
                e0.clone(),
                key(&[1, 15, 1]),
                e0_edges.iter().map(|target| target.reference()),
                [],
            )
            .unwrap(),
            KnownEnvironment::new(e1.clone(), key(&[1, 17, 0]), [], []).unwrap(),
        ];
        if knows_e2 {
            known.push(KnownEnvironment::new(e2.clone(), key(&[1, 17, 10]), [], []).unwrap());
        }
        EnvironmentObservation::new(known, [e0.reference()]).unwrap()
    };
    let upgrade_edges = || async {
        let edges: Vec<String> = sqlx::query_scalar(sql!(
            "SELECT upgrade_edges FROM runtime_environments WHERE environment_ref = $1"
        ))
        .bind(e0.reference().to_string())
        .fetch_one(&db.pool)
        .await?;
        TestResult::Ok(edges.into_iter().collect::<BTreeSet<_>>())
    };
    let (a, b, c) = (
        node(&db.pool).await?,
        node(&db.pool).await?,
        node(&db.pool).await?,
    );
    publish(
        &db.pool,
        a,
        Some(complete(inventory(Some(&build(&[&e1, &e2], true))))),
    )
    .await?;
    // B certifies only E1, so E2 is vetoed.
    publish(
        &db.pool,
        b,
        Some(complete(inventory(Some(&build(&[&e1], true))))),
    )
    .await?;
    // C's build doesn't know E2, so it doesn't vote on it.
    publish(
        &db.pool,
        c,
        Some(complete(inventory(Some(&build(&[&e1], false))))),
    )
    .await?;
    assert_eq!(
        upgrade_edges().await?,
        BTreeSet::from([e1.reference().to_string()])
    );
    // B upgrades to A's build: every build that knows E2 certifies it now.
    publish(
        &db.pool,
        b,
        Some(complete(inventory(Some(&build(&[&e1, &e2], true))))),
    )
    .await?;
    assert_eq!(
        upgrade_edges().await?,
        BTreeSet::from([e1.reference().to_string(), e2.reference().to_string()])
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_publication_never_waits_for_the_catalog_lock() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(3).await?
    else {
        return Ok(());
    };
    let (holder, waiter) = (node(&db.pool).await?, node(&db.pool).await?);
    // One node's certification change holds the aggregate lock, uncommitted.
    let mut held = db.pool.begin().await?;
    super::publish(&mut held, holder, &inventory(Some(&observation()))).await?;
    // Another node's publication still succeeds at once, under a lock timeout
    // that a wait would exceed. It records the arrays; its certifications
    // wait for a later publication.
    let mut other = db.pool.begin().await?;
    sqlx::query(sql!("SET LOCAL lock_timeout = '100ms'"))
        .execute(&mut *other)
        .await?;
    let refs = super::publish(&mut other, waiter, &inventory(Some(&observation()))).await?;
    assert!(
        matches!(refs, super::EnvironmentRefs::Record { .. }),
        "{refs:?}"
    );
    other.commit().await?;
    held.commit().await?;
    let certifications = || {
        sqlx::query_scalar::<_, i64>(sql!(
            "SELECT COUNT(*) FROM node_environment_certifications WHERE node_id = $1"
        ))
        .bind(waiter)
        .fetch_one(&db.pool)
    };
    assert_eq!(certifications().await?, 0);
    publish(
        &db.pool,
        waiter,
        Some(complete(inventory(Some(&observation())))),
    )
    .await?;
    assert_eq!(certifications().await?, 2);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_failed_recipe_sample_doesnt_fail_the_publication() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    // A catalog row this binary can't parse fails the recipe read. The schema
    // refuses one, so drop its CHECK in this isolated database.
    sqlx::query(sql!(
        "ALTER TABLE runtime_environments
         DROP CONSTRAINT runtime_environments_environment_ref_check"
    ))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO runtime_environments (
             environment_ref, runtime, runtime_version, kernel_version,
             state_format_name, state_format_version
         )
         VALUES ('sha256:not-a-digest', 'fc', '1.17.0', '6.1.155',
                 'firecracker-snapshot', '12.0.0')"
    ))
    .execute(&db.pool)
    .await?;
    // An old build's sample reads the whole catalog, bad row included. The
    // publication still commits, with the old build's verdict recorded.
    let node_id = node(&db.pool).await?;
    publish(&db.pool, node_id, Some(complete(inventory(None)))).await?;
    let status: Option<String> = sqlx::query_scalar(sql!(
        "SELECT execution_inventory_status FROM node_status_snapshots WHERE node_id = $1"
    ))
    .bind(node_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(status.as_deref(), Some("complete"));
    Ok(())
}
