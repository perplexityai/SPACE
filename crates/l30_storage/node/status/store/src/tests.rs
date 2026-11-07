use pplx_space_accounts::NodeId;
use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{insert_ready_worker_node, try_pg_test_db};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::test_support::{SnapshotSeed, upsert_snapshot};
use super::{EnvironmentRefusal, environment_evidence, environment_refusal};

async fn evidence(pool: &PgPool, node: NodeId) -> TestResult<bool> {
    let statement = sql!(format!(
        "SELECT {} FROM node_status_snapshots nss WHERE nss.node_id = $1",
        environment_evidence("nss")
    ));
    Ok(
        sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(statement))
            .bind(node)
            .fetch_one(pool)
            .await?,
    )
}

#[tokio::test(flavor = "current_thread")]
async fn a_snapshot_is_evidence_only_while_every_condition_holds() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let fresh = SnapshotSeed::default();
    for (seed, expected, why) in [
        (fresh, true, "complete, fresh and received now"),
        (
            SnapshotSeed {
                reachable: false,
                ..fresh
            },
            true,
            "reachability is the caller's to require",
        ),
        (
            SnapshotSeed {
                inventory: None,
                ..fresh
            },
            false,
            "no inventory report",
        ),
        (
            SnapshotSeed {
                inventory: Some("partial"),
                ..fresh
            },
            false,
            "an incomplete inventory",
        ),
        (
            SnapshotSeed {
                stale_in_secs: -1.0,
                ..fresh
            },
            false,
            "a stale snapshot",
        ),
        // Arrays received after the last observation stretch the window
        // past stale_after; the snapshot is stale all the same.
        (
            SnapshotSeed {
                observed_secs_ago: 30.0,
                stale_in_secs: -1.0,
                environment_refs_secs_ago: Some(5.0),
                ..fresh
            },
            false,
            "a stale snapshot whose arrays came after its observation",
        ),
        (
            SnapshotSeed {
                environment_refs_secs_ago: None,
                ..fresh
            },
            false,
            "no environment arrays",
        ),
        // The window is stale_after - observed_at: 60 s here, whatever is
        // left of it.
        (
            SnapshotSeed {
                observed_secs_ago: 50.0,
                stale_in_secs: 10.0,
                environment_refs_secs_ago: Some(55.0),
                ..fresh
            },
            true,
            "arrays received within the window, measured from now",
        ),
        (
            SnapshotSeed {
                observed_secs_ago: 50.0,
                stale_in_secs: 10.0,
                environment_refs_secs_ago: Some(65.0),
                ..fresh
            },
            false,
            "arrays older than the window",
        ),
    ] {
        let node = NodeId::new();
        insert_ready_worker_node(&db.pool, node).await?;
        upsert_snapshot(&db.pool, node, seed).await?;
        assert_eq!(evidence(&db.pool, node).await?, expected, "{why}");
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn the_predicate_is_never_null_so_it_negates_safely() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node = NodeId::new();
    insert_ready_worker_node(&db.pool, node).await?;
    upsert_snapshot(
        &db.pool,
        node,
        SnapshotSeed {
            inventory: None,
            environment_refs_secs_ago: None,
            ..SnapshotSeed::default()
        },
    )
    .await?;
    let statement = sql!(format!(
        "SELECT NOT {} FROM node_status_snapshots nss WHERE nss.node_id = $1",
        environment_evidence("nss")
    ));
    let negated = sqlx::query_scalar::<_, Option<bool>>(sqlx::AssertSqlSafe(statement))
        .bind(node)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(negated, Some(true));
    Ok(())
}

const LISTED: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const UNLISTED: &str = "sha256:2222222222222222222222222222222222222222222222222222222222222222";

/// The refusal for `environment` on `node`'s snapshot, and whether the
/// snapshot is evidence, read in one statement.
async fn refusal_and_evidence(
    pool: &PgPool,
    node: NodeId,
    environment: &str,
) -> TestResult<(Option<EnvironmentRefusal>, bool)> {
    let statement = sql!(format!(
        "SELECT {}, {} FROM node_status_snapshots nss WHERE nss.node_id = $1",
        environment_refusal("nss", "$2::TEXT"),
        environment_evidence("nss")
    ));
    let (refusal, evidence) =
        sqlx::query_as::<_, (Option<String>, bool)>(sqlx::AssertSqlSafe(statement))
            .bind(node)
            .bind(environment)
            .fetch_one(pool)
            .await?;
    Ok((refusal.map(|value| value.parse()).transpose()?, evidence))
}

#[tokio::test(flavor = "current_thread")]
async fn a_refusal_names_the_first_evidence_condition_that_fails() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let fresh = SnapshotSeed::default();
    for (seed, expected, why) in [
        (fresh, None, "evidence listing the environment"),
        (
            SnapshotSeed {
                reachable: false,
                ..fresh
            },
            None,
            "reachability is the caller's to require",
        ),
        (
            SnapshotSeed {
                inventory: None,
                ..fresh
            },
            Some(EnvironmentRefusal::NoEvidence),
            "no inventory report",
        ),
        (
            SnapshotSeed {
                environment_refs_secs_ago: None,
                ..fresh
            },
            Some(EnvironmentRefusal::NoEvidence),
            "a build that publishes no environments",
        ),
        (
            SnapshotSeed {
                inventory: Some("legacy"),
                ..fresh
            },
            Some(EnvironmentRefusal::Incomplete),
            "a legacy inventory",
        ),
        (
            SnapshotSeed {
                inventory: Some("unavailable"),
                stale_in_secs: -1.0,
                ..fresh
            },
            Some(EnvironmentRefusal::Incomplete),
            "an unavailable inventory, stale as well",
        ),
        (
            SnapshotSeed {
                stale_in_secs: -1.0,
                ..fresh
            },
            Some(EnvironmentRefusal::Stale),
            "a stale snapshot",
        ),
        (
            SnapshotSeed {
                observed_secs_ago: 50.0,
                stale_in_secs: 10.0,
                environment_refs_secs_ago: Some(65.0),
                ..fresh
            },
            Some(EnvironmentRefusal::Stale),
            "arrays older than the window",
        ),
        (
            SnapshotSeed {
                observed_secs_ago: 50.0,
                stale_in_secs: 10.0,
                environment_refs_secs_ago: Some(55.0),
                ..fresh
            },
            None,
            "arrays received within the window",
        ),
    ] {
        let node = NodeId::new();
        insert_ready_worker_node(&db.pool, node).await?;
        upsert_snapshot(&db.pool, node, seed).await?;
        if seed.environment_refs_secs_ago.is_some() {
            sqlx::query(sql!(
                "UPDATE node_status_snapshots
                 SET usable_environment_refs = ARRAY[$2], known_environment_refs = ARRAY[$2]
                 WHERE node_id = $1"
            ))
            .bind(node)
            .bind(LISTED)
            .execute(&db.pool)
            .await?;
        }
        let (refusal, evidence) = refusal_and_evidence(&db.pool, node, LISTED).await?;
        assert_eq!(refusal, expected, "{why}");
        // Evidence and the classifier agree: only evidence is refused for an
        // environment it doesn't list, and anything else is refused for the
        // same reason whatever the environment.
        assert_eq!(evidence, expected.is_none(), "{why}: evidence");
        let (unlisted, _) = refusal_and_evidence(&db.pool, node, UNLISTED).await?;
        assert_eq!(
            unlisted,
            Some(expected.unwrap_or(EnvironmentRefusal::EnvironmentNotUsable)),
            "{why}: an unlisted environment"
        );
        let unbound = sqlx::query_scalar::<_, Option<String>>(sqlx::AssertSqlSafe(sql!(format!(
            "SELECT {} FROM node_status_snapshots nss WHERE nss.node_id = $1",
            environment_refusal("nss", "NULL::TEXT")
        ))))
        .bind(node)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(unbound, None, "{why}: work bound to no environment");
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_node_without_a_snapshot_has_no_evidence() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node = NodeId::new();
    insert_ready_worker_node(&db.pool, node).await?;
    let statement = sql!(format!(
        "SELECT {} FROM nodes n
         LEFT JOIN node_status_snapshots nss ON nss.node_id = n.id
         WHERE n.id = $1",
        environment_refusal("nss", "$2::TEXT")
    ));
    let refusal = sqlx::query_scalar::<_, Option<String>>(sqlx::AssertSqlSafe(statement))
        .bind(node)
        .bind(LISTED)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(
        refusal.as_deref(),
        Some(EnvironmentRefusal::NoEvidence.as_str())
    );
    Ok(())
}

#[test]
fn a_refusal_round_trips_through_its_wire_value() {
    for refusal in EnvironmentRefusal::ALL {
        assert_eq!(refusal.as_str().parse(), Ok(refusal));
        assert_eq!(refusal.to_string(), refusal.as_str());
    }
    "refused"
        .parse::<EnvironmentRefusal>()
        .expect_err("an unknown refusal doesn't parse");
}

#[tokio::test(flavor = "current_thread")]
async fn a_null_in_the_usable_list_doesnt_admit_an_unlisted_environment() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node = NodeId::new();
    insert_ready_worker_node(&db.pool, node).await?;
    upsert_snapshot(&db.pool, node, SnapshotSeed::default()).await?;
    // `x = ANY(ARRAY[NULL, ...])` is NULL, not FALSE, for an unlisted `x`.
    sqlx::query(sql!(
        "UPDATE node_status_snapshots
         SET usable_environment_refs = ARRAY[NULL, $2], known_environment_refs = ARRAY[$2]
         WHERE node_id = $1"
    ))
    .bind(node)
    .bind(LISTED)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        refusal_and_evidence(&db.pool, node, UNLISTED).await?,
        (Some(EnvironmentRefusal::EnvironmentNotUsable), true)
    );
    assert_eq!(
        refusal_and_evidence(&db.pool, node, LISTED).await?,
        (None, true)
    );
    Ok(())
}
