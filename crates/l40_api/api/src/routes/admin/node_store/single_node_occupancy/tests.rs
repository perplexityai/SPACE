use pplx_space_common_core::types::NodeId;
use std::time::Duration;
use std::time::Instant;

use pplx_space_common_core::sql;
use pplx_space_common_svc::sql::sandbox_residency::ResidencyEvidence;
use pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections;
use pplx_space_test_env::TestResult;
use serde::Deserialize;
use sqlx::PgPool;

use super::super::{AdminNodeStore, OccupancyRow};

const AGE: Duration = Duration::from_secs(86_400);
const UNRELATED_ROWS: i32 = 10_000;

async fn fixture(pool: &PgPool) -> TestResult<[NodeId; 3]> {
    let nodes = [NodeId::new(), NodeId::new(), NodeId::new()];
    let [node, other, _] = nodes;
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         SELECT id, '10.0.0.12'::INET, 'worker', 'space', 'registered'
         FROM UNNEST($1::UUID[]) id"
    ))
    .bind(nodes.as_slice())
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots (
             node_id, observed_at, stale_after, reachable, services,
             sandbox_inventory_pruned_at, updated_at
         ) SELECT id, NOW(), NOW() + INTERVAL '1 hour', TRUE, '[]'::JSONB, NOW(), NOW()
           FROM UNNEST($1::UUID[]) id"
    ))
    .bind(nodes.as_slice())
    .execute(pool)
    .await?;
    // The matrix varies lifecycle, target/source drift, evacuation, exact vs
    // legacy identity, mismatched artifacts, freshness, and missing/orphaned
    // storage. Duplicate uploads and a newer failed snapshot exercise both
    // deduplication and the evacuation latest-snapshot rule.
    sqlx::query(sql!(
        "CREATE TABLE occupancy_cases AS
         SELECT i, space_uuid_v7() AS id, space_uuid_v7() AS artifact_id
         FROM generate_series(0, 767) i"
    ))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, target_node_id, placement_changed_at, lifecycle_changed_at,
             evacuation_restore_target, current_local_artifact_id,
             current_local_artifact_kind, current_local_artifact_origin,
             network_policy_generation
         ) SELECT id, '00000000-0000-0000-0000-000000000010',
                  '00000000-0000-0000-0000-000000000001',
                  (ARRAY['started','suspended','suspending','restoring','stopped','quarantined'])[i % 6 + 1],
                  'unplaced', 'k8s',
                  CASE WHEN i % 6 IN (0,2,4,5) THEN $1::UUID END,
                  CASE WHEN i / 6 % 2 = 0 THEN $1::UUID ELSE $2::UUID END,
                  NOW() - INTERVAL '1 hour', NOW() - INTERVAL '2 days',
                  CASE WHEN i / 12 % 2 = 0 THEN 'started' END,
                  CASE WHEN i / 24 % 2 = 0 THEN artifact_id END,
                  CASE WHEN i / 24 % 2 = 0 THEN 'full' END,
                  CASE WHEN i / 24 % 2 = 0 THEN 'suspend' END, 1
           FROM occupancy_cases"
    ))
    .bind(node)
    .bind(other)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, source_node_id, timestamp,
             local_artifact_id, artifact_kind, artifact_origin
         ) SELECT space_uuid_v7(), c.id, 'full',
                  CASE WHEN version = 0 THEN 'ready'
                       WHEN version = 3 THEN 'failed' ELSE 'creating' END,
                  CASE WHEN i / 48 % 2 = 0 THEN $1::UUID ELSE $2::UUID END,
                  NOW() - CASE WHEN i / 96 % 2 = 0 THEN INTERVAL '2 hours'
                               ELSE INTERVAL '30 minutes' END
                        + version * INTERVAL '1 minute',
                  CASE WHEN i / 24 % 2 = 0 THEN
                      CASE WHEN i / 192 % 2 = 0 THEN artifact_id ELSE space_uuid_v7() END END,
                  CASE WHEN i / 24 % 2 = 0 THEN 'full' END,
                  CASE WHEN i / 24 % 2 = 0 THEN 'suspend' END
           FROM occupancy_cases c CROSS JOIN generate_series(0,3) version"
    ))
    .bind(node)
    .bind(other)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string, orphaned_at)
         SELECT sn.id, 's3://test-occupancy/snapshot',
                CASE WHEN c.i / 384 % 2 = 0 THEN NOW() END
         FROM snapshots sn JOIN occupancy_cases c ON c.id = sn.sandbox_id
         WHERE sn.state = 'ready' AND c.i % 3 <> 0"
    ))
    .execute(pool)
    .await?;
    Ok(nodes)
}

async fn single(
    pool: &PgPool,
    node: NodeId,
    ranked: bool,
    residency: ResidencyEvidence,
) -> TestResult<OccupancyRow> {
    let query = super::query(ranked, residency);
    Ok(sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node)
        .bind(i64::try_from(AGE.as_secs())?)
        .fetch_one(pool)
        .await?)
}

/// The legacy batched rows for every ranking and residency combination,
/// sorted by node.
///
/// This is the differential tests' oracle and by far their most expensive
/// statement. A statement's rows do not depend on the plan Postgres picks for
/// it, so the oracle runs once per combination, under the session's default
/// plan mode, rather than once per mode. What the tests vary by plan mode is
/// the statement under test, which still runs, and is compared, under every
/// mode.
async fn legacy_oracle(
    store: &AdminNodeStore,
    nodes: &[NodeId],
) -> TestResult<Vec<(bool, ResidencyEvidence, Vec<OccupancyRow>)>> {
    let mut oracle = Vec::new();
    for ranked in [false, true] {
        for residency in [
            ResidencyEvidence::LifecycleOnly,
            ResidencyEvidence::NodeCensusAware,
        ] {
            let mut rows = store
                .batched_occupancy_rows(nodes, ranked.then_some(AGE), residency)
                .await?;
            rows.sort_by_key(|row| row.node_id);
            oracle.push((ranked, residency, rows));
        }
    }
    Ok(oracle)
}

#[tokio::test]
async fn single_matches_batched_durability_upload_and_residency_semantics() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(1).await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let nodes = fixture(pool).await?;
    let store = AdminNodeStore::new(pool.clone());
    let oracle = legacy_oracle(&store, &nodes).await?;
    for mode in ["force_custom_plan", "force_generic_plan"] {
        sqlx::query(sql!("SELECT set_config('plan_cache_mode', $1, false)"))
            .bind(mode)
            .execute(pool)
            .await?;
        for (ranked, residency, expected) in &oracle {
            for row in expected {
                assert_eq!(
                    &single(pool, row.node_id, *ranked, *residency).await?,
                    row,
                    "{mode}, ranked={ranked}, residency={residency:?}"
                );
            }
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct Plan {
    #[serde(rename = "Node Type")]
    node_type: String,
    #[serde(rename = "Index Name")]
    index: Option<String>,
    #[serde(rename = "Relation Name")]
    relation: Option<String>,
    #[serde(rename = "Actual Rows", default)]
    rows: f64,
    #[serde(rename = "Rows Removed by Filter", default)]
    filtered: f64,
    #[serde(rename = "Actual Loops", default)]
    loops: f64,
    #[serde(rename = "Plans", default)]
    children: Vec<Plan>,
}

impl Plan {
    fn print_snapshot_accesses(&self) {
        if let Some("snapshots" | "snapshot_storage_locations") = self.relation.as_deref() {
            println!(
                "  {} {:?}: rows/loop={} filtered/loop={} loops={}",
                self.node_type, self.index, self.rows, self.filtered, self.loops
            );
        }
        for child in &self.children {
            child.print_snapshot_accesses();
        }
    }

    fn snapshot_rows(&self) -> f64 {
        let own = match self.relation.as_deref() {
            Some("snapshots" | "snapshot_storage_locations") => {
                (self.rows + self.filtered) * self.loops
            }
            _ => 0.0,
        };
        own + self.children.iter().map(Self::snapshot_rows).sum::<f64>()
    }

    fn snapshot_bitmap_entries(&self) -> f64 {
        let own = if self.node_type == "Bitmap Index Scan"
            && self
                .index
                .as_deref()
                .is_some_and(|index| index.starts_with("idx_snapshots_"))
        {
            self.rows * self.loops
        } else {
            0.0
        };
        own + self
            .children
            .iter()
            .map(Self::snapshot_bitmap_entries)
            .sum::<f64>()
    }
}

#[derive(Deserialize)]
struct Explained {
    #[serde(rename = "Plan")]
    plan: Plan,
}

#[tokio::test]
async fn postgres_fleet_occupancy_bounds_history_across_cached_plans() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(1).await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let nodes = fixture(pool).await?;
    let [node, _, _] = nodes;
    // Keep a sizable unrelated catalog to make an accidental global hash build
    // observable in actual rows, without a wall-clock assertion in CI.
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, placement, managed_by, network_policy_generation)
         SELECT space_uuid_v7(), '00000000-0000-0000-0000-000000000010',
                '00000000-0000-0000-0000-000000000001', 'failed', 'unplaced', 'k8s', 1
         FROM generate_series(1, $1)"
    ))
    .bind(UNRELATED_ROWS)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, source_node_id, type, state, timestamp)
         SELECT space_uuid_v7(), id, $1, 'full', 'ready', NOW() - INTERVAL '1 day'
         FROM sandboxes WHERE lifecycle = 'failed'"
    ))
    .bind(node)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         SELECT sn.id, 's3://test-occupancy/unrelated'
         FROM snapshots sn WHERE NOT EXISTS (SELECT 1 FROM occupancy_cases c WHERE c.id=sn.sandbox_id)"
    ))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "ANALYZE sandboxes, snapshots, snapshot_storage_locations"
    ))
    .execute(pool)
    .await?;
    let store = AdminNodeStore::new(pool.clone());
    for _ in 0..5 {
        store
            .occupancy_for_nodes(&nodes, Some(AGE), ResidencyEvidence::LifecycleOnly)
            .await?;
    }
    for _ in 0..6 {
        let result = store
            .occupancy_for_nodes(
                std::slice::from_ref(&node),
                Some(AGE),
                ResidencyEvidence::LifecycleOnly,
            )
            .await?;
        assert!(result.get(&node).expect("requested node").occupancy.active > 0);
    }
    let (custom, generic): (i64, i64) = sqlx::query_as(sql!(
        "SELECT SUM(custom_plans)::BIGINT, SUM(generic_plans)::BIGINT
         FROM pg_prepared_statements WHERE statement LIKE '%WITH uploading AS MATERIALIZED%'
           AND parameter_types[1] = 'uuid'::regtype
           AND statement NOT LIKE '%pg_prepared_statements%'"
    ))
    .fetch_one(pool)
    .await?;
    assert_eq!(
        custom + generic,
        6,
        "all UUID calls reuse the scalar statement"
    );
    for _ in 0..6 {
        let rows = store
            .occupancy_for_nodes(&nodes, Some(AGE), ResidencyEvidence::LifecycleOnly)
            .await?;
        assert_eq!(rows.len(), nodes.len());
    }
    let fleet_plans: i64 = sqlx::query_scalar(sql!(
        "SELECT SUM(custom_plans + generic_plans)::BIGINT
         FROM pg_prepared_statements WHERE statement LIKE '%WITH uploading AS MATERIALIZED%'
           AND parameter_types[1] = 'uuid[]'::regtype
           AND statement NOT LIKE '%pg_prepared_statements%'"
    ))
    .fetch_one(pool)
    .await?;
    assert_eq!(
        fleet_plans, 11,
        "fleet calls retain their array statement after UUID reads"
    );
    let mut legacy = Vec::new();
    for ranked in [false, true] {
        legacy.push((
            ranked,
            store
                .batched_occupancy_rows(
                    &nodes,
                    ranked.then_some(AGE),
                    ResidencyEvidence::LifecycleOnly,
                )
                .await?,
        ));
    }
    for mode in ["auto", "force_custom_plan", "force_generic_plan"] {
        sqlx::query(sql!("SELECT set_config('plan_cache_mode', $1, false)"))
            .bind(mode)
            .execute(pool)
            .await?;
        let query = format!(
            "EXPLAIN (ANALYZE, FORMAT JSON, TIMING OFF) {}",
            super::query(true, ResidencyEvidence::LifecycleOnly)
        );
        let value: serde_json::Value =
            sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(query.as_str())))
                .bind(node)
                .bind(i64::try_from(AGE.as_secs())?)
                .fetch_one(pool)
                .await?;
        let explained: Vec<Explained> = serde_json::from_value(value)?;
        assert!(
            explained
                .first()
                .expect("one EXPLAIN result")
                .plan
                .snapshot_rows()
                < f64::from(UNRELATED_ROWS),
            "{mode} must not read the unrelated catalog: {:?}",
            explained.first().expect("one EXPLAIN result").plan
        );
        for (ranked, expected) in &legacy {
            let ranked = *ranked;
            let actual = store
                .occupancy_for_nodes(
                    &nodes,
                    ranked.then_some(AGE),
                    ResidencyEvidence::LifecycleOnly,
                )
                .await?;
            for row in expected {
                let count = actual.get(&row.node_id).expect("requested node").occupancy;
                assert_eq!(
                    (count.active, count.local_snapshots),
                    (row.active, row.local_snapshots)
                );
            }
            let before = explain_cached_legacy(pool, &nodes, ranked).await?;
            let before: Vec<Explained> = serde_json::from_value(before)?;
            let value = explain_cached_fleet(pool, &nodes, ranked).await?;
            let explained: Vec<Explained> = serde_json::from_value(value)?;
            println!(
                "{mode} ranked={ranked} actual snapshot/storage rows: legacy={} candidate={}",
                before.first().expect("one plan").plan.snapshot_rows(),
                explained.first().expect("one plan").plan.snapshot_rows()
            );
            let plan = &explained.first().expect("one EXPLAIN result").plan;
            assert!(
                plan.snapshot_rows() < f64::from(UNRELATED_ROWS),
                "{mode} ranked={ranked} fleet must not read unrelated history: {plan:?}"
            );
            assert!(
                plan.snapshot_bitmap_entries() < f64::from(UNRELATED_ROWS),
                "{mode} ranked={ranked} fleet must not bitmap retained source history: {plan:?}"
            );
        }
    }
    Ok(())
}

// EXPLAIN EXECUTE observes the actual cached statement, including AUTO's
// generic/custom choice. EXPLAIN followed by fresh SQL would replan the query.
async fn explain_cached_fleet(
    pool: &PgPool,
    nodes: &[NodeId],
    ranked: bool,
) -> TestResult<serde_json::Value> {
    let query = super::super::fleet_occupancy::query(ranked, ResidencyEvidence::LifecycleOnly);
    let name: String = sqlx::query_scalar(sql!(
        "SELECT quote_ident(name) FROM pg_prepared_statements
         WHERE strpos(statement, $1) > 0 AND statement NOT LIKE '%EXPLAIN%'
         ORDER BY custom_plans + generic_plans DESC LIMIT 1"
    ))
    .bind(query)
    .fetch_one(pool)
    .await?;
    explain_fleet_statement(pool, &name, nodes, Some(AGE)).await
}

async fn explain_cached_legacy(
    pool: &PgPool,
    nodes: &[NodeId],
    ranked: bool,
) -> TestResult<serde_json::Value> {
    let name: String = sqlx::query_scalar(sql!(
        "SELECT quote_ident(name) FROM pg_prepared_statements
         WHERE statement LIKE '%WITH scope AS (%' AND statement NOT LIKE '%EXPLAIN%'
           AND parameter_types[1] = 'uuid[]'::regtype
           AND cardinality(parameter_types) = $1
         ORDER BY custom_plans + generic_plans DESC LIMIT 1"
    ))
    .bind(if ranked { 2_i32 } else { 1 })
    .fetch_one(pool)
    .await?;
    explain_fleet_statement(pool, &name, nodes, ranked.then_some(AGE)).await
}

async fn explain_fleet_statement(
    pool: &PgPool,
    name: &str,
    nodes: &[NodeId],
    age: Option<Duration>,
) -> TestResult<serde_json::Value> {
    // The name is server-quoted; values are typed UUIDs and a fixed duration.
    let ids = nodes
        .iter()
        .map(|id| format!("'{id}'"))
        .collect::<Vec<_>>()
        .join(",");
    let age_bind = age
        .map(|value| format!(", {}", value.as_secs()))
        .unwrap_or_default();
    let explain = format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON, TIMING OFF) EXECUTE {name}(ARRAY[{ids}]::UUID[]{age_bind})"
    );
    Ok(
        sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(explain.as_str())))
            .persistent(false)
            .fetch_one(pool)
            .await?,
    )
}

async fn fleet(
    pool: &PgPool,
    nodes: &[NodeId],
    ranked: bool,
    residency: ResidencyEvidence,
) -> TestResult<Vec<OccupancyRow>> {
    let query = super::super::fleet_occupancy::query(ranked, residency);
    Ok(sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(nodes)
        .bind(i64::try_from(AGE.as_secs())?)
        .fetch_all(pool)
        .await?)
}

#[tokio::test]
async fn fleet_matches_legacy_occupancy_and_blockers() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(1).await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let nodes = fixture(pool).await?;
    let store = AdminNodeStore::new(pool.clone());
    let oracle = legacy_oracle(&store, &nodes).await?;
    for mode in ["force_custom_plan", "force_generic_plan", "auto"] {
        sqlx::query(sql!("SELECT set_config('plan_cache_mode', $1, false)"))
            .bind(mode)
            .execute(pool)
            .await?;
        for (ranked, residency, expected) in &oracle {
            let mut actual = fleet(pool, &nodes, *ranked, *residency).await?;
            actual.sort_by_key(|row| row.node_id);
            assert_eq!(
                &actual, expected,
                "{mode} ranked={ranked} residency={residency:?}"
            );
        }
    }
    Ok(())
}

// This opt-in benchmark uses the migrated schema and real store queries. It
// creates its own disposable database; no connection to a live SPACE DB is used.
#[tokio::test]
#[ignore = "local 400k-sandbox/2m-snapshot benchmark; see node occupancy benchmark notes"]
async fn benchmark_single_node_occupancy() -> TestResult {
    // CI runs ignored tests too; the large fixture is an explicit local opt-in.
    if std::env::var_os("SPACE_RUN_NODE_OCCUPANCY_BENCHMARK").is_none() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db_with_max_connections(1).await? else {
        return Ok(());
    };
    let pool = &db.pool;
    sqlx::query(sql!(
        "CREATE TABLE occupancy_benchmark_nodes AS
         SELECT i, space_uuid_v7() AS id FROM generate_series(0,447) i"
    ))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         SELECT id, '10.0.0.12'::INET, 'worker', 'space', 'registered'
         FROM occupancy_benchmark_nodes"
    ))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "CREATE TABLE occupancy_benchmark_sandboxes AS
         SELECT g.i, space_uuid_v7() AS id, space_uuid_v7() AS artifact_id, n.id AS node_id
         FROM generate_series(1,400000) g(i) JOIN occupancy_benchmark_nodes n ON n.i=g.i%448"
    ))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, target_node_id, placement_changed_at, lifecycle_changed_at,
             current_local_artifact_id, current_local_artifact_kind, current_local_artifact_origin,
             network_policy_generation)
         SELECT id, '00000000-0000-0000-0000-000000000010',
                '00000000-0000-0000-0000-000000000001',
                CASE WHEN i<=200000 THEN 'started' ELSE 'suspended' END, 'unplaced', 'k8s',
                CASE WHEN i<=200000 THEN node_id END, node_id,
                NOW()-INTERVAL '1 hour', NOW()-INTERVAL '2 days',
                CASE WHEN i%2=0 THEN artifact_id END,
                CASE WHEN i%2=0 THEN 'full' END, CASE WHEN i%2=0 THEN 'suspend' END, 1
         FROM occupancy_benchmark_sandboxes"
    ))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, source_node_id, type, state, timestamp,
             local_artifact_id, artifact_kind, artifact_origin)
         SELECT space_uuid_v7(), id, node_id, 'full',
                CASE WHEN version=0 THEN 'ready' ELSE 'creating' END, NOW(),
                CASE WHEN i%2=0 THEN artifact_id END,
                CASE WHEN i%2=0 THEN 'full' END, CASE WHEN i%2=0 THEN 'suspend' END
         FROM occupancy_benchmark_sandboxes CROSS JOIN generate_series(0,1) version
         WHERE i>200000 AND (version=0 OR i<=201000)"
    ))
    .execute(pool)
    .await?;
    let node: NodeId =
        sqlx::query_scalar(sql!("SELECT id FROM occupancy_benchmark_nodes WHERE i=0"))
            .fetch_one(pool)
            .await?;
    let store = AdminNodeStore::new(pool.clone());
    for stage in ["original", "history", "skew"] {
        if stage == "history" {
            sqlx::query(sql!(
                "INSERT INTO snapshots (id, sandbox_id, source_node_id, type, state, timestamp,
                     local_artifact_id, artifact_kind, artifact_origin)
                 SELECT space_uuid_v7(), id, node_id, 'full', 'ready', NOW()-version*INTERVAL '1 day',
                        CASE WHEN i%2=0 THEN space_uuid_v7() END,
                        CASE WHEN i%2=0 THEN 'full' END, CASE WHEN i%2=0 THEN 'suspend' END
                 FROM occupancy_benchmark_sandboxes CROSS JOIN generate_series(1,9) version
                 WHERE i>200000"
            ))
            .execute(pool).await?;
        }
        if stage == "skew" {
            sqlx::query(sql!(
                "UPDATE sandboxes SET target_node_id=$1
                 WHERE id IN (SELECT id FROM occupancy_benchmark_sandboxes WHERE i>200000 AND i<=225000)"
            ))
            .bind(node).execute(pool).await?;
        }
        sqlx::query(sql!(
            "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
             SELECT sn.id, 's3://test-occupancy/benchmark' FROM snapshots sn
             WHERE state='ready' AND NOT EXISTS (
                 SELECT 1 FROM snapshot_storage_locations ssl WHERE ssl.snapshot_id=sn.id)"
        ))
        .execute(pool)
        .await?;
        sqlx::query(sql!("VACUUM ANALYZE")).execute(pool).await?;
        if std::env::var_os("SPACE_RUN_FLEET_OCCUPANCY_BENCHMARK").is_some() {
            let nodes: Vec<NodeId> =
                sqlx::query_scalar(sql!("SELECT id FROM occupancy_benchmark_nodes ORDER BY i"))
                    .fetch_all(pool)
                    .await?;
            for mode in ["force_custom_plan", "force_generic_plan"] {
                sqlx::query(sql!("SELECT set_config('plan_cache_mode', $1, false)"))
                    .bind(mode)
                    .execute(pool)
                    .await?;
                for ranked in [false, true] {
                    let mut before = Vec::new();
                    let mut after = Vec::new();
                    for _ in 0..3 {
                        let start = Instant::now();
                        let mut expected = store
                            .batched_occupancy_rows(
                                &nodes,
                                ranked.then_some(AGE),
                                ResidencyEvidence::LifecycleOnly,
                            )
                            .await?;
                        before.push(start.elapsed());
                        let start = Instant::now();
                        let mut actual =
                            fleet(pool, &nodes, ranked, ResidencyEvidence::LifecycleOnly).await?;
                        after.push(start.elapsed());
                        expected.sort_by_key(|row| row.node_id);
                        actual.sort_by_key(|row| row.node_id);
                        assert_eq!(actual, expected);
                    }
                    before.sort();
                    after.sort();
                    println!(
                        "FLEET {stage} {mode} ranked={ranked}: before={:?} after={:?}",
                        before[1], after[1]
                    );
                    let value = explain_cached_fleet(pool, &nodes, ranked).await?;
                    println!("FLEET PLAN {stage} {mode} ranked={ranked}: {value}");
                    let before = explain_cached_legacy(pool, &nodes, ranked).await?;
                    println!("LEGACY PLAN {stage} {mode} ranked={ranked}: {before}");
                }
            }
            // A fresh connection gives AUTO a clean history, then alternates
            // the real store's fleet/scalar statements on that same session.
            let automatic = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect_with(pool.connect_options().as_ref().clone())
                .await?;
            let auto_store = AdminNodeStore::new(automatic.clone());
            let mut samples = Vec::new();
            for _ in 0..8 {
                let start = Instant::now();
                let rows = auto_store
                    .occupancy_for_nodes(&nodes, None, ResidencyEvidence::LifecycleOnly)
                    .await?;
                samples.push(start.elapsed());
                let one = auto_store
                    .occupancy_for_nodes(
                        std::slice::from_ref(&node),
                        None,
                        ResidencyEvidence::LifecycleOnly,
                    )
                    .await?;
                let fleet_count = rows.get(&node).expect("fleet node").occupancy;
                let single_count = one.get(&node).expect("single node").occupancy;
                assert_eq!(
                    (fleet_count.active, fleet_count.local_snapshots),
                    (single_count.active, single_count.local_snapshots)
                );
            }
            let plans: Vec<(String, i64, i64)> = sqlx::query_as(sql!(
                "SELECT parameter_types[1]::TEXT, custom_plans, generic_plans
                 FROM pg_prepared_statements WHERE statement LIKE '%WITH uploading AS MATERIALIZED%'
                   AND statement NOT LIKE '%pg_prepared_statements%' ORDER BY 1"
            ))
            .fetch_all(&automatic)
            .await?;
            println!("FLEET {stage} auto mixed: samples={samples:?} prepared={plans:?}");
            let value = explain_cached_fleet(&automatic, &nodes, false).await?;
            println!("FLEET PLAN {stage} auto ranked=false: {value}");
            automatic.close().await;
            if stage == "skew" {
                let concurrent = sqlx::postgres::PgPoolOptions::new()
                    .max_connections(4)
                    .connect_with(pool.connect_options().as_ref().clone())
                    .await?;
                let run = || async {
                    let start = Instant::now();
                    let mut rows =
                        fleet(&concurrent, &nodes, false, ResidencyEvidence::LifecycleOnly).await?;
                    rows.sort_by_key(|row| row.node_id);
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>((start.elapsed(), rows))
                };
                let (a, b, c, d) = tokio::try_join!(run(), run(), run(), run())?;
                assert_eq!(a.1, b.1);
                assert_eq!(a.1, c.1);
                assert_eq!(a.1, d.1);
                println!("FLEET {stage} four concurrent: {:?}", [a.0, b.0, c.0, d.0]);
                concurrent.close().await;
            }
            continue;
        }
        for mode in ["force_custom_plan", "force_generic_plan"] {
            sqlx::query(sql!("SELECT set_config('plan_cache_mode', $1, false)"))
                .bind(mode)
                .execute(pool)
                .await?;
            let mut before = Vec::new();
            let mut after = Vec::new();
            for _ in 0..3 {
                let start = Instant::now();
                let expected = store
                    .batched_occupancy_rows(
                        std::slice::from_ref(&node),
                        Some(AGE),
                        ResidencyEvidence::LifecycleOnly,
                    )
                    .await?;
                before.push(start.elapsed());
                let start = Instant::now();
                let actual = single(pool, node, true, ResidencyEvidence::LifecycleOnly).await?;
                after.push(start.elapsed());
                assert_eq!(Some(&actual), expected.first());
            }
            before.sort();
            after.sort();
            let query = format!(
                "EXPLAIN (ANALYZE, FORMAT JSON, TIMING OFF) {}",
                super::query(true, ResidencyEvidence::LifecycleOnly)
            );
            let value: serde_json::Value =
                sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(query.as_str())))
                    .bind(node)
                    .bind(i64::try_from(AGE.as_secs())?)
                    .fetch_one(pool)
                    .await?;
            let explained: Vec<Explained> = serde_json::from_value(value)?;
            println!(
                "{stage} {mode}: before={:?} after={:?} snapshot/storage rows={}",
                before.get(1).expect("three samples"),
                after.get(1).expect("three samples"),
                explained.first().expect("one plan").plan.snapshot_rows()
            );
            explained
                .first()
                .expect("one plan")
                .plan
                .print_snapshot_accesses();
        }
    }
    Ok(())
}
