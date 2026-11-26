//! Each transaction against the single statement it replaced, which
//! `pplx-space-api`'s admin node registry ran on the pool: the old statement,
//! verbatim, and the coordinator, each from the same rows. A test runs every
//! case in one database, which it seeds again before each run. Each case
//! compares what the two return, or how they fail, and every column of every
//! `nodes` and `node_direct` row they leave. The timestamps a transaction
//! writes are compared by which of the run's instants they are, since the two
//! run at different times.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use pplx_space_db_test_fixtures::{TestDb, TestDbResult, pg_test_db_with_max_connections};
use pplx_space_postgres_pool::DbErrorKind;
use pplx_space_sql_locator::sql;
use sqlx::PgPool;
use time::format_description::well_known::Rfc3339;
use uuid::Uuid;

use super::*;

/// The reactivation statement of api's `create_node` handler.
const OLD_REACTIVATE: &str = sql!(
    "WITH candidate AS (
           SELECT nd.node_id
           FROM node_direct nd
           JOIN nodes n ON n.id = nd.node_id
           WHERE nd.deleted_at IS NOT NULL
             AND n.managed_by = $10
             AND (
               n.id = $1
               OR ($8::BOOL AND nd.node_api_endpoint = $6)
             )
           ORDER BY (n.id = $1) DESC,
                    nd.updated_at DESC,
                    n.created_at DESC,
                    n.id DESC
           LIMIT 1
         ),
         updated_node AS (
           UPDATE nodes n
           SET address = NULLIF($3, '')::INET,
             role = $4,
             managed_by = $10,
             state = COALESCE($5, 'registered'),
             last_heartbeat = CASE
                 WHEN $9::BOOL THEN NOW()
                 ELSE n.last_heartbeat
             END
           WHERE n.id = (SELECT node_id FROM candidate)
           RETURNING n.id,
                     host(n.address) AS address,
                     n.role,
                     n.managed_by,
                     n.state,
                     n.last_heartbeat,
                     n.created_at
         ),
         updated_direct AS (
           UPDATE node_direct nd
           SET name = $2,
             node_api_endpoint = $6,
             metadata = $7,
             deleted_at = NULL,
             updated_at = NOW()
           WHERE nd.node_id = (SELECT id FROM updated_node)
           RETURNING nd.node_id,
                     nd.name,
                     nd.node_api_endpoint,
                     nd.metadata,
                     nd.updated_at,
                     nd.deleted_at
         )
         SELECT n.id,
                d.name,
                n.address,
                d.node_api_endpoint,
                n.role,
                n.managed_by,
                n.state,
                d.metadata,
                n.last_heartbeat,
                n.created_at,
                d.updated_at,
                d.deleted_at,
                NULL::TEXT AS node_name
         FROM updated_node n
         JOIN updated_direct d ON d.node_id = n.id"
);

/// The insert statement of api's `create_node` handler.
const OLD_CREATE: &str = sql!(
    "WITH inserted_node AS (
           INSERT INTO nodes (id, address, role, managed_by, state, last_heartbeat)
           VALUES (
               $1,
               NULLIF($3, '')::INET,
               $4,
               $9,
               COALESCE($5, 'registered'),
               CASE WHEN $8::BOOL THEN NOW() ELSE NULL END
           )
           RETURNING id,
                     host(address) AS address,
                     role,
                     managed_by,
                     state,
                     last_heartbeat,
                     created_at
         ),
         inserted_direct AS (
           INSERT INTO node_direct (node_id, name, node_api_endpoint, metadata)
           SELECT id, $2, $6, $7
           FROM inserted_node
           RETURNING node_id,
                     name,
                     node_api_endpoint,
                     metadata,
                     updated_at,
                     deleted_at
         )
         SELECT n.id,
                d.name,
                n.address,
                d.node_api_endpoint,
                n.role,
                n.managed_by,
                n.state,
                d.metadata,
                n.last_heartbeat,
                n.created_at,
                d.updated_at,
                d.deleted_at,
                NULL::TEXT AS node_name
         FROM inserted_node n
         JOIN inserted_direct d ON d.node_id = n.id"
);

/// `update_node`'s update.
const OLD_UPDATE: &str = sql!(
    "WITH updated_node AS (
           UPDATE nodes n
           SET address = COALESCE(NULLIF($2, '')::INET, address),
               role = COALESCE($4, role),
               state = COALESCE($5, state),
               last_heartbeat = CASE
                   WHEN $8::BOOL THEN NOW()
                   ELSE n.last_heartbeat
               END
           WHERE n.id = $7
             AND n.managed_by = $9
           RETURNING n.id,
                     host(n.address) AS address,
                     n.role,
                     n.managed_by,
                     n.state,
                     n.last_heartbeat,
                     n.created_at
         ),
         updated_direct AS (
           UPDATE node_direct nd
           SET name = COALESCE($1, name),
               node_api_endpoint = COALESCE($3, node_api_endpoint),
               metadata = COALESCE($6::JSONB, metadata),
               updated_at = NOW()
           FROM updated_node n
           WHERE nd.node_id = n.id
             AND nd.deleted_at IS NULL
           RETURNING nd.node_id,
                     nd.name,
                     nd.node_api_endpoint,
                     nd.metadata,
                     nd.updated_at,
                     nd.deleted_at
         )
         SELECT n.id,
                d.name,
                n.address,
                d.node_api_endpoint,
                n.role,
                n.managed_by,
                n.state,
                d.metadata,
                n.last_heartbeat,
                n.created_at,
                d.updated_at,
                d.deleted_at,
                NULL::TEXT AS node_name
         FROM updated_node n
         JOIN updated_direct d ON d.node_id = n.id"
);

/// The row the old statements return, as the API decoded it.
#[derive(Debug, sqlx::FromRow)]
struct OldRow {
    id: NodeId,
    name: Option<String>,
    address: Option<String>,
    node_api_endpoint: Option<String>,
    role: NodeRole,
    managed_by: NodeManagedBy,
    state: Option<String>,
    metadata: Option<sqlx::types::Json<serde_json::Value>>,
    last_heartbeat: Option<OffsetDateTime>,
    created_at: OffsetDateTime,
    updated_at: Option<OffsetDateTime>,
    deleted_at: Option<OffsetDateTime>,
    node_name: Option<String>,
}

async fn old_reactivate(
    executor: impl sqlx::PgExecutor<'_>,
    registration: &DirectNodeRegistration<'_>,
    match_endpoint: bool,
) -> Result<Option<OldRow>, sqlx::Error> {
    sqlx::query_as::<_, OldRow>(OLD_REACTIVATE)
        .bind(registration.node_id)
        .bind(registration.name)
        .bind(registration.address.unwrap_or(""))
        .bind(registration.role.as_str())
        .bind(registration.state)
        .bind(registration.node_api_endpoint)
        .bind(sqlx::types::Json(registration.metadata))
        .bind(match_endpoint)
        .bind(registration.stamp_heartbeat)
        .bind(registration.managed_by)
        .fetch_optional(executor)
        .await
}

async fn old_create(
    pool: &PgPool,
    registration: &DirectNodeRegistration<'_>,
) -> Result<OldRow, sqlx::Error> {
    sqlx::query_as::<_, OldRow>(OLD_CREATE)
        .bind(registration.node_id)
        .bind(registration.name)
        .bind(registration.address.unwrap_or(""))
        .bind(registration.role.as_str())
        .bind(registration.state)
        .bind(registration.node_api_endpoint)
        .bind(sqlx::types::Json(registration.metadata))
        .bind(registration.stamp_heartbeat)
        .bind(registration.managed_by)
        .fetch_one(pool)
        .await
}

async fn old_update(
    pool: &PgPool,
    update: &DirectNodeUpdate<'_>,
) -> Result<Option<OldRow>, sqlx::Error> {
    sqlx::query_as::<_, OldRow>(OLD_UPDATE)
        .bind(update.name)
        .bind(update.address.unwrap_or(""))
        .bind(update.node_api_endpoint)
        .bind(update.role.map(NodeRole::as_str))
        .bind(update.state)
        .bind(update.metadata.map(sqlx::types::Json))
        .bind(update.node_id)
        .bind(update.stamp_heartbeat)
        .bind(update.managed_by)
        .fetch_optional(pool)
        .await
}

/// A seeded `nodes` row and its optional `node_direct` row. Times are hours
/// after 2001-01-01, long before any instant a test's transactions write.
#[derive(Clone, Copy)]
struct Seed {
    id: NodeId,
    managed_by: SubstrateKind,
    address: Option<&'static str>,
    state: Option<&'static str>,
    created_hour: i64,
    heartbeat_hour: Option<i64>,
    direct: Option<SeedDirect>,
}

#[derive(Clone, Copy)]
struct SeedDirect {
    name: Option<&'static str>,
    endpoint: &'static str,
    updated_hour: i64,
    deleted_hour: Option<i64>,
}

fn node_id(n: u128) -> NodeId {
    NodeId::from_uuid(Uuid::from_u128(n))
}

fn hour(hour: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(978_307_200 + hour * 3600).expect("in range")
}

/// A Direct node registered at `endpoint`, retired at `deleted_hour` when
/// given.
fn seed(id: NodeId, endpoint: &'static str, deleted_hour: Option<i64>) -> Seed {
    Seed {
        id,
        managed_by: SubstrateKind::Space,
        address: Some("10.0.0.10"),
        state: if deleted_hour.is_some() {
            Some("deleted")
        } else {
            Some("registered")
        },
        created_hour: 0,
        heartbeat_hour: Some(1),
        direct: Some(SeedDirect {
            name: Some("seeded"),
            endpoint,
            updated_hour: 2,
            deleted_hour,
        }),
    }
}

/// A Direct node retired at `endpoint`, created and last updated at the given
/// hours.
fn retired_at(id: NodeId, endpoint: &'static str, created_hour: i64, updated_hour: i64) -> Seed {
    Seed {
        created_hour,
        direct: Some(SeedDirect {
            name: Some("seeded"),
            endpoint,
            updated_hour,
            deleted_hour: Some(20),
        }),
        ..seed(id, endpoint, Some(20))
    }
}

/// The database a test's cases share, each run starting from [`reseed`].
async fn registry_db() -> TestDbResult<TestDb> {
    // The concurrent cases hold a transaction open, run the statement that
    // waits for it, and watch the wait from a third connection.
    pg_test_db_with_max_connections(4).await
}

/// Leave `pool`'s database with `seeds` and nothing else written to either
/// table, as a fresh one seeded with them. An earlier run left only `nodes`
/// rows and their `node_direct` rows, which the delete cascades to: neither
/// table draws from a sequence or has a trigger that writes another table,
/// and no run writes another table or leaves a transaction that can still
/// commit: each commits, or fails and is rolled back once dropped, and the
/// delete waits for that rollback if it needs the rows.
async fn reseed(pool: &PgPool, seeds: &[Seed]) -> TestDbResult<()> {
    sqlx::query(sql!("DELETE FROM nodes")).execute(pool).await?;
    // One statement writes every seed. Its reads see the tables as they were
    // before its inserts, so its count is what the delete left.
    let left = sqlx::query_scalar::<_, i64>(sql!(
        "WITH seeded_nodes AS (
             INSERT INTO nodes (id, address, role, managed_by, state, last_heartbeat, created_at)
             SELECT id, address::INET, 'worker', managed_by, state, last_heartbeat, created_at
             FROM UNNEST($1::UUID[], $2::TEXT[], $3::TEXT[], $4::TEXT[],
                         $5::TIMESTAMPTZ[], $6::TIMESTAMPTZ[])
                 AS seed (id, address, managed_by, state, last_heartbeat, created_at)
         ),
         seeded_direct AS (
             INSERT INTO node_direct
                 (node_id, name, node_api_endpoint, metadata, updated_at, deleted_at)
             SELECT node_id, name, endpoint, '{\"seeded\": true}'::JSONB, updated_at, deleted_at
             FROM UNNEST($7::UUID[], $8::TEXT[], $9::TEXT[],
                         $10::TIMESTAMPTZ[], $11::TIMESTAMPTZ[])
                 AS seed (node_id, name, endpoint, updated_at, deleted_at)
         )
         SELECT (SELECT count(*) FROM nodes) + (SELECT count(*) FROM node_direct)"
    ))
    .bind(column(seeds, |seed| seed.id.as_uuid()))
    .bind(column(seeds, |seed| seed.address))
    .bind(column(seeds, |seed| seed.managed_by.as_str()))
    .bind(column(seeds, |seed| seed.state))
    .bind(column(seeds, |seed| seed.heartbeat_hour.map(hour)))
    .bind(column(seeds, |seed| hour(seed.created_hour)))
    .bind(direct_column(seeds, |id, _| id.as_uuid()))
    .bind(direct_column(seeds, |_, direct| direct.name))
    .bind(direct_column(seeds, |_, direct| direct.endpoint))
    .bind(direct_column(seeds, |_, direct| hour(direct.updated_hour)))
    .bind(direct_column(seeds, |_, direct| {
        direct.deleted_hour.map(hour)
    }))
    .fetch_one(pool)
    .await?;
    assert_eq!(left, 0, "rows besides the seeds");
    Ok(())
}

/// One column of `seeds`, as an `UNNEST` parameter.
fn column<T>(seeds: &[Seed], value: impl Fn(&Seed) -> T) -> Vec<T> {
    seeds.iter().map(value).collect()
}

/// One column of the seeds' `node_direct` rows, as an `UNNEST` parameter.
fn direct_column<T>(seeds: &[Seed], value: impl Fn(NodeId, SeedDirect) -> T) -> Vec<T> {
    seeds
        .iter()
        .filter_map(|seed| Some(value(seed.id, seed.direct?)))
        .collect()
}

/// What a registry write returned: the API's row, no row, or a failure by
/// its classification, SQLSTATE and constraint.
#[derive(Debug, PartialEq)]
enum Outcome {
    Row(serde_json::Value),
    NoRow,
    Failed {
        kind: DbErrorKind,
        code: Option<String>,
        constraint: Option<String>,
    },
}

impl Outcome {
    fn failed(error: sqlx::Error) -> Self {
        let code = error
            .as_database_error()
            .and_then(|database| database.code())
            .map(|code| code.into_owned());
        let error = DbError::from(error);
        Self::Failed {
            kind: error.kind(),
            code,
            constraint: error.constraint().map(str::to_string),
        }
    }

    fn old(result: Result<Option<OldRow>, sqlx::Error>) -> Self {
        match result {
            Ok(Some(row)) => Self::Row(serde_json::json!({
                "id": row.id.as_uuid().to_string(),
                "name": row.name,
                "address": row.address,
                "node_api_endpoint": row.node_api_endpoint,
                "role": row.role.as_str(),
                "managed_by": row.managed_by.as_str(),
                "state": row.state,
                "metadata": row.metadata.map(|metadata| metadata.0),
                "last_heartbeat": row.last_heartbeat.map(rfc3339),
                "created_at": rfc3339(row.created_at),
                "updated_at": row.updated_at.map(rfc3339),
                "deleted_at": row.deleted_at.map(rfc3339),
                "node_name": row.node_name,
            })),
            Ok(None) => Self::NoRow,
            Err(error) => Self::failed(error),
        }
    }

    /// The coordinator's node as the API maps it into the same row, which
    /// has no Kubernetes node name.
    fn new(result: Result<Option<DirectNode>, DbError>) -> Self {
        match result {
            Ok(Some(node)) => Self::Row(serde_json::json!({
                "id": node.id.as_uuid().to_string(),
                "name": node.name,
                "address": node.address,
                "node_api_endpoint": node.node_api_endpoint,
                "role": node.role.as_str(),
                "managed_by": node.managed_by.as_str(),
                "state": node.state,
                "metadata": node.metadata,
                "last_heartbeat": node.last_heartbeat.map(rfc3339),
                "created_at": rfc3339(node.created_at),
                "updated_at": rfc3339(node.updated_at),
                "deleted_at": node.deleted_at.map(rfc3339),
                "node_name": null,
            })),
            Ok(None) => Self::NoRow,
            Err(error) => Self::failed(error.into()),
        }
    }
}

fn rfc3339(instant: OffsetDateTime) -> String {
    instant.format(&Rfc3339).expect("formats")
}

/// One database after a case: the outcome and every row of both tables.
#[derive(Debug, PartialEq)]
struct Observed {
    outcome: Outcome,
    nodes: serde_json::Value,
    node_direct: serde_json::Value,
}

impl Observed {
    /// Every column of the `nodes` row `id`, or of its `node_direct` row.
    fn node(&self, id: NodeId) -> Option<&serde_json::Value> {
        find(&self.nodes, "id", id)
    }

    fn direct(&self, id: NodeId) -> Option<&serde_json::Value> {
        find(&self.node_direct, "node_id", id)
    }
}

fn find<'a>(rows: &'a serde_json::Value, key: &str, id: NodeId) -> Option<&'a serde_json::Value> {
    let id = id.as_uuid().to_string();
    rows.as_array()?.iter().find(|row| row[key] == id.as_str())
}

const NOW: &str = "<now>";

async fn observe(pool: &PgPool, outcome: Outcome) -> TestDbResult<Observed> {
    let (nodes, node_direct) = sqlx::query_as::<_, (serde_json::Value, serde_json::Value)>(sql!(
        "SELECT (SELECT COALESCE(jsonb_agg(to_jsonb(n) ORDER BY n.id), '[]') FROM nodes n),
                (SELECT COALESCE(jsonb_agg(to_jsonb(nd) ORDER BY nd.node_id), '[]')
                 FROM node_direct nd)"
    ))
    .fetch_one(pool)
    .await?;
    let mut observed = Observed {
        outcome,
        nodes,
        node_direct,
    };
    normalize_instants(&mut observed);
    Ok(observed)
}

/// Rewrite each timestamp column a case's transactions wrote, any after 2020,
/// as `<now>` followed by its rank among the distinct instants they wrote,
/// and every seeded one in one format. Equal instants keep equal labels, so a
/// case compares which of its transactions wrote each timestamp.
fn normalize_instants(observed: &mut Observed) {
    let cutoff = OffsetDateTime::from_unix_timestamp(1_577_836_800).expect("in range");
    let mut written = BTreeSet::new();
    let mut values = vec![&mut observed.nodes, &mut observed.node_direct];
    if let Outcome::Row(row) = &mut observed.outcome {
        values.push(row);
    }
    for value in &mut values {
        visit_instants(value, &mut |instant| {
            if instant >= cutoff {
                written.insert(instant);
            }
            None
        });
    }
    let labels = written
        .into_iter()
        .enumerate()
        .map(|(rank, instant)| (instant, format!("{NOW} {rank}")))
        .collect::<BTreeMap<_, _>>();
    for value in values {
        visit_instants(value, &mut |instant| {
            Some(
                labels
                    .get(&instant)
                    .cloned()
                    .unwrap_or_else(|| rfc3339(instant)),
            )
        });
    }
}

/// The timestamp columns of the two tables' rows and of the returned row.
/// Only these are normalized: a timestamp-shaped string anywhere else, such
/// as a value inside `metadata`, is compared as written.
const TIMESTAMP_COLUMNS: [&str; 4] = ["created_at", "updated_at", "last_heartbeat", "deleted_at"];

/// Call `f` on each timestamp column of `value`, a row or an array of rows,
/// and replace the column's value with what it returns.
fn visit_instants(
    value: &mut serde_json::Value,
    f: &mut impl FnMut(OffsetDateTime) -> Option<String>,
) {
    match value {
        serde_json::Value::Array(rows) => {
            for row in rows {
                visit_instants(row, f);
            }
        }
        serde_json::Value::Object(columns) => {
            for column in TIMESTAMP_COLUMNS {
                if let Some(serde_json::Value::String(text)) = columns.get_mut(column) {
                    let instant = OffsetDateTime::parse(text, &Rfc3339)
                        .expect("a timestamp column holds an RFC 3339 instant");
                    if let Some(replacement) = f(instant) {
                        *text = replacement;
                    }
                }
            }
        }
        _ => {}
    }
}

/// Seed `pool`'s database with `seeds` and run `old`, seed it again and run
/// `new`, and assert they agree. Returns what both observed.
async fn compare(
    pool: &PgPool,
    seeds: &[Seed],
    old: impl AsyncFnOnce(&PgPool) -> Outcome,
    new: impl AsyncFnOnce(&PgPool) -> Outcome,
) -> TestDbResult<Observed> {
    reseed(pool, seeds).await?;
    let before = observe(pool, Outcome::NoRow).await?;
    let old_outcome = old(pool).await;
    let old_observed = observe(pool, old_outcome).await?;
    reseed(pool, seeds).await?;
    assert_eq!(before, observe(pool, Outcome::NoRow).await?);
    let new_outcome = new(pool).await;
    let new_observed = observe(pool, new_outcome).await?;
    assert_eq!(old_observed, new_observed);
    Ok(old_observed)
}

async fn compare_reactivation(
    pool: &PgPool,
    seeds: &[Seed],
    registration: DirectNodeRegistration<'_>,
    match_endpoint: bool,
) -> TestDbResult<Observed> {
    compare(
        pool,
        seeds,
        async |pool: &PgPool| {
            Outcome::old(old_reactivate(pool, &registration, match_endpoint).await)
        },
        async |pool: &PgPool| {
            Outcome::new(
                reactivate_node(&Db::from(pool.clone()), &registration, match_endpoint).await,
            )
        },
    )
    .await
}

async fn compare_create(
    pool: &PgPool,
    seeds: &[Seed],
    registration: DirectNodeRegistration<'_>,
) -> TestDbResult<Observed> {
    compare(
        pool,
        seeds,
        async |pool: &PgPool| Outcome::old(old_create(pool, &registration).await.map(Some)),
        async |pool: &PgPool| {
            Outcome::new(
                create_node(&Db::from(pool.clone()), &registration)
                    .await
                    .map(Some),
            )
        },
    )
    .await
}

async fn compare_update(
    pool: &PgPool,
    seeds: &[Seed],
    update: DirectNodeUpdate<'_>,
) -> TestDbResult<Observed> {
    compare(
        pool,
        seeds,
        async |pool: &PgPool| Outcome::old(old_update(pool, &update).await),
        async |pool: &PgPool| Outcome::new(update_node(&Db::from(pool.clone()), &update).await),
    )
    .await
}

fn metadata() -> serde_json::Value {
    serde_json::json!({"rack": "r1", "zone": "z2"})
}

fn registration<'a>(
    node_id: NodeId,
    endpoint: &'a str,
    metadata: &'a serde_json::Value,
) -> DirectNodeRegistration<'a> {
    DirectNodeRegistration {
        node_id,
        name: Some("registered"),
        address: Some("10.0.1.20"),
        node_api_endpoint: endpoint,
        role: NodeRole::Builder,
        managed_by: SubstrateKind::Space,
        state: None,
        metadata,
        stamp_heartbeat: false,
    }
}

fn update(node_id: NodeId, metadata: &serde_json::Value) -> DirectNodeUpdate<'_> {
    DirectNodeUpdate {
        node_id,
        managed_by: SubstrateKind::Space,
        name: Some("updated"),
        address: Some("10.0.2.30"),
        node_api_endpoint: Some("10.0.2.30:9802"),
        role: Some(NodeRole::Builder),
        state: Some("cordoned"),
        metadata: Some(metadata),
        stamp_heartbeat: true,
    }
}

fn returned(observed: &Observed) -> &serde_json::Value {
    match &observed.outcome {
        Outcome::Row(row) => row,
        other => panic!("expected a row, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn reactivation_by_id_matches_the_old_statement() -> TestDbResult<()> {
    let db = registry_db().await?;
    let metadata = metadata();
    let retired = node_id(1);
    let seeds = [seed(retired, "10.0.0.1:9802", Some(3))];
    for (stamp_heartbeat, state) in [(false, None), (true, None), (false, Some("ready"))] {
        let registration = DirectNodeRegistration {
            stamp_heartbeat,
            state,
            ..registration(retired, "10.0.0.2:9802", &metadata)
        };
        let observed = compare_reactivation(&db.pool, &seeds, registration, false).await?;
        let row = returned(&observed);
        assert_eq!(row["id"], retired.as_uuid().to_string());
        assert_eq!(row["node_api_endpoint"], "10.0.0.2:9802");
        // A retired node's `deleted` state gives way to the one registered.
        assert_eq!(row["state"], state.unwrap_or("registered"));
        assert_eq!(row["deleted_at"], serde_json::Value::Null);
        assert_eq!(row["updated_at"], format!("{NOW} 0"));
        let heartbeat = if stamp_heartbeat {
            format!("{NOW} 0")
        } else {
            rfc3339(hour(1))
        };
        assert_eq!(row["last_heartbeat"], heartbeat);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn reactivation_prefers_the_requested_id_to_an_endpoint_match() -> TestDbResult<()> {
    let db = registry_db().await?;
    let metadata = metadata();
    let by_id = node_id(1);
    // Updated later, created later and with a higher id, the endpoint's
    // node would win every tie-break after the id.
    let seeds = [
        retired_at(by_id, "10.0.0.1:9802", 0, 2),
        retired_at(node_id(9), "10.0.0.2:9802", 5, 6),
    ];
    let observed = compare_reactivation(
        &db.pool,
        &seeds,
        registration(by_id, "10.0.0.2:9802", &metadata),
        true,
    )
    .await?;
    assert_eq!(returned(&observed)["id"], by_id.as_uuid().to_string());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn reactivation_by_endpoint_breaks_ties_as_the_old_statement() -> TestDbResult<()> {
    let db = registry_db().await?;
    let metadata = metadata();
    let endpoint = "10.0.0.5:9802";
    let at = |n, created_hour, updated_hour| {
        retired_at(node_id(n), endpoint, created_hour, updated_hour)
    };
    // Each case's winner, by the latest registration update, then the latest
    // node, then the highest id.
    let cases = [
        (vec![at(3, 9, 1), at(1, 0, 8), at(2, 9, 2)], node_id(1)),
        (vec![at(3, 1, 4), at(1, 7, 4), at(2, 2, 4)], node_id(1)),
        (vec![at(1, 5, 4), at(3, 5, 4), at(2, 5, 4)], node_id(3)),
    ];
    for (seeds, winner) in cases {
        // The requested id is new, as when a registration names none.
        let observed = compare_reactivation(
            &db.pool,
            &seeds,
            registration(node_id(99), endpoint, &metadata),
            true,
        )
        .await?;
        assert_eq!(returned(&observed)["id"], winner.as_uuid().to_string());
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn reactivation_without_a_candidate_writes_nothing() -> TestDbResult<()> {
    let db = registry_db().await?;
    let metadata = metadata();
    let endpoint = "10.0.0.1:9802";
    let live = node_id(1);
    let kubernetes = Seed {
        managed_by: SubstrateKind::Kubernetes,
        ..seed(node_id(2), endpoint, Some(3))
    };
    let unregistered = Seed {
        direct: None,
        ..seed(node_id(3), endpoint, None)
    };
    let seeds = [seed(live, endpoint, None), kubernetes, unregistered];
    for (node_id, match_endpoint) in [
        // A live node, by id and by endpoint.
        (live, true),
        // A retired node another substrate manages, by id and by endpoint.
        (kubernetes.id, true),
        // A node without a registration.
        (unregistered.id, false),
    ] {
        let observed = compare_reactivation(
            &db.pool,
            &seeds,
            registration(node_id, endpoint, &metadata),
            match_endpoint,
        )
        .await?;
        assert_eq!(observed.outcome, Outcome::NoRow);
    }
    // A retired node at the endpoint, when the endpoint may not match.
    let retired = seed(node_id(4), "10.0.0.4:9802", Some(3));
    let observed = compare_reactivation(
        &db.pool,
        &[retired],
        registration(node_id(5), "10.0.0.4:9802", &metadata),
        false,
    )
    .await?;
    assert_eq!(observed.outcome, Outcome::NoRow);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn reactivation_onto_a_live_endpoint_fails_whole() -> TestDbResult<()> {
    let db = registry_db().await?;
    let metadata = metadata();
    let retired = node_id(1);
    let seeds = [
        seed(retired, "10.0.0.1:9802", Some(3)),
        seed(node_id(2), "10.0.0.2:9802", None),
    ];
    let observed = compare_reactivation(
        &db.pool,
        &seeds,
        registration(retired, "10.0.0.2:9802", &metadata),
        false,
    )
    .await?;
    assert_eq!(
        observed.outcome,
        Outcome::Failed {
            kind: DbErrorKind::UniqueViolation,
            code: Some("23505".into()),
            constraint: Some("idx_node_direct_endpoint_active".into()),
        }
    );
    // The `nodes` write rolled back with the failed `node_direct` write.
    assert_eq!(observed.node(retired).expect("seeded")["role"], "worker");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn create_matches_the_old_statement() -> TestDbResult<()> {
    let db = registry_db().await?;
    let metadata = metadata();
    let retired = seed(node_id(1), "10.0.0.1:9802", Some(3));
    for (stamp_heartbeat, address, state) in [
        (false, Some(""), None),
        (true, Some("10.0.3.40"), Some("ready")),
        (false, None, None),
    ] {
        let registration = DirectNodeRegistration {
            stamp_heartbeat,
            address,
            state,
            name: None,
            // A retired registration keeps no hold on its endpoint.
            ..registration(node_id(2), "10.0.0.1:9802", &metadata)
        };
        let observed = compare_create(&db.pool, &[retired], registration).await?;
        let row = returned(&observed);
        assert_eq!(row["created_at"], format!("{NOW} 0"));
        assert_eq!(row["updated_at"], format!("{NOW} 0"));
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_timestamp_shaped_metadata_value_is_compared_as_written() -> TestDbResult<()> {
    async fn created(
        pool: &PgPool,
        node_id: NodeId,
        metadata: &serde_json::Value,
    ) -> TestDbResult<Observed> {
        reseed(pool, &[]).await?;
        let registration = registration(node_id, "10.0.0.2:9802", metadata);
        let outcome = Outcome::new(
            create_node(&Db::from(pool.clone()), &registration)
                .await
                .map(Some),
        );
        observe(pool, outcome).await
    }

    let db = registry_db().await?;
    let written = serde_json::json!({"expires_at": "2100-01-01T00:00:00Z"});
    let changed = serde_json::json!({"expires_at": "2101-01-01T00:00:00Z"});
    let fresh = node_id(2);
    // Both versions store the value as given, and the comparison keeps it.
    let observed = compare_create(
        &db.pool,
        &[],
        registration(fresh, "10.0.0.2:9802", &written),
    )
    .await?;
    assert_eq!(returned(&observed)["metadata"], written);
    assert_eq!(
        observed.direct(fresh).expect("registered")["metadata"],
        written
    );
    // So changing only that value is a difference the comparison sees.
    assert_ne!(
        created(&db.pool, fresh, &written).await?,
        created(&db.pool, fresh, &changed).await?
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn create_of_an_existing_id_fails_as_the_old_statement() -> TestDbResult<()> {
    let db = registry_db().await?;
    let metadata = metadata();
    let live = node_id(1);
    let kubernetes = Seed {
        managed_by: SubstrateKind::Kubernetes,
        direct: None,
        ..seed(node_id(2), "10.0.0.2:9802", None)
    };
    let seeds = [seed(live, "10.0.0.1:9802", None), kubernetes];
    for existing in [live, kubernetes.id] {
        let observed = compare_create(
            &db.pool,
            &seeds,
            registration(existing, "10.0.0.9:9802", &metadata),
        )
        .await?;
        assert_eq!(
            observed.outcome,
            Outcome::Failed {
                kind: DbErrorKind::UniqueViolation,
                code: Some("23505".into()),
                constraint: Some("nodes_pkey".into()),
            }
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn create_at_a_live_endpoint_fails_whole() -> TestDbResult<()> {
    let db = registry_db().await?;
    let metadata = metadata();
    let fresh = node_id(2);
    let observed = compare_create(
        &db.pool,
        &[seed(node_id(1), "10.0.0.1:9802", None)],
        registration(fresh, "10.0.0.1:9802", &metadata),
    )
    .await?;
    assert_eq!(
        observed.outcome,
        Outcome::Failed {
            kind: DbErrorKind::UniqueViolation,
            code: Some("23505".into()),
            constraint: Some("idx_node_direct_endpoint_active".into()),
        }
    );
    // The `nodes` insert rolled back with the failed `node_direct` insert.
    assert_eq!(observed.node(fresh), None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn update_matches_the_old_statement() -> TestDbResult<()> {
    let db = registry_db().await?;
    let metadata = metadata();
    let live = node_id(1);
    let seeds = [seed(live, "10.0.0.1:9802", None)];
    let observed = compare_update(&db.pool, &seeds, update(live, &metadata)).await?;
    let row = returned(&observed);
    assert_eq!(row["name"], "updated");
    assert_eq!(row["last_heartbeat"], format!("{NOW} 0"));
    assert_eq!(row["updated_at"], format!("{NOW} 0"));

    // Nothing given keeps every value but `updated_at`; an empty address
    // keeps the address.
    let keep = DirectNodeUpdate {
        name: None,
        address: Some(""),
        node_api_endpoint: None,
        role: None,
        state: None,
        metadata: None,
        stamp_heartbeat: false,
        ..update(live, &metadata)
    };
    let observed = compare_update(&db.pool, &seeds, keep).await?;
    let row = returned(&observed);
    assert_eq!(row["name"], "seeded");
    assert_eq!(row["address"], "10.0.0.10");
    assert_eq!(row["last_heartbeat"], rfc3339(hour(1)));
    assert_eq!(row["updated_at"], format!("{NOW} 0"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn update_of_a_missing_node_writes_nothing() -> TestDbResult<()> {
    let db = registry_db().await?;
    let metadata = metadata();
    let kubernetes = Seed {
        managed_by: SubstrateKind::Kubernetes,
        ..seed(node_id(2), "10.0.0.2:9802", None)
    };
    let seeds = [seed(node_id(1), "10.0.0.1:9802", None), kubernetes];
    // No node with the id, and a live registration another substrate's node
    // holds.
    for node_id in [node_id(3), kubernetes.id] {
        let observed = compare_update(&db.pool, &seeds, update(node_id, &metadata)).await?;
        assert_eq!(observed.outcome, Outcome::NoRow);
        assert_eq!(
            observed.node(kubernetes.id).expect("seeded")["state"],
            "registered"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn update_of_a_retired_or_unregistered_node_keeps_its_nodes_write() -> TestDbResult<()> {
    let db = registry_db().await?;
    let metadata = metadata();
    let retired = node_id(1);
    let unregistered = Seed {
        direct: None,
        ..seed(node_id(2), "10.0.0.2:9802", None)
    };
    let seeds = [seed(retired, "10.0.0.1:9802", Some(3)), unregistered];
    for node_id in [retired, unregistered.id] {
        let observed = compare_update(&db.pool, &seeds, update(node_id, &metadata)).await?;
        // No row, so the API answers 404, but the `nodes` row took the update.
        assert_eq!(observed.outcome, Outcome::NoRow);
        let node = observed.node(node_id).expect("seeded");
        assert_eq!(node["state"], "cordoned");
        assert_eq!(node["address"], "10.0.2.30");
        assert_eq!(node["last_heartbeat"], format!("{NOW} 0"));
        if node_id == retired {
            let direct = observed.direct(retired).expect("seeded");
            assert_eq!(direct["name"], "seeded");
            assert_eq!(direct["updated_at"], rfc3339(hour(2)));
        }
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn update_onto_a_live_endpoint_or_a_bad_address_fails_whole() -> TestDbResult<()> {
    let db = registry_db().await?;
    let metadata = metadata();
    let live = node_id(1);
    let seeds = [
        seed(live, "10.0.0.1:9802", None),
        seed(node_id(2), "10.0.2.30:9802", None),
    ];
    let observed = compare_update(&db.pool, &seeds, update(live, &metadata)).await?;
    assert_eq!(
        observed.outcome,
        Outcome::Failed {
            kind: DbErrorKind::UniqueViolation,
            code: Some("23505".into()),
            constraint: Some("idx_node_direct_endpoint_active".into()),
        }
    );
    // The `nodes` update rolled back with the failed `node_direct` update.
    assert_eq!(observed.node(live).expect("seeded")["state"], "registered");

    let bad_address = DirectNodeUpdate {
        address: Some("not-an-address"),
        node_api_endpoint: None,
        ..update(live, &metadata)
    };
    let observed = compare_update(&db.pool, &seeds, bad_address).await?;
    assert_eq!(
        observed.outcome,
        Outcome::Failed {
            kind: DbErrorKind::Other,
            code: Some("22P02".into()),
            constraint: None,
        }
    );
    Ok(())
}

/// How long a held case's waiter has to start waiting for a lock: well under
/// a test's duration budget, since a waiter that blocks does so at once.
const LOCK_WAIT_DEADLINE: Duration = Duration::from_secs(2);

/// Wait until a statement on `pool`'s database waits for a lock, or fail
/// after [`LOCK_WAIT_DEADLINE`].
async fn until_a_statement_waits(pool: &PgPool) -> TestDbResult<()> {
    let deadline = tokio::time::Instant::now() + LOCK_WAIT_DEADLINE;
    loop {
        let waiting = sqlx::query_scalar::<_, bool>(sql!(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_stat_activity
                 WHERE datname = current_database()
                   AND wait_event_type = 'Lock'
             )"
        ))
        .fetch_one(pool)
        .await?;
        if waiting {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("no statement waited for a lock".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Run `waiter` while a holder keeps its transaction open, and run `commit`,
/// the holder's commit, once the waiter waits for one of its locks. Fails at
/// once if the waiter finishes first, since then it never waited.
async fn run_behind<T>(
    pool: &PgPool,
    commit: impl Future<Output = TestDbResult<()>>,
    waiter: impl Future<Output = T>,
) -> TestDbResult<T> {
    tokio::pin!(waiter);
    tokio::select! {
        _ = &mut waiter => {
            return Err("the waiter finished without waiting for a lock".into());
        }
        waited = until_a_statement_waits(pool) => waited?,
    }
    commit.await?;
    Ok(waiter.await)
}

/// Which version of the registry runs a statement: the single statement api
/// ran before this crate, or this crate.
#[derive(Clone, Copy, Debug)]
enum Version {
    Old,
    New,
}

/// Seed `pool`'s database with `seeds`, hold `reactivation` open as `holder`
/// runs it, and run `update` as `waiter` runs it, which waits for the node's
/// `nodes` row until the reactivation commits.
async fn update_behind_reactivation(
    pool: &PgPool,
    seeds: &[Seed],
    reactivation: &DirectNodeRegistration<'_>,
    update: &DirectNodeUpdate<'_>,
    holder: Version,
    waiter: Version,
) -> TestDbResult<Observed> {
    reseed(pool, seeds).await?;
    let commit: Pin<Box<dyn Future<Output = TestDbResult<()>>>> = match holder {
        Version::Old => {
            let mut tx = pool.begin().await?;
            assert!(
                old_reactivate(&mut *tx, reactivation, false)
                    .await?
                    .is_some()
            );
            Box::pin(async move { Ok(tx.commit().await?) })
        }
        Version::New => {
            let mut tx = Db::from(pool.clone()).begin().await?;
            assert!(reactivate_in(&mut tx, reactivation, false).await?.is_some());
            Box::pin(async move { Ok(tx.commit().await?) })
        }
    };
    let outcome = match waiter {
        Version::Old => Outcome::old(run_behind(pool, commit, old_update(pool, update)).await?),
        Version::New => Outcome::new(
            run_behind(pool, commit, update_node(&Db::from(pool.clone()), update)).await?,
        ),
    };
    observe(pool, outcome).await
}

/// Seed `pool`'s database with `seeds`, run `reactivation` with the old
/// statement and then `update` with the old statement, one after the other.
async fn update_after_reactivation(
    pool: &PgPool,
    seeds: &[Seed],
    reactivation: &DirectNodeRegistration<'_>,
    update: &DirectNodeUpdate<'_>,
) -> TestDbResult<Observed> {
    reseed(pool, seeds).await?;
    assert!(old_reactivate(pool, reactivation, false).await?.is_some());
    let outcome = Outcome::old(old_update(pool, update).await);
    observe(pool, outcome).await
}

#[tokio::test(flavor = "current_thread")]
async fn update_behind_a_reactivation_of_its_node_writes_as_if_it_ran_after() -> TestDbResult<()> {
    let db = registry_db().await?;
    let metadata = metadata();
    let retired = node_id(1);
    let seeds = [seed(retired, "10.0.0.1:9802", Some(3))];
    let reactivation = registration(retired, "10.0.0.1:9802", &metadata);
    let update = update(retired, &metadata);

    // The old statement waiting behind the reactivation read the
    // registration as deleted, in the snapshot it took before the wait: no
    // row, its `nodes` write kept, and `node_direct` as the reactivation left
    // it. An old update behind a new reactivation does the same.
    let old = update_behind_reactivation(
        &db.pool,
        &seeds,
        &reactivation,
        &update,
        Version::Old,
        Version::Old,
    )
    .await?;
    assert_eq!(old.outcome, Outcome::NoRow);
    let node = old.node(retired).expect("seeded");
    assert_eq!(node["state"], "cordoned");
    assert_eq!(node["last_heartbeat"], format!("{NOW} 1"));
    let direct = old.direct(retired).expect("seeded");
    assert_eq!(direct["name"], "registered");
    assert_eq!(direct["deleted_at"], serde_json::Value::Null);
    assert_eq!(direct["updated_at"], format!("{NOW} 0"));
    assert_eq!(
        update_behind_reactivation(
            &db.pool,
            &seeds,
            &reactivation,
            &update,
            Version::New,
            Version::Old
        )
        .await?,
        old
    );

    // The coordinator's update sees the committed reactivation and writes
    // both rows: what the old statements do when the reactivation commits
    // before the update starts. So it does behind either version's
    // reactivation.
    let in_order = update_after_reactivation(&db.pool, &seeds, &reactivation, &update).await?;
    let row = returned(&in_order);
    assert_eq!(row["name"], "updated");
    assert_eq!(row["deleted_at"], serde_json::Value::Null);
    for holder in [Version::Old, Version::New] {
        let observed = update_behind_reactivation(
            &db.pool,
            &seeds,
            &reactivation,
            &update,
            holder,
            Version::New,
        )
        .await?;
        assert_eq!(observed, in_order, "behind the {holder:?} reactivation");
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn update_behind_a_reactivation_onto_a_live_endpoint_fails_as_if_it_ran_after()
-> TestDbResult<()> {
    let db = registry_db().await?;
    let metadata = metadata();
    let retired = node_id(1);
    // A second live node holds the endpoint the update requests.
    let endpoint = "10.0.2.30:9802";
    let seeds = [
        seed(retired, "10.0.0.1:9802", Some(3)),
        seed(node_id(2), endpoint, None),
    ];
    let reactivation = registration(retired, "10.0.0.1:9802", &metadata);
    let update = update(retired, &metadata);
    assert_eq!(update.node_api_endpoint, Some(endpoint));

    // The old update waiting behind the reactivation read the registration as
    // deleted, so it never wrote the endpoint: no row, and its `nodes` write
    // kept, behind either version's reactivation.
    let old = update_behind_reactivation(
        &db.pool,
        &seeds,
        &reactivation,
        &update,
        Version::Old,
        Version::Old,
    )
    .await?;
    assert_eq!(old.outcome, Outcome::NoRow);
    assert_eq!(old.node(retired).expect("seeded")["state"], "cordoned");
    assert_eq!(
        update_behind_reactivation(
            &db.pool,
            &seeds,
            &reactivation,
            &update,
            Version::New,
            Version::Old
        )
        .await?,
        old
    );

    // The coordinator's update writes the live registration, fails on the
    // endpoint and rolls back its `nodes` write: what the old statements do
    // when the reactivation commits before the update starts.
    let in_order = update_after_reactivation(&db.pool, &seeds, &reactivation, &update).await?;
    assert_eq!(
        in_order.outcome,
        Outcome::Failed {
            kind: DbErrorKind::UniqueViolation,
            code: Some("23505".into()),
            constraint: Some("idx_node_direct_endpoint_active".into()),
        }
    );
    assert_eq!(
        in_order.node(retired).expect("seeded")["state"],
        "registered"
    );
    for holder in [Version::Old, Version::New] {
        let observed = update_behind_reactivation(
            &db.pool,
            &seeds,
            &reactivation,
            &update,
            holder,
            Version::New,
        )
        .await?;
        assert_eq!(observed, in_order, "behind the {holder:?} reactivation");
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn reactivation_behind_a_reregistration_of_its_candidate_writes_nothing() -> TestDbResult<()>
{
    let metadata = metadata();
    let retired = node_id(1);
    let seeds = [seed(retired, "10.0.0.1:9802", Some(3))];
    let reactivation = registration(retired, "10.0.0.1:9802", &metadata);
    let reregister = async |pool: &PgPool| {
        let mut holder = pool.begin().await?;
        sqlx::query(sql!("DELETE FROM nodes WHERE id = $1"))
            .bind(retired)
            .execute(&mut *holder)
            .await?;
        sqlx::query(sql!(
            "INSERT INTO nodes (id, address, role, managed_by, state, created_at)
             VALUES ($1, '10.0.9.9'::INET, 'worker', 'space', 'registered', $2)"
        ))
        .bind(retired)
        .bind(hour(30))
        .execute(&mut *holder)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO node_direct (node_id, name, node_api_endpoint, updated_at)
             VALUES ($1, 'reregistered', '10.0.9.9:9802', $2)"
        ))
        .bind(retired)
        .bind(hour(30))
        .execute(&mut *holder)
        .await?;
        TestDbResult::Ok(holder)
    };

    // A transaction deletes the retired node, registers its id again, and
    // holds both while the reactivation, having read the retired node as its
    // candidate, waits for its `nodes` row.
    let db = registry_db().await?;
    let pool = &db.pool;
    reseed(pool, &seeds).await?;
    let holder = reregister(pool).await?;
    let old_outcome = run_behind(
        pool,
        async { Ok(holder.commit().await?) },
        old_reactivate(pool, &reactivation, false),
    )
    .await?;
    let old = observe(pool, Outcome::old(old_outcome)).await?;

    reseed(pool, &seeds).await?;
    let holder = reregister(pool).await?;
    let new_outcome = run_behind(
        pool,
        async { Ok(holder.commit().await?) },
        reactivate_node(&Db::from(pool.clone()), &reactivation, false),
    )
    .await?;
    let new = observe(pool, Outcome::new(new_outcome)).await?;

    assert_eq!(old, new);
    // The matched row is gone; the new row with its id is not the one the
    // reactivation read, and keeps what its registration wrote.
    assert_eq!(old.outcome, Outcome::NoRow);
    let node = old.node(retired).expect("registered again");
    assert_eq!(node["address"], "10.0.9.9");
    assert_eq!(node["state"], "registered");
    let direct = old.direct(retired).expect("registered again");
    assert_eq!(direct["name"], "reregistered");
    assert_eq!(direct["deleted_at"], serde_json::Value::Null);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn reactivation_whose_registration_goes_keeps_its_nodes_write() -> TestDbResult<()> {
    let metadata = metadata();
    let retired = node_id(1);
    let seeds = [seed(retired, "10.0.0.1:9802", Some(3))];
    let reactivation = registration(retired, "10.0.0.1:9802", &metadata);
    let delete_registration = async |pool: &PgPool| {
        let mut holder = pool.begin().await?;
        sqlx::query(sql!("DELETE FROM node_direct WHERE node_id = $1"))
            .bind(retired)
            .execute(&mut *holder)
            .await?;
        TestDbResult::Ok(holder)
    };

    // A transaction deletes the registration and holds it while the
    // reactivation, having read it as its candidate, waits to write it.
    let db = registry_db().await?;
    let pool = &db.pool;
    reseed(pool, &seeds).await?;
    let holder = delete_registration(pool).await?;
    let old_outcome = run_behind(
        pool,
        async { Ok(holder.commit().await?) },
        old_reactivate(pool, &reactivation, false),
    )
    .await?;
    let old = observe(pool, Outcome::old(old_outcome)).await?;

    reseed(pool, &seeds).await?;
    let holder = delete_registration(pool).await?;
    let new_outcome = run_behind(
        pool,
        async { Ok(holder.commit().await?) },
        reactivate_node(&Db::from(pool.clone()), &reactivation, false),
    )
    .await?;
    let new = observe(pool, Outcome::new(new_outcome)).await?;

    assert_eq!(old, new);
    assert_eq!(old.outcome, Outcome::NoRow);
    assert_eq!(old.node(retired).expect("seeded")["role"], "builder");
    assert_eq!(old.direct(retired), None);
    Ok(())
}
