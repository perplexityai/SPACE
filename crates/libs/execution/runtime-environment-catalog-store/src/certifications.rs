//! The catalog writer: each node's certification of the environments its build
//! knows, and the catalog rows and edges aggregated from them.
//!
//! The catalog keeps one edge set per environment. Node builds can disagree,
//! so an edge counts only while every node whose build knows both of its ends
//! certifies it. A build that doesn't know the target can't certify the edge,
//! so it abstains rather than vetoes, and a fleet with several architectures or
//! builds keeps every edge the builds that can judge it agree on. Each node's
//! rows are kept in `node_environment_certifications`, so the aggregate is
//! recomputed from them when a node upgrades or withdraws, and an edge a newer
//! build certifies is added once every build that can judge it does.
//!
//! The aggregate is recomputed only when a node's certifications change, which
//! is a build change, so a steady fleet writes nothing here. A node's rows stay
//! until it publishes a complete inventory without an observation, or until the
//! node is deleted (`ON DELETE CASCADE`, with no recomputation). So a node
//! that stops publishing keeps voting: its vetoes hold, and an edge only it
//! certifies stays until a node that knows both ends says otherwise. An
//! environment no node certifies any more keeps the edges last aggregated for
//! it, so its sandboxes keep their upgrade paths.

use std::collections::{BTreeMap, BTreeSet};

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{EnvironmentReference, NodeId, VersionSortKey};
use sqlx::types::Json;

/// Separates this lock from every other advisory lock on the database.
const CATALOG_AGGREGATE_LOCK_SEED: i64 = 412_667_309;
/// One lock for the whole aggregate: a node's change can move the edges of any
/// environment it knows, and writers that each lock their own rows would read
/// each other's certifications before they commit.
const CATALOG_AGGREGATE_LOCK_KEY: &str = "runtime_environments edges";

/// The catalog columns of an environment, taken from its definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogIdentity {
    pub runtime: String,
    pub runtime_version: String,
    pub kernel_version: String,
    pub state_format_name: String,
    pub state_format_version: String,
}

/// One environment as a node's build certifies it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CertifiedEnvironment {
    pub reference: EnvironmentReference,
    /// `None` when the definition has no kernel or no state format, which the
    /// catalog's columns require. The environment then gets no catalog row,
    /// and so is in no recipe, but the node's certification still counts: it
    /// knows the environment, so it votes on edges to it.
    pub identity: Option<CatalogIdentity>,
    pub version_sort_key: Option<VersionSortKey>,
    pub upgrade_edges: BTreeSet<EnvironmentReference>,
    pub memory_edges: BTreeSet<EnvironmentReference>,
}

/// A certification as stored: the sort key and the edge texts. Only this
/// function writes the rows, and Postgres keeps array order, so a stored row
/// equals a report's form exactly when nothing changed.
type Stored = (Option<Vec<i32>>, Vec<String>, Vec<String>);

fn stored_form(environment: &CertifiedEnvironment) -> Stored {
    let strings = |edges: &BTreeSet<EnvironmentReference>| {
        edges.iter().map(ToString::to_string).collect::<Vec<_>>()
    };
    (
        sort_key_ints(environment.version_sort_key.as_ref()),
        strings(&environment.upgrade_edges),
        strings(&environment.memory_edges),
    )
}

fn sort_key_ints(key: Option<&VersionSortKey>) -> Option<Vec<i32>> {
    key.map(|key| {
        key.components()
            .iter()
            .map(|&component| {
                i32::try_from(component).expect("VersionSortKey keeps every component within INT")
            })
            .collect()
    })
}

/// Replace `node_id`'s certifications with `certified`, its build's whole known
/// set (empty to withdraw them), and bring the catalog in line: add a row for
/// each new environment that has an identity, take the node's sort key, and
/// recompute the edges of every environment whose votes moved. Returns whether
/// anything changed; a report identical to the stored one writes nothing and
/// takes no lock.
///
/// Run it in the publication's transaction, after the node's row lock. A change
/// takes a transaction-scoped advisory lock first, so recomputations are
/// serialized and each reads every certification committed before it.
pub async fn record_node_certifications(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    certified: &[CertifiedEnvironment],
) -> Result<bool, sqlx::Error> {
    let desired: BTreeMap<String, Stored> = certified
        .iter()
        .map(|environment| (environment.reference.to_string(), stored_form(environment)))
        .collect();
    let stored: BTreeMap<String, Stored> =
        sqlx::query_as::<_, (String, Option<Vec<i32>>, Vec<String>, Vec<String>)>(sql!(
            "SELECT environment_ref, version_sort_key, upgrade_edges, memory_edges
             FROM node_environment_certifications
             WHERE node_id = $1"
        ))
        .bind(node_id)
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|(reference, key, upgrade, memory)| (reference, (key, upgrade, memory)))
        .collect();
    if stored == desired {
        return Ok(false);
    }

    sqlx::query(sql!(
        "SELECT pg_advisory_xact_lock(hashtextextended($1::text, $2))"
    ))
    .bind(CATALOG_AGGREGATE_LOCK_KEY)
    .bind(CATALOG_AGGREGATE_LOCK_SEED)
    .execute(&mut *conn)
    .await?;

    let catalog_rows: Vec<serde_json::Value> = certified
        .iter()
        .filter_map(|environment| {
            let identity = environment.identity.as_ref()?;
            Some(serde_json::json!({
                "environment_ref": environment.reference.to_string(),
                "runtime": identity.runtime,
                "runtime_version": identity.runtime_version,
                "kernel_version": identity.kernel_version,
                "state_format_name": identity.state_format_name,
                "state_format_version": identity.state_format_version,
                "version_sort_key": sort_key_ints(environment.version_sort_key.as_ref()),
            }))
        })
        .collect();
    // Identity columns are a function of the reference, so an existing row
    // keeps them. The sort key is the newest build's.
    sqlx::query(sql!(
        "INSERT INTO runtime_environments (
             environment_ref, runtime, runtime_version, version_sort_key,
             kernel_version, state_format_name, state_format_version
         )
         SELECT environment_ref, runtime, runtime_version, version_sort_key,
                kernel_version, state_format_name, state_format_version
         FROM jsonb_to_recordset($1::jsonb) AS input(
             environment_ref TEXT, runtime TEXT, runtime_version TEXT,
             version_sort_key INT[], kernel_version TEXT,
             state_format_name TEXT, state_format_version TEXT
         )
         ORDER BY environment_ref
         ON CONFLICT (environment_ref) DO UPDATE
         SET version_sort_key = EXCLUDED.version_sort_key
         WHERE runtime_environments.version_sort_key
               IS DISTINCT FROM EXCLUDED.version_sort_key"
    ))
    .bind(Json(serde_json::Value::Array(catalog_rows)))
    .execute(&mut *conn)
    .await?;

    let desired_refs: Vec<String> = desired.keys().cloned().collect();
    sqlx::query(sql!(
        "DELETE FROM node_environment_certifications
         WHERE node_id = $1 AND NOT (environment_ref = ANY($2))"
    ))
    .bind(node_id)
    .bind(&desired_refs)
    .execute(&mut *conn)
    .await?;
    let certification_rows: Vec<serde_json::Value> = desired
        .iter()
        .map(|(reference, (key, upgrade, memory))| {
            serde_json::json!({
                "environment_ref": reference,
                "version_sort_key": key,
                "upgrade_edges": upgrade,
                "memory_edges": memory,
            })
        })
        .collect();
    sqlx::query(sql!(
        "INSERT INTO node_environment_certifications (
             node_id, environment_ref, version_sort_key, upgrade_edges, memory_edges
         )
         SELECT $1, environment_ref, version_sort_key, upgrade_edges, memory_edges
         FROM jsonb_to_recordset($2::jsonb) AS input(
             environment_ref TEXT, version_sort_key INT[],
             upgrade_edges TEXT[], memory_edges TEXT[]
         )
         ON CONFLICT (node_id, environment_ref) DO UPDATE
         SET version_sort_key = EXCLUDED.version_sort_key,
             upgrade_edges = EXCLUDED.upgrade_edges,
             memory_edges = EXCLUDED.memory_edges
         WHERE (node_environment_certifications.version_sort_key,
                node_environment_certifications.upgrade_edges,
                node_environment_certifications.memory_edges)
               IS DISTINCT FROM
               (EXCLUDED.version_sort_key, EXCLUDED.upgrade_edges, EXCLUDED.memory_edges)"
    ))
    .bind(node_id)
    .bind(Json(serde_json::Value::Array(certification_rows)))
    .execute(&mut *conn)
    .await?;

    // Every environment this node certified before or certifies now may have
    // gained or lost a vote. An edge's voters are the nodes that know both of
    // its ends; it's kept when every one of them certifies it. Only an
    // environment some node still certifies is recomputed.
    let affected: Vec<String> = stored.into_keys().chain(desired.into_keys()).collect();
    sqlx::query(sql!(
        "WITH source AS (
             SELECT DISTINCT environment_ref
             FROM node_environment_certifications
             WHERE environment_ref = ANY($1)
         ),
         -- Only an edge some node certifies can be kept.
         candidate AS (
             SELECT DISTINCT certifying.environment_ref AS source, edge.target
             FROM node_environment_certifications certifying
             CROSS JOIN LATERAL unnest(certifying.upgrade_edges) AS edge(target)
             WHERE certifying.environment_ref = ANY($1)
         ),
         -- Kept unless a voter, a node that knows both ends, leaves it out.
         verdict AS (
             SELECT candidate.source, candidate.target,
                    NOT EXISTS (
                        SELECT 1
                        FROM node_environment_certifications voter
                        JOIN node_environment_certifications knows
                          ON knows.node_id = voter.node_id
                         AND knows.environment_ref = candidate.target
                        WHERE voter.environment_ref = candidate.source
                          AND NOT (voter.upgrade_edges @> ARRAY[candidate.target])
                    ) AS upgrade,
                    NOT EXISTS (
                        SELECT 1
                        FROM node_environment_certifications voter
                        JOIN node_environment_certifications knows
                          ON knows.node_id = voter.node_id
                         AND knows.environment_ref = candidate.target
                        WHERE voter.environment_ref = candidate.source
                          AND NOT (voter.memory_edges @> ARRAY[candidate.target])
                    ) AS memory
             FROM candidate
         ),
         aggregate AS (
             SELECT source.environment_ref,
                    COALESCE(
                        array_agg(verdict.target ORDER BY verdict.target COLLATE \"C\")
                            FILTER (WHERE verdict.upgrade),
                        '{}'
                    ) AS upgrade_edges,
                    COALESCE(
                        array_agg(verdict.target ORDER BY verdict.target COLLATE \"C\")
                            FILTER (WHERE verdict.upgrade AND verdict.memory),
                        '{}'
                    ) AS memory_edges
             FROM source
             LEFT JOIN verdict ON verdict.source = source.environment_ref
             GROUP BY source.environment_ref
         )
         UPDATE runtime_environments re
         SET upgrade_edges = aggregate.upgrade_edges,
             memory_edges = aggregate.memory_edges
         FROM aggregate
         WHERE re.environment_ref = aggregate.environment_ref
           AND (re.upgrade_edges, re.memory_edges)
               IS DISTINCT FROM (aggregate.upgrade_edges, aggregate.memory_edges)"
    ))
    .bind(&affected)
    .execute(&mut *conn)
    .await?;
    Ok(true)
}
