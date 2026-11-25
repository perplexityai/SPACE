//! The `node_status_snapshots` row: what each node last reported about
//! itself, one row per node.
//!
//! What lives here is the rule for reading the row's published
//! environments, so every reader asks the same question;
//! [`upsert_from_publish`], the internal API's write from a node-state
//! publication; [`create_failure`], the Direct substrate's write after a
//! create on a worker failed; [`status_summaries`], the admin node
//! inventory's read; and [`backup_inventory`], whether a node's pending
//! backup report is fresh enough for node drain to believe.

use std::fmt;
use std::str::FromStr;

use pplx_space_accounts::NodeId;
use pplx_space_execution_spec::ExecutionPolicyObservation;
use pplx_space_sql_locator::sql;
use sqlx::types::Json;
use time::OffsetDateTime;

pub mod backup_inventory;
pub mod create_failure;

#[cfg(test)]
mod tests;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

/// What a publication does to `local_backup_inventory_reported_at`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalBackupReportedAt {
    /// Set it to now.
    Stamp,
    /// Keep the stored value; a node's first publication stores none.
    Preserve,
    /// Clear it.
    Withdraw,
}

/// What a publication does to `usable_environment_refs`,
/// `known_environment_refs` and `environment_refs_received_at`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvironmentRefsWrite<'a> {
    /// Keep the stored arrays, which age out by their receipt time; a
    /// node's first publication stores none.
    Preserve,
    /// Record both arrays, received now.
    Record {
        usable: &'a [String],
        known: &'a [String],
    },
    /// Clear both arrays and their receipt time.
    Clear,
}

/// One node-state publication's write to the node's snapshot, the inputs of
/// [`upsert_from_publish`].
#[derive(Debug, Clone, Copy)]
pub struct NodeStatusPublication<'a> {
    pub node_id: NodeId,
    /// How long past now the node counts as fresh; `None` keeps the row's
    /// stored window (`stale_after - observed_at`).
    pub reported_window_secs: Option<f64>,
    pub scheduling_status: Option<&'a str>,
    pub scheduling_reason: Option<&'a str>,
    pub network_slots_ready: Option<i32>,
    pub network_pool_min_ready: Option<i32>,
    pub network_slots_degraded: Option<i32>,
    pub network_slots_quarantined: Option<i32>,
    pub network_slots_total: Option<i32>,
    pub network_slots_leased: Option<i32>,
    pub network_pool_max_slots: Option<i32>,
    /// node-ctl's status; `None` keeps the stored one, and also the stored
    /// health and services.
    pub node_ctl: Option<&'a serde_json::Value>,
    pub stamps_sandbox_inventory_pruned_at: bool,
    pub local_backup_inventory_reported_at: LocalBackupReportedAt,
    /// An execution inventory outcome; `None` preserves the previous
    /// evidence and its receipt times.
    pub execution_inventory_status: Option<&'a str>,
    pub execution_policy: Option<&'a ExecutionPolicyObservation>,
    /// The window of a node's first publication that reports no interval.
    pub default_window_secs: f64,
    pub environment_refs: EnvironmentRefsWrite<'a>,
}

/// Insert or update `publication.node_id`'s snapshot from a node-state
/// publication: the node is reachable now and fresh for its window, and each
/// column moves only as [`NodeStatusPublication`] says. Writes nothing when
/// the node isn't registered. Receipt times of execution evidence and the
/// environment arrays are the statement's clock (`clock_timestamp()`), the
/// rest the transaction's.
pub async fn upsert_from_publish(
    executor: impl sqlx::PgExecutor<'_>,
    publication: &NodeStatusPublication<'_>,
) -> Result<(), sqlx::Error> {
    let (usable_environment_refs, known_environment_refs) = match publication.environment_refs {
        EnvironmentRefsWrite::Record { usable, known } => (Some(usable), Some(known)),
        EnvironmentRefsWrite::Preserve | EnvironmentRefsWrite::Clear => (None, None),
    };
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots (
             node_id,
             observed_at,
             stale_after,
             reachable,
             error,
             role,
             node_id_reported,
             scheduling_status,
             scheduling_reason,
             network_slots_ready,
             network_pool_min_ready,
             network_slots_degraded,
             network_slots_quarantined,
             network_slots_total,
             network_slots_leased,
             network_pool_max_slots,
             health_healthy,
             services,
             node_ctl,
             sandbox_inventory_pruned_at,
             local_backup_inventory_reported_at,
             execution_policy,
             execution_policy_received_at,
             execution_inventory_status,
             execution_inventory_received_at,
             usable_environment_refs,
             known_environment_refs,
             environment_refs_received_at,
             updated_at
         )
         SELECT n.id,
                NOW(),
                NOW() + make_interval(
                    secs => COALESCE($2::DOUBLE PRECISION, $18::DOUBLE PRECISION)
                ),
                TRUE,
                NULL,
                n.role,
                NULL,
                $3::TEXT,
                $4::TEXT,
                $5::INT,
                $6::INT,
                $7::INT,
                $8::INT,
                $9::INT,
                $10::INT,
                $11::INT,
                CASE WHEN $12::JSONB IS NULL THEN NULL ELSE TRUE END,
                '[]'::JSONB,
                $12::JSONB,
                CASE WHEN $13::BOOLEAN THEN NOW() ELSE NULL END,
                CASE WHEN $14::BOOLEAN THEN NOW() ELSE NULL END,
                $17::JSONB,
                CASE WHEN $17::JSONB IS NOT NULL THEN clock_timestamp() ELSE NULL END,
                $16::TEXT,
                CASE WHEN $16::TEXT IS NOT NULL THEN clock_timestamp() ELSE NULL END,
                $19::TEXT[],
                $20::TEXT[],
                CASE WHEN $19::TEXT[] IS NOT NULL THEN clock_timestamp() ELSE NULL END,
                NOW()
         FROM nodes n
         WHERE n.id = $1
         ON CONFLICT (node_id) DO UPDATE
         SET observed_at = EXCLUDED.observed_at,
             -- No usable interval: keep the row's stored window.
             stale_after = CASE
                 WHEN $2::DOUBLE PRECISION IS NULL
                 THEN EXCLUDED.observed_at
                     + (node_status_snapshots.stale_after - node_status_snapshots.observed_at)
                 ELSE EXCLUDED.stale_after
             END,
             reachable = TRUE,
             error = NULL,
             role = EXCLUDED.role,
             scheduling_status = COALESCE(
                 EXCLUDED.scheduling_status,
                 node_status_snapshots.scheduling_status
             ),
             scheduling_reason = COALESCE(
                 EXCLUDED.scheduling_reason,
                 node_status_snapshots.scheduling_reason
             ),
             network_slots_ready = COALESCE(
                 EXCLUDED.network_slots_ready,
                 node_status_snapshots.network_slots_ready
             ),
             network_pool_min_ready = COALESCE(
                 EXCLUDED.network_pool_min_ready,
                 node_status_snapshots.network_pool_min_ready
             ),
             network_slots_degraded = COALESCE(
                 EXCLUDED.network_slots_degraded,
                 node_status_snapshots.network_slots_degraded
             ),
             network_slots_quarantined = COALESCE(
                 EXCLUDED.network_slots_quarantined,
                 node_status_snapshots.network_slots_quarantined
             ),
             network_slots_total = COALESCE(
                 EXCLUDED.network_slots_total,
                 node_status_snapshots.network_slots_total
             ),
             network_slots_leased = COALESCE(
                 EXCLUDED.network_slots_leased,
                 node_status_snapshots.network_slots_leased
             ),
             network_pool_max_slots = COALESCE(
                 EXCLUDED.network_pool_max_slots,
                 node_status_snapshots.network_pool_max_slots
             ),
             health_healthy = COALESCE(
                 EXCLUDED.health_healthy,
                 node_status_snapshots.health_healthy
             ),
             services = CASE
                 WHEN EXCLUDED.health_healthy IS NULL
                 THEN node_status_snapshots.services
                 ELSE EXCLUDED.services
             END,
             node_ctl = COALESCE(EXCLUDED.node_ctl, node_status_snapshots.node_ctl),
             -- Preserve, never overwrite: this runs on every publication, and
             -- the facets that do not prune send NULL here. A plain
             -- `= EXCLUDED.x` would blank the census timestamp on the template
             -- facet's tick, moments after the census facet set it.
             sandbox_inventory_pruned_at = COALESCE(
                 EXCLUDED.sandbox_inventory_pruned_at,
                 node_status_snapshots.sandbox_inventory_pruned_at
             ),
             -- Three-way, unlike the column above. A facet tick carries no
             -- pending-backup report and must leave the gate alone, same as
             -- the prune stamp. But a *census* that pruned the inventory and
             -- still could not mirror has told us the queue is stale, and
             -- preserving a recent timestamp there would leave a reader's
             -- freshness window certifying an empty queue the server knows
             -- it did not refresh. `$15` distinguishes the two.
             local_backup_inventory_reported_at = CASE
                 WHEN $15::BOOLEAN THEN NULL
                 ELSE COALESCE(
                     EXCLUDED.local_backup_inventory_reported_at,
                     node_status_snapshots.local_backup_inventory_reported_at
                 )
             END,
             execution_policy = CASE
                 WHEN $16::TEXT IS NOT NULL THEN EXCLUDED.execution_policy
                 ELSE node_status_snapshots.execution_policy
             END,
             execution_policy_received_at = CASE
                 WHEN $16::TEXT IS NOT NULL THEN EXCLUDED.execution_policy_received_at
                 ELSE node_status_snapshots.execution_policy_received_at
             END,
             execution_inventory_status = COALESCE(
                 EXCLUDED.execution_inventory_status,
                 node_status_snapshots.execution_inventory_status
             ),
             execution_inventory_received_at = CASE
                 WHEN $16::TEXT IS NOT NULL THEN clock_timestamp()
                 ELSE node_status_snapshots.execution_inventory_received_at
             END,
             -- Written together or not at all, as the row's CHECK requires:
             -- a complete inventory records or clears all three, and any
             -- other publication leaves them to age out by their receipt.
             usable_environment_refs = CASE
                 WHEN $21::BOOLEAN THEN EXCLUDED.usable_environment_refs
                 ELSE node_status_snapshots.usable_environment_refs
             END,
             known_environment_refs = CASE
                 WHEN $21::BOOLEAN THEN EXCLUDED.known_environment_refs
                 ELSE node_status_snapshots.known_environment_refs
             END,
             environment_refs_received_at = CASE
                 WHEN $21::BOOLEAN THEN EXCLUDED.environment_refs_received_at
                 ELSE node_status_snapshots.environment_refs_received_at
             END,
             updated_at = NOW()"
    ))
    .bind(publication.node_id)
    .bind(publication.reported_window_secs)
    .bind(publication.scheduling_status)
    .bind(publication.scheduling_reason)
    .bind(publication.network_slots_ready)
    .bind(publication.network_pool_min_ready)
    .bind(publication.network_slots_degraded)
    .bind(publication.network_slots_quarantined)
    .bind(publication.network_slots_total)
    .bind(publication.network_slots_leased)
    .bind(publication.network_pool_max_slots)
    .bind(publication.node_ctl.map(Json))
    .bind(publication.stamps_sandbox_inventory_pruned_at)
    .bind(matches!(
        publication.local_backup_inventory_reported_at,
        LocalBackupReportedAt::Stamp
    ))
    .bind(matches!(
        publication.local_backup_inventory_reported_at,
        LocalBackupReportedAt::Withdraw
    ))
    .bind(publication.execution_inventory_status)
    .bind(publication.execution_policy.map(Json))
    .bind(publication.default_window_secs)
    .bind(usable_environment_refs)
    .bind(known_environment_refs)
    .bind(!matches!(
        publication.environment_refs,
        EnvironmentRefsWrite::Preserve
    ))
    .execute(executor)
    .await?;
    Ok(())
}

/// A node's reachability and health from its snapshot. `services` and
/// `node_ctl` are JSON the caller decodes as `S` and `C`; a row that doesn't
/// decode fails the read.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct NodeStatusSummary<S, C> {
    pub node_id: NodeId,
    pub stale_after: OffsetDateTime,
    pub reachable: bool,
    pub error: Option<String>,
    pub health_healthy: Option<bool>,
    pub services: Json<S>,
    pub node_ctl: Option<Json<C>>,
}

/// The snapshots of those of `node_ids` that have one, in no particular
/// order. Takes no lock.
pub async fn status_summaries<S, C>(
    executor: impl sqlx::PgExecutor<'_>,
    node_ids: &[NodeId],
) -> Result<Vec<NodeStatusSummary<S, C>>, sqlx::Error>
where
    NodeStatusSummary<S, C>: for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> + Send + Unpin,
{
    sqlx::query_as::<_, NodeStatusSummary<S, C>>(sql!(
        "SELECT node_id,
                stale_after,
                reachable,
                error,
                health_healthy,
                services,
                node_ctl
         FROM node_status_snapshots
         WHERE node_id = ANY($1)"
    ))
    .bind(node_ids)
    .fetch_all(executor)
    .await
}

/// A boolean SQL predicate, TRUE iff the node status snapshot aliased as
/// `snapshot_alias` counts as evidence of the node's published environments
/// now (`usable_environment_refs` and `known_environment_refs`):
///
/// * its latest execution inventory report is `complete`;
/// * the snapshot is fresh: `stale_after` hasn't passed;
/// * the environment arrays were received within the node's freshness
///   window, `stale_after - observed_at`. The arrays have their own receipt
///   time because older writers refresh the inventory columns without them,
///   so arrays such a writer leaves behind age out.
///
/// A NULL in any of them (no inventory report, no arrays) is FALSE, never
/// NULL, so the predicate can be negated safely. It says nothing about
/// whether the node is reachable: a caller choosing a node to act on also
/// requires `reachable IS TRUE`.
///
/// Freshness is measured at the statement's own time (`clock_timestamp()`),
/// not the transaction's start (`now()`): a caller that checks evidence
/// after waiting on a lock inside a transaction mustn't count a snapshot that
/// went stale while it waited.
///
/// Every value is the row's own or the database clock's, so a caller builds
/// its statement once (a `LazyLock<String>`) and binds the rest.
pub fn environment_evidence(snapshot_alias: &str) -> String {
    format!(
        "COALESCE(\n\
             {complete}\n\
             AND {fresh}\n\
             AND {arrays_fresh},\n\
             FALSE\n\
         )",
        complete = inventory_complete(snapshot_alias),
        fresh = snapshot_fresh(snapshot_alias, STATEMENT_TIME),
        arrays_fresh = arrays_fresh(snapshot_alias, STATEMENT_TIME),
    )
}

/// A text SQL expression: why the snapshot aliased as `snapshot_alias`
/// doesn't show the node can run `environment`, a text SQL expression, as an
/// [`EnvironmentRefusal`] wire value. NULL when there's nothing to refuse:
/// the snapshot is evidence ([`environment_evidence`]) listing `environment`
/// among its usable environments, or `environment` is NULL, which is work
/// bound to no environment.
///
/// A missing snapshot (a `LEFT JOIN` that found no row) has no evidence.
///
/// Built from the same conditions as [`environment_evidence`], so a snapshot
/// that's evidence is refused only as `environment_not_usable`.
pub fn environment_refusal(snapshot_alias: &str, environment: &str) -> String {
    let no_evidence = EnvironmentRefusal::NoEvidence.as_str();
    let incomplete = EnvironmentRefusal::Incomplete.as_str();
    let stale = EnvironmentRefusal::Stale.as_str();
    let not_usable = EnvironmentRefusal::EnvironmentNotUsable.as_str();
    format!(
        "CASE\n\
             WHEN {environment} IS NULL THEN NULL\n\
             WHEN {snapshot_alias}.execution_inventory_status IS NULL THEN '{no_evidence}'\n\
             WHEN NOT {complete} THEN '{incomplete}'\n\
             WHEN {snapshot_alias}.environment_refs_received_at IS NULL THEN '{no_evidence}'\n\
             WHEN NOT ({fresh} AND {arrays_fresh}) THEN '{stale}'\n\
             WHEN NOT COALESCE({environment} = ANY({snapshot_alias}.usable_environment_refs), FALSE)\n\
                 THEN '{not_usable}'\n\
         END",
        complete = inventory_complete(snapshot_alias),
        fresh = snapshot_fresh(snapshot_alias, TRANSACTION_TIME),
        arrays_fresh = arrays_fresh(snapshot_alias, TRANSACTION_TIME),
    )
}

/// A boolean SQL predicate, TRUE iff the node status snapshot aliased as
/// `snapshot_alias` says the node's template-manager reads a template twin's
/// own metadata (`node_ctl.reads_twin_metadata`, from node-ctl's
/// publication). A snapshot without the key, from a node that predates it,
/// with it `null`, or with any value but JSON `true` is FALSE, never NULL, so
/// the predicate can be negated safely. Says nothing about freshness: combine
/// it with [`environment_evidence`].
pub fn reads_twin_metadata(snapshot_alias: &str) -> String {
    format!("COALESCE({snapshot_alias}.node_ctl -> 'reads_twin_metadata' = 'true'::jsonb, FALSE)")
}

fn inventory_complete(snapshot_alias: &str) -> String {
    format!("{snapshot_alias}.execution_inventory_status = 'complete'")
}

/// The clock [`environment_evidence`] measures freshness against.
const STATEMENT_TIME: &str = "clock_timestamp()";
/// The clock [`environment_refusal`] measures freshness against, as its
/// placement callers always have.
const TRANSACTION_TIME: &str = "now()";

fn snapshot_fresh(snapshot_alias: &str, clock: &str) -> String {
    format!("{snapshot_alias}.stale_after > {clock}")
}

fn arrays_fresh(snapshot_alias: &str, clock: &str) -> String {
    format!(
        "{snapshot_alias}.environment_refs_received_at\n\
             + ({snapshot_alias}.stale_after - {snapshot_alias}.observed_at) > {clock}"
    )
}

/// Why a node's snapshot doesn't show it can run an environment
/// ([`environment_refusal`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvironmentRefusal {
    /// No snapshot, no inventory report, or a complete inventory without
    /// environment arrays, from a build that publishes none.
    NoEvidence,
    /// The latest inventory report isn't `complete`.
    Incomplete,
    /// The snapshot is past `stale_after`, or its arrays are older than the
    /// node's freshness window.
    Stale,
    /// Evidence that doesn't list the environment as usable.
    EnvironmentNotUsable,
}

impl EnvironmentRefusal {
    pub const ALL: [Self; 4] = [
        Self::NoEvidence,
        Self::Incomplete,
        Self::Stale,
        Self::EnvironmentNotUsable,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoEvidence => "no_evidence",
            Self::Incomplete => "incomplete",
            Self::Stale => "stale",
            Self::EnvironmentNotUsable => "environment_not_usable",
        }
    }
}

impl fmt::Display for EnvironmentRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown environment refusal {0:?}")]
pub struct EnvironmentRefusalParseError(pub String);

impl FromStr for EnvironmentRefusal {
    type Err = EnvironmentRefusalParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|refusal| refusal.as_str() == value)
            .ok_or_else(|| EnvironmentRefusalParseError(value.to_string()))
    }
}
