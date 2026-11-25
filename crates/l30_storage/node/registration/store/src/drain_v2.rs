//! The v2 node drain's columns of `nodes`: `drain_engine`, and the
//! `drain_status` JSON a v2 drain keeps its generation, deadline, report and
//! fence in ([`StoredV2Status`], the one typed shape of its keys).
//! Every statement and SQL fragment over them is here, so the cluster
//! controller's v2 loop, the admin API, the drain gates and registration
//! read and write them one way.

use pplx_space_accounts::NodeId;
use pplx_space_node_drain::NodeDrainEngine;
use pplx_space_node_model::SubstrateKind;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use serde::{Deserialize, Serialize};

/// A v2 drain's `drain_status`, as the admin API starts it and the cluster
/// controller writes it, and as every reader reads it. Its keys are frozen:
/// a point in time ends in `_at` and is Unix seconds by the database clock.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredV2Status {
    /// The drain's id: `drain_started_at` in microseconds. Every `DrainNode`
    /// call of the drain names it, and the node takes generations only
    /// forward.
    pub generation: u64,
    /// When holding ends: `drain_started_at` plus `:drain`'s
    /// `hold_seconds`, moved earlier only. From it on, the controller asks
    /// the node to evacuate.
    pub deadline_at: i64,
    /// A best-effort set of the sandboxes the drain has seen running, which
    /// only grows: what the still-suspended gauge counts and the console
    /// marks.
    #[serde(default)]
    pub observed_running: Vec<String>,
    /// Every sandbox the controller has recorded as evacuated, appended
    /// only, so the console's counts survive each sandbox's release from
    /// the node's report.
    #[serde(default)]
    pub evacuated_total: Vec<String>,
    /// The fence: the EC2 instance the drain terminates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminate_instance: Option<String>,
    /// When Auto Scaling accepted the termination of the fenced instance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatched_at: Option<i64>,
    /// When EC2 first listed the drain's instance (the fence once written,
    /// else the recorded one) `terminated`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminated_observed_at: Option<i64>,
    /// The instance [`Self::terminated_observed_at`] is about: the stamp
    /// counts only while the drain asks about that instance. A stamp without
    /// one, written before the key existed, counts for whichever instance
    /// the drain asks about.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminated_observed_instance: Option<String>,
    /// The last report the controller stored under [`Self::generation`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report: Option<StoredV2Report>,
}

impl StoredV2Status {
    /// Read a `drain_status` column; `None` for NULL.
    pub fn from_column(
        value: Option<&serde_json::Value>,
    ) -> Result<Option<Self>, serde_json::Error> {
        value.map(Self::deserialize).transpose()
    }

    /// The drain fenced its instance for termination.
    pub const fn fenced(&self) -> bool {
        self.terminate_instance.is_some()
    }
}

/// The last `DrainReport` the controller stored for a drain's generation,
/// with what it recorded of it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredV2Report {
    /// When the controller stored it.
    pub reported_at: i64,
    /// The one stall stamp: the last time the set of sandboxes the node
    /// reports evacuated grew, or when the evacuation began
    /// ([`Self::evacuate_asked_at`]) if the set hasn't grown since, so this
    /// plus the stall bound is when the drain stalls.
    pub evacuated_grew_at: i64,
    /// The listing time of the drain generation's first evacuating tick
    /// whose report the loop stored; a failed or aborted evacuating call
    /// stamps nothing. Absent until then: the loop reads a report without
    /// it as not evacuating yet, whoever stored it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evacuate_asked_at: Option<i64>,
    #[serde(default)]
    pub evacuated: Vec<StoredV2Evacuated>,
    #[serde(default)]
    pub pending: Vec<StoredV2Pending>,
    #[serde(default)]
    pub blocked: Vec<StoredV2Blocked>,
    /// The evacuations the controller hasn't recorded yet.
    #[serde(default)]
    pub unrecorded: Vec<StoredV2Unrecorded>,
}

impl StoredV2Report {
    /// The node reported nothing pending or blocked, and the controller
    /// recorded every evacuation: the drain is finished.
    pub fn finished(&self) -> bool {
        self.pending.is_empty() && self.blocked.is_empty() && self.unrecorded.is_empty()
    }
}

/// A sandbox the node reports durable elsewhere.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredV2Evacuated {
    pub sandbox_id: String,
    pub artifact_kind: StoredV2ArtifactKind,
    pub artifact_id: String,
}

/// An evacuated sandbox's artifact: a suspended sandbox's full snapshot or a
/// stopped one's disk. A kind this build doesn't know reads as `unknown`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StoredV2ArtifactKind {
    Full,
    Disk,
    #[serde(other)]
    Unknown,
}

/// A sandbox the node reports it can't move yet.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredV2Pending {
    pub sandbox_id: String,
    pub wait: StoredV2Wait,
    /// A started VM at the node's planning read.
    #[serde(default)]
    pub running: bool,
    /// Human text: displayed, never parsed or asserted on.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    /// When the row entered its state, by the node's clock; display only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<i64>,
    /// A warm-pool member, not a user's sandbox. Absent reads false.
    #[serde(default)]
    pub pool_member: bool,
}

/// What a pending sandbox waits on, as the node's `DrainWait` says: kept
/// running until the drain evacuates (`hold`), being made durable by the
/// drain (`moving`), or waiting on work the drain doesn't drive
/// (`external`). A wait this build doesn't know reads as `unspecified`,
/// still pending.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StoredV2Wait {
    Hold,
    Moving,
    External,
    #[serde(other)]
    Unspecified,
}

/// A node-wide problem the node reports.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredV2Blocked {
    pub reason: String,
}

/// An evacuation the controller hasn't recorded yet, why, and since when.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredV2Unrecorded {
    pub sandbox_id: String,
    pub reason: String,
    pub since: i64,
}

/// A node's drain v2 columns, read under the caller's row lock.
#[derive(Debug, Clone, PartialEq)]
pub struct DrainV2Columns {
    pub engine: NodeDrainEngine,
    pub status: Option<StoredV2Status>,
    /// Whether `node_k8s` records the node's EC2 instance.
    pub recorded_instance: bool,
}

/// The node's v2 drain columns; `None` when there is no such node. A
/// `drain_status` that doesn't read as [`StoredV2Status`] is a decode error.
pub async fn drain_v2_columns(
    executor: impl Executor<'_>,
    node_id: NodeId,
) -> Result<Option<DrainV2Columns>, DbError> {
    type Row = (Option<String>, Option<serde_json::Value>, bool);
    let row: Option<Row> = sqlx::query_as(sql!(
        "SELECT n.drain_engine, n.drain_status, k.instance_id IS NOT NULL
           FROM nodes n
           LEFT JOIN node_k8s k ON k.node_id = n.id
          WHERE n.id = $1"
    ))
    .bind(node_id)
    .fetch_optional(executor.into_sqlx())
    .await?;
    row.map(|(engine, status, recorded_instance)| {
        Ok(DrainV2Columns {
            engine: NodeDrainEngine::from_column(engine.as_deref()),
            status: StoredV2Status::from_column(status.as_ref())
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?,
            recorded_instance,
        })
    })
    .transpose()
}

/// Start a v2 drain on a node `update_drain_phase` has just moved to
/// `draining` in this transaction: record the engine, clear the mode (a v2
/// drain takes none), and write the drain's `drain_status`: its generation,
/// the drain's start in microseconds, and its deadline, `hold_seconds` after
/// the drain's start in Unix seconds, by the database clock that stamped
/// `drain_started_at`. Until the deadline the node keeps its running
/// sandboxes running; a hold of 0 evacuates them at once. A later drain
/// starts later, so its generation is larger: the node takes generations
/// only forward.
///
/// `drain_engine` is bound from [`NodeDrainEngine`], the one closed set of
/// engines: the column has no CHECK, so no statement spells a value itself.
/// The caller must pass its open transaction.
pub async fn start_v2_drain<C>(
    tx: &mut C,
    node_id: NodeId,
    hold_seconds: i64,
) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    sqlx::query(sql!(
        "UPDATE nodes
            SET drain_engine = $3::TEXT,
                drain_mode = NULL,
                drain_status = jsonb_build_object(
                    'generation', (EXTRACT(EPOCH FROM drain_started_at) * 1000000)::BIGINT,
                    'deadline_at', FLOOR(EXTRACT(EPOCH FROM drain_started_at))::BIGINT + $2::BIGINT
                )
          WHERE id = $1 AND drain_phase = 'draining' AND drain_started_at IS NOT NULL"
    ))
    .bind(node_id)
    .bind(hold_seconds)
    .bind(NodeDrainEngine::V2.column())
    .execute(tx.into_sqlx())
    .await?;
    Ok(())
}

/// What a repeat `:drain` with a hold did to the deadline of the v2 drain in
/// flight.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HoldChange {
    /// The new deadline, `hold_seconds` after the drain's start, is earlier:
    /// stored.
    Shortened,
    /// The same deadline: nothing changed.
    Kept,
    /// Later than the stored deadline: nothing changed. A drain's deadline
    /// only moves earlier.
    Refused,
}

/// Move the deadline of the v2 drain in flight on `node_id`, whose row the
/// caller has locked, to `hold_seconds` after the drain's start, only if
/// that is earlier, by the clock [`start_v2_drain`] reads. A row with no
/// deadline is refused. The caller must pass its open transaction.
pub async fn shorten_v2_hold<C>(
    tx: &mut C,
    node_id: NodeId,
    hold_seconds: i64,
) -> Result<HoldChange, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    let (requested, stored): (Option<i64>, Option<i64>) = sqlx::query_as(sql!(
        "SELECT FLOOR(EXTRACT(EPOCH FROM drain_started_at))::BIGINT + $2::BIGINT,
                (drain_status->>'deadline_at')::BIGINT
           FROM nodes
          WHERE id = $1 AND drain_engine = 'v2'"
    ))
    .bind(node_id)
    .bind(hold_seconds)
    .fetch_optional((&mut *tx).into_sqlx())
    .await?
    .unwrap_or((None, None));
    let (Some(requested), Some(stored)) = (requested, stored) else {
        return Ok(HoldChange::Refused);
    };
    if requested > stored {
        return Ok(HoldChange::Refused);
    }
    if requested == stored {
        return Ok(HoldChange::Kept);
    }
    sqlx::query(sql!(
        "UPDATE nodes
            SET drain_status = drain_status || jsonb_build_object('deadline_at', $2::BIGINT)
          WHERE id = $1 AND drain_engine = 'v2'"
    ))
    .bind(node_id)
    .bind(requested)
    .execute(tx.into_sqlx())
    .await?;
    Ok(HoldChange::Shortened)
}

/// What a v2 drain's start reads under the cluster's evacuation lock: the
/// node's heartbeat and how many other v2 drains hold an evacuation slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct V2StartAdmission {
    /// The node's status heartbeat is reachable and not stale.
    pub heartbeat_fresh: bool,
    /// The v2 drains of other nodes that hold an evacuation slot.
    pub evacuating: i64,
}

/// SQL predicate over the `nodes` row aliased `n`: its v2 drain holds one of
/// the cluster's evacuation slots. A drain takes one when the admin route
/// starts it, and keeps it for its whole drain, its hold included: while it
/// is `draining` and, once undrained (a v2 engine with no phase), until its
/// cancel finishes on the node. `drained`, `terminating` and a deleted row
/// hold none. A drain that only holds running sandboxes keeps its slot
/// because its node evacuates them all at its deadline: freeing it would
/// let any number of such drains evacuate at once. This predicate is the one
/// statement of the rule.
pub const HOLDS_EVACUATION_SLOT: &str = "(n.drain_engine = 'v2'
     AND (n.drain_phase = 'draining' OR n.drain_phase IS NULL))";

/// `$1` = node id. Read under the evacuation lock: the node's heartbeat,
/// and how many other nodes' v2 drains hold an evacuation slot
/// ([`HOLDS_EVACUATION_SLOT`]).
static V2_START_ADMISSION: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    let fresh = pplx_space_sandbox_residency::node_heartbeat_fresh("$1");
    sql!(format!(
        "SELECT {fresh} AS heartbeat_fresh,
                (SELECT COUNT(*)
                   FROM nodes n
                  WHERE {HOLDS_EVACUATION_SLOT}
                    AND n.id <> $1) AS evacuating"
    ))
});

/// Serialize the start of v2 drains across the cluster and read what the
/// start of one on `node_id` needs: take the evacuation lock, a transaction
/// advisory lock every v2 start takes, held until the caller's transaction
/// ends, so two starts can't both see a free slot; then read the node's
/// heartbeat and count the slots other nodes hold ([`V2StartAdmission`]).
/// Only a v2 drain's start takes the lock; the caller writes the start in
/// the same transaction, so the lock covers the count and the write. The
/// caller must pass its open transaction.
pub async fn admit_v2_start<C>(tx: &mut C, node_id: NodeId) -> Result<V2StartAdmission, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    const EVACUATION_LOCK: &str = "node_drain_v2_evacuation_slots";
    sqlx::query(sql!(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"
    ))
    .bind(EVACUATION_LOCK)
    .execute((&mut *tx).into_sqlx())
    .await?;
    let (heartbeat_fresh, evacuating): (bool, i64) =
        sqlx::query_as(sqlx::AssertSqlSafe(V2_START_ADMISSION.as_str()))
            .bind(node_id)
            .fetch_one(tx.into_sqlx())
            .await?;
    Ok(V2StartAdmission {
        heartbeat_fresh,
        evacuating,
    })
}

/// SQL expression over a `nodes` row, unqualified: the generation of the
/// row's v2 drain, `drain_status.generation`.
const GENERATION: &str = "(drain_status->>'generation')::BIGINT";

/// SQL expression over the `nodes` row aliased `n`: the instance its v2
/// drain fenced, `drain_status.terminate_instance`.
pub const FENCE: &str = "n.drain_status->>'terminate_instance'";

/// One v2 node as [`list_v2_node_rows`] reads it.
#[derive(sqlx::FromRow)]
pub struct V2NodeRow {
    pub id: NodeId,
    pub drain_phase: Option<String>,
    /// `drain_status.generation`.
    pub generation: Option<i64>,
    /// `drain_status.deadline_at`.
    pub deadline_at: Option<i64>,
    /// The database clock had reached the deadline when the listing read the
    /// row, so the drain evacuates: a drain without a deadline holds
    /// nothing.
    pub evacuate: bool,
    /// `drain_status.report`, as stored; read it with [`Self::report`].
    pub report: Option<serde_json::Value>,
    pub k8s_node_name: Option<String>,
    pub k8s_node_uid: Option<String>,
    pub instance_id: Option<String>,
    pub heartbeat_fresh: bool,
    /// `drain_status.terminate_instance`.
    pub fence: Option<String>,
    /// `drain_status.dispatched_at`.
    pub dispatched_at: Option<i64>,
    /// `drain_last_error`, as the listing read it.
    pub last_error: Option<String>,
    /// `drain_status.terminated_observed_at`, while it counts for the
    /// instance the drain asks about (its fence, else its recorded
    /// instance): `None` once `drain_status.terminated_observed_instance`
    /// names another one.
    pub terminated_observed_at: Option<i64>,
    pub decrement_capacity: bool,
    /// The database clock when the listing read the row, in Unix seconds:
    /// the clock the drain's deadline, and the loop's stall rules, are kept
    /// in.
    pub db_now: i64,
}

impl V2NodeRow {
    /// The stored report, typed; `None` without one, and for one that
    /// doesn't read, which the loop then treats as never stored.
    pub fn report(&self) -> Option<StoredV2Report> {
        self.report
            .as_ref()
            .and_then(|report| StoredV2Report::deserialize(report).ok())
    }
}

/// Every v2 drain's node of the Kubernetes substrate, with its Kubernetes
/// identity, recorded instance and heartbeat. Whether the drain evacuates
/// is read by the database clock, the one its deadline was set by.
static LIST_V2_NODES: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    let fresh = pplx_space_sandbox_residency::node_heartbeat_fresh("n.id");
    sql!(format!(
        "SELECT n.id, n.drain_phase,
                (n.drain_status->>'generation')::BIGINT AS generation,
                (n.drain_status->>'deadline_at')::BIGINT AS deadline_at,
                COALESCE(
                    EXTRACT(EPOCH FROM NOW()) >= (n.drain_status->>'deadline_at')::NUMERIC,
                    TRUE
                ) AS evacuate,
                n.drain_status->'report' AS report,
                k.k8s_node_name, k.k8s_node_uid, k.instance_id,
                {fresh} AS heartbeat_fresh,
                {FENCE} AS fence,
                (n.drain_status->>'dispatched_at')::BIGINT AS dispatched_at,
                CASE
                    WHEN COALESCE(
                             n.drain_status->>'terminated_observed_instance',
                             COALESCE({FENCE}, k.instance_id)
                         ) IS NOT DISTINCT FROM COALESCE({FENCE}, k.instance_id)
                    THEN (n.drain_status->>'terminated_observed_at')::BIGINT
                END AS terminated_observed_at,
                n.terminate_decrement_capacity AS decrement_capacity,
                n.drain_last_error AS last_error,
                FLOOR(EXTRACT(EPOCH FROM NOW()))::BIGINT AS db_now
           FROM nodes n
           LEFT JOIN node_k8s k ON k.node_id = n.id
          WHERE n.drain_engine = 'v2' AND n.managed_by = $1
          ORDER BY n.id"
    ))
});

/// Every v2 drain's node of the Kubernetes substrate, as `LIST_V2_NODES`
/// reads it.
pub async fn list_v2_node_rows(executor: impl Executor<'_>) -> Result<Vec<V2NodeRow>, DbError> {
    Ok(sqlx::query_as(sqlx::AssertSqlSafe(LIST_V2_NODES.as_str()))
        .bind(SubstrateKind::Kubernetes)
        .fetch_all(executor.into_sqlx())
        .await?)
}

/// For each v2 drain's node of the Kubernetes substrate, how many of the
/// sandboxes its drain has seen running (`drain_status.observed_running`)
/// are `suspended`, their lifecycle unchanged for longer than `after_secs`
/// seconds by the database clock: a drain restores nothing, so each waits
/// for a call. A node with none is absent.
pub async fn evacuated_still_suspended(
    executor: impl Executor<'_>,
    after_secs: f64,
) -> Result<Vec<(NodeId, i64)>, DbError> {
    // An id in the set that isn't a UUID names no sandbox.
    Ok(sqlx::query_as(sql!(
        "SELECT n.id, COUNT(*)
           FROM nodes n
          CROSS JOIN LATERAL jsonb_array_elements_text(
                    COALESCE(n.drain_status->'observed_running', '[]'::JSONB)
                ) AS running(sandbox_id)
           JOIN sandboxes s
             ON s.id = CASE
                    WHEN running.sandbox_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                    THEN running.sandbox_id::UUID
                END
          WHERE n.drain_engine = 'v2'
            AND n.managed_by = $1
            AND s.lifecycle = 'suspended'
            AND s.lifecycle_changed_at < NOW() - make_interval(secs => $2)
          GROUP BY n.id"
    ))
    .bind(SubstrateKind::Kubernetes)
    .bind(after_secs)
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// The locked node's v2 state, as the terminate transaction and the
/// delete read it under the row lock.
#[derive(sqlx::FromRow)]
pub struct LockedV2Node {
    pub drain_engine: Option<String>,
    pub drain_phase: Option<String>,
    pub generation: Option<i64>,
    pub fence: Option<String>,
    pub dispatched: bool,
    /// The row records that EC2 listed the instance the drain asks about
    /// (its fence, else its recorded instance) `terminated`: a stamp about
    /// another instance doesn't count.
    pub terminated_observed: bool,
    pub k8s_node_name: Option<String>,
    pub k8s_node_uid: Option<String>,
    pub instance_id: Option<String>,
}

/// The locked node's v2 state, by its id `$1`.
static LOCKED_V2_NODE: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    sql!(format!(
        "SELECT n.drain_engine, n.drain_phase,
                (n.drain_status->>'generation')::BIGINT AS generation,
                {FENCE} AS fence,
                COALESCE(n.drain_status ? 'dispatched_at', FALSE) AS dispatched,
                COALESCE(
                    n.drain_status ? 'terminated_observed_at'
                        AND COALESCE(
                                n.drain_status->>'terminated_observed_instance',
                                COALESCE({FENCE}, k.instance_id)
                            ) IS NOT DISTINCT FROM COALESCE({FENCE}, k.instance_id),
                    FALSE
                ) AS terminated_observed,
                k.k8s_node_name, k.k8s_node_uid, k.instance_id
           FROM nodes n
           LEFT JOIN node_k8s k ON k.node_id = n.id
          WHERE n.id = $1"
    ))
});

/// Read the v2 state of `node_id`'s row, which the caller has locked
/// `FOR UPDATE`; `None` when the row is gone. The caller must pass its open
/// transaction.
pub async fn read_locked_v2_node<C>(
    tx: &mut C,
    node_id: NodeId,
) -> Result<Option<LockedV2Node>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_as(sqlx::AssertSqlSafe(LOCKED_V2_NODE.as_str()))
        .bind(node_id)
        .fetch_optional(tx.into_sqlx())
        .await?)
}

impl LockedV2Node {
    pub fn is_v2(&self, phase: &str, generation: i64) -> bool {
        self.drain_engine.as_deref() == NodeDrainEngine::V2.column()
            && self.drain_phase.as_deref() == Some(phase)
            && self.generation == Some(generation)
    }
}

/// Whether `node_id`'s row is still the v2 drain a listing read: the v2
/// engine in `phase` (`None`: a cancel pending) under `generation`, on the
/// machine registration recorded then, `k8s_node_uid` and `instance_id` as
/// listed (either may be absent). An undrain or another phase change moves
/// the phase; a registration that takes the row for another machine changes
/// the UID or the instance, or clears the drain. Takes no lock: the loop
/// reads it just before an effect on the node, a Kubernetes patch or a
/// `DrainNode` call, which no row lock may span.
pub async fn v2_node_as_listed(
    executor: impl Executor<'_>,
    node_id: NodeId,
    phase: Option<&str>,
    generation: i64,
    k8s_node_uid: Option<&str>,
    instance_id: Option<&str>,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1
               FROM nodes n
               LEFT JOIN node_k8s k ON k.node_id = n.id
              WHERE n.id = $1
                AND n.drain_engine = 'v2'
                AND (n.drain_status->>'generation')::BIGINT = $2
                AND k.k8s_node_uid IS NOT DISTINCT FROM $3::TEXT
                AND k.instance_id IS NOT DISTINCT FROM $4::TEXT
                AND n.drain_phase IS NOT DISTINCT FROM $5::TEXT
         )"
    ))
    .bind(node_id)
    .bind(generation)
    .bind(k8s_node_uid)
    .bind(instance_id)
    .bind(phase)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// Record what holds the locked node from its terminate transaction's fence
/// in `drain_last_error`, only when the row doesn't read it already. The
/// caller must pass its open transaction.
pub async fn record_terminate_held<C>(
    tx: &mut C,
    node_id: NodeId,
    held: &str,
) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    sqlx::query(sql!(
        "UPDATE nodes
            SET drain_last_error = $2
          WHERE id = $1
            AND drain_last_error IS DISTINCT FROM $2::TEXT"
    ))
    .bind(node_id)
    .bind(held)
    .execute(tx.into_sqlx())
    .await?;
    Ok(())
}

/// Write the locked node's fence, `drain_status.terminate_instance`, the
/// instance its v2 drain terminates, and clear its last error. The caller
/// must pass its open transaction.
pub async fn write_fence<C>(tx: &mut C, node_id: NodeId, instance_id: &str) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    sqlx::query(sql!(
        "UPDATE nodes
            SET drain_status = drain_status || jsonb_build_object('terminate_instance', $2::TEXT),
                drain_last_error = NULL
          WHERE id = $1"
    ))
    .bind(node_id)
    .bind(instance_id)
    .execute(tx.into_sqlx())
    .await?;
    Ok(())
}

/// Record that Auto Scaling accepted the termination of the fenced
/// `instance_id`, by the database clock in Unix seconds, so the loop never
/// dispatches it again. False for a row that is no longer that generation's
/// `terminating` node fenced on that instance with no dispatch recorded.
pub async fn record_v2_dispatched(
    executor: impl Executor<'_>,
    node_id: NodeId,
    generation: i64,
    instance_id: &str,
) -> Result<bool, DbError> {
    Ok(sqlx::query(sql!(
        "UPDATE nodes
            SET drain_status = drain_status
                    || jsonb_build_object('dispatched_at', EXTRACT(EPOCH FROM NOW())::BIGINT),
                drain_last_error = NULL
          WHERE id = $1
            AND drain_engine = 'v2'
            AND drain_phase = 'terminating'
            AND (drain_status->>'generation')::BIGINT = $2
            AND drain_status->>'terminate_instance' = $3
            AND NOT drain_status ? 'dispatched_at'"
    ))
    .bind(node_id)
    .bind(generation)
    .bind(instance_id)
    .execute(executor.into_sqlx())
    .await?
    .rows_affected()
        == 1)
}

/// `$1` = node id, `$2` = generation, `$3` = the instance EC2 was asked
/// about. Records, by the database clock in Unix seconds, that EC2 listed it
/// `terminated`, with the instance, on a row whose drain asks about that
/// instance: its fence once written, else the instance its registration
/// recorded. Keeps the first time of a stamp about that instance, or of one
/// that names none; replaces a stamp about another instance.
static RECORD_TERMINATED_OBSERVED: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    sql!(format!(
        "UPDATE nodes n
                SET drain_status = n.drain_status || CASE
                        WHEN n.drain_status ? 'terminated_observed_at'
                             AND COALESCE(n.drain_status->>'terminated_observed_instance', $3) = $3
                        THEN '{{}}'::JSONB
                        ELSE jsonb_build_object(
                            'terminated_observed_at', EXTRACT(EPOCH FROM NOW())::BIGINT,
                            'terminated_observed_instance', $3::TEXT
                        )
                    END
              WHERE n.id = $1
                AND n.drain_engine = 'v2'
                AND (n.drain_status->>'generation')::BIGINT = $2
                AND COALESCE(
                        {FENCE},
                        (SELECT k.instance_id FROM node_k8s k WHERE k.node_id = n.id)
                    ) = $3"
    ))
});

/// Record that EC2 listed `instance_id`, the instance the v2 drain under
/// `generation` asks about (its fence, else its recorded instance),
/// `terminated`, in `drain_status.terminated_observed_at` and
/// `drain_status.terminated_observed_instance`: EC2 stops listing a
/// terminated instance after a while, and the let-go and the delete of the
/// row need this record when it no longer does. Keeps the first time of a
/// record about that instance, or of one that names no instance; replaces
/// one about another instance. False for a row that is no longer that
/// generation's v2 drain asking about that instance.
pub async fn record_v2_terminated_observed(
    executor: impl Executor<'_>,
    node_id: NodeId,
    generation: i64,
    instance_id: &str,
) -> Result<bool, DbError> {
    Ok(
        sqlx::query(sqlx::AssertSqlSafe(RECORD_TERMINATED_OBSERVED.as_str()))
            .bind(node_id)
            .bind(generation)
            .bind(instance_id)
            .execute(executor.into_sqlx())
            .await?
            .rows_affected()
            == 1,
    )
}

/// `$1` = node id, `$2` = generation, `$3` = phase, `$4` = fence, `$5` =
/// trace. The let-go statement.
static LET_GO_V2_NODE: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    sql!(format!(
        "UPDATE nodes n
            SET drain_phase = NULL,
                drain_mode = NULL,
                drain_started_at = NULL,
                drain_engine = NULL,
                drain_status = NULL,
                drain_last_error = $5
          WHERE n.id = $1
            AND n.drain_engine = 'v2'
            AND (n.drain_status->>'generation')::BIGINT = $2
            AND n.drain_phase IS NOT DISTINCT FROM $3::TEXT
            AND {FENCE} IS NOT DISTINCT FROM $4::TEXT"
    ))
});

/// The let-go statement: clear the phase and the v2 columns of the v2 node
/// under `generation` in `phase` with fence `fence`, as the caller read
/// them, and record `trace`. Writes no sandbox row. Returns whether it
/// matched.
pub async fn let_go_v2_node(
    executor: impl Executor<'_>,
    node_id: NodeId,
    generation: i64,
    phase: Option<&str>,
    fence: Option<&str>,
    trace: &str,
) -> Result<bool, DbError> {
    Ok(sqlx::query(sqlx::AssertSqlSafe(LET_GO_V2_NODE.as_str()))
        .bind(node_id)
        .bind(generation)
        .bind(phase)
        .bind(fence)
        .bind(trace)
        .execute(executor.into_sqlx())
        .await?
        .rows_affected()
        == 1)
}

/// The `drain_status` id set `key` with the ids of the `TEXT[]` parameter
/// `param` added, sorted and without repeats.
fn id_set_union(key: &str, param: &str) -> String {
    format!(
        "(SELECT COALESCE(jsonb_agg(ids.sandbox_id ORDER BY ids.sandbox_id), '[]'::JSONB)
            FROM (
                  SELECT jsonb_array_elements_text(
                             COALESCE(drain_status->'{key}', '[]'::JSONB)
                         ) AS sandbox_id
                  UNION
                  SELECT unnest({param}::TEXT[])
                 ) AS ids)"
    )
}

/// `$1` = node id, `$2` = generation, `$3` = report, `$4` = the sandboxes it
/// lists running, `$5` = the evacuations recorded. Stores the report and
/// adds to the two id sets, each kept sorted and without repeats.
static RECORD_V2_REPORT_STATEMENT: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    let observed_running = id_set_union("observed_running", "$4");
    let evacuated_total = id_set_union("evacuated_total", "$5");
    sql!(format!(
        "UPDATE nodes
            SET drain_status = drain_status || jsonb_build_object(
                    'report', $3::JSONB,
                    'observed_running', {observed_running},
                    'evacuated_total', {evacuated_total}
                )
          WHERE id = $1
            AND drain_engine = 'v2'
            AND {GENERATION} = $2"
    ))
});

/// Store the node's `report` under `generation`, add `running`, the
/// sandboxes it lists running, to the drain's observed set
/// (`drain_status.observed_running`), and `evacuated`, the evacuations the
/// controller recorded, to `drain_status.evacuated_total`; both sets only
/// grow. Leaves `drain_last_error` to the loop's status write
/// ([`write_v2_status`]). Returns false when the row is no longer that
/// generation's v2 drain.
pub async fn record_v2_report(
    executor: impl Executor<'_>,
    node_id: NodeId,
    generation: i64,
    report: &StoredV2Report,
    running: &[String],
    evacuated: &[String],
) -> Result<bool, DbError> {
    let report = serde_json::to_value(report).map_err(|error| sqlx::Error::Encode(error.into()))?;
    Ok(
        sqlx::query(sqlx::AssertSqlSafe(RECORD_V2_REPORT_STATEMENT.as_str()))
            .bind(node_id)
            .bind(generation)
            .bind(report)
            .bind(running)
            .bind(evacuated)
            .execute(executor.into_sqlx())
            .await?
            .rows_affected()
            == 1,
    )
}

/// `$1` = node id, `$2` = generation, `$3` = the sandboxes evacuated,
/// `$4` = the drain phase.
static ADD_V2_EVACUATED_STATEMENT: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    let evacuated_total = id_set_union("evacuated_total", "$3");
    sql!(format!(
        "UPDATE nodes
            SET drain_status = drain_status
                    || jsonb_build_object('evacuated_total', {evacuated_total})
          WHERE id = $1
            AND drain_engine = 'v2'
            AND drain_phase = $4
            AND {GENERATION} = $2
            AND EXISTS (
                SELECT 1 FROM UNNEST($3::TEXT[]) AS added(id)
                 WHERE NOT COALESCE(drain_status->'evacuated_total', '[]'::JSONB) ? added.id
            )"
    ))
});

/// Add `evacuated`, sandboxes the drain under `generation` no longer needs
/// its node for, to `drain_status.evacuated_total`, which only grows, while
/// the row is still that generation's v2 drain in `phase`: the drained
/// tick's count of the rows the terminate would release, which needs no row
/// lock because this statement re-checks the phase, and the terminate's own
/// released rows. A sandbox the node evicted before a report named it
/// evacuated is counted this way. The row is written only when one of them
/// isn't in the set yet. Returns whether it wrote: false when every one was
/// counted already, or the row is no longer that generation's v2 drain in
/// `phase`.
pub async fn add_v2_evacuated(
    executor: impl Executor<'_>,
    node_id: NodeId,
    generation: i64,
    phase: &str,
    evacuated: &[String],
) -> Result<bool, DbError> {
    Ok(
        sqlx::query(sqlx::AssertSqlSafe(ADD_V2_EVACUATED_STATEMENT.as_str()))
            .bind(node_id)
            .bind(generation)
            .bind(evacuated)
            .bind(phase)
            .execute(executor.into_sqlx())
            .await?
            .rows_affected()
            == 1,
    )
}

/// SQL predicate over a `nodes` row, unqualified: its stored report lists
/// nothing pending, blocked or unrecorded ([`StoredV2Report::finished`]).
const REPORT_FINISHED: &str = "(drain_status ? 'report'
     AND jsonb_array_length(COALESCE(drain_status->'report'->'pending', '[]'::JSONB)) = 0
     AND jsonb_array_length(COALESCE(drain_status->'report'->'blocked', '[]'::JSONB)) = 0
     AND jsonb_array_length(COALESCE(drain_status->'report'->'unrecorded', '[]'::JSONB)) = 0)";

static ADVANCE_V2_DRAINED_STATEMENT: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    sql!(format!(
        "UPDATE nodes
            SET drain_phase = 'drained'
          WHERE id = $1
            AND drain_engine = 'v2'
            AND drain_phase = 'draining'
            AND {GENERATION} = $2
            AND {REPORT_FINISHED}"
    ))
});

/// `draining -> drained` for the v2 drain under `generation`, only with a
/// stored report that is finished: nothing pending, blocked or unrecorded.
pub async fn advance_v2_drained(
    executor: impl Executor<'_>,
    node_id: NodeId,
    generation: i64,
) -> Result<bool, DbError> {
    Ok(
        sqlx::query(sqlx::AssertSqlSafe(ADVANCE_V2_DRAINED_STATEMENT.as_str()))
            .bind(node_id)
            .bind(generation)
            .execute(executor.into_sqlx())
            .await?
            .rows_affected()
            == 1,
    )
}

static RETURN_V2_DRAINED_STATEMENT: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    sql!(format!(
        "UPDATE nodes
            SET drain_phase = 'drained',
                drain_last_error = $3
          WHERE id = $1
            AND drain_engine = 'v2'
            AND drain_phase = 'terminating'
            AND {GENERATION} = $2
            AND NOT drain_status ? 'terminate_instance'"
    ))
});

/// `terminating -> drained` for the v2 drain under `generation` whose
/// terminating tick's fresh check refused the fence, recording `refused`
/// as its last error: the operator retries `:terminate`. False for a row
/// that is no longer that generation's unfenced `terminating` node.
pub async fn return_v2_drained(
    executor: impl Executor<'_>,
    node_id: NodeId,
    generation: i64,
    refused: &str,
) -> Result<bool, DbError> {
    Ok(
        sqlx::query(sqlx::AssertSqlSafe(RETURN_V2_DRAINED_STATEMENT.as_str()))
            .bind(node_id)
            .bind(generation)
            .bind(refused)
            .execute(executor.into_sqlx())
            .await?
            .rows_affected()
            == 1,
    )
}

static FINISH_V2_CANCEL_STATEMENT: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    sql!(format!(
        "UPDATE nodes
            SET drain_engine = NULL,
                drain_status = NULL,
                drain_last_error = NULL
          WHERE id = $1
            AND drain_engine = 'v2'
            AND drain_phase IS NULL
            AND {GENERATION} = $2"
    ))
});

/// Clear an undrained v2 row's columns once its generation `generation` is
/// cancelled on the node, its status (`drain_last_error`) included: the
/// cancel's move writes the row's status with it, and a finished cancel
/// leaves nothing to report. False for a row that is no longer that
/// generation's cancel (a v2 engine with no phase).
pub async fn finish_v2_cancel(
    executor: impl Executor<'_>,
    node_id: NodeId,
    generation: i64,
) -> Result<bool, DbError> {
    Ok(
        sqlx::query(sqlx::AssertSqlSafe(FINISH_V2_CANCEL_STATEMENT.as_str()))
            .bind(node_id)
            .bind(generation)
            .execute(executor.into_sqlx())
            .await?
            .rows_affected()
            == 1,
    )
}

/// Make `drain_last_error` of the v2 drain under `generation` read
/// `status` (`None` clears it), the status the loop's tick decided on. Writes
/// only a row that is still that generation's v2 drain and doesn't read
/// `status` already. Returns whether it wrote.
pub async fn write_v2_status(
    executor: impl Executor<'_>,
    node_id: NodeId,
    generation: i64,
    status: Option<&str>,
) -> Result<bool, DbError> {
    Ok(sqlx::query(sql!(
        "UPDATE nodes
            SET drain_last_error = $3
          WHERE id = $1
            AND drain_engine = 'v2'
            AND (drain_status->>'generation')::BIGINT = $2
            AND drain_last_error IS DISTINCT FROM $3::TEXT"
    ))
    .bind(node_id)
    .bind(generation)
    .bind(status)
    .execute(executor.into_sqlx())
    .await?
    .rows_affected()
        == 1)
}

#[cfg(test)]
mod tests;
