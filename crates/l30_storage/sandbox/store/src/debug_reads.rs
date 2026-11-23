//! Reads for the admin debug view of one sandbox: its Kubernetes executions,
//! its node-reported snapshot inventory, and a merged timeline of its row,
//! executions, observed state, snapshots, snapshot inventory and port
//! exposures. The debug view's port exposures are
//! [`crate::port_exposures::list`].
//!
//! Plain reads, run on the executor the caller passes (the debug view reads
//! inside a read-only transaction).

use pplx_space_k8s_ids::PodUid;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::types::{Json, JsonValue};
use time::OffsetDateTime;
use uuid::Uuid;

/// One `sandbox_k8s_executions` row as the debug view shows it.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct ExecutionRow {
    pub id: Uuid,
    pub sandbox_id: SandboxId,
    pub generation: i64,
    pub pod_name: String,
    pub pod_namespace: String,
    /// `None` for a blank UID as well as a missing one.
    pub pod_uid: Option<PodUid>,
    /// `host(pod_ip)`.
    pub pod_ip: Option<String>,
    pub k8s_node_name: Option<String>,
    pub state: String,
    pub active: bool,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub terminated_at: Option<OffsetDateTime>,
}

/// Every execution of `sandbox_id`, newest first.
pub async fn executions(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Vec<ExecutionRow>, sqlx::Error> {
    sqlx::query_as::<_, ExecutionRow>(sql!(
        "SELECT id,
                sandbox_id,
                generation,
                pod_name,
                pod_namespace,
                NULLIF(BTRIM(pod_uid), '') AS pod_uid,
                host(pod_ip) AS pod_ip,
                k8s_node_name,
                state,
                active,
                created_at,
                updated_at,
                terminated_at
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
          ORDER BY created_at DESC, generation DESC"
    ))
    .bind(sandbox_id)
    .fetch_all(executor)
    .await
}

/// One `sandbox_snapshot_inventory` row: a snapshot a node reports holding.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct SnapshotInventoryRow {
    pub sandbox_id: SandboxId,
    pub snapshot_id: String,
    pub r#type: String,
    pub state: String,
    pub timestamp: Option<String>,
    pub source_node_id: Option<String>,
    pub label: Option<String>,
    pub retention_tier: Option<String>,
    pub vcpu: Option<i32>,
    pub memory_mib: Option<i32>,
    pub size_bytes: i64,
    pub template_id: Option<String>,
    pub content_hash: Option<String>,
    pub locations: Json<JsonValue>,
    pub observed_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// The snapshot inventory nodes report for `sandbox_id`, newest observation
/// first.
pub async fn snapshot_inventory(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Vec<SnapshotInventoryRow>, sqlx::Error> {
    sqlx::query_as::<_, SnapshotInventoryRow>(sql!(
        "SELECT sandbox_id,
                snapshot_id,
                type,
                state,
                timestamp,
                source_node_id,
                label,
                retention_tier,
                vcpu,
                memory_mib,
                size_bytes,
                template_id,
                content_hash,
                locations,
                observed_at,
                updated_at
           FROM sandbox_snapshot_inventory
          WHERE sandbox_id = $1
          ORDER BY observed_at DESC, snapshot_id DESC"
    ))
    .bind(sandbox_id)
    .fetch_all(executor)
    .await
}

/// One timeline entry: when, what kind of change, and a JSON summary of it.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct TimelineEvent {
    pub at: OffsetDateTime,
    pub kind: String,
    pub detail: Json<JsonValue>,
}

/// The newest 200 changes to `sandbox_id` across its row, observed state,
/// executions, snapshots, snapshot inventory and port exposures, newest
/// first. The `snapshots` arm reads a table `pplx-space-snapshot-store`
/// owns; it stays in this one statement because splitting it would change
/// which 200 entries the limit keeps.
pub async fn timeline(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Vec<TimelineEvent>, sqlx::Error> {
    sqlx::query_as::<_, TimelineEvent>(sql!(
        "SELECT at, kind, detail
           FROM (
                 SELECT s.created_at AS at,
                        'sandbox_created' AS kind,
                        jsonb_build_object(
                            'lifecycle', s.lifecycle,
                            'placement', s.placement,
                            'managed_by', s.managed_by,
                            'active_node_id', s.active_node_id,
                            'target_node_id', s.target_node_id
                        ) AS detail
                   FROM sandboxes s
                  WHERE s.id = $1
                 UNION ALL
                 SELECT s.updated_at AS at,
                        'sandbox_updated' AS kind,
                        jsonb_build_object(
                            'lifecycle', s.lifecycle,
                            'placement', s.placement,
                            'last_used_at', s.last_used_at,
                            'next_auto_event_at', s.next_auto_event_at
                        ) AS detail
                   FROM sandboxes s
                  WHERE s.id = $1
                    AND s.updated_at <> s.created_at
                 UNION ALL
                 SELECT sos.observed_at AS at,
                        'observed_state' AS kind,
                        jsonb_build_object(
                            'node_id', sos.node_id,
                            'status', sos.status,
                            'disk_usage_bytes', sos.disk_usage_bytes,
                            'failure_reason', sos.failure_reason,
                            'missing_publish_count', sos.missing_publish_count
                        ) AS detail
                   FROM sandbox_observed_states sos
                  WHERE sos.sandbox_id = $1
                 UNION ALL
                 SELECT ske.created_at AS at,
                        'pod_created' AS kind,
                        jsonb_build_object(
                            'id', ske.id,
                            'generation', ske.generation,
                            'pod_name', ske.pod_name,
                            'pod_namespace', ske.pod_namespace,
                            'k8s_node_name', ske.k8s_node_name,
                            'state', ske.state,
                            'active', ske.active
                        ) AS detail
                   FROM sandbox_k8s_executions ske
                  WHERE ske.sandbox_id = $1
                 UNION ALL
                 SELECT ske.updated_at AS at,
                        'pod_updated' AS kind,
                        jsonb_build_object(
                            'id', ske.id,
                            'generation', ske.generation,
                            'state', ske.state,
                            'active', ske.active
                        ) AS detail
                   FROM sandbox_k8s_executions ske
                  WHERE ske.sandbox_id = $1
                    AND ske.updated_at <> ske.created_at
                 UNION ALL
                 SELECT ske.terminated_at AS at,
                        'pod_terminated' AS kind,
                        jsonb_build_object(
                            'id', ske.id,
                            'generation', ske.generation,
                            'pod_name', ske.pod_name,
                            'state', ske.state
                        ) AS detail
                   FROM sandbox_k8s_executions ske
                  WHERE ske.sandbox_id = $1
                    AND ske.terminated_at IS NOT NULL
                 UNION ALL
                 SELECT snap.timestamp AS at,
                        ('snapshot_' || snap.state) AS kind,
                        jsonb_build_object(
                            'id', snap.id,
                            'snapshot_type', snap.type,
                            'state', snap.state,
                            'failure_reason', snap.failure_reason,
                            'retention_tier', snap.retention_tier,
                            'size_bytes', snap.size_bytes
                        ) AS detail
                   FROM snapshots snap
                  WHERE snap.sandbox_id = $1
                 UNION ALL
                 SELECT ssi.observed_at AS at,
                        ('snapshot_inventory_' || ssi.state) AS kind,
                        jsonb_build_object(
                            'snapshot_id', ssi.snapshot_id,
                            'snapshot_type', ssi.type,
                            'state', ssi.state,
                            'locations', ssi.locations,
                            'size_bytes', ssi.size_bytes
                        ) AS detail
                   FROM sandbox_snapshot_inventory ssi
                  WHERE ssi.sandbox_id = $1
                 UNION ALL
                 SELECT spe.updated_at AS at,
                        ('port_exposure_' || spe.apply_state) AS kind,
                        jsonb_build_object(
                            'protocol', spe.protocol,
                            'guest_port', spe.guest_port,
                            'visibility', spe.visibility,
                            'desired_state', spe.desired_state,
                            'apply_state', spe.apply_state,
                            'last_error', spe.last_error,
                            'last_bridge_status', spe.last_bridge_status
                        ) AS detail
                   FROM sandbox_port_exposures spe
                  WHERE spe.sandbox_id = $1
                ) timeline
          ORDER BY at DESC
          LIMIT 200"
    ))
    .bind(sandbox_id)
    .fetch_all(executor)
    .await
}
