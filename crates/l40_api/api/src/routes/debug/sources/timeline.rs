use pplx_space_common_core::types::SandboxId;

use pplx_space_common_core::sql;
use sqlx::PgPool;

use crate::routes::debug::{response::TimelineEvent, sources::begin_read_only};

pub(in crate::routes::debug) async fn fetch(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<Vec<TimelineEvent>, sqlx::Error> {
    let mut tx = begin_read_only(pool).await?;
    let timeline = sqlx::query_as::<_, TimelineEvent>(sql!(
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
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(timeline)
}
