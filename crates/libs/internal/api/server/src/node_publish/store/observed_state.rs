use pplx_space_common_core::{
    sql,
    types::{NodeId, SandboxId},
};
use sqlx::{PgConnection, types::Json};

#[derive(Debug, thiserror::Error)]
pub(crate) enum UpsertObservedStateError {
    #[error("write published sandbox observed state: {0}")]
    Upsert(#[source] sqlx::Error),
}

#[derive(Debug, sqlx::FromRow)]
pub(crate) struct AcceptedSandboxPublishRow {
    pub(crate) sandbox_id: SandboxId,
    pub(crate) assigned_to_node: bool,
    pub(crate) inserted: bool,
    pub(crate) updated: bool,
    pub(crate) first_seen: bool,
    pub(crate) published_status: Option<String>,
    pub(crate) state_changed_at: Option<time::OffsetDateTime>,
    pub(crate) status_changed: bool,
}

struct ObservedStateComparableColumn {
    stored: &'static str,
    incoming: &'static str,
    excluded: &'static str,
}

const OBSERVED_STATE_COMPARABLE_COLUMNS: &[ObservedStateComparableColumn] = &[
    ObservedStateComparableColumn {
        stored: "node_id",
        incoming: "$1",
        excluded: "{source}.node_id",
    },
    observed_state_column("status"),
    observed_state_column("runtime"),
    observed_state_column("spaced_version"),
    observed_state_column("runtime_version"),
    observed_state_column("guest_os"),
    observed_state_column("guest_arch"),
    observed_state_column("state_format_name"),
    observed_state_column("state_format_version"),
    observed_state_column("execution_metadata"),
    observed_state_column("pid"),
    observed_state_column("namespace"),
    observed_state_column("uptime"),
    observed_state_column("sandbox_owner"),
    observed_state_column("sandbox_user"),
    observed_state_column("image_hash"),
    ObservedStateComparableColumn {
        stored: "snapshots_enabled",
        incoming: "COALESCE({source}.snapshots_enabled, FALSE)",
        excluded: "{source}.snapshots_enabled",
    },
    observed_state_column("template"),
    observed_state_column("pool_template"),
    observed_state_column("failure_reason"),
    observed_state_column("observed_created_at"),
    observed_state_column("network_tx_bytes"),
    observed_state_column("network_rx_bytes"),
    ObservedStateComparableColumn {
        stored: "disk_usage_bytes",
        incoming: "COALESCE({source}.disk_usage_bytes, observed_current.disk_usage_bytes)",
        excluded: "COALESCE({source}.disk_usage_bytes, sandbox_observed_states.disk_usage_bytes)",
    },
    observed_state_column("auto_lifecycle_authority"),
    ObservedStateComparableColumn {
        stored: "missing_publish_count",
        incoming: "0",
        excluded: "0",
    },
];

const fn observed_state_column(column: &'static str) -> ObservedStateComparableColumn {
    ObservedStateComparableColumn {
        stored: column,
        incoming: "{source}.{column}",
        excluded: "{source}.{column}",
    }
}

fn observed_state_stored_tuple(source: &str) -> String {
    observed_state_tuple(
        OBSERVED_STATE_COMPARABLE_COLUMNS
            .iter()
            .map(|column| format!("{source}.{}", column.stored)),
    )
}

fn observed_state_incoming_tuple(source: &str) -> String {
    observed_state_tuple(
        OBSERVED_STATE_COMPARABLE_COLUMNS
            .iter()
            .map(|column| render_observed_state_expression(column.incoming, source, column.stored)),
    )
}

fn observed_state_excluded_tuple() -> String {
    observed_state_tuple(
        OBSERVED_STATE_COMPARABLE_COLUMNS.iter().map(|column| {
            render_observed_state_expression(column.excluded, "EXCLUDED", column.stored)
        }),
    )
}

fn observed_state_tuple(expressions: impl Iterator<Item = String>) -> String {
    format!(
        "({})",
        expressions.collect::<Vec<_>>().join(",\n                 ")
    )
}

fn render_observed_state_expression(template: &str, source: &str, column: &str) -> String {
    template
        .replace("{source}", source)
        .replace("{column}", column)
}

/// Accept observations for unknown sandboxes or the current publisher, using
/// Direct route ownership when present. Conflict handling locks observation
/// rows even for no-op updates, without explicit sandbox locks. Unchanged observations
/// remain accepted, and an empty result means no input passed the owner guard.
pub(crate) async fn upsert(
    conn: &mut PgConnection,
    node_id: NodeId,
    rows_json: Json<serde_json::Value>,
) -> Result<Vec<AcceptedSandboxPublishRow>, UpsertObservedStateError> {
    let observed_state_write_sql = format!(
        "WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 sandbox_id UUID,
                 status TEXT,
                 state_changed_at TIMESTAMPTZ,
                 template TEXT,
                 runtime TEXT,
                 spaced_version TEXT,
                 runtime_version TEXT,
                 guest_os TEXT,
                 guest_arch TEXT,
                 state_format_name TEXT,
                 state_format_version TEXT,
                 execution_metadata JSONB,
                 execution_components JSONB,
                 pid INTEGER,
                 namespace TEXT,
                 uptime TEXT,
                 sandbox_owner TEXT,
                 sandbox_user TEXT,
                 image_hash TEXT,
                 snapshots_enabled BOOLEAN,
                 pool_template TEXT,
                 failure_reason TEXT,
                 observed_created_at TEXT,
                 network_tx_bytes BIGINT,
                 network_rx_bytes BIGINT,
                 disk_usage_bytes BIGINT,
                 observed_network_policy_json JSONB,
                 observed_network_policy_generation BIGINT,
                 observed_network_policy_source TEXT,
                 observed_network_policy_authority TEXT,
                 auto_lifecycle_authority TEXT
             )
         ),
         publishable AS (
             SELECT input.*,
                    s.id IS NOT NULL AS assigned_to_node
             FROM input
             LEFT JOIN sandboxes s ON s.id = input.sandbox_id
             LEFT JOIN sandbox_direct_route_identities direct_route
               ON direct_route.sandbox_id = s.id
             WHERE s.id IS NULL
                OR COALESCE(direct_route.current_node_id, s.active_node_id) = $1
         ),
         observed_current AS MATERIALIZED (
             SELECT sos.*
             FROM sandbox_observed_states sos
             JOIN publishable ON publishable.sandbox_id = sos.sandbox_id
         ),
         existing AS (
             SELECT DISTINCT sandbox_id
             FROM observed_current
             WHERE node_id = $1
         ),
         changed_or_missing AS MATERIALIZED (
             SELECT publishable.*
             FROM publishable
             LEFT JOIN observed_current ON observed_current.sandbox_id = publishable.sandbox_id
             WHERE observed_current.sandbox_id IS NULL
                OR {prefilter_current} IS DISTINCT FROM {prefilter_incoming}
         ),
         upserted AS (
             INSERT INTO sandbox_observed_states (
                 sandbox_id, node_id, status, runtime, spaced_version, runtime_version, guest_os, guest_arch,
                 state_format_name, state_format_version, execution_metadata, pid, namespace, uptime,
                 sandbox_owner, sandbox_user, image_hash,
                 snapshots_enabled, template, pool_template, failure_reason,
                 observed_created_at, network_tx_bytes, network_rx_bytes, disk_usage_bytes,
                 auto_lifecycle_authority,
                 missing_publish_count, observed_at, updated_at
             )
             SELECT
                 changed_or_missing.sandbox_id,
                 $1,
                status,
                runtime,
                spaced_version,
                runtime_version,
                 guest_os,
                 guest_arch,
                 state_format_name,
                 state_format_version,
                 execution_metadata,
                 pid,
                 namespace,
                 uptime,
                 sandbox_owner,
                 sandbox_user,
                 image_hash,
                 COALESCE(snapshots_enabled, FALSE),
                 template,
                 pool_template,
                 failure_reason,
                 observed_created_at,
                network_tx_bytes,
                network_rx_bytes,
                disk_usage_bytes,
                auto_lifecycle_authority,
                0,
                NOW(),
                NOW()
             FROM changed_or_missing
             ON CONFLICT (sandbox_id) DO UPDATE
             SET node_id = EXCLUDED.node_id,
                 status = EXCLUDED.status,
                 runtime = EXCLUDED.runtime,
                 spaced_version = EXCLUDED.spaced_version,
                 runtime_version = EXCLUDED.runtime_version,
                 guest_os = EXCLUDED.guest_os,
                 guest_arch = EXCLUDED.guest_arch,
                 state_format_name = EXCLUDED.state_format_name,
                 state_format_version = EXCLUDED.state_format_version,
                 execution_metadata = EXCLUDED.execution_metadata,
                 pid = EXCLUDED.pid,
                 namespace = EXCLUDED.namespace,
                 uptime = EXCLUDED.uptime,
                 sandbox_owner = EXCLUDED.sandbox_owner,
                 sandbox_user = EXCLUDED.sandbox_user,
                 image_hash = EXCLUDED.image_hash,
                 snapshots_enabled = EXCLUDED.snapshots_enabled,
                 template = EXCLUDED.template,
                 pool_template = EXCLUDED.pool_template,
                 failure_reason = EXCLUDED.failure_reason,
                 observed_created_at = EXCLUDED.observed_created_at,
                 network_tx_bytes = EXCLUDED.network_tx_bytes,
                 network_rx_bytes = EXCLUDED.network_rx_bytes,
                 disk_usage_bytes = COALESCE(
                     EXCLUDED.disk_usage_bytes,
                     sandbox_observed_states.disk_usage_bytes
                 ),
                 auto_lifecycle_authority = EXCLUDED.auto_lifecycle_authority,
                 missing_publish_count = 0,
                 observed_at = EXCLUDED.observed_at,
                 updated_at = NOW()
            WHERE {conflict_current} IS DISTINCT FROM {conflict_excluded}
            RETURNING sandbox_id, (xmax = 0) AS inserted
        )
         SELECT publishable.sandbox_id,
                publishable.assigned_to_node,
                COALESCE(upserted.inserted, FALSE) AS inserted,
                COALESCE(NOT upserted.inserted, FALSE) AS updated,
                NOT EXISTS (
                    SELECT 1
                    FROM existing
                    WHERE existing.sandbox_id = publishable.sandbox_id
                ) AS first_seen,
                publishable.status AS published_status,
                publishable.state_changed_at,
                observed_current.sandbox_id IS NULL
                    OR observed_current.status IS DISTINCT FROM publishable.status
                    AS status_changed
         FROM publishable
         LEFT JOIN upserted ON upserted.sandbox_id = publishable.sandbox_id
         LEFT JOIN observed_current ON observed_current.sandbox_id = publishable.sandbox_id",
        prefilter_current = observed_state_stored_tuple("observed_current"),
        prefilter_incoming = observed_state_incoming_tuple("publishable"),
        conflict_current = observed_state_stored_tuple("sandbox_observed_states"),
        conflict_excluded = observed_state_excluded_tuple(),
    );
    sqlx::query_as::<_, AcceptedSandboxPublishRow>(sqlx::AssertSqlSafe(sql!(
        observed_state_write_sql.as_str(),
    )))
    .bind(node_id)
    .bind(rows_json)
    .fetch_all(&mut *conn)
    .await
    .map_err(UpsertObservedStateError::Upsert)
}
