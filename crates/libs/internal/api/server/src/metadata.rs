use pplx_space_common_core::types::{NodeId, OrgId, SandboxId};

use std::time::Instant;

use pplx_space_common_core::types::TemplateVisibility as CoreTemplateVisibility;
use pplx_space_metrics::template::{
    VisibilityLookupCacheOutcome, VisibilityLookupDegradation, visibility_lookup_cache,
    visibility_lookup_degraded,
};

use crate::template_visibility_cache::TemplateVisibilityCache;

use super::status::*;
use super::*;
use pplx_space_common_core::sql;

// ── Template metadata: row types and helpers ──────────────────────────────

#[derive(Debug, Clone, FromRow)]
pub(super) struct TemplateRow {
    pub(super) name: String,
    pub(super) description: Option<String>,
    pub(super) created_at: OffsetDateTime,
    pub(super) network_policy_json: Json<NetworkPolicyV1>,
    pub(super) network_policy_generation: i64,
}

#[derive(Debug, Clone, FromRow)]
pub(super) struct TemplatePolicyRow {
    pub(super) template: String,
    pub(super) network_policy_json: Json<NetworkPolicyV1>,
    pub(super) network_policy_generation: i64,
}

#[derive(Debug, Clone)]
pub(super) struct SandboxPolicyRow {
    pub(super) sandbox_id: SandboxId,
    pub(super) template_id: Option<String>,
    pub(super) network_policy_json: Option<Json<pplx_space_net_common::NetworkPolicy>>,
    pub(super) network_policy_generation: i64,
    pub(super) template_network_policy_json: Option<Json<NetworkPolicyV1>>,
}

/// The store returns the two policy documents untyped; decoding them here
/// keeps the policy schema with its owner. A stored document that no longer
/// parses is an internal error, as a failed column decode was before.
impl TryFrom<pplx_space_sandbox_store::SandboxNetworkPolicyInputs> for SandboxPolicyRow {
    type Error = Status;

    fn try_from(row: pplx_space_sandbox_store::SandboxNetworkPolicyInputs) -> Result<Self, Status> {
        let invalid = |column: &str, error: serde_json::Error| {
            Status::internal(format!("stored {column} is invalid: {error}"))
        };
        Ok(Self {
            sandbox_id: row.sandbox_id,
            template_id: row.template_id,
            network_policy_json: row
                .network_policy_json
                .map(serde_json::from_value)
                .transpose()
                .map_err(|error| invalid("sandbox network policy override", error))?
                .map(Json),
            network_policy_generation: row.network_policy_generation,
            template_network_policy_json: row
                .template_network_policy_json
                .map(serde_json::from_value)
                .transpose()
                .map_err(|error| invalid("template network policy", error))?
                .map(Json),
        })
    }
}

/// The generation the node fences a sandbox's effective policy on.
///
/// It is always the sandbox's own `network_policy_generation`, whether the
/// effective policy is an override or inherited from the template. The node's
/// override-update guard compares this against the generation it last installed
/// for the sandbox; the sandbox's generation is the single monotonic lineage
/// for that comparison. Substituting the template's generation on the inherited
/// path breaks a clear: clearing a create-time override bumps the *sandbox*
/// generation (e.g. 1 -> 2) but the template generation is an unrelated counter
/// (often lower), so an inherit apply carrying the template generation collides
/// with the installed override at the same-or-lower generation and the node
/// rejects it as a conflict forever. The template generation drives the
/// separate `TemplateNetworkPolicyState` lineage, not a sandbox's apply.
pub(super) fn sandbox_effective_policy_generation(row: &SandboxPolicyRow) -> i64 {
    row.network_policy_generation
}

#[derive(Debug, Clone, FromRow)]
pub(super) struct NodeTemplateBaseSnapshotPolicyRow {
    pub(super) template: String,
    pub(super) ensure_base_snapshot: bool,
    pub(super) snapshot_vcpu: Option<i32>,
    pub(super) snapshot_memory_mib: Option<i32>,
}

#[derive(Debug, Clone, FromRow)]
pub(super) struct TemplateDesiredStateRow {
    pub(super) template: String,
    pub(super) desired_state: String,
    pub(super) default_auto_lifecycle_json: Option<Json<AutoLifecyclePolicy>>,
    pub(super) pool_target: Option<i32>,
    pub(super) pool_boot_mode: Option<String>,
    pub(super) pool_vcpu: Option<i32>,
    pub(super) pool_memory_mib: Option<i32>,
}

/// One row per requested id that exists. The desired-state payload
/// columns are conditionally projected: populated only when the row is
/// assigned to the querying node, NULL otherwise — rows that exist but
/// live elsewhere are fetched solely for the exists/deleted partition,
/// and shipping their jsonb policy fleet-wide would multiply the result
/// bytes ~30x for data the handler discards.
#[derive(Debug, Clone, FromRow)]
pub(super) struct SandboxDesiredStateRow {
    pub(super) sandbox_id: SandboxId,
    pub(super) assigned_here: bool,
    pub(super) snapshots_enabled_desired: Option<bool>,
    pub(super) auto_lifecycle_json: Option<Json<AutoLifecyclePolicy>>,
}

#[derive(Debug, Clone, FromRow)]
pub(super) struct SandboxPortExposureDesiredStateRow {
    pub(super) sandbox_id: SandboxId,
    pub(super) protocol: String,
    pub(super) guest_port: i32,
    pub(super) desired_state: String,
}

#[derive(Debug, Clone, FromRow)]
pub(super) struct PortExposureApplyMissRow {
    pub(super) sandbox_id: SandboxId,
    pub(super) protocol: String,
    pub(super) guest_port: i32,
}

#[derive(Debug, Clone)]
pub(super) struct ParsedPortExposureApplyState {
    pub(super) sandbox_id: SandboxId,
    pub(super) protocol: String,
    pub(super) guest_port: u16,
    pub(super) apply_state: String,
    pub(super) error: Option<String>,
    /// Last observed loopback bridge status, lowercased to match the
    /// `last_bridge_status` CHECK constraint. `None` means the publisher
    /// didn't observe a bridge status (e.g., a `failed` apply that didn't
    /// reach the in-VM agent).
    pub(super) bridge_status: Option<String>,
    /// `service_generation` from the most recent `OpenLoopbackBridgeResponse`.
    /// Empty/None means the publisher couldn't observe one.
    pub(super) service_generation: Option<String>,
}

/// Producer-side kill switch for the deleted partition: the single choke
/// point every consumer depends on. internal-api restarts in seconds, so
/// flipping this stops all cluster-deleted eviction fleet-wide far faster
/// than rolling the 347-node node-ctl DaemonSet. Fail-safe: unset means
/// enabled; any set value other than an explicit truthy disables.
pub(super) fn deleted_partition_enabled() -> bool {
    pplx_space_common_core::env::env_destructive_gate_var(
        "SPACE_INTERNAL_API_DELETED_PARTITION_ENABLED",
    )
}

/// Plausibility ceiling for the desired-state deleted partition.
/// Consumers destroy local state for ids on that list, and its only
/// failure mode worse than a leak is a wrong answer at scale (a stale or
/// misrouted database reporting healthy sandboxes as deleted). A genuine
/// orphan backlog measures 15-25% of a node's local ids; at or beyond
/// half — or an absolute count no real node has accumulated — the caller
/// suppresses the list and alarms rather than authorize mass teardown.
/// Suppression is total (no degraded slice: a bounded slice of a wrong
/// answer would still drain wrong deletions at the consumer cap forever).
/// A node with a GENUINE backlog above the ceiling wedges by design; the
/// env overrides let an operator raise the ceiling after investigating
/// the error log rather than the code shipping a bypass.
/// "At or beyond" the ceiling is implausible: an answer sitting exactly
/// on the boundary must suppress, not squeak through.
pub(super) fn deleted_partition_should_suppress(
    deleted_count: usize,
    requested_count: usize,
) -> bool {
    deleted_count >= deleted_partition_suppress_threshold(requested_count)
}

pub(super) fn deleted_partition_suppress_threshold(requested_count: usize) -> usize {
    let absolute = std::env::var("SPACE_INTERNAL_API_DELETED_SUPPRESS_ABSOLUTE")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(500);
    let percent = 50;
    std::cmp::max(absolute, requested_count * percent / 100)
}

#[cfg(test)]
mod suppress_threshold_tests;

#[derive(Debug, Clone, FromRow)]
pub(super) struct QueuedFacetRefRow {
    pub(super) facet: String,
    pub(super) element: Option<String>,
}

/// The distinct valid traceparents saved with claimed refs, in the order the
/// claim picked the refs: longest queued first, by `requested_at`, then facet
/// and element. A traceparent shared by several refs takes its first ref's
/// place.
pub(super) fn claimed_origin_traceparents(rows: &[ClaimedFacetRefRow]) -> Vec<String> {
    let mut picked = rows.iter().collect::<Vec<_>>();
    picked.sort_by(|left, right| {
        (left.requested_at, &left.facet, &left.element).cmp(&(
            right.requested_at,
            &right.facet,
            &right.element,
        ))
    });
    let mut seen = std::collections::HashSet::new();
    picked
        .into_iter()
        .filter_map(|row| row.traceparent.as_deref())
        .filter(|traceparent| {
            pplx_space_common_svc::observability::is_storable_traceparent(traceparent)
        })
        .filter(|traceparent| seen.insert(*traceparent))
        .map(str::to_string)
        .collect()
}

/// A claimed ref, its saved trace context, and when it was first queued:
/// the claim's own ordering key.
#[derive(Debug, Clone, FromRow)]
pub(super) struct ClaimedFacetRefRow {
    pub(super) facet: String,
    pub(super) element: Option<String>,
    pub(super) traceparent: Option<String>,
    pub(super) requested_at: time::OffsetDateTime,
}

pub(super) fn proto_facet(value: ReconcileFacet) -> ProtoReconcileFacet {
    match value {
        ReconcileFacet::NetworkPolicy => ProtoReconcileFacet::NetworkPolicy,
        ReconcileFacet::SandboxInventory => ProtoReconcileFacet::SandboxInventory,
        ReconcileFacet::TemplateInventory => ProtoReconcileFacet::TemplateInventory,
        ReconcileFacet::TemplateBaseSnapshotPolicy => {
            ProtoReconcileFacet::TemplateBaseSnapshotPolicy
        }
        ReconcileFacet::SandboxDesiredState => ProtoReconcileFacet::SandboxDesiredState,
        ReconcileFacet::TemplateDesiredState => ProtoReconcileFacet::TemplateDesiredState,
        ReconcileFacet::NodeStatus => ProtoReconcileFacet::NodeStatus,
        ReconcileFacet::RuntimeProvisioning => ProtoReconcileFacet::RuntimeProvisioning,
    }
}

pub(super) fn queued_facet_ref_to_proto(row: QueuedFacetRefRow) -> Result<FacetRef, Status> {
    let facet = ReconcileFacet::parse(&row.facet)
        .ok_or_else(|| Status::internal(format!("queued unknown facet {:?}", row.facet)))?;
    Ok(FacetRef {
        facet: proto_facet(facet) as i32,
        element: row
            .element
            .as_deref()
            .map(|element| facet_element_to_proto(facet, element))
            .transpose()?,
    })
}

pub(super) fn facet_element_to_proto(
    facet: ReconcileFacet,
    element: &str,
) -> Result<facet_ref::Element, Status> {
    match facet {
        ReconcileFacet::NetworkPolicy => {
            let target = element.parse::<NetworkPolicyTarget>().map_err(|error| {
                Status::internal(format!("queued network policy target is invalid: {error}"))
            })?;
            let target = match target {
                NetworkPolicyTarget::Template(template) => NetworkPolicyTargetRef {
                    target: Some(network_policy_target_ref::Target::Template(
                        template.to_string(),
                    )),
                },
                NetworkPolicyTarget::Sandbox(sandbox_id) => NetworkPolicyTargetRef {
                    target: Some(network_policy_target_ref::Target::SandboxId(
                        sandbox_id.to_string(),
                    )),
                },
            };
            Ok(facet_ref::Element::NetworkPolicyTarget(target))
        }
        ReconcileFacet::SandboxInventory | ReconcileFacet::SandboxDesiredState => {
            Ok(facet_ref::Element::SandboxId(element.to_string()))
        }
        ReconcileFacet::TemplateInventory
        | ReconcileFacet::TemplateBaseSnapshotPolicy
        | ReconcileFacet::TemplateDesiredState => {
            Ok(facet_ref::Element::Template(element.to_string()))
        }
        ReconcileFacet::NodeStatus | ReconcileFacet::RuntimeProvisioning => Err(Status::internal(
            "queued whole-node facet must not have an element",
        )),
    }
}

pub(super) fn proto_facet_ref_to_storage(value: FacetRef) -> Result<QueuedFacetRefRow, Status> {
    let facet =
        ProtoReconcileFacet::try_from(value.facet).unwrap_or(ProtoReconcileFacet::Unspecified);
    let facet = match facet {
        ProtoReconcileFacet::NetworkPolicy => FACET_NETWORK_POLICY,
        ProtoReconcileFacet::SandboxInventory => FACET_SANDBOX_INVENTORY,
        ProtoReconcileFacet::TemplateInventory => FACET_TEMPLATE_INVENTORY,
        ProtoReconcileFacet::TemplateBaseSnapshotPolicy => FACET_TEMPLATE_BASE_SNAPSHOT_POLICY,
        ProtoReconcileFacet::SandboxDesiredState => FACET_SANDBOX_DESIRED_STATE,
        ProtoReconcileFacet::TemplateDesiredState => FACET_TEMPLATE_DESIRED_STATE,
        ProtoReconcileFacet::NodeStatus => FACET_NODE_STATUS,
        ProtoReconcileFacet::RuntimeProvisioning => FACET_RUNTIME_PROVISIONING,
        ProtoReconcileFacet::Unspecified => {
            return Err(Status::invalid_argument("facet is required"));
        }
    };
    let element = match (facet, value.element) {
        (_, None) => None,
        (FACET_NETWORK_POLICY, Some(facet_ref::Element::NetworkPolicyTarget(target))) => {
            Some(proto_network_policy_target_to_storage(target)?)
        }
        (
            FACET_SANDBOX_INVENTORY | FACET_SANDBOX_DESIRED_STATE,
            Some(facet_ref::Element::SandboxId(sandbox_id)),
        ) => Some(sandbox_id),
        (
            FACET_TEMPLATE_INVENTORY
            | FACET_TEMPLATE_BASE_SNAPSHOT_POLICY
            | FACET_TEMPLATE_DESIRED_STATE,
            Some(facet_ref::Element::Template(template)),
        ) => Some(template),
        (FACET_NODE_STATUS | FACET_RUNTIME_PROVISIONING, Some(_)) => {
            return Err(Status::invalid_argument(
                "whole-node facet does not accept an element",
            ));
        }
        (facet, Some(_)) => {
            return Err(Status::invalid_argument(format!(
                "facet ref element does not match facet {facet:?}"
            )));
        }
    };
    Ok(QueuedFacetRefRow {
        facet: facet.to_string(),
        element,
    })
}

pub(super) fn proto_network_policy_target_to_storage(
    target: NetworkPolicyTargetRef,
) -> Result<String, Status> {
    match target.target {
        Some(network_policy_target_ref::Target::Template(template)) => {
            let template = pplx_space_common_core::types::TemplateName::parse(template)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            Ok(NetworkPolicyTarget::template(template).as_element_string())
        }
        Some(network_policy_target_ref::Target::SandboxId(sandbox_id)) => {
            let sandbox_id = sandbox_id
                .parse::<SandboxId>()
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            Ok(NetworkPolicyTarget::sandbox(sandbox_id).as_element_string())
        }
        None => Err(Status::invalid_argument(
            "network policy target is required",
        )),
    }
}

pub(super) async fn clear_facet_apply_failed_condition(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    node_id: NodeId,
    reference: &QueuedFacetRefRow,
) -> Result<(), Status> {
    sqlx::query(sql!(
        "DELETE FROM node_facet_conditions
         WHERE node_id = $1
           AND facet = $2
           AND element = COALESCE($3, '')
           AND condition_type = 'ApplyFailed'"
    ))
    .bind(node_id)
    .bind(&reference.facet)
    .bind(reference.element.as_deref())
    .execute(&mut **tx)
    .await
    .map_err(|err| Status::internal(format!("clear facet apply condition: {err}")))?;
    Ok(())
}

pub(super) async fn record_facet_apply_failed_condition(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    node_id: NodeId,
    reference: &QueuedFacetRefRow,
    error: &str,
) -> Result<(), Status> {
    sqlx::query(sql!(
        "INSERT INTO node_facet_observed_status (
             node_id, facet, element, observed_at, updated_at
         )
         VALUES ($1, $2, COALESCE($3, ''), NOW(), NOW())
         ON CONFLICT (node_id, facet, element) DO UPDATE
         SET updated_at = NOW()"
    ))
    .bind(node_id)
    .bind(&reference.facet)
    .bind(reference.element.as_deref())
    .execute(&mut **tx)
    .await
    .map_err(|err| Status::internal(format!("ensure facet status for condition: {err}")))?;

    sqlx::query(
        sql!("INSERT INTO node_facet_conditions (
             node_id, facet, element, condition_type, condition_status,
             reason, message, last_transition_at, updated_at
         )
         VALUES (
             $1, $2, COALESCE($3, ''), 'ApplyFailed', 'True',
             'ReconcileFailed', $4, NOW(), NOW()
         )
         ON CONFLICT (node_id, facet, element, condition_type) DO UPDATE
         SET condition_status = EXCLUDED.condition_status,
             reason = EXCLUDED.reason,
             message = EXCLUDED.message,
             last_transition_at = CASE
                 WHEN node_facet_conditions.condition_status IS DISTINCT FROM EXCLUDED.condition_status
                 THEN NOW()
                 ELSE node_facet_conditions.last_transition_at
             END,
             updated_at = NOW()"),
    )
    .bind(node_id)
    .bind(&reference.facet)
    .bind(reference.element.as_deref())
    .bind(error)
    .execute(&mut **tx)
    .await
    .map_err(|err| Status::internal(format!("record facet apply condition: {err}")))?;
    Ok(())
}

#[derive(Debug, Clone, FromRow)]
pub(super) struct TemplateReleaseRow {
    pub(super) id: Uuid,
    pub(super) template_name: String,
    #[sqlx(rename = "type")]
    pub(super) release_type: String,
    pub(super) hash: String,
    pub(super) org_id: Option<OrgId>,
    pub(super) created_at: OffsetDateTime,
}

#[derive(Debug, Clone, FromRow)]
pub(super) struct TemplateReleaseTagRow {
    pub(super) id: Uuid,
    pub(super) tag: String,
    pub(super) created_at: OffsetDateTime,
}

#[derive(Debug, Clone, FromRow)]
pub(super) struct TemplateReleaseStorageLocationRow {
    pub(super) id: Uuid,
    pub(super) storage_connection_string: String,
    pub(super) created_at: OffsetDateTime,
}

/// SQLSTATE `undefined_column`, raised when the control-plane schema has no
/// `templates.visibility` column. The column is in the baseline schema, so
/// this means schema drift; it gets its own reason
/// because its remediation differs from a failing read.
const SQLSTATE_UNDEFINED_COLUMN: &str = "42703";

/// Classify a failed visibility lookup by the SQLSTATE the server returned.
///
/// Only `undefined_column` reports `column_missing`.
/// Note the consequence for the other durable codes — a renamed table
/// (`42P01`), a revoked grant (`42501`), a wrong database (`3D000`) and a dropped
/// database (`57P04`), which is `DURABLE_SQLSTATES` in full besides `42703`: they
/// all report `query_failed`, the same reason a momentary failure reports. They
/// are distinguished by *persistence*, not by tag. Splitting them would want
/// a reason per code, which is only
/// worth adding once something alerts on the difference.
/// Every other `sqlx::Error` — pool exhaustion, connection reset, permission
/// denied, the role's `statement_timeout` — reaches the same arm, and since the
/// visibility migration is already in the chain, DB trouble is the common case
/// and schema skew the rare one. Getting this backwards is not cosmetic: it
/// points an operator at the schema during a live DB incident, and it muddies
/// the `column_missing` signal that means the schema itself drifted. Split out
/// from the call site so the mapping is unit-testable without a database.
///
/// Cached prepared statements do not defeat this in either direction. Dropping
/// the column invalidates the relcache entry, which forces revalidation of the
/// cached plan; because the statement names `visibility` explicitly,
/// revalidation re-runs parse analysis and fails with `42703`, so a pooled
/// connection that had already prepared this query still classifies correctly.
/// Forward is clean for the simpler reason that a failed prepare is not
/// cached.
fn degradation_for_sqlstate(code: Option<&str>) -> VisibilityLookupDegradation {
    if code == Some(SQLSTATE_UNDEFINED_COLUMN) {
        VisibilityLookupDegradation::ColumnMissing
    } else {
        VisibilityLookupDegradation::QueryFailed
    }
}

/// Report a lookup that produced no usable answer, and return the value the
/// response carries.
///
/// The two counters are emitted together because they are one fact seen from two
/// angles: the cache could not answer (`outcome:miss`) and the response lost
/// fidelity (`reason:<why>`). Writing the pair out at each degradation arm — as
/// this did at three of them — invites a fourth arm that emits one and not the
/// other, which would leave either the cache-hit-rate series or the
/// prefetch-suppression monitor quietly under-counting. Returning
/// `Unspecified` from here means an arm cannot report a miss and then answer
/// with something else. The same call records the degradation in the
/// trailing-hour window behind `template.visibility_lookup.alertable_last_hour`,
/// so while the emitter runs that series and the `degraded` counter count the
/// same events. The emitter stops at shutdown, so a degradation during the
/// request drain reaches only the counter.
fn visibility_miss(
    reason: VisibilityLookupDegradation,
    observations: &crate::visibility_observations::Observations,
) -> TemplateVisibility {
    observations.record(reason);
    visibility_degraded_with(VisibilityLookupCacheOutcome::Miss, reason)
}

/// As [`visibility_miss`], but for the no-row case, which carries its own cache
/// outcome so `miss` keeps no routine floor. See
/// [`VisibilityLookupCacheOutcome::Absent`].
fn visibility_absent() -> TemplateVisibility {
    visibility_degraded_with(
        VisibilityLookupCacheOutcome::Absent,
        VisibilityLookupDegradation::TemplateMissing,
    )
}

fn visibility_degraded_with(
    outcome: VisibilityLookupCacheOutcome,
    reason: VisibilityLookupDegradation,
) -> TemplateVisibility {
    visibility_lookup_cache(outcome);
    visibility_lookup_degraded(reason);
    TemplateVisibility::Unspecified
}

/// Domain visibility -> the proto enum on the wire.
///
/// Total by construction, and that is the point: `CoreTemplateVisibility` has no
/// `Unspecified` variant, so there is no arm here that could fabricate one — or,
/// worse, quietly map an unknown to `Generic`. The proto's `Unspecified` is
/// produced only on the explicit degradation arms of [`template_visibility`],
/// never by a conversion.
///
/// A free function rather than a `From` impl because it has to be: both types
/// are foreign to this crate (`CoreTemplateVisibility` from `common-core`, the
/// proto enum re-exported from `pplx-space-node-api-client` at `lib.rs`'s
/// `pub use … ::proto`), so an impl here would violate Rust's orphan rule.
const fn proto_visibility(visibility: CoreTemplateVisibility) -> TemplateVisibility {
    match visibility {
        CoreTemplateVisibility::Generic => TemplateVisibility::Generic,
        CoreTemplateVisibility::OrgSpecific => TemplateVisibility::OrgSpecific,
    }
}

/// Individual transient codes outside a wholly-transient class.
const TRANSIENT_SQLSTATES: &[&str] = &[
    // Class `08` (connection_exception), transient members only.
    //
    // `08001 sqlclient_unable_to_establish_sqlconnection` is deliberately absent
    // rather than overlooked: "the client could not establish a connection" covers
    // a momentary network fault and a wrong host or exhausted `max_connections`
    // equally, and nothing in the code distinguishes them. Unclassified is the
    // right home for a code whose meaning is genuinely ambiguous — it degrades the
    // request without erasing state. Same for `08004`/`08P01`, which are standing.
    "08000", // connection_exception
    "08003", // connection_does_not_exist
    "08006", // connection_failure
    "08007", // transaction_resolution_unknown
    // Class `53` (insufficient_resources), transient members only — `53100`
    // (disk_full) and `53400` (configuration_limit_exceeded) are standing
    // conditions and stay unclassified.
    "53000", // insufficient_resources
    "53200", // out_of_memory
    "53300", // too_many_connections
    "40001", // serialization_failure
    "40P01", // deadlock_detected
    "40003", // statement_completion_unknown
    "55P03", // lock_not_available
    "55006", // object_in_use
    "57014", // query_canceled — the pool's own statement_timeout firing
    "57P01", // admin_shutdown
    "57P02", // crash_shutdown
    "57P03", // cannot_connect_now — starting up, retry is exactly right
    "57P05", // idle_session_timeout
    "58030", // io_error — a storage blip, not a standing condition
    // `0A000` (`feature_not_supported`) is what a pooled connection raises when
    // its cached plan outlived a result-type change. It has moved twice, so the
    // reasoning is recorded rather than re-derived:
    //
    // It was **durable** (erase). That was actively harmful:
    // `ALTER TABLE templates` is this module's named contention source and
    // raises both `57014` (reads contending with its lock) and `0A000`
    // (connections whose cached plan its commit invalidated), so the two arrive
    // together by construction and the erasure emptied the map out from under
    // exactly the `57014` readers about to be answered correctly.
    //
    // It was then **unclassified** (neither serve nor erase). That fixed the
    // erasure but left the fallback refused, and `0A000` does not self-heal:
    // sqlx-postgres 0.9.0 has no handling for it (`clear_cached_statements` is
    // an explicit `Connection` method nothing calls here), so a poisoned
    // connection keeps raising it until the pool recycles it. That bound is set
    // by `postgres_pool_options` (`common/svc/src/sql/mod.rs:214`), which this
    // pool goes through and which overrides *both* of sqlx's defaults: idle
    // connections are reclaimed at `DEFAULT_IDLE_TIMEOUT_SECS` (300s), and
    // `max_lifetime` is set to `None` — sqlx's acquire-time enforcement is
    // disabled on purpose — with retirement moved to an `after_release` age
    // check against a jittered `DEFAULT_MAX_LIFETIME_SECS`. At jitter fraction
    // 1/6 that lands in [25, 35) minutes, and it is evaluated when the
    // connection is *released*, so a continuously busy one is not retired
    // mid-use. So the replica failed closed on every lookup that connection
    // served — 5 minutes if it went idle, up to ~35 if it stayed busy — with a
    // valid cached entry sitting right there. That is the blast radius this cache
    // exists to remove, on the one event most likely to cause it.
    //
    // Transient is the remaining position and the correct one: `0A000` is a
    // statement about a *connection's plan*, not about the row — precisely
    // invariant 2's "we don't know what the row says". Serving costs at most one
    // staleness ceiling of a value whose column was being retyped, not rewritten,
    // after which the entry ages out and `degraded{reason:query_failed}` fires
    // continuously. It does not reopen the erasure defect, because serving and
    // erasing are separate predicates.
    "0A000",
];

/// SQLSTATEs that mean the row is not readable until the database changes.
const DURABLE_SQLSTATES: &[&str] = &[
    "42703", // undefined_column — the visibility column itself
    "42P01", // undefined_table
    "42501", // insufficient_privilege — a revoked grant
    "3D000", // invalid_catalog_name — wrong database
    "57P04", // database_dropped
];

// Deliberately in NEITHER list, so they degrade one request and leave the cached
// entry alone: `58P01`/`58P02` (`undefined_file`, `duplicate_file`) are a missing
// relation segment, i.e. corruption that persists until a restore — not the
// storage blip `58030` is. Serving through them would be wrong, and erasing on
// them claims knowledge about the row's *content* that a missing file does not
// give, so they get the neutral treatment. Any code on neither list lands here
// too, and that default is the deliberate one: both alternatives fail unsafely.
//
// `0A000` is **not** here — it is on the transient list, for the reasons recorded
// there. It was in this block for one revision, which is why the note is explicit.

/// Whether a failed lookup may answer from a cached read.
///
/// An **allowlist**: a code nobody enumerated does not serve. That direction is
/// the point — on a gate whose purpose is failing closed, an unrecognized failure
/// must degrade rather than hand back a remembered value. An earlier revision
/// enumerated the durable codes instead, so anything unlisted served stale for the
/// whole ceiling. `XX000 internal_error` is the example to reason with: Postgres
/// raises it for "could not open relation" and "cache lookup failed" under
/// concurrent DDL, exactly the window this lookup is exposed to, and under a
/// blocklist it would have answered from cache purely because nobody had named it.
///
/// Do not use `0A000` as that example. It reads like one and is not: it is on the
/// transient list *deliberately*, so it does serve stale, and the reasoning is
/// recorded on the constant. Removing it to "restore" fail-closed reinstates a
/// documented blast radius.
fn may_serve_stale(error: &sqlx::Error, code: Option<&str>) -> bool {
    if read_but_unusable(error) {
        return false;
    }
    match code {
        // No SQLSTATE means the server did not answer with one. Usually that is
        // the transient case this exists for — a pool acquire timeout, a closed
        // pool, a severed connection. It is not *only* those: `Configuration`,
        // `Encode`, `Protocol` and `Migrate` also carry no `DatabaseError` and
        // land here. Serving a remembered value through one of those is wrong but
        // bounded by invariant 3, and narrowing it means enumerating `sqlx`
        // variants that carry no stable meaning across versions. Bounded-wrong
        // beats a match that silently rots.
        None => true,
        // Every transient code is enumerated; there is deliberately no
        // class-prefix match. Each candidate class turned out to contain a
        // standing member (the note on `TRANSIENT_SQLSTATES` records which), and
        // a prefix is an allowlist entry for codes nobody has read — the wrong
        // direction on a gate whose purpose is failing closed.
        Some(code) => TRANSIENT_SQLSTATES.contains(&code),
    }
}

/// Whether a failed lookup means the cached read no longer describes the row, so
/// it must be dropped.
///
/// A **blocklist**, and the opposite direction to [`may_serve_stale`] on purpose.
/// The two questions have opposite fail-safe directions, and an earlier revision
/// answered both with one predicate — which was the more dangerous of the two
/// mistakes available. Using `!may_serve_stale` as the invalidation condition
/// meant any unenumerated code *erased* the last known good read: an
/// `58030 io_error` during a storage blip would drop the entry, and the lock
/// contention that followed it (`57014`, this module's own motivating case) would
/// then find an empty map and fail closed fleet-wide for the whole incident. The
/// cache would have handed back exactly the blast radius it exists to shrink.
///
/// So an unclassified failure now does neither: it degrades this one request and
/// leaves the entry alone. Only a positively-identified durable condition, or a
/// row that was read and could not be used, drops state.
fn should_invalidate(error: &sqlx::Error, code: Option<&str>) -> bool {
    read_but_unusable(error) || code.is_some_and(|code| DURABLE_SQLSTATES.contains(&code))
}

/// The row *was* read and its answer cannot be used — invariant 2's case arriving
/// through `Err`, because `sqlx` surfaces type mismatches as errors.
///
/// These carry no `DatabaseError`, so without naming them the no-SQLSTATE rule in
/// [`may_serve_stale`] would call them transient. Reachable without exotic input:
/// migrating `visibility` from `TEXT` to a Postgres enum succeeds server-side and
/// then fails to decode into `String` on every read.
///
/// `ColumnDecode` and `Decode` are the reachable pair. Three more are unreachable
/// against the current query shape and included for refactor-safety, since the
/// cost of listing one is nil and the cost of missing one is serving a remembered
/// value through a read-but-undecodable row.
///
/// `RowNotFound` needs its reason stated carefully, because the obvious gloss
/// ("read the row, no answer") is wrong and would contradict the `TypeNotFound`
/// exclusion below: `fetch_one` raises it when *zero rows came back*, so no row
/// was read. It belongs here anyway, for a different reason — under the bare
/// `WHERE name = $1` refactor that makes it live, zero rows is positive knowledge
/// that the `templates` row is **gone**, i.e. the `Ok(None)` case arriving through
/// `Err`, and a deleted template must report `UNSPECIFIED`. That is knowledge
/// about the row; `TypeNotFound` is a failure to ask the question at all. Then the
/// two column-addressing variants. `ColumnNotFound` is raised by
/// name-based `try_get`; `ColumnIndexOutOfBounds` is raised by index-based
/// decoding, which is what this query already does when it reads into a tuple — so
/// it is the one a change to *this* statement's column list would raise, and
/// omitting it while listing its name-based sibling would be backwards.
///
/// `TypeNotFound` is deliberately **not** here, and the reason is the contract in
/// this doc's first line rather than reachability. Its only raise site is
/// `sqlx-postgres-0.9.0/src/connection/resolve.rs:189`, in type-OID resolution for
/// types declared by name — so it fires *before* the statement executes, and
/// nothing whatsoever has been learned about the row. Erasing on it would claim
/// exactly the knowledge that `58P01`/`58P02` are left unclassified for lacking.
/// It carries no `DatabaseError`, so it lands on the no-SQLSTATE arm and serves
/// stale, the same bounded-wrong treatment the other pre-execution `sqlx` variants
/// get. Reachable only via a refactor to a named type
/// (`#[sqlx(type_name = "…")]`), which is the very migration that would also be
/// producing the lock waits this fallback exists to cover.
fn read_but_unusable(error: &sqlx::Error) -> bool {
    matches!(
        error,
        sqlx::Error::ColumnDecode { .. }
            | sqlx::Error::Decode(_)
            | sqlx::Error::ColumnNotFound(_)
            | sqlx::Error::ColumnIndexOutOfBounds { .. }
            | sqlx::Error::RowNotFound
    )
}

#[cfg(test)]
mod visibility_degradation_tests;

// Deliberately no client-side `tokio::time::timeout` around the lookup below,
// even though it sits on the sandbox-create path and a lock wait (any
// `ALTER TABLE templates`, including the visibility migration itself) makes it
// block rather than fail fast. Wrapping it would make things worse, not
// better: `timeout` cancels by *dropping* the query future, which does not
// cancel the statement server-side. sqlx then treats the connection as
// possibly inconsistent and, in `PoolConnection::return_to_pool`, pings it
// before reuse — that ping blocks draining the abandoned query's
// `ReadyForQuery` and hard-closes the connection if it fails. So the caller
// would return quickly while the connection stayed out of the pool for the
// rest of the statement's life. Under the very contention this would be
// guarding against, every in-flight RPC doing that can drain the pool and
// start failing the *release* query — turning bounded extra latency into
// failed sandbox creates.
//
// What bounds it instead is the pool it runs on. The release queries themselves
// never read `templates`, so DDL on that table cannot block them directly. It
// could still reach them through *this* lookup: on a shared pool each blocked
// lookup pins a connection until the role's `statement_timeout` (`space_user`,
// 10s), and a drained pool fails a later release query's 5s acquire despite
// that query needing no lock at all. So the lookup runs on its own small pool
// (`ClusterApiService::visibility_pool`) and the connections it can tie up are
// its own; exhausting them degrades this field and nothing else.
//
// Be precise about what that does *not* buy, because the tempting conclusion is
// wrong: release resolution is not immune to `templates` DDL. Several sibling
// handlers on this service read `templates` on the general pool — network
// policy and base-snapshot policy (`adapter.rs`) are node-facing and called per
// node, so across a fleet they are the dominant way that pool drains during
// such a window. This change removes the *release-resolution* path's own
// contribution; bounding the rest belongs at the role, not here.
//
// Sideline exhaustion is still observable, but *this counter is no longer where
// you see it* wherever the cache is enabled. An acquire failure carries no
// `DatabaseError`, so it takes the no-SQLSTATE serve-stale arm: with a warm entry
// the request is answered from cache and emits no `degraded` at all, and a
// ten-minute exhaustion can read zero here. The saturation signal is
// `template.visibility_lookup.cache{outcome:served_stale}`; `reason:query_failed`
// fires only once the cache can no longer cover, which is the later and more
// serious state.
//
// The pool deliberately emits no `db.pool.*` series of its own — those are
// tagged by `database_identity` only, and `monitor_db_pool.tf` pages on
// `max:space.db.pool.utilization` grouped without it, so a 3-connection pool's
// ratio would both page spuriously and mask the general pool's real number.
//
// What that still leaves is latency, not failure. A blocked lookup makes the
// RPC that triggered it wait — up to this pool's own 500ms
// `VISIBILITY_POOL_STATEMENT_TIMEOUT` (not the role's 10s, which this pool
// overrides) while the small pool has room, then up to the 5s acquire timeout
// once it doesn't, before degrading. Bounding that wait *at the source* means a
// role-level `lock_timeout`, and that mechanism now exists: `space_user` and
// `space_ro` take one from the `space-db-migrate` chart's `appLockTimeout`, at
// which point a blocked lookup fails with 55P03 inside the bound rather than
// waiting. With `appLockTimeout = "0"`, the wait above still applies. See
// `db/README.md`, "Bounding application lock waits", for configuration. This
// is neither client-side cancellation nor a cache-provided bound.
//
// (The same sideline-vs-cache distinction is drawn from the cache's side in
// `template_visibility_cache`'s module doc. They are deliberately both present —
// a reader arrives at one or the other — so an edit to either belongs in both.)
//
// The cache does not bound this wait. It removes the statement on a *hit*, so it
// lowers how often the wait is incurred without changing its length; every miss
// still pays the full 500ms and then the acquire timeout. Under the sustained
// `ALTER TABLE templates` contention that produces these waits, the miss rate is
// exactly what a TTL cannot control, so raising `ttlSeconds` to chase a p99 will
// not move it. Note which problem the cache does solve, because it is not this
// one. The pool
// sideline protects release resolution *from* this lookup; the cache protects
// this lookup's *consumers* from a failed read. They are complements, and the
// sideline sharpens the need for the cache rather than reducing it: this pool's
// own 500ms `statement_timeout` (`VISIBILITY_POOL_STATEMENT_TIMEOUT`) cancels a
// contended read in half a second, well short of the role-level bound the
// comment above cites. That is
// the trade — a latency risk to release resolution is exchanged for a higher
// `Unspecified` rate here — and `Unspecified` is what a visibility-gated
// consumer must read as "not known to be generic" and fail closed on. For the
// prefetch loop that means no warming, on every node, for as long as the read
// keeps failing. The cache is what keeps a blip from reaching that far.

/// Resolve `templates.visibility` for one template into the proto
/// [`TemplateVisibility`] enum carried on the release-resolution responses.
///
/// Best-effort by design, and advisory only: no consumer reads the field yet.
/// It exists so node-side consumers can gate on visibility — the intended
/// first consumer is the template-manager prefetch loop, which will refuse to
/// warm non-generic templates. Populating it must never
/// take down release resolution itself, because the same RPCs serve on-demand
/// sandbox-create pulls. So instead of joining `templates` into the release
/// queries, the lookup runs as its own statement: a joined `visibility`
/// against a column-less table would fail the *whole* release query, whereas a
/// separate statement can fail alone. Every failure degrades to `Unspecified`,
/// which enforcing consumers must treat as "not known to be generic" and fail
/// closed.
///
/// The cost of that isolation *when the cache is off, or misses* is one extra
/// round-trip per release-resolution RPC, bounded only server-side for the reason
/// spelled out above the function, and a response assembled from two snapshots
/// rather than one.
///
/// With a window open both halves of that change. A fresh hit issues **no**
/// visibility statement at all, so most calls carry no second round trip. And the
/// answer is no longer "never staler than the release": a fresh hit predates the
/// release query by up to the TTL, and a stale fallback by up to the ceiling. What
/// remains true is that the staleness is *bounded* by whichever window served it,
/// and that a concrete value always reflects what the row said at some point —
/// never a fabrication.
///
/// The stored string is parsed once here, at the wire boundary, through the
/// canonical [`CoreTemplateVisibility`] parser; nothing downstream handles the
/// raw string. The domain enum has no `Unspecified` variant by design (a
/// missed read must never silently read as generic), so this is where the
/// best-effort "couldn't determine it" case is folded into the proto's
/// `Unspecified`.
///
/// Every path that *degrades the answer* emits
/// `template.visibility_lookup.degraded` with a distinguishing `reason` tag,
/// because a field that silently loses fidelity is one no monitor can fire on.
/// Note the narrowing: a failed lookup answered from the cache does **not**
/// emit it, because the answer did not degrade — which does mean this counter is
/// no longer the saturation signal for the sideline pool. That role moves to
/// `template.visibility_lookup.cache{outcome:served_stale}`.
/// Every path — degraded or not — also emits
/// exactly one `template.visibility_lookup.cache` with an `outcome` tag, so the
/// two counters answer different questions: whether the response lost fidelity,
/// and which path produced the answer. `outcome` does not cleanly answer "was the
/// database reached" — `miss` covers both the parse-failure arm (reached, row
/// read, value unusable) and the failed-query arm (possibly never reached), so
/// separating those needs `degraded`'s `reason` tag.
///
/// Note one fleet-level consequence, since nothing else states it: the cache is
/// per replica, so during a read outage replicas disagree — one with a warm entry
/// answers a concrete visibility while one that restarted a minute ago answers
/// `Unspecified`, for the same template, seconds apart. That is inherent to
/// per-replica caching (a shared cache would be a new dependency on the path this
/// exists to protect), and the proto's per-response consistency guarantee is
/// unaffected: it promises agreement *within* one response, not across them.
///
/// Reads are served from `cache` when recent enough, and fall back to it when
/// the query cannot reach the row. See [`TemplateVisibilityCache`] for the
/// invariants — in particular why a row that *was* read but whose answer is
/// unusable invalidates the entry instead of falling back to it.
pub(super) async fn template_visibility(
    pool: &sqlx::PgPool,
    cache: &TemplateVisibilityCache,
    observations: &crate::visibility_observations::Observations,
    template_name: &str,
) -> TemplateVisibility {
    // `started` doubles as the cache entry's `read_at` on success. It is not
    // when the value was true — it is captured before the pool acquire, so the
    // statement's snapshot is taken some time after it (up to the 5s acquire
    // timeout, plus a round trip). Dating the entry here therefore makes it
    // conservatively *older* than the value actually is, which can only shorten
    // its life, never extend it past the ceiling. Reading the clock again after
    // the query would be more precise and wrong in the unsafe direction.
    let started = Instant::now();
    if let Some(visibility) = cache.get_fresh(template_name, started) {
        visibility_lookup_cache(VisibilityLookupCacheOutcome::Fresh);
        return proto_visibility(visibility);
    }
    // Shaped as a scalar subselect rather than `SELECT visibility ... WHERE
    // name = $1` so it returns exactly one row whether or not the template
    // exists, which is what lets one query carry both the value and the
    // server-side instant the cache orders writes on. `visibility` is `NOT
    // NULL`, so a NULL here unambiguously means "no such row" rather than "a row
    // with no value" — and `templates.name` is the primary key, so the subselect
    // cannot return more than one row.
    //
    // The no-row arm discards the instant: it invalidates, and a removal needs
    // no ordering. An earlier revision retained a dated marker there and did
    // need it; that is gone, so only the value/NULL disambiguation still
    // justifies this shape.
    let observed: Result<(OffsetDateTime, Option<String>), sqlx::Error> = sqlx::query_as(sql!(
        "SELECT clock_timestamp() AS observed_at,
                (SELECT visibility FROM templates WHERE name = $1) AS visibility"
    ))
    .bind(template_name)
    .fetch_one(pool)
    .await;
    match observed {
        Ok((observed_at, Some(value))) => match CoreTemplateVisibility::parse(&value) {
            Ok(parsed) => {
                cache.store(template_name, parsed, started, observed_at);
                visibility_lookup_cache(VisibilityLookupCacheOutcome::Refreshed);
                proto_visibility(parsed)
            }
            Err(error) => {
                // The row was read and its answer is unusable, so the cached
                // value can no longer be assumed to describe it — and an
                // unparseable value means the column's CHECK was bypassed,
                // which is the last moment to start trusting a remembered
                // `generic`. Invalidate rather than fall back.
                cache.invalidate(template_name);
                tracing::warn!(
                    // Named, not length-elided: the point of this warn is that a
                    // row needs fixing, and the name is how an operator finds it.
                    // Sibling handlers in this module log template names the same
                    // way, as does `template.rootfs.expansion.error`'s tag.
                    template = template_name,
                    error = %error,
                    "unrecognized templates.visibility value; reporting unspecified"
                );
                visibility_miss(VisibilityLookupDegradation::UnrecognizedValue, observations)
            }
        },
        // No `templates` row. Routine for `list_latest_template_releases`, whose
        // caller may name a template that doesn't exist (it gets zero entries
        // anyway). On the `find_template_release*` paths it is rarer but still
        // not proof of corruption: `template_releases.template_name` is a
        // foreign key onto `templates(name)` with `ON DELETE RESTRICT`, so a
        // resolved release implies its template row *at the instant the release
        // was read* — but this lookup is a second snapshot, so a delete that
        // lands in between reaches here benignly. Only a `templates` row
        // vanishing while its releases survive indicates real drift, and the FK
        // makes that hard to reach: `ON DELETE RESTRICT` binds a hand-run
        // `DELETE` exactly as it binds the app, and `TRUNCATE templates CASCADE`
        // takes `template_releases` with it. Getting there needs the constraint
        // actually bypassed — dropped, `DISABLE TRIGGER ALL`,
        // `session_replication_role = replica`, or a restore/logical-replication
        // path that skips FK validation. Worth knowing before chasing a DML
        // audit that cannot explain it.
        //
        // Counted rather than warned: the routine case is caller-driven and
        // unbounded, so a log line per occurrence would be noise. The counter is
        // documented as not alertable for the same reason — which does mean the
        // drift case has no distinct signal. Separating them wants an `rpc`
        // dimension, and is only worth adding once a consumer acts on it.
        //
        // No fallback to a cached read here, for the same reason as the
        // unparseable-value arm: the row was reached and it says the template is
        // gone. Answering `org_specific` from memory for a deleted template
        // would also contradict the `ListLatestTemplateReleases` response-level
        // field, which documents "template deleted" as reporting UNSPECIFIED.
        Ok((_observed_at, None)) => {
            cache.invalidate(template_name);
            visibility_absent()
        }
        Err(error) => {
            let code = error.as_database_error().and_then(|error| error.code());
            let reason = degradation_for_sqlstate(code.as_deref());
            // `reason` below drives only the log line and the `degraded` tag; it
            // gates nothing. The fallback and the invalidation are gated by the
            // two predicates further down, asked separately because their
            // fail-safe directions are opposite: an unclassified code must not
            // serve (fail closed on the answer) and must not erase state either
            // (a dropped entry makes the *next* failure fail closed too).
            if should_invalidate(&error, code.as_deref()) {
                cache.invalidate(template_name);
            }
            if may_serve_stale(&error, code.as_deref())
                && let Some(stale) = cache.get_stale(template_name, Instant::now())
            {
                visibility_lookup_cache(VisibilityLookupCacheOutcome::ServedStale);
                // `debug`, not `warn`: this fires once per release resolution on
                // every node for as long as the read is broken, and the
                // `outcome:served_stale` counter is the signal. The `warn`
                // belongs on the arm below, where the answer actually degrades.
                tracing::debug!(
                    template = template_name,
                    error = %error,
                    visibility = %stale,
                    "templates.visibility lookup failed; answering from the last successful read"
                );
                return proto_visibility(stale);
            }
            if reason == VisibilityLookupDegradation::ColumnMissing {
                tracing::warn!(
                    template = template_name,
                    error = %error,
                    "templates.visibility column is absent; reporting unspecified \
                     (schema drift: the column is in the baseline schema)"
                );
            } else {
                tracing::warn!(
                    template = template_name,
                    error = %error,
                    "templates.visibility lookup failed; reporting unspecified"
                );
            }
            visibility_miss(reason, observations)
        }
    }
}

/// Flat row from the `find_template_release` LEFT JOIN. Storage fields are
/// `NULL` when a release exists but has no storage locations yet.
#[derive(Debug, FromRow)]
pub(super) struct FindTemplateReleaseRow {
    pub(super) generic_authorized: bool,
    pub(super) release_id: Uuid,
    pub(super) release_template_name: String,
    pub(super) release_type: String,
    pub(super) release_hash: String,
    pub(super) release_created_at: OffsetDateTime,
    pub(super) storage_id: Option<Uuid>,
    pub(super) storage_connection_string: Option<String>,
    pub(super) storage_created_at: Option<OffsetDateTime>,
}

pub(super) fn trimmed_required(value: &str, field: &str) -> Result<String, Status> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        Err(Status::invalid_argument(format!(
            "{field} must not be empty"
        )))
    } else {
        Ok(trimmed.to_string())
    }
}

pub(super) fn optional_trimmed(value: Option<&str>) -> Option<String> {
    value.map(str::trim).and_then(|s| {
        if s.is_empty() {
            None
        } else {
            Some(s.to_string())
        }
    })
}

pub(super) fn dedupe_required_strings(
    values: Vec<String>,
    field: &str,
) -> Result<Vec<String>, Status> {
    let mut out: Vec<String> = Vec::with_capacity(values.len());
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(Status::invalid_argument(format!(
                "{field} entries must not be empty"
            )));
        }
        let owned = trimmed.to_string();
        if !out.contains(&owned) {
            out.push(owned);
        }
    }
    if out.is_empty() {
        return Err(Status::invalid_argument(format!(
            "at least one {field} is required"
        )));
    }
    Ok(out)
}

pub(super) fn dedupe_nonempty_strings(
    values: Vec<String>,
    field: &str,
) -> Result<Vec<String>, Status> {
    let mut out: Vec<String> = Vec::with_capacity(values.len());
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(Status::invalid_argument(format!(
                "{field} entries must not be empty"
            )));
        }
        let owned = trimmed.to_string();
        if !out.contains(&owned) {
            out.push(owned);
        }
    }
    Ok(out)
}

pub(super) fn dedupe_optional_strings(values: Vec<String>) -> Vec<String> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        let owned = trimmed.to_string();
        if !out.contains(&owned) {
            out.push(owned);
        }
    }
    out
}

pub(super) fn parse_sandbox_id(value: &str) -> Result<SandboxId, Status> {
    value
        .trim()
        .parse::<SandboxId>()
        .map_err(|err| Status::invalid_argument(format!("invalid sandbox_id '{value}': {err}")))
}

pub(super) fn parse_sandbox_ids(values: Vec<String>) -> Result<Vec<SandboxId>, Status> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        let id = parse_sandbox_id(trimmed)?;
        if !out.contains(&id) {
            out.push(id);
        }
    }
    Ok(out)
}

/// The artifact kind a node named on the wire, or `None` when the field is
/// unset or carries a variant this build does not know.
///
/// Returns `Option` rather than a `Status` because the four callers disagree
/// about what a bad value means: three reject the RPC with their own
/// field-specific message, and the pending-backup mirror folds it into a typed
/// report error. The mapping itself — which is the part that has to change when
/// `ArtifactKind` gains a variant — lives here once.
pub(super) fn artifact_kind_from_proto(value: i32) -> Option<ArtifactKind> {
    match pplx_space_sbx_mgr_client::proto::ArtifactKind::try_from(value) {
        Ok(pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk) => Some(ArtifactKind::Disk),
        Ok(pplx_space_sbx_mgr_client::proto::ArtifactKind::Full) => Some(ArtifactKind::Full),
        Ok(pplx_space_sbx_mgr_client::proto::ArtifactKind::Unspecified) | Err(_) => None,
    }
}

/// The inverse of [`artifact_kind_from_proto`]. Total, because every core
/// variant has a wire variant.
pub(super) fn artifact_kind_to_proto(kind: ArtifactKind) -> i32 {
    match kind {
        ArtifactKind::Disk => pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk as i32,
        ArtifactKind::Full => pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32,
    }
}

pub(super) fn port_protocol_to_proto(value: &str) -> SandboxPortProtocol {
    match value {
        "tcp" => SandboxPortProtocol::Tcp,
        _ => SandboxPortProtocol::Unspecified,
    }
}

pub(super) fn port_protocol_from_proto(value: i32) -> Result<&'static str, Status> {
    match SandboxPortProtocol::try_from(value).unwrap_or(SandboxPortProtocol::Unspecified) {
        SandboxPortProtocol::Tcp => Ok("tcp"),
        SandboxPortProtocol::Unspecified => Err(Status::invalid_argument(
            "sandbox port protocol is required",
        )),
    }
}

pub(super) fn port_desired_state_to_proto(value: &str) -> SandboxPortDesiredStateKind {
    match value {
        "open" => SandboxPortDesiredStateKind::SandboxPortDesiredStateOpen,
        "closed" => SandboxPortDesiredStateKind::SandboxPortDesiredStateClosed,
        _ => SandboxPortDesiredStateKind::SandboxPortDesiredStateUnspecified,
    }
}

#[cfg(test)]
pub(super) fn port_apply_state_to_proto(value: &str) -> SandboxPortApplyStateKind {
    match value {
        "applied" => SandboxPortApplyStateKind::SandboxPortApplyStateApplied,
        "failed" => SandboxPortApplyStateKind::SandboxPortApplyStateFailed,
        _ => SandboxPortApplyStateKind::SandboxPortApplyStateUnspecified,
    }
}

pub(super) fn port_apply_state_from_proto(value: i32) -> Result<&'static str, Status> {
    match SandboxPortApplyStateKind::try_from(value)
        .unwrap_or(SandboxPortApplyStateKind::SandboxPortApplyStateUnspecified)
    {
        SandboxPortApplyStateKind::SandboxPortApplyStateApplied => Ok("applied"),
        SandboxPortApplyStateKind::SandboxPortApplyStateFailed => Ok("failed"),
        SandboxPortApplyStateKind::SandboxPortApplyStateUnspecified => Err(
            Status::invalid_argument("sandbox port apply_state is required"),
        ),
    }
}

pub(super) fn validate_guest_port(value: u32) -> Result<u16, Status> {
    u16::try_from(value)
        .ok()
        .filter(|port| *port > 0)
        .ok_or_else(|| Status::invalid_argument(format!("invalid sandbox guest_port '{value}'")))
}

pub(super) fn parse_port_exposure_apply_states(
    states: Vec<crate::proto::SandboxPortExposureApplyState>,
) -> Result<Vec<ParsedPortExposureApplyState>, Status> {
    let mut parsed = Vec::with_capacity(states.len());
    let mut seen = std::collections::BTreeSet::new();
    for state in states {
        let sandbox_id = parse_sandbox_id(&state.sandbox_id)?;
        let protocol = port_protocol_from_proto(state.protocol)?.to_string();
        let guest_port = validate_guest_port(state.guest_port)?;
        let apply_state = port_apply_state_from_proto(state.apply_state)?.to_string();
        if !seen.insert((sandbox_id, protocol.clone(), guest_port)) {
            return Err(Status::invalid_argument(format!(
                "duplicate sandbox port exposure apply state for sandbox_id '{sandbox_id}', protocol '{protocol}', guest_port '{guest_port}'"
            )));
        }
        let error = optional_trimmed(state.error.as_deref());
        let bridge_status = state
            .bridge_status
            .map(port_bridge_status_from_proto)
            .transpose()?
            .map(str::to_string);
        let service_generation = optional_trimmed(state.service_generation.as_deref());
        parsed.push(ParsedPortExposureApplyState {
            sandbox_id,
            protocol,
            guest_port,
            apply_state,
            error,
            bridge_status,
            service_generation,
        });
    }
    Ok(parsed)
}

fn port_bridge_status_from_proto(value: i32) -> Result<&'static str, Status> {
    use crate::proto::SandboxPortBridgeStatus as B;
    match B::try_from(value) {
        Ok(B::Direct) => Ok("direct"),
        Ok(B::LoopbackForwarded) => Ok("loopback_forwarded"),
        Ok(B::WaitingForListener) => Ok("waiting_for_listener"),
        Ok(B::BindConflict) => Ok("bind_conflict"),
        Ok(B::Failed) => Ok("failed"),
        Ok(B::Unspecified) | Err(_) => Err(Status::invalid_argument(format!(
            "invalid sandbox port bridge status '{value}'"
        ))),
    }
}

pub(super) fn proto_network_policy_to_common(
    policy: &NetworkPolicyProto,
) -> Result<NetworkPolicyV1, Status> {
    NetworkPolicyV1::try_from(policy).map_err(|err| Status::invalid_argument(err.to_string()))
}

pub(super) fn common_network_policy_to_proto(policy: &NetworkPolicyV1) -> NetworkPolicyProto {
    NetworkPolicyProto::from(policy)
}

pub(super) fn common_auto_lifecycle_to_proto(
    policy: &AutoLifecyclePolicy,
) -> pplx_space_sbx_mgr_client::proto::AutoLifecyclePolicy {
    pplx_space_sbx_mgr_client::proto::AutoLifecyclePolicy::from(policy)
}

/// Upper bound on `ListLatestTemplateReleases.limit`. Releases per
/// template are append-only with content-addressable digests, so a
/// node only ever needs to warm the latest handful — capping at 50
/// keeps a misconfigured caller from issuing a result set large
/// enough to dominate the LEFT JOIN expansion to storage locations.
pub(super) const LIST_LATEST_TEMPLATE_RELEASES_MAX_LIMIT: u32 = 50;

pub(super) fn validated_list_limit(value: u32) -> Result<u32, Status> {
    if value == 0 {
        return Err(Status::invalid_argument("limit must be >= 1"));
    }
    if value > LIST_LATEST_TEMPLATE_RELEASES_MAX_LIMIT {
        return Err(Status::invalid_argument(format!(
            "limit must be <= {LIST_LATEST_TEMPLATE_RELEASES_MAX_LIMIT}"
        )));
    }
    Ok(value)
}

pub(super) fn validated_template_runtime(value: &str) -> Result<&'static str, Status> {
    let trimmed = value.trim();
    let runtime = RuntimeKind::parse(trimmed).map_err(|_| unsupported_template_runtime(trimmed))?;
    if SUPPORTED_TEMPLATE_RUNTIMES.contains(&runtime) {
        // The current cluster DB stores release type using the legacy runtime
        // alias (`fc`). Accept full names at the API boundary, then normalize
        // to the stored value for queries and inserts.
        Ok(runtime.alias())
    } else {
        Err(unsupported_template_runtime(trimmed))
    }
}

pub(super) fn unsupported_template_runtime(value: &str) -> Status {
    let expected = SUPPORTED_TEMPLATE_RUNTIMES
        .iter()
        .map(|runtime| format!("{} ({})", runtime.as_str(), runtime.alias()))
        .collect::<Vec<_>>()
        .join(", ");
    Status::invalid_argument(format!(
        "unsupported runtime '{value}'; expected one of [{expected}]"
    ))
}

/// Parses `hash` into a canonical
/// [`pplx_space_common_core::types::ContentHash`]
/// (`<algorithm>:<64 lowercase hex>`), tolerating a bare 64-hex digest by
/// assuming SHA-256, and returns the canonical algorithm-prefixed string.
/// Validating at insertion time means every consumer downstream —
/// template-manager, builders, snapshot reconcilers — can trust the
/// column's shape without re-checking; accepting a bare hash on the read
/// path lets a digest stored bare round-trip back through prefetch.
pub(super) fn validated_release_hash(value: &str) -> Result<String, Status> {
    let trimmed = trimmed_required(value, "hash")?;
    let Ok(content_hash) = trimmed.parse::<pplx_space_common_core::types::ContentHash>() else {
        return Err(Status::invalid_argument(format!(
            "hash '{trimmed}' must be formatted as 'sha256:<64 lowercase hex chars>' or 'blake3:<64 lowercase hex chars>'"
        )));
    };
    // Canonicalize the algorithm prefix so writes store one shape
    // and the by-hash lookup tolerates a bare hash that round-trips back
    // through prefetch from the `template_releases.hash` column.
    Ok(content_hash.to_string())
}

/// Validate every location before opening the registration transaction, preserving
/// its exact wire representation for credential scope matching.
pub(super) fn validated_storage_connection_strings(
    values: Vec<String>,
    trusted: &super::trusted_storage::TrustedTemplateStorageBuckets,
) -> Result<Vec<pplx_space_common_core::types::ExactStorageUri>, Status> {
    dedupe_nonempty_strings(values, "storage_connection_string")?
        .into_iter()
        .map(|value| {
            // Connection strings and parser errors can contain endpoint credentials.
            let uri = pplx_space_common_core::types::ExactStorageUri::parse(&value)
                .map_err(|_| Status::invalid_argument("invalid template storage URI"))?;
            trusted
                .check(uri.parsed())
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            Ok(uri)
        })
        .collect()
}

/// Validates and re-serializes the registration's execution metadata into the
/// JSONB shape stored on `template_releases.execution_metadata` (the same
/// serialization the node publish path produces, so both writers agree).
pub(super) fn validated_execution_metadata(
    execution: Option<&pplx_space_execution_common::proto::ExecutionMetadata>,
) -> Result<Option<serde_json::Value>, Status> {
    let Some(execution) = execution else {
        return Ok(None);
    };
    let execution = pplx_space_common_core::types::ExecutionMetadata::try_from(execution)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    serde_json::to_value(&execution)
        .map(Some)
        .map_err(|error| Status::internal(format!("serialize execution metadata: {error}")))
}

pub(super) async fn ensure_template_release(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    template_name: &str,
    runtime: &str,
    hash: &str,
    org_id: Option<OrgId>,
    execution_metadata: Option<&serde_json::Value>,
) -> Result<TemplateReleaseRow, Status> {
    if let Some(existing) = find_template_release_row(tx, template_name, hash).await? {
        if existing.release_type != runtime {
            return Err(Status::already_exists(format!(
                "release '{template_name}@{hash}' already exists with runtime '{}' instead of '{runtime}'",
                existing.release_type,
            )));
        }
        if existing.org_id != org_id {
            return Err(Status::permission_denied(
                "release ownership does not match template ownership",
            ));
        }
        if let Some(metadata) = execution_metadata {
            check_template_release_execution_metadata(tx, existing.id, metadata).await?;
        }
        return Ok(existing);
    }

    if let Some(inserted) = super::template_releases_store::insert_release(
        &mut **tx,
        template_name,
        runtime,
        hash,
        org_id,
        execution_metadata,
    )
    .await
    .map_err(|error| {
        crate::template_publication::storage_status("insert template release", &error)
    })? {
        return Ok(inserted);
    }

    let existing = find_template_release_row(tx, template_name, hash)
        .await?
        .ok_or_else(|| {
            Status::internal(format!(
                "release '{template_name}@{hash}' conflicted on insert but could not be reloaded",
            ))
        })?;
    if existing.release_type != runtime {
        return Err(Status::already_exists(format!(
            "release '{template_name}@{hash}' already exists with runtime '{}' instead of '{runtime}'",
            existing.release_type,
        )));
    }
    if existing.org_id != org_id {
        return Err(Status::permission_denied(
            "release ownership does not match template ownership",
        ));
    }
    if let Some(metadata) = execution_metadata {
        check_template_release_execution_metadata(tx, existing.id, metadata).await?;
    }
    Ok(existing)
}

/// Find the release for `(template_name, hash)` under either spelling of the
/// hash. `hash` arrives canonical (`sha256:<hex>`); releases registered before
/// the prefix was required are stored bare, and a republication of one of
/// those must attach its tags to that row rather than create a second,
/// artifact-less release beside it.
///
/// Where both spellings already exist, the row that owns a recorded storage
/// location wins regardless of age: tags attached to an artifact-less
/// duplicate would resolve to a release nothing can pull. Age breaks ties.
async fn find_template_release_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    template_name: &str,
    hash: &str,
) -> Result<Option<TemplateReleaseRow>, Status> {
    let bare_hash = hash.strip_prefix("sha256:").unwrap_or(hash);
    sqlx::query_as::<_, TemplateReleaseRow>(sql!(
        "SELECT r.id, r.template_name, r.type, r.hash, r.org_id, r.created_at
         FROM template_releases r
         WHERE r.template_name = $1
           AND r.hash IN ($2, $3)
         ORDER BY EXISTS (
                      SELECT 1
                      FROM template_release_storage_locations l
                      WHERE l.template_release_id = r.id) DESC,
                  r.created_at ASC
         LIMIT 1"
    ))
    .bind(template_name)
    .bind(hash)
    .bind(bare_hash)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|err| crate::template_publication::storage_status("query template_release", &err))
}

/// Release identity is immutable. A row without evidence may be filled once;
/// an identical retry is read-only, and contradictory evidence is never acknowledged.
async fn check_template_release_execution_metadata(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    release_id: Uuid,
    execution: &serde_json::Value,
) -> Result<(), Status> {
    // Fence the check and backfill against node publication and other writers,
    // including those that do not take the parent-template publication lock.
    let existing: Option<serde_json::Value> = sqlx::query_scalar(sql!(
        "SELECT execution_metadata FROM template_releases WHERE id = $1 FOR UPDATE"
    ))
    .bind(release_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| {
        crate::template_publication::storage_status("lock release execution metadata", &error)
    })?;
    match existing {
        Some(existing) if &existing != execution => Err(Status::already_exists(
            "template release already has different execution metadata",
        )),
        Some(_) => Ok(()),
        None => super::template_releases_store::fill_execution_metadata(
            &mut **tx, release_id, execution,
        )
        .await
        .map_err(|error| {
            crate::template_publication::storage_status("fill release execution metadata", &error)
        }),
    }
}

pub(super) async fn ensure_template_release_tag(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    release: &TemplateReleaseRow,
    tag: &str,
) -> Result<TemplateReleaseTagRow, Status> {
    sqlx::query_as::<_, TemplateReleaseTagRow>(sql!(
        "INSERT INTO template_release_tags (template_release_id, template_name, tag)
         VALUES ($1, $2, $3)
         ON CONFLICT (template_name, tag) DO UPDATE
             SET template_release_id = EXCLUDED.template_release_id,
                 modified_at = NOW()
         RETURNING id, tag, created_at"
    ))
    .bind(release.id)
    .bind(&release.template_name)
    .bind(tag)
    .fetch_one(&mut **tx)
    .await
    .map_err(|err| crate::template_publication::storage_status("insert tag", &err))
}

pub(super) async fn ensure_template_release_storage_location(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    release: &TemplateReleaseRow,
    connection_string: &str,
) -> Result<TemplateReleaseStorageLocationRow, Status> {
    if let Some(existing) =
        find_template_release_storage_location_row(tx, release.id, connection_string).await?
    {
        adopt_template_release_storage_location_org_id(tx, existing.id).await?;
        return Ok(existing);
    }

    if let Some(inserted) = insert_template_release_storage_location_row(
        tx,
        release.id,
        release.org_id,
        connection_string,
    )
    .await?
    {
        return Ok(inserted);
    }

    find_template_release_storage_location_row(tx, release.id, connection_string)
        .await?
        .ok_or_else(|| {
            Status::internal(
                "storage_connection_string conflicted on insert but could not be reloaded",
            )
        })
}

async fn find_template_release_storage_location_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    release_id: Uuid,
    connection_string: &str,
) -> Result<Option<TemplateReleaseStorageLocationRow>, Status> {
    sqlx::query_as::<_, TemplateReleaseStorageLocationRow>(sql!(
        "SELECT id, storage_connection_string, created_at
         FROM template_release_storage_locations
         WHERE template_release_id = $1
           AND storage_connection_string = $2"
    ))
    .bind(release_id)
    .bind(connection_string)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|err| {
        crate::template_publication::storage_status("query storage_connection_string", &err)
    })
}

async fn insert_template_release_storage_location_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    release_id: Uuid,
    org_id: Option<OrgId>,
    connection_string: &str,
) -> Result<Option<TemplateReleaseStorageLocationRow>, Status> {
    sqlx::query_as::<_, TemplateReleaseStorageLocationRow>(sql!(
        "INSERT INTO template_release_storage_locations
             (template_release_id, storage_connection_string, org_id)
         SELECT $1, $3, COALESCE($2, org_id)
         FROM template_releases
         WHERE id = $1
         ON CONFLICT (template_release_id, storage_connection_string) DO UPDATE
             SET org_id = COALESCE(
                 template_release_storage_locations.org_id,
                 EXCLUDED.org_id
             )
         RETURNING id, storage_connection_string, created_at"
    ))
    .bind(release_id)
    .bind(org_id)
    .bind(connection_string)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|err| {
        crate::template_publication::storage_status("insert storage_connection_string", &err)
    })
}

async fn adopt_template_release_storage_location_org_id(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    location_id: Uuid,
) -> Result<(), Status> {
    sqlx::query(sql!(
        "UPDATE template_release_storage_locations location
         SET org_id = release.org_id
         FROM template_releases release
         WHERE location.id = $1
           AND release.id = location.template_release_id
           AND location.org_id IS NULL"
    ))
    .bind(location_id)
    .execute(&mut **tx)
    .await
    .map_err(|err| {
        crate::template_publication::storage_status(
            "adopt template_release_storage_location org_id",
            &err,
        )
    })?;
    Ok(())
}

pub(super) fn template_proto(row: TemplateRow) -> Template {
    Template {
        name: row.name,
        description: row.description,
        created_at: format_time(row.created_at),
        network_policy: Some(common_network_policy_to_proto(&row.network_policy_json.0)),
        network_policy_generation: db_generation_to_proto(row.network_policy_generation),
    }
}

pub(super) fn template_release_proto(row: TemplateReleaseRow) -> TemplateRelease {
    TemplateRelease {
        id: row.id.to_string(),
        template_name: row.template_name,
        runtime: row.release_type,
        hash: row.hash,
        created_at: format_time(row.created_at),
    }
}

pub(super) fn template_release_tag_proto(row: TemplateReleaseTagRow) -> TemplateReleaseTag {
    TemplateReleaseTag {
        id: row.id.to_string(),
        tag: row.tag,
        created_at: format_time(row.created_at),
    }
}

pub(super) fn template_release_storage_location_proto(
    row: TemplateReleaseStorageLocationRow,
) -> TemplateReleaseStorageLocation {
    TemplateReleaseStorageLocation {
        id: row.id.to_string(),
        connection_string: row.storage_connection_string,
        created_at: format_time(row.created_at),
    }
}

#[cfg(test)]
mod policy_generation_tests;
