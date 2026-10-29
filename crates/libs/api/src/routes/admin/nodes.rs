use pplx_space_common_core::types::NodeId;

use axum::{
    Extension, Json,
    body::Bytes,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use pplx_space_common_core::types::admin::{
    AdminExecutionInventoryObservation, AdminNodeDrainability, AdminNodeHealthStatus,
    AdminNodeNetworkSlots, AdminNodeResponse, AdminNodeSchedulingStatus, AdminNodeSort,
    AdminNodeStatusSnapshot, DrainRequest, ForceUploadFailure, ForceUploadLocalSnapshotsResponse,
    GetAdminNodeFilters, ListAdminNodesFilters,
};
use pplx_space_common_core::types::{
    NodeDrainMode, NodeDrainPhase, NodeDrainState, NodeManagedBy, SandboxId,
};
use pplx_space_common_svc::sql::sandbox_residency::{NODE_CENSUS_RESIDENCY_ENV, ResidencyEvidence};
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::OnceLock;
use std::time::Duration;
use time::OffsetDateTime;

use super::audit::{self, AdminAuditEntry};
use super::node_store::{self, AdminNodeRow, AdminNodeStore, NodeOccupancy, SandboxOccupancy};
use super::support::{
    api_error, db_error, format_optional_time, format_time, require_managing_org_admin,
};
use crate::controllers::substrate::{NodeApiTarget, NodeSchedulability, NodeSchedulabilityRequest};
use crate::middleware::auth::AuthUser;
use crate::models::User;
use crate::routes::AppState;

/// Drain-phase states stored in `nodes.drain_phase`. NULL means the node is
/// in normal placement service (no drain in progress).
const PHASE_DRAINING: &str = "draining";
const PHASE_DRAINED: &str = "drained";
const PHASE_TERMINATING: &str = "terminating";

/// The 501 body's `error` for a node drain or cordon action on a substrate
/// without [`SubstrateCapabilities::supports_node_drain`]. Only Direct lacks
/// it.
///
/// [`SubstrateCapabilities::supports_node_drain`]: crate::controllers::substrate::SubstrateCapabilities::supports_node_drain
const NODE_DRAIN_UNSUPPORTED: &str =
    "node drain and cordon are not supported on the Direct substrate";

#[derive(Debug, Clone, Copy)]
enum DrainAction {
    Drain,
    Terminate,
    Undrain,
    Cordon,
    Uncordon,
    /// Operator escape hatch: enumerate suspended sandboxes whose
    /// `target_node_id` still pins them to this node and whose
    /// snapshot has not yet been registered to S3, then trigger an
    /// on-node sbx-mgr reupload for each. Once every pinned sandbox
    /// has a registered S3 snapshot the node's
    /// `sandboxes_local_snapshots` count drops to zero and the
    /// `:terminate` guard stops firing. The node-drain reconciler
    /// runs the same promotion automatically before flipping
    /// `draining → drained`; this endpoint is the immediate trigger
    /// operators can call when the reconciler is disabled or they
    /// want to nudge a specific node.
    ForceUploadLocalSnapshots,
}

impl DrainAction {
    fn label(self) -> &'static str {
        match self {
            Self::Drain => "drain",
            Self::Terminate => "terminate",
            Self::Undrain => "undrain",
            Self::Cordon => "cordon",
            Self::Uncordon => "uncordon",
            Self::ForceUploadLocalSnapshots => "force-upload-local-snapshots",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "drain" => Some(Self::Drain),
            "terminate" => Some(Self::Terminate),
            "undrain" => Some(Self::Undrain),
            "cordon" => Some(Self::Cordon),
            "uncordon" => Some(Self::Uncordon),
            "force-upload-local-snapshots" => Some(Self::ForceUploadLocalSnapshots),
            _ => None,
        }
    }

    /// Whether the action may run on a substrate without node drain. Only
    /// `:undrain` may: it is how an operator clears a `drain_phase` an
    /// earlier drain left, and it needs no substrate call there.
    /// `:force-upload-local-snapshots` is refused with the rest, because its
    /// gate opens only in a drain phase nothing on such a substrate can enter
    /// any more (a leftover one is cleared by `:undrain`), and the
    /// `:terminate` it unblocks is refused too.
    fn runs_without_node_drain(self) -> bool {
        matches!(self, Self::Undrain)
    }
}

/// Parse a `{spec}` segment that carries either a bare UUID (GET form) or a
/// `UUID:action` pair (POST form). Returns `(node_id, Some(action))` for the
/// action form and `(node_id, None)` for the bare form.
fn parse_spec(
    spec: &str,
) -> Result<(NodeId, Option<DrainAction>), (StatusCode, Json<serde_json::Value>)> {
    let (id_part, action_part) = match spec.split_once(':') {
        Some((id, action)) => (id, Some(action)),
        None => (spec, None),
    };
    let node_id = id_part
        .parse::<NodeId>()
        .map_err(|_| api_error(StatusCode::BAD_REQUEST, "node id must be a valid UUID"))?;
    let action = match action_part {
        None => None,
        Some(label) => Some(DrainAction::parse(label).ok_or_else(|| {
            api_error(
                StatusCode::NOT_FOUND,
                format!(
                    "unknown admin action {label:?}; expected one of: drain, terminate, undrain, cordon, uncordon, force-upload-local-snapshots"
                ),
            )
        })?),
    };
    Ok((node_id, action))
}

/// Outcome of validating a requested drain transition. Named for the drain
/// rather than the phase because [`Self::UpdateInFlightDrain`] moves
/// `drain_mode` while leaving `drain_phase` where it is.
#[derive(Debug, Clone, PartialEq, Eq)]
enum DrainTransition {
    /// Move `drain_phase` to `Some(new)` (or NULL when `new` is `None`).
    Move {
        new: Option<&'static str>,
        set_started_at: bool,
        clear_started_at: bool,
    },
    /// `:drain` against a node already `draining`. Moves `drain_mode` and
    /// clears `drain_last_error`, and leaves `drain_phase`,
    /// `drain_started_at` and `terminate_decrement_capacity` alone — a drain's
    /// replacement intent is fixed for its lifetime, and a request that
    /// disagrees with it is refused before reaching the write.
    ///
    /// Carries `Some(mode)` only when the mode actually moves. Still one arm
    /// rather than a separate no-op arm for the mode-unchanged case, because the
    /// mode is not the only thing this edge writes: `drain_last_error` clears on
    /// both, and splitting on the mode would drop that clear from the repeat —
    /// leaving a stale error on a node whose fresh request just re-asserted the
    /// cordon.
    ///
    /// `None` on a re-assert is not just an optimization. `from_column_str`
    /// absorbs a value this build cannot parse into `Gentle`, so writing the
    /// resolved mode back would replace a newer build's `drain_mode` with
    /// `'gentle'` — silently disarming a drain that the pod which wrote it
    /// treats as destructive. Not writing the column preserves it.
    ///
    /// `Some` is still the only way to reach the destructive value, so the
    /// branch is never entered from a defaulted *requested* mode further down.
    ///
    /// The *current* mode is the asymmetry worth naming: an unparseable column
    /// resolves to `Gentle`, so an escalation of such a node does normalize it
    /// to `'forced'`, losing the raw value the same-mode arm preserves. The
    /// audit entry compounds it — `from_drain_mode` carries the *resolved*
    /// `gentle`, so the trail affirmatively reports a prior state the row never
    /// held. Accepted because `forced` is the most destructive mode this build
    /// knows, so normalizing upward cannot disarm anything; a third mode above
    /// `forced` would make it a downgrade and would need this arm to pass `None`
    /// too, and would need the audit to carry the raw column.
    ///
    /// See [`node_store::update_in_flight_drain`] for what this edge must leave
    /// alone and why.
    UpdateInFlightDrain(Option<NodeDrainMode>),
    /// No drain change (the cordon-only paths).
    NoChange,
}

/// Parse the optional JSON body that accompanies `POST :drain`. An empty body
/// (no `Content-Length` or zero bytes) is treated as `DrainRequest::default()`:
/// mode `gentle`, so a bodiless `:drain` cordons and waits for residents to
/// leave rather than evacuating them, and `replace_after_terminate: None` — no
/// expressed opinion, which the edge that *starts* a drain resolves to
/// auto-replace and the in-flight edges take as "leave the stored value alone".
/// Non-empty bodies must be valid JSON for `DrainRequest`.
fn parse_drain_request(body: &[u8]) -> Result<DrainRequest, (StatusCode, Json<serde_json::Value>)> {
    if body.is_empty() {
        return Ok(DrainRequest::default());
    }
    serde_json::from_slice::<DrainRequest>(body).map_err(|err| {
        api_error(
            StatusCode::BAD_REQUEST,
            format!("invalid drain request body: {err}"),
        )
    })
}

#[derive(Debug)]
enum TransitionError {
    /// Current phase does not allow the requested action.
    Conflict {
        current: Option<String>,
        allowed: &'static [&'static str],
    },
    /// `:drain` asked to move an in-flight drain to a *less* aggressive mode.
    /// Separate from [`Self::Conflict`] because the phase is not the problem —
    /// `draining` does accept `:drain` now — and an `allowed_from` phase list
    /// would tell the operator nothing about why their call was refused.
    ///
    /// Carries only the current mode; the requested one is the handler's own
    /// `requested_mode`, and with two modes a downgrade request is always
    /// `Gentle` anyway.
    ModeDowngrade { current_mode: NodeDrainMode },
}

/// Whether a `:drain` carrying `require_undrained` must be refused: the node
/// already has a drain phase, so the request would escalate or repeat a drain
/// it did not start. Only `:drain` parses a body, so every other action
/// arrives with the field `false`; the action check states that rather than
/// relying on it.
fn refuses_existing_drain(
    action: DrainAction,
    require_undrained: bool,
    current_phase: Option<&str>,
) -> bool {
    matches!(action, DrainAction::Drain) && require_undrained && current_phase.is_some()
}

/// Pure state-machine validator. Kept separate so the table is testable
/// without a Postgres or substrate round trip.
///
/// This module owns the operator-facing transitions only:
///
/// | Edge | Driver |
/// |---|---|
/// | `NULL → draining` | `:drain` |
/// | `draining → draining` (mode only) | `:drain`; see the arm for the mode matrix |
/// | `drained → terminating` | `:terminate` |
/// | `draining → NULL` / `drained → NULL` | `:undrain` |
///
/// The complementary edges live in the sibling cluster-controller code, not
/// in this request path:
///
/// - `draining → drained` — `node_drain_reconciler` when the per-node
///   sandbox count reaches zero
/// - `drained → terminating` — `drained_node_terminator` periodic loop
///   (auto-terminates drained nodes unless the node carries the
///   `space.pplx.net/drain-keep` annotation in the substrate)
/// - `terminating → row deleted` — `node_drain_reconciler` after the ASG
///   `TerminateInstanceInAutoScalingGroup` call succeeds; the row is
///   removed from `nodes`, **no `deleted` phase exists**. The migration
///   constraint allows only `'draining' | 'drained' | 'terminating'`
///
/// Inlining the count-driven advance here would duplicate the node-drain
/// reconciler, miss non-API drain paths, and
/// embed polling in the request path.
///
/// `current_mode` is the locked row's `drain_mode` already resolved through
/// [`NodeDrainMode::from_column_str`], so NULL and unrecognized both arrive as
/// `Gentle` exactly as they do for the reconciler and the user-API gates. It is
/// only read on the `draining` arm of `:drain`; on every other edge the node
/// either has no drain or the mode is not part of the decision.
fn validate_drain_transition(
    current: Option<&str>,
    current_mode: NodeDrainMode,
    action: DrainAction,
    requested_mode: Option<NodeDrainMode>,
) -> Result<DrainTransition, TransitionError> {
    match action {
        DrainAction::Drain => match current {
            None => Ok(DrainTransition::Move {
                new: Some(PHASE_DRAINING),
                set_started_at: true,
                clear_started_at: false,
            }),
            // `:drain` on a node already draining is the in-place escalation
            // verb. It exists because the alternative an operator had —
            // `:undrain` then `:drain --forced` — uncordons in the same
            // transaction that clears the phase, so between the two calls fresh
            // placements land on the node being emptied, and the new drain
            // starts its age over.
            //
            // The mode matrix, not the phase, decides:
            //
            // - `gentle → forced` escalates in place. The node is already
            //   cordoned, so nothing about the drain restarts; the reconciler
            //   and the user-API gates both re-read `drain_mode` and switch to
            //   evacuating on the next tick.
            // - `forced → gentle` is refused. Evacuation is already in flight
            //   and suspends and cross-node restores cannot be un-run, so a
            //   downgrade would park a half-evacuated node in a state neither
            //   mode describes. Aborting outright is `:undrain`.
            // - Same mode leaves the persisted mode where it is, and is not an
            //   error: an operator re-running
            //   `space-admin-cli drain-node --forced` after a timeout should find
            //   the node in the state they asked for.
            //   (Automation that sets `require_undrained` gets a 409 instead,
            //   before this matrix runs.) It still
            //   re-patches the substrate cordon, a request-path repair for a
            //   diverged one. The reconciler re-asserts cordon every tick for a
            //   `draining` node too, so that patch is redundant whenever the
            //   reconciler is enabled; kept because it is the only repair
            //   available when it is not, and because a 200 that silently did
            //   less than `:cordon` would be the surprising shape.
            //
            //   Not write-free, unlike `:cordon`: it takes the same store call
            //   as the escalation, so `drain_last_error` clears. Nothing else
            //   moves — see `node_store::update_in_flight_drain`.
            Some(PHASE_DRAINING) => match (current_mode, requested_mode.unwrap_or_default()) {
                (NodeDrainMode::Gentle, NodeDrainMode::Forced) => Ok(
                    DrainTransition::UpdateInFlightDrain(Some(NodeDrainMode::Forced)),
                ),
                // Reached by a request that omitted `mode` as well as by one
                // that asked for `gentle`, and deliberately so: unlike
                // `replace_after_terminate`, a missing mode is not an
                // unexpressed opinion — `NodeDrainMode`'s type docs make
                // "missing, NULL or unrecognized ⇒ gentle" one uniform rule, so
                // that `forced` is reachable only through an explicit token. A
                // bodiless `:drain` is therefore a gentle *request*, and
                // refusing it against a forced drain is the same answer as
                // refusing an explicit one. Making `mode` an `Option` to soften
                // this would put a second rule beside that safety property.
                (NodeDrainMode::Forced, NodeDrainMode::Gentle) => {
                    Err(TransitionError::ModeDowngrade { current_mode })
                }
                // Same mode: the persisted mode does not move — `None`, so the
                // raw column survives even when this build could not parse it.
                // Same arm as the escalation because the write is still worth
                // making for the `drain_last_error` clear.
                (NodeDrainMode::Gentle, NodeDrainMode::Gentle)
                | (NodeDrainMode::Forced, NodeDrainMode::Forced) => {
                    Ok(DrainTransition::UpdateInFlightDrain(None))
                }
            },
            // `drained` and `terminating` are past the point where a mode
            // means anything: the node is empty and on its way to being
            // destroyed. Re-draining it is still a conflict.
            //
            // This arm also catches a `drain_phase` string this build does not
            // know, which `nodes_drain_phase_check` currently makes unwritable
            // (`drain_phase IS NULL OR drain_phase IN ('draining', 'drained',
            // 'terminating')`). It is reachable only after a migration widens
            // that constraint and a newer pod writes the new value while this
            // one is still serving — so it is a version-skew guard, not a live
            // case, and refusing is right for it: an unclassifiable phase is
            // the one state where this handler cannot know what it would be
            // overwriting.
            _ => Err(TransitionError::Conflict {
                current: current.map(str::to_string),
                allowed: &["<none>", PHASE_DRAINING],
            }),
        },
        // `drained` is written by the node-drain reconciler when the
        // sandboxes-on-node count drops to zero; this handler only accepts
        // the transition out of `drained` into `terminating`.
        DrainAction::Terminate => match current {
            Some(PHASE_DRAINED) => Ok(DrainTransition::Move {
                new: Some(PHASE_TERMINATING),
                set_started_at: false,
                clear_started_at: false,
            }),
            _ => Err(TransitionError::Conflict {
                current: current.map(str::to_string),
                allowed: &[PHASE_DRAINED],
            }),
        },
        DrainAction::Undrain => match current {
            Some(PHASE_DRAINING | PHASE_DRAINED) => Ok(DrainTransition::Move {
                new: None,
                set_started_at: false,
                clear_started_at: true,
            }),
            _ => Err(TransitionError::Conflict {
                current: current.map(str::to_string),
                allowed: &[PHASE_DRAINING, PHASE_DRAINED],
            }),
        },
        // Plain `:cordon` is always safe — it either re-asserts the cordon a
        // drain has already applied (idempotent), or applies a fresh cordon on
        // a non-draining node.
        DrainAction::Cordon => Ok(DrainTransition::NoChange),
        // `:force-upload-local-snapshots` is a pure remediation action — it
        // does not move the drain state machine. The action handler runs
        // outside of `apply_drain_action`; this arm only exists so the
        // transition validator's domain stays exhaustive over `DrainAction`.
        DrainAction::ForceUploadLocalSnapshots => Ok(DrainTransition::NoChange),
        // `:uncordon` must refuse to run while a drain is in flight: an
        // uncordon would re-enable scheduling on the substrate side while
        // leaving `drain_phase` set, so the scheduler and the admin API
        // would disagree about whether the node is taking new placements.
        // Operators who want to abort a drain must call `:undrain`, which
        // clears the phase *and* uncordons in a single transaction.
        DrainAction::Uncordon => match current {
            None => Ok(DrainTransition::NoChange),
            Some(_) => Err(TransitionError::Conflict {
                current: current.map(str::to_string),
                allowed: &["<none> (use :undrain to abort an active drain)"],
            }),
        },
    }
}

fn admin_node_status(row: &AdminNodeRow) -> Option<AdminNodeStatusSnapshot> {
    let observed_at = row.status_observed_at?;
    let stale_after = row.status_stale_after?;
    let scheduling = row
        .scheduling_status
        .as_ref()
        .map(|status| AdminNodeSchedulingStatus {
            status: status.clone(),
            reason: row.scheduling_reason.clone(),
            network_slots_ready: row.network_slots_ready,
            network_pool_min_ready: row.network_pool_min_ready,
            network_slots_degraded: row.network_slots_degraded,
            network_slots_quarantined: row.network_slots_quarantined,
            network_slots_total: row.network_slots_total,
            network_slots_leased: row.network_slots_leased,
            network_pool_max_slots: row.network_pool_max_slots,
        });

    Some(AdminNodeStatusSnapshot {
        execution_inventory: row
            .execution_inventory_status
            .as_ref()
            .zip(row.execution_inventory_received_at)
            .map(|(status, received_at)| {
                let (policy, policy_received_at) = match row.execution_policy() {
                    Some((policy, received_at)) => (Some(policy), received_at.map(format_time)),
                    None => (None, None),
                };
                AdminExecutionInventoryObservation {
                    policy,
                    policy_received_at,
                    status: status.clone(),
                    received_at: format_time(received_at),
                }
            }),
        observed_at: format_time(observed_at),
        stale_after: format_time(stale_after),
        stale: stale_after <= OffsetDateTime::now_utc(),
        reachable: row.status_reachable.unwrap_or(false),
        error: row.status_error.clone(),
        role: row.status_role.clone(),
        node_id_reported: row.status_node_id_reported.clone(),
        scheduling,
        health: Some(AdminNodeHealthStatus {
            reachable: row.status_reachable.unwrap_or(false),
            healthy: row.status_health_healthy.unwrap_or(false),
            error: row.status_error.clone(),
            services: row
                .status_services
                .as_ref()
                .map(|services| services.0.clone())
                .unwrap_or_default(),
            node_ctl: row
                .status_node_ctl
                .as_ref()
                .map(|node_ctl| node_ctl.0.clone()),
        }),
    })
}

/// `drainability` is `None` on every path that did not compute the
/// live-resident counts — inventory reads, action responses and unsorted listings.
/// It is not defaulted to zeros: an all-zero block is the signal that a node is
/// a prime gentle-drain candidate, so manufacturing one would be a lie.
fn admin_node_response(
    row: AdminNodeRow,
    occupancy: Option<SandboxOccupancy>,
    drainability: Option<(AdminNodeDrainability, ResidencyEvidence)>,
) -> AdminNodeResponse {
    // Split late and together, so a response can never carry counts without the
    // policy that produced them, or vice versa.
    let (drainability, residency_evidence) = match drainability {
        Some((block, residency)) => (Some(block), Some(residency.to_string())),
        None => (None, None),
    };
    let display_name = row.name.clone().or_else(|| row.substrate_node_id.clone());
    let status = admin_node_status(&row);
    AdminNodeResponse {
        id: row.id,
        name: display_name,
        address: row.address,
        node_api_endpoint: row.node_api_endpoint,
        role: row.role,
        managed_by: row.managed_by,
        state: row.state,
        scheduling_status: row.scheduling_status,
        scheduling_reason: row.scheduling_reason,
        drain_phase: row.drain_phase,
        // NULL stays `None` — "no drain, so no mode" is worth distinguishing
        // from "gentle" on the operator surface. A present-but-unrecognized
        // value resolves to gentle (and logs a warn) rather than failing the
        // listing, matching what the reconciler and the drain gates will do
        // with that row.
        drain_mode: row
            .drain_mode
            .as_deref()
            .map(|raw| NodeDrainMode::from_column_str(Some(raw))),
        drain_started_at: format_optional_time(row.drain_started_at),
        drain_last_error: row.drain_last_error,
        terminate_decrement_capacity: row.terminate_decrement_capacity,
        k8s_node_name: row.substrate_node_id,
        metadata: row
            .metadata
            .map(|m| m.0)
            .unwrap_or_else(|| serde_json::json!({})),
        last_heartbeat: format_optional_time(row.last_heartbeat.or(row.status_observed_at)),
        created_at: format_time(row.created_at),
        updated_at: format_optional_time(row.updated_at),
        deleted_at: format_optional_time(row.deleted_at),
        sandboxes_active: occupancy.map(|counts| counts.active),
        sandboxes_local_snapshots: occupancy.map(|counts| counts.local_snapshots),
        drainability,
        residency_evidence,
        network_slots: AdminNodeNetworkSlots {
            ready: row.network_slots_ready,
            min_ready: row.network_pool_min_ready,
            degraded: row.network_slots_degraded,
            quarantined: row.network_slots_quarantined,
            total: row.network_slots_total,
            leased: row.network_slots_leased,
            max_slots: row.network_pool_max_slots,
        },
        status,
    }
}

pub async fn list_admin_nodes(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Query(filters): Query<ListAdminNodesFilters>,
) -> impl IntoResponse {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    // Both parsed before any query runs, so a rejected parameter costs the
    // caller a 400 rather than a listing it then has to discard.
    let sort = match parse_sort(filters.sort.as_deref()) {
        Ok(sort) => sort,
        Err(err) => return err.into_response(),
    };
    let long_runner_age = match parse_long_runner_age(filters.long_runner_age_hours) {
        Ok(age) => age,
        Err(err) => return err.into_response(),
    };
    // Postgres-only listing for now. Freshly-joined substrate-known nodes that
    // no sandbox has been scheduled to yet are not visible until their first
    // nodes-table upsert. Mirroring substrate inventory here means joining it
    // against drain columns held only in Postgres and ensuring action handlers
    // upsert before locking; deferred until a follow-up.
    let store = AdminNodeStore::new(state.pool.clone());
    let nodes = match store.list(&filters).await {
        Ok(rows) => rows,
        Err(err) => return db_error(err).into_response(),
    };
    if filters.include_occupancy == Some(false) && !sort.needs_drainability() {
        return Json(
            nodes
                .into_iter()
                .map(|node| admin_node_response(node, None, None))
                .collect::<Vec<_>>(),
        )
        .into_response();
    }
    let node_ids: Vec<NodeId> = nodes.iter().map(|n| n.id).collect();
    // The census probes are only paid for when the caller is ranking by them:
    // they walk every resident row of every listed node, which is more than the
    // default "what's draining right now?" listing needs. Asking for them here
    // rather than in a second query is what keeps `live_residents` and `active`
    // on one database snapshot, so their difference is a real quantity.
    let ranking = sort.needs_drainability().then_some(long_runner_age);
    let residency = cluster_residency_evidence();
    let occupancy_by_id = match store
        .occupancy_for_nodes(&node_ids, ranking, residency)
        .await
    {
        Ok(map) => map,
        Err(err) => return db_error(err).into_response(),
    };
    let mut responses: Vec<_> = nodes
        .into_iter()
        .map(|node| {
            // The store reports every requested node, zero-filling the
            // counts, so this fallback is unreachable for a node in
            // `node_ids` — which is what keeps a zero-resident node's real
            // heartbeat in the ranking. It stays as a total-function default
            // rather than an `expect`: the fail-closed `live: None` +
            // `heartbeat_fresh: false` it yields sorts a node last, so a future
            // scope/key mismatch would under-offer a candidate instead of
            // panicking a listing.
            let node_occupancy = occupancy_by_id.get(&node.id).copied().unwrap_or_default();
            let occupancy = node_occupancy.occupancy;
            let drainability =
                ranking.map(|age| drainability_block(node_occupancy, residency, age));
            admin_node_response(node, Some(occupancy), drainability)
        })
        .collect();
    // Stable sort, so nodes that tie on the whole key keep the SQL's
    // (role, name) order and successive calls agree.
    if ranking.is_some() {
        responses.sort_by_key(drainability_rank);
    }
    Json(responses).into_response()
}

/// Ranking key, in the order an operator should care about it. Every term is
/// also a visible column, so the ranking can be read off the table.
///
/// There is no role term, deliberately — this ranks whatever the caller asked
/// for. That makes a `role` filter part of using it: builder and cluster nodes
/// host no sandboxes, so they win every count and lead an unfiltered listing.
///
/// 1. **Blind nodes last.** A node the control plane cannot currently see is
///    not a drain candidate at all: nothing will evacuate off it, because the
///    evacuation RPCs go *to* it. It also has the least trustworthy counts —
///    every resident row keeps counting precisely because absence cannot be
///    proven — so a blind node with few rows would otherwise sort ahead of a
///    healthy node with many.
/// 2. **Fewest live long-runners.** One live sandbox that has sat in its
///    current lifecycle past the age threshold is enough to make a gentle drain
///    wait on a human, so this dominates the remaining counts. It is a strong
///    signal, not a proof: the threshold measures time since the last lifecycle
///    change, so it under-reports a long-lived sandbox that keeps
///    pausing/resuming (see [`AdminNodeDrainability::live_long_runners`]).
///    Ordering by the count rather than a zero/non-zero flag keeps the
///    partition and additionally orders the nodes only a forced drain clears.
/// 3. **Fewest live residents** — the rows a drain actually waits on.
/// 4. **Fewest drain-gate blockers** — the reconciler's own
///    `draining → drained` count. It **overlaps** the residency counts rather
///    than nesting either way. A `suspending` row still carries its
///    `active_node_id` (that column is cleared at suspend *completion*, not
///    entry) and `suspending` is a resident lifecycle, so such a row is counted
///    here *and* in `live_residents`. Conversely a `suspended`-not-durable row,
///    or a non-evacuation `restoring` one, pins only through `target_node_id`
///    with `active_node_id` NULL — so a node can hold a blocked gate while
///    every residency count reads zero. Ranking on this supersedes ranking on
///    local-only snapshots, which these blockers already include.
/// 5. **Fewest terminate blockers.** The tiebreak that keeps a node which
///    cannot *finish* from looking ideal — the occupancy scope with the same
///    census discount the drained-node terminator applies. It keeps `stopped` /
///    `created` / `deleting` / `quarantined` rows, which the census reports and
///    the drain
///    pipeline ignores, and drops phantoms, which the terminator discounts.
///    Ranking on `live_residents` alone would promote exactly the nodes that
///    park in `drained`; ranking on the raw occupancy count would instead prefer
///    a node holding one reported `stopped` row over one holding fifty
///    phantoms, which is backwards.
///
/// Nodes without a computed block sort as all-zero; the caller only sorts when
/// it computed every block, so that case does not arise in practice.
fn drainability_rank(node: &AdminNodeResponse) -> (bool, i64, i64, i64, i64) {
    let d = node.drainability.unwrap_or_default();
    (
        // `Default` yields `heartbeat_fresh: false`, so a node with no computed
        // block sorts as blind rather than as a perfect candidate.
        !d.heartbeat_fresh,
        d.live_long_runners,
        d.live_residents,
        d.drain_blockers,
        d.terminate_blockers,
    )
}

/// Returns the block together with the policy the row was *computed* under,
/// rather than the policy the handler asked for. The two are the same today,
/// and reading it back off the row is what keeps them the same if that ever
/// stops being true — the response then cannot describe counts by a mode that
/// did not produce them. `requested` is the fallback for a row that carries no
/// counters at all.
fn drainability_block(
    row: NodeOccupancy,
    requested: ResidencyEvidence,
    requested_age: Duration,
) -> (AdminNodeDrainability, ResidencyEvidence) {
    let live = row.live.unwrap_or_default();
    let residency = row.live.map_or(requested, |live| live.residency);
    // Same fallback shape as `residency`, and for the same reason: a row with no
    // counters would otherwise report `Duration::ZERO`, i.e. a threshold of 0
    // hours — a value `parse_long_runner_age` rejects with a 400, so the
    // response would carry something the request layer refuses to accept.
    let age = row.live.map_or(requested_age, |live| live.long_runner_age);
    let block = AdminNodeDrainability {
        live_residents: live.live_residents,
        live_long_runners: live.live_long_runners,
        long_runner_age_hours: long_runner_age_hours(age),
        local_snapshots: row.occupancy.local_snapshots,
        // Fail-closed, and this `&&` is what makes it so rather than a comment
        // claiming it. Zeroed counts beside a *true* heartbeat is the best
        // possible rank key, so a row that carries no counters must not keep its
        // real visibility — it would sort first on the strength of counts that
        // were never computed. `Some(age) ⇒ Some(live)` makes the combination
        // unreachable today; the guard is here so the first partial-computation
        // path cannot invert the ranking's dominant term.
        heartbeat_fresh: row.heartbeat_fresh && row.live.is_some(),
        drain_blockers: live.drain_blockers,
        terminate_blockers: live.terminate_blockers,
    };
    (block, residency)
}

/// The store's effective age back on the wire's `u32` hours. Saturating rather
/// than wrapping: the store has already clamped to 100 years, which fits, so the
/// saturation is unreachable — but a wrong answer here would misreport the
/// threshold the counts were computed against, which is the one thing this field
/// exists to prevent.
fn long_runner_age_hours(age: Duration) -> u32 {
    u32::try_from(age.as_secs() / 3600).unwrap_or(u32::MAX)
}

fn parse_sort(raw: Option<&str>) -> Result<AdminNodeSort, (StatusCode, Json<serde_json::Value>)> {
    match raw {
        None => Ok(AdminNodeSort::default()),
        Some(value) => AdminNodeSort::from_str(value)
            .map_err(|err| api_error(StatusCode::BAD_REQUEST, err.to_string())),
    }
}

/// The cluster's residency policy, read once per process from the same env var
/// the cluster-controller reads and parsed by the same shared function.
///
/// Read once rather than per request for the reason the shared const documents:
/// the drain gate and the terminator gate evaluate this at consecutive gates, so
/// a value that could change between two reads is a value that could rank a node
/// under one policy and drain it under another.
///
/// Three consumers now, not two: this endpoint's `:terminate` guard evaluates
/// residency as well, and it is the one that *refuses*. A guard reading a
/// different policy than the drain gate that released the node is exactly the
/// wedge the drain/terminator gate agreement closes, one layer out — so it reads
/// here rather than growing a
/// second reader of its own. Unset or unrecognized falls back to
/// [`ResidencyEvidence::LifecycleOnly`], the count-everything mode, so a typo
/// makes that guard refuse more, never less.
///
/// This is a *second* deployment reading the same variable, and the chart
/// renders both containers' value from the cluster-controller's own
/// `reconcilers.nodeDrain.env` key — `required`, so a dropped key fails the
/// render rather than reaching a process as unset. A values file therefore
/// cannot put the ranking and the gates on different policies.
///
/// What it cannot rule out: the two pods restart independently, so a window
/// where they disagree survives any values change, and a hand-patched
/// deployment can still diverge. The response reports the mode it actually
/// used, which is what makes either case visible rather than silent.
///
/// Deriving the mode instead of reading it was the worse option: with the
/// discount defaulting off, a ranking that discounted unconditionally would
/// contradict the reconciler on every cluster as shipped.
///
/// **Do not set the env var from a test and expect this to see it.** The first
/// caller in a test binary fixes the value for every later test in that process,
/// silently. Tests that need a specific policy pass it explicitly to
/// [`node_store::sandbox_occupancy_for_node_in_tx`] and
/// [`node_store::AdminNodeStore::occupancy_for_nodes`], which is why both take
/// the evidence as a parameter rather than calling this.
fn cluster_residency_evidence() -> ResidencyEvidence {
    static CACHED: OnceLock<ResidencyEvidence> = OnceLock::new();
    *CACHED.get_or_init(|| {
        ResidencyEvidence::from_env_value(std::env::var(NODE_CENSUS_RESIDENCY_ENV).ok().as_deref())
    })
}

/// Default long-runner age: a chosen heuristic, not a value derived from
/// auto-lifecycle policy. A resident whose lifecycle has not moved in a day is
/// unlikely to move on its own within a drain's patience, which is the judgement
/// this encodes. Tune it per call with `long_runner_age_hours`; nothing
/// downstream treats 24 h as a threshold with independent meaning.
const DEFAULT_LONG_RUNNER_AGE: Duration = Duration::from_secs(24 * 60 * 60);

fn parse_long_runner_age(
    hours: Option<u32>,
) -> Result<Duration, (StatusCode, Json<serde_json::Value>)> {
    match hours {
        None => Ok(DEFAULT_LONG_RUNNER_AGE),
        // Zero would make every live resident a long-runner, which reads as
        // "nothing here is drainable" rather than as the misuse it is.
        Some(0) => Err(api_error(
            StatusCode::BAD_REQUEST,
            "long_runner_age_hours must be at least 1",
        )),
        Some(hours) => Ok(Duration::from_secs(u64::from(hours) * 60 * 60)),
    }
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct NodeSandboxesQuery {
    after: Option<SandboxId>,
}

pub(super) async fn list_node_sandboxes(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(node_id): Path<NodeId>,
    Query(query): Query<NodeSandboxesQuery>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let store = AdminNodeStore::new(state.pool.clone());
    match store.fetch(node_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return api_error(StatusCode::NOT_FOUND, "Node not found").into_response(),
        Err(err) => return db_error(err).into_response(),
    }
    let mut sandboxes = match store.list_sandboxes(node_id, query.after, 101).await {
        Ok(rows) => rows,
        Err(err) => return db_error(err).into_response(),
    };
    let next_after = if sandboxes.len() > 100 {
        sandboxes.truncate(100);
        sandboxes.last().map(|row| row.id)
    } else {
        None
    };
    Json(serde_json::json!({ "sandboxes": sandboxes, "next_after": next_after })).into_response()
}

pub async fn get_admin_node(
    Query(filters): Query<GetAdminNodeFilters>,
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(spec): Path<String>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let (node_id, action) = match parse_spec(&spec) {
        Ok(parsed) => parsed,
        Err(err) => return err.into_response(),
    };
    if action.is_some() {
        // `GET .../UUID:action` is meaningless — actions are POST only.
        return api_error(StatusCode::METHOD_NOT_ALLOWED, "admin actions require POST")
            .into_response();
    }
    let store = AdminNodeStore::new(state.pool.clone());
    let node = match store.fetch(node_id).await {
        Ok(Some(node)) => node,
        Ok(None) => return api_error(StatusCode::NOT_FOUND, "Node not found").into_response(),
        Err(err) => return db_error(err).into_response(),
    };
    if filters.include_occupancy == Some(false) {
        return Json(admin_node_response(node, None, None)).into_response();
    }
    // Legacy detail requests retain the authoritative diagnostic counts.
    let residency = cluster_residency_evidence();
    let node_occupancy = match store
        .occupancy_for_nodes(
            std::slice::from_ref(&node.id),
            Some(DEFAULT_LONG_RUNNER_AGE),
            residency,
        )
        .await
    {
        Ok(map) => map.get(&node.id).copied().unwrap_or_default(),
        Err(err) => return db_error(err).into_response(),
    };
    let occupancy = node_occupancy.occupancy;
    Json(admin_node_response(
        node,
        Some(occupancy),
        Some(drainability_block(
            node_occupancy,
            residency,
            DEFAULT_LONG_RUNNER_AGE,
        )),
    ))
    .into_response()
}

/// Dispatch a `POST /v1/nodes/{spec}` where `spec` is either a bare
/// UUID (rejected with 400 — actions require the `:action` suffix) or a
/// `UUID:action` pair.
///
/// `body` is consumed last (axum requirement for the `Bytes` extractor) and
/// is only inspected for `:drain`, which accepts an optional
/// [`DrainRequest`] payload. Other actions ignore the body bytes.
pub async fn dispatch_node_action(
    state: State<AppState>,
    actor: Extension<AuthUser>,
    Path(spec): Path<String>,
    body: Bytes,
) -> axum::response::Response {
    let (node_id, action) = match parse_spec(&spec) {
        Ok(parsed) => parsed,
        Err(err) => return err.into_response(),
    };
    let Some(action) = action else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "admin POST requires an :action suffix (drain, terminate, undrain, cordon, uncordon, force-upload-local-snapshots)",
        )
        .into_response();
    };
    if matches!(action, DrainAction::ForceUploadLocalSnapshots) {
        return apply_force_upload_local_snapshots(state, actor, node_id).await;
    }
    let drain_request = if matches!(action, DrainAction::Drain) {
        match parse_drain_request(&body) {
            Ok(req) => req,
            Err(err) => return err.into_response(),
        }
    } else {
        DrainRequest::default()
    };
    apply_drain_action(state, actor, node_id, action, drain_request).await
}

/// The 409 body for a refused `:terminate`.
///
/// Extracted from the handler for the same reason as
/// [`terminate_blocked_by_occupancy`] below: so the keys it emits are
/// testable without a handler, a database and an authenticated request.
/// They need testing because one of them exists for a reader outside this
/// repository's type system.
///
/// `sandboxes_drain_blockers` is the count's real name — it is
/// `drain_gate_blockers_for_node`, not a count of local snapshots.
/// `sandboxes_local_snapshots` is emitted alongside it as an alias of the same
/// number. `scripts/space-rollout-mng.py` reads `sandboxes_drain_blockers` off
/// this body and falls back to the alias, and a copy of the tool from before it
/// read the new key reads only the alias. That tool drives `:terminate`
/// unattended across a rollout, and its classification keys on
/// `sandboxes_active`, so dropping the alias wouldn't break an older copy; it
/// would silently null the count in the event an operator reads to see *why* a
/// wave stalled. Retire the alias once no such copy runs.
fn terminate_conflict_body(occupancy: &node_store::TerminateOccupancy) -> serde_json::Value {
    serde_json::json!({
        "error": "node still holds sandboxes; terminating would destroy \
                  resident sandboxes or strand local-disk snapshots",
        "sandboxes_active": occupancy.active,
        "sandboxes_drain_blockers": occupancy.drain_blockers,
        "sandboxes_local_snapshots": occupancy.drain_blockers,
    })
}

/// Pure predicate behind the `:terminate` occupancy guard: does this node still
/// hold something that terminating would destroy or strand?
///
/// Extracted so the guard is testable without a handler and a database — same
/// reason as `drain_phase_blocks_placement` in
/// `controllers::sandbox::lifecycle`. Both halves matter, and the
/// `active` half is the one that was computed and not checked; see the call site
/// for what that cost.
fn terminate_blocked_by_occupancy(occupancy: &node_store::TerminateOccupancy) -> bool {
    occupancy.active > 0 || occupancy.drain_blockers > 0
}

/// The 501 for an action [`DrainAction::runs_without_node_drain`] excludes,
/// on a substrate without [`SubstrateCapabilities::supports_node_drain`];
/// `None` when the action may run.
///
/// Callers check this after authorization and before reading or locking the
/// node, so a refused action writes nothing to `nodes` and never waits on its
/// row lock: on such a substrate the cordon patch can never succeed, and no
/// reconciler would advance or re-assert a committed phase.
///
/// [`SubstrateCapabilities::supports_node_drain`]: crate::controllers::substrate::SubstrateCapabilities::supports_node_drain
fn refuse_without_node_drain(
    state: &AppState,
    actor: &User,
    node_id: NodeId,
    action: DrainAction,
    drain_mode: Option<NodeDrainMode>,
    replace_after_terminate: Option<bool>,
) -> Option<axum::response::Response> {
    if action.runs_without_node_drain()
        || state
            .sandbox_controller
            .substrate_capabilities()
            .supports_node_drain
    {
        return None;
    }
    audit::record(AdminAuditEntry {
        actor,
        action: action.label(),
        node_id,
        node_name: None,
        from_phase: None,
        to_phase: None,
        // The refusal answers before the row is read, so there is no current
        // mode to record.
        from_drain_mode: None,
        replace_after_terminate,
        drain_mode,
        success: false,
        error: Some(NODE_DRAIN_UNSUPPORTED),
        outcome_counts: None,
    });
    Some(api_error(StatusCode::NOT_IMPLEMENTED, NODE_DRAIN_UNSUPPORTED).into_response())
}

/// The two questions a substrate-convergence failure raises, which the single
/// `phase_was_written` flag conflated.
///
/// It was already wrong before this edge existed: `:undrain` is a write that
/// leaves no phase, so the old flag promised a retry that nothing performs. The
/// new edge adds the mirror case — a write-free request that *does* leave a
/// phase — which is what made one bool visibly insufficient rather than merely
/// incorrect.
///
/// Returned as a named struct rather than a `(bool, bool)`: the failure this
/// exists to prevent is a call site binding the two in the wrong order, and a
/// tuple makes that swap both type-compatible and untestable — a test can only
/// assert what the function returns, never how a caller destructures it.
///
/// - **Written** gates the best-effort `drain_last_error` write. A divergence is
///   worth recording whenever this request wrote, even when nothing will repair
///   it — `:undrain` is that case, and the trace is all the operator gets.
/// - **Will re-assert** is what the 502 body promises. It depends on the phase
///   the node is *left* in, not on whether this request wrote it, and a 502 that
///   says a retry is coming is an instruction to wait.
///
/// They disagree on three reachable shapes, which is why they are computed
/// together here rather than inline where a reader sees only one of them:
///
/// | Edge | Written | Re-asserts | Why they differ |
/// |---|---|---|---|
/// | `:drain` starting or updating a drain | yes | yes | — |
/// | `:undrain` | yes | **no** | phase gone, nothing scans it |
/// | `:cordon` on a `draining` node | **no** | yes | wrote nothing, reconciler re-cordons anyway |
/// | `:cordon` on a `terminating` node | **no** | yes | same, via the terminating handler |
/// | `:cordon` on a `drained` node | no | no | — |
///
/// Every row assumes `managed_by = 'k8s'`. On a direct-managed node the
/// re-asserts column is `no` throughout, and separately every `:drain` against
/// one 502s for want of a `node_k8s` row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecoveryHints {
    /// Did this request write the drain columns? Gates the best-effort
    /// `drain_last_error` write.
    drain_columns_written: bool,
    /// Will the node-drain reconciler re-assert cordon on its own? What the 502
    /// body promises.
    reconciler_will_reassert: bool,
}

fn recovery_hints(
    transition: &DrainTransition,
    new_phase: Option<&str>,
    managed_by: NodeManagedBy,
) -> RecoveryHints {
    RecoveryHints {
        drain_columns_written: matches!(
            transition,
            DrainTransition::Move { .. } | DrainTransition::UpdateInFlightDrain(_)
        ),
        reconciler_will_reassert: reconciler_reasserts_cordon(new_phase, managed_by),
    }
}

/// Does this transition start a drain, and therefore carry a fresh
/// `replace_after_terminate` to persist?
///
/// `set_started_at` is the discriminator because starting the clock is what
/// makes an edge a new drain. Widening this to any `Move` is the silent failure:
/// `:terminate` would then write
/// `persisted_decrement_capacity(&DrainRequest::default())` — `false` — over the
/// initiating drain's `true`, at the moment the terminator is about to read it,
/// with no error and a normal-looking audit entry (`:terminate` carries no
/// requested mode or flag). The observable is a source MNG that stops shrinking.
///
/// Extracted because this is the only guard on the only writer of that column.
/// Inline in the handler it was unreachable by any test in this file, all of
/// which call pure helpers; `drain_handler_tests` now drives the handler too, so
/// the guard is covered at both levels — as a predicate over every transition
/// here, and through the committed column there.
fn transition_starts_a_drain(transition: &DrainTransition) -> bool {
    matches!(
        transition,
        DrainTransition::Move {
            set_started_at: true,
            ..
        }
    )
}

/// `nodes.terminate_decrement_capacity` for a request that is starting a drain.
///
/// The column and the request field are **inverses**: the column records whether
/// the eventual ASG terminate decrements desired capacity, the request records
/// whether the instance is replaced. This is the only place that inversion is
/// applied, and it is the only place the column is ever written, so getting it
/// backwards silently turns every blue-green shrink into an auto-replacing drain
/// — the source MNG never shrinks, and the first sign is `source_desired` rising
/// hours later.
///
/// A function rather than an inline `!` because a test cannot assert an
/// expression it has to restate. The test that used to cover this restated
/// `!or_default()` in its own helper and therefore asserted its own copy —
/// deleting the `!` from the handler failed nothing in the crate. It fails
/// `the_persisted_decrement_capacity_inverts_the_request_field` now.
fn persisted_decrement_capacity(request: &DrainRequest) -> bool {
    !request.replace_after_terminate_or_default()
}

/// The 409 body for a `:drain` whose `replace_after_terminate` disagrees with
/// the drain already in flight.
///
/// Extracted for the same reason as [`force_upload_gate_open_message`]: it is a
/// sentence an operator reads and acts on, and it has to say what to do instead.
/// Its first draft shipped with two 22-space runs from a hand-wrapped literal
/// missing its `\` continuations — invisible in review, obvious in a terminal,
/// and not reachable by any test while it was inline.
fn replace_conflict_message(stored_replace: bool, requested: bool) -> String {
    format!(
        "this node is already draining with replace_after_terminate={stored_replace}; the \
         request asked for {requested}. A drain's replacement intent is fixed when it starts, \
         because the terminator may act on it at any time after the node reaches drained. \
         Omit the field to escalate on the existing terms, or send \
         replace_after_terminate={stored_replace} to match them — note space-admin-cli reaches the \
         second only as --no-replace-after-terminate, so if the drain in flight auto-replaces, \
         omitting is the only remedy it can issue. To change the terms, :undrain and start a \
         fresh drain."
    )
}

/// The mode to write, on the actions that carry one.
///
/// `None` for every action but `:drain`, and load-bearing there: `:terminate` is
/// a `Move` with both stamp flags false, so `drain_mode = CASE WHEN $4 THEN NULL
/// ELSE COALESCE($6, drain_mode) END` would write this value over a forced
/// drain's `'forced'` at the `drained → terminating` edge. The user-API gates
/// and the reconciler both re-read that column, so a node one step from ASG
/// termination would present as gentle and reopen user calls against whatever
/// is still attached to it. `:undrain` is saved only by the SQL `CASE`'s
/// NULL-clearing arm taking precedence — the correctness of this argument is
/// split across the handler and the SQL, so the handler half needs its own
/// assertion.
fn requested_mode_for(action: DrainAction, request: &DrainRequest) -> Option<NodeDrainMode> {
    matches!(action, DrainAction::Drain).then_some(request.mode)
}

/// Should the replacement-intent conflict check run for this transition?
///
/// Only the in-flight edge. On the drain-*starting* edge the column is being
/// written from this very request, so there is nothing to disagree with — and
/// running the check there would refuse a large class of fresh drains.
///
/// Which class, precisely, because the arithmetic inverts twice and the
/// intuitive answer is wrong. The check reads `stored_replace =
/// !terminate_decrement_capacity`, so it refuses whenever
/// `terminate_decrement_capacity` is `false` and the request asks for `false`.
/// The column is `BOOLEAN NOT NULL DEFAULT FALSE` and is never cleared, not even
/// by `:undrain`, so `false` means either "drained bodiless at some point" or —
/// far more often — "never drained at all".
///
/// So an ungated check would 409 every first-ever
/// `--no-replace-after-terminate` drain, which is every node a shrink campaign
/// starts on. Nodes a previous wave *touched* are the ones it would let
/// through: that wave posted `replace_after_terminate: false`, which persists
/// `terminate_decrement_capacity = true` (see
/// [`persisted_decrement_capacity`], which inverts), giving
/// `stored_replace = false` — a match, not a conflict.
fn checks_replacement_intent_conflict(transition: &DrainTransition) -> bool {
    matches!(transition, DrainTransition::UpdateInFlightDrain(_))
}

/// The `drain_phase` the node holds after a transition is applied.
///
/// Both the audit entry's `to_phase` and [`recovery_hints`]' input read this, so
/// deriving it from the transition keeps one answer instead of a literal in the
/// handler that a refactor can drift. `UpdateInFlightDrain` is the case that
/// matters: it re-asserts `draining`, and returning `None` there — mirroring
/// `:undrain`, or copying `NoChange`'s pass-through — would tell an operator
/// whose cordon patch failed that no retry is coming on the one edge where the
/// reconciler re-cordons every tick.
fn phase_after(transition: &DrainTransition, current_phase: Option<&str>) -> Option<&'static str> {
    match transition {
        DrainTransition::Move { new, .. } => *new,
        DrainTransition::UpdateInFlightDrain(_) => Some(PHASE_DRAINING),
        // Cordon-only paths leave the phase untouched. Re-resolved through the
        // phase constants rather than passed through as a borrowed column value,
        // so the return type can stay `&'static str` like the others.
        DrainTransition::NoChange => match current_phase {
            Some(PHASE_DRAINING) => Some(PHASE_DRAINING),
            Some(PHASE_DRAINED) => Some(PHASE_DRAINED),
            Some(PHASE_TERMINATING) => Some(PHASE_TERMINATING),
            // A phase this build cannot classify — see the validator's `_`
            // arm for why that is a version-skew guard rather than a live case.
            // Reported as absent rather than echoed, so nothing downstream
            // promises a retry for a phase this build cannot place in a work
            // list.
            //
            // The audit entry pays for that: a `:cordon` against such a row
            // records `to_phase: null` while the row keeps its phase. Accepted
            // rather than fixed, because echoing the column would mean widening
            // the return past `&'static str` for a value no build should be
            // able to write — and the hint consumer, which decides whether an
            // operator is told to wait, is the one that must not guess.
            Some(_) | None => None,
        },
    }
}

/// The mode of the drain already in progress, for the audit trail.
///
/// Two guards, and both were mutation-silent while this was inline in the
/// handler, which had no test of its own at the time — the last instance of the
/// shape every extraction above exists for, and the one that prompted
/// `drain_handler_tests` rather than one more of them:
///
/// - The **action class**. `:cordon` and `:uncordon` share the terminal audit
///   entry but move nothing, so a from-mode on one of their entries would read
///   as a transition they never made.
/// - The **phase**. `from_column_str` answers `Gentle` for a NULL column, which
///   is right for a draining node and invented for a node nobody has drained.
///   Without this, every `:drain` that *starts* a drain would audit
///   `from_drain_mode: gentle` — affirmatively reporting a prior mode the row
///   never held.
fn audited_mode_in_progress(
    action: DrainAction,
    drain_phase: Option<&str>,
    current_mode: NodeDrainMode,
) -> Option<NodeDrainMode> {
    let moves_the_drain = matches!(
        action,
        DrainAction::Drain | DrainAction::Terminate | DrainAction::Undrain
    );
    (moves_the_drain && drain_phase.is_some()).then_some(current_mode)
}

/// Does a `:drain` against an already-draining node express a
/// `replace_after_terminate` that disagrees with the one the initiating drain
/// persisted? Returns the `(stored, requested)` pair to name in the refusal.
///
/// `stored_decrement` is `nodes.terminate_decrement_capacity`, which is the
/// *inverse* of `replace_after_terminate` — the column records whether the ASG
/// terminate decrements desired capacity, the request records whether the
/// instance is replaced. The inversion is the reason this is a named function
/// with a truth table rather than an inline comparison.
///
/// Returns a named struct for the same reason [`RecoveryHints`] does: both
/// fields are `bool`, all three consumers (the message and the two JSON keys)
/// read from one binding, and a positionally-swapped destructure at the call
/// site would compile, pass every test, and invert the entire refusal — telling
/// the operator the drain holds the value they asked for and to re-issue with
/// the one that conflicts.
///
/// Refusing rather than applying, and refusing rather than ignoring:
///
/// - **Applying** would honour a stale client. Every `space-admin-cli` older than
///   `replace_after_terminate: Option<bool>` sends an explicit
///   `!no_replace_after_terminate`, so a plain `--forced` escalation from one
///   carries `true` and would silently turn a blue-green shrink back into an
///   auto-replacing drain.
/// - **Ignoring** is where this started: a 200 that drops a deliberate
///   correction, discoverable only later as a source MNG that never shrinks.
///
/// So the operator is told. Changing the terms of a drain in flight is
/// `:undrain` plus a fresh `:drain`, which is the verb that already means
/// "restart this drain on different terms".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReplaceConflict {
    /// What the drain in flight was started with.
    stored_replace: bool,
    /// What this request asked for.
    requested_replace: bool,
}

fn in_flight_replace_conflict(
    requested_replace: Option<bool>,
    stored_decrement: bool,
) -> Option<ReplaceConflict> {
    let requested_replace = requested_replace?;
    let stored_replace = !stored_decrement;
    (requested_replace != stored_replace).then_some(ReplaceConflict {
        stored_replace,
        requested_replace,
    })
}

/// Will the node-drain reconciler re-assert this node's cordon on its own?
///
/// Answered from what the reconciler's work lists actually select on, which is
/// **both** conjuncts. `ensure_cordon` has exactly two callers —
/// `handle_draining_node` and `handle_terminating_node` — and their listings are
/// `drain_phase = 'draining' AND managed_by = 'k8s'` and
/// `drain_phase = 'terminating' AND managed_by = 'k8s'` respectively.
///
/// `drained` is in neither list: it is a resting state whose only mover is the
/// drained-node terminator, which terminates rather than repairs. Neither is a
/// `managed_by = 'space'` row in any phase — that scoping is deliberate on the
/// reconciler's side (a direct-managed node must not be fed a Kubernetes/ASG
/// pipeline), which means a substrate failure on one is genuinely unrepairable
/// and the operator has to be told so rather than told to wait. A
/// `managed_by = 'space'` node also has no `node_k8s` row, so
/// `converge_substrate_schedulability` fails it on every `:drain` — dropping the
/// conjunct here would answer "a retry is coming" to the one case where nothing
/// is coming at all.
///
/// There is a third conjunct this cannot see: the reconciler only runs when
/// `SPACE_NODE_DRAIN_ENABLED` is set, and its code default is `false`. A
/// deployment that runs the reconciler sets it `"true"` (the kind chart in this
/// tree does), but a `true` here is an assumption about the cluster's config and
/// not a fact about the row. The same-mode arm's comment reasons about the
/// disabled case explicitly; this predicate assumes it away.
///
/// Extracted for the same reason as [`terminate_blocked_by_occupancy`] — this is
/// what the 502 body promises an operator, and a 502 that says a retry is coming
/// is an instruction to wait. Three edges come apart here, and a predicate that
/// asks "is a phase set?" gets two of them right:
///
/// - A `:drain` that re-asserted the mode it already had leaves the node
///   `draining`, where cordon *is* re-asserted every tick.
/// - `:undrain` writes the phase to NULL, taking the node out of scope entirely.
/// - `:cordon` on a `drained` node writes nothing and leaves a non-NULL phase
///   that nothing scans. This is the dangerous one: the operator waits while the
///   node stays schedulable until the terminator destroys it, taking any sandbox
///   placed in that window with it.
fn reconciler_reasserts_cordon(new_phase: Option<&str>, managed_by: NodeManagedBy) -> bool {
    managed_by.has_node_drain_reconciler()
        && matches!(new_phase, Some(PHASE_DRAINING | PHASE_TERMINATING))
}

async fn apply_drain_action(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    node_id: NodeId,
    action: DrainAction,
    drain_request: DrainRequest,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }

    // The mode the operator asked for, on `:drain` only — every other action
    // arrives with a defaulted `DrainRequest` it never sent, so recording its
    // `mode` would invent an intent. Bound here rather than at the write below
    // because the audit entries on the rejection paths need it too: a refused
    // `:drain` is exactly when an operator wants to know which mode was
    // attempted.
    let requested_mode = requested_mode_for(action, &drain_request);
    // Bound here for the same audit reason, but *not* action-gated like
    // `requested_mode`, because it does not need to be: `mode` has a `Default`,
    // so a defaulted request carries a `Gentle` nobody chose and the gate is
    // what stops the audit inventing it. This field is an `Option<bool>` whose
    // default is `None`, and only `:drain` parses a body at all (see the
    // dispatcher), so every other action already arrives with `None`. Gating it
    // again would read as a second safeguard where there is one.
    let requested_replace_after_terminate = drain_request.replace_after_terminate;

    // `:undrain` passes on a substrate without node drain, and skips the
    // substrate patch below.
    if let Some(refusal) = refuse_without_node_drain(
        &state,
        &actor,
        node_id,
        action,
        requested_mode,
        requested_replace_after_terminate,
    ) {
        return refusal;
    }
    let supports_node_drain = state
        .sandbox_controller
        .substrate_capabilities()
        .supports_node_drain;

    // Phase 1 — DB transition under `FOR UPDATE`.
    //
    // Postgres is the source of truth. We validate the requested transition
    // against the locked row, write the new `drain_phase`, and **commit
    // before touching the substrate**. The earlier ordering (substrate patch
    // under the row lock, then DB commit) inverted the dependency: a
    // substrate success followed by a DB rollback would leave the scheduler
    // and the admin API disagreeing — especially destructive for `:undrain`,
    // which would unblock placement on a node the DB still reports as
    // draining. With Postgres committed first, a failed substrate patch is
    // recoverable: the node-drain reconciler observes `drain_phase` and
    // re-asserts cordon state, and `drain_last_error` captures the failure
    // for the operator.
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(err) => return db_error(err).into_response(),
    };

    let current = match node_store::lock_node_for_update(&mut tx, node_id).await {
        Ok(Some(row)) => row,
        Ok(None) => {
            return api_error(StatusCode::NOT_FOUND, "Node not found").into_response();
        }
        Err(err) => return db_error(err).into_response(),
    };

    let current_mode = NodeDrainMode::from_column_str(current.drain_mode.as_deref());
    let mode_in_progress =
        audited_mode_in_progress(action, current.drain_phase.as_deref(), current_mode);

    // The start-only precondition, checked against the locked row so nothing
    // can start a drain between this read and the write below.
    if refuses_existing_drain(
        action,
        drain_request.require_undrained,
        current.drain_phase.as_deref(),
    ) {
        audit::record(AdminAuditEntry {
            actor: &actor,
            action: action.label(),
            node_id,
            node_name: current.substrate_node_id.as_deref(),
            from_phase: current.drain_phase.as_deref(),
            to_phase: None,
            from_drain_mode: mode_in_progress,
            replace_after_terminate: requested_replace_after_terminate,
            drain_mode: requested_mode,
            success: false,
            error: Some("conflict: require_undrained and the node is already in a drain phase"),
            outcome_counts: None,
        });
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "node is already in a drain phase and the request set require_undrained",
                "current_drain_phase": current.drain_phase,
            })),
        )
            .into_response();
    }

    let transition = match validate_drain_transition(
        current.drain_phase.as_deref(),
        current_mode,
        action,
        requested_mode,
    ) {
        Ok(transition) => transition,
        Err(TransitionError::Conflict {
            current: current_phase,
            allowed,
        }) => {
            audit::record(AdminAuditEntry {
                actor: &actor,
                action: action.label(),
                node_id,
                node_name: current.substrate_node_id.as_deref(),
                from_phase: current_phase.as_deref(),
                to_phase: None,
                from_drain_mode: mode_in_progress,
                replace_after_terminate: requested_replace_after_terminate,
                drain_mode: requested_mode,
                success: false,
                error: Some("conflict: invalid drain-phase transition"),
                outcome_counts: None,
            });
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": "drain-phase transition not allowed",
                    "current_drain_phase": current_phase,
                    "allowed_from": allowed,
                })),
            )
                .into_response();
        }
        Err(TransitionError::ModeDowngrade { current_mode }) => {
            // The requested mode is this handler's own binding. It is `Some` on
            // every `:drain`, which is the only action that can reach this arm.
            let attempted = requested_mode.unwrap_or_default();
            audit::record(AdminAuditEntry {
                actor: &actor,
                action: action.label(),
                node_id,
                node_name: current.substrate_node_id.as_deref(),
                from_phase: current.drain_phase.as_deref(),
                to_phase: None,
                from_drain_mode: Some(current_mode),
                replace_after_terminate: requested_replace_after_terminate,
                drain_mode: requested_mode,
                success: false,
                error: Some("conflict: drain mode downgrade refused"),
                outcome_counts: None,
            });
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": format!(
                        "node is already draining in {current} mode; a drain cannot be \
                         downgraded to {attempted} because the evacuation already in flight \
                         cannot be un-run — suspends and cross-node restores have no inverse. \
                         Use :undrain to abort the drain entirely.",
                        current = current_mode.as_str(),
                        attempted = attempted.as_str(),
                    ),
                    "current_drain_phase": current.drain_phase,
                    "current_drain_mode": current_mode.as_str(),
                    "requested_drain_mode": attempted.as_str(),
                })),
            )
                .into_response();
        }
    };

    // Refuse a `:drain` that would change the terms of a drain already in
    // flight. Read under the same lock as the write, so the pair named in the
    // 409 is the pair the write would have contradicted.
    if checks_replacement_intent_conflict(&transition)
        && let Some(ReplaceConflict {
            stored_replace,
            requested_replace,
        }) = in_flight_replace_conflict(
            drain_request.replace_after_terminate,
            current.terminate_decrement_capacity,
        )
    {
        audit::record(AdminAuditEntry {
            actor: &actor,
            action: action.label(),
            node_id,
            node_name: current.substrate_node_id.as_deref(),
            from_phase: current.drain_phase.as_deref(),
            to_phase: None,
            from_drain_mode: mode_in_progress,
            replace_after_terminate: requested_replace_after_terminate,
            drain_mode: requested_mode,
            success: false,
            error: Some("conflict: replace_after_terminate disagrees with the drain in flight"),
            outcome_counts: None,
        });
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": replace_conflict_message(stored_replace, requested_replace),
                "current_replace_after_terminate": stored_replace,
                "requested_replace_after_terminate": requested_replace,
            })),
        )
            .into_response();
    }

    // Guard the destructive transition into `terminating`, on both halves of
    // occupancy. The check happens under the same `FOR UPDATE` lock as the phase
    // write, so it and the write agree on one snapshot.
    //
    // **That lock does not order every writer, and this guard is not a fence.**
    // It only orders transactions that themselves lock this `nodes` row.
    // `sandboxes.active_node_id` has no FK to `nodes(id)`, and the pooled
    // post-wake, post-create placement, pooled-create, and direct-restore
    // writers set it without touching `nodes` — so a placement can commit
    // immediately after this transaction does, on a node this endpoint has just
    // moved to `terminating`. Nothing on the `terminating` path consults the
    // resident guard again (it rides the `drained → terminating` edge), so before
    // the recheck described below that sandbox died with the instance.
    // `node_drain::handle_terminating_node` now recounts the terminator's
    // blocking set immediately before the provider terminate call and reverts to
    // `drained` instead, which turns the common case into a park — but the
    // microseconds between that recount and the provider call are still exposed.
    // A fence honoured by every placement writer and the provider-termination
    // path would be needed to close that race.
    //
    // * `drain_blockers` — a row the drain gate holds the node for: a suspended
    //   sandbox whose only snapshot is on the node named by `target_node_id`,
    //   a `suspending` or non-evacuation `restoring` row pinned there, or an
    //   upload still in flight from it; terminating would strand it.
    // * `active` — a resident sandbox dies with the instance. Both this route
    //   and the drained-node terminator must check residency before advancing.
    //   `space-rollout-mng.py` calls this route automatically for drained nodes.
    //   The sibling guard in `admin/node_groups.rs` also checks both halves,
    //   but reads `group_occupancy_in_tx`, which is not census-aware.
    //   Its refusal to move an organization can therefore count phantom rows.
    if matches!(action, DrainAction::Terminate) {
        let occupancy = match node_store::sandbox_occupancy_for_node_in_tx(
            &mut tx,
            node_id,
            cluster_residency_evidence(),
        )
        .await
        {
            Ok(value) => value,
            Err(err) => return db_error(err).into_response(),
        };
        if terminate_blocked_by_occupancy(&occupancy) {
            audit::record(AdminAuditEntry {
                actor: &actor,
                action: action.label(),
                node_id,
                node_name: current.substrate_node_id.as_deref(),
                from_phase: current.drain_phase.as_deref(),
                to_phase: None,
                from_drain_mode: mode_in_progress,
                replace_after_terminate: requested_replace_after_terminate,
                drain_mode: requested_mode,
                success: false,
                error: Some("conflict: node still holds sandboxes"),
                outcome_counts: None,
            });
            return (
                StatusCode::CONFLICT,
                Json(terminate_conflict_body(&occupancy)),
            )
                .into_response();
        }
    }

    // Persist a replacement intent on the edge that *starts* a drain, where an
    // unexpressed opinion resolves to the default. Keyed on `set_started_at`
    // rather than on `matches!(action, DrainAction::Drain)`, which now selects
    // two edges: starting the clock is what makes an edge a new drain.
    //
    // Every other edge leaves the column alone so the terminator honors the
    // initiating drain's intent — including the in-flight edge, whose store
    // wrapper hardcodes `None`. A request that expressed a disagreeing value
    // never reaches the write; it is refused above.
    let terminate_decrement_capacity = transition_starts_a_drain(&transition)
        .then(|| persisted_decrement_capacity(&drain_request));

    // The write. The phase the row ends up holding is derived separately by
    // `phase_after`, so this match only has to perform the write.
    match transition {
        DrainTransition::Move {
            new,
            set_started_at,
            clear_started_at,
        } => {
            if let Err(err) = node_store::update_drain_phase(
                &mut tx,
                node_id,
                new,
                set_started_at,
                clear_started_at,
                terminate_decrement_capacity,
                // `requested_mode` is `Some` only on the `:drain` edge, so
                // `:undrain` writes NULL here and clears the column through its
                // `clear_started_at` branch — an aborted drain leaves no mode
                // behind.
                requested_mode,
            )
            .await
            {
                return db_error(err).into_response();
            }
        }
        // A `:drain` against an in-flight drain. What must *not* move, and why,
        // is the store function's contract. A request carrying a conflicting
        // `replace_after_terminate` was already refused above, so reaching here
        // means the intent either matched what is stored or was never
        // expressed — either way there is nothing to write.
        DrainTransition::UpdateInFlightDrain(mode) => {
            if let Err(err) = node_store::update_in_flight_drain(&mut tx, node_id, mode).await {
                return db_error(err).into_response();
            }
        }
        DrainTransition::NoChange => {}
    }
    let new_phase = phase_after(&transition, current.drain_phase.as_deref());

    if let Err(err) = tx.commit().await {
        return db_error(err).into_response();
    }
    let RecoveryHints {
        drain_columns_written,
        reconciler_will_reassert,
    } = recovery_hints(&transition, new_phase, current.managed_by);

    // Phase 2 — converge the substrate's scheduling primitive.
    //
    // No row lock held here. `set_node_schedulability` goes through the
    // substrate so the route layer never imports substrate-specific client
    // crates. If it fails, we persist `drain_last_error` on the row (best
    // effort) so the node-drain reconciler can pick it up and re-assert
    // cordon state.
    //
    // The unlocked window also admits a *successful* patch that has been
    // overtaken. A `:drain` commits, an `:undrain` then commits and uncordons,
    // and this cordon patch lands after it: the kube Node is unschedulable while
    // `drain_phase` is NULL, and `reconciler_reasserts_cordon` is correctly
    // `false` for a NULL phase, so nothing repairs it — the node sits out of the
    // placement pool until someone runs `:uncordon`. Pre-existing (`:cordon` on
    // a draining node has the same shape), but wider now: a `:drain` against an
    // already-draining node used to 409 before reaching this point, and an
    // operator's repeat or escalation now gets here. Automation that sets
    // `require_undrained` doesn't: it is refused before the commit. Closing it
    // means holding the row lock across the substrate call, which inverts the
    // dependency this ordering exists to fix.
    let substrate_outcome = if supports_node_drain {
        converge_substrate_schedulability(&state, &current, action).await
    } else {
        // Only `:undrain` gets here. The substrate has no cordon to lift: no
        // admin action could have applied one.
        Ok(())
    };

    if let Err(ref substrate_err) = substrate_outcome
        && drain_columns_written
    {
        AdminNodeStore::new(state.pool.clone())
            .record_drain_last_error(node_id, &substrate_err.message)
            .await;
    }

    // Phase 3 — re-read post-commit, audit, respond.
    let store = AdminNodeStore::new(state.pool.clone());
    let refreshed = match store.fetch(node_id).await {
        Ok(Some(row)) => row,
        Ok(None) => {
            return api_error(StatusCode::NOT_FOUND, "Node not found").into_response();
        }
        Err(err) => return db_error(err).into_response(),
    };
    // No drainability on the action responses: this is a post-transition echo,
    // not a ranking, and the census probes would be work nobody reads.
    let occupancy = match store
        .occupancy_for_nodes(
            std::slice::from_ref(&node_id),
            None,
            cluster_residency_evidence(),
        )
        .await
    {
        Ok(map) => map.get(&node_id).copied().unwrap_or_default().occupancy,
        Err(err) => return db_error(err).into_response(),
    };

    let success = substrate_outcome.is_ok();
    audit::record(AdminAuditEntry {
        actor: &actor,
        action: action.label(),
        node_id,
        node_name: refreshed.substrate_node_id.as_deref(),
        from_phase: current.drain_phase.as_deref(),
        to_phase: new_phase,
        from_drain_mode: mode_in_progress,
        replace_after_terminate: requested_replace_after_terminate,
        drain_mode: requested_mode,
        success,
        error: substrate_outcome.as_ref().err().map(|e| e.message.as_str()),
        outcome_counts: None,
    });

    if let Err(substrate_err) = substrate_outcome {
        // DB transition succeeded; the substrate diverged. Surface a 502
        // with the committed DB state in the body so operators can see the
        // partial progress and either retry or wait for the drain
        // reconciler to re-assert cordon.
        return (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({
                "error": substrate_err.message,
                "db_state": admin_node_response(refreshed, Some(occupancy), None),
                "reconciler_will_retry": reconciler_will_reassert,
            })),
        )
            .into_response();
    }

    Json(admin_node_response(refreshed, Some(occupancy), None)).into_response()
}

struct SubstrateConvergeError {
    message: String,
}

fn desired_schedulability(action: DrainAction) -> Option<NodeSchedulability> {
    match action {
        DrainAction::Drain | DrainAction::Cordon => Some(NodeSchedulability::Unschedulable),
        DrainAction::Undrain | DrainAction::Uncordon => Some(NodeSchedulability::Schedulable),
        // `:terminate` is a pure DB transition; the node is already cordoned
        // from the prior `:drain` and we should not patch it again.
        DrainAction::Terminate => None,
        // `:force-upload-local-snapshots` is a remediation action that does
        // not change scheduling. Routed directly to its own handler before
        // reaching this point; this arm just keeps the match exhaustive.
        DrainAction::ForceUploadLocalSnapshots => None,
    }
}

async fn converge_substrate_schedulability(
    state: &AppState,
    node: &AdminNodeRow,
    action: DrainAction,
) -> Result<(), SubstrateConvergeError> {
    let Some(desired) = desired_schedulability(action) else {
        return Ok(());
    };
    let Some(substrate_node_id) = node.substrate_node_id.as_deref() else {
        return Err(SubstrateConvergeError {
            message: "node has no associated substrate node id".to_string(),
        });
    };
    state
        .sandbox_controller
        .set_node_schedulability(NodeSchedulabilityRequest { substrate_node_id }, desired)
        .await
        .map_err(|err| SubstrateConvergeError {
            message: format!("substrate cordon patch failed: {err}"),
        })
}

/// Per-RPC and per-invocation bounds for `:force-upload-local-snapshots`.
///
/// 16-wide × 4 s timeout × 2 batches at the per-invocation cap = ≤ 8 s,
/// which keeps the handler inside the 10 s admin-handler contract even
/// when the on-node sbx-mgr is slow. Sandboxes past the cap are returned
/// in the response's `deferred` array so the operator can re-invoke; the
/// node-drain reconciler also picks them up on its own ticks.
const FORCE_UPLOAD_RPC_CONCURRENCY: usize = 16;
const FORCE_UPLOAD_RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(4);
const FORCE_UPLOAD_MAX_PER_INVOCATION: usize = 32;

/// Hard cap on how many pinned-local-only sandbox IDs the
/// `list_pinned_local_only_sandboxes` query can enumerate per call.
///
/// The handler processes the first `FORCE_UPLOAD_MAX_PER_INVOCATION`
/// IDs and reports the remaining `ENUMERATION_CAP - MAX_PER_INVOCATION`
/// IDs in `deferred[]` for operator visibility. The query passes
/// `LIMIT cap + 1` so the response can flip `truncated: true` when
/// the actual pinned-list size exceeds the cap, signalling that
/// re-invoking will keep finding new work. Bounds the DB transfer
/// and JSON response size to O(1) regardless of how big the pinned
/// list is on a backlogged node.
pub(super) const FORCE_UPLOAD_ENUMERATION_CAP: usize = 256;

/// Maximum acceptable wall-clock for the handler. The
/// `:force-upload-local-snapshots` route lives under the admin API,
/// which carries a 10-second admin-handler latency contract.
const FORCE_UPLOAD_ADMIN_HANDLER_BUDGET_SECS: u64 = 10;

// Compile-time guard: every nudge to the three knobs above (cap,
// concurrency, per-RPC timeout) must keep the worst-case fan-out
// inside the admin-handler latency contract. `(cap / concurrency)`
// rounded up is the number of fully-blocking batches; each batch
// is bounded by the per-RPC timeout. If a future change loosens
// any of the knobs past the contract, this assertion fires at
// build time rather than letting the regression land silently.
const _: () = {
    let batches = FORCE_UPLOAD_MAX_PER_INVOCATION.div_ceil(FORCE_UPLOAD_RPC_CONCURRENCY) as u64;
    assert!(
        batches * FORCE_UPLOAD_RPC_TIMEOUT.as_secs() <= FORCE_UPLOAD_ADMIN_HANDLER_BUDGET_SECS,
        "force-upload-local-snapshots fan-out exceeds the 10s admin-handler budget; \
         lower FORCE_UPLOAD_MAX_PER_INVOCATION, raise FORCE_UPLOAD_RPC_CONCURRENCY, \
         or shorten FORCE_UPLOAD_RPC_TIMEOUT",
    );
};

/// `:force-upload-local-snapshots` action handler.
///
/// Enumerates every suspended sandbox whose `target_node_id` still pins
/// it to this node **and** whose snapshot has not yet been registered
/// to S3 (via [`node_store::list_pinned_local_only_sandboxes`]), then
/// calls the controller's `enqueue_suspended_snapshot_backup` per sandbox. That
/// RPC enqueues a durable-worker upload on the on-node sbx-mgr; the
/// worker lands a `state='ready'` row in `snapshots` +
/// `snapshot_storage_locations` and the sandbox stops counting toward
/// `sandboxes_local_snapshots`.
///
/// Returns a per-sandbox `reuploaded` / `failed` / `deferred` breakdown.
/// Idempotent: re-invoking against a node whose pinned list is empty
/// returns empty arrays. Each upload trigger is itself idempotent on
/// the substrate side — overwriting in place is safe because the drain
/// gate freezes user-driven wake during drain, so on-disk artifacts
/// don't change underneath us.
///
/// Latency contract enforced via [`FORCE_UPLOAD_RPC_TIMEOUT`],
/// [`FORCE_UPLOAD_RPC_CONCURRENCY`], and
/// [`FORCE_UPLOAD_MAX_PER_INVOCATION`].
/// Operator-facing refusal for `:force-upload-local-snapshots` when the wake
/// gate is open, as the JSON `error` / `detail` pair.
///
/// Extracted from the handler so the emitted text is directly assertable: the
/// handler needs a full `AppState` that the test module has no harness for, and
/// these strings are read by operators. A multi-line literal here bakes its own
/// source indentation into the message unless every line ends in a `\`
/// continuation, which is invisible in review and shows up as runs of spaces in
/// the API response.
fn force_upload_gate_open_message(
    drain_state: Option<NodeDrainState>,
) -> (&'static str, &'static str) {
    match drain_state {
        None => (
            "conflict: node is not draining",
            "node is not in a drain phase; force-upload-local-snapshots is only \
             valid while the drain gate is blocking concurrent user wake",
        ),
        Some(_) => (
            "conflict: node is draining gently, so the wake gate is open",
            "node is draining in gentle mode, which deliberately keeps serving \
             user traffic, so the drain gate is not blocking wakes. The legacy \
             snapshot reupload this action performs reads the live on-disk \
             snapshot directory and a concurrent wake plus re-suspend would let \
             it register a torn snapshot. Escalate the drain to forced (or wait \
             for `drained`) before forcing uploads.",
        ),
    }
}

async fn apply_force_upload_local_snapshots(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    node_id: NodeId,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    if let Some(refusal) = refuse_without_node_drain(
        &state,
        &actor,
        node_id,
        DrainAction::ForceUploadLocalSnapshots,
        None,
        None,
    ) {
        return refusal;
    }

    let store = AdminNodeStore::new(state.pool.clone());
    let node = match store.fetch(node_id).await {
        Ok(Some(node)) => node,
        Ok(None) => return api_error(StatusCode::NOT_FOUND, "Node not found").into_response(),
        Err(err) => return db_error(err).into_response(),
    };

    // The byte-stability invariant the per-RPC reupload depends on ("the snapshot
    // files on /space/cache/<sandbox>/snapshot/ do not change during the upload")
    // comes from the user-API drain gate refusing
    // `start`/`wake`/`resume`/`restore` for this node. If the gate is open, a
    // concurrent wake can touch the live VM and re-suspend with new bytes while
    // the durable upload is reading them, registering a torn snapshot as the
    // canonical S3 copy.
    //
    // So the precondition is not "the node is draining" — it is "the gate is
    // currently closed", which is exactly `NodeDrainState::blocks_placement`.
    // Those coincided until drain modes existed. They no longer do: a **gentle**
    // drain leaves the gate open on purpose, so it fails this check even though
    // its `drain_phase` is non-NULL. `drained` and `terminating` still pass in
    // either mode. Sharing the predicate with the gate is what keeps the two from
    // drifting — the whole hazard here is a reader that thinks wakes are blocked
    // when they aren't.
    let drain_state = match node
        .drain_phase
        .as_deref()
        .map(NodeDrainPhase::parse)
        .transpose()
    {
        Ok(phase) => NodeDrainState::from_columns(phase, node.drain_mode.as_deref()),
        Err(err) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("node has an unrecognized drain phase: {err}"),
            )
            .into_response();
        }
    };
    if !drain_state.is_some_and(NodeDrainState::blocks_placement) {
        let (error, detail) = force_upload_gate_open_message(drain_state);
        audit::record(AdminAuditEntry {
            actor: &actor,
            action: "force-upload-local-snapshots",
            node_id,
            node_name: node.substrate_node_id.as_deref(),
            from_phase: node.drain_phase.as_deref(),
            to_phase: None,
            from_drain_mode: None,
            replace_after_terminate: None,
            drain_mode: drain_state.map(|state| state.mode),
            success: false,
            error: Some(error),
            outcome_counts: None,
        });
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": detail })),
        )
            .into_response();
    }

    // `LIMIT cap + 1` so a returned slice of length `cap + 1` is the
    // unambiguous "more pinned sandboxes exist beyond what's listed"
    // signal — we trim the sentinel row and flip `truncated: true`
    // on the response.
    // `cap + 1` so a full page is distinguishable from a page that merely
    // filled the cap exactly, which is what flips `truncated` below.
    let enumeration_limit = NonZeroUsize::new(FORCE_UPLOAD_ENUMERATION_CAP + 1)
        .expect("FORCE_UPLOAD_ENUMERATION_CAP + 1 is a positive constant");
    let pinned =
        match node_store::list_pinned_local_only_sandboxes(&state.pool, node_id, enumeration_limit)
            .await
        {
            Ok(rows) => rows,
            Err(err) => return db_error(err).into_response(),
        };

    let mut response = ForceUploadLocalSnapshotsResponse {
        node_id,
        ..Default::default()
    };

    if pinned.is_empty() {
        // No local-only pins — every suspended sandbox already has a
        // registered S3 snapshot (or has no pin at all). 200 success so
        // the orchestrator's retry path becomes a fast idempotent ping.
        audit::record(AdminAuditEntry {
            actor: &actor,
            action: "force-upload-local-snapshots",
            node_id,
            node_name: node.substrate_node_id.as_deref(),
            from_phase: node.drain_phase.as_deref(),
            to_phase: node.drain_phase.as_deref(),
            from_drain_mode: None,
            replace_after_terminate: None,
            drain_mode: None,
            success: true,
            error: None,
            outcome_counts: Some("reuploaded=0 failed=0 deferred=0 truncated=false"),
        });
        return Json(response).into_response();
    }

    // The query asked for `ENUMERATION_CAP + 1` rows. A returned
    // slice of length `ENUMERATION_CAP + 1` means more pinned
    // sandboxes exist beyond what we can list — drop the sentinel
    // tail row and flip `truncated: true` so the operator knows
    // re-invoking will keep finding new work after the listed
    // sandboxes clear.
    let mut pinned = pinned;
    if pinned.len() > FORCE_UPLOAD_ENUMERATION_CAP {
        pinned.truncate(FORCE_UPLOAD_ENUMERATION_CAP);
        response.truncated = true;
    }
    let (to_process, deferred): (Vec<SandboxId>, Vec<SandboxId>) =
        if pinned.len() > FORCE_UPLOAD_MAX_PER_INVOCATION {
            let split = FORCE_UPLOAD_MAX_PER_INVOCATION;
            let deferred = pinned.split_off(split);
            (pinned, deferred)
        } else {
            (pinned, Vec::new())
        };
    response.deferred = deferred;

    let target = NodeApiTarget {
        managed_by: node.managed_by,
        role: node.role,
        direct_endpoint: node.node_api_endpoint.as_deref(),
        substrate_node_id: node.substrate_node_id.as_deref(),
    };
    let target_ref = &target;
    let controller_ref = &state.sandbox_controller;
    let pool_ref = &state.pool;
    use futures_util::stream::{self, StreamExt};
    // Per-RPC wake-gate re-check closes a TOCTOU between the up-front guard and
    // the per-sandbox RPC dispatch: a concurrent `:undrain` — or an `:undrain`
    // followed by a *gentle* `:drain`, which leaves the phase non-NULL while
    // reopening the gate — could otherwise let the user wake a sandbox while we
    // still have uploads in flight. It asks the same question as the entry guard,
    // through the same predicate, for exactly that reason. Cheap indexed PG
    // point-read; the short-circuit almost never trips, and when it does the
    // remaining sandboxes are reported in `failed[]` so the operator can
    // re-invoke or accept the abort.
    let results: Vec<(SandboxId, Result<(), String>)> = stream::iter(to_process)
        .map(|sandbox_id| async move {
            match node_store::is_node_wake_gate_closed(pool_ref, node_id).await {
                Ok(true) => {}
                Ok(false) => {
                    return (
                        sandbox_id,
                        Err(
                            "aborted: the node's wake gate opened during fan-out — it was \
                             undrained, or re-drained in gentle mode, which keeps serving \
                             user traffic. Re-invoke once the node is draining in forced \
                             mode (or drained), or accept that the local-only snapshot \
                             will be reuploaded by the next drain cycle"
                                .to_string(),
                        ),
                    );
                }
                Err(err) => {
                    return (
                        sandbox_id,
                        Err(format!(
                            "aborted: failed to revalidate the wake gate: {err}"
                        )),
                    );
                }
            }
            let typed = sandbox_id;
            let fut = controller_ref.enqueue_suspended_snapshot_backup(typed, target_ref);
            let outcome = match tokio::time::timeout(FORCE_UPLOAD_RPC_TIMEOUT, fut).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => Err(err.to_string()),
                Err(_) => Err(format!(
                    "timeout after {:?} contacting on-node sbx-mgr; \
                     the reconciler retries automatically on its next tick",
                    FORCE_UPLOAD_RPC_TIMEOUT,
                )),
            };
            (sandbox_id, outcome)
        })
        .buffer_unordered(FORCE_UPLOAD_RPC_CONCURRENCY)
        .collect()
        .await;
    let node_id_str = node_id.to_string();
    for (sandbox_id, outcome) in results {
        match outcome {
            Ok(()) => {
                response.reuploaded.push(sandbox_id);
                pplx_space_metrics::node_drain::local_snapshot_promotion(
                    &node_id_str,
                    "reuploaded",
                );
            }
            Err(err) => {
                // Both markers carry the same terminal classification —
                // a reupload can't ever succeed from this node, so the
                // controller should pick the terminal path rather than
                // retry. Keeping the classifier in sync with the
                // single-source-of-truth vocabulary documented in
                // `node_drain_signals` avoids drifting metrics +
                // operator-facing outcomes once the on-node RPC starts
                // emitting both strings.
                let outcome_tag = if err.contains(
                    pplx_space_common_core::node_drain_signals::REUPLOAD_ARTIFACT_MISSING_MARKER,
                ) || err.contains(
                    pplx_space_common_core::node_drain_signals::REUPLOAD_SANDBOX_UNKNOWN_MARKER,
                ) {
                    "failed_artifact_missing"
                } else {
                    "failed"
                };
                pplx_space_metrics::node_drain::local_snapshot_promotion(&node_id_str, outcome_tag);
                response.failed.push(ForceUploadFailure {
                    sandbox_id,
                    error: err,
                });
            }
        }
    }

    let success = response.failed.is_empty();
    // Pre-render the per-invocation outcome counts so the audit
    // trail tells an operator whether a prior call drained the
    // pinned-list or just chipped at it (non-empty `deferred` /
    // `truncated: true` means re-invoke).
    let outcome_counts = format!(
        "reuploaded={} failed={} deferred={} truncated={}",
        response.reuploaded.len(),
        response.failed.len(),
        response.deferred.len(),
        response.truncated,
    );
    audit::record(AdminAuditEntry {
        actor: &actor,
        action: "force-upload-local-snapshots",
        node_id,
        node_name: node.substrate_node_id.as_deref(),
        from_phase: node.drain_phase.as_deref(),
        to_phase: node.drain_phase.as_deref(),
        from_drain_mode: None,
        replace_after_terminate: None,
        drain_mode: None,
        success,
        error: if success {
            None
        } else {
            Some("one or more sandboxes failed force-upload")
        },
        outcome_counts: Some(outcome_counts.as_str()),
    });

    Json(response).into_response()
}

/// Postgres-backed tests that drive [`apply_drain_action`] itself, rather than
/// the pure helpers it composes.
///
/// Eight values on this handler were found mutation-silent inline and extracted
/// into named, tested functions. That closed each instance and none of the
/// class: the *compositions* — which helper feeds which argument, and what the
/// call site does with the result — are reachable only through the handler, and
/// mutations there survive the whole pure-function suite in [`tests`].
#[cfg(all(test, feature = "kubernetes"))]
mod drain_handler_tests;

#[cfg(test)]
mod tests;
