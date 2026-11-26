//! The admin API's drain v2 rules: which `:drain`, `:undrain` and
//! `:terminate` requests a v2 drain takes and what they write, and the
//! preview call the admin API makes of a node.

use super::*;
use pplx_space_node_model::NodeRole;
use pplx_space_node_registration_store::drain_v2::StoredV2Status;

/// The flag that lets `:drain` start a v2 drain
/// ([`crate::controllers::DrainV2Switch`]). The API's stop gate doesn't read
/// it: it follows the drain a node records.
pub const DRAIN_V2_ENABLED_ENV: &str = crate::controllers::DrainV2Switch::ENV;

/// The environment variable that bounds how many nodes v2 drains evacuate
/// at once.
pub const DRAIN_V2_MAX_EVACUATING_NODES_ENV: &str = "SPACE_DRAIN_V2_MAX_EVACUATING_NODES";

/// How many nodes v2 drains evacuate at once by default
/// ([`DRAIN_V2_MAX_EVACUATING_NODES_ENV`]). Each node runs its own window of
/// suspends and uploads (sandbox-manager's `SPACE_DRAIN_NODE_WINDOW`, 8), so
/// 4 nodes put at most 32 sandboxes' suspends and uploads in flight across
/// the cluster, half the 64 the v1 drain shared with its restores.
pub const DEFAULT_DRAIN_V2_MAX_EVACUATING_NODES: usize = 4;

/// The largest cap [`DRAIN_V2_MAX_EVACUATING_NODES_ENV`] may set.
pub const MAX_DRAIN_V2_MAX_EVACUATING_NODES: usize = 1024;

/// The cap `raw`, [`DRAIN_V2_MAX_EVACUATING_NODES_ENV`]'s value, sets: unset
/// is the default; anything but a number from 1 to
/// [`MAX_DRAIN_V2_MAX_EVACUATING_NODES`] is refused while drain v2 is
/// `enabled`. With drain v2 off no `:drain` reads the cap, so an invalid
/// value must not stop the API from starting: it is logged once, here, and
/// the default kept.
pub fn drain_v2_max_evacuating_nodes(enabled: bool, raw: Option<&str>) -> Result<usize, String> {
    let Some(raw) = raw else {
        return Ok(DEFAULT_DRAIN_V2_MAX_EVACUATING_NODES);
    };
    let cap = raw
        .trim()
        .parse::<usize>()
        .ok()
        .filter(|cap| (1..=MAX_DRAIN_V2_MAX_EVACUATING_NODES).contains(cap))
        .ok_or_else(|| {
            format!(
                "{DRAIN_V2_MAX_EVACUATING_NODES_ENV} must be a number from 1 to \
                 {MAX_DRAIN_V2_MAX_EVACUATING_NODES}, not {raw:?}"
            )
        });
    match cap {
        Err(error) if !enabled => {
            tracing::warn!(
                %error,
                "{DRAIN_V2_ENABLED_ENV} is off, so the invalid evacuation cap is ignored"
            );
            Ok(DEFAULT_DRAIN_V2_MAX_EVACUATING_NODES)
        }
        cap => cap,
    }
}

/// The longest hold a v2 drain takes, a year: what bounds the deadline's
/// arithmetic.
pub(super) const MAX_HOLD_SECONDS: u64 = 365 * 24 * 60 * 60;

/// The stable `code` of the 409 a v2 drain's start gets while the cluster's
/// evacuation slots are all held.
pub(super) const EVACUATION_CAP_CODE: &str = "drain_v2_evacuation_cap";

/// The stable `code` of the 409 a v2 drain's start gets on a node whose
/// heartbeat is stale.
pub(super) const HEARTBEAT_STALE_CODE: &str = "drain_v2_node_heartbeat_stale";

/// How long a v2 drain refused at the cap is told to wait before it asks
/// again: a slot frees as a drain reaches `drained` or is undrained.
pub(super) const EVACUATION_CAP_RETRY_AFTER: std::time::Duration =
    std::time::Duration::from_secs(60);

/// How long a v2 drain refused on a node whose heartbeat is stale is told to
/// wait before it asks again.
pub(super) const HEARTBEAT_STALE_RETRY_AFTER: std::time::Duration =
    std::time::Duration::from_secs(60);

/// The stable `code` of the 409 a v2 drain's start gets on a node whose own
/// preview lists a sandbox Postgres holds stopped.
pub(super) const STOPPED_SANDBOXES_CODE: &str = "drain_v2_stopped_sandboxes";

/// The most stopped sandbox ids the 409 of [`STOPPED_SANDBOXES_CODE`] names.
const STOPPED_SANDBOXES_SAMPLE: usize = 20;

/// How long the start of a v2 drain waits for the node's preview, connect
/// and call, before it skips the check of its stopped sandboxes.
pub(super) const STOPPED_CHECK_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

/// The stable `code` of the 409 a v1 `:drain` gets while drain v2 is on.
pub(super) const V1_DRAIN_REFUSED_CODE: &str = "drain_v1_refused_while_v2_enabled";

/// The `code` of `:uncordon`'s 409 while a v2 drain's cancel is pending:
/// the node may still be sealed, and the cluster controller uncordons it
/// once the cancel lands.
pub(super) const DRAIN_V2_CANCEL_PENDING_CODE: &str = "drain_v2_cancel_pending";

/// The 409 of a `:drain` while an undrained v2 drain's cancel hasn't finished
/// on the node: it may still be sealed under that drain's generation.
pub(super) const UNDRAIN_STILL_FINISHING: &str = "undrain still finishing: the previous v2 drain's cancel hasn't reached the node yet; \
     retry once the node's drain_engine is gone";

/// What a request does to the drain v2 columns, beside its phase write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DrainV2Write {
    None,
    /// Record the engine, the generation and the deadline of the drain this
    /// request starts, `hold_seconds` after its start, and clear the mode;
    /// the start takes one of the cluster's evacuation slots first
    /// ([`v2_start_refusal`]).
    Start {
        hold_seconds: i64,
    },
    /// Move the deadline of the v2 drain in flight to `hold_seconds` after
    /// its start, only earlier.
    Shorten {
        hold_seconds: i64,
    },
    /// An undrain of a v2 drain: the phase write leaves the engine and the
    /// status, a v2 engine with no phase, which is the cancel the controller
    /// finishes on the node. Nothing else is written, and the node keeps its
    /// cordon until then.
    Cancel,
}

/// Decide a request's drain v2 write, or refuse it. `requested` is the
/// engine the request names; `hold_seconds` its hold; `mode` the mode it
/// asked for. `starts_a_drain` is whether the transition starts a drain;
/// `node_engine` is the engine the node's drain runs under;
/// `recorded_instance` is whether the node's EC2 instance is recorded. A v2
/// drain needs a hold to start, and a repeat may carry one to move its
/// deadline earlier; a hold on any other drain is refused, and so is
/// `mode: forced` on a v2 drain, which `hold_seconds: 0` stands for.
#[expect(
    clippy::too_many_arguments,
    reason = "each is one fact of the request or the locked row the decision reads"
)]
pub(super) fn drain_v2_write(
    action: DrainAction,
    requested: Option<NodeDrainEngine>,
    hold_seconds: Option<u64>,
    mode: Option<NodeDrainMode>,
    managed_by: NodeManagedBy,
    role: NodeRole,
    node_engine: NodeDrainEngine,
    recorded_instance: bool,
    starts_a_drain: bool,
    enabled: bool,
) -> Result<DrainV2Write, (StatusCode, &'static str)> {
    // The drain this request runs under v2: one it names v2, or the v2 drain
    // in flight it repeats without naming an engine.
    let runs_v2 = match requested {
        Some(NodeDrainEngine::V2) => true,
        Some(NodeDrainEngine::V1) => false,
        None => !starts_a_drain && node_engine == NodeDrainEngine::V2,
    };
    let is_drain = matches!(action, DrainAction::Drain);
    let hold = match hold_seconds {
        None => None,
        Some(_) if !(is_drain && runs_v2) => {
            return Err((
                StatusCode::BAD_REQUEST,
                "hold_seconds is for a drain with engine v2 only",
            ));
        }
        Some(hold) if hold > MAX_HOLD_SECONDS => {
            return Err((
                StatusCode::BAD_REQUEST,
                "hold_seconds must be at most 31536000, a year",
            ));
        }
        Some(hold) => Some(i64::try_from(hold).unwrap_or(i64::MAX)),
    };
    if is_drain && runs_v2 && mode == Some(NodeDrainMode::Forced) {
        return Err((
            StatusCode::BAD_REQUEST,
            "a v2 drain takes no mode: drain with hold_seconds 0 to suspend its running \
             sandboxes at once",
        ));
    }
    match action {
        DrainAction::Drain => match requested {
            Some(NodeDrainEngine::V2) => {
                if !enabled {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        "drain engine v2 is not enabled on this cluster",
                    ));
                }
                drain_v2_drains_substrate(managed_by)?;
                if starts_a_drain && node_engine == NodeDrainEngine::V2 {
                    return Err((StatusCode::CONFLICT, UNDRAIN_STILL_FINISHING));
                }
                if starts_a_drain {
                    drain_v2_drains_workers(role)?;
                    drain_v2_needs_instance(recorded_instance)?;
                    let hold_seconds = hold.ok_or((
                        StatusCode::BAD_REQUEST,
                        "a drain with engine v2 needs hold_seconds: how long its running \
                         sandboxes may keep running, 0 for none",
                    ))?;
                    Ok(DrainV2Write::Start { hold_seconds })
                } else if node_engine == NodeDrainEngine::V2 {
                    Ok(
                        hold.map_or(DrainV2Write::None, |hold_seconds| DrainV2Write::Shorten {
                            hold_seconds,
                        }),
                    )
                } else {
                    Err((StatusCode::CONFLICT, "a drain in flight keeps its engine"))
                }
            }
            Some(NodeDrainEngine::V1) if !starts_a_drain && node_engine == NodeDrainEngine::V2 => {
                Err((StatusCode::CONFLICT, "a drain in flight keeps its engine"))
            }
            // An undrained v2 drain whose cancel hasn't finished on the node
            // yet: the node may still be sealed under its generation.
            _ if starts_a_drain && node_engine == NodeDrainEngine::V2 => {
                Err((StatusCode::CONFLICT, UNDRAIN_STILL_FINISHING))
            }
            // A repeat of the v2 drain in flight that names no engine.
            _ if runs_v2 => {
                Ok(
                    hold.map_or(DrainV2Write::None, |hold_seconds| DrainV2Write::Shorten {
                        hold_seconds,
                    }),
                )
            }
            _ => Ok(DrainV2Write::None),
        },
        DrainAction::Undrain if node_engine == NodeDrainEngine::V2 => Ok(DrainV2Write::Cancel),
        _ => Ok(DrainV2Write::None),
    }
}

/// Refuse a v1 `:drain` while drain v2 is `enabled`: v1 and v2 drains never
/// run together. A v1 `:drain` is one that doesn't
/// run v2 (`requested`, the engine it names, isn't v2, and it doesn't repeat
/// the v2 drain in flight on a node whose engine, `node_engine`, is v2), on
/// any node, whatever its mode; a repeat of a v1 drain in flight is refused
/// too. `starts_a_drain` is whether the request starts a drain: the node has
/// no phase. The handler asks before any check of a v1 drain's terms, so a
/// v1 `:drain` always gets this answer.
/// `:undrain` and `:terminate` are never refused here. A 409 with a stable
/// `code`; `None` lets the request through.
pub(super) fn v1_drain_refusal(
    action: DrainAction,
    requested: Option<NodeDrainEngine>,
    node_engine: NodeDrainEngine,
    starts_a_drain: bool,
    enabled: bool,
) -> Option<axum::response::Response> {
    let runs_v2 = match requested {
        Some(NodeDrainEngine::V2) => true,
        Some(NodeDrainEngine::V1) => false,
        None => !starts_a_drain && node_engine == NodeDrainEngine::V2,
    };
    if !enabled || !matches!(action, DrainAction::Drain) || runs_v2 {
        return None;
    }
    Some(
        (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": format!(
                    "v1 drains are refused while drain v2 is on ({DRAIN_V2_ENABLED_ENV}): v1 and \
                     v2 drains never run together; drain with engine v2 and hold_seconds"
                ),
                "code": V1_DRAIN_REFUSED_CODE,
            })),
        )
            .into_response(),
    )
}

/// Refuse the start of a v2 drain of `node` (`:drain {engine: v2}` of a node
/// with no drain in flight) while the node holds a stopped sandbox: a v2
/// drain never moves one, and never terminates a node that holds one. The
/// node says what it holds: its preview ([`V2DrainProbe::preview`]) lists
/// each sandbox it would leave pending, and Postgres says which of those are
/// stopped ([`pplx_space_sandbox_store::drain_admission::stopped_among`]),
/// so a stopped row whose node id a replacement node reused, and which the
/// node doesn't hold, refuses nothing. The pending reasons are display text
/// and aren't read. A 409 with a stable `code`, the count and how to clear
/// it.
///
/// The caller asks only for a `:drain {engine: v2}` with drain v2 on; a
/// node with a drain in flight, a repeat among them, asks nothing. A
/// preview that doesn't answer within
/// [`STOPPED_CHECK_DEADLINE`] (the node unreachable, another preview in
/// progress, a node already sealed) skips the check, logged: the drain
/// itself reports a stopped sandbox pending, so it still never finishes.
pub(super) async fn v2_start_stopped_refusal(
    state: &AppState,
    node: &super::node_store::AdminNodeRow,
) -> StoppedCheck {
    let starts_a_drain = node.drain_phase.is_none() && node.drain_engine.is_none();
    if !starts_a_drain {
        return StoppedCheck::NotAStart;
    }
    // A builder's start is refused under the lock (`drain_v2_drains_workers`);
    // its node serves no preview to ask.
    if node.role != pplx_space_node_model::NodeRole::Worker {
        return StoppedCheck::Ran;
    }
    let target = crate::controllers::substrate::OwnedNodeApiTarget {
        managed_by: node.managed_by,
        role: node.role,
        direct_endpoint: node.node_api_endpoint.clone(),
        substrate_node_id: node.substrate_node_id.clone(),
    };
    let skipped = |why: String| {
        tracing::info!(
            node_id = %node.id,
            why,
            "drain v2: skipped the up-front check for stopped sandboxes; the drain reports any \
             it finds pending"
        );
        StoppedCheck::Ran
    };
    let report = match tokio::time::timeout(
        STOPPED_CHECK_DEADLINE,
        state.v2_drain_probe.preview(state, &target),
    )
    .await
    {
        Ok(Ok(preview)) => preview.report.unwrap_or_default(),
        Ok(Err(error)) => return skipped(format!("the node's preview failed: {error}")),
        Err(_) => {
            return skipped(format!(
                "the node's preview took over {STOPPED_CHECK_DEADLINE:?}"
            ));
        }
    };
    let pending: Vec<SandboxId> = report
        .pending
        .iter()
        .filter_map(|entry| SandboxId::from_str(&entry.sandbox_id).ok())
        .collect();
    if pending.is_empty() {
        return StoppedCheck::Ran;
    }
    let stopped =
        match pplx_space_sandbox_store::drain_admission::stopped_among(&state.pool, &pending).await
        {
            Ok(stopped) => stopped,
            Err(error) => return StoppedCheck::ReadFailed(sqlx::Error::from(error)),
        };
    if stopped.is_empty() {
        return StoppedCheck::Ran;
    }
    let count = stopped.len();
    let sandboxes = if count == 1 { "sandbox" } else { "sandboxes" };
    StoppedCheck::Refused(
        (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": format!(
                    "the node holds {count} stopped {sandboxes}, and a v2 drain doesn't move \
                     stopped sandboxes: delete them, or start them, then drain again"
                ),
                "code": STOPPED_SANDBOXES_CODE,
                "stopped": count,
                "stopped_sandbox_ids": stopped
                    .iter()
                    .take(STOPPED_SANDBOXES_SAMPLE)
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
            })),
        )
            .into_response(),
    )
}

/// What [`v2_start_stopped_refusal`] made of a `:drain {engine: v2}`.
pub(super) enum StoppedCheck {
    /// The node had a drain phase or engine when read: not a start.
    NotAStart,
    /// The check ran and found no stopped sandbox, or was skipped as its
    /// doc says (no preview within its deadline, or not a worker).
    Ran,
    /// The node holds stopped sandboxes: the 409 to answer.
    Refused(axum::response::Response),
    /// Postgres couldn't say which of the preview's sandboxes are stopped.
    ReadFailed(sqlx::Error),
}

/// Whether a v2 drain's start under the lock must be retried, since the
/// up-front stopped-sandbox check didn't run on it (`ran`): the node wasn't
/// a start candidate when read, and a cancel finished between that read and
/// the lock.
pub(super) fn start_needs_recheck(ran: bool, write: &DrainV2Write) -> bool {
    !ran && matches!(write, DrainV2Write::Start { .. })
}

/// Why the start of a v2 drain is refused, from what it read under the
/// cluster's evacuation lock: the node's heartbeat is stale, so a dead node
/// would hold a slot until EC2 said it was gone; or `max_evacuating` nodes'
/// v2 drains hold every slot. A 409 with a stable `code`, and, at the cap, a
/// `Retry-After` and what frees a slot.
pub(super) fn v2_start_refusal(
    admission: node_drain_v2::V2StartAdmission,
    max_evacuating: usize,
) -> Option<axum::response::Response> {
    if !admission.heartbeat_fresh {
        let mut response = (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "the node's heartbeat is stale: a v2 drain starts only on a node \
                          that answers, since its node does the drain",
                "code": HEARTBEAT_STALE_CODE,
            })),
        )
            .into_response();
        if let Some(value) =
            crate::controllers::retry_after_header_value(HEARTBEAT_STALE_RETRY_AFTER)
        {
            response
                .headers_mut()
                .insert(axum::http::header::RETRY_AFTER, value);
        }
        return Some(response);
    }
    let evacuating = usize::try_from(admission.evacuating).unwrap_or(usize::MAX);
    if evacuating < max_evacuating {
        return None;
    }
    let mut response = (
        StatusCode::CONFLICT,
        Json(serde_json::json!({
            "error": format!(
                "{evacuating} nodes' v2 drains hold every evacuation slot \
                 ({DRAIN_V2_MAX_EVACUATING_NODES_ENV} is {max_evacuating}); retry once one \
                 reaches drained, or free a slot sooner: shorten a drain in flight with \
                 hold_seconds 0, which frees its slot once it reaches drained, or undrain \
                 one, which frees it once its cancel finishes on the node"
            ),
            "code": EVACUATION_CAP_CODE,
            "evacuating": evacuating,
            "max_evacuating": max_evacuating,
        })),
    )
        .into_response();
    if let Some(value) = crate::controllers::retry_after_header_value(EVACUATION_CAP_RETRY_AFTER) {
        response
            .headers_mut()
            .insert(axum::http::header::RETRY_AFTER, value);
    }
    Some(response)
}

/// `:undrain` of a v2 node in `terminating`, which no other engine allows:
/// before the controller fences the instance, it moves the node to no phase
/// like any undrain, and the controller cancels the drain on the node; once
/// fenced, it is refused, since the termination may already be accepted.
/// The fence write takes the node's row lock, as this request does, so the
/// two are ordered. `None` for every other request, which
/// [`validate_drain_transition`] decides.
pub(super) fn v2_undrain_transition(
    action: DrainAction,
    phase: Option<&str>,
    columns: &node_drain_v2::DrainV2Columns,
) -> Option<Result<DrainTransition, TransitionError>> {
    if !matches!(action, DrainAction::Undrain)
        || phase != Some(PHASE_TERMINATING)
        || columns.engine != NodeDrainEngine::V2
    {
        return None;
    }
    Some(
        if columns.status.as_ref().is_some_and(StoredV2Status::fenced) {
            Err(TransitionError::Conflict {
                current: phase.map(str::to_owned),
                allowed: &[
                    PHASE_DRAINING,
                    PHASE_DRAINED,
                    "terminating (v2, before its instance is fenced)",
                ],
            })
        } else {
            Ok(DrainTransition::Move {
                new: None,
                set_started_at: false,
                clear_started_at: true,
            })
        },
    )
}

/// [`drain_v2_write`]'s refusal of a node a v2 drain can't run on by its
/// substrate: it drains Kubernetes nodes only.
pub(super) fn drain_v2_drains_substrate(
    managed_by: NodeManagedBy,
) -> Result<(), (StatusCode, &'static str)> {
    if managed_by == NodeManagedBy::Kubernetes {
        Ok(())
    } else {
        Err((
            StatusCode::CONFLICT,
            "drain engine v2 drains Kubernetes nodes only",
        ))
    }
}

/// [`drain_v2_write`]'s refusal to start a v2 drain on a node that isn't a
/// worker: the drain is the node's sandbox-manager's `DrainNode`, which a
/// builder's node-api doesn't serve.
pub(super) fn drain_v2_drains_workers(role: NodeRole) -> Result<(), (StatusCode, &'static str)> {
    if role == NodeRole::Worker {
        Ok(())
    } else {
        Err((
            StatusCode::CONFLICT,
            "drain engine v2 drains worker nodes only: a builder's node-api serves no DrainNode",
        ))
    }
}

/// [`drain_v2_write`]'s refusal to start a v2 drain on a node whose EC2
/// instance isn't recorded.
pub(super) fn drain_v2_needs_instance(
    recorded_instance: bool,
) -> Result<(), (StatusCode, &'static str)> {
    if recorded_instance {
        Ok(())
    } else {
        // The drain terminates exactly the recorded instance, and asks EC2
        // about it to tell a node that died.
        Err((
            StatusCode::CONFLICT,
            "drain engine v2 needs the node's EC2 instance recorded: the drain terminates that \
             instance, and asks EC2 about it when the node stops answering",
        ))
    }
}

/// Why `:drain` would refuse to start a v2 drain on a node whatever the
/// request asks, from the rules [`drain_v2_write`] applies: the node's
/// substrate, its role and its recorded instance. Whether v2 is enabled is
/// left out: that is the cluster's, not the node's.
pub(super) fn drain_v2_node_refusals(
    managed_by: NodeManagedBy,
    role: NodeRole,
    recorded_instance: bool,
) -> Vec<&'static str> {
    [
        drain_v2_drains_substrate(managed_by),
        drain_v2_drains_workers(role),
        drain_v2_needs_instance(recorded_instance),
    ]
    .into_iter()
    .filter_map(Result::err)
    .map(|(_, reason)| reason)
    .collect()
}

/// A node's answer to [`V2DrainProbe::preview`].
pub type V2DrainPreview<'a> = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<
                    pplx_space_sbx_mgr_client::proto::PreviewDrainNodeResponse,
                    DrainPreviewError,
                >,
            > + Send
            + 'a,
    >,
>;

/// The `PreviewDrainNode` call the node page and `:drain`'s up-front
/// stopped-sandbox check ask for. Production calls the
/// node through its node-api ([`NodeApiV2DrainProbe`]); tests script it.
/// The admin API makes no `DrainNode` call: only the cluster controller
/// drives a drain.
pub trait V2DrainProbe: Send + Sync {
    /// The node's answer to `PreviewDrainNode`: what an evacuating v2 drain
    /// would find on it now, with nothing changed on the node. The preview
    /// handler bounds it (`DRAIN_PREVIEW_DEADLINE` in
    /// `nodes/drain_preview.rs`).
    fn preview<'a>(
        &'a self,
        state: &'a AppState,
        target: &'a crate::controllers::substrate::OwnedNodeApiTarget,
    ) -> V2DrainPreview<'a>;
}

/// [`V2DrainProbe`] through the node's internal node-api.
pub struct NodeApiV2DrainProbe;

/// How long the `PreviewDrainNode` call waits for the node's answer, once
/// connected.
pub(super) const DRAIN_NODE_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

impl V2DrainProbe for NodeApiV2DrainProbe {
    fn preview<'a>(
        &'a self,
        state: &'a AppState,
        target: &'a crate::controllers::substrate::OwnedNodeApiTarget,
    ) -> V2DrainPreview<'a> {
        Box::pin(async move {
            let mut client =
                crate::controllers::substrate::connect_internal_node_api_client_for_target(
                    state.sandbox_controller.substrate(),
                    &target.as_target(),
                    DRAIN_NODE_PROBE_TIMEOUT,
                )
                .await
                .map_err(|error| DrainPreviewError::Unreachable(error.to_string()))?;
            client
                .preview_drain_node(pplx_space_sbx_mgr_client::proto::PreviewDrainNodeRequest {})
                .await
                .map(tonic::Response::into_inner)
                .map_err(DrainPreviewError::from_status)
        })
    }
}

/// RFC 3339 of Unix seconds `seconds`; `None` out of range.
fn unix_time(seconds: Option<i64>) -> Option<String> {
    seconds
        .and_then(|seconds| time::OffsetDateTime::from_unix_timestamp(seconds).ok())
        .map(format_time)
}

/// The admin node response's `drain` of a node: its v2 drain, read from the
/// typed `drain_status` ([`StoredV2Status`], the store's one shape of it)
/// into [`AdminNodeDrain`] for display. `None` without a v2 drain, and when
/// the stored state doesn't read as the controller writes it, which is
/// logged: the node's read goes on without it. A v2 engine with no phase is
/// a cancel pending.
pub(super) fn v2_drain_view(
    node_id: NodeId,
    engine: Option<&str>,
    phase: Option<&str>,
    status: Option<&serde_json::Value>,
) -> Option<AdminNodeDrain> {
    if NodeDrainEngine::from_column(engine) != NodeDrainEngine::V2 {
        return None;
    }
    let stored = match StoredV2Status::from_column(status) {
        Ok(stored) => stored?,
        Err(error) => {
            tracing::warn!(%node_id, %error, "a v2 drain's stored status doesn't read; the node's drain is left out");
            return None;
        }
    };
    let report = stored.report.as_ref();
    let unrecorded: std::collections::BTreeSet<&str> = report
        .iter()
        .flat_map(|report| &report.unrecorded)
        .map(|entry| entry.sandbox_id.as_str())
        .collect();
    let was_running: std::collections::BTreeSet<&str> =
        stored.observed_running.iter().map(String::as_str).collect();
    let text = |value: serde_json::Value| value.as_str().unwrap_or("unknown").to_owned();
    Some(AdminNodeDrain {
        generation: stored.generation,
        deadline: unix_time(Some(stored.deadline_at)),
        cancelling: phase.is_none(),
        fenced: stored.fenced(),
        reported_at: unix_time(report.map(|report| report.reported_at)),
        evacuated_grew_at: unix_time(report.map(|report| report.evacuated_grew_at)),
        evacuated_total: stored.evacuated_total.clone(),
        evacuated: report
            .iter()
            .flat_map(|report| &report.evacuated)
            .map(|entry| AdminNodeDrainEvacuated {
                sandbox_id: entry.sandbox_id.clone(),
                artifact_kind: text(serde_json::json!(entry.artifact_kind)),
                was_running: was_running.contains(entry.sandbox_id.as_str()),
                recorded: !unrecorded.contains(entry.sandbox_id.as_str()),
            })
            .collect(),
        pending: report
            .iter()
            .flat_map(|report| &report.pending)
            .map(|entry| AdminNodeDrainPending {
                sandbox_id: entry.sandbox_id.clone(),
                wait: text(serde_json::json!(entry.wait)),
                running: entry.running,
                pool_member: entry.pool_member,
                reason: (!entry.reason.is_empty()).then(|| entry.reason.clone()),
                since: unix_time(entry.since),
            })
            .collect(),
        blocked: report
            .iter()
            .flat_map(|report| &report.blocked)
            .map(|entry| AdminNodeDrainBlocked {
                reason: entry.reason.clone(),
            })
            .collect(),
    })
}
