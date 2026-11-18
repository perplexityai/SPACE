//! The admin API's drain v2 rules: which `:drain`, `:undrain` and
//! `:terminate` requests a v2 drain takes and what they write, and the node
//! calls the admin API makes of a v2 node.

use super::*;

/// The flag that lets `:drain` start a v2 drain.
pub const DRAIN_V2_ENABLED_ENV: &str = "SPACE_DRAIN_V2_ENABLED";

/// What a request does to the drain v2 columns, beside its phase write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DrainV2Write {
    None,
    /// Record the engine and the epoch of the drain this request starts, and
    /// a gentle one's deadline, in Unix seconds.
    Start {
        gentle_deadline: Option<i64>,
    },
    /// Mark the epoch for the controller to cancel on the node.
    Cancel,
}

/// A request's `deadline` and the drain's own, in Unix seconds, and now.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct DrainDeadlines<'a> {
    /// The request's, as it sent it.
    pub(super) requested: Option<&'a str>,
    /// The gentle v2 drain in flight's, `drain_status.deadline`.
    pub(super) stored: Option<i64>,
    pub(super) now: i64,
}

/// An RFC 3339 timestamp in Unix seconds.
pub(super) fn parse_deadline(raw: &str) -> Option<i64> {
    time::OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339)
        .ok()
        .map(time::OffsetDateTime::unix_timestamp)
}

/// Decide a request's drain v2 write, or refuse it. `starts_a_drain` is
/// whether the transition starts a drain; `node_engine` is the engine the
/// node's drain runs under; `recorded_instance` is whether the node's EC2
/// instance is recorded. A gentle v2 drain needs a deadline in the future to
/// start; a deadline on any other drain is refused, and one that disagrees
/// with the gentle v2 drain in flight is a conflict.
#[expect(
    clippy::too_many_arguments,
    reason = "each is one fact of the request or the locked row the decision reads"
)]
pub(super) fn drain_v2_write(
    action: DrainAction,
    requested: Option<NodeDrainEngine>,
    mode: Option<NodeDrainMode>,
    managed_by: NodeManagedBy,
    node_engine: NodeDrainEngine,
    recorded_instance: bool,
    starts_a_drain: bool,
    enabled: bool,
    deadlines: DrainDeadlines<'_>,
) -> Result<DrainV2Write, (StatusCode, &'static str)> {
    let gentle = mode == Some(NodeDrainMode::Gentle);
    // The drain this request runs under v2: one it names v2, or the v2 drain
    // in flight it repeats or escalates without naming an engine.
    let runs_v2 = match requested {
        Some(NodeDrainEngine::V2) => true,
        Some(NodeDrainEngine::V1) => false,
        None => !starts_a_drain && node_engine == NodeDrainEngine::V2,
    };
    let deadline = match deadlines.requested {
        None => None,
        Some(_) if !(matches!(action, DrainAction::Drain) && runs_v2 && gentle) => {
            return Err((
                StatusCode::BAD_REQUEST,
                "a deadline is for a gentle drain with engine v2 only",
            ));
        }
        Some(raw) => Some(parse_deadline(raw).ok_or((
            StatusCode::BAD_REQUEST,
            "the deadline must be an RFC 3339 timestamp",
        ))?),
    };
    let in_flight = |write: DrainV2Write| match deadline {
        Some(deadline) if deadlines.stored != Some(deadline) => Err((
            StatusCode::CONFLICT,
            "a gentle drain in flight keeps its deadline; escalate it with mode forced, or \
             :undrain and drain again",
        )),
        _ => Ok(write),
    };
    match action {
        DrainAction::Drain => match requested {
            Some(NodeDrainEngine::V2) => {
                if !enabled {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        "drain engine v2 is not enabled on this cluster",
                    ));
                }
                if managed_by != NodeManagedBy::Kubernetes {
                    return Err((
                        StatusCode::CONFLICT,
                        "drain engine v2 drains Kubernetes nodes only",
                    ));
                }
                if starts_a_drain && !recorded_instance {
                    // The drain terminates exactly the recorded instance,
                    // and asks EC2 about it to tell a node that died.
                    return Err((
                        StatusCode::CONFLICT,
                        "drain engine v2 needs the node's EC2 instance recorded: the drain \
                         terminates that instance, and asks EC2 about it when the node stops \
                         answering",
                    ));
                }
                if starts_a_drain {
                    let gentle_deadline = if gentle {
                        let deadline = deadline.ok_or((
                            StatusCode::BAD_REQUEST,
                            "a gentle drain with engine v2 needs a deadline, when it turns forced",
                        ))?;
                        if deadline <= deadlines.now {
                            return Err((
                                StatusCode::BAD_REQUEST,
                                "the deadline must be in the future",
                            ));
                        }
                        Some(deadline)
                    } else {
                        None
                    };
                    Ok(DrainV2Write::Start { gentle_deadline })
                } else if node_engine == NodeDrainEngine::V2 {
                    in_flight(DrainV2Write::None)
                } else {
                    Err((StatusCode::CONFLICT, "a drain in flight keeps its engine"))
                }
            }
            Some(NodeDrainEngine::V1) if !starts_a_drain && node_engine == NodeDrainEngine::V2 => {
                Err((StatusCode::CONFLICT, "a drain in flight keeps its engine"))
            }
            // An undrained v2 drain whose cancel hasn't reached the node yet:
            // a drain of another engine would inherit its epoch and cancel.
            _ if starts_a_drain && node_engine == NodeDrainEngine::V2 => Err((
                StatusCode::CONFLICT,
                "the previous v2 drain is still being cancelled on the node; retry, or drain \
                 with engine v2",
            )),
            _ => in_flight(DrainV2Write::None),
        },
        DrainAction::Undrain if node_engine == NodeDrainEngine::V2 => Ok(DrainV2Write::Cancel),
        _ => Ok(DrainV2Write::None),
    }
}

/// How many sandbox ids of each guard a refused v2 `:terminate` names.
pub(super) const V2_TERMINATE_REFERENCE_SAMPLE: u32 = 10;

/// `:undrain` of a v2 node in `terminating`, which no other engine allows:
/// before the controller fences the instance, it moves the node to no phase
/// like any undrain, and the controller cancels the epoch on the node; once
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
    Some(if node_drain_v2::drain_v2_fenced(columns.status.as_ref()) {
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
    })
}

/// What a fresh `DrainNode` call reported, as `:terminate` reads it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct V2DrainReport {
    pub sealed_epoch: Option<u64>,
    pub done: bool,
}

/// The `DrainNode` call `:terminate` makes of a v2 node under its drain's
/// epoch, right before it lets the node go. Production calls the node through
/// its node-api ([`NodeApiV2DrainProbe`]); tests script it.
pub trait V2DrainProbe: Send + Sync {
    fn drain_node<'a>(
        &'a self,
        state: &'a AppState,
        node_id: NodeId,
        epoch: u64,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<V2DrainReport, String>> + Send + 'a>,
    >;
}

/// [`V2DrainProbe`] through the node's internal node-api.
pub struct NodeApiV2DrainProbe;

/// How long `:terminate` waits for the node's answer; it answers without
/// waiting for anything.
pub(super) const DRAIN_NODE_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

impl V2DrainProbe for NodeApiV2DrainProbe {
    fn drain_node<'a>(
        &'a self,
        state: &'a AppState,
        node_id: NodeId,
        epoch: u64,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<V2DrainReport, String>> + Send + 'a>,
    > {
        Box::pin(async move {
            let target = node_store::node_api_target(&state.pool, node_id)
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| format!("node {node_id} not found"))?;
            let mut client =
                crate::controllers::substrate::connect_internal_node_api_client_for_target(
                    state.sandbox_controller.substrate(),
                    &target.as_target(),
                    DRAIN_NODE_PROBE_TIMEOUT,
                )
                .await
                .map_err(|error| error.to_string())?;
            let response = client
                .drain_node(pplx_space_sbx_mgr_client::proto::DrainNodeRequest {
                    epoch,
                    ..Default::default()
                })
                .await
                .map_err(|status| status.to_string())?
                .into_inner();
            Ok(V2DrainReport {
                sealed_epoch: response.sealed_epoch,
                done: response.done,
            })
        })
    }
}
