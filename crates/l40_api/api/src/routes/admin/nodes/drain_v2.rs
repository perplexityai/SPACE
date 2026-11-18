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
    /// Record the engine and the epoch of the drain this request starts.
    Start,
    /// Mark the epoch for the controller to cancel on the node.
    Cancel,
}

/// Decide a request's drain v2 write, or refuse it. `starts_a_drain` is
/// whether the transition starts a drain; `node_engine` is the engine the
/// node's drain runs under; `recorded_instance` is whether the node's EC2
/// instance is recorded.
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
) -> Result<DrainV2Write, (StatusCode, &'static str)> {
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
                if mode != Some(NodeDrainMode::Forced) {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        "drain engine v2 runs forced drains only; a gentle drain is :cordon",
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
                    Ok(DrainV2Write::Start)
                } else if node_engine == NodeDrainEngine::V2 {
                    Ok(DrainV2Write::None)
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
            _ => Ok(DrainV2Write::None),
        },
        DrainAction::Undrain if node_engine == NodeDrainEngine::V2 => Ok(DrainV2Write::Cancel),
        _ => Ok(DrainV2Write::None),
    }
}
