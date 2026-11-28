//! Cleanup: the drained tick's count, the terminate transaction's fence,
//! dispatch, delete and let-go.

use super::*;

/// The `drain_last_error` of a `terminating` node no authority terminates.
pub const NOT_AWS_TERMINATOR: &str = "a v2 node is terminated only through the aws terminator (SPACE_NODE_DRAIN_TERMINATOR_KIND=aws)";

/// How every reason a terminate returned the node to `drained` starts: the
/// status the `drained` node then keeps until the next `:terminate` or
/// `:undrain`, unless the controller finds the node's machine terminated
/// and waits on it.
pub(super) const TERMINATE_REFUSED: &str = "terminate refused";

/// Why a terminate returned a node that records no EC2 instance to
/// `drained`.
pub(super) const NO_INSTANCE_TO_TERMINATE: &str =
    "terminate refused: the node records no EC2 instance to terminate";

/// When each fenced node is next asked about: its EC2 check, and the
/// dispatch it may lead to, wait out a backoff that doubles from its base
/// ([`V2LoopConfig::dispatch_backoff_base`]) to [`DISPATCH_BACKOFF_CAP`]. In
/// memory only: a controller that restarts asks again at once, and only a
/// crash, a lost `dispatched_at` write or an unknown outcome repeats a
/// dispatch.
#[derive(Debug)]
pub(super) struct DispatchBackoff {
    base: Duration,
    next: Mutex<HashMap<NodeId, (Instant, u32)>>,
}

impl DispatchBackoff {
    pub(super) fn new(base: Duration) -> Self {
        Self {
            base,
            next: Mutex::default(),
        }
    }

    pub(super) fn due(&self, node_id: NodeId, now: Instant) -> bool {
        self.next
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&node_id)
            .is_none_or(|(at, _)| *at <= now)
    }

    /// Wait out the next backoff before asking about `node_id` again.
    pub(super) fn defer(&self, node_id: NodeId, now: Instant) {
        let mut next = self
            .next
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let attempts = next.get(&node_id).map_or(0, |(_, attempts)| *attempts);
        let wait = self
            .base
            .saturating_mul(2_u32.saturating_pow(attempts.min(16)))
            .min(DISPATCH_BACKOFF_CAP);
        next.insert(node_id, (now + wait, attempts.saturating_add(1)));
    }

    /// Keep only the fenced nodes' entries.
    pub(super) fn retain(&self, fenced: &HashSet<NodeId>) {
        self.next
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|node_id, _| fenced.contains(node_id));
    }
}

/// What the terminate transaction did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fence {
    /// The fence is written: the instance to terminate.
    Fenced(String),
    /// Rows or executions still name the node; the blockers are in
    /// `drain_last_error`.
    Held(String),
    /// The node records no instance to terminate.
    NoInstance,
    /// The row is no longer the generation's unfenced `terminating` v2
    /// node.
    Moved,
}

/// What the delete of a terminated node's row did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TerminatedDelete {
    /// The row is gone, with no tombstone.
    Deleted,
    /// A row or execution guard refused the delete, so the drain let the
    /// node go: the trace it recorded.
    LetGo(String),
    /// The row no longer carries the fence: a registration took it for
    /// another instance. Nothing to do.
    Unfenced,
    /// Not yet, and why: a fresh heartbeat, a moved identity or phase.
    Wait(String),
}

impl V2Node {
    /// Auto Scaling accepted the fenced instance's termination.
    pub(super) const fn dispatched(&self) -> bool {
        self.dispatched_at.is_some()
    }

    /// `terminating` with its fence written.
    pub(super) fn fenced(&self) -> Option<&str> {
        self.fence
            .as_deref()
            .filter(|_| self.phase.as_deref() == Some("terminating"))
    }

    /// The instance to ask EC2 about: the fence once written, else the
    /// recorded instance.
    pub(super) fn asked_instance(&self) -> Option<&str> {
        self.fenced().or(self.instance_id.as_deref())
    }
}

/// Add every row still naming a `drained` node that the terminate would
/// release (suspended, durable, no restore or activation in flight) to its
/// drain's `evacuated_total`: a `drain_status` write only. `Err` with the
/// status when the count failed.
pub(super) async fn count_drained<S: DrainV2Sink>(
    sink: &S,
    node_id: NodeId,
    generation: u64,
) -> Result<(), String> {
    sink.count_drained_evacuations(node_id, generation)
        .await
        .map(|_| ())
        .map_err(|error| {
            format!("counting the drained node's durable rows evacuated failed: {error}")
        })
}

/// Run the terminate transaction of an unfenced `terminating` node whose
/// fresh check found its report finished. The transaction writes the
/// node's status with its outcome, the fence or what holds it; a node that
/// records no instance to terminate returns to `drained`.
pub(super) async fn fence<S: DrainV2Sink>(sink: &S, node: &V2Node, generation: u64) -> NodeOutcome {
    match sink.fence(node.id, generation).await {
        Ok(Fence::Fenced(instance_id)) => {
            tracing::info!(
                node_id = %node.id,
                instance_id,
                "node_drain v2: fenced the node's instance for termination"
            );
            NodeOutcome::moved(NodeStep::Fenced)
        }
        Ok(Fence::Held(_) | Fence::Moved) => NodeOutcome::kept(),
        Ok(Fence::NoInstance) => {
            return_drained(sink, node, generation, NO_INSTANCE_TO_TERMINATE).await
        }
        Err(error) => NodeOutcome::says(format!("the terminate transaction failed: {error}")),
    }
}

/// Return a `terminating` node to `drained`, recording `refused` with the
/// move ([`DrainV2Sink::return_drained`]): the operator retries
/// `:terminate`.
pub(super) async fn return_drained<S: DrainV2Sink>(
    sink: &S,
    node: &V2Node,
    generation: u64,
    refused: &str,
) -> NodeOutcome {
    match sink.return_drained(node.id, generation, refused).await {
        Ok(true) => {
            tracing::warn!(node_id = %node.id, refused, "node_drain v2: returned the node to drained");
            NodeOutcome::moved(NodeStep::Returned)
        }
        Ok(false) => NodeOutcome::kept(),
        Err(error) => NodeOutcome::says(format!(
            "{refused}; returning the node to drained failed: {error}"
        )),
    }
}

/// A fenced node, with EC2's answer for its fenced instance. The first
/// `terminated` answer is recorded; after it, an instance EC2 no longer
/// lists reads as terminated, since EC2 stops listing a terminated instance
/// after a while. Dispatch the termination while none is confirmed and EC2
/// lists the instance in a state other than `shutting-down` or
/// `terminated`; an instance it doesn't list, or no answer, dispatches
/// nothing. Once the instance reads terminated, delete the row if the
/// termination was confirmed and the Node is gone, or let the node go if it
/// wasn't, the node's heartbeat is stale and the machine EC2 answered about
/// is the registered one ([`unregistered_machine`]). Every path that waits
/// defers the next check from the clock after the calls it waited on, and
/// its outcome says what it waits for.
#[expect(
    clippy::too_many_arguments,
    reason = "the node, its fence and EC2's answer, and the loop's backoff and bounds"
)]
pub(super) async fn terminated_or_dispatch<S: DrainV2Sink, A: V2InstanceAuthority>(
    sink: &S,
    authority: &A,
    node: &V2Node,
    generation: u64,
    fence: &str,
    evidence: &InstanceEvidence,
    backoff: &DispatchBackoff,
    config: &V2LoopConfig,
) -> NodeOutcome {
    // Defer the node's next check, and say why it waits.
    let waits = |why: String| {
        backoff.defer(node.id, Instant::now());
        NodeOutcome::says(why)
    };
    if *evidence == InstanceEvidence::Terminated && node.terminated_observed_at.is_none() {
        match sink
            .record_terminated_observed(node.id, generation, fence)
            .await
        {
            Ok(true) => {}
            Ok(false) => return NodeOutcome::kept(),
            Err(error) => {
                return waits(format!(
                    "recording that {fence} is terminated failed: {error}"
                ));
            }
        }
    }
    let terminated = match evidence {
        InstanceEvidence::Terminated => true,
        InstanceEvidence::NotListed => node.terminated_observed_at.is_some(),
        _ => false,
    };
    if !node.dispatched() {
        if terminated {
            if node.heartbeat_fresh {
                return waits(format!(
                    "fenced instance {fence} is terminated, but the node's heartbeat is fresh; \
                     not letting it go"
                ));
            }
            if let Some(why) = unregistered_machine(sink, node, fence).await {
                return waits(format!(
                    "fenced instance {fence} is terminated, but {why}; not letting it go"
                ));
            }
            let trace = format!(
                "v2 drain let go: fenced instance {fence} terminated; dispatch unconfirmed"
            );
            let outcome = let_go(sink, node, generation, &trace).await;
            if outcome.step.is_none() {
                backoff.defer(node.id, Instant::now());
            }
            return outcome;
        }
        let InstanceEvidence::Listed(_) = evidence else {
            return waits(if *evidence == InstanceEvidence::NotListed {
                format!(
                    "EC2 does not list fenced instance {fence}, and was never seen to list it \
                     terminated; not dispatching its termination"
                )
            } else {
                format!(
                    "EC2 answers {evidence} for fenced instance {fence}; not dispatching its \
                     termination"
                )
            });
        };
        let dispatched = authority
            .terminate_instance(fence, node.decrement_capacity)
            .await;
        backoff.defer(node.id, Instant::now());
        return match dispatched {
            // The dispatch's record clears the status with its write.
            Ok(()) => match sink.record_dispatched(node.id, generation, fence).await {
                Ok(true) => {
                    tracing::info!(
                        node_id = %node.id,
                        instance_id = fence,
                        "node_drain v2: Auto Scaling accepted the fenced instance's termination"
                    );
                    NodeOutcome::moved(NodeStep::Dispatched)
                }
                Ok(false) => NodeOutcome::kept(),
                Err(error) => {
                    NodeOutcome::says(format!("recording the dispatch of {fence} failed: {error}"))
                }
            },
            Err(error) => {
                crate::metrics::node_drain::v2_terminate_failure(&node.id.to_string());
                NodeOutcome::says(format!(
                    "terminating fenced instance {fence} failed: {error}"
                ))
            }
        };
    }
    if !terminated {
        // By the database clock the dispatch was recorded on.
        let elapsed = node
            .dispatched_at
            .map(|dispatched_at| node.db_now.saturating_sub(dispatched_at));
        let bound = i64::try_from(config.terminate_bound.as_secs()).unwrap_or(i64::MAX);
        let why = if *evidence == InstanceEvidence::NotListed {
            Some(format!(
                "EC2 does not list fenced instance {fence}, and was never seen to list it \
                 terminated; not deleting the row"
            ))
        } else if let InstanceEvidence::Unknown(_) = evidence {
            // No answer says nothing about the machine, however long ago the
            // dispatch was: say so on every check, not only past the bound.
            // The status names no age, so it changes only with the situation:
            // `drain_status.dispatched_at` carries the time.
            Some(format!(
                "the termination of fenced instance {fence} was dispatched; EC2 answers \
                 {evidence}, so the drain can't tell whether it is terminated; not deleting the \
                 row"
            ))
        } else {
            elapsed.filter(|elapsed| *elapsed >= bound).map(|_| {
                format!(
                    "Auto Scaling accepted the termination of {fence}; EC2 still answers \
                     {evidence} past the terminate bound"
                )
            })
        };
        backoff.defer(node.id, Instant::now());
        return why.map_or_else(NodeOutcome::default, NodeOutcome::says);
    }
    let Some(node_name) = node.k8s_node_name.as_deref() else {
        return waits("the node has no Kubernetes name to look up".to_owned());
    };
    match sink.node_exists(node_name).await {
        Ok(false) => {}
        Ok(true) => {
            return waits(format!(
                "fenced instance {fence} is terminated; waiting for its Kubernetes Node \
                 {node_name} to go"
            ));
        }
        Err(error) => {
            return waits(format!(
                "looking up the Kubernetes Node {node_name} failed: {error}"
            ));
        }
    }
    match sink.delete_terminated(node, generation, fence).await {
        Ok(TerminatedDelete::Deleted) => {
            crate::metrics::node_drain::clear_v2_gauges(&node.id.to_string());
            crate::metrics::node_drain::v2_node_deleted();
            tracing::info!(
                node_id = %node.id,
                instance_id = fence,
                "node_drain v2: deleted the row of the node whose instance it terminated"
            );
            NodeOutcome::moved(NodeStep::Deleted)
        }
        Ok(TerminatedDelete::LetGo(trace)) => {
            crate::metrics::node_drain::clear_v2_gauges(&node.id.to_string());
            crate::metrics::node_drain::v2_let_go("guard_refused");
            tracing::warn!(node_id = %node.id, trace, "node_drain v2: let the node go");
            NodeOutcome::moved(NodeStep::LetGo)
        }
        Ok(TerminatedDelete::Unfenced) => NodeOutcome::kept(),
        Ok(TerminatedDelete::Wait(why)) => waits(format!(
            "fenced instance {fence} is terminated; not deleting the row yet: {why}"
        )),
        Err(error) => waits(format!(
            "deleting the terminated node's row failed: {error}"
        )),
    }
}

/// Why the machine EC2 answered about as `instance` can't be taken for the
/// one the node registered, if it can't. The drain lets a node go on EC2's
/// `terminated` only when its Kubernetes Node is gone, or when the Node's
/// AWS `providerID` names `instance`: a recorded instance can be stale (an
/// older registration writer leaves it across a Node UID change, and a Node
/// that reports no instance never corrects it), and then EC2's answer is
/// about another machine. A live Node on another instance, or naming none,
/// or a lookup that fails, is a machine EC2 said nothing about, so the
/// drain waits.
pub(super) async fn unregistered_machine<S: DrainV2Sink>(
    sink: &S,
    node: &V2Node,
    instance: &str,
) -> Option<String> {
    let Some(node_name) = node.k8s_node_name.as_deref() else {
        return Some("the node has no Kubernetes name to look up".to_owned());
    };
    match sink.node_machine(node_name).await {
        Ok(NodeMachine::Gone) => None,
        Ok(NodeMachine::Live(Some(named))) if named == instance => None,
        Ok(NodeMachine::Live(Some(named))) => Some(format!(
            "its Kubernetes Node {node_name} is live on instance {named}"
        )),
        Ok(NodeMachine::Live(None)) => Some(format!(
            "its Kubernetes Node {node_name} is live and names no EC2 instance"
        )),
        Err(error) => Some(format!(
            "looking up the Kubernetes Node {node_name} failed: {error}"
        )),
    }
}

/// Let `node` go, recording `trace` with the move: `LetGo` once the row is
/// cleared, nothing when it moved on.
pub(super) async fn let_go<S: DrainV2Sink>(
    sink: &S,
    node: &V2Node,
    generation: u64,
    trace: &str,
) -> NodeOutcome {
    match sink.let_go(node, generation, trace).await {
        Ok(true) => {
            crate::metrics::node_drain::clear_v2_gauges(&node.id.to_string());
            crate::metrics::node_drain::v2_let_go(if node.fenced().is_some() {
                "dispatch_unconfirmed"
            } else {
                "terminated_outside_the_drain"
            });
            tracing::warn!(node_id = %node.id, trace, "node_drain v2: let the node go");
            NodeOutcome::moved(NodeStep::LetGo)
        }
        Ok(false) => NodeOutcome::kept(),
        Err(error) => NodeOutcome::says(format!("letting the node go failed: {error}")),
    }
}
