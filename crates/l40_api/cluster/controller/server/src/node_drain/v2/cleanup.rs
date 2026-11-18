//! Cleanup: the terminate transaction's fence, dispatch, delete and let-go.

use super::*;

/// The `drain_last_error` of a `terminating` node no authority terminates.
pub const REFUSED_TERMINATE: &str = "a v2 node is terminated only through the aws terminator (SPACE_NODE_DRAIN_TERMINATOR_KIND=aws)";

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

/// Run the terminate transaction of an unfenced `terminating` node whose
/// fresh check found nothing pending or blocked.
pub(super) async fn fence<S: DrainV2Sink>(
    sink: &S,
    node_id: NodeId,
    generation: u64,
) -> Option<NodeStep> {
    match sink.fence(node_id, generation).await {
        Ok(Fence::Fenced(instance_id)) => {
            tracing::info!(
                node_id = %node_id,
                instance_id,
                "node_drain v2: fenced the node's instance for termination"
            );
            Some(NodeStep::Fenced)
        }
        Ok(Fence::Held(_) | Fence::Moved) => None,
        Err(error) => {
            sink.record_error(
                node_id,
                &format!("the terminate transaction failed: {error}"),
            )
            .await;
            None
        }
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
/// defers the next check.
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
) -> Option<NodeStep> {
    let now = Instant::now();
    if *evidence == InstanceEvidence::Terminated && node.terminated_observed_at.is_none() {
        match sink
            .record_terminated_observed(node.id, generation, fence)
            .await
        {
            Ok(true) => {}
            Ok(false) => return None,
            Err(error) => {
                backoff.defer(node.id, now);
                sink.record_error(
                    node.id,
                    &format!("recording that {fence} is terminated failed: {error}"),
                )
                .await;
                return None;
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
                backoff.defer(node.id, now);
                sink.record_error(
                    node.id,
                    &format!(
                        "fenced instance {fence} is terminated, but the node's heartbeat is \
                         fresh; not letting it go"
                    ),
                )
                .await;
                return None;
            }
            if let Some(why) = unregistered_machine(sink, node, fence).await {
                backoff.defer(node.id, now);
                sink.record_error(
                    node.id,
                    &format!("fenced instance {fence} is terminated, but {why}; not letting it go"),
                )
                .await;
                return None;
            }
            let trace = format!(
                "v2 drain let go: fenced instance {fence} terminated; dispatch unconfirmed"
            );
            let gone = let_go(sink, node, generation, &trace).await;
            if !gone {
                backoff.defer(node.id, now);
            }
            return gone.then_some(NodeStep::LetGo);
        }
        let InstanceEvidence::Listed(_) = evidence else {
            backoff.defer(node.id, now);
            let why = if *evidence == InstanceEvidence::NotListed {
                format!(
                    "EC2 does not list fenced instance {fence}, and was never seen to list it \
                     terminated; not dispatching its termination"
                )
            } else {
                format!(
                    "EC2 answers {evidence} for fenced instance {fence}; not dispatching its \
                     termination"
                )
            };
            sink.record_error(node.id, &why).await;
            return None;
        };
        backoff.defer(node.id, now);
        return match authority
            .terminate_instance(fence, node.decrement_capacity)
            .await
        {
            Ok(()) => match sink.record_dispatched(node.id, generation, fence).await {
                Ok(recorded) => {
                    if recorded {
                        tracing::info!(
                            node_id = %node.id,
                            instance_id = fence,
                            "node_drain v2: Auto Scaling accepted the fenced instance's termination"
                        );
                    }
                    recorded.then_some(NodeStep::Dispatched)
                }
                Err(error) => {
                    sink.record_error(
                        node.id,
                        &format!("recording the dispatch of {fence} failed: {error}"),
                    )
                    .await;
                    None
                }
            },
            Err(error) => {
                crate::metrics::node_drain::v2_terminate_failure(&node.id.to_string());
                sink.record_error(
                    node.id,
                    &format!("terminating fenced instance {fence} failed: {error}"),
                )
                .await;
                None
            }
        };
    }
    if !terminated {
        backoff.defer(node.id, now);
        // By the database clock the dispatch was recorded on.
        let elapsed = node
            .dispatched_at
            .map(|dispatched_at| node.db_now.saturating_sub(dispatched_at));
        let bound = i64::try_from(config.terminate_bound.as_secs()).unwrap_or(i64::MAX);
        if *evidence == InstanceEvidence::NotListed {
            sink.record_error(
                node.id,
                &format!(
                    "EC2 does not list fenced instance {fence}, and was never seen to list it \
                     terminated; not deleting the row"
                ),
            )
            .await;
        } else if let InstanceEvidence::Unknown(_) = evidence {
            // No answer says nothing about the machine, however long ago the
            // dispatch was: say so on every check, not only past the bound.
            let since = elapsed.map_or_else(String::new, |elapsed| {
                format!(" {} minutes ago", elapsed / 60)
            });
            sink.record_error(
                node.id,
                &format!(
                    "the termination of fenced instance {fence} was dispatched{since}; EC2 \
                     answers {evidence}, so the drain can't tell whether it is terminated; \
                     not deleting the row"
                ),
            )
            .await;
        } else if let Some(elapsed) = elapsed.filter(|elapsed| *elapsed >= bound) {
            sink.record_error(
                node.id,
                &format!(
                    "Auto Scaling accepted the termination of {fence}; EC2 still answers \
                     {evidence} after {} minutes",
                    elapsed / 60
                ),
            )
            .await;
        }
        return None;
    }
    let Some(node_name) = node.k8s_node_name.as_deref() else {
        backoff.defer(node.id, now);
        sink.record_error(node.id, "the node has no Kubernetes name to look up")
            .await;
        return None;
    };
    match sink.node_exists(node_name).await {
        Ok(false) => {}
        Ok(true) => {
            backoff.defer(node.id, now);
            sink.record_error(
                node.id,
                &format!(
                    "fenced instance {fence} is terminated; waiting for its Kubernetes Node \
                     {node_name} to go"
                ),
            )
            .await;
            return None;
        }
        Err(error) => {
            backoff.defer(node.id, now);
            sink.record_error(
                node.id,
                &format!("looking up the Kubernetes Node {node_name} failed: {error}"),
            )
            .await;
            return None;
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
            Some(NodeStep::Deleted)
        }
        Ok(TerminatedDelete::LetGo(trace)) => {
            crate::metrics::node_drain::clear_v2_gauges(&node.id.to_string());
            crate::metrics::node_drain::v2_let_go("guard_refused");
            tracing::warn!(node_id = %node.id, trace, "node_drain v2: let the node go");
            Some(NodeStep::LetGo)
        }
        Ok(TerminatedDelete::Unfenced) => None,
        Err(error) => {
            backoff.defer(node.id, now);
            sink.record_error(
                node.id,
                &format!("deleting the terminated node's row failed: {error}"),
            )
            .await;
            None
        }
    }
}

/// Let `node` go, recording `trace`. Returns whether the row was cleared.
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

pub(super) async fn let_go<S: DrainV2Sink>(
    sink: &S,
    node: &V2Node,
    generation: u64,
    trace: &str,
) -> bool {
    match sink.let_go(node, generation, trace).await {
        Ok(true) => {
            crate::metrics::node_drain::clear_v2_gauges(&node.id.to_string());
            crate::metrics::node_drain::v2_let_go(if node.fenced().is_some() {
                "dispatch_unconfirmed"
            } else {
                "terminated_outside_the_drain"
            });
            tracing::warn!(node_id = %node.id, trace, "node_drain v2: let the node go");
            true
        }
        Ok(false) => false,
        Err(error) => {
            sink.record_error(node.id, &format!("letting the node go failed: {error}"))
                .await;
            false
        }
    }
}
