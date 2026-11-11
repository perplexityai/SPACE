//! The public operation dispatcher's slot metrics.

use pplx_space_metrics_emit::gauge;

/// Executions the cluster-controller dispatcher is running, against its
/// `execution_concurrency` slot limit; and of those, the executions other
/// than teardowns, against the slots they may hold, every slot but the
/// teardown reservation. A backlog of other work saturates the second pair
/// while the first still shows the reserved slots as headroom.
pub fn dispatcher_slots(
    in_flight: usize,
    capacity: usize,
    other_in_flight: usize,
    other_capacity: usize,
) {
    gauge("operations.dispatcher.in_flight", in_flight as f64, &[]);
    gauge("operations.dispatcher.capacity", capacity as f64, &[]);
    gauge(
        "operations.dispatcher.other_in_flight",
        other_in_flight as f64,
        &[],
    );
    gauge(
        "operations.dispatcher.other_capacity",
        other_capacity as f64,
        &[],
    );
}
