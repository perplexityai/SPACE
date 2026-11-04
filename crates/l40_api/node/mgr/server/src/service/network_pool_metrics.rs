use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{StreamExt, stream};
use pplx_space_metrics::node_manager::GuestUdp443Action;
use pplx_space_metrics::node_network::{self, NetworkSlotState};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::slot_pool::{NetworkPool, SlotState};

const EMIT_INTERVAL: Duration = Duration::from_secs(15);
const PROBE_CONCURRENCY: usize = 8;

pub fn start_network_pool_metrics(
    pool: Arc<NetworkPool>,
    shutdown_token: CancellationToken,
    task_tracker: &TaskTracker,
) {
    task_tracker.spawn(run_loop(pool, shutdown_token, probe_firewall_counters));
}

/// One lease's identity. A reject is logged once per lease so operators can
/// attribute `node_mgr.guest_udp443_packets{action:reject}` to a sandbox.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RejectingLease {
    sandbox_id: String,
    slot: u32,
    generation: u64,
}

struct GuestFirewallCounters {
    namespace: String,
    udp443: Option<(u64, GuestUdp443Action)>,
    vlan_packets: Option<u64>,
}

async fn run_loop<F, Fut>(pool: Arc<NetworkPool>, shutdown_token: CancellationToken, probe: F)
where
    F: Fn(String, CancellationToken) -> Fut,
    Fut: std::future::Future<Output = Option<GuestFirewallCounters>>,
{
    let mut interval = tokio::time::interval(EMIT_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut previous_vlan_packets = HashMap::<String, u64>::new();
    let mut reported_udp443_rejects = HashSet::<RejectingLease>::new();
    loop {
        tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => return,
            _ = interval.tick() => {},
        }
        let pass = async {
            let pool = pool.clone();
            let records = match tokio::task::spawn_blocking(move || {
                emit(&pool);
                pool.all_records()
            })
            .await
            {
                Ok(Ok(records)) => records,
                error => {
                    tracing::warn!(
                        ?error,
                        "cannot sample guest UDP/443 counters without slot inventory"
                    );
                    return;
                }
            };
            let active: Vec<RejectingLease> = records
                .into_iter()
                .filter(|r| matches!(r.state, SlotState::Leasing | SlotState::Leased))
                .map(|record| RejectingLease {
                    sandbox_id: record.info.sandbox_id,
                    slot: record.info.slot,
                    generation: record.info.generation,
                })
                .collect();
            // Forget leases that ended so a reused slot reports its new owner.
            reported_udp443_rejects.retain(|lease| active.contains(lease));
            let samples = stream::iter(active)
                .map(|lease| {
                    let sample = probe(
                        crate::net::NetId::from_slot(lease.slot).ns(),
                        shutdown_token.clone(),
                    );
                    async move { (lease, sample.await) }
                })
                .buffer_unordered(PROBE_CONCURRENCY);
            futures_util::pin_mut!(samples);
            let (mut observed, mut rejected, mut vlan_packets) = (0_u64, 0_u64, 0_u64);
            while let Some((lease, sample)) = samples.next().await {
                let Some(sample) = sample else { continue };
                if let Some(packets) =
                    first_udp443_reject(&mut reported_udp443_rejects, &lease, sample.udp443)
                {
                    tracing::warn!(
                        sandbox_id = lease.sandbox_id,
                        slot = lease.slot,
                        slot_generation = lease.generation,
                        namespace = sample.namespace,
                        packets,
                        "guest UDP/443 rejected; client must fall back to TCP"
                    );
                }
                if let Some(packets) = sample.vlan_packets {
                    let previous = previous_vlan_packets
                        .insert(sample.namespace.clone(), packets)
                        .unwrap_or_default();
                    if packets > previous {
                        tracing::warn!(
                            namespace = sample.namespace,
                            packets,
                            delta = packets - previous,
                            "guest VLAN frame dropped at TAP ingress"
                        );
                    }
                    vlan_packets = vlan_packets.saturating_add(packets);
                }
                match sample.udp443 {
                    Some((packets, GuestUdp443Action::Observe)) => observed += packets,
                    Some((packets, GuestUdp443Action::Reject)) => rejected += packets,
                    None => {}
                }
            }
            for (action, packets) in [
                (GuestUdp443Action::Observe, observed),
                (GuestUdp443Action::Reject, rejected),
            ] {
                pplx_space_metrics::node_manager::guest_udp443_packets(packets, action);
            }
            pplx_space_metrics::node_manager::guest_vlan_packets(vlan_packets);
        };
        // Dropping the pass stops admission and discards incomplete totals.
        // At most eight started probes retain the existing host-command timeout.
        tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => return,
            _ = pass => {},
        }
    }
}

/// Returns the reject count the first time a lease's UDP/443 reject counter is
/// nonzero, and `None` on every later sample for that lease.
fn first_udp443_reject(
    reported: &mut HashSet<RejectingLease>,
    lease: &RejectingLease,
    udp443: Option<(u64, GuestUdp443Action)>,
) -> Option<u64> {
    let (packets, GuestUdp443Action::Reject) = udp443? else {
        return None;
    };
    (packets > 0 && !lease.sandbox_id.is_empty() && reported.insert(lease.clone()))
        .then_some(packets)
}

async fn probe_firewall_counters(
    ns: String,
    shutdown: CancellationToken,
) -> Option<GuestFirewallCounters> {
    match tokio::task::spawn_blocking(move || {
        // A queued blocking job must not launch nft after shutdown.
        if shutdown.is_cancelled() {
            return None;
        }
        let udp443 = match crate::firewall::guest_udp443_packets(&ns) {
            Ok(sample) => sample,
            Err(error) => {
                tracing::debug!(namespace = ns, %error, "cannot sample guest UDP/443 counter");
                None
            }
        };
        let vlan_packets = match crate::firewall::guest_vlan_packets(&ns) {
            Ok(sample) => Some(sample),
            Err(error) => {
                tracing::warn!(namespace = ns, %error, "cannot sample guest VLAN counter");
                None
            }
        };
        Some(GuestFirewallCounters {
            namespace: ns,
            udp443,
            vlan_packets,
        })
    })
    .await
    {
        Ok(sample) => sample,
        Err(error) => {
            tracing::warn!(%error, "guest UDP/443 probe task failed");
            None
        }
    }
}

fn emit(pool: &NetworkPool) {
    let counts = match pool.counts() {
        Ok(counts) => counts,
        Err(error) => {
            tracing::warn!(%error, "cannot emit network slot counts without database inventory");
            return;
        }
    };
    let max_slots = pool.config().max_slots;
    node_network::slots_max(max_slots);
    node_network::slots_available(max_slots.saturating_sub(counts.leased));
    for (state, count) in [
        (NetworkSlotState::Ready, counts.ready),
        (NetworkSlotState::Leased, counts.leased),
        (NetworkSlotState::Dirty, counts.dirty),
        (NetworkSlotState::Resetting, counts.resetting),
        (NetworkSlotState::Creating, counts.creating),
        (NetworkSlotState::Quarantined, counts.quarantined),
        (NetworkSlotState::Degraded, counts.degraded),
    ] {
        node_network::slots_by_state(state, count);
    }
}

#[cfg(test)]
#[tokio::test]
async fn shutdown_stops_a_bounded_pass_of_wedged_probes() {
    use crate::slot_pool::{NetworkPoolConfig, NewLeaseMemoryReservation};
    use pplx_space_common_core::types::SandboxId;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let pool = NetworkPool::open_in_memory_for_test(NetworkPoolConfig::new(32));
    pool.set_all_slots_ready_for_test();
    for _ in 0..32 {
        let id = SandboxId::new().to_string();
        pool.claim_allocation(
            &id,
            None,
            Some(NewLeaseMemoryReservation {
                memory_reservation_mib: 8320,
            }),
        )
        .unwrap();
    }
    let shutdown = CancellationToken::new();
    let started = Arc::new(AtomicUsize::new(0));
    let full = Arc::new(tokio::sync::Notify::new());
    let (probe_started, probe_full) = (started.clone(), full.clone());
    let task = tokio::spawn(run_loop(pool, shutdown.clone(), move |_, _| {
        let (started, full) = (probe_started.clone(), probe_full.clone());
        async move {
            if started.fetch_add(1, Ordering::SeqCst) + 1 == PROBE_CONCURRENCY {
                full.notify_one();
            }
            std::future::pending().await
        }
    }));
    tokio::time::timeout(Duration::from_secs(1), full.notified())
        .await
        .unwrap();
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(started.load(Ordering::SeqCst), PROBE_CONCURRENCY);
}

#[cfg(test)]
#[test]
fn udp443_reject_is_reported_once_per_lease() {
    let lease = |sandbox_id: &str, generation| RejectingLease {
        sandbox_id: sandbox_id.to_owned(),
        slot: 7,
        generation,
    };
    let reject = |packets| Some((packets, GuestUdp443Action::Reject));
    let mut reported = HashSet::new();
    let first = lease("sbx-a", 1);

    assert_eq!(first_udp443_reject(&mut reported, &first, None), None);
    assert_eq!(first_udp443_reject(&mut reported, &first, reject(0)), None);
    assert_eq!(
        first_udp443_reject(&mut reported, &first, Some((5, GuestUdp443Action::Observe))),
        None
    );
    assert_eq!(
        first_udp443_reject(&mut reported, &first, reject(3)),
        Some(3)
    );
    assert_eq!(first_udp443_reject(&mut reported, &first, reject(9)), None);
    // A reused slot is a new lease and reports its own first reject.
    let reused = lease("sbx-b", 2);
    assert_eq!(
        first_udp443_reject(&mut reported, &reused, reject(1)),
        Some(1)
    );
    assert_eq!(
        first_udp443_reject(&mut reported, &lease("", 3), reject(1)),
        None
    );
}
