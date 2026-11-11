use super::*;

fn channels() -> WorkerChannels {
    WorkerChannels::new(Duration::from_secs(3), Duration::from_secs(10))
}

#[test]
fn an_entry_is_kept_until_it_is_idle_or_old() {
    let channels = channels();
    let start = Instant::now();
    let first = channels.entry("worker-1:9800", start).unwrap();
    assert!(first.ptr_eq(&channels.entry("worker-1:9800", start).unwrap()));
    // Use keeps an entry alive past the idle window, but not past its age.
    let mut now = start;
    while now < start + MAX_AGE - IDLE_TTL / 2 {
        now += IDLE_TTL / 2;
        assert!(first.ptr_eq(&channels.entry("worker-1:9800", now).unwrap()));
    }
    let renewed = channels.entry("worker-1:9800", start + MAX_AGE).unwrap();
    assert!(!first.ptr_eq(&renewed), "an old entry is replaced");

    let idle = channels.entry("worker-2:9800", start + MAX_AGE).unwrap();
    let after_idle = channels
        .entry("worker-2:9800", start + MAX_AGE + IDLE_TTL)
        .unwrap();
    assert!(!idle.ptr_eq(&after_idle), "an idle entry is replaced");
}

#[test]
fn a_new_entry_sweeps_retired_ones_and_the_map_is_bounded() {
    let channels = channels();
    let start = Instant::now();
    assert!(channels.entry("10.0.0.1:9800", start).is_some());
    assert!(channels.entry("10.0.0.2:9800", start + IDLE_TTL).is_some());
    assert_eq!(channels.len(), 1, "the idle address is dropped");

    // One entry is already kept.
    for index in 1..MAX_ENTRIES {
        assert!(
            channels
                .entry(
                    &format!("10.1.{}.{}:9800", index / 256, index % 256),
                    start + IDLE_TTL,
                )
                .is_some()
        );
    }
    assert!(
        channels.entry("10.2.0.1:9800", start + IDLE_TTL).is_none(),
        "a full map keeps no more workers"
    );
}

// Concurrent callers to an unreachable worker share one failed dial: they
// fail together after one dial's budget, not one budget after another, and
// the failed entry is gone so the next call dials afresh.
#[tokio::test]
async fn callers_share_a_failed_dial_and_the_next_call_dials_again() {
    if !crate::test_support::enter_sim_worker_process() {
        return;
    }
    let refused = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = refused.local_addr().unwrap().to_string();
    drop(refused);
    let mut channels = channels();
    channels.retry_policy = GrpcRetryPolicy {
        max_retries: 20,
        initial_delay: Duration::from_millis(50),
        max_delay: Duration::from_millis(100),
        max_elapsed: Duration::from_millis(300),
        jitter_factor: 0.0,
        ..pplx_space_common_grpc::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY
    };
    let started = std::time::Instant::now();
    let results = futures_util::future::join_all((0..8).map(|_| channels.client(&addr))).await;
    let elapsed = started.elapsed();
    assert!(results.iter().all(Result::is_err));
    assert!(
        elapsed >= Duration::from_millis(250),
        "the dial retried for its budget ({elapsed:?})"
    );
    assert!(
        elapsed < Duration::from_millis(1200),
        "eight callers took {elapsed:?}, as if each ran its own retries in turn"
    );
    assert_eq!(channels.len(), 0, "a failed dial is not kept");
}
