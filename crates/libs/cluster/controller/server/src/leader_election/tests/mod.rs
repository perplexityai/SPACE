//! Tests for the cluster-controller leader election.
//!
//! The `Lease` takeover decision (`takeable`) is unit-tested in
//! [`super::store`]. This module exercises the substrate-agnostic election loop
//! and renewal/resignation wiring against an in-memory [`FakeStore`], so it runs
//! everywhere with no apiserver.

use std::time::Duration;

use pplx_space_common_core::types::PositiveDuration;
use tokio_util::sync::CancellationToken;

mod fixtures;

use fixtures::{
    FakeStore, InjectedFailure, assert_budget_near, assert_still_standing_by, budgeted_config,
    expect_lease_unreachable, marker_path, test_config, wait_for_marker,
};

use super::{Campaign, LeaderElectionConfig};

/// A replica contends for the lease only while the kill-switch leaves election
/// on, and then when the process runs in a Kubernetes pod (where an apiserver
/// can host the `Lease` and a Deployment may run several replicas) or its
/// substrate drives the Kubernetes API (whose client reaches the `Lease` from
/// anywhere, beside any in-cluster replicas). So a Direct controller in a pod
/// contends like a Kubernetes one, a Kubernetes-substrate controller contends
/// wherever it runs, and only an off-cluster Direct controller (a systemd host)
/// runs the reconcilers without election.
#[test]
fn contention_follows_the_process_host_the_substrate_and_the_kill_switch() {
    use pplx_space_api::controllers::substrate::SubstrateCapabilities;

    use super::{Election, ElectionSkip, ProcessHost};

    const DIRECT: (&str, SubstrateCapabilities) = ("direct", SubstrateCapabilities::DIRECT);
    const KUBERNETES: (&str, SubstrateCapabilities) =
        ("kubernetes", SubstrateCapabilities::KUBERNETES);

    for (enabled, host, (substrate, capabilities), expected) in [
        (true, ProcessHost::Kubernetes, DIRECT, Election::Contend),
        (true, ProcessHost::Kubernetes, KUBERNETES, Election::Contend),
        (
            true,
            ProcessHost::OutsideKubernetes,
            DIRECT,
            Election::Skip(ElectionSkip::OutsideKubernetes),
        ),
        (
            true,
            ProcessHost::OutsideKubernetes,
            KUBERNETES,
            Election::Contend,
        ),
        (
            false,
            ProcessHost::Kubernetes,
            DIRECT,
            Election::Skip(ElectionSkip::Disabled),
        ),
        (
            false,
            ProcessHost::Kubernetes,
            KUBERNETES,
            Election::Skip(ElectionSkip::Disabled),
        ),
        (
            false,
            ProcessHost::OutsideKubernetes,
            DIRECT,
            Election::Skip(ElectionSkip::Disabled),
        ),
        (
            false,
            ProcessHost::OutsideKubernetes,
            KUBERNETES,
            Election::Skip(ElectionSkip::Disabled),
        ),
    ] {
        let config = LeaderElectionConfig {
            enabled,
            ..test_config("replica-a")
        };
        assert_eq!(
            config.election(host, capabilities),
            expected,
            "election enabled: {enabled}, host: {host:?}, {substrate} substrate"
        );
    }
}

/// The process runs inside Kubernetes when both service variables the kubelet
/// injects into every pod are set, which is the rule the kube client applies
/// before it builds an in-cluster config. Either one alone is not a pod.
#[test]
fn process_host_is_kubernetes_only_with_both_service_variables() {
    use super::ProcessHost;

    for (service_host, service_port, expected) in [
        (None, None, ProcessHost::OutsideKubernetes),
        (Some("10.96.0.1"), None, ProcessHost::OutsideKubernetes),
        (None, Some("443"), ProcessHost::OutsideKubernetes),
        (Some("10.96.0.1"), Some("443"), ProcessHost::Kubernetes),
    ] {
        let mut env = pplx_space_test_env::TestEnv::default();
        if let Some(value) = service_host {
            env.set("KUBERNETES_SERVICE_HOST", value);
        }
        if let Some(value) = service_port {
            env.set("KUBERNETES_SERVICE_PORT", value);
        }
        assert_eq!(
            ProcessHost::from_environment(&env),
            expected,
            "KUBERNETES_SERVICE_HOST: {service_host:?}, KUBERNETES_SERVICE_PORT: {service_port:?}"
        );
    }
}

/// A free lease is won immediately; the leadership token stays live while held,
/// and `resign` releases the lease.
#[tokio::test]
async fn campaign_elects_and_resign_releases() {
    let store = std::sync::Arc::new(FakeStore::default());
    let shutdown = CancellationToken::new();

    let Campaign::Elected(leadership) =
        super::campaign(store.clone(), test_config("me"), shutdown.clone()).await
    else {
        panic!("must be elected when the lease is free");
    };
    assert_eq!(
        store.holder().as_deref(),
        Some("me"),
        "we should hold the lease"
    );
    assert!(
        !leadership.token().is_cancelled(),
        "leadership token stays live while we hold the lease"
    );

    leadership.resign().await;
    assert!(
        store.holder().is_none(),
        "resign must release the lease for a standby"
    );
}

/// Dropping the guard without resigning or abandoning must stop the renewal
/// task. Otherwise the lease stays held by a process running no reconcilers:
/// every standby gets a valid denial, reports Ready and resets its liveness
/// budget, so there is no reconciler anywhere and nothing observes it — every
/// call succeeds. `main` is correct today, but `result?` sits one line below the
/// `relinquish` call, so the guard fails closed rather than relying on that.
#[tokio::test]
async fn dropping_leadership_stops_renewing() {
    let store = std::sync::Arc::new(FakeStore::default());
    let Campaign::Elected(leadership) =
        super::campaign(store.clone(), test_config("me"), CancellationToken::new()).await
    else {
        panic!("a free lease must be won");
    };
    let token = leadership.token();
    assert!(!token.is_cancelled());

    // Let it renew at least once so "stopped" is distinguishable from "never
    // started".
    tokio::time::sleep(Duration::from_millis(80)).await;
    let before_drop = store.calls();
    assert!(
        before_drop > 1,
        "the renewal task must have run before we test that dropping stops it"
    );

    drop(leadership);
    assert!(
        token.is_cancelled(),
        "dropping the guard must cancel the renewal token"
    );
    // And the renewals must actually stop. Asserting only on the token would pass
    // over a `Drop` that cancelled something nothing was listening to.
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(
        store.calls(),
        before_drop,
        "renewals must stop when the guard is dropped, or the lease is renewed \
         forever by a process running no reconcilers"
    );
}

/// A clean drain releases the lease; a timed-out or panicked one must not.
/// Releasing after an unclean drain is the dangerous direction: it makes the
/// lease immediately acquirable while a wedged tick of ours may still be
/// detached and running, so a standby starts its reconciler set alongside it.
/// The two paths differ only in that one line of `main`, which has no tests, so
/// the choice is pinned here.
#[tokio::test]
async fn relinquish_releases_only_after_a_clean_drain() {
    let store = std::sync::Arc::new(FakeStore::default());
    let Campaign::Elected(leadership) =
        super::campaign(store.clone(), test_config("me"), CancellationToken::new()).await
    else {
        panic!("a free lease must be won");
    };
    leadership.relinquish(true).await;
    assert!(
        store.holder().is_none(),
        "a clean drain must release the lease so a standby takes over at once"
    );

    let Campaign::Elected(leadership) =
        super::campaign(store.clone(), test_config("me"), CancellationToken::new()).await
    else {
        panic!("the released lease must be winnable again");
    };
    leadership.relinquish(false).await;
    assert_eq!(
        store.holder().as_deref(),
        Some("me"),
        "an unclean drain must leave the lease held, to expire at its TTL after this process \
         exits — releasing it would let a standby overlap a tick that may still be running"
    );
}

/// If a peer takes the lease out from under the holder, the renewal loop
/// observes the loss and cancels the leadership token so the reconcilers stop.
#[tokio::test]
async fn leadership_token_cancels_when_lease_stolen() {
    let store = std::sync::Arc::new(FakeStore::default());
    let shutdown = CancellationToken::new();

    let Campaign::Elected(leadership) =
        super::campaign(store.clone(), test_config("me"), shutdown.clone()).await
    else {
        panic!("must be elected when the lease is free");
    };
    let token = leadership.token();

    // A peer takes the lease; the next renewal tick sees a foreign holder.
    store.set_holder(Some("thief"));

    tokio::time::timeout(Duration::from_secs(5), token.cancelled())
        .await
        .expect("leadership token must cancel after the lease is stolen");
    assert!(token.is_cancelled());

    leadership.resign().await;
}

/// Shutdown requested before winning returns cleanly without ever acquiring.
#[tokio::test]
async fn campaign_shutdown_before_elected() {
    let store = std::sync::Arc::new(FakeStore::default());
    // A peer already holds the lease, so this replica can only stand by.
    store.set_holder(Some("other"));
    let shutdown = CancellationToken::new();
    shutdown.cancel();

    match super::campaign(store.clone(), test_config("me"), shutdown).await {
        Campaign::ShutdownBeforeElected => {}
        Campaign::Elected(_) => panic!("must not acquire a lease held by a peer"),
        Campaign::LeaseUnreachable => panic!("the lease was reachable and the peer answered"),
    }
    assert_eq!(
        store.holder().as_deref(),
        Some("other"),
        "standby must not steal the peer's lease"
    );
    // Shutdown was already requested, so the loop must exit without touching the
    // lease at all. Without this, the assertion above passes for the wrong
    // reason — "didn't steal" is indistinguishable from "never ran".
    assert_eq!(
        store.calls(),
        0,
        "a pre-cancelled campaign must make no lease calls"
    );
}

/// If the holder cannot confirm a renewal within the TTL because every renewal
/// call errors, the deadline safety net resigns it — the leadership token
/// cancels even though no peer explicitly took the lease.
#[tokio::test]
async fn leadership_resigns_when_renewal_errors_past_ttl() {
    let store = std::sync::Arc::new(FakeStore::default());
    let shutdown = CancellationToken::new();
    let mut config = test_config("me");
    config.lease_ttl = Duration::from_millis(300);
    config.renew_deadline = Duration::from_millis(150);
    config.renew_interval = Duration::from_millis(50);

    let Campaign::Elected(leadership) = super::campaign(store.clone(), config, shutdown).await
    else {
        panic!("must be elected when the lease is free");
    };
    let token = leadership.token();

    // Every renewal call now errors; the deadline (150ms) must resign us.
    store.set_fail_every(1);
    tokio::time::timeout(Duration::from_secs(5), token.cancelled())
        .await
        .expect("must resign after the renew deadline of failed renewals");
    assert!(token.is_cancelled());

    leadership.resign().await;
}

/// A resigning holder must stop reporting Ready.
///
/// The path this pins is the one no other test reaches: losing the lease to a
/// peer is observed on a *successful* renewal, so the last thing the marker was
/// told is `true`. If the resignation doesn't clear it, nothing else will — the
/// renewal loop returns immediately and drops its handle — and the pod advertises
/// Ready for the whole drain while holding no lease and running no reconcilers.
/// Under `maxUnavailable: 0` that is a replica a concurrent rollout counts as
/// available, which is exactly what the readiness gate exists to prevent.
///
/// Deliberately asserted on the marker file rather than on the resignation log:
/// the log line existed before this fix and would keep passing.
#[tokio::test]
async fn a_resigning_holder_stops_reporting_ready() {
    let path = marker_path("resigning-holder-clears-readiness");
    let _ = std::fs::remove_file(&path);
    let store = std::sync::Arc::new(FakeStore::default());
    let shutdown = CancellationToken::new();
    let mut config = test_config("me");
    config.readiness_file = Some(path.clone());
    config.lease_ttl = Duration::from_millis(300);
    config.renew_deadline = Duration::from_millis(150);
    config.renew_interval = Duration::from_millis(20);

    let Campaign::Elected(leadership) = super::campaign(store.clone(), config, shutdown).await
    else {
        panic!("must be elected when the lease is free");
    };
    // Ready first, so the clear below has a presence to remove — otherwise this
    // would pass against a build that never writes the marker at all.
    wait_for_marker(&path, true).await;

    // A peer takes the lease. The next renewal is *answered*, with `Ok(false)`,
    // which is the arm that leaves the marker saying `true`.
    store.set_holder(Some("other"));
    let token = leadership.token();
    tokio::time::timeout(Duration::from_secs(5), token.cancelled())
        .await
        .expect("must resign once a peer holds the lease");

    wait_for_marker(&path, false).await;
    leadership.abandon().await;
    let _ = std::fs::remove_file(&path);
}

/// Regression for the split-brain window: even if a renewal call hangs forever
/// (half-open connection), the independent deadline must still resign the holder
/// rather than leave it acting while the lease expires.
#[tokio::test]
async fn leadership_resigns_when_renewal_hangs_past_ttl() {
    let store = std::sync::Arc::new(FakeStore::default());
    let shutdown = CancellationToken::new();
    let mut config = test_config("me");
    config.lease_ttl = Duration::from_millis(300);
    config.renew_deadline = Duration::from_millis(150);
    config.renew_interval = Duration::from_millis(50);

    let Campaign::Elected(leadership) = super::campaign(store.clone(), config, shutdown).await
    else {
        panic!("must be elected when the lease is free");
    };
    let token = leadership.token();

    // Renewal calls now block forever; the deadline resigns us regardless.
    store.set_hang(true);
    tokio::time::timeout(Duration::from_secs(5), token.cancelled())
        .await
        .expect("must resign despite a hung renewal call");
    assert!(token.is_cancelled());

    leadership.resign().await;
}

/// Derived timings preserve `renew_interval < renew_deadline < lease_ttl` by
/// construction, keep the drain margin between the renew deadline and the TTL
/// (the property that prevents concurrent runners), and raise a too-small
/// requested TTL to the floor.
#[test]
fn derive_timings_preserves_ordering_and_margin() {
    use super::{MIN_LEASE_TTL, RENEW_DEADLINE_MARGIN, Timings};

    let t = Timings::derive(Duration::from_secs(30), Duration::from_secs(2), None, "me");
    assert_eq!(t.lease_ttl, Duration::from_secs(30));
    assert_eq!(
        t.renew_deadline,
        Duration::from_secs(30) - RENEW_DEADLINE_MARGIN
    );
    assert!(t.renew_interval < t.renew_deadline, "{t:?}");
    assert!(t.renew_deadline < t.lease_ttl, "{t:?}");
    // The safety invariant: the margin between "stop acting" and "lease
    // expires" must cover the reconciler drain window.
    // Absolute, not `lease_ttl - renew_deadline > SHUTDOWN_DRAIN_TIMEOUT`: that
    // form reduces to `RENEW_DEADLINE_MARGIN - DRAIN > 0`, i.e. `2 > 0`, and so
    // passes for any margin of the form `DRAIN + k`. Verified — shrinking the
    // buffer to 1s kept the whole suite green.
    assert_eq!(
        t.lease_ttl - t.renew_deadline,
        Duration::from_secs(12),
        "the holder must stop acting a full 12s before the lease can expire: {t:?}"
    );
    // The renewal cadence has to leave at least two attempts *strictly* inside
    // the deadline. With `deadline / 3` the third attempt ties with the deadline
    // and `select!` picks randomly, so two must land before it; with `/2` only
    // one does, and a single dropped renewal becomes a coin-flip resignation.
    assert!(
        t.renew_interval * 2 < t.renew_deadline,
        "at least two renewal attempts must land strictly inside the deadline: {t:?}"
    );

    // A too-small requested TTL is raised to the floor, still consistent.
    let floored = Timings::derive(Duration::from_secs(1), Duration::from_secs(2), None, "me");
    assert_eq!(floored.lease_ttl, MIN_LEASE_TTL);
    assert!(floored.renew_interval >= Duration::from_secs(1));
    assert!(
        floored.renew_interval < floored.renew_deadline,
        "{floored:?}"
    );
    assert!(floored.renew_deadline < floored.lease_ttl, "{floored:?}");
}

/// A readiness write that is still in flight must never land *after* a newer
/// NotReady, whatever it costs the filesystem.
///
/// This is the ordering a timeout-on-the-caller's-stack design permits and this
/// one does not: `tokio::fs` runs on the blocking pool, so an abandoned write can
/// complete later. Sequence exercised here — Ready is requested, its apply is
/// still running, NotReady is requested, and the end state must be NotReady. If
/// the stale Ready could land last, a replica unable to reach the lease would
/// advertise itself Ready, which is what `maxUnavailable: 0` relies on not
/// happening.
///
/// Driven through `marker_loop`'s injectable reconciler because a real
/// filesystem cannot be made slow in a test without hanging the runtime at drop.
#[tokio::test(start_paused = true)]
async fn a_slow_readiness_write_cannot_land_after_a_newer_clear() {
    let applied = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let (tx, rx) = tokio::sync::watch::channel(None);

    let recorder = applied.clone();
    let task = tokio::spawn(crate::leader_election::marker_loop(rx, move |healthy| {
        let recorder = recorder.clone();
        async move {
            // Long enough that the next desired state arrives mid-flight.
            tokio::time::sleep(Duration::from_secs(5)).await;
            recorder.lock().expect("lock").push(healthy);
        }
    }));

    tx.send_replace(Some(true));
    // Let the task pick Ready up and begin applying it.
    tokio::time::sleep(Duration::from_secs(1)).await;
    // NotReady arrives while the Ready apply is still running.
    tx.send_replace(Some(false));
    tokio::time::sleep(Duration::from_secs(30)).await;

    let applied = applied.lock().expect("lock").clone();
    assert_eq!(
        applied,
        vec![true, false],
        "states must be applied in request order and settle on the newest"
    );
    drop(tx);
    let _ = task.await;
}

/// The same ordering, end to end against a real file: the marker must be absent
/// after Ready is superseded by NotReady.
#[tokio::test(start_paused = true)]
async fn the_marker_settles_on_the_newest_requested_state() {
    let path = marker_path("ordering");
    let _ = std::fs::remove_file(&path);
    let marker = crate::leader_election::ReadinessMarker::spawn(Some(path.clone()));

    marker.set(true);
    wait_for_marker(&path, true).await;
    marker.set(false);
    wait_for_marker(&path, false).await;

    // And it stays absent — nothing arrives late to undo it.
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert!(
        !path.exists(),
        "the marker must stay absent once NotReady is the newest state"
    );
    let _ = std::fs::remove_file(&path);
}

/// The readiness marker follows recent lease contact: written after a successful
/// call, cleared after a failed one. That is what makes the chart's
/// `maxUnavailable: 0` sound — a replacement that can't reach the lease never
/// goes Ready, so a rollout won't tear down a working pod for one that can't take
/// over.
///
/// These tests drive the store from the test body and wait for the *effect*,
/// because the marker is written by a background task: sampling at a fixed
/// instant would race that task rather than assert on it.
#[tokio::test(start_paused = true)]
async fn readiness_marker_tracks_lease_contact() {
    let path = marker_path("tracks");
    let _ = std::fs::remove_file(&path);
    let store = std::sync::Arc::new(FakeStore::default());
    // A peer holds the lease, so this replica is a validly-denied standby — the
    // path that must still report Ready.
    store.set_holder(Some("other"));
    let mut config = budgeted_config("me", Duration::from_secs(600));
    config.readiness_file = Some(path.clone());

    let shutdown = CancellationToken::new();
    let campaign = tokio::spawn(crate::leader_election::campaign(
        store.clone(),
        config,
        shutdown.clone(),
    ));

    wait_for_marker(&path, true).await;
    store.set_fail_every(1);
    wait_for_marker(&path, false).await;

    shutdown.cancel();
    let _ = campaign.await;
    let _ = std::fs::remove_file(&path);
}

/// A failed lease call must clear a marker this process never wrote.
///
/// Why that has to hold rather than being merely tidy: `tokio::fs` runs on the
/// blocking pool, so nothing in memory can be treated as authoritative about the
/// file. Anything that could would have nothing to reconcile on a later failed
/// call, and the pod would advertise Ready while unable to reach the lease.
#[tokio::test(start_paused = true)]
async fn a_failed_call_clears_a_marker_this_process_never_wrote() {
    let path = marker_path("foreign");
    std::fs::write(&path, b"ready\n").expect("seed the marker");
    let store = std::sync::Arc::new(FakeStore::default());
    store.set_holder(Some("other"));
    store.set_fail_every(1);
    let mut config = budgeted_config("me", Duration::from_secs(600));
    config.readiness_file = Some(path.clone());

    let shutdown = CancellationToken::new();
    let campaign = tokio::spawn(crate::leader_election::campaign(
        store,
        config,
        shutdown.clone(),
    ));

    wait_for_marker(&path, false).await;

    shutdown.cancel();
    let _ = campaign.await;
    let _ = std::fs::remove_file(&path);
}

/// Readiness must *recover*, not just drop. A pod that goes NotReady during a
/// brownout and stays there after the apiserver returns stalls a rollout
/// indefinitely under `maxUnavailable: 0`, with no `livenessProbe` to break it.
#[tokio::test(start_paused = true)]
async fn readiness_marker_recovers_after_the_lease_comes_back() {
    let path = marker_path("recovers");
    let _ = std::fs::remove_file(&path);
    let store = std::sync::Arc::new(FakeStore::default());
    store.set_holder(Some("other"));
    let mut config = budgeted_config("me", Duration::from_secs(600));
    config.readiness_file = Some(path.clone());

    let shutdown = CancellationToken::new();
    let campaign = tokio::spawn(crate::leader_election::campaign(
        store.clone(),
        config,
        shutdown.clone(),
    ));

    // present -> absent -> present, each step waited for, so the recovery is a
    // real second transition rather than the first write.
    wait_for_marker(&path, true).await;
    store.set_fail_every(1);
    wait_for_marker(&path, false).await;
    store.set_fail_every(0);
    wait_for_marker(&path, true).await;

    shutdown.cancel();
    let _ = campaign.await;
    let _ = std::fs::remove_file(&path);
}

/// One isolated failed call clears the marker and the next success puts it back.
#[tokio::test(start_paused = true)]
async fn readiness_marker_recovers_from_a_single_failed_call() {
    let path = marker_path("single");
    let _ = std::fs::remove_file(&path);
    let store = std::sync::Arc::new(FakeStore::default());
    store.set_holder(Some("other"));
    let mut config = budgeted_config("me", Duration::from_secs(600));
    config.readiness_file = Some(path.clone());

    let shutdown = CancellationToken::new();
    let campaign = tokio::spawn(crate::leader_election::campaign(
        store.clone(),
        config,
        shutdown.clone(),
    ));

    wait_for_marker(&path, true).await;
    store.set_fail_once();
    wait_for_marker(&path, false).await;
    wait_for_marker(&path, true).await;

    shutdown.cancel();
    let _ = campaign.await;
    let _ = std::fs::remove_file(&path);
}

/// A marker removed out-of-band must come back while lease calls keep
/// *succeeding*. Nothing fails here, so a design that only acted on a change of
/// its own opinion would never look at the file again and the pod would stay
/// NotReady for the container's life — and with
/// `unhealthyPodEvictionPolicy: AlwaysAllow` on the PDB it becomes the *preferred*
/// eviction target while actually running every reconciler.
#[tokio::test(start_paused = true)]
async fn readiness_marker_is_restored_while_the_lease_stays_reachable() {
    let path = marker_path("selfheal");
    let _ = std::fs::remove_file(&path);
    let store = std::sync::Arc::new(FakeStore::default());
    store.set_holder(Some("other"));
    let mut config = budgeted_config("me", Duration::from_secs(600));
    config.readiness_file = Some(path.clone());

    let shutdown = CancellationToken::new();
    let campaign = tokio::spawn(crate::leader_election::campaign(
        store,
        config,
        shutdown.clone(),
    ));

    wait_for_marker(&path, true).await;
    std::fs::remove_file(&path).expect("marker was written");
    // No injected failure: every lease call from here keeps succeeding.
    wait_for_marker(&path, true).await;

    shutdown.cancel();
    let _ = campaign.await;
    let _ = std::fs::remove_file(&path);
}

/// The renewal loop keeps the leader's readiness in step: a confirmed renewal
/// reports Ready and a failed one clears it, so a holder that has lost apiserver
/// contact stops advertising itself before it resigns.
#[tokio::test(start_paused = true)]
async fn readiness_marker_follows_the_leaders_renewals() {
    let path = marker_path("renewals");
    let _ = std::fs::remove_file(&path);
    let store = std::sync::Arc::new(FakeStore::default());
    let mut config = budgeted_config("me", Duration::from_secs(600));
    config.readiness_file = Some(path.clone());
    config.lease_ttl = Duration::from_secs(60);
    config.renew_deadline = Duration::from_secs(48);
    config.renew_interval = Duration::from_secs(16);

    let Campaign::Elected(leadership) =
        crate::leader_election::campaign(store.clone(), config, CancellationToken::new()).await
    else {
        panic!("a free lease must be won");
    };
    let token = leadership.token();
    wait_for_marker(&path, true).await;

    // Ride a few confirmed renewals; still Ready.
    tokio::time::sleep(Duration::from_secs(40)).await;
    assert!(
        path.exists(),
        "a leader confirming renewals must keep reporting Ready"
    );

    // Now renewals fail: the marker must clear well before the holder resigns.
    store.set_fail_every(1);
    wait_for_marker(&path, false).await;
    tokio::time::timeout(Duration::from_secs(300), token.cancelled())
        .await
        .expect("the deadline must resign the holder");

    leadership.abandon().await;
    let _ = std::fs::remove_file(&path);
}

/// Winning the lease reports Ready, and `signal_ready` does so for the
/// non-election paths (kill-switch, or a Direct process outside a pod) where
/// the process runs the reconcilers unconditionally.
#[tokio::test(start_paused = true)]
async fn readiness_marker_written_for_leader_and_for_the_non_election_path() {
    let path = marker_path("leader");
    let _ = std::fs::remove_file(&path);
    let store = std::sync::Arc::new(FakeStore::default());
    let mut config = budgeted_config("me", Duration::from_secs(600));
    config.readiness_file = Some(path.clone());

    let Campaign::Elected(leadership) =
        crate::leader_election::campaign(store, config, CancellationToken::new()).await
    else {
        panic!("a free lease must be won");
    };
    wait_for_marker(&path, true).await;
    leadership.resign().await;
    let _ = std::fs::remove_file(&path);

    let path = marker_path("signal");
    let _ = std::fs::remove_file(&path);
    let mut config = budgeted_config("me", Duration::from_secs(600));
    config.readiness_file = Some(path.clone());
    // Held so the writer task outlives the assertion.
    let _marker = crate::leader_election::signal_ready(&config);
    wait_for_marker(&path, true).await;
    let _ = std::fs::remove_file(&path);
}

/// The renew deadline must be anchored on the instant *before* each renewal, not
/// on its response. This is the module's central safety property: the apiserver
/// stamps `renewTime` during the call, so a deadline measured from the response
/// is later than the lease's real expiry by the call's latency, and the holder
/// keeps acting past the point a standby may acquire — two concurrent reconciler
/// runners.
///
/// Nothing observed the deadline's absolute value before this: the existing
/// resign-on-failure tests assert only *eventual* cancellation, so anchoring on
/// the response passed them. Measured on the paused clock, with 8s call latency
/// and the fixture's timings (ttl 60 / deadline 48 / renew 16):
///
/// * acquire is issued at 0s and answers at 8s, so the renewal loop starts at 8s
///   and its first tick lands at 24s;
/// * that renewal is issued at 24s and answers at 32s, so the deadline is 72s
///   when anchored pre-call and 80s when anchored on the response;
/// * renewals then start failing, so the deadline is what resigns the holder.
///
/// The lease stamped during the 24s renewal can expire as early as 24+60 = 84s,
/// and the reconcilers get up to `SHUTDOWN_DRAIN_TIMEOUT` after the token is
/// cancelled — so resigning any later than 74s eats into the drain window that
/// keeps the old holder's last tick from overlapping the new one's first.
#[tokio::test(start_paused = true)]
async fn renew_deadline_is_anchored_before_the_call_not_after_it() {
    let store = std::sync::Arc::new(FakeStore::default());
    store.set_latency(Duration::from_secs(8));
    let mut config = test_config("me");
    config.lease_ttl = Duration::from_secs(60);
    config.renew_deadline = Duration::from_secs(48);
    config.renew_interval = Duration::from_secs(16);
    config.retry_interval = Duration::from_secs(8);

    let start = tokio::time::Instant::now();
    let Campaign::Elected(leadership) =
        super::campaign(store.clone(), config, CancellationToken::new()).await
    else {
        panic!("a free lease must be won");
    };
    let token = leadership.token();

    // Renewals stop confirming right after the first one, so the deadline — not
    // a peer, and not a lost-lease observation — is what ends the leadership.
    // 35s, not 40s: at 40s the fail-setter's timer expires in the same instant
    // as the t=40 renewal's latency sleep, and which continuation is polled
    // first decides whether that renewal confirms — a tie on tokio's timer
    // wheel, not a guarantee.
    let failing = store.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(35)).await;
        failing.set_fail_every(1);
    });

    tokio::time::timeout(Duration::from_secs(600), token.cancelled())
        .await
        .expect("the deadline must resign the holder");
    let resigned_at = start.elapsed();

    let last_confirmed_renewal = Duration::from_secs(24);
    let must_resign_by = last_confirmed_renewal + Duration::from_secs(60)
        - crate::reconcilers::SHUTDOWN_DRAIN_TIMEOUT;
    assert!(
        resigned_at <= must_resign_by,
        "the holder resigned at {resigned_at:?}, past the {must_resign_by:?} that leaves a full \
         drain window inside the lease it last renewed — the deadline is anchored on the \
         response instead of the instant before the call"
    );
    // Bounded below as well. An upper bound alone accepts *any* early
    // resignation, so it passes both when a confirmed renewal fails to extend
    // the deadline at all — leadership would then end a fixed interval after
    // every acquisition, forever, whatever the apiserver does — and when the
    // acquire's own deadline is anchored on its response. Verified: both
    // mutations survived the upper bound.
    let earliest_valid = last_confirmed_renewal + Duration::from_secs(48);
    assert!(
        resigned_at >= earliest_valid,
        "the holder resigned at {resigned_at:?}, before the {earliest_valid:?} its last \
         confirmed renewal bought it — a confirmed renewal must extend the deadline"
    );
    leadership.abandon().await;
}

/// The outcome-to-action mapping, which `main` only carries out. The one that
/// matters is `LeaseUnreachable -> ExitNonZero`: routing it to a clean exit, or
/// onto the run-unconditionally path, type-checks and reads plausibly, and the
/// second would run the full singleton reconciler set concurrently with the real
/// leader. `main` itself has no tests, so this is where that decision is pinned.
#[tokio::test(start_paused = true)]
async fn campaign_outcomes_resolve_to_the_right_process_action() {
    use super::CampaignAction;

    assert!(
        matches!(
            Campaign::<FakeStore>::LeaseUnreachable.resolve(),
            CampaignAction::ExitNonZero
        ),
        "an unreachable lease must exit non-zero so kubelet restarts the replica"
    );
    assert!(
        matches!(
            Campaign::<FakeStore>::ShutdownBeforeElected.resolve(),
            CampaignAction::ExitZero
        ),
        "a standby that never got its turn shut down cleanly"
    );

    let store = std::sync::Arc::new(FakeStore::default());
    let elected = super::campaign(
        store,
        budgeted_config("me", Duration::from_secs(600)),
        CancellationToken::new(),
    )
    .await;
    match elected.resolve() {
        CampaignAction::Lead(leadership) => leadership.resign().await,
        _ => panic!("winning the lease must authorise leading, not an exit"),
    }
}

/// `ReconcilerAuthority` is what keeps a lease-less replica from running the
/// reconcilers. The disabled case is named rather than spelled `None`, so a
/// replica that failed to get the lease cannot be routed onto the unconditional
/// path by a plausible-looking `Option` mapping — the mistake that would run the
/// full singleton set concurrently with the real leader.
#[tokio::test(start_paused = true)]
async fn reconciler_authority_routes_the_token_by_who_holds_the_lease() {
    let shutdown = CancellationToken::new();
    let store = std::sync::Arc::new(FakeStore::default());
    let Campaign::Elected(leadership) = super::campaign(
        store,
        budgeted_config("me", Duration::from_secs(600)),
        shutdown.clone(),
    )
    .await
    else {
        panic!("a free lease must be won");
    };
    let leader_token = leadership.token();
    let authority = super::ReconcilerAuthority::Leader(leadership);

    // A leader runs under the leadership token, which stops on lease loss as
    // well as shutdown — not under the raw shutdown token.
    let routed = authority.reconciler_token(&shutdown);
    assert!(
        !routed.is_cancelled(),
        "the leader's reconcilers run while the lease is held"
    );
    leader_token.cancel();
    assert!(
        routed.is_cancelled(),
        "losing the lease must stop the leader's reconcilers"
    );
    authority
        .into_leadership()
        .expect("a leader carries a guard to resign")
        .abandon()
        .await;

    // The disabled case runs off the process shutdown token and carries no
    // guard, since there is no lease to release.
    let disabled = super::ReconcilerAuthority::<FakeStore>::ElectionDisabled;
    let routed = disabled.reconciler_token(&shutdown);
    assert!(!routed.is_cancelled());
    shutdown.cancel();
    assert!(
        routed.is_cancelled(),
        "without election the reconcilers stop on process shutdown"
    );
    assert!(
        super::ReconcilerAuthority::<FakeStore>::ElectionDisabled
            .into_leadership()
            .is_none()
    );
}

/// The leadership token must be a *child* of the process shutdown token, so a
/// SIGTERM stops the reconcilers. They are driven off `leadership.token()`, not
/// off the shutdown token directly, so if the two are unrelated the reconcilers
/// never stop, `run_reconcilers` never returns, `resign` is never reached, and
/// the pod is SIGKILLed at the grace period with the lease left to expire at its
/// full TTL instead of being released for the standby.
#[tokio::test(start_paused = true)]
async fn leadership_token_is_cancelled_by_process_shutdown() {
    let store = std::sync::Arc::new(FakeStore::default());
    let shutdown = CancellationToken::new();
    let Campaign::Elected(leadership) = super::campaign(
        store,
        budgeted_config("me", Duration::from_secs(600)),
        shutdown.clone(),
    )
    .await
    else {
        panic!("a free lease must be won");
    };
    let token = leadership.token();
    assert!(
        !token.is_cancelled(),
        "leadership is live while the lease is held"
    );

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(60), token.cancelled())
        .await
        .expect("process shutdown must cancel the leadership token");
    leadership.resign().await;
}

/// The renewal loop's *outer* `sleep_until(deadline)` arm is what bounds
/// overshoot when the deadline does not land on a tick. The inner race — the one
/// wrapped around the renewal call — only fires once a tick has already arrived,
/// so with the outer arm gone the holder keeps acting until the next tick and
/// then resigns late.
///
/// The other resign tests can't see this: their deadline coincides with a tick,
/// so both arms answer at the same instant. Here the cadence (20s) deliberately
/// straddles the deadline (48s) — ticks at 20, 40, 60 — so the outer arm resigns
/// at 48s and its absence would resign at 60s. At 60s the reconcilers then drain
/// for up to `SHUTDOWN_DRAIN_TIMEOUT`, finishing at 70s, past the 60s at which
/// the lease can expire and a standby may start its own ticks.
#[tokio::test(start_paused = true)]
async fn the_outer_deadline_arm_bounds_overshoot_between_ticks() {
    let store = std::sync::Arc::new(FakeStore::default());
    let mut config = test_config("me");
    config.lease_ttl = Duration::from_secs(60);
    config.renew_deadline = Duration::from_secs(48);
    config.renew_interval = Duration::from_secs(20);
    config.retry_interval = Duration::from_secs(2);

    let start = tokio::time::Instant::now();
    let Campaign::Elected(leadership) =
        crate::leader_election::campaign(store.clone(), config, CancellationToken::new()).await
    else {
        panic!("a free lease must be won");
    };
    let token = leadership.token();

    // Every renewal fails from here, and fails fast, so no call is ever in
    // flight when the deadline arrives — only the outer arm can catch it.
    store.set_fail_every(1);

    tokio::time::timeout(Duration::from_secs(600), token.cancelled())
        .await
        .expect("the deadline must resign the holder");
    let resigned_at = start.elapsed();

    assert!(
        resigned_at + crate::reconcilers::SHUTDOWN_DRAIN_TIMEOUT <= Duration::from_secs(60),
        "the holder resigned at {resigned_at:?}, leaving less than the drain window inside the \
         lease — its reconcilers would still be finishing when a standby may start its own"
    );
    leadership.abandon().await;
}

/// The *acquire's* deadline is anchored pre-call too. The test above only pins
/// the renewal anchor, because a confirmed renewal overwrites the deadline the
/// acquire set — so anchoring the acquire on its response survives it. Here no
/// renewal ever confirms, which leaves the acquire's own anchor as the thing that
/// resigns the holder.
///
/// With 8s acquire latency the lease's `renewTime` can be stamped as early as
/// t=0, so a pre-call anchor resigns at 48s and a response anchor at 56s. The
/// lease can expire at 60s and the reconcilers need 10s of drain, so 56s eats
/// into the window that keeps the old holder's last tick clear of the new one's
/// first.
#[tokio::test(start_paused = true)]
async fn the_acquire_deadline_is_anchored_before_the_call_too() {
    let store = std::sync::Arc::new(FakeStore::default());
    store.set_latency(Duration::from_secs(8));
    let mut config = test_config("me");
    config.lease_ttl = Duration::from_secs(60);
    config.renew_deadline = Duration::from_secs(48);
    config.renew_interval = Duration::from_secs(16);
    config.retry_interval = Duration::from_secs(8);

    let start = tokio::time::Instant::now();
    let Campaign::Elected(leadership) =
        super::campaign(store.clone(), config, CancellationToken::new()).await
    else {
        panic!("a free lease must be won");
    };
    let token = leadership.token();

    // Every renewal fails from here, so the deadline the acquire set is never
    // replaced by a confirmed one.
    let failing = store.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        failing.set_fail_every(1);
    });

    tokio::time::timeout(Duration::from_secs(600), token.cancelled())
        .await
        .expect("the acquire's deadline must resign the holder");
    let resigned_at = start.elapsed();

    let must_resign_by = Duration::from_secs(60) - crate::reconcilers::SHUTDOWN_DRAIN_TIMEOUT;
    assert!(
        resigned_at <= must_resign_by,
        "the holder resigned at {resigned_at:?}, past the {must_resign_by:?} that leaves a full \
         drain window inside the lease its acquire stamped — the acquire's deadline is anchored \
         on the response instead of the instant before the call"
    );
    leadership.abandon().await;
}

/// The per-attempt acquisition timeout is `retry_interval`, which is now a
/// documented contract (`values.yaml` tells operators it doubles as the timeout,
/// and the liveness budget's cadence floor is derived from it). Timing the call
/// out against anything else — the lease TTL, say — silently changes how often a
/// wedged standby samples the lease, defeating the `MIN_GRACE_RETRY_INTERVALS`
/// floor through a path that floor cannot see.
///
/// `assert_still_standing_by` can't catch that: it derives its expected call
/// count from `retry_interval`, so it is scale-invariant to the timeout. This
/// asserts the count in absolute terms instead.
#[tokio::test(start_paused = true)]
async fn a_hung_acquisition_is_bounded_by_the_retry_interval() {
    let store = std::sync::Arc::new(FakeStore::default());
    store.set_hang(true);
    let mut config = budgeted_config("me", Duration::from_secs(600));
    config.lease_ttl = Duration::from_secs(60);
    config.retry_interval = Duration::from_secs(8);

    // 600s of budget at an 8s cadence is ~75 attempts; bounded by the 60s TTL it
    // would be ~10, and bounded by the budget itself, 1.
    expect_lease_unreachable(store.clone(), config).await;
    assert!(
        store.calls() >= 50,
        "a hung call must be abandoned after retry_interval, giving ~75 attempts \
         across the budget; got {}",
        store.calls()
    );
}

/// The budget's own arithmetic, without the loop. For an uninterrupted segment,
/// `charge` reports the accumulated time only once it spans the whole grace; the
/// anchor is the *first* failure, so the interval grows across calls rather than
/// restarting.
#[test]
fn failure_budget_reports_only_once_the_grace_is_spanned() {
    let grace = Duration::from_secs(600);
    let mut budget = super::FailureBudget::new(PositiveDuration::new_panic(grace));
    let start = tokio::time::Instant::now();

    assert_eq!(budget.charge(start), None, "the first failure never trips");
    assert_eq!(
        budget.charge(start + Duration::from_secs(599)),
        None,
        "one second short of the grace is still transient"
    );
    assert_eq!(
        budget.charge(start + grace),
        Some(grace),
        "the grace elapsed since the first failure, so the budget is spent"
    );
}

/// A single success returns the whole budget, so isolated failures cannot
/// accumulate into a restart.
#[test]
fn failure_budget_reset_returns_the_whole_window() {
    let grace = Duration::from_secs(600);
    let mut budget = super::FailureBudget::new(PositiveDuration::new_panic(grace));
    let start = tokio::time::Instant::now();

    assert_eq!(budget.charge(start), None);
    assert_eq!(budget.charge(start + Duration::from_secs(599)), None);
    budget.reset();
    // Re-anchored: 599s of prior failure buys nothing toward the next run.
    assert_eq!(budget.charge(start + Duration::from_secs(600)), None);
    assert_eq!(
        budget.charge(start + Duration::from_secs(1199)),
        None,
        "still one second short of a full grace since the reset"
    );
    assert_eq!(
        budget.charge(start + Duration::from_secs(1200)),
        Some(grace),
        "a full grace after the reset spends the budget"
    );
}

/// The liveness budget end to end: a standby that fails *every* lease call for
/// the whole grace reports [`Campaign::LeaseUnreachable`], which `main` turns
/// into a non-zero exit so kubelet restarts it. Without this the process stays
/// alive and NotReady forever with nothing to restart it.
#[tokio::test(start_paused = true)]
async fn campaign_reports_lease_unreachable_past_the_liveness_budget() {
    let store = std::sync::Arc::new(FakeStore::default());
    store.set_fail_every(1);
    expect_lease_unreachable(store, budgeted_config("me", Duration::from_secs(600))).await;
}

/// The other failure shape charges the same budget: a half-open connection whose
/// every query hangs until the per-attempt timeout is exactly the wedge a
/// restart fixes, and it reaches the loop as `Elapsed` rather than an error.
#[tokio::test(start_paused = true)]
async fn campaign_reports_lease_unreachable_when_every_query_hangs() {
    let store = std::sync::Arc::new(FakeStore::default());
    store.set_hang(true);
    expect_lease_unreachable(store, budgeted_config("me", Duration::from_secs(600))).await;
}

/// An unusable clock spends the budget too, and for a different reason than a
/// transport failure: the replica cannot reason about expiry at all, and a clock
/// may have been stepped since the process started, so a restart is worth trying.
#[tokio::test(start_paused = true)]
async fn campaign_reports_lease_unreachable_on_a_sustained_clock_failure() {
    let store = std::sync::Arc::new(FakeStore::default());
    store.set_fail_every(1);
    store.set_failure_shape(InjectedFailure::Clock);
    expect_lease_unreachable(store, budgeted_config("me", Duration::from_secs(600))).await;
}

/// A brownout shorter than the budget must end in election after recovery,
/// without costing a restart.
#[tokio::test(start_paused = true)]
async fn campaign_rides_out_a_brownout_shorter_than_the_budget() {
    let store = std::sync::Arc::new(FakeStore::default());
    store.set_fail_every(1);

    // The apiserver comes back well inside the budget.
    let recovering = store.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(120)).await;
        recovering.set_fail_every(0);
    });

    let outcome = tokio::time::timeout(
        Duration::from_secs(6_000),
        super::campaign(
            store.clone(),
            budgeted_config("me", Duration::from_secs(600)),
            CancellationToken::new(),
        ),
    )
    .await
    .expect("campaign must reach a decision once the lease is reachable again");
    match outcome {
        Campaign::Elected(leadership) => {
            assert_eq!(store.holder().as_deref(), Some("me"));
            leadership.resign().await;
        }
        Campaign::LeaseUnreachable => {
            panic!("a brownout shorter than the budget must not spend it")
        }
        Campaign::ShutdownBeforeElected => panic!("shutdown was never requested"),
    }
}

/// A single successful call resets the budget in full. A standby watching a peer
/// hold the lease while every other call errors is a flapping apiserver, not a
/// wedged client — it must stand by indefinitely, well past the grace window,
/// rather than accumulate its way to a restart.
#[tokio::test(start_paused = true)]
async fn campaign_resets_the_liveness_budget_on_any_successful_call() {
    let store = std::sync::Arc::new(FakeStore::default());
    // A peer holds the lease, so successful calls return `Ok(false)` — the
    // healthy-standby path, the one that has to clear the failure run.
    store.set_holder(Some("other"));
    store.set_fail_every(2);
    let grace = Duration::from_secs(600);

    // Five grace windows of half-failing calls: if a success did not reset the
    // run, the budget would be spent in the first one.
    assert_still_standing_by(store.clone(), budgeted_config("me", grace), grace * 5).await;
    assert_eq!(
        store.holder().as_deref(),
        Some("other"),
        "standby must not steal the peer's lease"
    );
}

/// A control plane shedding load must not be answered with a restart: the
/// replacement re-LISTs every Node against the same overloaded apiserver, which
/// is the opposite of help. So a run of 429s longer than the whole budget still
/// ends in no decision — the replica stays up, NotReady, retrying.
///
/// Table-driven over the codes that mean "not now" and the codes that mean "no",
/// because both categories share this behaviour and a separate test per code
/// would be four copies of one contract.
#[tokio::test(start_paused = true)]
async fn campaign_does_not_spend_the_budget_on_answers_a_restart_cannot_change() {
    let grace = Duration::from_secs(600);
    for code in [429, 503, 403, 401, 404] {
        let store = std::sync::Arc::new(FakeStore::default());
        store.set_fail_every(1);
        store.set_failure_shape(InjectedFailure::Status(code));
        // Five budgets' worth. A charging category would exit in the first.
        assert_still_standing_by(store, budgeted_config("me", grace), grace * 5).await;
    }
}

/// A replica must not restart unless it has been unable to reach the lease, for
/// reasons a restart could fix, for the whole budget. A run that starts
/// unreachable, receives 429s for a while, and returns to unreachable therefore
/// excludes the answered interval: those 429s prove it did reach the lease.
#[tokio::test(start_paused = true)]
async fn answered_failures_do_not_count_toward_the_liveness_budget() {
    let grace = Duration::from_secs(600);
    let store = std::sync::Arc::new(FakeStore::default());
    store.set_fail_every(1);

    // Unreachable for the first third, 429 for the second, then unreachable
    // again. Only the two unreachable spans may accumulate toward `grace`.
    let flipping = store.clone();
    tokio::spawn(async move {
        tokio::time::sleep(grace / 3).await;
        flipping.set_failure_shape(InjectedFailure::Status(429));
        tokio::time::sleep(grace / 3).await;
        flipping.set_failure_shape(InjectedFailure::Transport);
    });

    let started = tokio::time::Instant::now();
    expect_lease_unreachable(store, budgeted_config("me", grace)).await;
    let took = started.elapsed();
    // The bounds separate the three policies this sequence can accidentally
    // encode, and they're derived from this fixture rather than picked:
    // `budgeted_config` sets `lease_failure_grace` on the struct directly and
    // never calls `Timings::derive`, so the per-identity spread is not applied
    // here and the budget is exactly `grace`. (An earlier version of this comment
    // reasoned from a 25% spread that this path never adds.)
    //
    //   wall-clock anchor: exits around `grace`, retroactively billing the 429s;
    //   chargeable spans: exits around `grace + grace / 3`, after accumulating
    //                     the unreachable time on both sides of the 429s;
    //   full reset:       exits around `grace + grace * 2 / 3`, after discarding
    //                     the first unreachable span along with the 429s.
    //
    // The implementation deliberately undercounts one retry step when a segment
    // resumes, so the lower bound is the exact ideal accumulation time and the
    // upper bound leaves one sixth of the grace for timer slack while remaining
    // below the full-reset outcome.
    assert!(
        took >= grace + grace / 3 && took < grace + grace / 2,
        "exit must exclude the answered 429 interval and accumulate only the two unreachable \
         spans, took {took:?}"
    );
}

/// A refusal is not a restart, but it is still NotReady: the replica cannot take
/// over, so the readiness marker has to be cleared even though the budget is
/// untouched. Otherwise a fleet held out of the election by revoked RBAC would
/// report Ready with no reconciler running.
#[tokio::test(start_paused = true)]
async fn a_refused_lease_call_still_clears_readiness() {
    let path = marker_path("refused-clears-readiness");
    let _ = std::fs::remove_file(&path);
    let store = std::sync::Arc::new(FakeStore::default());
    // A peer holds the lease, so the first call answers `Ok(false)` and this
    // replica reports itself a healthy standby.
    store.set_holder(Some("other"));
    let mut config = budgeted_config("me", Duration::from_secs(600));
    config.readiness_file = Some(path.clone());

    let shutdown = CancellationToken::new();
    let campaign = tokio::spawn(super::campaign(store.clone(), config, shutdown.clone()));
    wait_for_marker(&path, true).await;

    // RBAC is revoked out from under it.
    store.set_fail_every(1);
    store.set_failure_shape(InjectedFailure::Status(403));
    wait_for_marker(&path, false).await;

    shutdown.cancel();
    campaign.await.expect("campaign task");
    let _ = std::fs::remove_file(&path);
}

/// Grace of `0` (`None` after parsing) is the kill-switch: the replica retries
/// forever exactly as it did before the liveness budget existed.
#[tokio::test(start_paused = true)]
async fn campaign_never_gives_up_when_the_liveness_budget_is_disabled() {
    let store = std::sync::Arc::new(FakeStore::default());
    store.set_fail_every(1);
    let mut config = budgeted_config("me", Duration::from_secs(600));
    config.lease_failure_grace = None;

    // Many times the budget it would have had, with every call failing.
    assert_still_standing_by(store, config, Duration::from_secs(6_000)).await;
}

/// Shutdown wins over the liveness budget: a pod being deleted mid-outage must
/// report the clean standby-never-elected outcome, not the restart-me one, or a
/// normal termination that happens to land at the end of a long brownout reports
/// a failure exit.
///
/// The race this pins is narrow and the timings are load-bearing. `select!`
/// picks randomly between two ready arms, so a cancelled token plus a due tick
/// can still run one more lease call — and if *that* call is the one that spends
/// the budget, only the post-call check stops the loop reporting
/// `LeaseUnreachable` for a pod that is already going away. Cancelling anywhere
/// earlier in the window is not a test of that: the `select!` arm alone answers
/// correctly, so the check could be deleted and this would still pass.
///
/// So: hang every call, which makes each attempt span exactly one
/// `retry_interval`, giving attempts at 0s, 2s, ... 600s. The attempt starting
/// at 600s is the one that spends the 600s budget, and it runs until 602s.
/// Cancelling at 601s lands *inside* it, so the loop reaches the post-call check
/// with the budget spent and shutdown pending — the one interleaving where the
/// two disagree.
#[tokio::test(start_paused = true)]
async fn campaign_prefers_shutdown_over_a_budget_spent_in_the_same_iteration() {
    let store = std::sync::Arc::new(FakeStore::default());
    store.set_hang(true);
    let shutdown = CancellationToken::new();

    let cancelling = shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(601)).await;
        cancelling.cancel();
    });

    let outcome = tokio::time::timeout(
        Duration::from_secs(6_000),
        super::campaign(
            store,
            budgeted_config("me", Duration::from_secs(600)),
            shutdown,
        ),
    )
    .await
    .expect("campaign must return once shutdown is requested");
    match outcome {
        Campaign::ShutdownBeforeElected => {}
        Campaign::LeaseUnreachable => {
            panic!("a terminating pod must not report itself as needing a restart")
        }
        Campaign::Elected(_) => panic!("every lease call failed, so election is impossible"),
    }
}

/// The liveness budget an operator asks for is floored the same way the TTL is,
/// against the *resolved* TTL. Without this the env knob is the one input that
/// can defeat the whole design: a grace near the lease timings restarts the pod
/// on exactly the transients readiness exists to absorb.
#[test]
fn derive_timings_floors_an_operator_supplied_liveness_budget() {
    use super::{MIN_GRACE_LEASE_TTL_MULTIPLE, Timings};

    // Note: the expected floors below are derived from the same multiples they
    // test, so on their own they are scale-invariant — 4 -> 3 keeps them green.
    // What actually fences both multiples is
    // `configured_timings_above_coincident_floors_are_honoured_unchanged`, which pins
    // `60s * MULTIPLE == 240s` and `8s * INTERVALS == 240s` absolutely. Deleting
    // those two lines would silently unpin both.
    let ttl = Duration::from_secs(30);
    let retry = Duration::from_secs(2);
    let floor = ttl * MIN_GRACE_LEASE_TTL_MULTIPLE;

    // Far too short: raised to the floor rather than honoured.
    let tightened = Timings::derive(
        ttl,
        retry,
        PositiveDuration::new(Duration::from_secs(3)),
        "me",
    );
    assert_budget_near(
        tightened.lease_failure_grace,
        floor,
        "a 3s budget must not survive",
    );

    // Comfortably above the floor: honoured as requested.
    let honoured = Timings::derive(
        ttl,
        retry,
        PositiveDuration::new(Duration::from_secs(600)),
        "me",
    );
    assert_budget_near(
        honoured.lease_failure_grace,
        Duration::from_secs(600),
        "a budget above both floors is honoured",
    );

    // The floor tracks the resolved TTL, not a constant.
    let long_ttl = Duration::from_secs(120);
    let scaled = Timings::derive(
        long_ttl,
        retry,
        PositiveDuration::new(Duration::from_secs(60)),
        "me",
    );
    assert_budget_near(
        scaled.lease_failure_grace,
        long_ttl * MIN_GRACE_LEASE_TTL_MULTIPLE,
        "the floor scales with the TTL",
    );

    // The floor uses the *resolved* TTL: a sub-floor TTL is raised to
    // MIN_LEASE_TTL first, so the budget floor is 4x18s rather than 4x1s.
    let tiny_ttl = Timings::derive(
        Duration::from_secs(1),
        retry,
        PositiveDuration::new(Duration::from_secs(60)),
        "me",
    );
    assert_budget_near(
        tiny_ttl.lease_failure_grace,
        super::MIN_LEASE_TTL * MIN_GRACE_LEASE_TTL_MULTIPLE,
        "the floor must key off the resolved TTL, not the requested one",
    );

    // Disabled stays disabled — there's no floor to clear when there's no budget.
    assert_eq!(
        Timings::derive(ttl, retry, None, "me").lease_failure_grace,
        None
    );
}

/// The test-hook budget is a request to `Timings::derive` like the default:
/// honoured between the floor and the ceiling, clamped outside them, and an
/// absent or unusable value leaves the default.
#[test]
fn the_liveness_budget_override_is_bounded_like_the_default() {
    use super::{
        DEFAULT_LEASE_FAILURE_GRACE, DEFAULT_LEASE_TTL, LEASE_FAILURE_GRACE_TEST_ENV,
        LeaderElectionConfig, MAX_LEASE_FAILURE_GRACE, MIN_GRACE_LEASE_TTL_MULTIPLE,
    };

    let budget = |value: Option<&str>| {
        let mut env = pplx_space_test_env::TestEnv::default();
        env.set("HOSTNAME", "space-cluster-controller-abc123");
        if let Some(value) = value {
            env.set(LEASE_FAILURE_GRACE_TEST_ENV, value);
        }
        LeaderElectionConfig::from_environment(&env).lease_failure_grace
    };
    let floor = DEFAULT_LEASE_TTL * MIN_GRACE_LEASE_TTL_MULTIPLE;

    assert_budget_near(
        budget(Some("300")),
        Duration::from_secs(300),
        "a budget between the bounds is honoured",
    );
    assert_budget_near(
        budget(Some("10")),
        floor,
        "a budget below the floor is raised to it",
    );
    assert_budget_near(
        budget(Some("999999")),
        MAX_LEASE_FAILURE_GRACE,
        "a budget above the ceiling is lowered to it",
    );
    for unusable in [None, Some(""), Some("0"), Some("ten"), Some("-5")] {
        assert_budget_near(
            budget(unusable),
            DEFAULT_LEASE_FAILURE_GRACE,
            &format!("{unusable:?} leaves the default"),
        );
    }
}

/// The test-hook TTL is a request to `Timings::derive` like the default:
/// clamped into the TTL's range, and the retry interval and the budget's floor
/// follow the resolved TTL. At the TTL floor the brownout scenario's budget
/// floor is 72 s; an absent or unusable value leaves the default TTL.
#[test]
fn the_lease_ttl_override_is_bounded_like_the_default() {
    use super::{
        DEFAULT_LEASE_TTL, LEASE_FAILURE_GRACE_TEST_ENV, LEASE_TTL_TEST_ENV, LeaderElectionConfig,
        MAX_LEASE_TTL, MIN_LEASE_TTL,
    };

    let config = |ttl: Option<&str>, budget: Option<&str>| {
        let mut env = pplx_space_test_env::TestEnv::default();
        env.set("HOSTNAME", "space-cluster-controller-abc123");
        if let Some(ttl) = ttl {
            env.set(LEASE_TTL_TEST_ENV, ttl);
        }
        if let Some(budget) = budget {
            env.set(LEASE_FAILURE_GRACE_TEST_ENV, budget);
        }
        LeaderElectionConfig::from_environment(&env)
    };

    let floor = config(Some("18"), Some("1"));
    assert_eq!(floor.lease_ttl, MIN_LEASE_TTL);
    assert_eq!(floor.renew_deadline, Duration::from_secs(6));
    assert_eq!(floor.retry_interval, Duration::from_secs(2));
    assert_budget_near(
        floor.lease_failure_grace,
        Duration::from_secs(72),
        "the budget's floor follows the TTL floor",
    );

    assert_eq!(config(Some("30"), None).lease_ttl, Duration::from_secs(30));
    assert_eq!(config(Some("5"), None).lease_ttl, MIN_LEASE_TTL);
    assert_eq!(config(Some("999999"), None).lease_ttl, MAX_LEASE_TTL);
    for unusable in [None, Some(""), Some("0"), Some("ten"), Some("-5")] {
        assert_eq!(
            config(unusable, None).lease_ttl,
            DEFAULT_LEASE_TTL,
            "{unusable:?} leaves the default"
        );
    }
}

/// The cadence floor must never grow so large that it disables the budget it
/// exists to protect. A typo'd retry interval of ~1e9 seconds would otherwise
/// saturate the floor to `Duration::MAX`, leaving the replica permanently
/// NotReady with a liveness budget it can never reach — the exact failure the
/// floor prevents, reached from the other side.
#[test]
fn derive_timings_bounds_the_cadence_floor_for_an_absurd_retry_interval() {
    use super::{MIN_GRACE_RETRY_INTERVALS, Timings};

    // A 1e9s retry interval clamps to 10s at this TTL — the room left after the
    // renewal cadence, the readiness write and a strictness margin — so the
    // cadence floor is 300s rather than the years the raw value would demand.
    let ttl = Duration::from_secs(30);
    let derived = Timings::derive(
        ttl,
        Duration::from_secs(1_000_000_000),
        PositiveDuration::new(Duration::from_secs(600)),
        "me",
    );
    assert_eq!(derived.retry_interval, Duration::from_secs(10));
    let floor = derived.retry_interval * MIN_GRACE_RETRY_INTERVALS;
    assert_eq!(floor, Duration::from_secs(300));
    // The requested 600s clears that floor, so it survives — the point being
    // that the floor stayed reachable at all.
    assert_budget_near(
        derived.lease_failure_grace,
        Duration::from_secs(600),
        "an absurd retry interval must not put the budget out of reach",
    );

    // The largest floor any TTL can produce is still far inside the ceiling.
    for ttl_secs in [18u64, 30, 60, 300] {
        let t = Timings::derive(
            Duration::from_secs(ttl_secs),
            Duration::from_secs(1_000_000_000),
            PositiveDuration::new(Duration::from_secs(600)),
            "me",
        );
        let cadence_floor = t.retry_interval * MIN_GRACE_RETRY_INTERVALS;
        assert!(
            cadence_floor <= super::MAX_LEASE_FAILURE_GRACE,
            "at ttl {ttl_secs}s the cadence floor {cadence_floor:?} must stay inside the ceiling"
        );
    }
}

/// The lease TTL is clamped from above as well as below. Without a ceiling it is
/// the one knob that can defeat the budget from the other side: a seconds-for-
/// millis slip puts the TTL-derived grace floor hours out, so the budget is
/// floored beyond reach and the liveness exit is silently dead.
#[test]
fn derive_timings_clamps_an_oversized_lease_ttl() {
    use super::{MAX_LEASE_TTL, MIN_GRACE_LEASE_TTL_MULTIPLE, Timings};

    let derived = Timings::derive(
        Duration::from_secs(30_000),
        Duration::from_secs(2),
        PositiveDuration::new(Duration::from_secs(600)),
        "me",
    );
    assert_eq!(
        derived.lease_ttl, MAX_LEASE_TTL,
        "TTL must clamp to the ceiling"
    );
    assert_budget_near(
        derived.lease_failure_grace,
        MAX_LEASE_TTL * MIN_GRACE_LEASE_TTL_MULTIPLE,
        "the grace floor must follow the clamped TTL, not the requested one",
    );
    // And the derived relationships still hold at the ceiling.
    assert!(derived.renew_interval < derived.renew_deadline);
    assert!(derived.renew_deadline < derived.lease_ttl);
}

/// Replicas must not give up in lockstep: they lose the lease path at the same
/// instant, so an unjittered budget makes them exit, restart and back off
/// together, and an outage ending inside a shared backoff window leaves *no*
/// running replica to acquire the lease — the zero-reconciler gap this whole
/// design removes. Different pod names must therefore yield different budgets,
/// and the spread must only ever add, so every floor still holds.
#[test]
fn the_budget_is_spread_across_replica_identities() {
    let base = Duration::from_secs(600);
    let budget = |identity: &str| {
        super::spread_across_replicas(PositiveDuration::new_panic(base), identity).get()
    };

    let a = budget("space-cluster-controller-abc123");
    let b = budget("space-cluster-controller-def456");
    assert_ne!(a, b, "two pods must not share a deadline");
    for (identity, value) in [("abc123", a), ("def456", b)] {
        assert!(
            value >= base && value <= base + base / 4,
            "{identity}: {value:?} must add at most 25% and never subtract"
        );
    }
    // Stable across calls, so the resolved budget is reproducible for a pod.
    assert_eq!(a, budget("space-cluster-controller-abc123"));

    // The band must actually be *used*, not merely non-zero. Capping the spread
    // at a second would satisfy an "a != b" assertion while leaving replicas
    // effectively in lockstep, which is the whole failure being avoided — so
    // sample realistic pod names (long shared prefix, short random suffix, which
    // is the hard case for a hash) and require the draws to cover most of the
    // band and to reach well into its upper half.
    // Real ReplicaSet pod names: a long shared prefix and a 5-character suffix
    // from the alphabet Kubernetes actually uses. The shared prefix is the hard
    // case for a hash, so sampling any other shape wouldn't test what it claims.
    // Index each character independently so the space is 27^5 rather than a
    // single arithmetic progression — an earlier version used `i * 7 + k * 31`,
    // and since 31 = 4 (mod 27) that collapsed 200 names onto 27 distinct ones.
    const SUFFIX: &[u8] = b"bcdfghjklmnpqrstvwxz2456789";
    let names: Vec<String> = (0..200usize)
        .map(|i| {
            let suffix: String = (0..5)
                .map(|k| {
                    let idx =
                        (i / SUFFIX.len().pow(u32::try_from(k).expect("small"))) % SUFFIX.len();
                    char::from(SUFFIX[idx])
                })
                .collect();
            format!("space-cluster-controller-67dlhfgcjr-{suffix}")
        })
        .collect();
    assert_eq!(
        names
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        names.len(),
        "the sample must be distinct names, or the histogram overstates its evidence"
    );

    // The band is a literal quarter, NOT `base / GRACE_SPREAD_DIVISOR`. Deriving
    // it from the constant under test makes every check below scale-invariant:
    // shrink the spread and the yardstick shrinks with it, so a 600ms spread over
    // a 600s budget — total lockstep, the thing this exists to prevent — would
    // still pass. Verified: it did, twice.
    let band = base / 4;
    // Require a meaningful absolute spread for this 600s example budget.
    assert!(
        base / super::GRACE_SPREAD_DIVISOR >= Duration::from_secs(60),
        "the spread must be worth at least a minute on a 600s budget"
    );
    // Decile occupancy, not just min/max: a hash that piled most names onto one
    // value and sent one to the far end would satisfy a range check while
    // leaving replicas in lockstep.
    let mut deciles = [0usize; 10];
    for name in &names {
        let draw = budget(name) - base;
        assert!(draw < band, "the spread must stay inside the band");
        let decile = usize::try_from(draw.as_millis() * 10 / band.as_millis())
            .expect("decile fits")
            .min(9);
        deciles[decile] += 1;
    }
    assert!(
        deciles.iter().all(|count| *count > 0),
        "the spread must reach every decile of the band, got {deciles:?}"
    );

    // And the spread is actually *wired into* the resolved config — testing the
    // function alone would pass even if `derive` never called it.
    let resolved = |identity: &str| {
        super::Timings::derive(
            Duration::from_secs(30),
            Duration::from_secs(2),
            PositiveDuration::new(base),
            identity,
        )
        .lease_failure_grace
        .expect("budget must be set")
        .get()
    };
    assert_ne!(
        resolved("space-cluster-controller-abc123"),
        resolved("space-cluster-controller-def456"),
        "two replicas must not resolve the same budget"
    );

    // And a *floored* budget must be spread too. This is the common case, not a
    // corner: raising the retry interval floors every replica to the same value,
    // so spreading only the honoured path would leave the whole fleet in
    // lockstep exactly when an operator has tuned the knobs.
    let floored = |identity: &str| {
        super::Timings::derive(
            Duration::from_secs(30),
            Duration::from_secs(2),
            PositiveDuration::new(Duration::from_secs(3)),
            identity,
        )
        .lease_failure_grace
        .expect("budget must be set")
        .get()
    };
    assert_ne!(
        floored("space-cluster-controller-abc123"),
        floored("space-cluster-controller-def456"),
        "a budget raised to the floor must still be spread across replicas"
    );
}

/// The budget is spent by *sampling* the lease, so it is floored against the
/// retry cadence as well as the TTL. Without this a wide retry interval decides
/// from too few points: an apiserver healthy except at the sampling instants
/// would restart the pod on a handful of them.
#[test]
fn derive_timings_floors_the_budget_against_the_retry_cadence() {
    use super::{MIN_GRACE_RETRY_INTERVALS, Timings};

    // At a 30s TTL the deadline is 18s and the renewal cadence 6s, so the retry
    // ceiling is 10s — the room left for one renewal attempt strictly inside the
    // deadline once the readiness write is accounted for, tighter than both the
    // TTL and MAX_RETRY_INTERVAL.
    let ttl = Duration::from_secs(30);
    let floored = Timings::derive(
        ttl,
        Duration::from_secs(600),
        PositiveDuration::new(Duration::from_secs(600)),
        "me",
    );
    let resolved_retry = Duration::from_secs(10);
    assert_eq!(
        floored.retry_interval, resolved_retry,
        "the retry ceiling must leave room for a renewal attempt inside the deadline"
    );
    // 600s at a 10s cadence is 60 samples, comfortably over the 30 the floor
    // demands, so it is honoured — the floor only bites a smaller budget.
    assert_budget_near(
        floored.lease_failure_grace,
        Duration::from_secs(600),
        "a budget worth 60 samples must be honoured",
    );

    // 100s at that cadence is 10 samples, so the cadence floor raises it to 300s
    // — above the 120s the TTL floor would have asked for.
    let too_few = Timings::derive(
        ttl,
        Duration::from_secs(600),
        PositiveDuration::new(Duration::from_secs(100)),
        "me",
    );
    // Spelled against the *resolved retry interval*, not the TTL. An earlier
    // version multiplied the TTL by this multiple and was right only because
    // that case's TTL happened to equal MAX_RETRY_INTERVAL.
    assert_budget_near(
        too_few.lease_failure_grace,
        resolved_retry * MIN_GRACE_RETRY_INTERVALS,
        "a budget worth too few samples must be raised to the cadence floor",
    );

    // The larger of the two floors wins.
    let tight_retry = Timings::derive(
        Duration::from_secs(300),
        Duration::from_secs(1),
        PositiveDuration::new(Duration::from_secs(10)),
        "me",
    );
    assert_budget_near(
        tight_retry.lease_failure_grace,
        Duration::from_secs(1200),
        "with a 300s TTL the TTL floor (1200s) outranks the cadence floor (30s)",
    );
}

/// The retry interval is also the per-attempt *acquire* timeout, and the first
/// renew deadline is anchored on the instant before that acquire. So an acquire
/// allowed to outlast `renew_deadline - renew_interval` can succeed with its own
/// deadline already in the past: the holder resigns before its first renewal
/// tick, releases the lease, exits non-zero, re-acquires, and repeats — a
/// crash-loop with no reconciler running that the liveness budget never sees,
/// because every call *succeeded*.
///
/// The TTL cap alone accepts it: at a 30s TTL, a 25s retry interval
/// sits under both the TTL and `MAX_RETRY_INTERVAL`. Worse, the operator text
/// used to suggest raising this very knob when acquisitions time out.
#[test]
fn derive_timings_leaves_room_for_a_renewal_inside_the_deadline() {
    use super::Timings;

    for ttl_secs in [18u64, 20, 30, 41, 60, 120, 300] {
        let t = Timings::derive(
            Duration::from_secs(ttl_secs),
            Duration::from_secs(ttl_secs),
            PositiveDuration::new(Duration::from_secs(600)),
            "me",
        );
        // Strict, and inclusive of the readiness write that sits between the
        // acquire returning and the renewal loop's first tick. Non-strict was
        // reachable at ttl 18 (4 + 2 == 6 == deadline), where a tie makes
        // `renew_loop`'s `select!` a coin flip on resigning with no renewal
        // attempted at all.
        // Note this reduces to `1s > 0` whenever the deadline-room term is the
        // binding one, which is most of this loop — what actually pins the
        // strictness margin is the absolute `assert_eq!(retry_interval, 10s)`
        // below and in `derive_timings_floors_the_budget_against_the_retry_cadence`.
        // Deleting either would silently unpin it.
        assert!(
            t.retry_interval + t.renew_interval + super::FIRST_RENEWAL_HEADROOM <= t.renew_deadline,
            "at ttl {ttl_secs}s an acquire consuming its whole timeout, plus the readiness \
             write, must still leave a renewal attempt strictly inside the deadline: {t:?}"
        );
        assert!(
            t.retry_interval >= Duration::from_secs(1),
            "the retry interval must stay positive: {t:?}"
        );
    }

    // The specific configuration that used to be accepted.
    let t = Timings::derive(
        Duration::from_secs(30),
        Duration::from_secs(25),
        PositiveDuration::new(Duration::from_secs(600)),
        "me",
    );
    assert_eq!(
        t.retry_interval,
        Duration::from_secs(10),
        "a 25s acquire timeout under an 18s renew deadline must be clamped to the room left \
         after the renewal cadence, the readiness write and a strictness margin"
    );
}

/// Every knob is bounded, so no env combination leaves the budget unreachable or
/// panics the process. Asserts the resolved *values*: checking only `is_some()`
/// is satisfied by any non-panicking implementation, and the "floor arithmetic
/// saturates" rationale an earlier version gave is unreachable now that the TTL
/// is clamped before any multiplication.
#[test]
fn derive_timings_bounds_every_knob_against_absurd_input() {
    use super::{MAX_LEASE_FAILURE_GRACE, MAX_LEASE_TTL, MIN_GRACE_LEASE_TTL_MULTIPLE, Timings};

    // Absurd everything: each knob clamps, and the budget stays reachable.
    let absurd = Duration::from_secs(u64::MAX / 2);
    let derived = Timings::derive(absurd, absurd, PositiveDuration::new(absurd), "me");
    assert_eq!(derived.lease_ttl, MAX_LEASE_TTL);
    assert_eq!(derived.retry_interval, super::MAX_RETRY_INTERVAL);
    let grace = derived.lease_failure_grace.expect("budget set").get();
    // Exactly the ceiling plus at most a quarter of spread — not merely
    // somewhere in a range three times wider than the value.
    assert!(
        grace >= MAX_LEASE_FAILURE_GRACE
            && grace <= MAX_LEASE_FAILURE_GRACE + MAX_LEASE_FAILURE_GRACE / 4,
        "an absurd budget must clamp to the ceiling, got {grace:?}"
    );

    // The seconds-for-millis slip on the documented default: 600000 instead of
    // 600 is ~6.9 days, which no pod lives long enough to spend.
    let slipped = Timings::derive(
        Duration::from_secs(60),
        Duration::from_secs(8),
        PositiveDuration::new(Duration::from_secs(600_000)),
        "me",
    );
    assert_budget_near(
        slipped.lease_failure_grace,
        MAX_LEASE_FAILURE_GRACE,
        "a budget above the ceiling must clamp down, not be honoured",
    );

    // A wide retry interval must not push the floor past the ceiling either.
    let wide_retry = Timings::derive(
        MAX_LEASE_TTL,
        Duration::from_secs(300),
        PositiveDuration::new(Duration::from_secs(600)),
        "me",
    );
    // TTL 300 -> TTL floor 1200s; retry clamps to 30s -> cadence floor 900s; so
    // the requested 600s is raised to 1200s, well inside the 3600s ceiling.
    assert_budget_near(
        wide_retry.lease_failure_grace,
        MAX_LEASE_TTL * MIN_GRACE_LEASE_TTL_MULTIPLE,
        "the larger floor must win and stay inside the ceiling",
    );
}

/// When both derived floors coincide below the configured budget, preserve
/// the caller's timings instead of replacing the budget with the floor.
#[test]
fn configured_timings_above_coincident_floors_are_honoured_unchanged() {
    use super::{MIN_GRACE_LEASE_TTL_MULTIPLE, MIN_GRACE_RETRY_INTERVALS, Timings};

    let ttl = Duration::from_secs(60);
    let retry = Duration::from_secs(8);
    let derived = Timings::derive(
        ttl,
        retry,
        PositiveDuration::new(Duration::from_secs(600)),
        "space-cluster-controller-abc123",
    );
    assert_eq!(derived.lease_ttl, ttl, "configured TTL must not be clamped");
    assert_eq!(
        derived.retry_interval, retry,
        "configured retry must not be clamped"
    );
    assert_eq!(ttl * MIN_GRACE_LEASE_TTL_MULTIPLE, Duration::from_secs(240));
    assert_eq!(retry * MIN_GRACE_RETRY_INTERVALS, Duration::from_secs(240));
    assert_budget_near(
        derived.lease_failure_grace,
        Duration::from_secs(600),
        "configured budget must be honoured, not floored",
    );
}
