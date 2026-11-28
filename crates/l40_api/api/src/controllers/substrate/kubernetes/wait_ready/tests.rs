use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::http::{Method, StatusCode};
use k8s_openapi::api::core::v1::{NodeAddress, NodeStatus};
use pplx_space_sbx_mgr_client::proto::SandboxInfo;

use super::*;
use crate::controllers::substrate::kubernetes::adapter_characterization_tests::sandbox_pod;
use crate::controllers::substrate::kubernetes::scripted_test_doubles::{
    BoxError, KubeReply, KubeRequest, WORKER_NODE, kube_status, pod_path, pod_reply,
    requests_matching, scripted_kube_client, scripted_kube_client_async,
};
use crate::controllers::substrate::kubernetes::test_substrate::seeded_substrate;
use crate::controllers::substrate::kubernetes::tests::{
    insert_k8s_execution, insert_test_sandbox_row,
};
use crate::metrics::sandbox::api::SandboxApiMetricIdentity;

type TestResult = Result<(), BoxError>;

fn info(sandbox_id: SandboxId, generation: i64, pod_uid: &str, pod_ip: &str) -> SandboxInfo {
    SandboxInfo {
        sandbox_id: sandbox_id.to_string(),
        execution_generation: Some(generation),
        pod_uid: Some(pod_uid.to_owned()),
        external_address: Some(pod_ip.to_owned()),
        ..Default::default()
    }
}

fn worker_node() -> Node {
    Node {
        metadata: kube::api::ObjectMeta {
            name: Some(WORKER_NODE.to_string()),
            uid: Some(format!("{WORKER_NODE}-uid")),
            ..Default::default()
        },
        status: Some(NodeStatus {
            addresses: Some(vec![NodeAddress {
                type_: "InternalIP".to_string(),
                address: "127.0.0.1".to_string(),
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn seed_unpersisted_creating_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    pod_name: &str,
) -> TestResult {
    insert_test_sandbox_row(pool, sandbox_id, "creating").await?;
    sqlx::query(pplx_space_sql_locator::sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip, state, active)
         VALUES ($1, 1, $2, 'space', NULL, NULL, 'creating', TRUE)"
    ))
    .bind(sandbox_id)
    .bind(pod_name)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_created_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    pod_name: &str,
    pod_uid: &str,
) -> TestResult {
    insert_test_sandbox_row(pool, sandbox_id, "creating").await?;
    insert_k8s_execution(pool, sandbox_id, 1, pod_name, pod_uid, None, true).await?;
    sqlx::query(pplx_space_sql_locator::sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'created'
          WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    Ok(())
}

fn pod_get_only_client(pod_name: &str, reply_pod: Pod) -> (Client, Arc<Mutex<Vec<KubeRequest>>>) {
    let expected_path = pod_path(pod_name);
    scripted_kube_client(move |req| {
        if req.method == Method::GET && req.path == expected_path {
            pod_reply(&reply_pod)
        } else {
            kube_status(StatusCode::INTERNAL_SERVER_ERROR, "unexpected request")
        }
    })
}

fn spawn_pod_ip_waiter(
    substrate: &Arc<KubernetesSubstrate>,
    sandbox_id: SandboxId,
    pod_name: &str,
    pod_uid: &'static str,
    window: Duration,
) -> tokio::task::JoinHandle<Result<Option<(PodIp, PodIpSource)>, SubstrateError>> {
    let substrate = Arc::clone(substrate);
    let pod_name = pod_name.to_owned();
    tokio::spawn(async move {
        substrate
            .wait_for_pod_ip_assignment(
                SandboxApiMetricIdentity::native("create"),
                sandbox_id,
                &PodProxyWaitTarget {
                    pod_name: &pod_name,
                    pod_uid: Some(pod_uid),
                    generation: 1,
                },
                Instant::now() + window,
                "create",
            )
            .await
    })
}

#[test]
fn only_the_observed_executions_address_is_taken() {
    let sandbox_id = SandboxId::new();
    let other_sandbox = SandboxId::new();
    let reported = vec![
        info(other_sandbox, 1, "pod-a", "10.0.0.1"),
        info(sandbox_id, 1, "pod-old", "10.0.0.2"),
        info(sandbox_id, 1, "pod-a", "10.0.0.3"),
    ];
    let taken = |reported, generation, pod_uid| {
        node_reported_address(reported, sandbox_id, generation, pod_uid)
    };

    assert_eq!(
        taken(reported.clone(), 1, "pod-a"),
        Some(PodIp::parse("10.0.0.3").unwrap())
    );
    assert_eq!(taken(reported.clone(), 1, "pod-new"), None);
    assert_eq!(taken(reported, 2, "pod-a"), None);
    assert_eq!(
        taken(vec![info(sandbox_id, 1, "pod-a", "not-an-ip")], 1, "pod-a"),
        None
    );
}

/// The readiness probe accepts only a node serving the target execution
/// at the probed address. For a kubelet-published address, fields an older
/// node omits are not compared; a node-reported one needs them all.
#[test]
fn a_probe_accepts_only_the_target_served_at_its_address() {
    let sandbox_id = SandboxId::new();
    let probed = PodIp::parse("10.0.0.3").unwrap();
    let target = PodProxyWaitTarget {
        pod_name: "pod",
        pod_uid: Some("pod-a"),
        generation: 1,
    };
    let degraded = SandboxInfo {
        sandbox_id: sandbox_id.to_string(),
        ..Default::default()
    };
    for source in [PodIpSource::Published, PodIpSource::NodeReported] {
        let serves =
            |info: SandboxInfo| serves_target(vec![info], sandbox_id, &target, probed, source);
        assert!(serves(info(sandbox_id, 1, "pod-a", "10.0.0.3")));
        // A recreated runtime sandbox's node reports its new allocation.
        assert!(!serves(info(sandbox_id, 1, "pod-a", "10.0.0.4")));
        assert!(!serves(info(sandbox_id, 1, "pod-b", "10.0.0.3")));
        assert!(!serves(info(sandbox_id, 2, "pod-a", "10.0.0.3")));
        assert!(!serves(info(SandboxId::new(), 1, "pod-a", "10.0.0.3")));
        assert!(!serves_target(
            Vec::new(),
            sandbox_id,
            &target,
            probed,
            source
        ));
        assert_eq!(serves(degraded.clone()), source == PodIpSource::Published);
    }
}

/// A teardown acts on a runtime its worker names by identity, skips it
/// only when the worker reports another execution, and does not read
/// missing fields or a moved address as either.
#[test]
fn a_runtime_answer_skips_only_a_worker_that_reports_another_runtime() {
    let sandbox_id = SandboxId::new();
    let probed = PodIp::parse("10.0.0.3").unwrap();
    let target = PodProxyWaitTarget {
        pod_name: "pod",
        pod_uid: Some("pod-a"),
        generation: 1,
    };
    let answer = |info: SandboxInfo| runtime_answer(vec![info], sandbox_id, &target, probed);
    assert_eq!(
        answer(info(sandbox_id, 1, "pod-a", "10.0.0.3")),
        RuntimeAnswer::Serves
    );
    // A restarted sandbox manager no longer reports the address.
    let restarted = SandboxInfo {
        external_address: None,
        ..info(sandbox_id, 1, "pod-a", "10.0.0.3")
    };
    assert_eq!(answer(restarted), RuntimeAnswer::Serves);
    for elsewhere in [
        info(sandbox_id, 1, "pod-b", "10.0.0.3"),
        info(sandbox_id, 2, "pod-a", "10.0.0.3"),
        info(SandboxId::new(), 1, "pod-a", "10.0.0.3"),
    ] {
        assert_eq!(answer(elsewhere), RuntimeAnswer::Elsewhere);
    }
    // The target's execution at another address is still a runtime to
    // clean up, so the teardown waits for an address it can confirm.
    let moved = info(sandbox_id, 1, "pod-a", "10.0.0.4");
    let unidentified_moved = SandboxInfo {
        pod_uid: None,
        ..info(sandbox_id, 1, "pod-a", "10.0.0.4")
    };
    let degraded = SandboxInfo {
        sandbox_id: sandbox_id.to_string(),
        ..Default::default()
    };
    for unconfirmed in [moved, unidentified_moved, degraded] {
        assert_eq!(answer(unconfirmed), RuntimeAnswer::Unconfirmed);
    }
}

#[tokio::test(start_paused = true)]
async fn readiness_fallback_gate_caps_in_flight_and_spaces_starts() {
    let gate = Arc::new(ReadinessFallbackGate::default());
    let in_flight = Arc::new(AtomicUsize::new(0));
    let peak_in_flight = Arc::new(AtomicUsize::new(0));
    let starts = Arc::new(Mutex::new(Vec::new()));
    let deadline = Instant::now() + Duration::from_secs(30);
    let wait_budget = Duration::from_secs(10);

    let mut joins = Vec::new();
    for _ in 0..10 {
        let gate = Arc::clone(&gate);
        let in_flight = Arc::clone(&in_flight);
        let peak_in_flight = Arc::clone(&peak_in_flight);
        let starts = Arc::clone(&starts);
        joins.push(tokio::spawn(async move {
            let permit = gate
                .acquire(deadline, wait_budget)
                .await
                .expect("gate permit must succeed within budget");
            starts.lock().unwrap().push(Instant::now());
            let active = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            peak_in_flight.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(350)).await;
            in_flight.fetch_sub(1, Ordering::SeqCst);
            drop(permit);
        }));
    }

    for join in joins {
        join.await.unwrap();
    }

    assert_eq!(
        peak_in_flight.load(Ordering::SeqCst),
        READINESS_FALLBACK_MAX_IN_FLIGHT,
        "10 overlapping 350ms reads spaced 100ms apart must reach and stop at the 4-slot cap"
    );
    let mut starts = starts.lock().unwrap().clone();
    starts.sort();
    assert_eq!(starts.len(), 10);
    for pair in starts.windows(2) {
        assert!(
            pair[1].duration_since(pair[0]) >= READINESS_FALLBACK_MIN_SPACING,
            "consecutive fallback starts must be spaced by at least 100ms: {:?}",
            pair[1].duration_since(pair[0])
        );
    }
}

#[tokio::test(start_paused = true)]
async fn readiness_fallback_gate_distinguishes_saturation_expiry_and_cancellation() {
    let gate = Arc::new(ReadinessFallbackGate::default());
    let long_deadline = Instant::now() + Duration::from_secs(30);
    let mut held = Vec::new();
    for _ in 0..READINESS_FALLBACK_MAX_IN_FLIGHT {
        held.push(
            gate.acquire(long_deadline, Duration::from_secs(5))
                .await
                .expect("initial slot"),
        );
    }

    let saturated_started = Instant::now();
    let saturated = gate
        .acquire(
            saturated_started + Duration::from_secs(30),
            Duration::from_secs(3),
        )
        .await;
    assert!(matches!(
        saturated,
        Err(FallbackAcquireError::WaitBudgetSaturated)
    ));
    assert_eq!(saturated_started.elapsed(), Duration::from_secs(3));

    let phase_started = Instant::now();
    let phase_elapsed = gate
        .acquire(
            phase_started + Duration::from_millis(500),
            Duration::from_secs(3),
        )
        .await;
    assert!(matches!(
        phase_elapsed,
        Err(FallbackAcquireError::PhaseElapsed)
    ));
    assert_eq!(phase_started.elapsed(), Duration::from_millis(500));

    drop(held);

    // Acquire one slot at t0, cancel a second waiter mid-pacing-sleep at
    // t0 + 40ms, and confirm a third waiter still starts at t0 + 100ms rather
    // than paying a second 100ms penalty from the cancelled waiter.
    let t0 = Instant::now();
    let first = gate
        .acquire(t0 + Duration::from_secs(5), Duration::from_secs(3))
        .await
        .expect("first slot");
    let gate_for_cancelled = Arc::clone(&gate);
    let cancelled = tokio::spawn(async move {
        gate_for_cancelled
            .acquire(t0 + Duration::from_secs(5), Duration::from_secs(3))
            .await
    });
    tokio::time::sleep(Duration::from_millis(40)).await;
    cancelled.abort();
    let _ = cancelled.await;

    let third = gate
        .acquire(t0 + Duration::from_secs(5), Duration::from_secs(3))
        .await
        .expect("third slot after cancellation");
    assert_eq!(
        t0.elapsed(),
        READINESS_FALLBACK_MIN_SPACING,
        "cancelled waiter must not burn a pacing slot"
    );
    drop((first, third));
}

#[tokio::test(flavor = "current_thread")]
async fn reflected_pod_ip_recovers_across_relist_lag_and_queue_recovery_with_zero_gets()
-> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = db.pool.clone();

    // Subcase 1: Healthy relist (`Init` -> `InitApply` -> `InitDone`) delivers
    // the pod IP through the `""` broadcast with zero GETs and zero watches.
    {
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{sandbox_id}-g1");
        let pod_uid = "relist-uid";
        seed_created_execution(&pool, sandbox_id, &pod_name, pod_uid).await?;
        let pending = sandbox_pod(&pod_name, pod_uid, sandbox_id, 1, "Pending", None);
        let running = sandbox_pod(
            &pod_name,
            pod_uid,
            sandbox_id,
            1,
            "Running",
            Some("192.0.2.10"),
        );
        let (client, log) = pod_get_only_client(&pod_name, running.clone());
        let (substrate, mut feed) = seeded_substrate(
            pool.clone(),
            client,
            50051,
            vec![pending.clone()],
            vec![worker_node()],
        )
        .with_pod_cache_feed(vec![pending]);
        let substrate = Arc::new(substrate);
        let waiter = spawn_pod_ip_waiter(
            &substrate,
            sandbox_id,
            &pod_name,
            pod_uid,
            Duration::from_secs(2),
        );
        tokio::task::yield_now().await;
        feed.begin_relist();
        feed.relist_apply(running);
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            requests_matching(&log, &Method::GET).is_empty(),
            "in-progress healthy relist must not trigger fallback GETs"
        );
        feed.finish_relist();

        let resolved = waiter.await??;
        assert_eq!(
            resolved,
            Some((PodIp::parse("192.0.2.10")?, PodIpSource::Published))
        );
        assert!(
            requests_matching(&log, &Method::GET).is_empty(),
            "InitDone wakeup must resolve from pod_store with zero GETs"
        );
    }

    // Subcase 2: Broadcast receiver lags past `EVENT_CHANNEL_CAPACITY` (1024),
    // and the execution row was left in `creating` with `pod_uid = NULL`
    // because `create_execution`'s initial post-create metadata write
    // transiently failed; `Lagged` repairs `pod_uid` / `pod_ip` from
    // `pod_store` with zero GETs.
    {
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{sandbox_id}-g1");
        let pod_uid = "lagged-uid";
        seed_unpersisted_creating_execution(&pool, sandbox_id, &pod_name).await?;
        let pending = sandbox_pod(&pod_name, pod_uid, sandbox_id, 1, "Pending", None);
        let running = sandbox_pod(
            &pod_name,
            pod_uid,
            sandbox_id,
            1,
            "Running",
            Some("192.0.2.11"),
        );
        let (client, log) = pod_get_only_client(&pod_name, running.clone());
        let (substrate, mut feed) =
            seeded_substrate(pool.clone(), client, 50051, Vec::new(), vec![worker_node()])
                .with_pod_cache_feed(vec![pending]);
        let substrate = Arc::new(substrate);
        let waiter = spawn_pod_ip_waiter(
            &substrate,
            sandbox_id,
            &pod_name,
            pod_uid,
            Duration::from_secs(2),
        );
        tokio::task::yield_now().await;
        // Apply our pod's Running state first, then overflow the 1024-slot
        // broadcast ring with unrelated pods before the waiter polls again.
        feed.apply(running);
        let noise_sandbox = SandboxId::new();
        for idx in 0..1_100 {
            let noise_name = format!("space-sbx-noise-{idx}");
            feed.apply(sandbox_pod(
                &noise_name,
                "noise-uid",
                noise_sandbox,
                1,
                "Pending",
                None,
            ));
        }

        let resolved = waiter.await??;
        assert_eq!(
            resolved,
            Some((PodIp::parse("192.0.2.11")?, PodIpSource::Published))
        );
        assert!(
            requests_matching(&log, &Method::GET).is_empty(),
            "Lagged consumer must recover from pod_store with zero GETs"
        );
        let (state, stored_uid, stored_ip): (String, Option<String>, Option<String>) =
            sqlx::query_as(pplx_space_sql_locator::sql!(
                "SELECT state, pod_uid, host(pod_ip)
                   FROM sandbox_k8s_executions
                  WHERE sandbox_id = $1 AND generation = 1"
            ))
            .bind(sandbox_id)
            .fetch_one(&pool)
            .await?;
        assert_eq!(state, "running");
        assert_eq!(stored_uid.as_deref(), Some(pod_uid));
        assert_eq!(stored_ip.as_deref(), Some("192.0.2.11"));
    }

    // Subcase 3: A create queued in `ReadinessFallbackGate` during a reflector
    // watch outage cancels its queued fallback wait with zero GETs as soon as
    // the reflector recovers, even when its row is still in `creating` with
    // `pod_uid = NULL` and settles immediately in `pod_store`.
    {
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{sandbox_id}-g1");
        let pod_uid = "recover-queue-uid";
        seed_unpersisted_creating_execution(&pool, sandbox_id, &pod_name).await?;
        let running = sandbox_pod(
            &pod_name,
            pod_uid,
            sandbox_id,
            1,
            "Running",
            Some("192.0.2.12"),
        );
        let (client, log) = pod_get_only_client(&pod_name, running.clone());
        let (substrate, mut feed) =
            seeded_substrate(pool.clone(), client, 50051, Vec::new(), vec![worker_node()])
                .with_pod_cache_feed(Vec::new());
        feed.fail_watch();
        assert!(!substrate.pod_reflector_healthy());

        // Occupy all 4 fallback permits so the waiting create queues inside
        // `ReadinessFallbackGate::acquire`.
        let gate_deadline = Instant::now() + Duration::from_secs(5);
        let mut occupied = Vec::new();
        for _ in 0..READINESS_FALLBACK_MAX_IN_FLIGHT {
            occupied.push(
                substrate
                    .readiness_fallback_gate
                    .acquire(gate_deadline, Duration::from_secs(5))
                    .await
                    .expect("occupy gate slot"),
            );
        }

        let substrate = Arc::new(substrate);
        let waiter = spawn_pod_ip_waiter(
            &substrate,
            sandbox_id,
            &pod_name,
            pod_uid,
            Duration::from_secs(2),
        );
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Recover the reflector while all 4 fallback permits are STILL held:
        // the waiter must cancel its queued gate wait and resolve from cache.
        feed.begin_relist();
        feed.relist_apply(running);
        feed.finish_relist();

        let resolved = tokio::time::timeout(Duration::from_millis(500), waiter)
            .await
            .expect("queued waiter must exit before occupied fallback permits release")??;
        assert_eq!(
            resolved,
            Some((PodIp::parse("192.0.2.12")?, PodIpSource::Published))
        );
        assert!(
            requests_matching(&log, &Method::GET).is_empty(),
            "cancelled fallback queue waiter must issue zero apiserver GETs"
        );
        let (state, stored_uid, stored_ip): (String, Option<String>, Option<String>) =
            sqlx::query_as(pplx_space_sql_locator::sql!(
                "SELECT state, pod_uid, host(pod_ip)
                   FROM sandbox_k8s_executions
                  WHERE sandbox_id = $1 AND generation = 1"
            ))
            .bind(sandbox_id)
            .fetch_one(&pool)
            .await?;
        assert_eq!(state, "running");
        assert_eq!(stored_uid.as_deref(), Some(pod_uid));
        assert_eq!(stored_ip.as_deref(), Some("192.0.2.12"));
        drop(occupied);
    }

    // Subcase 4: During proxy re-resolution with an unsettled cached pod and a
    // failing reflector watch, `reresolve_pod_ip_during_proxy_wait` falls back
    // to one gated apiserver GET (repairing an unpersisted `creating` row and
    // pacing follow-up polls to `readiness_quiet_reflector`), and active wake
    // readiness waits on `recovering` executions (`target.pod_uid = None`)
    // observe the materialized pod's IP with zero GETs while keeping
    // `state = 'recovering'` gated from passive `resolve_pod_ip` callers.
    {
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{sandbox_id}-g1");
        let pod_uid = "degraded-reresolve-uid";
        seed_unpersisted_creating_execution(&pool, sandbox_id, &pod_name).await?;
        let pending = sandbox_pod(&pod_name, pod_uid, sandbox_id, 1, "Pending", None);
        let running = sandbox_pod(
            &pod_name,
            pod_uid,
            sandbox_id,
            1,
            "Running",
            Some("192.0.2.13"),
        );
        let (client, log) = pod_get_only_client(&pod_name, running);
        let (substrate, mut feed) =
            seeded_substrate(pool.clone(), client, 50051, Vec::new(), vec![worker_node()])
                .with_pod_cache_feed(vec![pending]);
        feed.fail_watch();
        assert!(!substrate.pod_reflector_healthy());

        let target = PodProxyWaitTarget {
            pod_name: &pod_name,
            pod_uid: Some(pod_uid),
            generation: 1,
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut next_degraded = None;
        let resolved = substrate
            .reresolve_pod_ip_during_proxy_wait(sandbox_id, &target, deadline, &mut next_degraded)
            .await?;
        assert_eq!(
            resolved,
            Some((PodIp::parse("192.0.2.13")?, PodIpSource::Published))
        );
        assert_eq!(requests_matching(&log, &Method::GET).len(), 1);

        // Immediate retry within the `readiness_quiet_reflector` window reuses
        // cached/recorded state without firing a second GET.
        let second = substrate
            .reresolve_pod_ip_during_proxy_wait(sandbox_id, &target, deadline, &mut next_degraded)
            .await?;
        assert_eq!(
            second,
            Some((PodIp::parse("192.0.2.13")?, PodIpSource::NodeReported))
        );
        assert_eq!(requests_matching(&log, &Method::GET).len(), 1);

        // Restore reflector health and verify recovery-owned wake readiness
        // (`target.pod_uid = None`, `state = 'recovering'`, `pod_uid` set by
        // `ensure_pod_exists`): stale predecessor views (including cached
        // `Unschedulable` and old node placements) are ignored, the recovery
        // pod's IP resolves from `pod_store` with zero GETs while keeping
        // `state = 'recovering'`, and passive `resolve_pod_ip` still returns
        // `None`.
        feed.begin_relist();
        feed.finish_relist();
        assert!(substrate.pod_reflector_healthy());

        let recovering_id = SandboxId::new();
        let recovering_pod = format!("space-sbx-{recovering_id}-g1");
        insert_test_sandbox_row(&pool, recovering_id, "started").await?;
        sqlx::query(pplx_space_sql_locator::sql!(
            "INSERT INTO sandbox_k8s_executions
                (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip, state, active)
             VALUES ($1, 1, $2, 'space', 'recovering-uid', NULL, 'recovering', TRUE)"
        ))
        .bind(recovering_id)
        .bind(&recovering_pod)
        .execute(&pool)
        .await?;

        let wake_target = PodProxyWaitTarget {
            pod_name: &recovering_pod,
            pod_uid: None,
            generation: 1,
        };
        let mut stale_unschedulable = sandbox_pod(
            &recovering_pod,
            "predecessor-uid",
            recovering_id,
            1,
            "Pending",
            None,
        );
        stale_unschedulable
            .status
            .as_mut()
            .expect("pod has status")
            .conditions = Some(vec![k8s_openapi::api::core::v1::PodCondition {
            type_: "PodScheduled".into(),
            status: "False".into(),
            reason: Some("Unschedulable".into()),
            message: Some("0/2 nodes are available".into()),
            ..Default::default()
        }]);
        feed.apply(stale_unschedulable.clone());
        assert_eq!(
            substrate
                .resolve_settled_cached_pod_for_readiness(
                    recovering_id,
                    &wake_target,
                    &SandboxPodView::from(stale_unschedulable),
                    deadline,
                )
                .await?,
            None
        );

        let substrate_arc = Arc::new(substrate.clone());
        let wake_pod_name = recovering_pod.clone();
        let wake_handle = tokio::spawn({
            let substrate = Arc::clone(&substrate_arc);
            async move {
                let (observed, _) = tokio::sync::watch::channel(None);
                let target = PodProxyWaitTarget {
                    pod_name: &wake_pod_name,
                    pod_uid: None,
                    generation: 1,
                };
                substrate
                    .reflected_pod_ip(recovering_id, &target, &observed, deadline)
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !wake_handle.is_finished(),
            "stale Unschedulable predecessor must not fail recovering wake"
        );

        let mut stale_bound_predecessor = sandbox_pod(
            &recovering_pod,
            "predecessor-uid",
            recovering_id,
            1,
            "Running",
            Some("192.0.2.99"),
        );
        stale_bound_predecessor
            .spec
            .get_or_insert_with(Default::default)
            .node_name = Some("stale-node".to_string());
        feed.apply(stale_bound_predecessor);
        tokio::time::sleep(Duration::from_millis(20)).await;
        let stale_node: Option<String> = sqlx::query_scalar(pplx_space_sql_locator::sql!(
            "SELECT k8s_node_name
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND generation = 1"
        ))
        .bind(recovering_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            stale_node, None,
            "stale predecessor must not write k8s_node_name onto recovering execution"
        );

        let mut recovering_running = sandbox_pod(
            &recovering_pod,
            "recovering-uid",
            recovering_id,
            1,
            "Running",
            Some("192.0.2.14"),
        );
        recovering_running
            .spec
            .get_or_insert_with(Default::default)
            .node_name = Some(WORKER_NODE.to_string());
        feed.apply(recovering_running);
        let reflected = tokio::time::timeout(Duration::from_millis(500), wake_handle)
            .await
            .expect("recovering wake must resolve once matching pod_uid arrives")??;
        assert_eq!(
            reflected,
            Some((
                (PodIp::parse("192.0.2.14")?, PodIpSource::Published),
                super::PodIpLearned::Reflected,
            ))
        );
        let mut recover_next_degraded = None;
        assert_eq!(
            substrate
                .reresolve_pod_ip_during_proxy_wait(
                    recovering_id,
                    &wake_target,
                    deadline,
                    &mut recover_next_degraded,
                )
                .await?,
            Some((PodIp::parse("192.0.2.14")?, PodIpSource::Published))
        );
        assert_eq!(requests_matching(&log, &Method::GET).len(), 1);

        let (state, stored_uid, stored_ip, stored_node): (
            String,
            Option<String>,
            Option<String>,
            Option<String>,
        ) = sqlx::query_as(pplx_space_sql_locator::sql!(
            "SELECT state, pod_uid, host(pod_ip), k8s_node_name
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND generation = 1"
        ))
        .bind(recovering_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(state, "recovering");
        assert_eq!(stored_uid.as_deref(), Some("recovering-uid"));
        assert_eq!(stored_ip.as_deref(), Some("192.0.2.14"));
        assert_eq!(stored_node.as_deref(), Some(WORKER_NODE));
        assert_eq!(substrate.resolve_pod_ip(recovering_id).await?, None);
    }

    // Subcase 5: Disappearance (One-shot A) and quiet-reflector (One-shot B)
    // waiters queued behind saturated `ReadinessFallbackGate` permits cancel
    // their queued fallback wait with zero GETs (instead of saturating with
    // `503`) as soon as `target` settles into a non-terminal status in
    // `pod_store`.
    {
        let absence_id = SandboxId::new();
        let absence_pod = format!("space-sbx-{absence_id}-g1");
        let absence_uid = "oneshot-absence-uid";
        seed_created_execution(&pool, absence_id, &absence_pod, absence_uid).await?;

        let quiet_id = SandboxId::new();
        let quiet_pod = format!("space-sbx-{quiet_id}-g1");
        let quiet_uid = "oneshot-quiet-uid";
        seed_created_execution(&pool, quiet_id, &quiet_pod, quiet_uid).await?;

        let absence_pending =
            sandbox_pod(&absence_pod, absence_uid, absence_id, 1, "Pending", None);
        let absence_running = sandbox_pod(
            &absence_pod,
            absence_uid,
            absence_id,
            1,
            "Running",
            Some("192.0.2.15"),
        );
        let quiet_running = sandbox_pod(
            &quiet_pod,
            quiet_uid,
            quiet_id,
            1,
            "Running",
            Some("192.0.2.16"),
        );

        let (client, log) = pod_get_only_client(&absence_pod, absence_running.clone());
        let (mut substrate, mut feed) =
            seeded_substrate(pool.clone(), client, 50051, Vec::new(), vec![worker_node()])
                .with_pod_cache_feed(vec![absence_pending.clone()]);
        substrate.budgets.readiness_quiet_reflector = Duration::from_millis(25);
        substrate.budgets.readiness_fallback_wait = Duration::from_millis(250);

        let gate_deadline = Instant::now() + Duration::from_secs(5);
        let mut occupied = Vec::new();
        for _ in 0..READINESS_FALLBACK_MAX_IN_FLIGHT {
            occupied.push(
                substrate
                    .readiness_fallback_gate
                    .acquire(gate_deadline, Duration::from_secs(5))
                    .await
                    .expect("occupy gate slot"),
            );
        }

        let substrate = Arc::new(substrate);
        let absence_waiter = spawn_pod_ip_waiter(
            &substrate,
            absence_id,
            &absence_pod,
            absence_uid,
            Duration::from_secs(2),
        );
        tokio::task::yield_now().await;
        // Vanish `absence_pod` so One-shot A queues on `ReadinessFallbackGate`.
        feed.delete(absence_pending);
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Spawn `quiet_waiter` after the delete epoch so `delivery_epoch` stays
        // unchanged across `readiness_quiet_reflector` (`25ms`) and One-shot B
        // also queues on `ReadinessFallbackGate`.
        let quiet_waiter = spawn_pod_ip_waiter(
            &substrate,
            quiet_id,
            &quiet_pod,
            quiet_uid,
            Duration::from_secs(2),
        );
        tokio::time::sleep(Duration::from_millis(45)).await;

        // Deliver settled pods into `pod_store` while all 4 fallback permits
        // remain occupied: both waiters must cancel their gate wait with zero
        // apiserver GETs instead of hitting `WaitBudgetSaturated` (`503`).
        feed.apply(absence_running);
        feed.apply(quiet_running);

        assert_eq!(
            tokio::time::timeout(Duration::from_millis(500), absence_waiter)
                .await
                .expect("One-shot A waiter must cancel queued gate wait")??,
            Some((PodIp::parse("192.0.2.15")?, PodIpSource::Published))
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(500), quiet_waiter)
                .await
                .expect("One-shot B waiter must cancel queued gate wait")??,
            Some((PodIp::parse("192.0.2.16")?, PodIpSource::Published))
        );
        assert!(
            requests_matching(&log, &Method::GET).is_empty(),
            "settled pods cancelling queued One-shot A/B waits must issue zero GETs"
        );
        drop(occupied);

        // Once a fallback GET is already in flight and hangs on the apiserver,
        // reflector settlement must preempt the hung GET immediately rather than
        // expiring `readiness_pod_get` into `SubstrateError::Unavailable` (`503`).
        let inflight_id = SandboxId::new();
        let inflight_pod = format!("space-sbx-{inflight_id}-g1");
        let inflight_uid = "oneshot-inflight-uid";
        seed_created_execution(&pool, inflight_id, &inflight_pod, inflight_uid).await?;
        let inflight_running = sandbox_pod(
            &inflight_pod,
            inflight_uid,
            inflight_id,
            1,
            "Running",
            Some("192.0.2.17"),
        );
        let expected_inflight_path = pod_path(&inflight_pod);
        let (hang_client, hang_log) = scripted_kube_client(move |req| {
            if req.method == Method::GET && req.path == expected_inflight_path {
                KubeReply::Hang
            } else {
                kube_status(StatusCode::INTERNAL_SERVER_ERROR, "unexpected request")
            }
        });
        let (mut hang_substrate, mut hang_feed) = seeded_substrate(
            pool.clone(),
            hang_client,
            50051,
            Vec::new(),
            vec![worker_node()],
        )
        .with_pod_cache_feed(Vec::new());
        hang_substrate.budgets.readiness_quiet_reflector = Duration::from_millis(20);
        hang_substrate.budgets.readiness_pod_get = Duration::from_millis(120);
        let hang_substrate = Arc::new(hang_substrate);
        let inflight_waiter = spawn_pod_ip_waiter(
            &hang_substrate,
            inflight_id,
            &inflight_pod,
            inflight_uid,
            Duration::from_secs(2),
        );
        let get_seen_deadline = Instant::now() + Duration::from_millis(500);
        while requests_matching(&hang_log, &Method::GET).is_empty() {
            assert!(
                Instant::now() < get_seen_deadline,
                "quiet-reflector One-shot B must start its fallback GET"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        hang_feed.apply(inflight_running);
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(500), inflight_waiter)
                .await
                .expect("reflector settlement must preempt hung fallback GET")??,
            Some((PodIp::parse("192.0.2.17")?, PodIpSource::Published))
        );
        assert_eq!(requests_matching(&hang_log, &Method::GET).len(), 1);
    }

    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum ApiserverMode {
    SlowPendingGet,
    Throttled429,
    Unavailable503,
    GetTimeout,
}

#[tokio::test(flavor = "current_thread")]
async fn degraded_live_pod_reflector_bounds_stalled_creates_and_fails_fast_on_overload()
-> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = db.pool.clone();
    let mode = Arc::new(Mutex::new(ApiserverMode::SlowPendingGet));
    let in_flight_gets = Arc::new(AtomicUsize::new(0));
    let peak_in_flight_gets = Arc::new(AtomicUsize::new(0));
    let get_starts = Arc::new(Mutex::new(Vec::new()));

    let mode_for_svc = Arc::clone(&mode);
    let in_flight_for_svc = Arc::clone(&in_flight_gets);
    let peak_for_svc = Arc::clone(&peak_in_flight_gets);
    let starts_for_svc = Arc::clone(&get_starts);
    let (client, log) = scripted_kube_client_async(move |req| {
        let mode = *mode_for_svc.lock().unwrap();
        let in_flight = Arc::clone(&in_flight_for_svc);
        let peak = Arc::clone(&peak_for_svc);
        let starts = Arc::clone(&starts_for_svc);
        async move {
            if req.method == Method::GET && req.path == "/api/v1/namespaces/space/pods" {
                let is_watch = req
                    .query
                    .as_deref()
                    .is_some_and(|query| query.contains("watch=true"));
                if is_watch {
                    return kube_status(StatusCode::INTERNAL_SERVER_ERROR, "watch stream broken");
                }
                return KubeReply::Json(
                    StatusCode::OK,
                    serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "PodList",
                        "metadata": { "resourceVersion": "1" },
                        "items": [],
                    }),
                );
            }
            if req.method == Method::GET && req.path.starts_with("/api/v1/namespaces/space/pods/") {
                let pod_name = req.path.rsplit('/').next().unwrap_or("unknown").to_string();
                match mode {
                    ApiserverMode::SlowPendingGet => {
                        starts.lock().unwrap().push(Instant::now());
                        let active = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(active, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(320)).await;
                        in_flight.fetch_sub(1, Ordering::SeqCst);
                        let pod = sandbox_pod(
                            &pod_name,
                            "stalled-uid",
                            SandboxId::new(),
                            1,
                            "Pending",
                            None,
                        );
                        pod_reply(&pod)
                    }
                    ApiserverMode::Throttled429 => {
                        kube_status(StatusCode::TOO_MANY_REQUESTS, "TooManyRequests")
                    }
                    ApiserverMode::Unavailable503 => {
                        kube_status(StatusCode::SERVICE_UNAVAILABLE, "ServiceUnavailable")
                    }
                    ApiserverMode::GetTimeout => KubeReply::Hang,
                }
            } else {
                kube_status(StatusCode::INTERNAL_SERVER_ERROR, "unexpected request")
            }
        }
    });

    let mut substrate =
        seeded_substrate(pool.clone(), client, 50051, Vec::new(), vec![worker_node()])
            .with_live_pod_reflector();
    substrate.budgets.readiness_pod_get = Duration::from_millis(500);
    substrate
        .pod_reflector_ready
        .wait_until_ready()
        .await
        .expect("initial LIST succeeds");
    // Wait briefly for the real watcher to hit the 500 on `watch=true` and
    // flip `is_watch_failing()`.
    let watch_fail_deadline = Instant::now() + Duration::from_secs(1);
    while !substrate.pod_client_index.is_watch_failing() {
        assert!(
            Instant::now() < watch_fail_deadline,
            "live pod reflector must record watch failure"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let substrate = Arc::new(substrate);
    let mut stalled = Vec::new();
    for idx in 0..10 {
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-stalled-{idx}-g1");
        seed_created_execution(&pool, sandbox_id, &pod_name, "stalled-uid").await?;
        stalled.push(spawn_pod_ip_waiter(
            &substrate,
            sandbox_id,
            &pod_name,
            "stalled-uid",
            Duration::from_millis(550),
        ));
    }

    for join in stalled {
        assert_eq!(join.await??, None);
    }

    let peak = peak_in_flight_gets.load(Ordering::SeqCst);
    assert!(
        (2..=READINESS_FALLBACK_MAX_IN_FLIGHT).contains(&peak),
        "concurrent stalled creates must contend on the gate without exceeding 4 in-flight GETs: peak={peak}"
    );
    let starts = get_starts.lock().unwrap().clone();
    assert!(
        starts.len() >= 4,
        "fallback gate must allow paced GETs under reflector failure: count={}",
        starts.len()
    );
    let per_pod_watches = requests_matching(&log, &Method::GET)
        .into_iter()
        .filter(|req| {
            req.query
                .as_deref()
                .is_some_and(|q| q.contains("metadata.name="))
        })
        .count();
    assert_eq!(per_pod_watches, 0, "no per-pod watch may be opened");

    // Overload fail-fast checks: 429, 503, and individual GET timeout before
    // the readiness deadline must all return `SubstrateError::Unavailable`
    // (HTTP 503) without tripping `is_nodemgr_unreachable`.
    let mut overload_substrate = (*substrate).clone();
    overload_substrate.budgets.readiness_pod_get = Duration::from_millis(40);
    for overload_mode in [
        ApiserverMode::Throttled429,
        ApiserverMode::Unavailable503,
        ApiserverMode::GetTimeout,
    ] {
        *mode.lock().unwrap() = overload_mode;
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-overload-{sandbox_id}-g1");
        seed_created_execution(&pool, sandbox_id, &pod_name, "overload-uid").await?;
        let err = overload_substrate
            .wait_for_pod_ip_assignment(
                SandboxApiMetricIdentity::native("create"),
                sandbox_id,
                &PodProxyWaitTarget {
                    pod_name: &pod_name,
                    pod_uid: Some("overload-uid"),
                    generation: 1,
                },
                Instant::now() + Duration::from_secs(2),
                "create",
            )
            .await
            .expect_err("apiserver overload must fail fast");
        assert!(
            matches!(err, SubstrateError::Unavailable { .. }),
            "expected Unavailable for {overload_mode:?}, got {err:?}"
        );
        assert!(!is_nodemgr_unreachable(&err), "{err:?}");
        assert_eq!(
            ControllerError::from(err).status,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    Ok(())
}
