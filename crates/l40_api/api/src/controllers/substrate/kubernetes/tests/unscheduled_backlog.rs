//! Cold-create admission against the unscheduled-pod reflector index: trips at
//! `max_unscheduled_pods`, excludes pods `kube-scheduler` already evaluated as
//! `Unschedulable`, holds across bounded apiserver brownouts when the last
//! snapshot was tripped, and fails open on healthy watch reconnects, expired
//! stale windows, cold caches, or unsupported substrates.

use std::collections::BTreeSet;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::runtime::reflector;
use sqlx::postgres::PgPoolOptions;

use super::*;
use crate::cluster_admission::{
    ClusterActivationClass, ClusterSandboxAdmission, ClusterSandboxAdmissionConfig,
};
use crate::controllers::ControllerError;
use crate::controllers::substrate::kubernetes::test_substrate::seeded_substrate;
use crate::controllers::substrate::{
    ActivationAdmission, ActivationAdmissionClass, ActivationRejectionKind, Substrate,
    UnscheduledPodBacklog, UnscheduledPodBacklogUnknown,
};

fn lazy_pool() -> sqlx::PgPool {
    PgPoolOptions::new()
        .connect_lazy("postgres://unused@localhost/unused")
        .expect("lazy pool builds without connecting")
}

fn substrate_with_caches(
    nodes: &[Arc<Node>],
    pods: &[Pod],
    nodes_ready: bool,
    pods_ready: bool,
) -> KubernetesSubstrate {
    let mut substrate = seeded_substrate(
        lazy_pool(),
        inert_kubernetes_client(),
        DEFAULT_SANDBOX_PROXY_PORT,
        pods.to_vec(),
        nodes.iter().map(|node| (**node).clone()).collect(),
    );
    if !nodes_ready {
        let (node_api_pods, _) = reflector::store::<Pod>();
        let (nodes, _) = reflector::store::<Node>();
        substrate.k8s.caches = Arc::new(K8sCaches::for_test(node_api_pods, nodes, false));
    }
    if !pods_ready {
        substrate.pod_reflector_ready = ReflectorReady::new();
    }
    substrate
}

fn worker(name: &str, slots: Option<u64>) -> Arc<Node> {
    let labels = SandboxPodConfigValues::default()
        .base_node_selector()
        .into_labels();
    node_with_network_slots(name, labels, slots, None, true, false)
}

fn unscheduled_pod(name: &str) -> Pod {
    serde_json::from_value(serde_json::json!({
        "metadata": { "name": name, "namespace": "space" },
        "spec": { "containers": [] },
        "status": { "phase": "Pending" },
    }))
    .expect("valid pod")
}

fn deleting_unscheduled_pod(name: &str) -> Pod {
    serde_json::from_value(serde_json::json!({
        "metadata": {
            "name": name,
            "namespace": "space",
            "deletionTimestamp": "2026-11-25T00:00:00Z",
        },
        "spec": { "containers": [] },
        "status": { "phase": "Pending" },
    }))
    .expect("valid pod")
}

fn unschedulable_pod(name: &str) -> Pod {
    serde_json::from_value(serde_json::json!({
        "metadata": {
            "name": name,
            "namespace": "space",
            "labels": {
                "space.pplx.net/pool-role": "warm",
            },
        },
        "spec": { "containers": [] },
        "status": {
            "phase": "Pending",
            "conditions": [{
                "type": "PodScheduled",
                "status": "False",
                "reason": "Unschedulable",
                "message": "0/3 nodes are available: node affinity mismatch.",
            }],
        },
    }))
    .expect("valid pod")
}

fn admission(
    substrate: KubernetesSubstrate,
    max_unscheduled_pods: Option<u64>,
    max_stale: Option<Duration>,
) -> ClusterSandboxAdmission {
    ClusterSandboxAdmission::with_config(
        Substrate::from_kubernetes(substrate),
        ClusterSandboxAdmissionConfig {
            max_unscheduled_pods: max_unscheduled_pods.and_then(NonZeroU64::new),
            unscheduled_pod_max_stale: max_stale,
            ..crate::test_support::enabled_config()
        },
        pplx_space_feature_flags::FeatureFlagClient::disabled(),
    )
}

#[tokio::test(flavor = "current_thread")]
async fn backlog_admits_below_ceiling_and_refuses_with_503_at_ceiling() {
    let nodes = [worker("node-a", Some(8))];

    admission(
        substrate_with_caches(&nodes, &[unscheduled_pod("pending-1")], true, true),
        Some(2),
        Some(Duration::from_secs(30)),
    )
    .admit_cold_create()
    .await
    .expect("count 1 below ceiling 2 admits");

    // At the ceiling, cold creates shed with 503 + jittered 1..=5s Retry-After.
    let hints: BTreeSet<Duration> = {
        let mut seen = BTreeSet::new();
        for _ in 0..80 {
            let error = admission(
                substrate_with_caches(
                    &nodes,
                    &[unscheduled_pod("pending-1"), unscheduled_pod("pending-2")],
                    true,
                    true,
                ),
                Some(2),
                Some(Duration::from_secs(30)),
            )
            .admit_cold_create()
            .await
            .expect_err("count 2 at ceiling 2 must refuse");
            assert_eq!(error.kind(), ActivationRejectionKind::UnscheduledPodBacklog);
            let controller_error = ControllerError::from(error);
            assert_eq!(controller_error.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(controller_error.retry_after_cause(), None);
            assert_eq!(controller_error.retry_after(), None);
            let hint = controller_error
                .retry_after_header()
                .expect("Retry-After header");
            seen.insert(hint);
        }
        seen
    };
    let window: BTreeSet<Duration> = (1..=5).map(Duration::from_secs).collect();
    assert!(hints.is_subset(&window), "{hints:?}");
    assert!(hints.len() > 1, "expected jittered hints, got {hints:?}");
}

#[tokio::test(flavor = "current_thread")]
async fn bound_deleting_terminal_and_unschedulable_pods_do_not_trip_backlog_ceiling() {
    let nodes = [worker("node-a", Some(8))];
    let non_qualifying = [
        (*sandbox_pod_on_node("bound-pending", "node-a", 1, "Pending")).clone(),
        (*sandbox_pod_on_node("bound-running", "node-a", 1, "Running")).clone(),
        deleting_unscheduled_pod("deleting-pending"),
        unschedulable_pod("warm-pool-waiting-on-scale-up"),
        serde_json::from_value(serde_json::json!({
            "metadata": { "name": "failed-unscheduled", "namespace": "space" },
            "spec": { "containers": [] },
            "status": { "phase": "Failed" },
        }))
        .expect("valid pod"),
    ];
    let substrate = substrate_with_caches(&nodes, &non_qualifying, true, true);
    assert_eq!(
        substrate.unscheduled_pod_backlog(),
        UnscheduledPodBacklog::Fresh(0),
    );
    admission(substrate, Some(1), Some(Duration::from_secs(30)))
        .admit_cold_create()
        .await
        .expect(
            "bound, deleting, terminal, and already-evaluated Unschedulable pods do not count as scheduler backlog",
        );
}

#[tokio::test(flavor = "current_thread")]
async fn stale_reflector_holds_active_trip_within_window_and_fails_open_past_it_or_when_healthy() {
    let nodes = [worker("node-a", Some(8))];
    let now = Instant::now();

    // 1. Active backlog + brief watch failure (10s <= 30s): stays closed across
    //    the bounded stale-hold window.
    let tripped = substrate_with_caches(&nodes, &[unscheduled_pod("pending-1")], true, true);
    tripped.mark_pod_watch_error_at_for_test(now - Duration::from_secs(10));
    let error = admission(tripped, Some(1), Some(Duration::from_secs(30)))
        .admit_cold_create()
        .await
        .expect_err("stale watch within max_stale must hold active trip");
    assert_eq!(error.kind(), ActivationRejectionKind::UnscheduledPodBacklog);

    // 2. Active backlog + expired stale window (35s > 30s): fails open so a
    //    permanently broken watch after recovery can't wedge cold creates.
    let expired = substrate_with_caches(&nodes, &[unscheduled_pod("pending-1")], true, true);
    expired.mark_pod_watch_error_at_for_test(now - Duration::from_secs(35));
    admission(expired, Some(1), Some(Duration::from_secs(30)))
        .admit_cold_create()
        .await
        .expect("stale watch past max_stale must fail open");

    // 3. `unscheduled_pod_max_stale = None` (`0` in env): fails open immediately on stale watch.
    let zero_hold = substrate_with_caches(&nodes, &[unscheduled_pod("pending-1")], true, true);
    zero_hold.mark_pod_watch_error_for_test();
    admission(zero_hold, Some(1), None)
        .admit_cold_create()
        .await
        .expect("max_stale None disables stale-trip hold");

    // 4. Healthy snapshot (`0 < 1`) + routine watch reconnect: fails open.
    let healthy_reconnect = substrate_with_caches(&nodes, &[], true, true);
    healthy_reconnect.mark_pod_watch_error_at_for_test(now - Duration::from_secs(2));
    admission(healthy_reconnect, Some(1), Some(Duration::from_secs(30)))
        .admit_cold_create()
        .await
        .expect("healthy cluster watch reconnect must not reject cold creates");
}

#[tokio::test(flavor = "current_thread")]
async fn cold_cache_direct_substrate_and_non_cold_activations_admit() {
    let nodes = [worker("node-a", Some(8))];

    // Cold pod reflector -> `PodCacheCold` -> fail open.
    let cold = substrate_with_caches(&nodes, &[unscheduled_pod("pending-1")], true, false);
    assert_eq!(
        cold.unscheduled_pod_backlog(),
        UnscheduledPodBacklog::Unknown(UnscheduledPodBacklogUnknown::PodCacheCold),
    );
    admission(cold, Some(1), Some(Duration::from_secs(30)))
        .admit_cold_create()
        .await
        .expect("cold pod cache fails open");

    // Direct substrate -> `Unsupported` -> fail open.
    let direct = crate::test_support::direct_substrate(&lazy_pool());
    assert_eq!(
        direct.unscheduled_pod_backlog(),
        UnscheduledPodBacklog::Unknown(UnscheduledPodBacklogUnknown::Unsupported),
    );
    ClusterSandboxAdmission::with_config(
        direct,
        ClusterSandboxAdmissionConfig {
            enabled: false,
            max_unscheduled_pods: NonZeroU64::new(1),
            ..crate::test_support::enabled_config()
        },
        pplx_space_feature_flags::FeatureFlagClient::disabled(),
    )
    .admit_cold_create()
    .await
    .expect("Direct substrate fails open");

    // Non-cold-create activations (restores, warm-pool claims, substrate charges) bypass the gate.
    for class in [
        ClusterActivationClass::Cold,
        ClusterActivationClass::Restore,
    ] {
        admission(
            substrate_with_caches(&nodes, &[unscheduled_pod("pending-1")], true, true),
            Some(1),
            Some(Duration::from_secs(30)),
        )
        .admit_activation(class)
        .await
        .unwrap_or_else(|error| panic!("{class:?} bypassed gate should admit: {error:?}"));
    }
    for class in [
        ActivationAdmissionClass::Cold,
        ActivationAdmissionClass::Restore,
    ] {
        let adm = admission(
            substrate_with_caches(&nodes, &[unscheduled_pod("pending-1")], true, true),
            Some(1),
            Some(Duration::from_secs(30)),
        );
        ActivationAdmission::admit(&adm, class)
            .await
            .unwrap_or_else(|error| panic!("{class:?} substrate charge should admit: {error}"));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn gate_runs_with_limiter_off_and_orders_after_empty_cluster_and_no_free_slot() {
    // 1. Runs independently when the cluster token-bucket limiter is disabled.
    let error = ClusterSandboxAdmission::with_config(
        Substrate::from_kubernetes(substrate_with_caches(
            &[worker("node-a", Some(8))],
            &[unscheduled_pod("pending-1")],
            true,
            true,
        )),
        ClusterSandboxAdmissionConfig {
            enabled: false,
            max_unscheduled_pods: NonZeroU64::new(1),
            ..crate::test_support::enabled_config()
        },
        pplx_space_feature_flags::FeatureFlagClient::disabled(),
    )
    .admit_cold_create()
    .await
    .expect_err("backlog gate runs with cluster limiter disabled");
    assert_eq!(error.kind(), ActivationRejectionKind::UnscheduledPodBacklog);

    // 2. Empty cluster (`429 EmptyCluster`) wins when limiter is enabled.
    let empty_err = admission(
        substrate_with_caches(&[], &[unscheduled_pod("pending-1")], true, true),
        Some(1),
        Some(Duration::from_secs(30)),
    )
    .admit_cold_create()
    .await
    .expect_err("empty cluster refuses before backlog");
    assert_eq!(empty_err.kind(), ActivationRejectionKind::EmptyCluster);

    // 3. Full network slots (`503 NoFreeSlot`) wins when `slot_check_enabled` is also on.
    let full_err = ClusterSandboxAdmission::with_config(
        Substrate::from_kubernetes(substrate_with_caches(
            &[worker("node-a", Some(1))],
            &[
                (*sandbox_pod_on_node("bound-1", "node-a", 1, "Running")).clone(),
                unscheduled_pod("pending-1"),
            ],
            true,
            true,
        )),
        ClusterSandboxAdmissionConfig {
            slot_check_enabled: true,
            max_unscheduled_pods: NonZeroU64::new(1),
            ..crate::test_support::enabled_config()
        },
        pplx_space_feature_flags::FeatureFlagClient::disabled(),
    )
    .admit_cold_create()
    .await
    .expect_err("full slots refuses before unscheduled backlog");
    assert_eq!(full_err.kind(), ActivationRejectionKind::NoFreeSlot);
}
