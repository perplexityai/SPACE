//! The network-slot headroom read and the cluster limiter's slot check built
//! on it: a confident full cluster is a 503, anything less certain admits.

use std::sync::Arc;

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
    ActivationAdmission, ActivationAdmissionClass, ActivationRejectionKind, NetworkSlotHeadroom,
    NetworkSlotHeadroomUnknown, Substrate,
};

/// Every read under test is a cache read, so the pool is lazy and never dials.
fn lazy_pool() -> sqlx::PgPool {
    PgPoolOptions::new()
        .connect_lazy("postgres://unused@localhost/unused")
        .expect("lazy pool builds without connecting")
}

/// A substrate whose caches hold `nodes` and `pods`, warm or cold as asked.
fn substrate_with_caches(
    nodes: &[Arc<Node>],
    pods: &[Arc<Pod>],
    nodes_ready: bool,
    pods_ready: bool,
) -> KubernetesSubstrate {
    let mut substrate = seeded_substrate(
        lazy_pool(),
        inert_kubernetes_client(),
        DEFAULT_SANDBOX_PROXY_PORT,
        pods.iter().map(|pod| (**pod).clone()).collect(),
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

fn running(name: &str, node: &str) -> Arc<Pod> {
    sandbox_pod_on_node(name, node, 1, "Running")
}

#[tokio::test(flavor = "current_thread")]
async fn headroom_is_full_only_when_every_eligible_node_is_full() {
    let full = [worker("node-a", Some(1)), worker("node-b", Some(2))];
    let held = [
        running("sbx-1", "node-a"),
        running("sbx-2", "node-b"),
        running("sbx-3", "node-b"),
    ];
    assert_eq!(
        substrate_with_caches(&full, &held, true, true).network_slot_headroom(),
        NetworkSlotHeadroom::Full {
            slotless_nodes: false
        },
    );

    // One free slot anywhere in the eligible set is headroom.
    assert_eq!(
        substrate_with_caches(&full, &held[..2], true, true).network_slot_headroom(),
        NetworkSlotHeadroom::Available {
            slotless_nodes: false
        },
    );

    // A terminal pod holds no slot.
    let finished = [sandbox_pod_on_node("sbx-1", "node-a", 1, "Succeeded")];
    assert_eq!(
        substrate_with_caches(&full[..1], &finished, true, true).network_slot_headroom(),
        NetworkSlotHeadroom::Available {
            slotless_nodes: false
        },
    );
}

#[tokio::test(flavor = "current_thread")]
async fn headroom_ignores_nodes_outside_the_eligible_set() {
    // A cordoned node with free slots isn't where a new pod can go.
    let config = SandboxPodConfigValues::default();
    let cordoned = node_with_network_slots(
        "cordoned",
        config.base_node_selector().into_labels(),
        Some(8),
        None,
        true,
        true,
    );
    let nodes = [worker("node-a", Some(1)), cordoned];
    assert_eq!(
        substrate_with_caches(&nodes, &[running("sbx-1", "node-a")], true, true)
            .network_slot_headroom(),
        NetworkSlotHeadroom::Full {
            slotless_nodes: false
        },
    );
}

#[tokio::test(flavor = "current_thread")]
async fn headroom_is_unknown_whenever_it_cannot_be_read() {
    let full = [worker("node-a", Some(1))];
    let held = [running("sbx-1", "node-a")];
    assert_eq!(
        substrate_with_caches(&full, &held, false, true).network_slot_headroom(),
        NetworkSlotHeadroom::Unknown(NetworkSlotHeadroomUnknown::NodeCacheCold),
    );
    assert_eq!(
        substrate_with_caches(&full, &held, true, false).network_slot_headroom(),
        NetworkSlotHeadroom::Unknown(NetworkSlotHeadroomUnknown::PodCacheCold),
    );
}

/// Every sandbox pod requests a slot, so a node that advertises none, or
/// zero, can't take one: it counts as full, and is reported so a slot device
/// plugin outage stays visible.
#[tokio::test(flavor = "current_thread")]
async fn a_slotless_node_adds_no_headroom_but_is_reported() {
    let held = [running("sbx-1", "node-a"), running("sbx-2", "node-b")];
    let full_and_slotless = [
        worker("node-a", Some(1)),
        worker("node-b", Some(1)),
        worker("unadvertised", None),
        worker("zero", Some(0)),
    ];
    assert_eq!(
        substrate_with_caches(&full_and_slotless, &held, true, true).network_slot_headroom(),
        NetworkSlotHeadroom::Full {
            slotless_nodes: true
        },
    );
    assert_eq!(
        substrate_with_caches(&[worker("unadvertised", None)], &[], true, true)
            .network_slot_headroom(),
        NetworkSlotHeadroom::Full {
            slotless_nodes: true
        },
    );

    // A free node anywhere is still headroom, even listed after the slotless
    // ones; the slotless node is still reported.
    let with_a_free_node = [
        worker("unadvertised", None),
        worker("node-a", Some(1)),
        worker("node-c", Some(2)),
    ];
    assert_eq!(
        substrate_with_caches(&with_a_free_node, &held[..1], true, true).network_slot_headroom(),
        NetworkSlotHeadroom::Available {
            slotless_nodes: true
        },
    );
}

#[tokio::test(flavor = "current_thread")]
async fn direct_has_no_slot_headroom_to_read() {
    let direct = Substrate::from_direct(
        crate::controllers::substrate::direct::DirectSubstrate::new(lazy_pool()),
    );
    assert_eq!(
        direct.network_slot_headroom(),
        NetworkSlotHeadroom::Unknown(NetworkSlotHeadroomUnknown::Unsupported),
    );
}

/// Admission over `substrate` with no Redis, so anything the gates admit
/// falls through to the fail-open `misconfigured` path and returns `Ok`.
fn admission(substrate: KubernetesSubstrate, slot_check_enabled: bool) -> ClusterSandboxAdmission {
    ClusterSandboxAdmission::with_config(
        Substrate::from_kubernetes(substrate),
        ClusterSandboxAdmissionConfig {
            slot_check_enabled,
            ..crate::test_support::enabled_config()
        },
        pplx_space_feature_flags::FeatureFlagClient::disabled(),
    )
}

#[tokio::test(flavor = "current_thread")]
async fn a_full_cluster_refuses_a_new_pod_with_a_retryable_503() {
    let full = [worker("node-a", Some(1))];
    let held = [running("sbx-1", "node-a")];
    let error = admission(substrate_with_caches(&full, &held, true, true), true)
        .admit_cold_create()
        .await
        .expect_err("a full cluster must refuse a new pod");
    assert_eq!(
        error.kind(),
        ActivationRejectionKind::NoFreeSlot,
        "{error:?}"
    );
    let retry_after = error.retry_after();
    assert!(
        (Duration::from_secs(1)..=Duration::from_secs(5)).contains(&retry_after),
        "{retry_after:?}"
    );

    let controller = ControllerError::from(error);
    assert_eq!(controller.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(controller.retry_after_header(), Some(retry_after));
    // Not backpressure: the async dispatcher retries it against its bounded
    // budget, as it does an empty cluster.
    assert_eq!(controller.retry_after_cause(), None);
}

#[tokio::test(flavor = "current_thread")]
async fn slotless_nodes_beside_full_ones_refuse_and_a_free_node_admits() {
    let held = [running("sbx-1", "node-a")];
    let error = admission(
        substrate_with_caches(
            &[worker("node-a", Some(1)), worker("unadvertised", None)],
            &held,
            true,
            true,
        ),
        true,
    )
    .admit_cold_create()
    .await
    .expect_err("full and slotless nodes leave no slot");
    assert_eq!(
        error.kind(),
        ActivationRejectionKind::NoFreeSlot,
        "{error:?}"
    );
    assert_eq!(
        ControllerError::from(error).status(),
        StatusCode::SERVICE_UNAVAILABLE
    );

    admission(
        substrate_with_caches(
            &[
                worker("node-a", Some(1)),
                worker("unadvertised", None),
                worker("node-b", Some(1)),
            ],
            &held,
            true,
            true,
        ),
        true,
    )
    .admit_cold_create()
    .await
    .expect("a free node admits beside a slotless one");
}

/// Every other charge may reuse an existing pod or be bound to one node, so a
/// full cluster doesn't refuse it.
#[tokio::test(flavor = "current_thread")]
async fn only_a_new_pod_activation_is_slot_checked() {
    let full = [worker("node-a", Some(1))];
    let held = [running("sbx-1", "node-a")];
    for class in [
        ClusterActivationClass::Cold,
        ClusterActivationClass::Restore,
    ] {
        admission(substrate_with_caches(&full, &held, true, true), true)
            .admit_activation(class)
            .await
            .unwrap_or_else(|error| panic!("{class:?}: {error:?}"));
    }
    for class in [
        ActivationAdmissionClass::Cold,
        ActivationAdmissionClass::Restore,
    ] {
        let admission = admission(substrate_with_caches(&full, &held, true, true), true);
        ActivationAdmission::admit(&admission, class)
            .await
            .unwrap_or_else(|error| panic!("{class:?}: a substrate charge: {error}"));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn the_slot_check_admits_when_off_or_when_headroom_is_not_confidently_full() {
    let full = [worker("node-a", Some(1))];
    let held = [running("sbx-1", "node-a")];
    // Off by default, and off it never reads the slots.
    assert!(!ClusterSandboxAdmissionConfig::default().slot_check_enabled);
    admission(substrate_with_caches(&full, &held, true, true), false)
        .admit_cold_create()
        .await
        .expect("the slot check is off");

    for (substrate, case) in [
        (substrate_with_caches(&full, &[], true, true), "free slot"),
        (
            substrate_with_caches(&full, &held, true, false),
            "pod cache cold",
        ),
        (
            substrate_with_caches(&full, &held, false, true),
            "node cache cold",
        ),
    ] {
        admission(substrate, true)
            .admit_cold_create()
            .await
            .unwrap_or_else(|error| panic!("{case}: {error:?}"));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn an_empty_cluster_still_answers_429_with_the_slot_check_on() {
    let error = admission(substrate_with_caches(&[], &[], true, true), true)
        .admit_cold_create()
        .await
        .expect_err("no eligible node must refuse the activation");
    assert_eq!(
        error.kind(),
        ActivationRejectionKind::EmptyCluster,
        "{error:?}"
    );
    assert_eq!(
        ControllerError::from(error).status(),
        StatusCode::TOO_MANY_REQUESTS
    );
}

/// A pod the scheduler found no node for answers like a capacity refusal: a
/// retryable 503 whose header-only hint spreads retries over 1 to 5 seconds.
#[test]
fn an_unschedulable_pod_answers_503_with_a_jittered_hint() {
    let hints: std::collections::BTreeSet<Duration> = (0..200)
        .map(|_| {
            let error = ControllerError::from(SubstrateError::Unschedulable {
                message: "sandbox pod is unschedulable: 0/3 nodes are available".to_string(),
            });
            assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(error.retry_after_cause(), None);
            assert_eq!(error.retry_after(), None, "not an ECW convergence wait");
            error.retry_after_header().expect("a capacity hint")
        })
        .collect();
    let window: std::collections::BTreeSet<Duration> = (1..=5).map(Duration::from_secs).collect();
    assert!(hints.is_subset(&window), "{hints:?}");
    assert!(hints.len() > 1, "the hint is jittered: {hints:?}");
}

/// Each reading is its own metric value, so slotless nodes stay visible
/// whether or not they tip the cluster into full.
#[test]
fn every_headroom_reading_has_its_own_metric_value() {
    use pplx_space_metrics::sandbox::admission::ClusterSlotHeadroom;
    let cases = [
        (
            NetworkSlotHeadroom::Available {
                slotless_nodes: false,
            },
            ClusterSlotHeadroom::Available,
        ),
        (
            NetworkSlotHeadroom::Available {
                slotless_nodes: true,
            },
            ClusterSlotHeadroom::AvailableWithSlotlessNodes,
        ),
        (
            NetworkSlotHeadroom::Full {
                slotless_nodes: false,
            },
            ClusterSlotHeadroom::Full,
        ),
        (
            NetworkSlotHeadroom::Full {
                slotless_nodes: true,
            },
            ClusterSlotHeadroom::FullWithSlotlessNodes,
        ),
        (
            NetworkSlotHeadroom::Unknown(NetworkSlotHeadroomUnknown::Unsupported),
            ClusterSlotHeadroom::Unsupported,
        ),
        (
            NetworkSlotHeadroom::Unknown(NetworkSlotHeadroomUnknown::NodeCacheCold),
            ClusterSlotHeadroom::NodeCacheCold,
        ),
        (
            NetworkSlotHeadroom::Unknown(NetworkSlotHeadroomUnknown::PodCacheCold),
            ClusterSlotHeadroom::PodCacheCold,
        ),
    ];
    for (headroom, metric) in cases {
        assert_eq!(headroom.metric(), metric, "{headroom:?}");
    }
    let values: std::collections::BTreeSet<&str> =
        cases.iter().map(|(_, metric)| metric.as_str()).collect();
    assert_eq!(values.len(), cases.len(), "{values:?}");
}

/// The slot check runs on its own switch: with the cluster activation limiter
/// off, a full cluster still refuses a cold create, and a free slot admits.
/// The refusal is metered once, as `no_free_slot`, not also as a `disabled`
/// admission.
#[tokio::test(flavor = "current_thread")]
async fn the_slot_check_runs_with_the_cluster_limiter_off() {
    let limiter_off = |substrate| {
        ClusterSandboxAdmission::with_config(
            Substrate::from_kubernetes(substrate),
            ClusterSandboxAdmissionConfig {
                enabled: false,
                slot_check_enabled: true,
                ..crate::test_support::enabled_config()
            },
            pplx_space_feature_flags::FeatureFlagClient::disabled(),
        )
    };
    let full = [worker("node-a", Some(1))];
    let held = [running("sbx-1", "node-a")];

    let capture = pplx_space_metrics::test_capture::capture_counters_guard();
    let error = limiter_off(substrate_with_caches(&full, &held, true, true))
        .admit_cold_create()
        .await
        .expect_err("the slot check refuses with the limiter off");
    let decisions: Vec<Vec<String>> = capture
        .finish()
        .into_iter()
        .filter(|counter| counter.name == "cluster.sandbox_admission.rate.decision")
        .map(|counter| counter.tags)
        .collect();
    assert_eq!(
        error.kind(),
        ActivationRejectionKind::NoFreeSlot,
        "{error:?}"
    );
    assert_eq!(decisions.len(), 1, "{decisions:?}");
    assert!(
        decisions[0].iter().any(|tag| tag == "reason:no_free_slot"),
        "{decisions:?}"
    );

    limiter_off(substrate_with_caches(&full, &[], true, true))
        .admit_cold_create()
        .await
        .expect("a free slot admits with the limiter off");
}

/// With the limiter off, its empty-cluster guard doesn't run, so a cluster
/// with no eligible node is refused by the slot check instead: a `503`, not
/// the limiter's `429`.
#[tokio::test(flavor = "current_thread")]
async fn an_empty_cluster_answers_503_with_only_the_slot_check_on() {
    let error = ClusterSandboxAdmission::with_config(
        Substrate::from_kubernetes(substrate_with_caches(&[], &[], true, true)),
        ClusterSandboxAdmissionConfig {
            enabled: false,
            slot_check_enabled: true,
            ..crate::test_support::enabled_config()
        },
        pplx_space_feature_flags::FeatureFlagClient::disabled(),
    )
    .admit_cold_create()
    .await
    .expect_err("no eligible node has a free slot");
    assert_eq!(
        error.kind(),
        ActivationRejectionKind::NoFreeSlot,
        "{error:?}"
    );
    assert_eq!(
        ControllerError::from(error).status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}
