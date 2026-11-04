use std::str::FromStr;
use std::sync::Mutex;

use tokio_util::sync::CancellationToken;

use super::backend::BackendError;
use super::*;

#[derive(Default)]
struct FakeBackend {
    caches_ready: bool,
    nodes: Option<Vec<NodeLabelState>>,
    pods: Vec<ServicePod>,
    writes: Mutex<Vec<(String, NodeServicesReady)>>,
    fail_writes: bool,
}

impl FakeBackend {
    fn new(nodes: Vec<NodeLabelState>, pods: Vec<ServicePod>) -> Self {
        Self {
            caches_ready: true,
            nodes: Some(nodes),
            pods,
            writes: Mutex::new(Vec::new()),
            fail_writes: false,
        }
    }

    fn writes(&self) -> Vec<(String, NodeServicesReady)> {
        self.writes.lock().expect("writes lock").clone()
    }
}

impl ReadinessBackend for FakeBackend {
    fn caches_ready(&self) -> bool {
        self.caches_ready
    }

    fn placement_nodes(&self) -> Option<Vec<NodeLabelState>> {
        self.nodes.clone()
    }

    fn serving_pods(&self) -> Vec<ServicePod> {
        self.pods.clone()
    }

    async fn set_label(
        &self,
        node_name: &str,
        desired: NodeServicesReady,
    ) -> Result<(), BackendError> {
        if self.fail_writes {
            return Err(BackendError::Kube(
                kube::Error::LinesCodecMaxLineLengthExceeded,
            ));
        }
        self.writes
            .lock()
            .expect("writes lock")
            .push((node_name.to_string(), desired));
        Ok(())
    }
}

fn node(name: &str, current: Option<&str>) -> NodeLabelState {
    NodeLabelState {
        name: name.to_string(),
        current: current.map(str::to_string),
    }
}

fn pod(node_name: &str, component: NodeLocalComponent) -> ServicePod {
    ServicePod {
        node_name: node_name.to_string(),
        component,
    }
}

fn both_components() -> Vec<NodeLocalComponent> {
    vec![
        NodeLocalComponent::NodeCtl,
        NodeLocalComponent::VolumeManager,
    ]
}

fn live_config() -> ReconcilerConfig {
    ReconcilerConfig {
        enabled: true,
        ..ReconcilerConfig::default()
    }
}

/// Plan with the default (component-derived) fail-open threshold.
fn plan_for_test(nodes: &[NodeLabelState], pods: &[ServicePod]) -> LabelPlan {
    plan_label_changes(
        nodes,
        pods,
        &both_components(),
        ReconcilerConfig::default().max_not_ready_fraction,
    )
}

#[test]
fn node_with_every_component_serving_is_ready() {
    let nodes = vec![node("worker-a", None)];
    let pods = vec![
        pod("worker-a", NodeLocalComponent::NodeCtl),
        pod("worker-a", NodeLocalComponent::VolumeManager),
    ];

    let plan = plan_for_test(&nodes, &pods);
    let (changes, not_ready) = (plan.changes, plan.not_ready);

    assert_eq!(not_ready, 0);
    assert_eq!(
        changes,
        vec![NodeLabelChange {
            node_name: "worker-a".to_string(),
            desired: NodeServicesReady::Ready,
        }]
    );
}

/// The DaemonSet rolling-update window: the old pod is deleted before the
/// replacement is created, so the node has *no* pod for that component. That
/// absence is the outage this reconciler exists to publish.
#[test]
fn node_missing_a_component_pod_is_not_ready() {
    let nodes = vec![node("worker-a", Some("true"))];
    let pods = vec![pod("worker-a", NodeLocalComponent::NodeCtl)];

    let plan = plan_for_test(&nodes, &pods);
    let (changes, not_ready) = (plan.changes, plan.not_ready);

    assert_eq!(not_ready, 1);
    assert_eq!(
        changes,
        vec![NodeLabelChange {
            node_name: "worker-a".to_string(),
            desired: NodeServicesReady::NotReady,
        }]
    );
}

/// A pod serving on one node must never make a *different* node read as
/// serviced.
#[test]
fn readiness_does_not_leak_across_nodes() {
    let nodes = vec![node("worker-a", None), node("worker-b", None)];
    let pods = vec![
        pod("worker-a", NodeLocalComponent::NodeCtl),
        pod("worker-a", NodeLocalComponent::VolumeManager),
        pod("worker-b", NodeLocalComponent::NodeCtl),
    ];

    let plan = plan_for_test(&nodes, &pods);
    let (changes, not_ready) = (plan.changes, plan.not_ready);

    assert_eq!(not_ready, 1);
    assert_eq!(
        changes,
        vec![
            NodeLabelChange {
                node_name: "worker-a".to_string(),
                desired: NodeServicesReady::Ready,
            },
            NodeLabelChange {
                node_name: "worker-b".to_string(),
                desired: NodeServicesReady::NotReady,
            },
        ]
    );
}

#[test]
fn already_correct_labels_produce_no_writes() {
    let nodes = vec![
        node("worker-a", Some("true")),
        node("worker-b", Some("false")),
    ];
    let pods = vec![
        pod("worker-a", NodeLocalComponent::NodeCtl),
        pod("worker-a", NodeLocalComponent::VolumeManager),
    ];

    let plan = plan_for_test(&nodes, &pods);
    let (changes, not_ready) = (plan.changes, plan.not_ready);

    assert!(changes.is_empty(), "no label churn when already converged");
    assert_eq!(
        not_ready, 1,
        "the not-ready gauge counts the fleet, not just the writes"
    );
}

#[tokio::test]
async fn live_run_patches_changed_nodes_only() {
    let backend = FakeBackend::new(
        vec![
            node("worker-a", Some("true")),
            node("worker-b", Some("false")),
        ],
        vec![
            pod("worker-a", NodeLocalComponent::NodeCtl),
            pod("worker-b", NodeLocalComponent::NodeCtl),
            pod("worker-b", NodeLocalComponent::VolumeManager),
        ],
    );

    reconcile_once(&backend, &live_config(), &mut FairnessCursor::default()).await;

    assert_eq!(
        backend.writes(),
        vec![
            ("worker-a".to_string(), NodeServicesReady::NotReady),
            ("worker-b".to_string(), NodeServicesReady::Ready),
        ]
    );
}

/// A cold pod cache reads as "no pods anywhere", which would mark the entire
/// fleet not-ready in one tick. The tick must be skipped instead.
#[tokio::test]
async fn cold_pod_cache_skips_the_tick() {
    let backend = FakeBackend {
        caches_ready: false,
        ..FakeBackend::new(vec![node("worker-a", Some("true"))], Vec::new())
    };

    reconcile_once(&backend, &live_config(), &mut FairnessCursor::default()).await;

    assert!(backend.writes().is_empty());
}

#[tokio::test]
async fn cold_node_cache_skips_the_tick() {
    let backend = FakeBackend {
        nodes: None,
        ..FakeBackend::new(Vec::new(), Vec::new())
    };

    reconcile_once(&backend, &live_config(), &mut FairnessCursor::default()).await;

    assert!(backend.writes().is_empty());
}

#[tokio::test]
async fn per_tick_write_bound_defers_the_remainder() {
    let nodes: Vec<NodeLabelState> = (0..5)
        .map(|index| node(&format!("worker-{index}"), Some("true")))
        .collect();
    let backend = FakeBackend::new(nodes, Vec::new());
    let config = ReconcilerConfig {
        max_writes_per_tick: 2,
        ..live_config()
    };

    reconcile_once(&backend, &config, &mut FairnessCursor::default()).await;

    assert_eq!(backend.writes().len(), 2);
}

#[tokio::test]
async fn write_failures_do_not_abort_the_tick() {
    let backend = FakeBackend {
        fail_writes: true,
        ..FakeBackend::new(
            vec![
                node("worker-a", Some("true")),
                node("worker-b", Some("true")),
            ],
            Vec::new(),
        )
    };

    // Completing without panicking is the assertion: one node's failed patch
    // must not strand the rest of the fleet on stale labels.
    reconcile_once(&backend, &live_config(), &mut FairnessCursor::default()).await;

    assert!(backend.writes().is_empty());
}

/// An empty component list makes every node satisfy "all components serving",
/// which would publish `ready` fleet-wide — a signal that lies in the safest-
/// looking direction. The loop must refuse to run instead.
#[tokio::test]
async fn empty_component_list_refuses_to_publish() {
    let backend = FakeBackend::new(vec![node("worker-a", None)], Vec::new());
    let shutdown = CancellationToken::new();
    shutdown.cancel();

    run_with_backend(
        backend,
        ReconcilerConfig {
            components: Vec::new(),
            ..live_config()
        },
        shutdown,
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn slow_label_writes_are_bounded_and_deferred() {
    struct StallingBackend {
        nodes: Vec<NodeLabelState>,
        writes: Mutex<usize>,
    }

    impl ReadinessBackend for StallingBackend {
        fn caches_ready(&self) -> bool {
            true
        }

        fn placement_nodes(&self) -> Option<Vec<NodeLabelState>> {
            Some(self.nodes.clone())
        }

        fn serving_pods(&self) -> Vec<ServicePod> {
            Vec::new()
        }

        async fn set_label(
            &self,
            _node_name: &str,
            _desired: NodeServicesReady,
        ) -> Result<(), BackendError> {
            *self.writes.lock().expect("writes lock") += 1;
            // Longer than LABEL_WRITE_TIMEOUT: a wedged apiserver patch.
            tokio::time::sleep(Duration::from_secs(600)).await;
            Ok(())
        }
    }

    let backend = StallingBackend {
        nodes: (0..4)
            .map(|index| node(&format!("worker-{index}"), Some("true")))
            .collect(),
        writes: Mutex::new(0),
    };
    let config = ReconcilerConfig {
        tick: Duration::from_secs(5),
        ..live_config()
    };

    // Completes at all — rather than hanging for the kube client's own
    // multi-minute deadline — because every write is timeout-bounded.
    let started = tokio::time::Instant::now();
    reconcile_once(&backend, &config, &mut FairnessCursor::default()).await;
    let elapsed = started.elapsed();

    // All four wedged writes start together (concurrency 8), so the tick costs
    // one timeout rather than four serialized ones. That is the point of the
    // fan-out: a serial writer would have spent the whole budget on one node.
    assert_eq!(*backend.writes.lock().expect("writes lock"), 4);
    assert!(
        elapsed <= config.tick,
        "action phase took {elapsed:?}, which overruns the {:?} tick budget",
        config.tick
    );
}

/// The tick budget bounds the whole action phase, not just when a write is
/// *started*. A write that begins just inside the deadline must be clamped to
/// the time remaining — otherwise a fast patch followed by a wedged one runs
/// `elapsed = fast + LABEL_WRITE_TIMEOUT`, overrunning the tick and pushing
/// every node's next readiness update out behind it.
#[tokio::test(start_paused = true)]
async fn a_write_starting_near_the_deadline_cannot_overrun_the_tick() {
    struct FastThenWedgedBackend {
        nodes: Vec<NodeLabelState>,
        writes: Mutex<usize>,
    }

    impl ReadinessBackend for FastThenWedgedBackend {
        fn caches_ready(&self) -> bool {
            true
        }

        fn placement_nodes(&self) -> Option<Vec<NodeLabelState>> {
            Some(self.nodes.clone())
        }

        fn serving_pods(&self) -> Vec<ServicePod> {
            Vec::new()
        }

        async fn set_label(
            &self,
            _node_name: &str,
            _desired: NodeServicesReady,
        ) -> Result<(), BackendError> {
            let attempt = {
                let mut writes = self.writes.lock().expect("writes lock");
                *writes += 1;
                *writes
            };
            if attempt == 1 {
                // Slow but successful: eats most of the tick budget.
                tokio::time::sleep(Duration::from_secs(4)).await;
                Ok(())
            } else {
                // Wedged apiserver: never returns on its own.
                tokio::time::sleep(Duration::from_secs(600)).await;
                Ok(())
            }
        }
    }

    let tick = Duration::from_secs(5);
    let backend = FastThenWedgedBackend {
        nodes: (0..3)
            .map(|index| node(&format!("worker-{index}"), Some("true")))
            .collect(),
        writes: Mutex::new(0),
    };
    let config = ReconcilerConfig {
        tick,
        ..live_config()
    };

    let started = tokio::time::Instant::now();
    reconcile_once(&backend, &config, &mut FairnessCursor::default()).await;
    let elapsed = started.elapsed();

    assert!(
        elapsed <= tick,
        "action phase took {elapsed:?}, which overruns the {tick:?} tick budget"
    );
}

/// One node whose patch reliably eats the whole tick budget must not starve the
/// rest of the fleet. The change list is rebuilt in a stable order every tick,
/// so without a rotation cursor the wedged node is retried first forever and
/// every node behind it stays stale indefinitely.
#[tokio::test(start_paused = true)]
async fn a_wedged_node_does_not_starve_the_rest_of_the_fleet() {
    struct OneWedgedNodeBackend {
        nodes: Vec<NodeLabelState>,
        attempts: Mutex<Vec<String>>,
    }

    impl ReadinessBackend for OneWedgedNodeBackend {
        fn caches_ready(&self) -> bool {
            true
        }

        fn placement_nodes(&self) -> Option<Vec<NodeLabelState>> {
            Some(self.nodes.clone())
        }

        fn serving_pods(&self) -> Vec<ServicePod> {
            Vec::new()
        }

        async fn set_label(
            &self,
            node_name: &str,
            _desired: NodeServicesReady,
        ) -> Result<(), BackendError> {
            self.attempts
                .lock()
                .expect("attempts lock")
                .push(node_name.to_string());
            if node_name == "worker-0" {
                // Wedged: consumes the entire tick budget, every tick.
                tokio::time::sleep(Duration::from_secs(600)).await;
            }
            Ok(())
        }
    }

    let backend = OneWedgedNodeBackend {
        nodes: (0..3)
            .map(|index| node(&format!("worker-{index}"), Some("true")))
            .collect(),
        attempts: Mutex::new(Vec::new()),
    };
    let config = ReconcilerConfig {
        tick: Duration::from_secs(5),
        ..live_config()
    };
    let mut cursor = FairnessCursor::default();

    // Three ticks. `worker-0` eats each tick it leads, so a cursor-less loop
    // would attempt only `worker-0`, three times over.
    for _ in 0..3 {
        reconcile_once(&backend, &config, &mut cursor).await;
    }

    let attempts = backend.attempts.lock().expect("attempts lock").clone();
    let distinct: std::collections::BTreeSet<&str> = attempts.iter().map(String::as_str).collect();
    assert!(
        distinct.len() > 1,
        "every tick retried the same wedged node, starving the rest: {attempts:?}"
    );
    assert!(
        distinct.contains("worker-1") || distinct.contains("worker-2"),
        "a node behind the wedged one must get a turn: {attempts:?}"
    );
}

#[test]
fn fairness_cursor_rotation_survives_a_shrinking_fleet() {
    let mut cursor = FairnessCursor::default();
    cursor.advance(7);

    // The offset outruns the list; rotation takes it modulo the current length
    // rather than panicking or silently pinning index 0.
    let mut changes = vec![
        NodeLabelChange {
            node_name: "worker-a".to_string(),
            desired: NodeServicesReady::Ready,
        },
        NodeLabelChange {
            node_name: "worker-b".to_string(),
            desired: NodeServicesReady::Ready,
        },
    ];
    cursor.apply(&mut changes);
    assert_eq!(changes[0].node_name, "worker-b");

    // Empty list is a no-op, not a modulo-by-zero.
    let mut empty: Vec<NodeLabelChange> = Vec::new();
    cursor.apply(&mut empty);
    assert!(empty.is_empty());
}

#[tokio::test]
async fn disabled_reconciler_parks_until_shutdown() {
    let shutdown = CancellationToken::new();
    let config = ReconcilerConfig::default();
    assert!(!config.enabled);

    let handle = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            run_with_backend(
                FakeBackend::new(Vec::new(), Vec::new()),
                ReconcilerConfig {
                    enabled: true,
                    ..config
                },
                shutdown,
            )
            .await
        }
    });
    shutdown.cancel();
    handle.await.expect("reconciler task should exit cleanly");
}

/// A fleet where more nodes read not-ready than concurrent DaemonSet waves can
/// explain is a *signal* failure. The writer must not publish those verdicts: it
/// republishes `Ready` and actively clears the stale `false` labels, because the
/// soft affinity term has no reader-side valve and — with the template-cache
/// preference ANDed to readiness — a wide band of `false` also strips cache
/// locality from those nodes.
#[test]
fn writer_fails_open_and_clears_labels_when_coverage_exceeds_rollout_budgets() {
    // 20 nodes, 12 not-ready (60%) — far past what two 10% waves can produce.
    // The 12 degraded ones already carry a stale `false`; the 8 serving ones
    // are correctly `true`, so only the 12 need clearing.
    let nodes: Vec<NodeLabelState> = (0..20)
        .map(|index| {
            let current = if index < 12 { "false" } else { "true" };
            node(&format!("worker-{index}"), Some(current))
        })
        .collect();
    let pods: Vec<ServicePod> = (12..20)
        .flat_map(|index| {
            let name = format!("worker-{index}");
            [
                pod(&name, NodeLocalComponent::NodeCtl),
                pod(&name, NodeLocalComponent::VolumeManager),
            ]
        })
        .collect();

    let plan = plan_for_test(&nodes, &pods);

    assert!(plan.failed_open);
    assert_eq!(plan.not_ready, 12, "the gauge still reports the real count");
    assert!(
        plan.changes
            .iter()
            .all(|change| change.desired == NodeServicesReady::Ready),
        "fail-open must clear labels to Ready, not leave stale falses in place"
    );
    assert_eq!(
        plan.changes.len(),
        12,
        "only the 12 nodes carrying a stale false need a write"
    );
}

/// A routine wave — two watched components at the charts' `maxUnavailable: 10%`
/// — must sit inside the threshold, or the feature is off in normal operation.
#[test]
fn a_routine_concurrent_rollout_does_not_fail_open() {
    let nodes: Vec<NodeLabelState> = (0..20)
        .map(|index| node(&format!("worker-{index}"), Some("true")))
        .collect();
    // 4 of 20 not-ready (20%) = the disjoint union of two 10% waves.
    let pods: Vec<ServicePod> = (4..20)
        .flat_map(|index| {
            let name = format!("worker-{index}");
            [
                pod(&name, NodeLocalComponent::NodeCtl),
                pod(&name, NodeLocalComponent::VolumeManager),
            ]
        })
        .collect();

    let plan = plan_for_test(&nodes, &pods);

    assert!(!plan.failed_open, "a 20% wave is a rollout, not a failure");
    assert_eq!(plan.not_ready, 4);
    assert!(
        plan.changes
            .iter()
            .all(|change| change.desired == NodeServicesReady::NotReady)
    );
}

/// The threshold has to widen with the component list: Helm doesn't serialize
/// DaemonSet updates, so N watched components can leave ~10·N% not-ready at
/// once. A fixed threshold would make every rollout fail open once the list
/// grows.
#[test]
fn fail_open_threshold_scales_with_the_component_count() {
    let two = default_max_not_ready_fraction(2);
    let six = default_max_not_ready_fraction(6);
    assert!(
        two > 2.0 * DAEMONSET_MAX_UNAVAILABLE_FRACTION,
        "two 10% waves ({}) must fit under the two-component threshold ({two})",
        2.0 * DAEMONSET_MAX_UNAVAILABLE_FRACTION
    );
    assert!(
        six > two,
        "more components must allow a wider not-ready band"
    );
    assert!(
        six <= MAX_NOT_READY_FRACTION_CEILING,
        "past half the fleet the signal is not describing a rollout"
    );
}

/// Small fleets are exempt: Kubernetes rounds `maxUnavailable: 10%` up to at
/// least one node, so on a handful of nodes one legitimately-rolling node is
/// already a large share and a fraction guard would fire on every rollout.
#[test]
fn small_fleets_never_fail_open() {
    let nodes: Vec<NodeLabelState> = (0..3)
        .map(|index| node(&format!("worker-{index}"), Some("true")))
        .collect();

    let plan = plan_for_test(&nodes, &[]);

    assert!(!plan.failed_open);
    assert_eq!(plan.not_ready, 3);
    assert!(
        plan.changes
            .iter()
            .all(|change| change.desired == NodeServicesReady::NotReady)
    );
}

/// The plan is computed once per tick, but a rollout moves under it. Publishing
/// a planned verdict that the current cache already contradicts writes a value
/// we know is stale — and while it stands it steers placement on evidence that
/// no longer holds.
#[tokio::test]
async fn a_verdict_that_changed_since_planning_is_not_published() {
    struct RecoveredNodeBackend {
        writes: Mutex<Vec<String>>,
    }

    impl ReadinessBackend for RecoveredNodeBackend {
        fn caches_ready(&self) -> bool {
            true
        }

        fn placement_nodes(&self) -> Option<Vec<NodeLabelState>> {
            // Published `true`, so the plan below wants to write `false`.
            Some(vec![node("worker-a", Some("true"))])
        }

        /// Plan phase sees no serving pods → verdict NotReady.
        fn serving_pods(&self) -> Vec<ServicePod> {
            Vec::new()
        }

        /// Pre-patch re-check sees the pod back → verdict Ready, contradicting
        /// the planned `false`.
        fn verdict_now(
            &self,
            _node_name: &str,
            _components: &[NodeLocalComponent],
        ) -> NodeServicesReady {
            NodeServicesReady::Ready
        }

        async fn set_label(
            &self,
            node_name: &str,
            _desired: NodeServicesReady,
        ) -> Result<(), BackendError> {
            self.writes
                .lock()
                .expect("writes lock")
                .push(node_name.to_string());
            Ok(())
        }
    }

    let backend = RecoveredNodeBackend {
        writes: Mutex::new(Vec::new()),
    };

    reconcile_once(&backend, &live_config(), &mut FairnessCursor::default()).await;

    assert!(
        backend.writes.lock().expect("writes lock").is_empty(),
        "a verdict contradicted by the current cache must not be published"
    );
}

#[test]
fn component_names_round_trip_through_the_label_value() {
    for component in [
        NodeLocalComponent::NodeCtl,
        NodeLocalComponent::VolumeManager,
        NodeLocalComponent::SandboxManager,
        NodeLocalComponent::NodeManager,
        NodeLocalComponent::NodeApi,
        NodeLocalComponent::TemplateManager,
    ] {
        assert_eq!(
            NodeLocalComponent::from_str(component.as_str()),
            Ok(component)
        );
    }
}

mod pod_classification {
    use k8s_openapi::api::core::v1::{Pod, PodCondition, PodSpec, PodStatus};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;

    use super::super::backend::{component_selector, node_label_state, serving_pod};
    use super::*;

    struct PodShape {
        node_name: Option<&'static str>,
        component: Option<&'static str>,
        ready: bool,
        terminating: bool,
    }

    impl Default for PodShape {
        fn default() -> Self {
            Self {
                node_name: Some("worker-a"),
                component: Some("node-ctl"),
                ready: true,
                terminating: false,
            }
        }
    }

    /// A fixed `metadata.deletionTimestamp`, built through the wire form so
    /// the test needs no direct dependency on k8s-openapi's clock crate.
    fn deletion_timestamp() -> Time {
        serde_json::from_value(serde_json::json!("2026-07-24T11:34:00Z"))
            .expect("fixed RFC 3339 timestamp deserializes")
    }

    fn service_pod(shape: PodShape) -> Pod {
        Pod {
            metadata: kube::api::ObjectMeta {
                name: Some("space-worker-node-ctl-abcde".to_string()),
                labels: shape.component.map(|component| {
                    std::collections::BTreeMap::from([(
                        "app.kubernetes.io/component".to_string(),
                        component.to_string(),
                    )])
                }),
                deletion_timestamp: shape.terminating.then(deletion_timestamp),
                ..Default::default()
            },
            spec: Some(PodSpec {
                node_name: shape.node_name.map(str::to_string),
                ..Default::default()
            }),
            status: Some(PodStatus {
                conditions: Some(vec![PodCondition {
                    type_: "Ready".to_string(),
                    status: if shape.ready { "True" } else { "False" }.to_string(),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn ready_bound_pod_is_serving() {
        assert_eq!(
            serving_pod(&service_pod(PodShape::default())),
            Some(ServicePod {
                node_name: "worker-a".to_string(),
                component: NodeLocalComponent::NodeCtl,
            })
        );
    }

    /// The lead signal: kubelet keeps `Ready=True` on a pod under deletion
    /// until its container actually stops, so a terminating pod must read as
    /// not-serving or the bias would only engage after the daemon is already
    /// gone.
    #[test]
    fn terminating_pod_is_not_serving() {
        assert_eq!(
            serving_pod(&service_pod(PodShape {
                terminating: true,
                ..PodShape::default()
            })),
            None
        );
    }

    #[test]
    fn unready_or_unbound_pod_is_not_serving() {
        assert_eq!(
            serving_pod(&service_pod(PodShape {
                ready: false,
                ..PodShape::default()
            })),
            None
        );
        assert_eq!(
            serving_pod(&service_pod(PodShape {
                node_name: None,
                ..PodShape::default()
            })),
            None
        );
        assert_eq!(
            serving_pod(&service_pod(PodShape {
                component: Some("some-other-daemon"),
                ..PodShape::default()
            })),
            None
        );
    }

    #[test]
    fn node_label_state_reads_the_published_label() {
        let node = k8s_openapi::api::core::v1::Node {
            metadata: kube::api::ObjectMeta {
                name: Some("worker-a".to_string()),
                labels: Some(std::collections::BTreeMap::from([(
                    pplx_space_common_core::kubernetes::NODE_SERVICES_READY_LABEL.to_string(),
                    "false".to_string(),
                )])),
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(
            node_label_state(&node),
            Some(NodeLabelState {
                name: "worker-a".to_string(),
                current: Some("false".to_string()),
            })
        );
    }

    #[test]
    fn watch_selector_covers_every_configured_component() {
        let selector = component_selector(&ReconcilerConfig::default());
        assert_eq!(
            selector,
            "app.kubernetes.io/component in (node-ctl,volume-manager)"
        );
    }
}

#[test]
fn label_values_match_the_published_vocabulary() {
    assert_eq!(
        NodeServicesReady::Ready.as_str(),
        pplx_space_common_core::kubernetes::NODE_SERVICES_READY_VALUE
    );
    assert_eq!(
        NodeServicesReady::NotReady.as_str(),
        pplx_space_common_core::kubernetes::NODE_SERVICES_NOT_READY_VALUE
    );
    assert_eq!(
        node_label_apply("worker-a", NodeServicesReady::NotReady),
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": "worker-a",
                "labels": {
                    "space.pplx.net/node-services-ready": "false",
                },
            }
        })
    );
}
