use pplx_space_common_core::types::{NodeGroupId, SnapshotId};

use super::execution::{
    RecoveryJoinBackoffPolicy, ensure_wake_recovery_lifecycle, should_join_started_recovery,
};
use super::pod_ops::{
    ActivationWakeFailure, PodCreateContext, RestoreContext, WakeDispatchStatus,
    parse_pod_create_context,
};
use super::support::*;
use super::*;

pub(crate) use kubernetes_tests::{
    existing_then_created_running_pod_kubernetes_client,
    insert_ready_full_snapshot_relative_to_lifecycle, mock_kubernetes_client,
    running_pod_kubernetes_client, running_pod_kubernetes_client_with_create_barrier,
    substrate_with_store_and_client, substrate_with_store_client_proxy_port_and_pod,
};

mod kubernetes_tests {

    use pplx_space_common_core::types::{LocalArtifactId, SandboxId, UserId};
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant as StdInstant};

    use super::*;
    use crate::controllers::sandbox::lifecycle::SandboxAction;
    use crate::controllers::sandbox::{SandboxController, fetch_sandbox};
    use crate::controllers::substrate::kubernetes::store::{
        ActivationFenceOwnership, ActivationRoutePublication, classify_activation_route_publication,
    };
    use crate::controllers::substrate::{
        NodeApiTarget, PooledExecutionClaim, PublishedLocalArtifact, RecordedExecution, Substrate,
        SubstrateAdapter, TeardownProgress,
    };
    use axum::http::{Method, Request, Response, StatusCode};
    use http_body_util::BodyExt;
    use k8s_openapi::api::core::v1::{
        Container, NodeAddress as K8sNodeAddress, NodeCondition, NodeSpec as K8sNodeSpec,
        NodeStatus as K8sNodeStatus, PodCondition, PodIP, PodSpec as K8sPodSpec,
        PodStatus as K8sPodStatus, ResourceRequirements, Taint,
    };
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    use kube::client::{Body, ClientBuilder};
    use kube::runtime::{reflector, watcher::Event};
    use pplx_space_common_core::kubernetes::{
        NodeGroupPlacement, SANDBOX_CHECKPOINT_ANNOTATION, SANDBOX_EXECUTION_GENERATION_ANNOTATION,
        SANDBOX_NETWORK_SLOTS_RESOURCE, SANDBOX_NODE_GROUP_LABEL,
        SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION,
        SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION,
    };
    use pplx_space_common_core::sql;
    use pplx_space_common_core::types::{
        ArtifactKind, ArtifactOrigin, ExecutionGeneration, NodeAddress, NodeGroup, NodeGroupName,
        NodeGroupPolicy, NodeId, NodeManagedBy as SpaceNodeManagedBy, NodeRole as SpaceNodeRole,
        OrgId, PodName, PodResourceVersion, PodUid, SandboxActivationAttemptId, SandboxLifecycle,
        SandboxOwner, SandboxOwnerSource, SandboxTransitionTargetState, TemplateCompatSpec,
        TemplateName, TemplateVisibility,
    };
    use pplx_space_common_svc::space_api_route_ed25519::{
        SPACE_API_ROUTE_ED25519_ISSUER_ENV, SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV,
        SPACE_API_ROUTE_ED25519_SIGNING_KEY_PATH_ENV, SpaceApiRouteEd25519Authority,
    };
    use pplx_space_internal_api_server::publish_sandbox_states;
    use pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity;
    use pplx_space_node_api_client::proto::PublishedSandboxState;
    use pplx_space_node_api_client::proto::worker_api_internal_server::{
        WorkerApiInternal, WorkerApiInternalServer,
    };
    use pplx_space_node_api_client::proto::worker_api_server::WorkerApiServer;
    use pplx_space_node_api_sim::{SimState, SimWorkerApiService, SimWorkerHarness};
    use pplx_space_sbx_mgr_client::{
        operations as spaced_ops,
        proto::{SpacedStartProcessRequest, spaced_process_event},
    };

    use sqlx::{PgPool, postgres::PgPoolOptions};
    use tokio::sync::Semaphore;
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;
    use tonic::transport::{Server, server::TcpIncoming};

    type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

    #[derive(Clone, Default)]
    struct SandboxEventCapture {
        events: Arc<Mutex<Vec<BTreeMap<String, String>>>>,
    }

    impl<S> tracing_subscriber::Layer<S> for SandboxEventCapture
    where
        S: tracing::Subscriber,
    {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut visitor = pplx_space_observability::test_capture::RecordingVisitor::default();
            event.record(&mut visitor);
            let fields: BTreeMap<String, String> = visitor.into_fields().into_iter().collect();
            if fields.get("event_stream").map(String::as_str) == Some("space.sandbox") {
                self.events.lock().unwrap().push(fields);
            }
        }
    }

    fn inventory_entry(id: Uuid, state: &str, created_at: OffsetDateTime) -> NodeInventoryEntry {
        NodeInventoryEntry {
            id: NodeId::from_uuid(id),
            name: Some("ip-10-0-0-1.ec2.internal".to_string()),
            address: Some("10.0.0.1".parse::<NodeAddress>().unwrap()),
            role: SpaceNodeRole::Worker,
            managed_by: SpaceNodeManagedBy::Kubernetes,
            state: Some(state.to_string()),
            last_heartbeat: None,
            created_at,
            substrate_node_id: Some("ip-10-0-0-1.ec2.internal".to_string()),
        }
    }

    fn node_api_pod_on_node(name: &str, node_name: &str) -> Pod {
        Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(name.to_string()),
                labels: Some(BTreeMap::from([(
                    "app.kubernetes.io/name".to_string(),
                    "space-worker".to_string(),
                )])),
                ..Default::default()
            },
            spec: Some(K8sPodSpec {
                node_name: Some(node_name.to_string()),
                ..Default::default()
            }),
            status: Some(K8sPodStatus {
                conditions: Some(vec![PodCondition {
                    type_: "Ready".to_string(),
                    status: "True".to_string(),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
        }
    }

    fn unready_node_api_pod_on_node(name: &str, node_name: &str) -> Pod {
        let mut pod = node_api_pod_on_node(name, node_name);
        pod.status = Some(K8sPodStatus {
            conditions: Some(vec![PodCondition {
                type_: "Ready".to_string(),
                status: "False".to_string(),
                ..Default::default()
            }]),
            ..Default::default()
        });
        pod
    }

    fn k8s_node_with_internal_ip(name: &str, internal_ip: &str) -> Node {
        Node {
            metadata: kube::api::ObjectMeta {
                name: Some(name.to_string()),
                uid: Some(format!("{name}-uid")),
                ..Default::default()
            },
            status: Some(K8sNodeStatus {
                addresses: Some(vec![K8sNodeAddress {
                    type_: "InternalIP".to_string(),
                    address: internal_ip.to_string(),
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn substrate_with_inventory_caches(pods: Vec<Pod>, nodes: Vec<Node>) -> KubernetesSubstrate {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .expect("lazy pool builds without connecting");
        substrate_with_inventory_caches_and_pool(pods, nodes, pool)
    }

    fn substrate_with_inventory_caches_and_pool(
        pods: Vec<Pod>,
        nodes: Vec<Node>,
        pool: PgPool,
    ) -> KubernetesSubstrate {
        let shutdown_token = CancellationToken::new();
        let task_tracker = tokio_util::task::TaskTracker::new();
        let (pod_store, mut pod_writer) = reflector::store::<Pod>();
        pod_writer.apply_watcher_event(&Event::Init);
        for pod in pods {
            pod_writer.apply_watcher_event(&Event::InitApply(pod));
        }
        pod_writer.apply_watcher_event(&Event::InitDone);

        let (node_store, mut node_writer) = reflector::store::<Node>();
        node_writer.apply_watcher_event(&Event::Init);
        for node in nodes {
            node_writer.apply_watcher_event(&Event::InitApply(node));
        }
        node_writer.apply_watcher_event(&Event::InitDone);

        let caches = Arc::new(K8sCaches::for_test(pod_store.clone(), node_store, true));
        KubernetesSubstrate {
            k8s: KubernetesState {
                client: inert_kubernetes_client(),
                sandbox_namespace: "space".to_string(),
                sandbox_proxy_port: DEFAULT_SANDBOX_PROXY_PORT,
                node_api_port: DEFAULT_SANDBOX_PROXY_PORT,
                sandbox_pod_config: SandboxPodConfigValues::default(),
                create_command_ready_timeout: Duration::from_secs(1),
                create_data_plane_ready_timeout: Duration::from_secs(1),
                k8s_blocking_delete_timeout: Duration::from_secs(1),
                caches,
            },
            restore_waiters: RestoreWaiters::new(
                K8sStore::new(pool.clone(), "space".to_string()),
                shutdown_token.clone(),
                task_tracker.clone(),
            ),
            wake_coalescer: Arc::new(
                pplx_space_common_work_coalescer::WorkCoalescer::with_tracker(task_tracker.clone()),
            ),
            store: K8sStore::new(pool.clone(), "space".to_string()),
            pod_store,
            pod_events: pod_reflector::PodEvents::for_test(),
            pod_reflector_ready: ReflectorReady::always_ready(),
            cancel_tracker: Arc::new(CancelRateTracker::with_budget(64)),
            // Gate closed: these tests exercise pod lifecycle, not placement,
            // and a closed gate issues no query — so the lazily-connected pool
            // stays undialed. Placement rendering is covered by the pod-builder
            // snapshots; resolution by `node_group_placement`'s own tests.
            node_group_placement: NodeGroupPlacementResolver::new(pool, false),
            recovery_join_backoff: RecoveryJoinBackoffPolicy::default(),
            shutdown_token,
            task_tracker,
        }
    }

    #[test]
    fn wake_recovery_lifecycle_excludes_stop_and_delete_states() {
        for lifecycle in [
            "created",
            "creating",
            "starting",
            "started",
            "pausing",
            "paused",
            "resuming",
            "checkpointing",
            "suspending",
            "suspended",
            "restoring",
        ] {
            assert!(
                is_wake_recovery_lifecycle(lifecycle),
                "{lifecycle} should allow wake recovery"
            );
        }

        for lifecycle in [
            "stopping",
            "stopped",
            "deleting",
            "deleted",
            "cleanup_required",
            "failed",
        ] {
            assert!(
                !is_wake_recovery_lifecycle(lifecycle),
                "{lifecycle} should reject wake recovery"
            );
        }

        assert!(
            is_recoverable_live_lifecycle("stopped"),
            "start still handles stopped sandboxes"
        );
    }

    #[test]
    fn suspending_wake_recovery_preserves_retryable_suspend_ownership() {
        let sandbox = test_sandbox_with_lifecycle(SandboxLifecycle::Suspending.as_str());

        let error = ensure_wake_recovery_lifecycle(&sandbox)
            .expect_err("wake recovery must defer to suspend finalization");

        assert!(
            matches!(error, SubstrateError::SuspendInFlight { .. }),
            "expected the retryable suspend ownership signal, got {error:?}",
        );
    }

    #[test]
    fn paused_restore_does_not_join_started_recovery() {
        let should_join = should_join_started_recovery(
            pplx_space_common_core::types::SandboxLifecycle::Started,
            pplx_space_common_core::types::SandboxLifecycle::Paused,
        );

        assert!(!should_join);
    }

    #[test]
    fn shortlist_from_candidates_puts_source_first_dedupes_and_is_deterministic() {
        let sandbox_a = SandboxId::new();
        let sandbox_b = SandboxId::new();
        let candidates: Vec<String> = (0..100).map(|i| format!("node-{i:03}")).collect();

        let mut with_source = candidates.clone();
        with_source.push("source-node".to_string());
        let shortlist = shortlist_from_candidates(sandbox_a, "source-node", with_source);

        // Source first, exactly once, capped at source + 63 fallbacks.
        assert_eq!(shortlist[0], "source-node");
        assert_eq!(shortlist.len(), 64);
        assert_eq!(
            shortlist
                .iter()
                .filter(|name| name.as_str() == "source-node")
                .count(),
            1
        );

        // Deterministic per sandbox (stable across retries)...
        assert_eq!(
            shortlist,
            shortlist_from_candidates(sandbox_a, "source-node", {
                let mut again = candidates.clone();
                again.push("source-node".to_string());
                again
            })
        );
        // ...but a different sandbox spreads across a different fallback tail.
        assert_ne!(
            shortlist,
            shortlist_from_candidates(sandbox_b, "source-node", {
                let mut other = candidates.clone();
                other.push("source-node".to_string());
                other
            })
        );

        // Fewer candidates than the cap: everything is kept.
        let small = shortlist_from_candidates(
            sandbox_a,
            "source-node",
            vec!["only-a".to_string(), "only-b".to_string()],
        );
        assert_eq!(small.len(), 3);
        assert_eq!(small[0], "source-node");
    }

    fn pod_with_scheduling(node_name: Option<&str>, unschedulable: bool) -> Pod {
        let conditions = unschedulable.then(|| {
            vec![PodCondition {
                type_: "PodScheduled".to_string(),
                status: "False".to_string(),
                reason: Some("Unschedulable".to_string()),
                message: Some("0/3 nodes are available".to_string()),
                ..Default::default()
            }]
        });
        Pod {
            spec: Some(K8sPodSpec {
                node_name: node_name.map(str::to_string),
                ..Default::default()
            }),
            status: Some(K8sPodStatus {
                conditions,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn scheduling_outcome_classifies_scheduler_decision() {
        // Bound to a node — the outcome carries the landing site so shortlist
        // restores can record source-vs-fallback placement.
        assert_eq!(
            scheduling_outcome_of(&pod_with_scheduling(Some("worker-a"), false)),
            Some(SchedulingOutcome::Scheduled {
                node_name: "worker-a".to_string()
            })
        );
        // Scheduler rejected every pinned node: still Pending, no nodeName.
        assert_eq!(
            scheduling_outcome_of(&pod_with_scheduling(None, true)),
            Some(SchedulingOutcome::Unschedulable)
        );
        // Not yet decided — neither bound nor marked unschedulable.
        assert_eq!(
            scheduling_outcome_of(&pod_with_scheduling(None, false)),
            None
        );
        // An empty nodeName is not a bind.
        assert_eq!(
            scheduling_outcome_of(&pod_with_scheduling(Some(""), false)),
            None
        );
        // A bind is authoritative even when a stale Unschedulable condition
        // lingers: nodeName is immutable once set, so the pod is no longer
        // inert and must never be classified deletable-unschedulable.
        assert_eq!(
            scheduling_outcome_of(&pod_with_scheduling(Some("worker-a"), true)),
            Some(SchedulingOutcome::Scheduled {
                node_name: "worker-a".to_string()
            })
        );
    }

    #[test]
    fn pod_has_required_source_pin_detects_only_the_required_matchfields_pin() {
        use k8s_openapi::api::core::v1::{
            Affinity, NodeAffinity, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm,
            PreferredSchedulingTerm,
        };
        let pod_with_affinity = |affinity: Option<Affinity>| Pod {
            spec: Some(K8sPodSpec {
                affinity,
                ..Default::default()
            }),
            ..Default::default()
        };
        let name_match = || NodeSelectorRequirement {
            key: "metadata.name".to_string(),
            operator: "In".to_string(),
            values: Some(vec!["worker-a".to_string()]),
        };

        // Required matchFields metadata.name term -> pinned.
        let required = pod_with_affinity(Some(Affinity {
            node_affinity: Some(NodeAffinity {
                required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                    node_selector_terms: vec![NodeSelectorTerm {
                        match_fields: Some(vec![name_match()]),
                        ..Default::default()
                    }],
                }),
                ..Default::default()
            }),
            ..Default::default()
        }));
        assert!(pod_has_required_source_pin(&required));

        // Only a *preferred* metadata.name term (legacy soft affinity) -> not pinned.
        let preferred = pod_with_affinity(Some(Affinity {
            node_affinity: Some(NodeAffinity {
                preferred_during_scheduling_ignored_during_execution: Some(vec![
                    PreferredSchedulingTerm {
                        weight: 100,
                        preference: NodeSelectorTerm {
                            match_fields: Some(vec![name_match()]),
                            ..Default::default()
                        },
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        }));
        assert!(!pod_has_required_source_pin(&preferred));

        // Required term without matchFields (e.g. only the ssd-block exclusion)
        // -> not pinned (a cross-node re-driven pod).
        let required_no_pin = pod_with_affinity(Some(Affinity {
            node_affinity: Some(NodeAffinity {
                required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                    node_selector_terms: vec![NodeSelectorTerm {
                        match_expressions: Some(vec![NodeSelectorRequirement {
                            key: "space.pplx.net/ssd-placement-blocked".to_string(),
                            operator: "NotIn".to_string(),
                            values: Some(vec!["true".to_string()]),
                        }]),
                        ..Default::default()
                    }],
                }),
                ..Default::default()
            }),
            ..Default::default()
        }));
        assert!(!pod_has_required_source_pin(&required_no_pin));

        // No affinity at all -> not pinned.
        assert!(!pod_has_required_source_pin(&pod_with_affinity(None)));
    }

    fn node_with_network_slots(
        name: &str,
        labels: BTreeMap<String, String>,
        capacity: Option<u64>,
        allocatable: Option<u64>,
        ready: bool,
        unschedulable: bool,
    ) -> Arc<Node> {
        let resource_map = |slots: u64| {
            BTreeMap::from([(
                SANDBOX_NETWORK_SLOTS_RESOURCE.to_string(),
                Quantity(slots.to_string()),
            )])
        };
        Arc::new(Node {
            metadata: kube::api::ObjectMeta {
                name: Some(name.to_string()),
                labels: Some(labels),
                ..Default::default()
            },
            spec: Some(K8sNodeSpec {
                unschedulable: Some(unschedulable),
                ..Default::default()
            }),
            status: Some(K8sNodeStatus {
                capacity: capacity.map(resource_map),
                allocatable: allocatable.map(resource_map),
                conditions: Some(vec![NodeCondition {
                    type_: "Ready".to_string(),
                    status: if ready { "True" } else { "False" }.to_string(),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
        })
    }

    fn sandbox_pod_on_node(name: &str, node_name: &str, slots: u64, phase: &str) -> Arc<Pod> {
        Arc::new(Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(name.to_string()),
                ..Default::default()
            },
            spec: Some(K8sPodSpec {
                node_name: Some(node_name.to_string()),
                containers: vec![Container {
                    name: "sandbox".to_string(),
                    resources: Some(ResourceRequirements {
                        requests: Some(BTreeMap::from([(
                            SANDBOX_NETWORK_SLOTS_RESOURCE.to_string(),
                            Quantity(slots.to_string()),
                        )])),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(phase.to_string()),
                ..Default::default()
            }),
        })
    }

    #[test]
    fn network_slot_preflight_admits_matching_node_with_free_capacity() {
        let config = SandboxPodConfigValues::default();
        let nodes = vec![node_with_network_slots(
            "node-a",
            config.base_node_selector().into_labels(),
            Some(2),
            None,
            true,
            false,
        )];
        let pods = vec![sandbox_pod_on_node("sandbox-a", "node-a", 1, "Running")];

        assert_eq!(
            super::adapter::network_slot_capacity_preflight(&nodes, &pods, &config),
            super::adapter::NetworkSlotCapacityPreflight::Available
        );
    }

    #[test]
    fn network_slot_preflight_allows_unknown_empty_cache() {
        let config = SandboxPodConfigValues::default();

        assert_eq!(
            super::adapter::network_slot_capacity_preflight(&[], &[], &config),
            super::adapter::NetworkSlotCapacityPreflight::Unknown
        );
    }

    #[test]
    fn network_slot_preflight_rejects_when_matching_nodes_are_full() {
        let config = SandboxPodConfigValues::default();
        let nodes = vec![node_with_network_slots(
            "node-a",
            config.base_node_selector().into_labels(),
            Some(1),
            None,
            true,
            false,
        )];
        let pods = vec![sandbox_pod_on_node("sandbox-a", "node-a", 1, "Running")];

        assert_eq!(
            super::adapter::network_slot_capacity_preflight(&nodes, &pods, &config),
            super::adapter::NetworkSlotCapacityPreflight::Full
        );
    }

    #[test]
    fn network_slot_preflight_ignores_terminal_pods() {
        let config = SandboxPodConfigValues::default();
        let nodes = vec![node_with_network_slots(
            "node-a",
            config.base_node_selector().into_labels(),
            Some(1),
            None,
            true,
            false,
        )];
        let pods = vec![sandbox_pod_on_node("sandbox-a", "node-a", 1, "Succeeded")];

        assert_eq!(
            super::adapter::network_slot_capacity_preflight(&nodes, &pods, &config),
            super::adapter::NetworkSlotCapacityPreflight::Available
        );
    }

    #[test]
    fn network_slot_preflight_reports_unknown_without_matching_schedulable_capacity() {
        let config = SandboxPodConfigValues::default();
        let mut unmatched_labels = config.base_node_selector().into_labels();
        unmatched_labels.insert(
            config.sandbox_node_selector_key.clone(),
            "other-pool".to_string(),
        );
        let nodes = vec![
            node_with_network_slots("node-a", unmatched_labels, Some(8), None, true, false),
            node_with_network_slots(
                "node-b",
                config.base_node_selector().into_labels(),
                Some(8),
                None,
                true,
                true,
            ),
            node_with_network_slots(
                "node-c",
                config.base_node_selector().into_labels(),
                Some(8),
                None,
                false,
                false,
            ),
        ];

        assert_eq!(
            super::adapter::network_slot_capacity_preflight(&nodes, &[], &config),
            super::adapter::NetworkSlotCapacityPreflight::Unknown
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn started_recovery_local_source_must_pass_direct_placement_gates() {
        let config = SandboxPodConfigValues::default();
        let mut blocked_labels = config.base_node_selector().into_labels();
        blocked_labels.insert(
            pplx_space_common_core::kubernetes::SSD_PLACEMENT_BLOCKED_LABEL.to_string(),
            pplx_space_common_core::kubernetes::SSD_PLACEMENT_BLOCKED_VALUE.to_string(),
        );
        let mut wrong_selector = config.base_node_selector().into_labels();
        wrong_selector.insert(
            config.sandbox_node_selector_key.clone(),
            "other-pool".to_string(),
        );
        let nodes = vec![
            (*node_with_network_slots(
                "healthy",
                config.base_node_selector().into_labels(),
                Some(1),
                None,
                true,
                false,
            ))
            .clone(),
            (*node_with_network_slots("blocked", blocked_labels, Some(1), None, true, false))
                .clone(),
            (*node_with_network_slots(
                "wrong-selector",
                wrong_selector,
                Some(1),
                None,
                true,
                false,
            ))
            .clone(),
            (*node_with_network_slots(
                "full",
                config.base_node_selector().into_labels(),
                Some(1),
                None,
                true,
                false,
            ))
            .clone(),
        ];
        let full_pod = (*sandbox_pod_on_node("existing", "full", 1, "Running")).clone();
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .expect("lazy pool builds without connecting");
        let substrate = substrate_with_store_client_proxy_port_pod_and_nodes(
            pool,
            inert_kubernetes_client(),
            DEFAULT_SANDBOX_PROXY_PORT,
            Some(full_pod),
            nodes,
        );

        assert!(substrate.started_recovery_source_node_available("healthy"));
        for unavailable in ["blocked", "wrong-selector", "full"] {
            assert!(
                !substrate.started_recovery_source_node_available(unavailable),
                "{unavailable} must not receive a direct nodeName restore"
            );
        }
    }

    /// A node missing one configured extra label is not admission capacity.
    ///
    /// Driven through `eligible_start_node_count` rather than the matcher
    /// directly: equality-on-every-entry is owned and tested one layer down,
    /// in `common-core`'s `node_selector::tests`, so what this asserts is the
    /// api-crate delta — that the count admission actually gates on carries
    /// the extra labels through, and reports a confident zero rather than
    /// counting a partially-labeled worker.
    #[tokio::test(flavor = "current_thread")]
    async fn sandbox_start_candidate_selector_requires_extra_labels() {
        let capacity_class = "space.pplx.net/capacity-class";
        let config = SandboxPodConfigValues {
            extra_node_selector_labels: BTreeMap::from([(
                capacity_class.to_string(),
                "sandbox".to_string(),
            )]),
            ..SandboxPodConfigValues::default()
        };
        let mut labels = config.base_node_selector().into_labels();
        let matching =
            node_with_network_slots("node-a", labels.clone(), Some(8), None, true, false);
        assert_eq!(
            substrate_with_node_cache_and_config(&[matching], true, config.clone())
                .eligible_start_node_count(),
            Some(1),
            "a fully labeled worker is admission capacity"
        );

        labels.remove(capacity_class);
        let missing_extra =
            node_with_network_slots("node-b", labels.clone(), Some(8), None, true, false);
        assert_eq!(
            substrate_with_node_cache_and_config(&[missing_extra], true, config)
                .eligible_start_node_count(),
            Some(0),
            "a node carrying only the discriminator must not count as capacity"
        );
    }

    #[test]
    fn sandbox_start_candidate_filter_rejects_unschedulable_and_not_ready_but_counts_tainted_nodes()
    {
        let config = SandboxPodConfigValues::default();
        let healthy = node_with_network_slots(
            "node-a",
            config.base_node_selector().into_labels(),
            Some(8),
            None,
            true,
            false,
        );
        assert!(super::adapter::node_schedulable(&healthy));

        let unschedulable = node_with_network_slots(
            "node-b",
            config.base_node_selector().into_labels(),
            Some(8),
            None,
            true,
            true,
        );
        assert!(!super::adapter::node_schedulable(&unschedulable));

        let not_ready = node_with_network_slots(
            "node-c",
            config.base_node_selector().into_labels(),
            Some(8),
            None,
            false,
            false,
        );
        assert!(!super::adapter::node_schedulable(&not_ready));

        let mut tainted = (*node_with_network_slots(
            "node-d",
            config.base_node_selector().into_labels(),
            Some(8),
            None,
            true,
            false,
        ))
        .clone();
        tainted.spec.as_mut().unwrap().taints = Some(vec![Taint {
            effect: "NoSchedule".to_string(),
            ..Default::default()
        }]);
        // The empty-cluster guard is coarse ("is the cluster start-capable at
        // all?"), so a Ready, schedulable node still counts even when tainted.
        // Restore/evacuation pods tolerate cohort taints via extra_tolerations,
        // and taint-vs-toleration matching is the scheduler's job, not this
        // preflight's; rejecting tainted nodes here would return a false
        // `empty_cluster` during a tainted-cohort rollout. Only Ready=false and
        // cordoned (unschedulable) nodes are truly ineligible.
        assert!(super::adapter::node_schedulable(&tainted));
    }

    fn inert_kubernetes_client() -> Client {
        let service = tower::service_fn(|_req: Request<Body>| async {
            Ok::<_, BoxError>(Response::new(Body::from(Vec::new())))
        });
        ClientBuilder::new(service, "space").build()
    }

    fn blocking_failed_pod_create_kubernetes_client(
        request_count: Arc<AtomicUsize>,
        request_started: Arc<Semaphore>,
        release_request: Arc<Semaphore>,
    ) -> Client {
        let service = tower::service_fn(move |req: Request<Body>| {
            let request_count = Arc::clone(&request_count);
            let request_started = Arc::clone(&request_started);
            let release_request = Arc::clone(&release_request);
            async move {
                let method = req.method().clone();
                let path = req.uri().path().to_string();
                match (method, path.as_str()) {
                    (Method::POST, "/api/v1/namespaces/space/pods") => {
                        request_count.fetch_add(1, Ordering::SeqCst);
                        request_started.add_permits(1);
                        release_request.acquire().await?.forget();
                        let body = serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "message": "injected pod create failure",
                            "reason": "InternalError",
                            "code": 500
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&body)?))?,
                        )
                    }
                    (Method::GET, path) if path.starts_with("/api/v1/namespaces/space/pods/") => {
                        let body = serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "message": "injected missing pod after create failure",
                            "reason": "NotFound",
                            "code": 404
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&body)?))?,
                        )
                    }
                    (method, path) => {
                        let body = serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "message": format!("unexpected mock Kubernetes request: {method} {path}"),
                            "reason": "BadRequest",
                            "code": 400
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&body)?))?,
                        )
                    }
                }
            }
        });
        ClientBuilder::new(service, "space").build()
    }

    fn ambiguous_failed_pod_create_kubernetes_client(post_status: StatusCode) -> Client {
        let post_attempted = Arc::new(AtomicBool::new(false));
        let service = tower::service_fn(move |req: Request<Body>| {
            let post_attempted = Arc::clone(&post_attempted);
            async move {
                let method = req.method().clone();
                let path = req.uri().path().to_string();
                let (status, reason, message) = match (method, path.as_str()) {
                    (Method::POST, "/api/v1/namespaces/space/pods") => {
                        post_attempted.store(true, Ordering::SeqCst);
                        let (reason, message) = if post_status == StatusCode::CONFLICT {
                            ("AlreadyExists", "injected pod create conflict")
                        } else {
                            ("InternalError", "injected ambiguous pod create failure")
                        };
                        (post_status, reason, message)
                    }
                    (Method::GET, path)
                        if path.starts_with("/api/v1/namespaces/space/pods/")
                            && post_attempted.load(Ordering::SeqCst) =>
                    {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "InternalError",
                            "injected failed observation after pod create",
                        )
                    }
                    (Method::GET, path) if path.starts_with("/api/v1/namespaces/space/pods/") => {
                        (StatusCode::NOT_FOUND, "NotFound", "injected missing pod")
                    }
                    _ => (StatusCode::BAD_REQUEST, "BadRequest", "unexpected request"),
                };
                let body = serde_json::json!({
                    "kind": "Status",
                    "apiVersion": "v1",
                    "status": "Failure",
                    "message": message,
                    "reason": reason,
                    "code": status.as_u16()
                });
                Ok::<_, BoxError>(
                    Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&body)?))?,
                )
            }
        });
        ClientBuilder::new(service, "space").build()
    }

    fn blocking_missing_pod_kubernetes_client(
        request_count: Arc<AtomicUsize>,
        request_started: Arc<Semaphore>,
        release_request: Arc<Semaphore>,
    ) -> Client {
        let service = tower::service_fn(move |req: Request<Body>| {
            let request_count = Arc::clone(&request_count);
            let request_started = Arc::clone(&request_started);
            let release_request = Arc::clone(&release_request);
            async move {
                let method = req.method().clone();
                let path = req.uri().path().to_string();
                let (status, message, reason) = match (method, path.as_str()) {
                    (Method::GET, path) if path.starts_with("/api/v1/namespaces/space/pods/") => {
                        request_count.fetch_add(1, Ordering::SeqCst);
                        request_started.add_permits(1);
                        release_request.acquire().await?.forget();
                        (StatusCode::NOT_FOUND, "injected missing pod", "NotFound")
                    }
                    (method, _path) => (
                        StatusCode::BAD_REQUEST,
                        if method == Method::POST {
                            "unexpected pod create"
                        } else {
                            "unexpected Kubernetes request"
                        },
                        "BadRequest",
                    ),
                };
                let body = serde_json::json!({
                    "kind": "Status",
                    "apiVersion": "v1",
                    "status": "Failure",
                    "message": message,
                    "reason": reason,
                    "code": status.as_u16()
                });
                Ok::<_, BoxError>(
                    Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&body)?))?,
                )
            }
        });
        ClientBuilder::new(service, "space").build()
    }

    fn failed_create_then_stale_restore_pod_kubernetes_client(
        deleted_pods: Arc<AtomicUsize>,
    ) -> Client {
        let service = tower::service_fn(move |req: Request<Body>| {
            let deleted_pods = Arc::clone(&deleted_pods);
            async move {
                let method = req.method().clone();
                let path = req.uri().path().to_string();
                match (method, path.as_str()) {
                    (Method::POST, "/api/v1/namespaces/space/pods") => {
                        let body = serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "message": "injected pod create failure",
                            "reason": "InternalError",
                            "code": 500
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&body)?))?,
                        )
                    }
                    (Method::GET, path) if path.starts_with("/api/v1/namespaces/space/pods/") => {
                        let pod_name = path.rsplit('/').next().unwrap_or("space-sbx-stale");
                        let annotations = BTreeMap::from([(
                            SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION.to_string(),
                            "source-node".to_string(),
                        )]);
                        let body = serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "Pod",
                            "metadata": {
                                "name": pod_name,
                                "namespace": "space",
                                "uid": format!("{pod_name}-uid"),
                                "annotations": annotations
                            },
                            "status": {
                                "phase": "Pending"
                            }
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&body)?))?,
                        )
                    }
                    (Method::DELETE, path)
                        if path.starts_with("/api/v1/namespaces/space/pods/") =>
                    {
                        deleted_pods.fetch_add(1, Ordering::SeqCst);
                        let body = serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Success",
                            "code": 200
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&body)?))?,
                        )
                    }
                    (method, path) => {
                        let body = serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "message": format!("unexpected mock Kubernetes request: {method} {path}"),
                            "reason": "BadRequest",
                            "code": 400
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&body)?))?,
                        )
                    }
                }
            }
        });
        ClientBuilder::new(service, "space").build()
    }

    fn replace_existing_pod_kubernetes_client(
        existing_pod: Pod,
        captured_pod_creates: Arc<Mutex<Vec<serde_json::Value>>>,
        deleted_pods: Arc<AtomicUsize>,
    ) -> Client {
        let existing_pod = Arc::new(Mutex::new(Some(
            serde_json::to_value(existing_pod).expect("existing pod should serialize"),
        )));
        let service = tower::service_fn(move |req: Request<Body>| {
            let existing_pod = Arc::clone(&existing_pod);
            let captured_pod_creates = Arc::clone(&captured_pod_creates);
            let deleted_pods = Arc::clone(&deleted_pods);
            async move {
                let method = req.method().clone();
                let path = req.uri().path().to_string();
                match (method, path.as_str()) {
                    (Method::GET, path) if path.starts_with("/api/v1/namespaces/space/pods/") => {
                        if let Some(pod) = existing_pod
                            .lock()
                            .expect("existing pod mutex poisoned")
                            .clone()
                        {
                            return Ok::<_, BoxError>(
                                Response::builder()
                                    .status(StatusCode::OK)
                                    .header("content-type", "application/json")
                                    .body(Body::from(serde_json::to_vec(&pod)?))?,
                            );
                        }
                        let body = serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "reason": "NotFound",
                            "code": 404
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&body)?))?,
                        )
                    }
                    (Method::DELETE, path)
                        if path.starts_with("/api/v1/namespaces/space/pods/") =>
                    {
                        *existing_pod.lock().expect("existing pod mutex poisoned") = None;
                        deleted_pods.fetch_add(1, Ordering::SeqCst);
                        let body = serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Success",
                            "code": 200
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&body)?))?,
                        )
                    }
                    (Method::POST, "/api/v1/namespaces/space/pods") => {
                        let body = req.into_body().collect().await?.to_bytes();
                        let request_pod: serde_json::Value = serde_json::from_slice(&body)?;
                        let pod_name = request_pod
                            .pointer("/metadata/name")
                            .and_then(serde_json::Value::as_str)
                            .ok_or_else(|| {
                                std::io::Error::other(
                                    "created pod request is missing metadata.name",
                                )
                            })?
                            .to_string();
                        captured_pod_creates
                            .lock()
                            .expect("capture mutex poisoned")
                            .push(request_pod);
                        let response_pod = serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "Pod",
                            "metadata": {
                                "name": pod_name,
                                "namespace": "space",
                                "uid": format!("{pod_name}-uid")
                            },
                            "status": {
                                "phase": "Pending"
                            }
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::CREATED)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&response_pod)?))?,
                        )
                    }
                    (method, path) => {
                        let body = serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "message": format!("unexpected mock Kubernetes request: {method} {path}"),
                            "reason": "BadRequest",
                            "code": 400
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&body)?))?,
                        )
                    }
                }
            }
        });
        ClientBuilder::new(service, "space").build()
    }

    pub(crate) fn mock_kubernetes_client(
        captured_pod_creates: Arc<Mutex<Vec<serde_json::Value>>>,
    ) -> Client {
        mock_kubernetes_client_with_pod_create_assertion(captured_pod_creates, |_| {})
    }

    fn mock_kubernetes_client_with_pod_create_assertion(
        captured_pod_creates: Arc<Mutex<Vec<serde_json::Value>>>,
        assert_pod: impl Fn(&serde_json::Value) + Send + Sync + 'static,
    ) -> Client {
        let assert_pod = Arc::new(assert_pod);
        let service = tower::service_fn(move |req: Request<Body>| {
            let captured_pod_creates = Arc::clone(&captured_pod_creates);
            let assert_pod = Arc::clone(&assert_pod);
            async move {
                let method = req.method().clone();
                let path = req.uri().path().to_string();
                match (method, path.as_str()) {
                    (Method::GET, path) if path.starts_with("/api/v1/namespaces/space/pods/") => {
                        let body = serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "reason": "NotFound",
                            "code": 404
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&body)?))?,
                        )
                    }
                    (Method::POST, "/api/v1/namespaces/space/pods") => {
                        let body = req.into_body().collect().await?.to_bytes();
                        let request_pod: serde_json::Value = serde_json::from_slice(&body)?;
                        assert_pod(&request_pod);
                        let pod_name = request_pod
                            .pointer("/metadata/name")
                            .and_then(serde_json::Value::as_str)
                            .ok_or_else(|| {
                                std::io::Error::other(
                                    "created pod request is missing metadata.name",
                                )
                            })?
                            .to_string();
                        captured_pod_creates
                            .lock()
                            .expect("capture mutex poisoned")
                            .push(request_pod);
                        let response_pod = serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "Pod",
                            "metadata": {
                                "name": pod_name,
                                "namespace": "space",
                                "uid": format!("{pod_name}-uid")
                            },
                            "status": {
                                "phase": "Pending"
                            }
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::CREATED)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&response_pod)?))?,
                        )
                    }
                    (method, path) => {
                        let body = serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "message": format!("unexpected mock Kubernetes request: {method} {path}"),
                            "reason": "BadRequest",
                            "code": 400
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&body)?))?,
                        )
                    }
                }
            }
        });
        ClientBuilder::new(service, "space").build()
    }

    fn annotation_value<'a>(
        annotations: &'a serde_json::Map<String, serde_json::Value>,
        key: &str,
    ) -> &'a str {
        annotations
            .get(key)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_else(|| panic!("created pod request is missing annotation {key}"))
    }

    fn assert_restore_pod_request(
        request_pod: &serde_json::Value,
        expected_sandbox_id: SandboxId,
        expected_generation: i64,
        expected_source_node: &str,
    ) {
        let expected_pod_name = format!(
            "space-sbx-{}-g{expected_generation}",
            expected_sandbox_id.as_uuid().simple()
        );
        assert_eq!(
            request_pod
                .pointer("/metadata/name")
                .and_then(serde_json::Value::as_str),
            Some(expected_pod_name.as_str())
        );
        let annotations = request_pod
            .pointer("/metadata/annotations")
            .and_then(serde_json::Value::as_object)
            .expect("created pod request should carry annotations");
        // Golden snapshots pin the rest of the restore-branch annotation set
        // (`sandbox_pod_restore*.snap` in `pplx-space-common-core`), so it is
        // not re-asserted here.
        assert_eq!(
            annotation_value(annotations, SANDBOX_CHECKPOINT_ANNOTATION),
            "suspend",
            "bad pod sent to Kubernetes: expected restore pod, got fresh pod"
        );
        assert_eq!(
            annotation_value(annotations, SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION),
            expected_source_node
        );
    }

    fn conflict_then_created_kubernetes_client(
        captured_pod_creates: Arc<Mutex<Vec<serde_json::Value>>>,
    ) -> Client {
        let create_attempts = Arc::new(Mutex::new(0usize));
        let service = tower::service_fn(move |req: Request<Body>| {
            let captured_pod_creates = Arc::clone(&captured_pod_creates);
            let create_attempts = Arc::clone(&create_attempts);
            async move {
                let method = req.method().clone();
                let path = req.uri().path().to_string();
                match (method, path.as_str()) {
                    (Method::GET, path) if path.starts_with("/api/v1/namespaces/space/pods/") => {
                        let pod_name = path.rsplit('/').next().unwrap_or("space-sbx-conflict");
                        let body = serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "Pod",
                            "metadata": {
                                "name": pod_name,
                                "namespace": "space",
                                "uid": format!("{pod_name}-uid")
                            },
                            "status": {
                                "phase": "Succeeded"
                            }
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&body)?))?,
                        )
                    }
                    (Method::DELETE, path)
                        if path.starts_with("/api/v1/namespaces/space/pods/") =>
                    {
                        let body = serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Success",
                            "code": 200
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&body)?))?,
                        )
                    }
                    (Method::POST, "/api/v1/namespaces/space/pods") => {
                        let body = req.into_body().collect().await?.to_bytes();
                        let request_pod: serde_json::Value = serde_json::from_slice(&body)?;
                        let pod_name = request_pod
                            .pointer("/metadata/name")
                            .and_then(serde_json::Value::as_str)
                            .ok_or_else(|| {
                                std::io::Error::other(
                                    "created pod request is missing metadata.name",
                                )
                            })?
                            .to_string();
                        captured_pod_creates
                            .lock()
                            .expect("capture mutex poisoned")
                            .push(request_pod);

                        let mut attempts = create_attempts.lock().expect("attempt mutex poisoned");
                        let attempt = *attempts;
                        *attempts += 1;
                        drop(attempts);

                        if attempt == 0 {
                            let body = serde_json::json!({
                                "kind": "Status",
                                "apiVersion": "v1",
                                "status": "Failure",
                                "message": "pods already exists",
                                "reason": "AlreadyExists",
                                "code": 409
                            });
                            return Ok::<_, BoxError>(
                                Response::builder()
                                    .status(StatusCode::CONFLICT)
                                    .header("content-type", "application/json")
                                    .body(Body::from(serde_json::to_vec(&body)?))?,
                            );
                        }

                        let response_pod = serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "Pod",
                            "metadata": {
                                "name": pod_name,
                                "namespace": "space",
                                "uid": format!("{pod_name}-uid")
                            },
                            "status": {
                                "phase": "Pending"
                            }
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::CREATED)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&response_pod)?))?,
                        )
                    }
                    (method, path) => {
                        let body = serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "message": format!("unexpected mock Kubernetes request: {method} {path}"),
                            "reason": "BadRequest",
                            "code": 400
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&body)?))?,
                        )
                    }
                }
            }
        });
        ClientBuilder::new(service, "space").build()
    }

    pub(crate) fn running_pod_kubernetes_client(
        captured_pod_creates: Arc<Mutex<Vec<serde_json::Value>>>,
        pod_ip: &'static str,
    ) -> Client {
        running_pod_kubernetes_client_with_create_barrier(
            captured_pod_creates,
            pod_ip,
            None,
            false,
            None,
        )
    }

    fn running_pod_kubernetes_client_with_unconfirmed_delete(
        captured_pod_creates: Arc<Mutex<Vec<serde_json::Value>>>,
        pod_ip: &'static str,
    ) -> Client {
        running_pod_kubernetes_client_with_create_barrier(
            captured_pod_creates,
            pod_ip,
            None,
            true,
            None,
        )
    }

    fn blocking_running_pod_kubernetes_client(
        captured_pod_creates: Arc<Mutex<Vec<serde_json::Value>>>,
        pod_ip: &'static str,
        request_started: Arc<Semaphore>,
        release_request: Arc<Semaphore>,
    ) -> Client {
        running_pod_kubernetes_client_with_create_barrier(
            captured_pod_creates,
            pod_ip,
            Some((request_started, release_request)),
            false,
            None,
        )
    }

    pub(crate) fn existing_then_created_running_pod_kubernetes_client(
        captured_pod_creates: Arc<Mutex<Vec<serde_json::Value>>>,
        existing_pod_name: &str,
    ) -> Client {
        let existing_pod = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": existing_pod_name,
                "namespace": "space",
                "uid": format!("{existing_pod_name}-uid")
            },
            "status": {
                "phase": "Running",
                "podIP": "127.0.0.2",
                "podIPs": [{ "ip": "127.0.0.2" }]
            }
        });
        running_pod_kubernetes_client_with_create_barrier(
            captured_pod_creates,
            "127.0.0.1",
            None,
            false,
            Some(existing_pod),
        )
    }

    pub(crate) fn running_pod_kubernetes_client_with_create_barrier(
        captured_pod_creates: Arc<Mutex<Vec<serde_json::Value>>>,
        pod_ip: &'static str,
        create_barrier: Option<(Arc<Semaphore>, Arc<Semaphore>)>,
        retain_pod_after_delete: bool,
        initial_pod: Option<serde_json::Value>,
    ) -> Client {
        let created_pod = Arc::new(Mutex::new(initial_pod));
        let service = tower::service_fn(move |req: Request<Body>| {
            let captured_pod_creates = Arc::clone(&captured_pod_creates);
            let created_pod = Arc::clone(&created_pod);
            let create_barrier = create_barrier.clone();
            async move {
                let method = req.method().clone();
                let path = req.uri().path().to_string();
                match (method, path.as_str()) {
                    (Method::GET, path) if path.starts_with("/api/v1/namespaces/space/pods/") => {
                        let pod = created_pod
                            .lock()
                            .expect("created pod mutex poisoned")
                            .clone();
                        if let Some(pod) = pod {
                            Ok::<_, BoxError>(
                                Response::builder()
                                    .status(StatusCode::OK)
                                    .header("content-type", "application/json")
                                    .body(Body::from(serde_json::to_vec(&pod)?))?,
                            )
                        } else {
                            let body = serde_json::json!({
                                "kind": "Status",
                                "apiVersion": "v1",
                                "status": "Failure",
                                "reason": "NotFound",
                                "code": 404
                            });
                            Ok::<_, BoxError>(
                                Response::builder()
                                    .status(StatusCode::NOT_FOUND)
                                    .header("content-type", "application/json")
                                    .body(Body::from(serde_json::to_vec(&body)?))?,
                            )
                        }
                    }
                    (Method::POST, "/api/v1/namespaces/space/pods") => {
                        if let Some((request_started, release_request)) = create_barrier {
                            request_started.add_permits(1);
                            release_request.acquire().await?.forget();
                        }
                        let body = req.into_body().collect().await?.to_bytes();
                        let request_pod: serde_json::Value = serde_json::from_slice(&body)?;
                        let pod_name = request_pod
                            .pointer("/metadata/name")
                            .and_then(serde_json::Value::as_str)
                            .ok_or_else(|| {
                                std::io::Error::other(
                                    "created pod request is missing metadata.name",
                                )
                            })?
                            .to_string();
                        captured_pod_creates
                            .lock()
                            .expect("capture mutex poisoned")
                            .push(request_pod);
                        let response_pod = serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "Pod",
                            "metadata": {
                                "name": pod_name,
                                "namespace": "space",
                                "uid": format!("{pod_name}-uid")
                            },
                            "status": {
                                "phase": "Running",
                                "podIP": pod_ip,
                                "podIPs": [{ "ip": pod_ip }]
                            }
                        });
                        *created_pod.lock().expect("created pod mutex poisoned") =
                            Some(response_pod.clone());
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::CREATED)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&response_pod)?))?,
                        )
                    }
                    (Method::DELETE, path)
                        if path.starts_with("/api/v1/namespaces/space/pods/") =>
                    {
                        if !retain_pod_after_delete {
                            *created_pod.lock().expect("created pod mutex poisoned") = None;
                        }
                        let body = serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Success",
                            "code": 200
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&body)?))?,
                        )
                    }
                    (method, path) => {
                        let body = serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "message": format!("unexpected mock Kubernetes request: {method} {path}"),
                            "reason": "BadRequest",
                            "code": 400
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&body)?))?,
                        )
                    }
                }
            }
        });
        ClientBuilder::new(service, "space").build()
    }

    fn existing_running_pod_kubernetes_client(
        pod_name: &'static str,
        pod_ip: &'static str,
        pod_gets: Arc<AtomicUsize>,
    ) -> Client {
        let service = tower::service_fn(move |req: Request<Body>| {
            let pod_gets = Arc::clone(&pod_gets);
            async move {
                let method = req.method().clone();
                let path = req.uri().path().to_string();
                match (method, path.as_str()) {
                    (Method::GET, path) if path.ends_with(pod_name) => {
                        pod_gets.fetch_add(1, Ordering::SeqCst);
                        let pod = serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "Pod",
                            "metadata": {
                                "name": pod_name,
                                "namespace": "space",
                                "uid": format!("{pod_name}-uid")
                            },
                            "status": {
                                "phase": "Running",
                                "podIP": pod_ip,
                                "podIPs": [{ "ip": pod_ip }]
                            }
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&pod)?))?,
                        )
                    }
                    (method, path) => {
                        let body = serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "message": format!("unexpected mock Kubernetes request: {method} {path}"),
                            "reason": "BadRequest",
                            "code": 400
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&body)?))?,
                        )
                    }
                }
            }
        });
        ClientBuilder::new(service, "space").build()
    }

    fn deleted_pod_kubernetes_client(
        pod_name: &'static str,
        pod_uid: &'static str,
        delete_attempts: Arc<AtomicUsize>,
    ) -> Client {
        let service = tower::service_fn(move |req: Request<Body>| {
            let delete_attempts = Arc::clone(&delete_attempts);
            async move {
                let method = req.method().clone();
                let path = req.uri().path().to_string();
                let expected_path = format!("/api/v1/namespaces/space/pods/{pod_name}");
                match (method, path.as_str()) {
                    (Method::DELETE, path) if path == expected_path => {
                        let body = req.into_body().collect().await?.to_bytes();
                        let delete: serde_json::Value = serde_json::from_slice(&body)?;
                        assert_eq!(
                            delete
                                .pointer("/preconditions/uid")
                                .and_then(|uid| uid.as_str()),
                            Some(pod_uid),
                            "suspend handoff must delete only the exact pod UID"
                        );
                        delete_attempts.fetch_add(1, Ordering::SeqCst);
                        let body = serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Success",
                            "code": 200
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&body)?))?,
                        )
                    }
                    (Method::GET, path) if path == expected_path => {
                        let body = serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "message": format!("pods \"{pod_name}\" not found"),
                            "reason": "NotFound",
                            "code": 404
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&body)?))?,
                        )
                    }
                    (method, path) => {
                        let body = serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "message": format!("unexpected mock Kubernetes request: {method} {path}"),
                            "reason": "BadRequest",
                            "code": 400
                        });
                        Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&body)?))?,
                        )
                    }
                }
            }
        });
        ClientBuilder::new(service, "space").build()
    }

    fn substrate_with_unreachable_store() -> KubernetesSubstrate {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_millis(1))
            .connect_lazy("postgres://postgres@127.0.0.1:1/postgres")
            .expect("lazy pool should parse");
        substrate_with_store(pool)
    }

    fn substrate_with_store(pool: PgPool) -> KubernetesSubstrate {
        substrate_with_store_and_client(pool, inert_kubernetes_client())
    }

    pub(crate) fn substrate_with_store_and_client(
        pool: PgPool,
        client: Client,
    ) -> KubernetesSubstrate {
        substrate_with_store_client_and_proxy_port(pool, client, DEFAULT_SANDBOX_PROXY_PORT)
    }

    fn substrate_with_store_client_and_proxy_port(
        pool: PgPool,
        client: Client,
        sandbox_proxy_port: u16,
    ) -> KubernetesSubstrate {
        substrate_with_store_client_proxy_port_and_pod(pool, client, sandbox_proxy_port, None)
    }

    pub(crate) fn substrate_with_store_client_proxy_port_and_pod(
        pool: PgPool,
        client: Client,
        sandbox_proxy_port: u16,
        initial_pod: Option<Pod>,
    ) -> KubernetesSubstrate {
        substrate_with_store_client_proxy_port_pod_and_nodes(
            pool,
            client,
            sandbox_proxy_port,
            initial_pod,
            Vec::new(),
        )
    }

    fn substrate_with_store_client_proxy_port_pod_and_nodes(
        pool: PgPool,
        client: Client,
        sandbox_proxy_port: u16,
        initial_pod: Option<Pod>,
        nodes: Vec<Node>,
    ) -> KubernetesSubstrate {
        let shutdown_token = CancellationToken::new();
        let task_tracker = tokio_util::task::TaskTracker::new();
        let (pod_store, mut pod_writer) = reflector::store::<Pod>();
        if let Some(pod) = initial_pod {
            use kube::runtime::watcher::Event;
            pod_writer.apply_watcher_event(&Event::Init);
            pod_writer.apply_watcher_event(&Event::InitApply(pod));
            pod_writer.apply_watcher_event(&Event::InitDone);
        }
        let (node_store, mut node_writer) = reflector::store::<Node>();
        node_writer.apply_watcher_event(&Event::Init);
        for node in nodes {
            node_writer.apply_watcher_event(&Event::InitApply(node));
        }
        node_writer.apply_watcher_event(&Event::InitDone);
        let caches = Arc::new(K8sCaches::for_test(pod_store.clone(), node_store, true));

        KubernetesSubstrate {
            k8s: KubernetesState {
                client,
                sandbox_namespace: "space".to_string(),
                sandbox_proxy_port,
                node_api_port: sandbox_proxy_port,
                sandbox_pod_config: SandboxPodConfigValues::default(),
                create_command_ready_timeout: Duration::from_secs(1),
                create_data_plane_ready_timeout: Duration::from_secs(1),
                k8s_blocking_delete_timeout: Duration::from_secs(1),
                caches,
            },
            restore_waiters: RestoreWaiters::new(
                K8sStore::new(pool.clone(), "space".to_string()),
                shutdown_token.clone(),
                task_tracker.clone(),
            ),
            wake_coalescer: Arc::new(
                pplx_space_common_work_coalescer::WorkCoalescer::with_tracker(task_tracker.clone()),
            ),
            store: K8sStore::new(pool.clone(), "space".to_string()),
            pod_store,
            pod_events: pod_reflector::PodEvents::for_test(),
            pod_reflector_ready: ReflectorReady::always_ready(),
            cancel_tracker: Arc::new(CancelRateTracker::with_budget(64)),
            // Gate closed: these tests exercise pod lifecycle, not placement,
            // and a closed gate issues no query — so the lazily-connected pool
            // stays undialed. Placement rendering is covered by the pod-builder
            // snapshots; resolution by `node_group_placement`'s own tests.
            node_group_placement: NodeGroupPlacementResolver::new(pool, false),
            recovery_join_backoff: RecoveryJoinBackoffPolicy::default(),
            shutdown_token,
            task_tracker,
        }
    }

    fn test_worker_node_id() -> NodeId {
        pplx_space_common_svc::types::k8s::space_k8s_node_id("node-a", "worker")
    }

    async fn start_sim_worker() -> Result<SimWorkerHarness, BoxError> {
        Ok(SimWorkerHarness::builder("node-a")
            .with_internal_api()
            .start()
            .await?)
    }

    struct SequencedWakeWorker {
        wake_attempts: Arc<AtomicUsize>,
        succeed_after_attempt: Option<usize>,
        echo_route_identity: bool,
    }

    #[tonic::async_trait]
    impl WorkerApiInternal for SequencedWakeWorker {
        async fn adopt_pool_sandbox(
            &self,
            _request: tonic::Request<pplx_space_sbx_mgr_client::proto::AdoptPoolSandboxRequest>,
        ) -> Result<
            tonic::Response<pplx_space_sbx_mgr_client::proto::AdoptPoolSandboxResponse>,
            tonic::Status,
        > {
            Err(tonic::Status::unimplemented("not used by this test"))
        }

        async fn wake_sandbox(
            &self,
            request: tonic::Request<pplx_space_sbx_mgr_client::proto::WakeRequest>,
        ) -> Result<tonic::Response<pplx_space_sbx_mgr_client::proto::WakeResponse>, tonic::Status>
        {
            let request = request.into_inner();
            let attempt = self.wake_attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if self
                .succeed_after_attempt
                .is_none_or(|succeed_after| attempt < succeed_after)
            {
                return Err(tonic::Status::failed_precondition(
                    pplx_space_common_core::wake_signals::MISSING_RUNTIME_CONTEXT_MARKER,
                ));
            }
            Ok(tonic::Response::new(
                pplx_space_sbx_mgr_client::proto::WakeResponse {
                    sandbox_id: request.sandbox_id,
                    transition: pplx_space_sbx_mgr_client::proto::WakeTransition::Noop as i32,
                    route_identity: self
                        .echo_route_identity
                        .then(|| {
                            request
                                .start_context
                                .and_then(|context| context.route_identity)
                                .map(|mut identity| {
                                    identity.current_node_id =
                                        Some(test_worker_node_id().to_string());
                                    identity
                                })
                        })
                        .flatten(),
                },
            ))
        }
    }

    // `SequencedWakeWorker` is a custom `WorkerApiInternal` test double (not
    // the `pplx-space-node-api-sim` simulator), so it keeps its own minimal
    // spawn/shutdown scaffolding rather than going through
    // `SimWorkerHarness`.
    struct SequencedWakeWorkerHandle {
        port: u16,
        health_reporter: tonic_health::server::HealthReporter,
        shutdown: CancellationToken,
        task: JoinHandle<()>,
        _dir: tempfile::TempDir,
    }

    impl Drop for SequencedWakeWorkerHandle {
        fn drop(&mut self) {
            self.shutdown.cancel();
            self.task.abort();
        }
    }

    async fn start_sequenced_wake_worker(
        wake_attempts: Arc<AtomicUsize>,
        succeed_after_attempt: Option<usize>,
        echo_route_identity: bool,
    ) -> Result<SequencedWakeWorkerHandle, BoxError> {
        start_sequenced_wake_worker_with_capability(
            wake_attempts,
            succeed_after_attempt,
            echo_route_identity,
            true,
        )
        .await
    }

    async fn start_sequenced_wake_worker_with_capability(
        wake_attempts: Arc<AtomicUsize>,
        succeed_after_attempt: Option<usize>,
        echo_route_identity: bool,
        advertise_operation_receipts: bool,
    ) -> Result<SequencedWakeWorkerHandle, BoxError> {
        let dir = tempfile::tempdir()?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let sim_state = SimState::new(
            dir.path().join("snapshots"),
            dir.path().join("sandboxes"),
            "node-a".to_string(),
        );
        let worker_service = SimWorkerApiService::new(sim_state);
        let service = SequencedWakeWorker {
            wake_attempts,
            succeed_after_attempt,
            echo_route_identity,
        };
        let (health_reporter, health_service) = pplx_space_common_svc::grpc_health_service();
        health_reporter
            .set_service_status(
                pplx_space_sbx_mgr_client::ACTIVATION_POLICY_HEALTH_SERVICE,
                tonic_health::ServingStatus::Serving,
            )
            .await;
        if advertise_operation_receipts {
            health_reporter
                .set_service_status(
                    pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE,
                    tonic_health::ServingStatus::Serving,
                )
                .await;
        }
        let task = tokio::spawn(async move {
            let result = Server::builder()
                .add_service(health_service)
                .add_service(WorkerApiServer::new(worker_service))
                .add_service(WorkerApiInternalServer::new(service))
                .serve_with_incoming_shutdown(
                    TcpIncoming::from(listener).with_nodelay(Some(true)),
                    server_shutdown.cancelled_owned(),
                )
                .await;
            if let Err(error) = result {
                tracing::warn!(%error, "test worker server exited with error");
            }
        });
        Ok(SequencedWakeWorkerHandle {
            port,
            health_reporter,
            shutdown,
            task,
            _dir: dir,
        })
    }

    #[tokio::test]
    async fn split_operation_producer_does_not_call_uncapable_node_api() -> Result<(), BoxError> {
        if !crate::test_support::enter_sim_worker_process() {
            return Ok(());
        }

        let wake_attempts = Arc::new(AtomicUsize::new(0));
        let worker = start_sequenced_wake_worker_with_capability(
            Arc::clone(&wake_attempts),
            Some(1),
            true,
            false,
        )
        .await?;

        let error = KubernetesSubstrate::pod_worker_internal_client(
            PodIp::parse("127.0.0.1")?,
            worker.port,
            Duration::from_secs(1),
            true,
        )
        .await
        .expect_err("old node-api must be a named retryable no-send outcome");

        assert!(matches!(
            error,
            SubstrateError::Unavailable { message }
                if message.contains("operation-receipt capability check failed")
        ));
        assert_eq!(wake_attempts.load(Ordering::SeqCst), 0);

        let compatible_without_split = KubernetesSubstrate::pod_worker_internal_client(
            PodIp::parse("127.0.0.1")?,
            worker.port,
            Duration::from_secs(1),
            false,
        )
        .await?;
        assert!(
            compatible_without_split.is_some(),
            "B/B initialization does not depend on the split-receipt capability"
        );
        Ok(())
    }

    fn start_process_request(sandbox_id: SandboxId) -> SpacedStartProcessRequest {
        SpacedStartProcessRequest {
            sandbox_id: sandbox_id.to_string(),
            operation: Some(spaced_ops::StartRequest {
                command: vec!["true".to_string()],
                ..Default::default()
            }),
        }
    }

    async fn insert_test_sandbox_row(
        pool: &PgPool,
        sandbox_id: SandboxId,
        lifecycle: &str,
    ) -> Result<(), BoxError> {
        sqlx::query(
            "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, placement, managed_by, network_policy_override_json, network_policy_generation)
             VALUES (
                $1,
                '00000000-0000-0000-0000-000000000010',
                '00000000-0000-0000-0000-000000000001',
                $2,
                'on_node',
                'k8s',
                $3,
                1
             )",
        )
        .bind(sandbox_id)
        .bind(lifecycle)
        .bind(sqlx::types::Json(
            &pplx_space_net_common::NetworkPolicyV1::default(),
        ))
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn insert_suspending_finalization_gap(
        pool: &PgPool,
        sandbox_id: SandboxId,
        pod_name: &str,
    ) -> Result<(), BoxError> {
        insert_test_sandbox_row(pool, sandbox_id, "suspending").await?;
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(Uuid::now_v7())
        .execute(pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active)
             VALUES
                 ($1, 1, $2, 'space', 'pod-uid-g1', 'suspended', FALSE)"
        ))
        .bind(sandbox_id)
        .bind(pod_name)
        .execute(pool)
        .await?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn suspend_handoff_accepts_projection_before_pod_retirement() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let typed_sandbox_id = sandbox_id;
        let generation = ExecutionGeneration::new(1)?;
        let pod_name = "space-sbx-projected-suspend-g1";
        let pod_uid = PodUid::parse("projected-suspend-pod-uid")?;
        let published_artifact = PublishedLocalArtifact {
            id: LocalArtifactId::new(),
            kind: ArtifactKind::Full,
        };

        insert_test_sandbox_row(&pool, sandbox_id, "suspended").await?;
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET placement = 'unplaced',
                    current_local_artifact_id = $2,
                    current_local_artifact_kind = $3,
                    current_local_artifact_origin = $4
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(published_artifact.id.as_uuid())
        .bind(published_artifact.kind.as_str())
        .bind(ArtifactOrigin::Suspend.as_str())
        .execute(&pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active)
             VALUES ($1, $2, $3, 'space', $4, 'suspended', FALSE)"
        ))
        .bind(sandbox_id)
        .bind(generation.get())
        .bind(pod_name)
        .bind(pod_uid.as_str())
        .execute(&pool)
        .await?;

        let delete_attempts = Arc::new(AtomicUsize::new(0));
        let mut substrate = substrate_with_store_and_client(
            pool.clone(),
            deleted_pod_kubernetes_client(
                pod_name,
                "projected-suspend-pod-uid",
                Arc::clone(&delete_attempts),
            ),
        );
        substrate.k8s.k8s_blocking_delete_timeout = Duration::from_millis(10);

        let mismatched_artifact = PublishedLocalArtifact {
            id: LocalArtifactId::new(),
            kind: ArtifactKind::Full,
        };
        assert!(matches!(
            substrate
                .finish_suspend_handoff(
                    typed_sandbox_id,
                    generation,
                    pod_uid.clone(),
                    pod_name.to_string(),
                    Some(mismatched_artifact),
                )
                .await,
            Err(SubstrateError::Unavailable { .. })
        ));
        assert_eq!(
            delete_attempts.load(Ordering::SeqCst),
            0,
            "artifact mismatch must fail before pod retirement"
        );

        let returned_artifact = substrate
            .finish_suspend_handoff(
                typed_sandbox_id,
                generation,
                pod_uid,
                pod_name.to_string(),
                Some(published_artifact),
            )
            .await?;

        assert_eq!(returned_artifact, published_artifact);
        assert_eq!(delete_attempts.load(Ordering::SeqCst), 1);
        let lifecycle: String =
            sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
                .bind(sandbox_id)
                .fetch_one(&pool)
                .await?;
        assert_eq!(lifecycle, SandboxLifecycle::Suspended.as_str());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn suspend_handoff_accepts_projection_between_lookup_and_commit() -> Result<(), BoxError>
    {
        let db = pplx_space_db_test_fixtures::pg_test_db_with_max_connections(4).await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let typed_sandbox_id = sandbox_id;
        let generation = ExecutionGeneration::new(1)?;
        let pod_name = "space-sbx-concurrent-projected-suspend-g1";
        let pod_uid = PodUid::parse("concurrent-projected-suspend-pod-uid")?;
        let published_artifact = PublishedLocalArtifact {
            id: LocalArtifactId::new(),
            kind: ArtifactKind::Full,
        };

        insert_test_sandbox_row(&pool, sandbox_id, "suspending").await?;
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active)
             VALUES ($1, $2, $3, 'space', $4, 'running', TRUE)"
        ))
        .bind(sandbox_id)
        .bind(generation.get())
        .bind(pod_name)
        .bind(pod_uid.as_str())
        .execute(&pool)
        .await?;

        let delete_attempts = Arc::new(AtomicUsize::new(0));
        let mut substrate = substrate_with_store_and_client(
            pool.clone(),
            deleted_pod_kubernetes_client(
                pod_name,
                "concurrent-projected-suspend-pod-uid",
                Arc::clone(&delete_attempts),
            ),
        );
        substrate.k8s.k8s_blocking_delete_timeout = Duration::from_millis(10);

        let mut projector = pool.begin().await?;
        sqlx::query(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
            .bind(sandbox_id)
            .fetch_one(&mut *projector)
            .await?;

        let handoff = tokio::spawn(async move {
            substrate
                .finish_suspend_handoff(
                    typed_sandbox_id,
                    generation,
                    pod_uid,
                    pod_name.to_string(),
                    Some(published_artifact),
                )
                .await
        });
        let handoff_waiting_on_sandbox_lock = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if handoff.is_finished() {
                    return Ok::<_, sqlx::Error>(false);
                }
                let waiting = sqlx::query_scalar::<_, bool>(sql!(
                    "SELECT EXISTS (
                             SELECT 1
                               FROM pg_stat_activity
                              WHERE datname = current_database()
                                AND wait_event_type = 'Lock'
                                AND query LIKE '%SELECT id%FROM sandboxes%FOR UPDATE%'
                         )"
                ))
                .fetch_one(&pool)
                .await?;
                if waiting {
                    return Ok::<_, sqlx::Error>(true);
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
        assert!(
            handoff_waiting_on_sandbox_lock,
            "suspend handoff must reach the row-locked commit before projection"
        );

        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET state = 'suspended', active = FALSE, updated_at = NOW()
              WHERE sandbox_id = $1 AND generation = $2"
        ))
        .bind(sandbox_id)
        .bind(generation.get())
        .execute(&mut *projector)
        .await?;
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET lifecycle = 'suspended',
                    placement = 'unplaced',
                    current_local_artifact_id = $2,
                    current_local_artifact_kind = $3,
                    current_local_artifact_origin = $4
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(published_artifact.id.as_uuid())
        .bind(published_artifact.kind.as_str())
        .bind(ArtifactOrigin::Suspend.as_str())
        .execute(&mut *projector)
        .await?;
        projector.commit().await?;

        let returned_artifact = handoff.await??;
        assert_eq!(returned_artifact, published_artifact);
        assert_eq!(delete_attempts.load(Ordering::SeqCst), 1);
        Ok(())
    }

    pub(crate) async fn insert_ready_full_snapshot_relative_to_lifecycle(
        pool: &PgPool,
        sandbox_id: SandboxId,
        storage_uri: &str,
        timestamp_offset_secs: i64,
        artifact_id: Uuid,
    ) -> Result<(), BoxError> {
        let snapshot_id = SnapshotId::new();
        sqlx::query(
            "INSERT INTO snapshots
                 (id, sandbox_id, type, state, timestamp, org_id,
                  local_artifact_id, artifact_kind, artifact_origin)
             VALUES (
                 $1,
                 $2,
                 'full',
                 'ready',
                 (SELECT lifecycle_changed_at + make_interval(secs => $3::BIGINT)
                    FROM sandboxes WHERE id = $2),
                 '00000000-0000-0000-0000-000000000001',
                 $4,
                 'full',
                 'suspend'
             )",
        )
        .bind(snapshot_id)
        .bind(sandbox_id)
        .bind(timestamp_offset_secs)
        .bind(artifact_id)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO snapshot_storage_locations
                 (snapshot_id, storage_connection_string, org_id)
             VALUES ($1, $2, '00000000-0000-0000-0000-000000000001')",
        )
        .bind(snapshot_id)
        .bind(storage_uri)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn paused_restore_test_subject(
        pool: &PgPool,
        sandbox_id: SandboxId,
        succeed_after_attempt: Option<usize>,
        echo_route_identity: bool,
    ) -> Result<
        (
            KubernetesSubstrate,
            Arc<AtomicUsize>,
            Arc<AtomicUsize>,
            SequencedWakeWorkerHandle,
        ),
        BoxError,
    > {
        insert_test_sandbox_row(pool, sandbox_id, "suspended").await?;
        sqlx::query(
            "UPDATE sandboxes
                SET evacuation_restore_target = 'paused'
              WHERE id = $1",
        )
        .bind(sandbox_id)
        .execute(pool)
        .await?;
        insert_ready_full_snapshot_relative_to_lifecycle(
            pool,
            sandbox_id,
            "s3://space-test/suspend-snapshots/paused?region=us-east-1",
            1,
            Uuid::now_v7(),
        )
        .await?;
        let node_id = test_worker_node_id();
        sqlx::query(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '127.0.0.1', 'worker', 'k8s', 'ready')",
        )
        .bind(node_id)
        .execute(pool)
        .await?;
        sqlx::query("UPDATE sandboxes SET active_node_id = $2 WHERE id = $1")
            .bind(sandbox_id)
            .bind(node_id)
            .execute(pool)
            .await?;
        let pod_name = "space-sbx-paused-g1";
        let route_activation_id = SandboxActivationAttemptId::new();
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip, state, active,
                  route_activation_id)
             VALUES
                 ($1, 1, $2, 'space', $3, '127.0.0.1', 'running', TRUE, $4)",
        )
        .bind(sandbox_id)
        .bind(pod_name)
        .bind(format!("{pod_name}-uid"))
        .bind(route_activation_id)
        .execute(pool)
        .await?;
        let initial_pod = Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(pod_name.to_string()),
                namespace: Some("space".to_string()),
                uid: Some(format!("{pod_name}-uid")),
                ..Default::default()
            },
            status: Some(PodStatus {
                phase: Some("Running".to_string()),
                pod_ip: Some("127.0.0.1".to_string()),
                pod_ips: Some(vec![PodIP {
                    ip: "127.0.0.1".to_string(),
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let wake_attempts = Arc::new(AtomicUsize::new(0));
        let pod_gets = Arc::new(AtomicUsize::new(0));
        let worker = start_sequenced_wake_worker(
            Arc::clone(&wake_attempts),
            succeed_after_attempt,
            echo_route_identity,
        )
        .await?;
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            existing_running_pod_kubernetes_client(pod_name, "127.0.0.1", Arc::clone(&pod_gets)),
            worker.port,
            Some(initial_pod),
        );
        Ok((substrate, wake_attempts, pod_gets, worker))
    }

    #[rstest::rstest]
    #[case(SandboxLifecycle::Paused)]
    #[case(SandboxLifecycle::Started)]
    #[tokio::test(flavor = "current_thread")]
    async fn resident_restore_joins_shim_after_missing_context_wake(
        #[case] target: SandboxLifecycle,
    ) -> Result<(), BoxError> {
        if !crate::test_support::enter_sim_worker_process() {
            return Ok(());
        }

        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let (substrate, wake_attempts, pod_gets, _worker) =
            paused_restore_test_subject(&pool, sandbox_id, Some(2), true).await?;

        // Act.
        let result = substrate
            .restore_evacuated_execution(
                SandboxApiMetricIdentity::ecw("evacuation"),
                sandbox_id,
                ActivationOptions {
                    blocking: true,
                    ready_timeout: Duration::from_secs(2),
                },
                None,
                target,
            )
            .await;

        // Assert.
        let wake_attempts = wake_attempts.load(Ordering::SeqCst);
        assert!(
            result.is_ok(),
            "paused restore must join the shim wake instead of rolling back after {wake_attempts} \
             attempts: {result:?}"
        );
        assert_eq!(
            wake_attempts, 2,
            "the API should retry once to observe the shim-completed paused wake"
        );
        assert_eq!(
            pod_gets.load(Ordering::SeqCst),
            2,
            "materialization plus Wake may each resolve the Pod once, but Wake retries and command readiness must share their authoritative selection"
        );
        let active_generation: i64 = sqlx::query_scalar(
            "SELECT generation
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(active_generation, 1, "the restore pod must not be replaced");

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn split_operation_waits_for_transient_receipt_capability() -> Result<(), BoxError> {
        if !crate::test_support::enter_sim_worker_process() {
            return Ok(());
        }

        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let sandbox_id = SandboxId::new();
        let (substrate, wake_attempts, _pod_gets, worker) =
            paused_restore_test_subject(&db.pool, sandbox_id, Some(1), true).await?;
        worker
            .health_reporter
            .set_service_status(
                pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE,
                tonic_health::ServingStatus::NotServing,
            )
            .await;
        let health_reporter = worker.health_reporter.clone();
        let capability = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            health_reporter
                .set_service_status(
                    pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE,
                    tonic_health::ServingStatus::Serving,
                )
                .await;
        });

        let result = substrate
            .restore_evacuated_execution(
                SandboxApiMetricIdentity::ecw("evacuation"),
                sandbox_id,
                ActivationOptions {
                    blocking: true,
                    ready_timeout: Duration::from_secs(2),
                },
                None,
                SandboxLifecycle::Paused,
            )
            .await;
        capability.await?;

        assert!(result.is_ok(), "transient health must retry: {result:?}");
        assert_eq!(wake_attempts.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn split_operation_capability_deadline_is_a_clean_no_send_failure() -> Result<(), BoxError>
    {
        if !crate::test_support::enter_sim_worker_process() {
            return Ok(());
        }

        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let sandbox_id = SandboxId::new();
        let (substrate, wake_attempts, _pod_gets, worker) =
            paused_restore_test_subject(&db.pool, sandbox_id, Some(1), false).await?;
        worker
            .health_reporter
            .set_service_status(
                pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE,
                tonic_health::ServingStatus::NotServing,
            )
            .await;

        let error = substrate
            .restore_evacuated_execution(
                SandboxApiMetricIdentity::ecw("evacuation"),
                sandbox_id,
                ActivationOptions {
                    blocking: true,
                    // The deadline also covers the Postgres work before the
                    // first capability probe. A budget that expires first
                    // fails through the generic no-send path instead.
                    ready_timeout: Duration::from_secs(2),
                },
                None,
                SandboxLifecycle::Paused,
            )
            .await
            .expect_err("a capability that never becomes ready must fail before Wake");

        assert!(
            matches!(
                error,
                SubstrateError::Unavailable { ref message }
                    if message.contains("does not advertise split operation-receipt support")
            ),
            "the capability probe must be the reason Wake was not sent: {error:?}"
        );
        assert_eq!(wake_attempts.load(Ordering::SeqCst), 0);
        let lifecycle: String = sqlx::query_scalar("SELECT lifecycle FROM sandboxes WHERE id = $1")
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
        assert_eq!(
            lifecycle, "suspended",
            "a provable no-send failure is safe to roll back"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn legacy_paused_restore_preserves_handoff_without_route_identity() -> Result<(), BoxError>
    {
        if !crate::test_support::enter_sim_worker_process() {
            return Ok(());
        }

        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let (substrate, wake_attempts, _pod_gets, _worker) =
            paused_restore_test_subject(&pool, sandbox_id, Some(1), false).await?;
        sqlx::query(
            "UPDATE sandbox_k8s_executions SET route_activation_id = NULL WHERE sandbox_id = $1",
        )
        .bind(sandbox_id)
        .execute(&pool)
        .await?;

        let error = substrate
            .restore_evacuated_execution(
                SandboxApiMetricIdentity::ecw("evacuation"),
                sandbox_id,
                ActivationOptions {
                    blocking: true,
                    ready_timeout: Duration::from_secs(2),
                },
                None,
                SandboxLifecycle::Paused,
            )
            .await
            .expect_err("a successful Wake without identity evidence must not complete");

        assert!(matches!(
            error,
            SubstrateError::Timeout { message, .. }
                if message.contains("omitted the authoritative route identity")
        ));
        assert_eq!(wake_attempts.load(Ordering::SeqCst), 1);
        let lifecycle: String = sqlx::query_scalar("SELECT lifecycle FROM sandboxes WHERE id = $1")
            .bind(sandbox_id)
            .fetch_one(&pool)
            .await?;
        assert_eq!(
            lifecycle, "restoring",
            "a successful Wake with unusable evidence is not safe to roll back"
        );
        let (route, operation): (Option<Uuid>, Option<Uuid>) = sqlx::query_as(
            "SELECT route_activation_id, activation_attempt_id
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(route, None, "unacknowledged identity must not be published");
        assert!(
            operation.is_some(),
            "a dispatched Wake must retain its operation owner for a delayed receipt"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn paused_restore_requires_existing_route_acknowledgement() -> Result<(), BoxError> {
        if !crate::test_support::enter_sim_worker_process() {
            return Ok(());
        }

        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let sandbox_id = SandboxId::new();
        let (substrate, wake_attempts, _pod_gets, _worker) =
            paused_restore_test_subject(&db.pool, sandbox_id, Some(1), false).await?;

        let error = substrate
            .restore_evacuated_execution(
                SandboxApiMetricIdentity::ecw("evacuation"),
                sandbox_id,
                ActivationOptions {
                    blocking: true,
                    ready_timeout: Duration::from_secs(2),
                },
                None,
                SandboxLifecycle::Paused,
            )
            .await
            .expect_err("Paused has no command-readiness fallback for missing route evidence");

        assert!(matches!(
            error,
            SubstrateError::Timeout { message, .. }
                if message.contains("omitted the authoritative route identity")
        ));
        assert_eq!(wake_attempts.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[test]
    fn only_a_proven_no_send_wake_failure_may_release_the_operation() {
        let failure = |dispatch| ActivationWakeFailure {
            error: SubstrateError::Unavailable {
                message: "test".to_string(),
            },
            dispatch,
        };
        assert!(
            failure(WakeDispatchStatus::NotDispatched).operation_release_is_safe(),
            "pre-dispatch failure has no node-side work or receipt to preserve"
        );
        assert!(
            !failure(WakeDispatchStatus::MayHaveLanded).operation_release_is_safe(),
            "post-dispatch uncertainty must retain ownership for delayed convergence"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dormant_access_preserves_operation_after_dispatched_wake_failure()
    -> Result<(), BoxError> {
        if !crate::test_support::enter_sim_worker_process() {
            return Ok(());
        }

        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let sandbox_id = SandboxId::new();
        let (substrate, wake_attempts, _pod_gets, _worker) =
            paused_restore_test_subject(&db.pool, sandbox_id, Some(1), false).await?;
        sqlx::query(
            "UPDATE sandboxes
                SET lifecycle = 'paused',
                    auto_lifecycle_json = '{\"wake\":{\"on\":[\"access\"]}}'::jsonb
              WHERE id = $1",
        )
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;
        sqlx::query(
            "UPDATE sandbox_k8s_executions
                SET route_activation_id = NULL
              WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;
        let sandbox = fetch_sandbox(&db.pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");

        substrate
            .access_sandbox_ip(
                SandboxApiMetricIdentity::ecw("connect"),
                &sandbox,
                Duration::from_secs(2),
                crate::controllers::substrate::RecoveryCharge::Exempt,
            )
            .await
            .expect_err("missing route acknowledgement must fail dormant access");

        assert_eq!(wake_attempts.load(Ordering::SeqCst), 1);
        let (route, operation): (Option<Uuid>, Option<Uuid>) = sqlx::query_as(
            "SELECT route_activation_id, activation_attempt_id
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(route, None, "unacknowledged identity must not be published");
        assert!(
            operation.is_some(),
            "dormant access must retain the operation owner for a delayed receipt"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn started_restore_does_not_use_command_readiness_as_identity_evidence()
    -> Result<(), BoxError> {
        if !crate::test_support::enter_sim_worker_process() {
            return Ok(());
        }

        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let (substrate, wake_attempts, _pod_gets, _worker) =
            paused_restore_test_subject(&pool, sandbox_id, Some(1), false).await?;
        sqlx::query(
            "UPDATE sandbox_k8s_executions SET route_activation_id = NULL WHERE sandbox_id = $1",
        )
        .bind(sandbox_id)
        .execute(&pool)
        .await?;

        let error = substrate
            .restore_evacuated_execution(
                SandboxApiMetricIdentity::ecw("evacuation"),
                sandbox_id,
                ActivationOptions {
                    blocking: true,
                    ready_timeout: Duration::from_secs(2),
                },
                None,
                SandboxLifecycle::Started,
            )
            .await
            .expect_err("command readiness must not replace the missing route acknowledgement");

        assert!(matches!(
            error,
            SubstrateError::Timeout { message, .. }
                if message.contains("omitted the authoritative route identity")
        ));
        assert_eq!(wake_attempts.load(Ordering::SeqCst), 1);
        let lifecycle: String = sqlx::query_scalar("SELECT lifecycle FROM sandboxes WHERE id = $1")
            .bind(sandbox_id)
            .fetch_one(&pool)
            .await?;
        assert_eq!(
            lifecycle, "restoring",
            "command readiness cannot make an acknowledged-but-unproven Wake safe to roll back"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn route_ack_join_uses_phase_deadline_without_clean_rollback() -> Result<(), BoxError> {
        if !crate::test_support::enter_sim_worker_process() {
            return Ok(());
        }

        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let (substrate, wake_attempts, _pod_gets, _worker) =
            paused_restore_test_subject(&pool, sandbox_id, None, true).await?;
        let ready_timeout = Duration::from_millis(100);
        // The deadline starts when the restore is entered, so in real time the
        // Postgres claim, materialization, and fence before the Wake also
        // spend it, and a slow run takes the no-send path instead of the stuck
        // join. Freeze the clock so that it moves only through the `advance`
        // below, once the Wake is sent. A paused clock still auto-advances
        // while the runtime waits on I/O, unless a blocking task is running.
        let (release_clock, clock_released) = std::sync::mpsc::channel::<()>();
        // Blocks until `release_clock` is dropped.
        let clock_hold =
            tokio::task::spawn_blocking(move || while clock_released.recv().is_ok() {});
        tokio::time::pause();
        let observed_attempts = Arc::clone(&wake_attempts);
        let wake_sent = tokio::task::spawn_blocking(move || {
            let give_up = StdInstant::now() + Duration::from_secs(30);
            while observed_attempts.load(Ordering::SeqCst) == 0 && StdInstant::now() < give_up {
                std::thread::sleep(Duration::from_millis(1));
            }
        });

        // Act.
        let (result, advanced) = tokio::join!(
            substrate.restore_evacuated_execution(
                SandboxApiMetricIdentity::ecw("evacuation"),
                sandbox_id,
                ActivationOptions {
                    blocking: true,
                    ready_timeout,
                },
                None,
                pplx_space_common_core::types::SandboxLifecycle::Paused,
            ),
            async {
                // The worker keeps reporting missing runtime context, so the
                // join is still stuck when the budget runs out. Timer
                // deadlines round up to the next millisecond.
                wake_sent.await?;
                tokio::time::advance(ready_timeout + Duration::from_millis(1)).await;
                Ok::<_, BoxError>(())
            },
        );
        advanced?;

        // Assert.
        let wake_attempts = wake_attempts.load(Ordering::SeqCst);
        assert!(
            matches!(
                result,
                Err(SubstrateError::Timeout { ref message, phase: None })
                    if message.contains("required Wake did not complete within the readiness budget")
            ),
            "stuck paused join must return its own non-rollback timeout after \
             {wake_attempts} Wake attempts: {result:?}"
        );
        assert!(wake_attempts >= 1);
        let lifecycle: String = sqlx::query_scalar("SELECT lifecycle FROM sandboxes WHERE id = $1")
            .bind(sandbox_id)
            .fetch_one(&pool)
            .await?;
        assert_eq!(
            lifecycle, "restoring",
            "a timed-out join may still be completing in sandbox-manager"
        );
        // Released only now: auto-advance during the Postgres reads above
        // could fire their pool timeouts.
        drop(release_clock);
        clock_hold.await?;

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn substrate_enum_forwards_observed_delete_to_kubernetes_override() {
        let substrate = Substrate::Kubernetes(Box::new(substrate_with_unreachable_store()));
        let target = NodeApiTarget {
            managed_by: SpaceNodeManagedBy::Kubernetes,
            role: SpaceNodeRole::Worker,
            direct_endpoint: None,
            substrate_node_id: None,
        };

        let err = substrate
            .delete_observed_execution(SandboxId::new(), &target)
            .await
            .expect_err("unreachable store should fail inside the Kubernetes override");

        assert!(
            matches!(&err, SubstrateError::Failed { message } if message.contains("failed to start pool slot delete transaction")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn process_start_requires_sandbox_connect_to_create_k8s_execution() -> Result<(), BoxError>
    {
        if !crate::test_support::enter_sim_worker_process() {
            return Ok(());
        }

        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "paused").await?;
        let active_node_id = test_worker_node_id();
        sqlx::query(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '10.0.0.10', 'worker', 'k8s', 'ready')",
        )
        .bind(active_node_id)
        .execute(&pool)
        .await?;
        sqlx::query("UPDATE sandboxes SET active_node_id = $2 WHERE id = $1")
            .bind(sandbox_id)
            .bind(active_node_id)
            .execute(&pool)
            .await?;

        let sim_worker = start_sim_worker().await?;
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let controller = SandboxController::new(Substrate::Kubernetes(Box::new(
            substrate_with_store_client_and_proxy_port(
                pool.clone(),
                running_pod_kubernetes_client(Arc::clone(&captured_pod_creates), "127.0.0.1"),
                sim_worker.port(),
            ),
        )))
        .with_route_ed25519_authority(Some(test_route_ed25519_authority()));
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");

        let start_before_connect = match controller
            .spaced_start_process(
                &pool,
                &sandbox,
                SandboxApiMetricIdentity::ecw("spaced"),
                start_process_request(sandbox_id),
                Duration::from_secs(1),
            )
            .await
        {
            Ok(_) => panic!("missing runtime must not start a process"),
            Err(err) => err,
        };
        assert_eq!(
            start_before_connect.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );

        assert_eq!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .len(),
            0,
            "Process/Start must not create a sandbox pod"
        );
        let execution_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1")
                .bind(sandbox_id)
                .fetch_one(&pool)
                .await?;
        assert_eq!(
            execution_count, 0,
            "Process/Start must not create a sandbox_k8s_executions row"
        );

        controller
            .action(
                &pool,
                sandbox_id,
                Some(&sandbox),
                SandboxApiMetricIdentity::ecw("connect"),
                SandboxAction::Wake,
                None,
            )
            .await?;

        assert_eq!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .len(),
            1,
            "sandbox connect must create the sandbox pod"
        );
        let execution_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1")
                .bind(sandbox_id)
                .fetch_one(&pool)
                .await?;
        assert_eq!(
            execution_count, 1,
            "sandbox connect must create a sandbox_k8s_executions row"
        );
        let (state, pod_ip): (String, Option<String>) = sqlx::query_as(
            "SELECT state, host(pod_ip)
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(state, "running");
        assert_eq!(pod_ip.as_deref(), Some("127.0.0.1"));

        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("woken sandbox must remain visible");
        let mut start_stream = controller
            .spaced_start_process(
                &pool,
                &sandbox,
                SandboxApiMetricIdentity::ecw("spaced"),
                start_process_request(sandbox_id),
                Duration::from_secs(1),
            )
            .await?;
        let start_event = start_stream
            .message()
            .await?
            .expect("start stream should emit a started event");
        assert!(
            matches!(
                start_event.event,
                Some(spaced_process_event::Event::Started(_))
            ),
            "unexpected start event: {start_event:?}"
        );
        assert_eq!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .len(),
            1,
            "Process/Start must use the execution created by sandbox connect"
        );

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_started_wakes_with_existing_execution_wait_for_one_pod_create()
    -> Result<(), BoxError> {
        assert_concurrent_started_wakes_issue_one_pod_create(true).await
    }

    fn test_route_ed25519_authority() -> Arc<SpaceApiRouteEd25519Authority> {
        {
            let mut env = pplx_space_test_env::TestEnv::default();
            env.set("SPACE_CLUSTER_ID", "testing");
            env.set(
                SPACE_API_ROUTE_ED25519_SIGNING_KEY_PATH_ENV,
                pplx_space_common_svc::space_api_route_ed25519::test_signing_key_path()
                    .to_str()
                    .unwrap(),
            );
            env.set(
                SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV,
                "kubernetes-controller-test-key",
            );
            env.set(SPACE_API_ROUTE_ED25519_ISSUER_ENV, "space-api");
            Arc::new(SpaceApiRouteEd25519Authority::load_required_from_environment(&env).unwrap())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_started_wakes_without_active_execution_wait_for_one_pod_create()
    -> Result<(), BoxError> {
        assert_concurrent_started_wakes_issue_one_pod_create(false).await
    }

    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_started_access_returns_when_leader_lacks_restore_context()
    -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let artifact_id = Uuid::now_v7();
        let pod_name = "space-sbx-missing-restore-context-g2";
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(artifact_id)
        .execute(&pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_ip, state, active)
             VALUES
                 ($1, 2, $2, 'space', '127.0.0.1', 'running', TRUE)"
        ))
        .bind(sandbox_id)
        .bind(pod_name)
        .execute(&pool)
        .await?;
        let sandbox = Arc::new(
            fetch_sandbox(&pool, sandbox_id)
                .await?
                .expect("inserted sandbox must be visible"),
        );
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_started = Arc::new(Semaphore::new(0));
        let release_request = Arc::new(Semaphore::new(0));
        let cached_pod = Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(pod_name.to_string()),
                namespace: Some("space".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            blocking_missing_pod_kubernetes_client(
                Arc::clone(&request_count),
                Arc::clone(&request_started),
                Arc::clone(&release_request),
            ),
            DEFAULT_SANDBOX_PROXY_PORT,
            Some(cached_pod),
        );

        let leader_substrate = substrate.clone();
        let leader_sandbox = Arc::clone(&sandbox);
        let leader = tokio::spawn(async move {
            leader_substrate
                .ensure_execution_ready_for_access(
                    &leader_sandbox,
                    SandboxApiMetricIdentity::ecw("connect"),
                    Duration::from_secs(3),
                    crate::controllers::substrate::RecoveryCharge::Exempt,
                    None,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), request_started.acquire())
            .await??
            .forget();

        // Act.
        let follower_substrate = substrate.clone();
        let follower_sandbox = Arc::clone(&sandbox);
        let mut follower = tokio::spawn(async move {
            follower_substrate
                .ensure_execution_ready_for_access(
                    &follower_sandbox,
                    SandboxApiMetricIdentity::ecw("connect"),
                    Duration::from_secs(3),
                    crate::controllers::substrate::RecoveryCharge::Exempt,
                    None,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let follower_finished_while_leader_blocked = follower.is_finished();
        let requests_while_leader_blocked = request_count.load(Ordering::SeqCst);
        release_request.add_permits(1);

        let leader_error = tokio::time::timeout(Duration::from_secs(1), leader)
            .await??
            .expect_err("missing restore context must fail the recovery leader");
        let follower_result = tokio::time::timeout(Duration::from_millis(750), &mut follower).await;
        if follower_result.is_err() {
            follower.abort();
        }
        let follower_error = follower_result
            .expect("follower must observe the leader failure without exhausting its wait budget")?
            .expect_err("leader failure must fail its follower");
        let state: String = sqlx::query_scalar(sql!(
            "SELECT state
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active"
        ))
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;

        // Assert.
        assert!(
            !follower_finished_while_leader_blocked,
            "follower should wait while the recovery leader is active"
        );
        assert_eq!(
            requests_while_leader_blocked, 1,
            "follower must not issue another Kubernetes request"
        );
        assert_eq!(leader_error.status(), StatusCode::PRECONDITION_FAILED);
        assert_eq!(leader_error.retry_after_header(), None);
        assert!(
            follower_error
                .to_string()
                .contains("recovery attempt failed"),
            "unexpected follower error: {follower_error:?}"
        );
        assert_eq!(state, "recovery_failed");

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn started_access_redrives_when_suspend_finalization_retires_execution()
    -> Result<(), BoxError> {
        if !crate::test_support::enter_sim_worker_process() {
            return Ok(());
        }

        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let artifact_id = Uuid::now_v7();
        let snapshot_id = SnapshotId::new();
        let pod_name = "space-sbx-started-access-suspend-race-g1";
        let pod_uid = format!("{pod_name}-uid");
        let storage_uri = "s3://space-test/suspend-snapshots/started-access-race?region=us-east-1";
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(
            "UPDATE sandboxes
                SET current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1",
        )
        .bind(sandbox_id)
        .bind(artifact_id)
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO snapshots
                 (id, sandbox_id, type, state, timestamp, org_id,
                  local_artifact_id, artifact_kind, artifact_origin)
             VALUES (
                 $1, $2, 'full', 'creating',
                 (SELECT lifecycle_changed_at - INTERVAL '1 second'
                    FROM sandboxes WHERE id = $2),
                 '00000000-0000-0000-0000-000000000001',
                 $3, 'full', 'suspend'
             )",
        )
        .bind(snapshot_id)
        .bind(sandbox_id)
        .bind(artifact_id)
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO snapshot_storage_locations
                 (snapshot_id, storage_connection_string, org_id)
             VALUES ($1, $2, '00000000-0000-0000-0000-000000000001')",
        )
        .bind(snapshot_id)
        .bind(storage_uri)
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip, state, active)
             VALUES ($1, 1, $2, 'space', $3, '127.0.0.2', 'running', TRUE)",
        )
        .bind(sandbox_id)
        .bind(pod_name)
        .bind(&pod_uid)
        .execute(&pool)
        .await?;
        let sandbox = Arc::new(
            fetch_sandbox(&pool, sandbox_id)
                .await?
                .expect("inserted sandbox must be visible"),
        );
        let initial_pod = Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(pod_name.to_string()),
                namespace: Some("space".to_string()),
                uid: Some(pod_uid),
                ..Default::default()
            },
            status: Some(PodStatus {
                phase: Some("Running".to_string()),
                pod_ip: Some("127.0.0.2".to_string()),
                pod_ips: Some(vec![PodIP {
                    ip: "127.0.0.2".to_string(),
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let pod_creates = Arc::new(Mutex::new(Vec::new()));
        let sim_worker = start_sim_worker().await?;
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            existing_then_created_running_pod_kubernetes_client(Arc::clone(&pod_creates), pod_name),
            sim_worker.port(),
            Some(initial_pod),
        );
        let access_substrate = substrate.clone();
        let access_sandbox = Arc::clone(&sandbox);
        let access = tokio::spawn(async move {
            access_substrate
                .ensure_execution_ready_for_access(
                    &access_sandbox,
                    SandboxApiMetricIdentity::native("runtime_client"),
                    Duration::from_secs(5),
                    crate::controllers::substrate::RecoveryCharge::Exempt,
                    None,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let state: String = sqlx::query_scalar(
                    "SELECT state
                       FROM sandbox_k8s_executions
                      WHERE sandbox_id = $1 AND generation = 1",
                )
                .bind(sandbox_id)
                .fetch_one(&pool)
                .await?;
                if state == "recovering" {
                    return Ok::<_, sqlx::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;

        // Act.
        let mut finalization = pool.begin().await?;
        sqlx::query("UPDATE snapshots SET state = 'ready' WHERE id = $1")
            .bind(snapshot_id)
            .execute(&mut *finalization)
            .await?;
        sqlx::query("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE")
            .bind(sandbox_id)
            .fetch_one(&mut *finalization)
            .await?;
        sqlx::query(
            "UPDATE sandbox_k8s_executions
                SET state = 'suspended', active = FALSE, pod_ip = NULL, updated_at = NOW()
              WHERE sandbox_id = $1 AND generation = 1",
        )
        .bind(sandbox_id)
        .execute(&mut *finalization)
        .await?;
        sqlx::query(
            "UPDATE sandboxes
                SET lifecycle = 'suspended', placement = 'unplaced'
              WHERE id = $1",
        )
        .bind(sandbox_id)
        .execute(&mut *finalization)
        .await?;
        finalization.commit().await?;
        let binding = tokio::time::timeout(Duration::from_secs(5), access).await???;

        // Assert.
        assert_eq!(binding.execution_generation, 2);
        let active_generation: i64 = sqlx::query_scalar(
            "SELECT generation
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(active_generation, 2);
        let pod_creates = pod_creates.lock().expect("capture mutex poisoned");
        assert_eq!(pod_creates.len(), 1);
        assert_eq!(
            pod_creates[0]
                .pointer("/metadata/annotations")
                .and_then(serde_json::Value::as_object)
                .and_then(|annotations| {
                    annotations
                        .get(SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION)
                        .and_then(serde_json::Value::as_str)
                }),
            Some(storage_uri)
        );

        Ok(())
    }

    /// The old Pod is live during materialization, then stops before the
    /// selected-route GET. Subsequent creates belong to the restored execution.
    fn suspend_handoff_kubernetes_client(
        initial_pod: Pod,
        terminal_observed: Arc<Semaphore>,
        pod_creates: Arc<Mutex<Vec<serde_json::Value>>>,
    ) -> Client {
        let state = Arc::new(Mutex::new((Some(initial_pod), 0_usize, false)));
        let service = tower::service_fn(move |req: Request<Body>| {
            let state = Arc::clone(&state);
            let terminal_observed = Arc::clone(&terminal_observed);
            let pod_creates = Arc::clone(&pod_creates);
            async move {
                let method = req.method().clone();
                let path = req.uri().path().to_string();
                let mut status = StatusCode::OK;
                let body = match (method, path.as_str()) {
                    (Method::GET, path) if path.starts_with("/api/v1/namespaces/space/pods/") => {
                        let mut state = state.lock().unwrap();
                        state.1 += 1;
                        if state.1 >= 2 && !state.2 {
                            if let Some(pod) = state.0.as_mut() {
                                pod.status.as_mut().unwrap().phase = Some("Succeeded".to_string());
                            }
                            terminal_observed.add_permits(1);
                        }
                        match &state.0 {
                            Some(pod) => serde_json::to_value(pod)?,
                            None => {
                                status = StatusCode::NOT_FOUND;
                                serde_json::json!({"kind":"Status", "apiVersion":"v1", "reason":"NotFound", "code":404})
                            }
                        }
                    }
                    (Method::DELETE, _) => {
                        state.lock().unwrap().0 = None;
                        serde_json::json!({"kind":"Status", "apiVersion":"v1", "status":"Success", "code":200})
                    }
                    (Method::POST, "/api/v1/namespaces/space/pods") => {
                        let bytes = req.into_body().collect().await?.to_bytes();
                        let mut pod: Pod = serde_json::from_slice(&bytes)?;
                        pod_creates
                            .lock()
                            .unwrap()
                            .push(serde_json::to_value(&pod)?);
                        pod.metadata.uid =
                            Some(format!("{}-uid", pod.metadata.name.as_deref().unwrap()));
                        pod.status = Some(PodStatus {
                            phase: Some("Running".to_string()),
                            pod_ip: Some("127.0.0.1".to_string()),
                            ..Default::default()
                        });
                        let body = serde_json::to_value(&pod)?;
                        let mut state = state.lock().unwrap();
                        state.0 = Some(pod);
                        state.2 = true;
                        status = StatusCode::CREATED;
                        body
                    }
                    (method, path) => panic!("unexpected Kubernetes request: {method} {path}"),
                };
                Ok::<_, BoxError>(
                    Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&body)?))?,
                )
            }
        });
        ClientBuilder::new(service, "space").build()
    }

    fn local_suspend_publication(
        sandbox_id: SandboxId,
        artifact_id: Uuid,
        generation: i64,
        pod_uid: &str,
    ) -> PublishedSandboxState {
        PublishedSandboxState {
            sandbox_id: sandbox_id.to_string(),
            present: true,
            status: Some("suspended".to_string()),
            execution_generation: Some(generation),
            pod_uid: Some(pod_uid.to_string()),
            current_local_artifact_id: Some(artifact_id.to_string()),
            current_local_artifact_kind: Some(
                pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32,
            ),
            current_local_artifact_origin: Some(
                pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend as i32,
            ),
            ..Default::default()
        }
    }

    #[rstest::rstest]
    #[case::first_suspend_finalizes(true, false)]
    #[case::first_suspend_remains_pending(false, false)]
    #[case::prior_marker_finalizes(true, true)]
    #[case::prior_marker_remains_pending(false, true)]
    #[tokio::test(flavor = "current_thread")]
    async fn reconnect_waits_for_local_suspend_backup(
        #[case] finalize: bool,
        #[case] prior_marker: bool,
    ) -> Result<(), BoxError> {
        if !crate::test_support::enter_sim_worker_process() {
            return Ok(());
        }

        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let artifact_id = Uuid::now_v7();
        let snapshot_id = SnapshotId::new();
        let node_id = NodeId::new();
        let prior_artifact_id = prior_marker.then(Uuid::now_v7);
        let route = SandboxActivationAttemptId::new();
        let pod_name = "space-sbx-local-suspend-gap-g1";
        let storage_uri = "s3://space-test/suspend-snapshots/reconnect-gap?region=us-east-1";
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        pplx_space_db_test_fixtures::insert_ready_worker_node(&pool, node_id).await?;
        sqlx::query(sql!(
            "UPDATE sandboxes SET active_node_id = $3, target_node_id = $3,
                 current_local_artifact_id = $2,
                 current_local_artifact_kind = CASE WHEN $2::UUID IS NOT NULL THEN 'full' END,
                 current_local_artifact_origin = CASE WHEN $2::UUID IS NOT NULL THEN 'suspend' END
             WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(prior_artifact_id)
        .bind(node_id)
        .execute(&pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp, org_id,
                 local_artifact_id, artifact_kind, artifact_origin, source_node_id)
             VALUES ($1, $2, 'full', 'creating', NOW(),
                 '00000000-0000-0000-0000-000000000001', $3, 'full', 'suspend', $4)"
        ))
        .bind(snapshot_id)
        .bind(sandbox_id)
        .bind(artifact_id)
        .bind(node_id)
        .execute(&pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string, org_id)
             VALUES ($1, $2, '00000000-0000-0000-0000-000000000001')"
        ))
        .bind(snapshot_id)
        .bind(storage_uri)
        .execute(&pool)
        .await?;
        if let Some(prior_artifact_id) = prior_artifact_id {
            let prior_snapshot_id = SnapshotId::new();
            sqlx::query(sql!(
                "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp, org_id,
                     local_artifact_id, artifact_kind, artifact_origin, source_node_id)
                 VALUES ($1, $2, 'full', 'ready', NOW() - INTERVAL '1 minute',
                     '00000000-0000-0000-0000-000000000001', $3, 'full', 'suspend', $4)"
            ))
            .bind(prior_snapshot_id)
            .bind(sandbox_id)
            .bind(prior_artifact_id)
            .bind(node_id)
            .execute(&pool)
            .await?;
            sqlx::query(sql!(
                "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string, org_id)
                 VALUES ($1, 's3://space-test/suspend-snapshots/prior-cycle?region=us-east-1',
                     '00000000-0000-0000-0000-000000000001')"
            ))
            .bind(prior_snapshot_id).execute(&pool).await?;
        }
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions (sandbox_id, generation, pod_name, pod_namespace,
                 pod_uid, pod_ip, state, active, activation_attempt_id, route_activation_id)
             VALUES ($1, 1, $2, 'space', 'local-suspend-pod-uid', '127.0.0.1', 'running', TRUE, $3, $3)"
        ))
        .bind(sandbox_id).bind(pod_name).bind(route.as_uuid()).execute(&pool).await?;
        let published =
            local_suspend_publication(sandbox_id, artifact_id, 1, "local-suspend-pod-uid");
        publish_sandbox_states(
            &mut *pool.acquire().await?,
            node_id,
            std::slice::from_ref(&published),
        )
        .await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id).await?.unwrap();
        assert_eq!(sandbox.lifecycle, "started");
        let marker: Option<Uuid> = sqlx::query_scalar(sql!(
            "SELECT current_local_artifact_id FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            marker, prior_artifact_id,
            "publication cannot commit the artifact marker before its backup is ready"
        );
        let active_state: String = sqlx::query_scalar(sql!(
            "SELECT state FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
        ))
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            active_state, "running",
            "publication cannot retire an unfinished suspend"
        );
        let initial_pod = Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(pod_name.to_string()),
                namespace: Some("space".to_string()),
                uid: Some("local-suspend-pod-uid".to_string()),
                ..Default::default()
            },
            status: Some(PodStatus {
                phase: Some("Running".to_string()),
                pod_ip: Some("127.0.0.1".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let terminal_observed = Arc::new(Semaphore::new(0));
        let pod_creates = Arc::new(Mutex::new(Vec::new()));
        let worker = start_sim_worker().await?;
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            suspend_handoff_kubernetes_client(
                initial_pod.clone(),
                Arc::clone(&terminal_observed),
                Arc::clone(&pod_creates),
            ),
            worker.port(),
            Some(initial_pod),
        );
        let wake_substrate = substrate.clone();
        let budget = if finalize {
            Duration::from_secs(5)
        } else {
            Duration::from_millis(500)
        };
        let started_at = StdInstant::now();
        let mut wake = tokio::spawn(async move {
            wake_substrate
                .wake_execution_with_metric_identity(
                    &sandbox,
                    budget,
                    SandboxApiMetricIdentity::ecw("connect"),
                    crate::controllers::substrate::RecoveryCharge::Exempt,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), terminal_observed.acquire())
            .await??
            .forget();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut wake)
                .await
                .is_err(),
            "connect must wait for backup finalization after observing the old terminal Pod"
        );
        assert!(
            pod_creates.lock().unwrap().is_empty(),
            "no replacement before finalization"
        );

        if finalize {
            // Exercise the real publication/finalization boundary once the
            // backup becomes durable, rather than setting lifecycle by hand.
            let mut tx = pool.begin().await?;
            sqlx::query(sql!("UPDATE snapshots SET state = 'ready' WHERE id = $1"))
                .bind(snapshot_id)
                .execute(&mut *tx)
                .await?;
            publish_sandbox_states(&mut tx, node_id, &[published]).await?;
            tx.commit().await?;
            let activation = tokio::time::timeout(Duration::from_secs(5), wake).await???;
            assert_eq!(activation.binding.execution_generation, 2);
            let creates = pod_creates.lock().unwrap();
            assert_eq!(creates.len(), 1);
            assert_eq!(
                creates[0]
                    .pointer("/metadata/annotations")
                    .and_then(|a| a.get(SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION))
                    .and_then(serde_json::Value::as_str),
                Some(storage_uri)
            );
        } else {
            let error = tokio::time::timeout(Duration::from_secs(2), wake)
                .await??
                .unwrap_err();
            assert!(
                matches!(error, SubstrateError::SuspendInFlight { .. }),
                "{error:?}"
            );
            assert!(started_at.elapsed() >= budget);
            assert!(started_at.elapsed() < Duration::from_secs(2));
            let state: String = sqlx::query_scalar(sql!(
                "SELECT state FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
            ))
            .bind(sandbox_id)
            .fetch_one(&pool)
            .await?;
            assert_eq!(
                state, "recovering",
                "deadline must not fail the pending suspend owner"
            );
            assert!(pod_creates.lock().unwrap().is_empty());
        }
        Ok(())
    }

    #[derive(Clone, Copy)]
    enum MissingTrackedPodObservation {
        AuthoritativeTerminal,
        AuthoritativeTerminalWithExplicitLocalPlan,
        ReflectorMiss,
    }

    impl MissingTrackedPodObservation {
        fn snapshot_state(self) -> &'static str {
            match self {
                Self::AuthoritativeTerminalWithExplicitLocalPlan => "ready",
                Self::AuthoritativeTerminal | Self::ReflectorMiss => "creating",
            }
        }

        fn explicit_source_node(self, source_node_name: &str) -> Option<&str> {
            matches!(self, Self::AuthoritativeTerminalWithExplicitLocalPlan)
                .then_some(source_node_name)
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn started_recovery_replaces_authoritative_terminal_pod_with_local_restore()
    -> Result<(), BoxError> {
        assert_started_recovery_restores_after_tracked_pod_disappears(
            MissingTrackedPodObservation::AuthoritativeTerminal,
        )
        .await
    }

    #[tokio::test(flavor = "current_thread")]
    async fn started_recovery_restores_locally_after_reflector_cache_miss() -> Result<(), BoxError>
    {
        assert_started_recovery_restores_after_tracked_pod_disappears(
            MissingTrackedPodObservation::ReflectorMiss,
        )
        .await
    }

    #[tokio::test(flavor = "current_thread")]
    async fn started_recovery_stale_pod_preserves_explicit_local_restore_plan()
    -> Result<(), BoxError> {
        assert_started_recovery_restores_after_tracked_pod_disappears(
            MissingTrackedPodObservation::AuthoritativeTerminalWithExplicitLocalPlan,
        )
        .await
    }

    async fn assert_started_recovery_restores_after_tracked_pod_disappears(
        observation: MissingTrackedPodObservation,
    ) -> Result<(), BoxError> {
        use tracing_subscriber::layer::SubscriberExt as _;

        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let artifact_id = Uuid::now_v7();
        let source_node_id = NodeId::new();
        let source_node_name = "source-node";
        let storage_uri = "s3://space-test/suspend-snapshots/current?region=us-east-1&endpoint=https://access:do-not-log@example.com";
        let pod_name = "space-sbx-vanished-g2";
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(sql!(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '10.0.0.10', 'worker', 'k8s', 'ready')"
        ))
        .bind(source_node_id)
        .execute(&pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO node_k8s (node_id, k8s_node_name)
             VALUES ($1, $2)"
        ))
        .bind(source_node_id)
        .bind(source_node_name)
        .execute(&pool)
        .await?;
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend',
                    lifecycle_changed_at = NOW() - INTERVAL '2 minutes'
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(artifact_id)
        .execute(&pool)
        .await?;
        insert_ready_full_snapshot_relative_to_lifecycle(
            &pool,
            sandbox_id,
            storage_uri,
            60,
            artifact_id,
        )
        .await?;
        sqlx::query(sql!(
            "UPDATE snapshots
                SET state = $2,
                    source_node_id = $3
              WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .bind(observation.snapshot_state())
        .bind(source_node_id)
        .execute(&pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_ip, state, active)
             VALUES
                 ($1, 2, $2, 'space', '127.0.0.1', 'running', TRUE)"
        ))
        .bind(sandbox_id)
        .bind(pod_name)
        .execute(&pool)
        .await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let tracked_pod = Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(pod_name.to_string()),
                namespace: Some("space".to_string()),
                uid: Some(format!("{pod_name}-uid")),
                ..Default::default()
            },
            status: Some(PodStatus {
                phase: Some("Succeeded".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let deleted_tracked_pods = Arc::new(AtomicUsize::new(0));
        let (client, initial_pod) = match observation {
            MissingTrackedPodObservation::AuthoritativeTerminal
            | MissingTrackedPodObservation::AuthoritativeTerminalWithExplicitLocalPlan => (
                replace_existing_pod_kubernetes_client(
                    tracked_pod.clone(),
                    Arc::clone(&captured_pod_creates),
                    Arc::clone(&deleted_tracked_pods),
                ),
                Some(tracked_pod),
            ),
            MissingTrackedPodObservation::ReflectorMiss => (
                mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
                None,
            ),
        };
        let source_node = node_with_network_slots(
            source_node_name,
            SandboxPodConfigValues::default()
                .base_node_selector()
                .into_labels(),
            Some(8),
            None,
            true,
            false,
        );
        let substrate = substrate_with_store_client_proxy_port_pod_and_nodes(
            pool.clone(),
            client,
            DEFAULT_SANDBOX_PROXY_PORT,
            initial_pod,
            vec![(*source_node).clone()],
        );
        let recovery_generation = match substrate.store.claim_started_recovery(sandbox_id).await? {
            StartedRecoveryClaim::ExistingExecution { generation } => generation,
            claim => panic!("expected the tracked execution recovery claim, got {claim:?}"),
        };
        let activation_attempt_id =
            pplx_space_common_core::types::SandboxActivationAttemptId::new();
        let event_capture = SandboxEventCapture::default();
        let subscriber = tracing_subscriber::registry().with(event_capture.clone());
        let event_guard = tracing::subscriber::set_default(subscriber);

        // Act.
        tokio::time::timeout(
            Duration::from_secs(2),
            substrate.ensure_pod_exists(
                &sandbox,
                None,
                observation.explicit_source_node(source_node_name),
                None,
                PodMaterializationOwnership::new(
                    Some(StartedRecoveryOwnership::ExistingExecution {
                        generation: recovery_generation,
                    }),
                    None,
                    Some(ActivationAttemptAssignment::new(
                        activation_attempt_id,
                        SandboxTransitionTargetState::Started,
                    )),
                ),
            ),
        )
        .await
        .expect("creating snapshot should restore locally without waiting")?;

        // Assert.
        assert_eq!(
            deleted_tracked_pods.load(Ordering::SeqCst),
            usize::from(!matches!(
                observation,
                MissingTrackedPodObservation::ReflectorMiss
            ))
        );
        {
            let created_pods = captured_pod_creates.lock().expect("capture mutex poisoned");
            assert_eq!(created_pods.len(), 1);
            let annotations = created_pods[0]
                .pointer("/metadata/annotations")
                .and_then(serde_json::Value::as_object)
                .expect("created pod request should carry annotations");
            assert_eq!(
                annotation_value(annotations, SANDBOX_CHECKPOINT_ANNOTATION),
                "suspend"
            );
            assert_eq!(
                annotation_value(annotations, SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION),
                source_node_name
            );
            assert!(!annotations.contains_key(SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION));
            assert_eq!(
                created_pods[0]
                    .pointer("/spec/nodeName")
                    .and_then(serde_json::Value::as_str),
                Some(source_node_name)
            );
        }

        let (lifecycle, generation, successor_pod_name, state, successor_attempt): (
            String,
            i64,
            String,
            String,
            Option<Uuid>,
        ) = sqlx::query_as(sql!(
            "SELECT s.lifecycle, e.generation, e.pod_name, e.state, e.activation_attempt_id
               FROM sandboxes s
               JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id AND e.active
              WHERE s.id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(lifecycle, "started");
        assert_eq!(generation, 3);
        assert_eq!(state, "recovering");
        assert_eq!(successor_attempt, Some(activation_attempt_id.as_uuid()));
        assert_eq!(
            substrate
                .store
                .observe_started_recovery(sandbox_id, recovery_generation)
                .await?,
            StartedRecoveryObservation::Pending
        );

        let retired_state: String = sqlx::query_scalar(sql!(
            "SELECT state
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND generation = 2"
        ))
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(retired_state, "replaced");

        // Arrange a source-node loss after the local restore handoff.
        sqlx::query(sql!(
            "UPDATE snapshots
                SET state = 'ready'
              WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
        sqlx::query(sql!(
            "UPDATE nodes
                SET state = 'registered'
              WHERE id = $1"
        ))
        .bind(source_node_id)
        .execute(&pool)
        .await?;
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET updated_at = NOW() - INTERVAL '6 minutes',
                    activation_attempt_expires_at = NOW() - INTERVAL '1 second'
              WHERE sandbox_id = $1 AND active"
        ))
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
        let pending_local_pod = Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(successor_pod_name),
                namespace: Some("space".to_string()),
                uid: Some("local-restore-pod-uid".to_string()),
                annotations: Some(BTreeMap::from([(
                    SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION.to_string(),
                    source_node_name.to_string(),
                )])),
                ..Default::default()
            },
            status: Some(PodStatus {
                phase: Some("Pending".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let fallback_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let deleted_pods = Arc::new(AtomicUsize::new(0));
        let fallback_substrate = substrate_with_store_client_proxy_port_pod_and_nodes(
            pool.clone(),
            replace_existing_pod_kubernetes_client(
                pending_local_pod.clone(),
                Arc::clone(&fallback_pod_creates),
                Arc::clone(&deleted_pods),
            ),
            DEFAULT_SANDBOX_PROXY_PORT,
            Some(pending_local_pod),
            Vec::new(),
        );
        let fallback_recovery_generation = match fallback_substrate
            .store
            .claim_started_recovery(sandbox_id)
            .await?
        {
            StartedRecoveryClaim::ExistingExecution { generation } => generation,
            claim => panic!("expected the expired recovery to be reclaimed, got {claim:?}"),
        };
        let fallback_attempt_id = pplx_space_common_core::types::SandboxActivationAttemptId::new();

        // Act.
        fallback_substrate
            .ensure_pod_exists(
                &sandbox,
                None,
                None,
                None,
                PodMaterializationOwnership::new(
                    Some(StartedRecoveryOwnership::ExistingExecution {
                        generation: fallback_recovery_generation,
                    }),
                    None,
                    Some(ActivationAttemptAssignment::new(
                        fallback_attempt_id,
                        SandboxTransitionTargetState::Started,
                    )),
                ),
            )
            .await?;

        // Assert.
        assert_eq!(deleted_pods.load(Ordering::SeqCst), 1);
        {
            let fallback_pods = fallback_pod_creates.lock().expect("capture mutex poisoned");
            assert_eq!(fallback_pods.len(), 1);
            let fallback_annotations = fallback_pods[0]
                .pointer("/metadata/annotations")
                .and_then(serde_json::Value::as_object)
                .expect("fallback pod request should carry annotations");
            assert_eq!(
                annotation_value(
                    fallback_annotations,
                    SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION
                ),
                storage_uri
            );
            assert!(
                !fallback_annotations.contains_key(SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION)
            );
            assert_eq!(
                fallback_pods[0]
                    .pointer("/spec/nodeName")
                    .and_then(serde_json::Value::as_str),
                None
            );
        }

        let (fallback_generation, fallback_state, fallback_attempt): (i64, String, Option<Uuid>) =
            sqlx::query_as(sql!(
                "SELECT generation, state, activation_attempt_id
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active"
            ))
            .bind(sandbox_id)
            .fetch_one(&pool)
            .await?;
        assert_eq!(fallback_generation, 4);
        assert_eq!(fallback_state, "recovering");
        assert_eq!(fallback_attempt, Some(fallback_attempt_id.as_uuid()));
        drop(event_guard);

        let replacement_events: Vec<BTreeMap<String, String>> = event_capture
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| {
                event.get("event_type").map(String::as_str) == Some("execution_replaced")
            })
            .cloned()
            .collect();
        let local_event = replacement_events
            .iter()
            .find(|event| event.get("execution_generation").map(String::as_str) == Some("2"))
            .expect("local recovery replacement event");
        assert_eq!(
            local_event.get("source_node_name").map(String::as_str),
            Some(source_node_name)
        );
        assert_eq!(
            local_event.get("restore_provenance").map(String::as_str),
            Some("local_only")
        );
        let remote_event = replacement_events
            .iter()
            .find(|event| event.get("execution_generation").map(String::as_str) == Some("3"))
            .expect("remote recovery replacement event");
        assert!(!remote_event.contains_key("restored_snapshot_storage_uri"));
        assert_eq!(
            remote_event
                .get("restore_storage_uri_present")
                .map(String::as_str),
            Some("true")
        );
        assert!(
            remote_event
                .values()
                .all(|value| !value.contains("do-not-log")),
            "replacement telemetry leaked the credential-bearing storage URI: {remote_event:?}"
        );
        assert_eq!(
            remote_event.get("restore_provenance").map(String::as_str),
            Some("remote_matches_local")
        );

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn non_owner_ensure_does_not_duplicate_started_recovery_pod_create()
    -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_ip, state, active)
             VALUES
                 ($1, 1, 'space-sbx-existing-g1', 'space', '127.0.0.1', 'running', TRUE)",
        )
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
        let sandbox = Arc::new(
            fetch_sandbox(&pool, sandbox_id)
                .await?
                .expect("inserted sandbox must be visible"),
        );
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_started = Arc::new(Semaphore::new(0));
        let release_request = Arc::new(Semaphore::new(0));
        let substrate = substrate_with_store_and_client(
            pool,
            blocking_failed_pod_create_kubernetes_client(
                Arc::clone(&request_count),
                Arc::clone(&request_started),
                Arc::clone(&release_request),
            ),
        );
        let owner_substrate = substrate.clone();
        let owner_sandbox = Arc::clone(&sandbox);
        let owner = tokio::spawn(async move {
            owner_substrate
                .wake_execution_with_metric_identity(
                    &owner_sandbox,
                    Duration::from_secs(1),
                    SandboxApiMetricIdentity::ecw("connect"),
                    crate::controllers::substrate::RecoveryCharge::Exempt,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), request_started.acquire())
            .await??
            .forget();

        // Act.
        let non_owner_substrate = substrate.clone();
        let non_owner_sandbox = Arc::clone(&sandbox);
        let non_owner = tokio::spawn(async move {
            non_owner_substrate
                .ensure_pod_exists(
                    &non_owner_sandbox,
                    None,
                    None,
                    None,
                    PodMaterializationOwnership::default(),
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let requests_while_owner_blocked = request_count.load(Ordering::SeqCst);
        release_request.add_permits(2);
        let _ = tokio::time::timeout(Duration::from_secs(2), owner).await??;
        let _ = tokio::time::timeout(Duration::from_secs(2), non_owner).await??;

        // Assert.
        assert_eq!(
            requests_while_owner_blocked, 1,
            "a non-owner ensure must not duplicate an active started recovery Pod create"
        );

        Ok(())
    }

    /// Seed a row whose restore lease another request holds, and spawn the
    /// "owner": a task that completes the restore out-of-band (routable
    /// execution + `started` flip) while the caller under test waits.
    async fn seed_leased_restore_with_completer(
        pool: &PgPool,
        sandbox_id: SandboxId,
        lifecycle: SandboxLifecycle,
    ) -> Result<tokio::task::JoinHandle<Result<(), BoxError>>, BoxError> {
        seed_leased_restore_with_delayed_completer(
            pool,
            sandbox_id,
            lifecycle,
            Duration::from_secs(120),
            Duration::from_millis(250),
        )
        .await
    }

    async fn seed_leased_restore_with_delayed_completer(
        pool: &PgPool,
        sandbox_id: SandboxId,
        lifecycle: SandboxLifecycle,
        lease_duration: Duration,
        completion_delay: Duration,
    ) -> Result<tokio::task::JoinHandle<Result<(), BoxError>>, BoxError> {
        insert_test_sandbox_row(pool, sandbox_id, lifecycle.as_str()).await?;
        sqlx::query(
            "UPDATE sandboxes
                SET restore_lease_expires_at =
                        NOW() + ($2::bigint * INTERVAL '1 millisecond'),
                    restore_lease_token = $3
              WHERE id = $1",
        )
        .bind(sandbox_id)
        .bind(i64::try_from(lease_duration.as_millis())?)
        .bind(Uuid::now_v7())
        .execute(pool)
        .await?;
        let completer_pool = pool.clone();
        Ok(tokio::spawn(async move {
            tokio::time::sleep(completion_delay).await;
            sqlx::query(
                "INSERT INTO sandbox_k8s_executions
                     (sandbox_id, generation, pod_name, pod_namespace, pod_ip, state, active)
                 VALUES ($1, 2, 'space-sbx-owner-g2', 'space', '127.0.0.1', 'running', TRUE)",
            )
            .bind(sandbox_id)
            .execute(&completer_pool)
            .await?;
            sqlx::query("UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1")
                .bind(sandbox_id)
                .execute(&completer_pool)
                .await?;
            Ok(())
        }))
    }

    async fn seed_restoring_sandbox_with_active_lease(
        pool: &PgPool,
        sandbox_id: SandboxId,
        lease_duration: Duration,
    ) -> Result<(), BoxError> {
        insert_test_sandbox_row(pool, sandbox_id, SandboxLifecycle::Restoring.as_str()).await?;
        let node_id = test_worker_node_id();
        sqlx::query(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '127.0.0.1', 'worker', 'k8s', 'ready')",
        )
        .bind(node_id)
        .execute(pool)
        .await?;
        insert_ready_full_snapshot_relative_to_lifecycle(
            pool,
            sandbox_id,
            "s3://space-test/suspend-snapshots/expired-owner?region=us-east-1",
            -1,
            Uuid::now_v7(),
        )
        .await?;
        sqlx::query(
            "UPDATE sandboxes
                SET restore_lease_expires_at =
                        NOW() + ($2::bigint * INTERVAL '1 millisecond'),
                    restore_lease_token = $3,
                    active_node_id = $4
              WHERE id = $1",
        )
        .bind(sandbox_id)
        .bind(i64::try_from(lease_duration.as_millis())?)
        .bind(Uuid::now_v7())
        .bind(node_id)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn publish_completed_restore(
        pool: &PgPool,
        sandbox_id: SandboxId,
    ) -> Result<(), BoxError> {
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_ip, state, active)
             VALUES ($1, 2, 'space-sbx-owner-g2', 'space', '127.0.0.1', 'running', TRUE)",
        )
        .bind(sandbox_id)
        .execute(pool)
        .await?;
        sqlx::query("UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1")
            .bind(sandbox_id)
            .execute(pool)
            .await?;
        Ok(())
    }

    fn substrate_with_failed_pod_creates(pool: PgPool) -> (KubernetesSubstrate, Arc<AtomicUsize>) {
        let request_count = Arc::new(AtomicUsize::new(0));
        let client = blocking_failed_pod_create_kubernetes_client(
            Arc::clone(&request_count),
            Arc::new(Semaphore::new(0)),
            Arc::new(Semaphore::new(1)),
        );
        (substrate_with_store_and_client(pool, client), request_count)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stale_suspended_wake_reuses_owner_completion_without_pod_create()
    -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "suspended").await?;
        let stale_sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        publish_completed_restore(&pool, sandbox_id).await?;
        let (substrate, request_count) = substrate_with_failed_pod_creates(pool);

        // Act.
        let binding = substrate
            .wake_execution_with_metric_identity(
                &stale_sandbox,
                Duration::from_secs(5),
                SandboxApiMetricIdentity::ecw("connect"),
                crate::controllers::substrate::RecoveryCharge::Exempt,
            )
            .await?;

        // Assert.
        assert_eq!(binding.completion, ActivationCompletion::Joined);
        assert_eq!(binding.execution_object_id, "space-sbx-owner-g2");
        assert_eq!(binding.execution_generation, 2);
        assert_eq!(request_count.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stale_restoring_wake_reuses_owner_completion_without_pod_create()
    -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "restoring").await?;
        let stale_sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        publish_completed_restore(&pool, sandbox_id).await?;
        let (substrate, request_count) = substrate_with_failed_pod_creates(pool);

        // Act.
        let binding = substrate
            .wake_execution_with_metric_identity(
                &stale_sandbox,
                Duration::from_secs(5),
                SandboxApiMetricIdentity::ecw("connect"),
                crate::controllers::substrate::RecoveryCharge::Exempt,
            )
            .await?;

        // Assert.
        assert_eq!(binding.completion, ActivationCompletion::Joined);
        assert_eq!(binding.execution_object_id, "space-sbx-owner-g2");
        assert_eq!(binding.execution_generation, 2);
        assert_eq!(request_count.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stale_restoring_wake_retries_after_owner_rollback() -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "restoring").await?;
        let stale_sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        sqlx::query("UPDATE sandboxes SET lifecycle = 'suspended' WHERE id = $1")
            .bind(sandbox_id)
            .execute(&pool)
            .await?;
        let (substrate, request_count) = substrate_with_failed_pod_creates(pool);

        // Act.
        let err = substrate
            .wake_execution_with_metric_identity(
                &stale_sandbox,
                Duration::from_secs(5),
                SandboxApiMetricIdentity::ecw("connect"),
                crate::controllers::substrate::RecoveryCharge::Exempt,
            )
            .await
            .expect_err("restore without a source must fail");

        // Assert.
        assert!(
            matches!(err, SubstrateError::Timeout { .. }),
            "the follower must retry the suspended row before reporting the missing restore source: {err:?}"
        );
        assert_eq!(
            request_count.load(Ordering::SeqCst),
            0,
            "a restore without a source must not create a Pod"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn restore_claim_reuses_owner_completion_without_pod_create() -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "suspended").await?;
        publish_completed_restore(&pool, sandbox_id).await?;
        let (substrate, request_count) = substrate_with_failed_pod_creates(pool);

        // Act.
        let completion = substrate
            .restore_execution_inner(
                SandboxApiMetricIdentity::ecw("connect"),
                sandbox_id,
                crate::controllers::substrate::ActivationOptions {
                    blocking: true,
                    ready_timeout: Duration::from_secs(5),
                },
                None,
                pplx_space_common_core::types::SandboxLifecycle::Started,
                crate::controllers::substrate::RecoveryCharge::Exempt,
            )
            .await?;

        // Assert.
        assert_eq!(completion, ActivationCompletion::Joined);
        assert_eq!(request_count.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn restore_claim_preserves_conflict_when_started_is_unroutable() -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        let substrate = substrate_with_store_and_client(pool, inert_kubernetes_client());

        // Act.
        let error = substrate
            .restore_execution_inner(
                SandboxApiMetricIdentity::ecw("connect"),
                sandbox_id,
                crate::controllers::substrate::ActivationOptions {
                    blocking: true,
                    ready_timeout: Duration::from_secs(5),
                },
                None,
                pplx_space_common_core::types::SandboxLifecycle::Started,
                crate::controllers::substrate::RecoveryCharge::Exempt,
            )
            .await
            .expect_err("an unroutable started sandbox must remain a conflict");

        // Assert.
        assert!(matches!(error, SubstrateError::Conflict { .. }));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wake_losing_restore_lease_joins_owner_binding_without_pod_create()
    -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let completer =
            seed_leased_restore_with_completer(&pool, sandbox_id, SandboxLifecycle::Restoring)
                .await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_started = Arc::new(Semaphore::new(0));
        let release_request = Arc::new(Semaphore::new(0));
        let substrate = substrate_with_store_and_client(
            pool,
            blocking_failed_pod_create_kubernetes_client(
                Arc::clone(&request_count),
                Arc::clone(&request_started),
                Arc::clone(&release_request),
            ),
        );

        // Act: the wake loses the lease CAS and joins the owner's attempt.
        let binding = substrate
            .wake_execution_with_metric_identity(
                &sandbox,
                Duration::from_secs(5),
                SandboxApiMetricIdentity::ecw("connect"),
                crate::controllers::substrate::RecoveryCharge::Exempt,
            )
            .await?;
        completer.await??;

        // Assert.
        assert_eq!(binding.completion, ActivationCompletion::Joined);
        assert_eq!(binding.execution_object_id, "space-sbx-owner-g2");
        assert_eq!(binding.execution_generation, 2);
        assert_eq!(
            request_count.load(Ordering::SeqCst),
            0,
            "a joining follower must not drive its own pod create"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn user_restore_losing_lease_joins_owner_completion() -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        // `restoring`: a live lease only survives on a restoring row (the
        // clear-lease trigger drops it elsewhere), and a concurrent connect
        // against an in-flight restore is exactly this shape.
        let completer =
            seed_leased_restore_with_completer(&pool, sandbox_id, SandboxLifecycle::Restoring)
                .await?;
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_started = Arc::new(Semaphore::new(0));
        let release_request = Arc::new(Semaphore::new(0));
        let substrate = substrate_with_store_and_client(
            pool,
            blocking_failed_pod_create_kubernetes_client(
                Arc::clone(&request_count),
                Arc::clone(&request_started),
                Arc::clone(&release_request),
            ),
        );

        // Act: the user restore loses the lease CAS and joins the owner.
        let completion = substrate
            .restore_execution_inner(
                SandboxApiMetricIdentity::ecw("connect"),
                sandbox_id,
                crate::controllers::substrate::ActivationOptions {
                    blocking: true,
                    ready_timeout: Duration::from_secs(5),
                },
                None,
                pplx_space_common_core::types::SandboxLifecycle::Started,
                crate::controllers::substrate::RecoveryCharge::Exempt,
            )
            .await?;
        completer.await??;

        // Assert: joined instead of driving node work.
        assert_eq!(completion, ActivationCompletion::Joined);
        assert_eq!(
            request_count.load(Ordering::SeqCst),
            0,
            "a joining user restore must not drive its own pod create"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn published_activation_releases_only_its_completed_lease() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&db.pool, sandbox_id, "creating").await?;
        let substrate = substrate_with_store_and_client(db.pool.clone(), inert_kubernetes_client());
        let attempt_id = SandboxActivationAttemptId::new();
        let execution = substrate
            .store
            .begin_activation_execution_row(
                sandbox_id,
                attempt_id,
                SandboxTransitionTargetState::Started,
            )
            .await?;
        let completion = Some(crate::controllers::substrate::ActivationLeaseCompletion {
            generation: execution.generation,
            attempt_id,
        });
        // CREATE's pre-publication release is a no-op: the target is not yet
        // visible. This is why completion must also run after publication.
        substrate
            .store
            .release_activation_attempt_best_effort(sandbox_id, execution.generation, attempt_id)
            .await;
        assert_eq!(
            substrate
                .store
                .active_execution_status(sandbox_id)
                .await?
                .unwrap()
                .activation_attempt_id,
            Some(attempt_id)
        );
        sqlx::query(sql!(
            "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
        ))
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;
        // STOP may win immediately after publication, before lease release.
        sqlx::query(sql!(
            "UPDATE sandboxes SET lifecycle = 'stopped' WHERE id = $1"
        ))
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;
        substrate
            .complete_activation_lease(sandbox_id, completion)
            .await;
        let next = substrate
            .store
            .assign_activation_attempt(
                sandbox_id,
                execution.generation,
                SandboxActivationAttemptId::new(),
                SandboxTransitionTargetState::Started,
            )
            .await?;
        // An immediate START must own its materialization instead of joining
        // CREATE and trying to wake the exited pod. Late CREATE completion
        // must not release that new owner, even after its target is published.
        sqlx::query(sql!(
            "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
        ))
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;
        substrate
            .complete_activation_lease(sandbox_id, completion)
            .await;
        let current = substrate
            .store
            .active_execution_status(sandbox_id)
            .await?
            .unwrap();
        assert_eq!(current.activation_attempt_id, Some(next));
        assert_eq!(current.route_activation_id, Some(attempt_id));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn start_failure_before_wake_releases_its_activation_lease() -> Result<(), BoxError> {
        for source_pending in [true, false] {
            let db = pplx_space_db_test_fixtures::pg_test_db().await?;
            let sandbox_id = SandboxId::new();
            let lifecycle = if source_pending { "stopped" } else { "started" };
            insert_test_sandbox_row(&db.pool, sandbox_id, lifecycle).await?;
            insert_k8s_execution(
                &db.pool,
                sandbox_id,
                1,
                "space-sbx-stopped-g1",
                "stopped-pod-uid",
                Some("127.0.0.1"),
                true,
            )
            .await?;
            let route_id = SandboxActivationAttemptId::new();
            sqlx::query(sql!(
                "UPDATE sandbox_k8s_executions SET route_activation_id = $2 WHERE sandbox_id = $1"
            ))
            .bind(sandbox_id)
            .bind(route_id)
            .execute(&db.pool)
            .await?;
            let pod_creates = Arc::new(Mutex::new(Vec::new()));
            let pod = Pod {
                metadata: kube::api::ObjectMeta {
                    name: Some("space-sbx-stopped-g1".into()),
                    namespace: Some("space".into()),
                    uid: Some("stopped-pod-uid".into()),
                    ..Default::default()
                },
                status: Some(PodStatus {
                    phase: Some("Running".into()),
                    pod_ip: Some("127.0.0.1".into()),
                    ..Default::default()
                }),
                ..Default::default()
            };
            // Model a stop racing START: materialization sees Running, then
            // the authoritative pre-dispatch route check sees a terminal pod.
            let client = suspend_handoff_kubernetes_client(
                pod.clone(),
                Arc::new(Semaphore::new(0)),
                Arc::clone(&pod_creates),
            );
            let substrate = substrate_with_store_client_proxy_port_and_pod(
                db.pool.clone(),
                client,
                DEFAULT_SANDBOX_PROXY_PORT,
                Some(pod),
            );
            let error = substrate
                .start_execution(
                    SandboxApiMetricIdentity::native("start"),
                    sandbox_id,
                    crate::controllers::substrate::ActivationOptions {
                        blocking: true,
                        ready_timeout: Duration::from_secs(1),
                    },
                    crate::controllers::substrate::RecoveryCharge::Exempt,
                )
                .await
                .expect_err("no worker Wake can be dispatched");
            if source_pending {
                assert!(
                    matches!(error, SubstrateError::Timeout { ref message, .. }
                    if message.contains("source node or evacuated")),
                    "{error:?}"
                );
            } else {
                assert!(
                    matches!(error, SubstrateError::Unavailable { ref message }
                    if message.contains("terminating or terminal")),
                    "{error:?}"
                );
            }
            let lease_live: bool = sqlx::query_scalar(sql!(
                "SELECT COALESCE(activation_attempt_expires_at > NOW(), FALSE)
                 FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
            ))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
            assert!(
                !lease_live,
                "a retry must recover the pod, not join an abandoned activation (source_pending={source_pending})"
            );
            assert!(pod_creates.lock().unwrap().is_empty());
            let replacement = substrate
                .store
                .assign_activation_attempt(
                    sandbox_id,
                    1,
                    SandboxActivationAttemptId::new(),
                    SandboxTransitionTargetState::Started,
                )
                .await?;
            substrate
                .store
                .release_finished_activation_attempt_best_effort(sandbox_id, 1, route_id)
                .await;
            let current = substrate
                .store
                .active_execution_status(sandbox_id)
                .await?
                .unwrap();
            assert_eq!(current.route_activation_id, Some(route_id));
            assert_eq!(
                current.activation_attempt_id,
                Some(replacement),
                "stale cancellation must preserve a successor's lease"
            );
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn start_losing_restore_lease_reports_joined_completion() -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let completer =
            seed_leased_restore_with_completer(&pool, sandbox_id, SandboxLifecycle::Restoring)
                .await?;
        let (substrate, request_count) = substrate_with_failed_pod_creates(pool);

        // Act.
        let activation = substrate
            .start_execution(
                SandboxApiMetricIdentity::native("start"),
                sandbox_id,
                crate::controllers::substrate::ActivationOptions {
                    blocking: true,
                    ready_timeout: Duration::from_secs(5),
                },
                crate::controllers::substrate::RecoveryCharge::Exempt,
            )
            .await?;
        completer.await??;

        // Assert.
        assert_eq!(activation.completion, ActivationCompletion::Joined);
        assert_eq!(activation.execution_generation, 2);
        assert_eq!(request_count.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wake_follower_respects_readiness_budget_while_restore_remains_in_flight()
    -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let completer = seed_leased_restore_with_delayed_completer(
            &pool,
            sandbox_id,
            SandboxLifecycle::Restoring,
            Duration::from_secs(120),
            Duration::from_millis(750),
        )
        .await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let (substrate, request_count) = substrate_with_failed_pod_creates(pool);

        // Act.
        let result = tokio::time::timeout(
            Duration::from_millis(300),
            substrate.wake_execution_with_metric_identity(
                &sandbox,
                Duration::from_millis(200),
                SandboxApiMetricIdentity::ecw("connect"),
                crate::controllers::substrate::RecoveryCharge::Exempt,
            ),
        )
        .await
        .expect("the follower must return inside its readiness budget");
        completer.await??;

        // Assert.
        let error = match result {
            Ok(_) => panic!("the follower must not outlive its readiness budget"),
            Err(error) => error,
        };
        let SubstrateError::RestoreInFlight {
            lease_remaining: Some(lease_remaining),
            ..
        } = error
        else {
            panic!("expected RestoreInFlight with a lease-derived retry hint")
        };
        assert!(lease_remaining > Duration::from_secs(1));
        assert_eq!(request_count.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wake_follower_defers_if_suspend_claims_during_restore_join() -> Result<(), BoxError> {
        // Arrange: wake starts as a follower of an existing restore. Suspend
        // then claims the sandbox while the follower is sleeping between
        // observations, so its refreshed sandbox is `suspending`.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "restoring").await?;
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET restore_lease_expires_at = NOW() + INTERVAL '10 seconds',
                    restore_lease_token = $2
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await?;
        let stale_restoring = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let (substrate, request_count) = substrate_with_failed_pod_creates(pool.clone());
        let suspend_pool = pool.clone();
        let suspend_claim = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            sqlx::query(sql!(
                "UPDATE sandboxes SET lifecycle = 'suspending' WHERE id = $1"
            ))
            .bind(sandbox_id)
            .execute(&suspend_pool)
            .await?;
            Ok::<_, BoxError>(())
        });

        // Act.
        let error = substrate
            .wake_execution_with_metric_identity(
                &stale_restoring,
                Duration::from_secs(1),
                SandboxApiMetricIdentity::ecw("connect"),
                crate::controllers::substrate::RecoveryCharge::Exempt,
            )
            .await
            .expect_err("wake must defer to suspend after refreshing its joined restore");
        suspend_claim.await??;

        // Assert: the refreshed lifecycle preserves suspend's retryable
        // ownership signal before any execution or Pod materialization.
        assert!(
            matches!(error, SubstrateError::SuspendInFlight { .. }),
            "expected the retryable suspend ownership signal, got {error:?}",
        );
        assert_eq!(request_count.load(Ordering::SeqCst), 0);
        let execution_count: i64 = sqlx::query_scalar(sql!(
            "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(execution_count, 0);
        Ok(())
    }

    /// A ghost-fenced suspended row makes every claim miss surface as
    /// `Conflict`; the re-drive lap must stop at the readiness budget with
    /// the normal retryable error instead of re-racing past the deadline.
    #[tokio::test(flavor = "current_thread")]
    async fn fence_blocked_conflict_redrive_respects_readiness_budget() -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "suspended").await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_ip, state, active)
             VALUES ($1, 1, 'space-sbx-ghost-g1', 'space', '127.0.0.1', 'creating', TRUE)",
        )
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let (substrate, request_count) = substrate_with_failed_pod_creates(pool);

        // Act.
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            substrate.wake_execution_with_metric_identity(
                &sandbox,
                Duration::from_millis(200),
                SandboxApiMetricIdentity::ecw("connect"),
                crate::controllers::substrate::RecoveryCharge::Exempt,
            ),
        )
        .await
        .expect("the fenced re-drive must return inside its readiness budget");

        // Assert.
        let error = match result {
            Ok(_) => panic!("a fence-blocked wake cannot win the claim"),
            Err(error) => error,
        };
        assert!(
            matches!(error, SubstrateError::RestoreInFlight { .. }),
            "expected the budget-elapsed retryable error, got {error:?}"
        );
        assert_eq!(request_count.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn follower_losing_expiry_reclaim_keeps_original_budget_and_new_lease_hint()
    -> Result<(), BoxError> {
        if !crate::test_support::enter_sim_worker_process() {
            return Ok(());
        }

        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db_with_max_connections(8).await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        seed_restoring_sandbox_with_active_lease(&pool, sandbox_id, Duration::from_millis(400))
            .await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let sandbox = Arc::new(sandbox);
        let request_started = Arc::new(Semaphore::new(0));
        let release_request = Arc::new(Semaphore::new(0));
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let sim_worker = start_sim_worker().await?;
        let substrate = Arc::new(substrate_with_store_client_and_proxy_port(
            pool,
            blocking_running_pod_kubernetes_client(
                Arc::clone(&captured_pod_creates),
                "127.0.0.1",
                Arc::clone(&request_started),
                Arc::clone(&release_request),
            ),
            sim_worker.port(),
        ));

        // Act.
        let race_started = StdInstant::now();
        let first_substrate = Arc::clone(&substrate);
        let first_sandbox = Arc::clone(&sandbox);
        // The first caller becomes the coalesced-run owner; give it a budget
        // that comfortably outlives the deliberately-late pod-create release
        // below (which waits on the second caller's ~1.3s expiry first) so
        // the test can assert its owned completion even on slow CI hardware.
        // The second caller keeps the short budget whose expiry this test is
        // about.
        let first = tokio::spawn(async move {
            first_substrate
                .wake_execution_with_metric_identity(
                    &first_sandbox,
                    Duration::from_secs(15),
                    SandboxApiMetricIdentity::ecw("connect"),
                    crate::controllers::substrate::RecoveryCharge::Exempt,
                )
                .await
        });
        let second_substrate = Arc::clone(&substrate);
        let second_sandbox = Arc::clone(&sandbox);
        let second = tokio::spawn(async move {
            second_substrate
                .wake_execution_with_metric_identity(
                    &second_sandbox,
                    Duration::from_millis(800),
                    SandboxApiMetricIdentity::ecw("connect"),
                    crate::controllers::substrate::RecoveryCharge::Exempt,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(3), request_started.acquire())
            .await??
            .forget();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !first.is_finished() && !second.is_finished() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        let (loser, owner) = if first.is_finished() {
            (first.await?, second)
        } else {
            (second.await?, first)
        };

        // Assert. The joiner's wait is bounded by its own 800ms readiness
        // budget plus the coalescer's publish grace — never a fresh budget.
        assert!(
            race_started.elapsed() < Duration::from_secs(2),
            "the reclaim loser received a fresh readiness budget"
        );
        let error = match loser {
            Ok(_) => panic!("the reclaim loser must not receive a fresh readiness budget"),
            Err(error) => error,
        };
        let SubstrateError::RestoreInFlight {
            lease_remaining: Some(lease_remaining),
            ..
        } = error
        else {
            panic!("expected RestoreInFlight with the new owner's lease hint")
        };
        assert!(lease_remaining > Duration::from_secs(100));
        release_request.add_permits(1);
        let binding = owner.await??;
        assert_eq!(binding.completion, ActivationCompletion::Owned);
        assert_eq!(binding.execution_generation, 1);
        assert_eq!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .len(),
            1,
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wake_follower_reclaims_expired_restore_with_remaining_readiness_budget()
    -> Result<(), BoxError> {
        if !crate::test_support::enter_sim_worker_process() {
            return Ok(());
        }

        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        seed_restoring_sandbox_with_active_lease(&pool, sandbox_id, Duration::from_millis(200))
            .await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let sim_worker = start_sim_worker().await?;
        let substrate = substrate_with_store_client_and_proxy_port(
            pool,
            running_pod_kubernetes_client(Arc::clone(&captured_pod_creates), "127.0.0.1"),
            sim_worker.port(),
        );

        // Act.
        let binding = substrate
            .wake_execution_with_metric_identity(
                &sandbox,
                Duration::from_secs(2),
                SandboxApiMetricIdentity::ecw("connect"),
                crate::controllers::substrate::RecoveryCharge::Exempt,
            )
            .await?;

        // Assert.
        assert_eq!(binding.completion, ActivationCompletion::Owned);
        assert_eq!(binding.execution_generation, 1);
        assert_eq!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .len(),
            1,
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stale_restore_owner_cannot_materialize_after_lease_reclaim() -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "suspended").await?;
        let stale_sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
        );
        insert_ready_full_snapshot_relative_to_lifecycle(
            &pool,
            sandbox_id,
            "s3://space-test/suspend-snapshots/fenced-owner?region=us-east-1",
            0,
            Uuid::now_v7(),
        )
        .await?;
        let old_claim = substrate
            .store
            .begin_restore_lifecycle(sandbox_id, false)
            .await?;
        let old_token = old_claim
            .restore_lease_token()
            .expect("user restore must carry a lease token");
        let old_owner = substrate
            .store
            .prepare_wake_sandbox(&stale_sandbox, sandbox_id, Some(old_claim))
            .await?;
        sqlx::query(
            "UPDATE sandboxes
                SET restore_lease_expires_at = NOW() - INTERVAL '1 second'
              WHERE id = $1",
        )
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
        let new_claim = substrate
            .store
            .begin_restore_lifecycle(sandbox_id, false)
            .await?;
        let new_token = new_claim
            .restore_lease_token()
            .expect("replacement restore must carry a lease token");
        let new_owner = substrate
            .store
            .prepare_wake_sandbox(&old_owner.sandbox, sandbox_id, Some(new_claim))
            .await?;

        // Act.
        let stale_error = substrate
            .ensure_pod_exists(
                &old_owner.sandbox,
                None,
                None,
                None,
                PodMaterializationOwnership::new(None, Some(old_token), None),
            )
            .await
            .expect_err("a stale restore owner must lose before pod materialization");
        substrate
            .ensure_pod_exists(
                &new_owner.sandbox,
                None,
                None,
                None,
                PodMaterializationOwnership::new(None, Some(new_token), None),
            )
            .await?;
        let lease_remaining_secs: f64 = sqlx::query_scalar(
            "SELECT EXTRACT(EPOCH FROM (restore_lease_expires_at - NOW()))::float8
               FROM sandboxes
              WHERE id = $1",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;

        // Assert.
        assert!(matches!(
            stale_error,
            SubstrateError::RestoreInFlight { .. }
        ));
        assert_eq!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .len(),
            1,
        );
        assert!(lease_remaining_secs > crate::controllers::substrate::NODE_RPC_TIMEOUT_SECS as f64);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failed_restore_pod_create_without_pod_retires_execution_before_rollback()
    -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "suspended").await?;
        insert_ready_full_snapshot_relative_to_lifecycle(
            &pool,
            sandbox_id,
            "s3://space-test/suspend-snapshots/failed-create?region=us-east-1",
            0,
            Uuid::now_v7(),
        )
        .await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let (substrate, request_count) = substrate_with_failed_pod_creates(pool.clone());

        // Act.
        let error = substrate
            .wake_execution_with_metric_identity(
                &sandbox,
                Duration::from_secs(1),
                SandboxApiMetricIdentity::ecw("connect"),
                crate::controllers::substrate::RecoveryCharge::Exempt,
            )
            .await
            .expect_err("injected Kubernetes Pod create failure must reach the caller");

        // Assert.
        assert!(
            matches!(error, SubstrateError::Failed { .. }),
            "unexpected restore error: {error:?}"
        );
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
        let (lifecycle, lease_token, active_executions, execution_state): (
            String,
            Option<Uuid>,
            i64,
            String,
        ) = sqlx::query_as(
            "SELECT s.lifecycle,
                    s.restore_lease_token,
                    COUNT(*) FILTER (WHERE e.active),
                    MAX(e.state)
               FROM sandboxes s
               JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
              WHERE s.id = $1
              GROUP BY s.id",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(lifecycle, "suspended");
        assert_eq!(lease_token, None);
        assert_eq!(
            active_executions, 0,
            "a confirmed-absent Pod must not leave an active execution"
        );
        assert_eq!(execution_state, "replaced");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn disconnected_restore_leader_finishes_node_work_and_cleanup() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "suspended").await?;
        insert_ready_full_snapshot_relative_to_lifecycle(
            &pool,
            sandbox_id,
            "s3://space-test/suspend-snapshots/cancelled-leader?region=us-east-1",
            0,
            Uuid::now_v7(),
        )
        .await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_started = Arc::new(Semaphore::new(0));
        let release_request = Arc::new(Semaphore::new(0));
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            blocking_failed_pod_create_kubernetes_client(
                Arc::clone(&request_count),
                Arc::clone(&request_started),
                Arc::clone(&release_request),
            ),
        );
        let leader_tasks = substrate.task_tracker.clone();
        let shutdown_token = substrate.shutdown_token.clone();

        let handler = tokio::spawn(async move {
            substrate
                .wake_execution_with_metric_identity(
                    &sandbox,
                    Duration::from_secs(5),
                    SandboxApiMetricIdentity::ecw("connect"),
                    crate::controllers::substrate::RecoveryCharge::Exempt,
                )
                .await
        });
        request_started.acquire().await?.forget();
        assert_eq!(
            leader_tasks.len(),
            2,
            "both the coalesced wake run and its detached restore leader must \
             be visible to graceful shutdown"
        );
        handler.abort();
        assert!(
            handler
                .await
                .expect_err("handler must be cancelled")
                .is_cancelled()
        );
        shutdown_token.cancel();
        release_request.add_permits(1);

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let (lifecycle, active_executions): (String, i64) = sqlx::query_as(
                    "SELECT s.lifecycle, COUNT(*) FILTER (WHERE e.active)
                       FROM sandboxes s
                       LEFT JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
                      WHERE s.id = $1
                      GROUP BY s.id",
                )
                .bind(sandbox_id)
                .fetch_one(&pool)
                .await?;
                if lifecycle == "suspended" && active_executions == 0 {
                    return Ok::<_, sqlx::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;

        assert_eq!(request_count.load(Ordering::SeqCst), 1);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !leader_tasks.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached restore leader must leave the tracker after cleanup");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn joined_restore_does_not_resurrect_concurrent_delete() -> Result<(), BoxError> {
        // Arrange: the caller holds a stale restoring snapshot after the owner
        // has already published a routable started execution.
        let db = pplx_space_db_test_fixtures::pg_test_db_with_max_connections(4).await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "restoring").await?;
        let stale_sandbox = Arc::new(
            fetch_sandbox(&pool, sandbox_id)
                .await?
                .expect("inserted sandbox must be visible"),
        );
        publish_completed_restore(&pool, sandbox_id).await?;
        let (substrate, request_count) = substrate_with_failed_pod_creates(pool.clone());
        let controller = SandboxController::new(Substrate::Kubernetes(Box::new(substrate)));

        // Hold the row lock so the joined follower pauses after observing the
        // owner's binding but before its controller completion write lands.
        let mut teardown = pool.begin().await?;
        sqlx::query("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE")
            .bind(sandbox_id)
            .fetch_one(&mut *teardown)
            .await?;

        // Act: start the stale follower and wait until its post-restore UPDATE
        // is blocked behind the teardown transaction.
        let follower_pool = pool.clone();
        let follower_sandbox = Arc::clone(&stale_sandbox);
        let follower = tokio::spawn(async move {
            controller
                .action(
                    &follower_pool,
                    sandbox_id,
                    Some(&follower_sandbox),
                    SandboxApiMetricIdentity::ecw("restore"),
                    SandboxAction::Restore,
                    None,
                )
                .await
        });
        let completion_write_blocked = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if follower.is_finished() {
                    return Ok::<_, sqlx::Error>(false);
                }
                let completion_waiting_on_lock = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS (
                         SELECT 1
                         FROM pg_stat_activity
                         WHERE datname = current_database()
                           AND wait_event_type = 'Lock'
                           AND query LIKE '%SET lifecycle = ''started'', placement = ''on_node''%'
                     )",
                )
                .fetch_one(&pool)
                .await?;
                if completion_waiting_on_lock {
                    return Ok::<_, sqlx::Error>(true);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;

        sqlx::query(
            "UPDATE sandboxes
                SET lifecycle = 'deleting', placement = 'unplaced'
              WHERE id = $1",
        )
        .bind(sandbox_id)
        .execute(&mut *teardown)
        .await?;
        teardown.commit().await?;
        follower.await??;

        // Assert: the joined follower is not an owner and must not overwrite
        // the later teardown state or create another pod.
        let lifecycle: String = sqlx::query_scalar("SELECT lifecycle FROM sandboxes WHERE id = $1")
            .bind(sandbox_id)
            .fetch_one(&pool)
            .await?;
        assert_eq!(lifecycle, "deleting");
        assert!(
            !completion_write_blocked,
            "a joined follower must not attempt a controller lifecycle completion write"
        );
        assert_eq!(request_count.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_wakes_for_one_sandbox_hold_one_claim_connection() -> Result<(), BoxError> {
        // A herd of concurrent wakes for one suspended sandbox must not each
        // occupy a Postgres connection at the restore-lease claim. The claim
        // CAS serializes contenders on the sandbox row's `FOR UPDATE` lock,
        // and a query blocked on a row lock holds its checked-out pool
        // connection for the whole wait — so per-request claim execution
        // turns a request herd into pool saturation. Pod-local callers must
        // coalesce in memory: at most one claim contender per process.
        // Modest fixture pool: the nextest run shares one Postgres server
        // (default max_connections) across every concurrently-running test
        // process, and this test deliberately parks its herd's claim
        // executions on a row lock. Keep the footprint small so the herd
        // cannot starve neighbouring tests of server connections.
        let db = pplx_space_db_test_fixtures::pg_test_db_with_max_connections(8).await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "suspended").await?;
        insert_ready_full_snapshot_relative_to_lifecycle(
            &pool,
            sandbox_id,
            "s3://space-test/suspend-snapshots/claim-herd?region=us-east-1",
            0,
            Uuid::now_v7(),
        )
        .await?;
        let sandbox = Arc::new(
            fetch_sandbox(&pool, sandbox_id)
                .await?
                .expect("inserted sandbox must be visible"),
        );
        let (substrate, _request_count) = substrate_with_failed_pod_creates(pool.clone());
        let substrate = Arc::new(substrate);

        // Reproduce the stampede choke point: hold the sandbox row lock so
        // any claim CAS that reaches Postgres parks on the row lock, visibly
        // holding its connection, while the herd arrives.
        let mut claim_block = pool.begin().await?;
        sqlx::query("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE")
            .bind(sandbox_id)
            .fetch_one(&mut *claim_block)
            .await?;

        let mut herd = Vec::new();
        for _ in 0..4 {
            let substrate = Arc::clone(&substrate);
            let sandbox = Arc::clone(&sandbox);
            herd.push(tokio::spawn(async move {
                substrate
                    .wake_execution_with_metric_identity(
                        &sandbox,
                        Duration::from_secs(2),
                        SandboxApiMetricIdentity::ecw("connect"),
                        crate::controllers::substrate::RecoveryCharge::Exempt,
                    )
                    .await
            }));
        }

        // Wait until at least one contender is parked on the row lock, then
        // keep sampling: the peak number of simultaneously lock-blocked claim
        // executions is the number of pool connections the herd consumed.
        let claim_contenders = || async {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)
                   FROM pg_stat_activity
                  WHERE datname = current_database()
                    AND wait_event_type = 'Lock'
                    AND query LIKE '%kubernetes::store%'
                    AND query LIKE '%WITH candidate AS (%'",
            )
            .fetch_one(&pool)
            .await
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if claim_contenders().await? >= 1 {
                    return Ok::<_, sqlx::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        let mut peak_contenders = 0;
        for _ in 0..40 {
            peak_contenders = peak_contenders.max(claim_contenders().await?);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            peak_contenders <= 1,
            "a wake herd must coalesce per process before the restore-lease \
             claim; observed {peak_contenders} concurrent claim executions \
             each holding a pool connection"
        );

        // Release the row lock and let the herd settle; outcomes are errors
        // here (the fixture fails Pod creates) — this test asserts admission
        // concurrency, not wake success.
        claim_block.rollback().await?;
        for caller in herd {
            let _ = caller.await?;
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn owned_restore_does_not_resurrect_concurrent_delete() -> Result<(), BoxError> {
        if !crate::test_support::enter_sim_worker_process() {
            return Ok(());
        }

        // Arrange: pause an owned Restore after its lifecycle claim and Pod
        // request, then let Delete hold the sandbox row through completion.
        let db = pplx_space_db_test_fixtures::pg_test_db_with_max_connections(4).await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "suspended").await?;
        insert_ready_full_snapshot_relative_to_lifecycle(
            &pool,
            sandbox_id,
            "s3://space-test/suspend-snapshots/owned-race?region=us-east-1",
            -1,
            Uuid::now_v7(),
        )
        .await?;
        let sandbox = Arc::new(
            fetch_sandbox(&pool, sandbox_id)
                .await?
                .expect("inserted sandbox must be visible"),
        );
        let request_started = Arc::new(Semaphore::new(0));
        let release_request = Arc::new(Semaphore::new(0));
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let sim_worker = start_sim_worker().await?;
        let controller = SandboxController::new(Substrate::Kubernetes(Box::new(
            substrate_with_store_client_and_proxy_port(
                pool.clone(),
                blocking_running_pod_kubernetes_client(
                    Arc::clone(&captured_pod_creates),
                    "127.0.0.1",
                    Arc::clone(&request_started),
                    Arc::clone(&release_request),
                ),
                sim_worker.port(),
            ),
        )));

        // Act: once Restore has claimed ownership and reached Pod creation,
        // queue Delete behind the materialization fence, then let it retire
        // the execution before Restore can finalize.
        let restore_pool = pool.clone();
        let restore_sandbox = Arc::clone(&sandbox);
        let restore = tokio::spawn(async move {
            controller
                .action(
                    &restore_pool,
                    sandbox_id,
                    Some(&restore_sandbox),
                    SandboxApiMetricIdentity::ecw("restore"),
                    SandboxAction::Restore,
                    None,
                )
                .await
        });
        let pod_create =
            tokio::time::timeout(Duration::from_secs(10), request_started.acquire()).await;
        let pod_create = match pod_create {
            Ok(pod_create) => pod_create?,
            Err(_) if restore.is_finished() => {
                panic!(
                    "owned Restore failed before Pod creation: {:?}",
                    restore.await?
                )
            }
            Err(error) => return Err(error.into()),
        };
        pod_create.forget();
        let teardown_locked = Arc::new(Semaphore::new(0));
        let release_teardown = Arc::new(Semaphore::new(0));
        let teardown_pool = pool.clone();
        let teardown_locked_signal = Arc::clone(&teardown_locked);
        let release_teardown_signal = Arc::clone(&release_teardown);
        let teardown = tokio::spawn(async move {
            let mut tx = teardown_pool.begin().await?;
            sqlx::query(
                "UPDATE sandboxes
                    SET lifecycle = 'deleting', placement = 'unplaced'
                  WHERE id = $1",
            )
            .bind(sandbox_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "UPDATE sandbox_k8s_executions
                    SET state = 'terminated', active = FALSE, terminated_at = NOW()
                  WHERE sandbox_id = $1 AND active",
            )
            .bind(sandbox_id)
            .execute(&mut *tx)
            .await?;
            teardown_locked_signal.add_permits(1);
            release_teardown_signal.acquire().await?.forget();
            tx.commit().await?;
            Ok::<_, BoxError>(())
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !teardown.is_finished(),
            "Delete must wait while the Kubernetes Pod create is in flight"
        );
        release_request.add_permits(1);
        tokio::time::timeout(Duration::from_secs(10), teardown_locked.acquire())
            .await??
            .forget();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !restore.is_finished(),
            "owned Restore must wait while Delete retires its execution"
        );
        release_teardown.add_permits(1);
        teardown.await??;
        restore
            .await?
            .expect_err("owned Restore must lose finalization to committed teardown");

        // Assert.
        let lifecycle: String = sqlx::query_scalar("SELECT lifecycle FROM sandboxes WHERE id = $1")
            .bind(sandbox_id)
            .fetch_one(&pool)
            .await?;
        assert_eq!(lifecycle, "deleting");
        let active_executions: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(active_executions, 0);
        assert_eq!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .len(),
            1
        );
        Ok(())
    }

    /// An enabled admission gate over an empty cluster (no eligible start
    /// nodes) deterministically rejects every activation charge — no Redis
    /// bucket needed.
    fn rejecting_admission(pool: &PgPool) -> crate::cluster_admission::ClusterSandboxAdmission {
        crate::cluster_admission::ClusterSandboxAdmission::with_config(
            crate::controllers::substrate::Substrate::Direct(Box::new(
                crate::controllers::substrate::direct::DirectSubstrate::new(pool.clone()),
            )),
            crate::cluster_admission::ClusterSandboxAdmissionConfig {
                enabled: true,
                refill_rps: 1.0,
                burst: 1.0,
                redis_url: None,
            },
            pplx_space_feature_flags::FeatureFlagClient::disabled(),
        )
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stale_start_snapshot_cannot_bypass_admission_after_lifecycle_refresh()
    -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "suspended").await?;
        let stale_sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        sqlx::query("UPDATE sandboxes SET lifecycle = 'stopped' WHERE id = $1")
            .bind(sandbox_id)
            .execute(&pool)
            .await?;
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let substrate = Substrate::Kubernetes(Box::new(substrate_with_store_and_client(
            pool.clone(),
            mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
        )));
        let controller =
            SandboxController::with_cluster_admission(substrate, rejecting_admission(&pool));

        // Act.
        let error = controller
            .action(
                &pool,
                sandbox_id,
                Some(&stale_sandbox),
                SandboxApiMetricIdentity::native("start"),
                SandboxAction::Start,
                None,
            )
            .await
            .expect_err("the refreshed cold start must pay admission");

        // Assert.
        assert_eq!(error.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .is_empty(),
            "rejected admission must stop the stale start before pod creation",
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn start_restore_takeover_charges_admission() -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "restoring").await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let substrate = Substrate::Kubernetes(Box::new(substrate_with_store_and_client(
            pool.clone(),
            mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
        )));
        let controller =
            SandboxController::with_cluster_admission(substrate, rejecting_admission(&pool));

        // Act.
        let error = controller
            .action(
                &pool,
                sandbox_id,
                Some(&sandbox),
                SandboxApiMetricIdentity::native("start"),
                SandboxAction::Start,
                None,
            )
            .await
            .expect_err("a restore takeover must pay admission");

        // Assert.
        assert_eq!(error.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .is_empty(),
            "rejected admission must stop the takeover before pod creation",
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn joining_follower_never_charges_admission() -> Result<(), BoxError> {
        // Arrange: a leased restore whose owner completes out-of-band, plus
        // an admission gate that rejects every charge. A follower that joins
        // must never consult the gate, so the join still succeeds.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let completer =
            seed_leased_restore_with_completer(&pool, sandbox_id, SandboxLifecycle::Restoring)
                .await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let admission = rejecting_admission(&pool);
        assert!(
            admission
                .admit_activation(crate::cluster_admission::ClusterActivationClass::Restore)
                .await
                .is_err(),
            "the gate must reject direct charges for this test to mean anything"
        );
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_started = Arc::new(Semaphore::new(0));
        let release_request = Arc::new(Semaphore::new(0));
        let substrate = Substrate::Kubernetes(Box::new(substrate_with_store_and_client(
            pool.clone(),
            blocking_failed_pod_create_kubernetes_client(
                Arc::clone(&request_count),
                Arc::clone(&request_started),
                Arc::clone(&release_request),
            ),
        )));
        let controller = SandboxController::with_cluster_admission(substrate, admission);

        // Act.
        let response = controller
            .action(
                &pool,
                sandbox_id,
                Some(&sandbox),
                SandboxApiMetricIdentity::ecw("connect"),
                SandboxAction::Wake,
                None,
            )
            .await?;
        completer.await??;

        // Assert.
        assert_eq!(response["ok"], true);
        let active_generation: i64 = sqlx::query_scalar(
            "SELECT generation
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(active_generation, 2);
        assert_eq!(request_count.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn lease_takeover_charges_admission_and_releases_claim_on_rejection()
    -> Result<(), BoxError> {
        // Arrange: a restoring row whose lease lapses with no owner outcome,
        // and a rejecting admission gate. The takeover wins the re-claim,
        // must pay the deferred charge, and must hand the lease back when
        // the charge is rejected.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "restoring").await?;
        sqlx::query(
            "UPDATE sandboxes
                SET restore_lease_expires_at = NOW() + INTERVAL '300 milliseconds',
                    restore_lease_token = $2
              WHERE id = $1",
        )
        .bind(sandbox_id)
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_started = Arc::new(Semaphore::new(0));
        let release_request = Arc::new(Semaphore::new(0));
        let substrate = Substrate::Kubernetes(Box::new(substrate_with_store_and_client(
            pool.clone(),
            blocking_failed_pod_create_kubernetes_client(
                Arc::clone(&request_count),
                Arc::clone(&request_started),
                Arc::clone(&release_request),
            ),
        )));
        let controller =
            SandboxController::with_cluster_admission(substrate, rejecting_admission(&pool));

        // Act.
        let err = controller
            .action(
                &pool,
                sandbox_id,
                Some(&sandbox),
                SandboxApiMetricIdentity::ecw("connect"),
                SandboxAction::Wake,
                None,
            )
            .await
            .expect_err("a rejected takeover charge must fail the wake");

        // Assert.
        assert_eq!(err.status(), StatusCode::TOO_MANY_REQUESTS);
        let (lifecycle, lease_live): (String, bool) = sqlx::query_as(
            "SELECT lifecycle, COALESCE(restore_lease_expires_at > NOW(), FALSE)
               FROM sandboxes WHERE id = $1",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            lifecycle, "restoring",
            "a redrive rejection keeps the row restoring"
        );
        assert!(
            !lease_live,
            "the rejected takeover must release the lease it won"
        );
        assert_eq!(
            request_count.load(Ordering::SeqCst),
            0,
            "a rejected charge must never reach pod creation"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rejected_fresh_restore_claim_reverts_to_suspended() -> Result<(), BoxError> {
        // Arrange: a suspended row with no lease; the restore wins a fresh
        // claim, the deferred charge is rejected, and the claim must roll
        // back so the sandbox stays cleanly restorable.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "suspended").await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_started = Arc::new(Semaphore::new(0));
        let release_request = Arc::new(Semaphore::new(0));
        let substrate = Substrate::Kubernetes(Box::new(substrate_with_store_and_client(
            pool.clone(),
            blocking_failed_pod_create_kubernetes_client(
                Arc::clone(&request_count),
                Arc::clone(&request_started),
                Arc::clone(&release_request),
            ),
        )));
        let controller =
            SandboxController::with_cluster_admission(substrate, rejecting_admission(&pool));

        // Act.
        let err = controller
            .action(
                &pool,
                sandbox_id,
                Some(&sandbox),
                SandboxApiMetricIdentity::ecw("connect"),
                SandboxAction::Restore,
                None,
            )
            .await
            .expect_err("a rejected fresh-claim charge must fail the restore");

        // Assert.
        assert_eq!(err.status(), StatusCode::TOO_MANY_REQUESTS);
        let (lifecycle, lease_live): (String, bool) = sqlx::query_as(
            "SELECT lifecycle, COALESCE(restore_lease_expires_at > NOW(), FALSE)
               FROM sandboxes WHERE id = $1",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            lifecycle, "suspended",
            "a rejected fresh claim reverts to suspended"
        );
        assert!(
            !lease_live,
            "the rejected claim must not leave a lease behind"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn expired_recovery_is_reclaimed_once_by_runtime_access() -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active, updated_at)
             VALUES
                 ($1, 1, 'space-sbx-expired-g1', 'space', 'recovering', TRUE,
                  NOW() - make_interval(secs => $2))",
        )
        .bind(sandbox_id)
        .bind(
            i64::try_from(crate::controllers::substrate::NODE_RPC_TIMEOUT_SECS)
                .expect("node RPC timeout seconds fit i64")
                + 1,
        )
        .execute(&pool)
        .await?;
        let sandbox = Arc::new(
            fetch_sandbox(&pool, sandbox_id)
                .await?
                .expect("inserted sandbox must be visible"),
        );
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_started = Arc::new(Semaphore::new(0));
        let release_request = Arc::new(Semaphore::new(0));
        let substrate = substrate_with_store_and_client(
            pool,
            blocking_failed_pod_create_kubernetes_client(
                Arc::clone(&request_count),
                Arc::clone(&request_started),
                Arc::clone(&release_request),
            ),
        );

        // Act.
        let first_substrate = substrate.clone();
        let first_sandbox = Arc::clone(&sandbox);
        let first = tokio::spawn(async move {
            first_substrate
                .ensure_execution_ready_for_access(
                    &first_sandbox,
                    SandboxApiMetricIdentity::ecw("spaced"),
                    Duration::from_secs(1),
                    crate::controllers::substrate::RecoveryCharge::Exempt,
                    None,
                )
                .await
        });
        let first_request =
            tokio::time::timeout(Duration::from_secs(1), request_started.acquire()).await;
        let first_request_started = match first_request {
            Ok(Ok(permit)) => {
                permit.forget();
                true
            }
            Ok(Err(_)) | Err(_) => false,
        };

        let second_substrate = substrate.clone();
        let second_sandbox = Arc::clone(&sandbox);
        let second = tokio::spawn(async move {
            second_substrate
                .ensure_execution_ready_for_access(
                    &second_sandbox,
                    SandboxApiMetricIdentity::ecw("spaced"),
                    Duration::from_secs(1),
                    crate::controllers::substrate::RecoveryCharge::Exempt,
                    None,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let requests_while_first_blocked = request_count.load(Ordering::SeqCst);
        release_request.add_permits(2);
        let _ = tokio::time::timeout(Duration::from_secs(2), first).await??;
        let _ = tokio::time::timeout(Duration::from_secs(2), second).await??;

        // Assert.
        assert!(
            first_request_started,
            "runtime access must reclaim an expired started recovery"
        );
        assert_eq!(
            requests_while_first_blocked, 1,
            "only one runtime access may reclaim an expired started recovery"
        );

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn expired_recovery_reclaimed_by_runtime_access_publishes_completion()
    -> Result<(), BoxError> {
        if !crate::test_support::enter_sim_worker_process() {
            return Ok(());
        }

        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active, updated_at)
             VALUES
                 ($1, 1, 'space-sbx-expired-g1', 'space', 'recovering', TRUE,
                  NOW() - make_interval(secs => $2))",
        )
        .bind(sandbox_id)
        .bind(
            i64::try_from(crate::controllers::substrate::NODE_RPC_TIMEOUT_SECS)
                .expect("node RPC timeout seconds fit i64")
                + 1,
        )
        .execute(&pool)
        .await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let sim_worker = start_sim_worker().await?;
        let substrate = substrate_with_store_client_and_proxy_port(
            pool.clone(),
            running_pod_kubernetes_client(Arc::clone(&captured_pod_creates), "127.0.0.1"),
            sim_worker.port(),
        );

        // Act.
        let access = substrate
            .access_runtime_client(
                SandboxApiMetricIdentity::ecw("spaced"),
                &sandbox,
                ExecutionActivityKind::Access,
                Duration::from_secs(2),
                crate::controllers::substrate::RecoveryCharge::Exempt,
            )
            .await;
        let state: String = sqlx::query_scalar(
            "SELECT state FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;

        // Assert.
        assert!(
            access.is_ok(),
            "runtime access must finish the expired recovery it reclaimed: {access:?}"
        );
        assert_eq!(
            state, "running",
            "successful runtime access must publish recovery completion"
        );
        assert_eq!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .len(),
            1,
            "runtime access must create exactly one replacement Pod"
        );

        Ok(())
    }

    async fn assert_concurrent_started_wakes_issue_one_pod_create(
        seed_active_execution: bool,
    ) -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        if seed_active_execution {
            sqlx::query(
                "INSERT INTO sandbox_k8s_executions
                     (sandbox_id, generation, pod_name, pod_namespace, pod_ip, state, active, created_at)
                 VALUES
                     ($1, 1, 'space-sbx-existing-g1', 'space', '127.0.0.1', 'running', TRUE, NOW())",
            )
            .bind(sandbox_id)
            .execute(&pool)
            .await?;
        }
        let sandbox = Arc::new(
            fetch_sandbox(&pool, sandbox_id)
                .await?
                .expect("inserted sandbox must be visible"),
        );
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_started = Arc::new(Semaphore::new(0));
        let release_request = Arc::new(Semaphore::new(0));
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            blocking_failed_pod_create_kubernetes_client(
                Arc::clone(&request_count),
                Arc::clone(&request_started),
                Arc::clone(&release_request),
            ),
        );

        let first_substrate = substrate.clone();
        let first_sandbox = Arc::clone(&sandbox);
        let first = tokio::spawn(async move {
            first_substrate
                .wake_execution_with_metric_identity(
                    &first_sandbox,
                    Duration::from_secs(1),
                    SandboxApiMetricIdentity::ecw("connect"),
                    crate::controllers::substrate::RecoveryCharge::Exempt,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), request_started.acquire())
            .await??
            .forget();

        let second_substrate = substrate.clone();
        let second_sandbox = Arc::clone(&sandbox);
        let second = tokio::spawn(async move {
            second_substrate
                .wake_execution_with_metric_identity(
                    &second_sandbox,
                    Duration::from_secs(1),
                    SandboxApiMetricIdentity::ecw("connect"),
                    crate::controllers::substrate::RecoveryCharge::Exempt,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let second_finished_while_first_blocked = second.is_finished();
        let requests_while_first_blocked = request_count.load(Ordering::SeqCst);
        release_request.add_permits(2);

        let first_error = tokio::time::timeout(Duration::from_secs(2), first)
            .await??
            .expect_err("injected Kubernetes failure must fail the first wake");
        let second_error = tokio::time::timeout(Duration::from_secs(2), second)
            .await??
            .expect_err("the concurrent wake must return a retryable error");

        assert!(
            !second_finished_while_first_blocked,
            "the concurrent wake should wait for the recovery leader"
        );
        assert_eq!(
            requests_while_first_blocked, 1,
            "concurrent wake must not issue another Kubernetes request"
        );
        assert!(matches!(first_error, SubstrateError::Failed { .. }));
        assert!(
            matches!(
                second_error,
                SubstrateError::Timeout { ref message, .. }
                    if message.contains("recovery attempt failed")
            ),
            "unexpected concurrent wake error: {second_error:?}"
        );
        let state: String = sqlx::query_scalar(
            "SELECT state FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(state, "recovery_failed");

        Ok(())
    }

    async fn assert_ambiguous_pod_create_keeps_started_recovery_pending(
        post_status: StatusCode,
    ) -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            ambiguous_failed_pod_create_kubernetes_client(post_status),
        );

        let error = substrate
            .wake_execution_with_metric_identity(
                &sandbox,
                Duration::from_secs(1),
                SandboxApiMetricIdentity::ecw("connect"),
                crate::controllers::substrate::RecoveryCharge::Exempt,
            )
            .await
            .expect_err("an unobservable pod create must remain retryable");

        assert!(
            matches!(error, SubstrateError::RestoreInFlight { .. }),
            "an ambiguous create must preserve recovery ownership: {error:?}"
        );
        let lifecycle: String =
            sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
                .bind(sandbox_id)
                .fetch_one(&pool)
                .await?;
        assert_eq!(lifecycle, "started");
        let execution: (String, bool) = sqlx::query_as(sql!(
            "SELECT state, active
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(execution, ("recovering".to_string(), true));

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ambiguous_pod_create_keeps_started_recovery_pending() -> Result<(), BoxError> {
        assert_ambiguous_pod_create_keeps_started_recovery_pending(
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pod_create_conflict_with_failed_get_keeps_started_recovery_pending()
    -> Result<(), BoxError> {
        assert_ambiguous_pod_create_keeps_started_recovery_pending(StatusCode::CONFLICT).await
    }

    /// The regression point is the 3.2-second wait: a follower that gave up
    /// at the old 3.1-second cutoff never saw the owner's `running`. So the
    /// owner still publishes past that cutoff, and only the follower's poll
    /// pacing is shortened — its re-check after the publish, which at
    /// production's doubling backoff lands a full 3.2-second lap later.
    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_started_wake_returns_when_owner_publishes_running() -> Result<(), BoxError>
    {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_ip, state, active, created_at)
             VALUES
                 ($1, 1, 'space-sbx-existing-g1', 'space', '127.0.0.1', 'recovering', TRUE, NOW())",
        )
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
        let sandbox = Arc::new(
            fetch_sandbox(&pool, sandbox_id)
                .await?
                .expect("inserted sandbox must be visible"),
        );
        // Production's initial delay, capped so a lap cannot outlast the
        // publish by more than one poll; the join's growth shape is intact.
        let substrate = substrate_with_store(pool.clone())
            .with_recovery_join_backoff(Duration::from_millis(100), Duration::from_millis(200));

        let follower_substrate = substrate.clone();
        let follower_sandbox = Arc::clone(&sandbox);
        let follower = tokio::spawn(async move {
            follower_substrate
                .wake_execution_with_metric_identity(
                    &follower_sandbox,
                    Duration::from_secs(8),
                    SandboxApiMetricIdentity::ecw("connect"),
                    crate::controllers::substrate::RecoveryCharge::Exempt,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(3_200)).await;
        assert!(
            !follower.is_finished(),
            "the follower must remain joined past the old 3.1-second cutoff"
        );

        sqlx::query(
            "UPDATE sandbox_k8s_executions
             SET state = 'running', updated_at = NOW()
             WHERE sandbox_id = $1 AND generation = 1 AND active",
        )
        .bind(sandbox_id)
        .execute(&pool)
        .await?;

        let binding = tokio::time::timeout(Duration::from_secs(4), follower).await???;
        assert_eq!(binding.execution_object_id, "space-sbx-existing-g1");
        assert_eq!(binding.execution_generation, 1);

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_started_wake_stops_waiting_after_readiness_budget() -> Result<(), BoxError>
    {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_ip, state, active, created_at)
             VALUES
                 ($1, 1, 'space-sbx-existing-g1', 'space', '127.0.0.1', 'recovering', TRUE, NOW())",
        )
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let substrate = substrate_with_store(pool);

        let started_at = StdInstant::now();
        let error = tokio::time::timeout(
            Duration::from_millis(2_500),
            substrate.wake_execution_with_metric_identity(
                &sandbox,
                Duration::from_secs(1),
                SandboxApiMetricIdentity::ecw("connect"),
                crate::controllers::substrate::RecoveryCharge::Exempt,
            ),
        )
        .await?
        .expect_err("the follower must stop waiting while recovery remains in flight");

        let elapsed = started_at.elapsed();
        assert!(elapsed >= Duration::from_millis(900));
        assert!(elapsed < Duration::from_millis(2_500));
        assert!(
            matches!(error, SubstrateError::RestoreInFlight { .. }),
            "unexpected follower result: {error:?}"
        );

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_started_wake_stops_joining_when_owner_lease_expires() -> Result<(), BoxError>
    {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_ip, state, active, created_at)
             VALUES
                 ($1, 1, 'space-sbx-existing-g1', 'space', '127.0.0.1', 'recovering', TRUE, NOW())",
        )
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
        let owner_updated_at: OffsetDateTime = sqlx::query_scalar(
            "UPDATE sandbox_k8s_executions
             SET updated_at = NOW() - make_interval(secs => $2)
             WHERE sandbox_id = $1 AND generation = 1 AND active
             RETURNING updated_at",
        )
        .bind(sandbox_id)
        .bind(i64::try_from(
            crate::controllers::substrate::NODE_RPC_TIMEOUT_SECS - 1,
        )?)
        .fetch_one(&pool)
        .await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let substrate = substrate_with_store(pool.clone());

        let started_at = StdInstant::now();
        let error = tokio::time::timeout(
            Duration::from_millis(2_500),
            substrate.wake_execution_with_metric_identity(
                &sandbox,
                Duration::from_secs(5),
                SandboxApiMetricIdentity::ecw("connect"),
                crate::controllers::substrate::RecoveryCharge::Exempt,
            ),
        )
        .await?
        .expect_err("the follower must stop joining when owner authority expires");

        let execution: (String, OffsetDateTime) = sqlx::query_as(
            "SELECT state, updated_at
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1 AND generation = 1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert!(started_at.elapsed() >= Duration::from_millis(500));
        assert!(matches!(error, SubstrateError::RestoreInFlight { .. }));
        assert_eq!(execution.0, "recovering");
        assert_eq!(execution.1, owner_updated_at, "a follower must not reclaim");

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn process_start_does_not_replace_stale_k8s_execution() -> Result<(), BoxError> {
        if !crate::test_support::enter_sim_worker_process() {
            return Ok(());
        }

        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active, created_at)
             VALUES
                 ($1, 1, 'space-sbx-old-g1', 'space', 'creating', TRUE, NOW() - INTERVAL '10 minutes')",
        )
        .bind(sandbox_id)
        .execute(&pool)
        .await?;

        let sim_worker = start_sim_worker().await?;
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let controller = SandboxController::new(Substrate::Kubernetes(Box::new(
            substrate_with_store_client_and_proxy_port(
                pool.clone(),
                running_pod_kubernetes_client(Arc::clone(&captured_pod_creates), "127.0.0.1"),
                sim_worker.port(),
            ),
        )));
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");

        let start = match controller
            .spaced_start_process(
                &pool,
                &sandbox,
                SandboxApiMetricIdentity::ecw("spaced"),
                start_process_request(sandbox_id),
                Duration::from_secs(1),
            )
            .await
        {
            Ok(_) => panic!("stale execution must not be recovered by Process.Start"),
            Err(err) => err,
        };
        assert_eq!(start.status(), StatusCode::SERVICE_UNAVAILABLE);

        assert_eq!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .len(),
            0,
            "Process/Start must not create a replacement sandbox pod"
        );
        let rows: Vec<(i64, String, bool, bool)> = sqlx::query_as(
            "SELECT generation, state, active, terminated_at IS NULL
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1
             ORDER BY generation",
        )
        .bind(sandbox_id)
        .fetch_all(&pool)
        .await?;
        assert_eq!(
            rows,
            vec![(1, "creating".to_string(), true, true)],
            "Process/Start must not deactivate or replace the active execution"
        );

        Ok(())
    }

    #[test]
    fn dedupe_inventory_prefers_ready_over_not_ready() {
        let id = Uuid::from_u128(1);
        let older = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
        let newer = OffsetDateTime::from_unix_timestamp(2_000_000).unwrap();
        // Newer non-ready (starting) + older ready (steady) — Ready must win
        // even though it's older, since the user-visible state is what matters
        // for the inventory listing.
        let entries = dedupe_inventory_entries(vec![
            inventory_entry(id, "registered", newer),
            inventory_entry(id, "ready", older),
        ]);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].state.as_deref(), Some("ready"));
        assert_eq!(entries[0].created_at, older);
    }

    #[test]
    fn dedupe_inventory_breaks_tie_by_newer_created_at() {
        let id = Uuid::from_u128(2);
        let older = OffsetDateTime::from_unix_timestamp(1_000_000).unwrap();
        let newer = OffsetDateTime::from_unix_timestamp(2_000_000).unwrap();
        let entries = dedupe_inventory_entries(vec![
            inventory_entry(id, "ready", older),
            inventory_entry(id, "ready", newer),
        ]);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].created_at, newer);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn inventory_skips_kubernetes_node_with_invalid_internal_ip() -> Result<(), BoxError> {
        let substrate = substrate_with_inventory_caches(
            vec![node_api_pod_on_node("space-worker-node-a", "node-a")],
            vec![k8s_node_with_internal_ip("node-a", "not-an-ip")],
        );

        let entries = substrate
            .list_inventory_nodes(Some(SpaceNodeRole::Worker.as_str()))
            .await?
            .expect("kubernetes substrate supplies inventory");

        assert!(
            entries.is_empty(),
            "invalid node address should skip only that inventory row"
        );
        Ok(())
    }

    /// `list_inventory_nodes` derives node state from node-api *pod* readiness,
    /// which lags Kubernetes Node readiness during a node-api rollout. Driving
    /// the real listing (not the store directly) proves the caller uses
    /// insert-only state semantics: once a node row exists as `ready`, a later
    /// listing that observes an unready pod must not downgrade it to
    /// `registered`. This fails if the listing reverts to `Manage`.
    #[tokio::test(flavor = "current_thread")]
    async fn inventory_listing_does_not_downgrade_ready_node_on_unready_pod() -> Result<(), BoxError>
    {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let node_name = "space-worker-inventory-race";
        let pod_name = "space-worker-inventory-race-pod";

        // First listing observes a ready pod and seeds the node row as `ready`.
        let ready_substrate = substrate_with_inventory_caches_and_pool(
            vec![node_api_pod_on_node(pod_name, node_name)],
            vec![k8s_node_with_internal_ip(node_name, "10.0.0.60")],
            db.pool.clone(),
        );
        ready_substrate
            .list_inventory_nodes(Some(SpaceNodeRole::Worker.as_str()))
            .await?
            .expect("kubernetes substrate supplies inventory");
        let seeded: String = sqlx::query_scalar(
            "SELECT n.state FROM nodes n
             JOIN node_k8s k ON k.node_id = n.id
             WHERE k.k8s_node_name = $1",
        )
        .bind(node_name)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(seeded, "ready", "first listing must seed the node as ready");

        // A later listing during a node-api rollout observes an unready pod on
        // the same (Kubernetes-ready) node. Insert-only semantics must leave
        // the persisted `ready` state intact.
        let unready_substrate = substrate_with_inventory_caches_and_pool(
            vec![unready_node_api_pod_on_node(pod_name, node_name)],
            vec![k8s_node_with_internal_ip(node_name, "10.0.0.60")],
            db.pool.clone(),
        );
        unready_substrate
            .list_inventory_nodes(Some(SpaceNodeRole::Worker.as_str()))
            .await?
            .expect("kubernetes substrate supplies inventory");
        let after: String = sqlx::query_scalar(
            "SELECT n.state FROM nodes n
             JOIN node_k8s k ON k.node_id = n.id
             WHERE k.k8s_node_name = $1",
        )
        .bind(node_name)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(
            after, "ready",
            "inventory listing observing an unready pod must not downgrade a live ready node"
        );
        Ok(())
    }

    fn test_sandbox_with_lifecycle(lifecycle: &str) -> Sandbox {
        Sandbox {
            id: SandboxId::from_uuid(Uuid::nil()),
            name: None,
            user_id: UserId::from_uuid(Uuid::nil()),
            org_id: OrgId::from_uuid(Uuid::nil()),
            owner_source: SandboxOwnerSource::Local,
            upstream_issuer: None,
            upstream_org_id: None,
            upstream_subject_id: None,
            managed_by: "k8s".to_string(),
            lifecycle: lifecycle.to_string(),
            placement: "on_node".to_string(),
            active_node_id: None,
            active_node_name: None,
            active_node_role: None,
            active_node_address: None,
            active_node_managed_by: None,
            active_execution_generation: None,
            active_pod_uid: None,
            active_activation_attempt_id: None,
            ecw_credential_epoch: pplx_space_common_core::types::EcwCredentialEpoch::new(1)
                .expect("credential epoch is valid"),
            target_node_id: None,
            placement_changed_at: None,
            template_id: Some("seed".to_string()),
            content_hash: None,
            release_tags: Vec::new(),
            vcpu: None,
            memory_mib: None,
            disk_mib: None,
            disk_io_max_operations_per_sec: 1_000,
            disk_io_max_bandwidth_mib_per_sec: 100,
            env: sqlx::types::Json(BTreeMap::new()),
            metadata: sqlx::types::Json(BTreeMap::new()),
            auto_lifecycle: sqlx::types::Json(
                pplx_space_common_core::types::AutoLifecyclePolicy::disabled(),
            ),
            auto_lifecycle_state: sqlx::types::Json(
                pplx_space_common_core::types::AutoLifecycleRuntimeState::default(),
            ),
            network_policy: Some(sqlx::types::Json(
                pplx_space_net_common::NetworkPolicyV1::default(),
            )),
            network_policy_generation: 1,
            template_network_policy: None,
            template_network_policy_generation: None,
            snapshots_enabled_desired: false,
            command_ready_pending: false,
            volume_encryption: false,
            next_auto_event_at: None,
            last_used_at: None,
            last_access_at: None,
            last_command_at: None,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            updated_at: time::OffsetDateTime::UNIX_EPOCH,
        }
    }

    /// The boundary has to reach the *PodSpec* on every context
    /// `sandbox_pod_request` renders. Each arm is a one-expression
    /// pass-through, so a single dropped `node_group_placement` would render a
    /// pod byte-identical to a pre-node-group one, and no other test would
    /// notice.
    ///
    /// Scope, since the name reads broader than the coverage: this is the three
    /// `pod_ops` contexts. The substrate adapter builds two more requests
    /// directly (the cold create and the fork restore) and `build_pool_pod` has
    /// its own test — reverting either adapter site to `None` would pass here.
    ///
    /// Rendering rather than inspecting the request matters: the pod builder
    /// cross-checks the placement's org and template against the pod's own and
    /// answers a mismatch with an *unsatisfiable* boundary (`In (C)` AND
    /// `NotIn (C)`). Asserting the term is present and alone therefore also
    /// asserts the resolver's subject agrees with the pod being built.
    // A Tokio context: building the substrate constructs a `PgPool`, which
    // needs a reactor even though this test issues no query.
    #[tokio::test(flavor = "current_thread")]
    async fn every_pod_build_path_renders_the_node_group_boundary() {
        let substrate = substrate_with_inventory_caches(Vec::new(), Vec::new());
        let sandbox = test_sandbox_with_lifecycle("started");
        let route = SandboxActivationAttemptId::new();
        let operation = SandboxActivationAttemptId::new_after(route).unwrap();
        let execution = SandboxExecutionRow {
            generation: 3,
            pod_name: "space-sbx-test-g3".to_string(),
            pod_materialized: false,
            state: ActiveExecutionState::Creating,
            create_age_expired: false,
            activation_attempt_id: Some(operation),
            route_activation_id: Some(route),
        };
        // Resolved for the sandbox's own org and template, which is what the
        // production resolver does — `sandbox.org_id` is nil here and
        // `template_id` is `seed`.
        let group = NodeGroup {
            id: NodeGroupId::new(),
            name: NodeGroupName::parse("workers").expect("valid group name"),
            policy: NodeGroupPolicy::Generic { is_default: true },
            template_allowlist: std::collections::BTreeSet::new(),
        };
        let template = TemplateCompatSpec {
            name: TemplateName::parse("seed").expect("valid template name"),
            org_id: None,
            visibility: TemplateVisibility::Generic,
        };
        let placement = NodeGroupPlacement::for_caller(
            Some(sandbox.org_id),
            &template,
            std::slice::from_ref(&group),
        )
        .expect("a generic group admits a generic template");
        let owner = SandboxOwner::platform_user(sandbox.user_id);
        let auto_lifecycle_json = "{}";
        let network_policy_json = "{}";

        for context in [
            PodCreateContext::OnDemand,
            PodCreateContext::Restore(RestoreContext::UserRestore {
                suspend_restore_storage_uri: None,
                suspend_restore_source_node: Some("source-node"),
                direct_node_name: Some("source-node"),
                preferred_node_name: None,
            }),
            PodCreateContext::Restore(RestoreContext::Evacuation {
                suspend_restore_storage_uri: Some("s3://bucket/key"),
                extra_tolerations: &[],
                extra_required_affinity: None,
            }),
        ] {
            let label = match &context {
                PodCreateContext::OnDemand => "on-demand",
                PodCreateContext::Restore(RestoreContext::UserRestore { .. }) => "user-restore",
                PodCreateContext::Restore(RestoreContext::Evacuation { .. }) => "evacuation",
            };
            let request = substrate.sandbox_pod_request(
                &sandbox,
                &execution,
                crate::controllers::substrate::kubernetes::pod_ops::SandboxPodRequestInputs {
                    sandbox_owner: &owner,
                    auto_lifecycle_json,
                    network_policy_json,
                    node_group_placement: Some(&placement),
                },
                context,
            );
            let pod = build_sandbox_pod(&sandbox_pod_config(&substrate.k8s), request);
            assert_eq!(
                pod.metadata.annotations.as_ref().unwrap().get(
                    pplx_space_common_core::kubernetes::SANDBOX_ACTIVATION_ATTEMPT_ID_ANNOTATION
                ),
                Some(&route.to_string()),
                "{label}: shim bootstrap must install the published route, not the operation lease"
            );
            let terms = pod
                .spec
                .as_ref()
                .and_then(|spec| spec.affinity.as_ref())
                .and_then(|affinity| affinity.node_affinity.as_ref())
                .and_then(|node_affinity| {
                    node_affinity
                        .required_during_scheduling_ignored_during_execution
                        .as_ref()
                })
                .map(|selector| selector.node_selector_terms.clone())
                .unwrap_or_default();
            let node_group_expressions: Vec<_> = terms
                .iter()
                .flat_map(|term| term.match_expressions.clone().unwrap_or_default())
                .filter(|expression| expression.key == SANDBOX_NODE_GROUP_LABEL)
                .collect();
            assert!(
                !node_group_expressions.is_empty(),
                "{label} pod dropped the node-group boundary"
            );
            for expression in &node_group_expressions {
                assert_eq!(
                    (expression.operator.as_str(), expression.values.as_deref()),
                    ("In", Some(["workers".to_string()].as_slice())),
                    "{label} pod rendered an unexpected node-group term"
                );
            }
        }
    }

    #[test]
    fn stale_existing_pod_reason_treats_suspended_sandbox_as_stale() {
        let sandbox = test_sandbox_with_lifecycle("suspended");
        let pod = Pod::default();

        assert_eq!(
            stale_existing_pod_reason(&sandbox, &pod, None),
            Some(StalePodReason::SandboxSuspended)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stopped_restart_replaces_only_execution_retired_by_current_stop()
    -> Result<(), BoxError> {
        for predates_stop in [true, false] {
            let db = pplx_space_db_test_fixtures::pg_test_db().await?;
            let id = SandboxId::new();
            insert_test_sandbox_row(&db.pool, id, "stopped").await?;
            let pod_name = format!("space-sbx-{}-g2", id.as_uuid().simple());
            insert_k8s_execution(
                &db.pool,
                id,
                2,
                &pod_name,
                "restart-pod",
                Some("127.0.0.1"),
                true,
            )
            .await?;
            let old_attempt = SandboxActivationAttemptId::new();
            sqlx::query(sql!(
                "UPDATE sandbox_k8s_executions SET
                    created_at = (
                        SELECT lifecycle_changed_at + make_interval(secs => $3)
                        FROM sandboxes WHERE id = $1
                    ),
                    route_activation_id = $2, activation_attempt_id = $2,
                    activation_attempt_target_state = 'started',
                    activation_attempt_expires_at = NOW() - INTERVAL '1 second'
                 WHERE sandbox_id = $1"
            ))
            .bind(id)
            .bind(old_attempt)
            .bind(if predates_stop { -1.0_f64 } else { 1.0_f64 })
            .execute(&db.pool)
            .await?;
            let pod = Pod {
                metadata: kube::api::ObjectMeta {
                    name: Some(pod_name),
                    namespace: Some("space".into()),
                    uid: Some("restart-pod".into()),
                    annotations: Some(BTreeMap::from([(
                        SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION.into(),
                        "source-node".into(),
                    )])),
                    ..Default::default()
                },
                status: Some(PodStatus {
                    phase: Some("Running".into()),
                    pod_ip: Some("127.0.0.1".into()),
                    ..Default::default()
                }),
                ..Default::default()
            };
            let creates = Arc::new(Mutex::new(Vec::new()));
            let client = running_pod_kubernetes_client_with_create_barrier(
                Arc::clone(&creates),
                "127.0.0.1",
                None,
                false,
                Some(serde_json::to_value(&pod)?),
            );
            let substrate = substrate_with_store_client_proxy_port_and_pod(
                db.pool.clone(),
                client,
                DEFAULT_SANDBOX_PROXY_PORT,
                Some(pod),
            );
            let sandbox = fetch_sandbox(&db.pool, id).await?.unwrap();
            substrate
                .ensure_pod_exists(
                    &sandbox,
                    None,
                    Some("source-node"),
                    None,
                    PodMaterializationOwnership::new(
                        None,
                        None,
                        Some(ActivationAttemptAssignment::new(
                            SandboxActivationAttemptId::new(),
                            SandboxTransitionTargetState::Started,
                        )),
                    ),
                )
                .await?;
            {
                let created = creates.lock().unwrap();
                if predates_stop {
                    assert_eq!(created.len(), 1, "the stopped predecessor must be replaced");
                    assert_eq!(
                        created[0]
                            .pointer("/metadata/annotations/space.pplx.net~1checkpoint")
                            .and_then(serde_json::Value::as_str),
                        Some("stopped")
                    );
                } else {
                    assert!(
                        created.is_empty(),
                        "retry must reattach to the running successor"
                    );
                }
            }
            assert_eq!(
                substrate
                    .store
                    .active_execution_status(id)
                    .await?
                    .unwrap()
                    .generation,
                if predates_stop { 3 } else { 2 }
            );
        }
        Ok(())
    }

    #[test]
    fn execution_pod_lookup_in_flight_includes_started_recovery() {
        assert!(execution_pod_create_in_flight(
            ActiveExecutionState::Creating
        ));
        assert!(!execution_pod_create_in_flight(
            ActiveExecutionState::Recovering
        ));
        assert!(!execution_pod_create_in_flight(
            ActiveExecutionState::Running
        ));

        assert!(execution_pod_lookup_in_flight(
            ActiveExecutionState::Creating
        ));
        assert!(execution_pod_lookup_in_flight(
            ActiveExecutionState::Recovering
        ));
        assert!(!execution_pod_lookup_in_flight(
            ActiveExecutionState::Running
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn missing_recovering_execution_is_left_for_recovery_owner() -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active)
             VALUES ($1, 1, 'space-sbx-recovering-g1', 'space', 'recovering', TRUE)",
        )
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            mock_kubernetes_client(Arc::new(Mutex::new(Vec::new()))),
        );

        // Act.
        let pod_ip = substrate.resolve_pod_ip(sandbox_id).await?;

        // Assert.
        assert_eq!(pod_ip, None);
        let state: Option<String> = sqlx::query_scalar(
            "SELECT state
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1 AND generation = 1 AND active",
        )
        .bind(sandbox_id)
        .fetch_optional(&pool)
        .await?;
        assert_eq!(state.as_deref(), Some("recovering"));

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn recent_succeeded_pod_without_suspend_ownership_is_cleaned_up() -> Result<(), BoxError>
    {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let pod_name = "space-sbx-publishing-g1";
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_uid,
                  pod_ip, state, active)
             VALUES ($1, 1, $2, 'space', 'publishing-pod-uid',
                     '127.0.0.1', 'running', TRUE)",
        ))
        .bind(sandbox_id)
        .bind(pod_name)
        .execute(&pool)
        .await?;
        let finished_at = jiff::Timestamp::now().to_string();
        let terminal_pod: Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": "space",
                "uid": "publishing-pod-uid"
            },
            "status": {
                "phase": "Succeeded",
                "containerStatuses": [{
                    "name": "sandbox",
                    "image": "space-node-stack:test",
                    "imageID": "space-node-stack@test",
                    "ready": false,
                    "restartCount": 0,
                    "state": {
                        "terminated": {
                            "exitCode": 0,
                            "finishedAt": finished_at
                        }
                    }
                }]
            }
        }))?;
        let deleted_pods = Arc::new(AtomicUsize::new(0));
        let substrate = substrate_with_store_client_proxy_port_pod_and_nodes(
            pool.clone(),
            replace_existing_pod_kubernetes_client(
                terminal_pod.clone(),
                Arc::new(Mutex::new(Vec::new())),
                Arc::clone(&deleted_pods),
            ),
            DEFAULT_SANDBOX_PROXY_PORT,
            Some(terminal_pod),
            Vec::new(),
        );

        assert_eq!(substrate.resolve_pod_ip(sandbox_id).await?, None);
        assert_eq!(deleted_pods.load(Ordering::SeqCst), 1);
        let execution_active: bool = sqlx::query_scalar(sql!(
            "SELECT active
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND generation = 1",
        ))
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert!(!execution_active);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn recent_succeeded_pod_with_suspend_ownership_preserves_publication()
    -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let pod_name = "space-sbx-publishing-g1";
        insert_test_sandbox_row(&pool, sandbox_id, "suspending").await?;
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_uid,
                  pod_ip, state, active)
             VALUES ($1, 1, $2, 'space', 'publishing-pod-uid',
                     '127.0.0.1', 'running', TRUE)",
        ))
        .bind(sandbox_id)
        .bind(pod_name)
        .execute(&pool)
        .await?;
        let finished_at = jiff::Timestamp::now().to_string();
        let terminal_pod: Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": "space",
                "uid": "publishing-pod-uid"
            },
            "status": {
                "phase": "Succeeded",
                "containerStatuses": [{
                    "name": "sandbox",
                    "image": "space-node-stack:test",
                    "imageID": "space-node-stack@test",
                    "ready": false,
                    "restartCount": 0,
                    "state": {
                        "terminated": {
                            "exitCode": 0,
                            "finishedAt": finished_at
                        }
                    }
                }]
            }
        }))?;
        let deleted_pods = Arc::new(AtomicUsize::new(0));
        let substrate = substrate_with_store_client_proxy_port_pod_and_nodes(
            pool.clone(),
            replace_existing_pod_kubernetes_client(
                terminal_pod.clone(),
                Arc::new(Mutex::new(Vec::new())),
                Arc::clone(&deleted_pods),
            ),
            DEFAULT_SANDBOX_PROXY_PORT,
            Some(terminal_pod),
            Vec::new(),
        );

        let error = substrate
            .resolve_pod_ip(sandbox_id)
            .await
            .expect_err("recent successful suspension must defer to node publication");

        assert!(matches!(error, SubstrateError::SuspendInFlight { .. }));
        assert_eq!(deleted_pods.load(Ordering::SeqCst), 0);
        let execution_active: bool = sqlx::query_scalar(sql!(
            "SELECT active
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND generation = 1",
        ))
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert!(execution_active);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn started_recovery_without_prior_state_materializes_on_demand() -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active)
             VALUES ($1, 2, 'space-sbx-recovering-g2', 'space', 'running', TRUE)"
        ))
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let recovery_generation = match substrate_with_store(pool.clone())
            .store
            .claim_started_recovery(sandbox_id)
            .await?
        {
            StartedRecoveryClaim::ExistingExecution { generation } => generation,
            claim => panic!("expected the tracked execution recovery claim, got {claim:?}"),
        };
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
        );

        // Act.
        substrate
            .ensure_pod_exists(
                &sandbox,
                None,
                None,
                None,
                PodMaterializationOwnership::new(
                    Some(StartedRecoveryOwnership::ExistingExecution {
                        generation: recovery_generation,
                    }),
                    None,
                    None,
                ),
            )
            .await?;

        // Assert.
        let created_pods = captured_pod_creates.lock().expect("capture mutex poisoned");
        assert_eq!(created_pods.len(), 1);
        let annotations = created_pods[0]
            .pointer("/metadata/annotations")
            .and_then(serde_json::Value::as_object)
            .expect("created pod request should carry annotations");
        assert_eq!(
            annotation_value(annotations, SANDBOX_EXECUTION_GENERATION_ANNOTATION),
            "2"
        );
        assert!(!annotations.contains_key(SANDBOX_CHECKPOINT_ANNOTATION));

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unavailable_started_recovery_source_preserves_claim_across_retries()
    -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let artifact_id = Uuid::now_v7();
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(artifact_id)
        .execute(&pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active)
             VALUES ($1, 1, 'space-sbx-retired-g1', 'space', 'replaced', FALSE)"
        ))
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_started = Arc::new(Semaphore::new(0));
        let release_request = Arc::new(Semaphore::new(2));
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            blocking_missing_pod_kubernetes_client(
                Arc::clone(&request_count),
                request_started,
                release_request,
            ),
        );
        let ownership = PodMaterializationOwnership::new(
            Some(StartedRecoveryOwnership::ExistingExecution { generation: 1 }),
            None,
            None,
        );

        // Act.
        for _ in 0..2 {
            let error = substrate
                .ensure_pod_exists(&sandbox, None, None, None, ownership.clone())
                .await
                .expect_err("unverified prior state must fail closed");
            assert!(matches!(
                error,
                SubstrateError::RestoreContextUnavailable { .. }
            ));
        }

        // Assert.
        let executions: Vec<(i64, bool)> = sqlx::query_as(sql!(
            "SELECT generation, active
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1
              ORDER BY generation"
        ))
        .bind(sandbox_id)
        .fetch_all(&pool)
        .await?;
        assert_eq!(executions, vec![(1, false), (2, true)]);
        assert_eq!(
            request_count.load(Ordering::SeqCst),
            1,
            "the first request must authoritatively confirm the successor pod is absent"
        );

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn missing_pod_observation_preserves_failed_started_recovery_claim_across_retries()
    -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active)
             VALUES ($1, 1, 'space-sbx-missing-g1', 'space', 'running', TRUE)"
        ))
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            mock_kubernetes_client(Arc::new(Mutex::new(Vec::new()))),
        );

        // Act.
        for _ in 0..2 {
            let error = substrate
                .runtime_client(
                    sandbox_id,
                    crate::controllers::substrate::RecoveryCharge::Exempt,
                )
                .await
                .expect_err("unverified prior state must fail closed");
            assert_eq!(error.status(), axum::http::StatusCode::PRECONDITION_FAILED);
            assert_eq!(error.retry_after_header(), None);
        }

        // Assert.
        let executions: Vec<(i64, bool, String)> = sqlx::query_as(sql!(
            "SELECT generation, active, state
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1
              ORDER BY generation"
        ))
        .bind(sandbox_id)
        .fetch_all(&pool)
        .await?;
        assert_eq!(
            executions,
            vec![
                (1, false, "forgotten".to_string()),
                (2, true, "recovery_failed".to_string()),
            ]
        );
        let activation_attempt_cleared: bool = sqlx::query_scalar(sql!(
            "SELECT activation_attempt_id IS NULL
                    AND activation_attempt_expires_at IS NULL
                    AND activation_attempt_target_state IS NULL
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active"
        ))
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert!(activation_attempt_cleared);

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn started_recovery_reports_pending_restore_source_without_naming_suspend_finalization()
    -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let artifact_id = Uuid::now_v7();
        let source_node_id = NodeId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(sql!(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '10.0.0.10', 'worker', 'k8s', 'ready')"
        ))
        .bind(source_node_id)
        .execute(&pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO node_k8s (node_id, k8s_node_name)
             VALUES ($1, 'source-node')"
        ))
        .bind(source_node_id)
        .execute(&pool)
        .await?;
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(artifact_id)
        .execute(&pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO snapshots
                 (sandbox_id, type, state, source_node_id, org_id,
                  local_artifact_id, artifact_kind, artifact_origin)
             VALUES ($1, 'full', 'creating', $2,
                     '00000000-0000-0000-0000-000000000001',
                     $3, 'full', 'suspend')"
        ))
        .bind(sandbox_id)
        .bind(source_node_id)
        .bind(artifact_id)
        .execute(&pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active)
             VALUES ($1, 2, 'space-sbx-missing-g2', 'space', 'recovering', TRUE)"
        ))
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_and_client(
            pool,
            mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
        );

        // Act.
        let error = substrate
            .ensure_pod_exists(
                &sandbox,
                None,
                None,
                None,
                PodMaterializationOwnership::new(
                    Some(StartedRecoveryOwnership::ExistingExecution { generation: 2 }),
                    None,
                    None,
                ),
            )
            .await
            .expect_err("an unavailable local source must defer recovery");

        // Assert.
        assert!(
            error
                .to_string()
                .contains("restore source is not yet portable"),
            "unexpected recovery deferral: {error:?}"
        );
        assert!(matches!(error, SubstrateError::RestoreSourcePending { .. }));
        assert!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .is_empty()
        );

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn missing_started_recovery_remote_restore_replaces_generation() -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let artifact_id = Uuid::now_v7();
        let storage_uri = "s3://space-test/suspend-snapshots/ready?region=us-east-1";
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(artifact_id)
        .execute(&pool)
        .await?;
        insert_ready_full_snapshot_relative_to_lifecycle(
            &pool,
            sandbox_id,
            storage_uri,
            1,
            artifact_id,
        )
        .await?;
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active)
             VALUES
                 ($1, 2, 'space-sbx-missing-g2', 'space', 'missing-pod-uid',
                  'recovering', TRUE)"
        ))
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
        );
        let assignment = ActivationAttemptAssignment::new(
            SandboxActivationAttemptId::new(),
            SandboxTransitionTargetState::Started,
        );

        // Act.
        substrate
            .ensure_pod_exists(
                &sandbox,
                None,
                None,
                None,
                PodMaterializationOwnership::new(
                    Some(StartedRecoveryOwnership::ExistingExecution { generation: 2 }),
                    None,
                    Some(assignment.clone()),
                ),
            )
            .await?;

        // Assert.
        let current = substrate
            .store
            .active_execution_status(sandbox_id)
            .await?
            .unwrap();
        assert_eq!(current.generation, 3);
        assert_eq!(
            assignment.assigned(),
            current.activation_attempt_id.map(|id| (id, 3)),
            "cleanup must follow the atomically transferred operation into its replacement generation"
        );
        {
            let created_pods = captured_pod_creates.lock().expect("capture mutex poisoned");
            assert_eq!(created_pods.len(), 1);
            let annotations = created_pods[0]
                .pointer("/metadata/annotations")
                .and_then(serde_json::Value::as_object)
                .expect("created pod request should carry annotations");
            assert_eq!(
                annotation_value(annotations, SANDBOX_EXECUTION_GENERATION_ANNOTATION),
                "3"
            );
            assert_eq!(
                annotation_value(annotations, SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION),
                storage_uri
            );
            assert!(!annotations.contains_key(SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION));
        }

        let active_generation: i64 = sqlx::query_scalar(sql!(
            "SELECT generation
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active"
        ))
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(active_generation, 3);

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn started_recovery_created_execution_keeps_generation_for_local_restore()
    -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let artifact_id = Uuid::now_v7();
        let source_node_id = NodeId::new();
        let source_node_name = "source-node";
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(sql!(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '10.0.0.10', 'worker', 'k8s', 'ready')"
        ))
        .bind(source_node_id)
        .execute(&pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO node_k8s (node_id, k8s_node_name)
             VALUES ($1, $2)"
        ))
        .bind(source_node_id)
        .bind(source_node_name)
        .execute(&pool)
        .await?;
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(artifact_id)
        .execute(&pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO snapshots
                 (id, sandbox_id, type, state, timestamp, org_id, source_node_id,
                  local_artifact_id, artifact_kind, artifact_origin, generation)
             VALUES
                 ($1, $2, 'full', 'creating', NOW(),
                  '00000000-0000-0000-0000-000000000001', $3,
                  $4, 'full', 'suspend', 2)"
        ))
        .bind(Uuid::now_v7())
        .bind(sandbox_id)
        .bind(source_node_id)
        .bind(artifact_id)
        .execute(&pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active)
             VALUES
                 ($1, 2, 'space-sbx-retired-g2', 'space', 'forgotten', FALSE)"
        ))
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_client_proxy_port_pod_and_nodes(
            pool.clone(),
            mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
            DEFAULT_SANDBOX_PROXY_PORT,
            None,
            vec![
                (*node_with_network_slots(
                    source_node_name,
                    SandboxPodConfigValues::default()
                        .base_node_selector()
                        .into_labels(),
                    Some(8),
                    None,
                    true,
                    false,
                ))
                .clone(),
            ],
        );
        let recovery_generation = match substrate.store.claim_started_recovery(sandbox_id).await? {
            StartedRecoveryClaim::CreatedExecution { generation } => generation,
            claim => panic!("expected a fresh recovery execution, got {claim:?}"),
        };

        // Act.
        substrate
            .ensure_pod_exists(
                &sandbox,
                None,
                None,
                None,
                PodMaterializationOwnership::new(
                    Some(StartedRecoveryOwnership::CreatedExecution {
                        generation: recovery_generation,
                    }),
                    None,
                    Some(ActivationAttemptAssignment::new(
                        SandboxActivationAttemptId::new(),
                        SandboxTransitionTargetState::Started,
                    )),
                ),
            )
            .await?;

        // Assert.
        {
            let created_pods = captured_pod_creates.lock().expect("capture mutex poisoned");
            assert_eq!(created_pods.len(), 1);
            let annotations = created_pods[0]
                .pointer("/metadata/annotations")
                .and_then(serde_json::Value::as_object)
                .expect("created pod request should carry annotations");
            assert_eq!(
                annotation_value(annotations, SANDBOX_EXECUTION_GENERATION_ANNOTATION),
                recovery_generation.to_string()
            );
            assert_eq!(
                annotation_value(annotations, SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION),
                source_node_name
            );
        }

        let active_generation: i64 = sqlx::query_scalar(sql!(
            "SELECT generation
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active"
        ))
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(active_generation, recovery_generation);

        Ok(())
    }

    #[test]
    fn stale_restore_execution_create_requires_old_restoring_claim() {
        assert!(stale_restore_execution_create(
            "restoring",
            ActiveExecutionState::Creating,
            true
        ));
        assert!(!stale_restore_execution_create(
            "restoring",
            ActiveExecutionState::Creating,
            false
        ));
        assert!(!stale_restore_execution_create(
            "starting",
            ActiveExecutionState::Creating,
            true
        ));
        assert!(!stale_restore_execution_create(
            "restoring",
            ActiveExecutionState::Running,
            true
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ensure_pod_exists_replaces_stale_first_create_claim() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "creating").await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active, created_at, updated_at)
             VALUES
                 ($1, 1, 'space-sbx-old-g1', 'space', 'replaced', FALSE, NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes'),
                 ($1, 2, 'space-sbx-old-g2', 'space', 'creating', TRUE, NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes')",
        )
        .bind(sandbox_id)
        .execute(&pool)
        .await?;

        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
        );
        let mut sandbox = test_sandbox_with_lifecycle("creating");
        sandbox.id = sandbox_id;

        substrate
            .ensure_pod_exists(
                &sandbox,
                None,
                None,
                None,
                PodMaterializationOwnership::default(),
            )
            .await?;

        {
            let created_pods = captured_pod_creates.lock().expect("capture mutex poisoned");
            assert_eq!(created_pods.len(), 1);
            let annotations = created_pods[0]
                .pointer("/metadata/annotations")
                .and_then(serde_json::Value::as_object)
                .expect("created pod request should carry annotations");
            assert_eq!(
                annotations
                    .get(SANDBOX_EXECUTION_GENERATION_ANNOTATION)
                    .and_then(serde_json::Value::as_str),
                Some("3")
            );
            assert!(
                annotations.get(SANDBOX_CHECKPOINT_ANNOTATION).is_none(),
                "first-create replacement has no prior state and should remain on-demand"
            );
        }

        let active_generation: i64 = sqlx::query_scalar(
            "SELECT generation FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            active_generation, 3,
            "first-create replacement should allocate the next on-demand generation"
        );

        let stale_state: String = sqlx::query_scalar(
            "SELECT state FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND generation = 2",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(stale_state, "replaced");

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ensure_pod_exists_refreshes_stale_started_before_pod_create() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let node_id = NodeId::new();

        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '10.0.0.10', 'worker', 'k8s', 'ready')",
        )
        .bind(node_id)
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO node_k8s (node_id, k8s_node_name)
             VALUES ($1, 'source-node')",
        )
        .bind(node_id)
        .execute(&pool)
        .await?;
        sqlx::query(
            "UPDATE sandboxes
                SET active_node_id = $2
              WHERE id = $1",
        )
        .bind(sandbox_id)
        .bind(node_id)
        .execute(&pool)
        .await?;
        let stale_sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        sqlx::query(
            "UPDATE sandboxes
                SET lifecycle = 'restoring',
                    placement = 'unplaced'
              WHERE id = $1",
        )
        .bind(sandbox_id)
        .execute(&pool)
        .await?;

        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            mock_kubernetes_client_with_pod_create_assertion(
                Arc::clone(&captured_pod_creates),
                move |request_pod| {
                    assert_restore_pod_request(request_pod, sandbox_id, 1, "source-node")
                },
            ),
        );

        substrate
            .ensure_pod_exists(
                &stale_sandbox,
                None,
                None,
                None,
                PodMaterializationOwnership::default(),
            )
            .await?;

        let created_pods = captured_pod_creates.lock().expect("capture mutex poisoned");
        assert_eq!(created_pods.len(), 1);

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn started_recovery_materialization_does_not_recreate_after_teardown()
    -> Result<(), BoxError> {
        // Arrange: a recovery owner keeps its stale `started` snapshot after
        // teardown commits and retires the generation it originally claimed.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active)
             VALUES ($1, 2, 'space-sbx-recovering-g2', 'space', 'recovering', TRUE)",
        )
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
        let stale_sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let mut teardown = pool.begin().await?;
        sqlx::query(
            "UPDATE sandboxes
                SET lifecycle = 'deleting', placement = 'unplaced'
              WHERE id = $1",
        )
        .bind(sandbox_id)
        .execute(&mut *teardown)
        .await?;
        sqlx::query(
            "UPDATE sandbox_k8s_executions
                SET state = 'terminated', active = FALSE, terminated_at = NOW()
              WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .execute(&mut *teardown)
        .await?;
        teardown.commit().await?;
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
        );

        // Act.
        let error = substrate
            .ensure_pod_exists(
                &stale_sandbox,
                None,
                None,
                None,
                PodMaterializationOwnership::new(
                    Some(StartedRecoveryOwnership::ExistingExecution { generation: 2 }),
                    None,
                    None,
                ),
            )
            .await
            .expect_err("recovery materialization must lose to committed teardown");

        // Assert.
        assert!(matches!(error, SubstrateError::Conflict { .. }));
        let active_executions: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(active_executions, 0);
        assert!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .is_empty(),
            "recovery materialization must not create a pod after teardown",
        );

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn suspending_materialization_does_not_consume_successor_generation()
    -> Result<(), BoxError> {
        // Arrange: teardown durably suspended generation 1 and the exact pod
        // disappeared, but control-plane finalization has not yet moved the
        // lifecycle out of `suspending`.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_suspending_finalization_gap(&pool, sandbox_id, "space-sbx-suspending-g1").await?;
        let stale_sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
        );

        // Act.
        let error = substrate
            .ensure_pod_exists(
                &stale_sandbox,
                None,
                None,
                None,
                PodMaterializationOwnership::default(),
            )
            .await
            .expect_err("wake materialization must join the in-flight suspend");

        // Assert: a retryable deferral is acceptable, but consuming generation
        // 2 is not — the suspend owner must retain a stable execution fence.
        assert!(matches!(error, SubstrateError::SuspendInFlight { .. }));
        let generations: Vec<(i64, bool)> = sqlx::query_as(sql!(
            "SELECT generation, active
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1
              ORDER BY generation"
        ))
        .bind(sandbox_id)
        .fetch_all(&pool)
        .await?;
        assert_eq!(
            generations,
            vec![(1, false)],
            "wake must not allocate a successor while suspend finalization owns generation 1",
        );
        assert!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .is_empty(),
            "wake must not create a pod while suspend finalization is incomplete",
        );

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ordinary_restore_materialization_does_not_recreate_after_teardown()
    -> Result<(), BoxError> {
        // Arrange: an ordinary restore caller keeps its stale `restoring`
        // snapshot after teardown retires the execution it originally claimed.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "restoring").await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active)
             VALUES ($1, 2, 'space-sbx-restoring-g2', 'space', 'creating', TRUE)",
        )
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
        let stale_sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let mut teardown = pool.begin().await?;
        sqlx::query(
            "UPDATE sandboxes
                SET lifecycle = 'deleting', placement = 'unplaced'
              WHERE id = $1",
        )
        .bind(sandbox_id)
        .execute(&mut *teardown)
        .await?;
        sqlx::query(
            "UPDATE sandbox_k8s_executions
                SET state = 'terminated', active = FALSE, terminated_at = NOW()
              WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .execute(&mut *teardown)
        .await?;
        teardown.commit().await?;
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
        );

        // Act.
        let error = substrate
            .ensure_pod_exists(
                &stale_sandbox,
                None,
                None,
                Some("s3://space-test/suspend-snapshots/ordinary-race?region=us-east-1"),
                PodMaterializationOwnership::default(),
            )
            .await
            .expect_err("ordinary restore materialization must lose to committed teardown");

        // Assert.
        assert!(matches!(error, SubstrateError::Conflict { .. }));
        let active_executions: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(active_executions, 0);
        assert!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .is_empty(),
            "ordinary restore materialization must not create a pod after teardown",
        );

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn restore_create_defers_when_pod_reflector_cache_is_cold() -> Result<(), BoxError> {
        // Arrange: a fresh restore whose pod-reflector cache is still cold. The
        // lingering prior-generation sweep is a create prerequisite, but a cold
        // cache cannot enumerate pods, so the create must defer retryably rather
        // than materialize a successor unswept.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "restoring").await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let mut substrate = substrate_with_store_and_client(
            pool.clone(),
            mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
        );
        // Pending (never marked ready) — the reflector's initial LIST has not
        // completed.
        substrate.pod_reflector_ready = ReflectorReady::new();

        // Act.
        let error = substrate
            .ensure_pod_exists(
                &sandbox,
                None,
                None,
                Some("s3://space-test/suspend-snapshots/cold-reflector?region=us-east-1"),
                PodMaterializationOwnership::default(),
            )
            .await
            .expect_err("a cold reflector must defer the restore create retryably");

        // Assert: retryable deferral, no pod POSTed, and the freshly-created
        // claim retired so the retry re-runs the sweep instead of no-oping on
        // the create-in-flight early return.
        assert!(matches!(error, SubstrateError::Unavailable { .. }));
        assert!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .is_empty(),
            "a cold-reflector restore create must not POST a pod",
        );
        let active_executions: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            active_executions, 0,
            "the deferred create's claim must be retired for a clean retry"
        );

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn materialization_inherits_policy_committed_before_execution_claim()
    -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db_with_max_connections(4).await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "restoring").await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let captured = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            running_pod_kubernetes_client(Arc::clone(&captured), "127.0.0.1"),
        );

        // A timeout owns the sandbox row while activation reads its old policy.
        // Wait for the activation claim to block, then commit the new policy:
        // materialization must inherit that accepted pre-claim update.
        let mut policy_update = pool.begin().await?;
        sqlx::query(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
            .bind(sandbox_id)
            .fetch_one(&mut *policy_update)
            .await?;
        let materialization = tokio::spawn(async move {
            substrate
                .ensure_pod_exists(
                    &sandbox,
                    None,
                    None,
                    Some("s3://space-test/suspend-snapshots/policy-race?region=us-east-1"),
                    PodMaterializationOwnership::default(),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let blocked: bool = sqlx::query_scalar(sql!(
                    "SELECT EXISTS (
                         SELECT 1 FROM pg_stat_activity
                         WHERE datname = current_database()
                           AND wait_event_type = 'Lock'
                           AND query LIKE '%SELECT lifecycle FROM sandboxes%FOR UPDATE%'
                     )"
                ))
                .fetch_one(&pool)
                .await?;
                if blocked {
                    return Ok::<_, sqlx::Error>(());
                }
                assert!(
                    !materialization.is_finished(),
                    "activation must wait for the policy writer"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        let expected_policy = serde_json::json!({"wake": {"on": ["access"]}});
        sqlx::query(sql!(
            "UPDATE sandboxes SET auto_lifecycle_json = $2 WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(sqlx::types::Json(&expected_policy))
        .execute(&mut *policy_update)
        .await?;
        policy_update.commit().await?;
        materialization.await??;

        let pods = captured.lock().expect("capture mutex poisoned");
        assert_eq!(pods.len(), 1);
        let encoded = pods[0]["metadata"]["annotations"]["space.pplx.net/auto-lifecycle"]
            .as_str()
            .expect("materialized pod must carry the auto-lifecycle policy");
        let actual: pplx_space_common_core::types::AutoLifecyclePolicy =
            serde_json::from_str(encoded)?;
        let expected: pplx_space_common_core::types::AutoLifecyclePolicy =
            serde_json::from_value(expected_policy)?;
        assert_eq!(
            actual, expected,
            "pod must inherit the policy committed before its claim"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pod_materialization_claim_rejects_teardown_lifecycle() -> Result<(), BoxError> {
        // Arrange: the ordinary restore claimed its execution, then teardown
        // advanced lifecycle before the final pre-POST materialization claim.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "restoring").await?;
        let substrate = substrate_with_store(pool.clone());
        let claim = substrate
            .store
            .ensure_active_execution_claim(sandbox_id)
            .await?;
        sqlx::query("UPDATE sandboxes SET lifecycle = 'deleting' WHERE id = $1")
            .bind(sandbox_id)
            .execute(&pool)
            .await?;

        // Act.
        let result = substrate
            .store
            .claim_active_execution_pod_materialization(
                sandbox_id,
                claim.row.generation,
                pplx_space_common_core::types::SandboxLifecycle::Restoring,
            )
            .await;

        // Assert.
        let error = match result {
            Ok(_) => panic!("pod materialization must not begin after teardown advances lifecycle"),
            Err(error) => error,
        };
        assert!(matches!(error, SubstrateError::Conflict { .. }));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn teardown_revokes_slow_pod_materialization() -> Result<(), BoxError> {
        // Arrange: pause the Kubernetes POST after materialization ownership is
        // durable so teardown attempts to revoke it in that exact gap.
        let db = pplx_space_db_test_fixtures::pg_test_db_with_max_connections(4).await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "restoring").await?;
        let sandbox = Arc::new(
            fetch_sandbox(&pool, sandbox_id)
                .await?
                .expect("inserted sandbox must be visible"),
        );
        let request_started = Arc::new(Semaphore::new(0));
        let release_request = Arc::new(Semaphore::new(0));
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let kubernetes_client = blocking_running_pod_kubernetes_client(
            Arc::clone(&captured_pod_creates),
            "127.0.0.1",
            Arc::clone(&request_started),
            Arc::clone(&release_request),
        );
        let substrate = substrate_with_store_and_client(pool.clone(), kubernetes_client.clone());

        // Act.
        let materializing_sandbox = Arc::clone(&sandbox);
        let materialization = tokio::spawn(async move {
            substrate
                .ensure_pod_exists(
                    &materializing_sandbox,
                    None,
                    None,
                    Some("s3://space-test/suspend-snapshots/materialization?region=us-east-1"),
                    PodMaterializationOwnership::default(),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(10), request_started.acquire())
            .await??
            .forget();
        let teardown_pool = pool.clone();
        let mut teardown = tokio::spawn(async move {
            let mut tx = teardown_pool.begin().await?;
            sqlx::query(
                "UPDATE sandboxes
                    SET lifecycle = 'deleting', placement = 'unplaced'
                  WHERE id = $1",
            )
            .bind(sandbox_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "UPDATE sandbox_k8s_executions
                    SET state = 'terminated', active = FALSE, terminated_at = NOW()
                  WHERE sandbox_id = $1 AND active",
            )
            .bind(sandbox_id)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok::<_, sqlx::Error>(())
        });
        tokio::time::timeout(Duration::from_secs(5), &mut teardown)
            .await
            .expect("teardown must not wait for a slow Kubernetes Pod create")??;
        release_request.add_permits(1);
        materialization.await??;

        // Assert.
        let lifecycle: String = sqlx::query_scalar("SELECT lifecycle FROM sandboxes WHERE id = $1")
            .bind(sandbox_id)
            .fetch_one(&pool)
            .await?;
        assert_eq!(lifecycle, "deleting");
        let active_executions: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(active_executions, 0);
        assert_eq!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .len(),
            1
        );
        let pod_name = captured_pod_creates
            .lock()
            .expect("capture mutex poisoned")
            .first()
            .and_then(|pod| pod.pointer("/metadata/name"))
            .and_then(serde_json::Value::as_str)
            .expect("created Pod must have a name")
            .to_string();
        let pods: Api<Pod> = Api::namespaced(kubernetes_client, "space");
        assert!(
            pods.get_opt(&pod_name).await?.is_none(),
            "a Pod created after teardown revokes materialization ownership must be deleted"
        );
        Ok(())
    }

    async fn install_test_pod_metadata_persist_failure(pool: &PgPool) -> Result<(), BoxError> {
        sqlx::query(
            "CREATE FUNCTION fail_test_pod_metadata_persist() RETURNS trigger
             LANGUAGE plpgsql AS $$
             BEGIN
                 IF NEW.pod_uid IS NOT NULL THEN
                     RAISE EXCEPTION 'injected pod metadata persistence failure';
                 END IF;
                 RETURN NEW;
             END;
             $$",
        )
        .execute(pool)
        .await?;
        sqlx::query(
            "CREATE TRIGGER fail_test_pod_metadata_persist
             BEFORE UPDATE OF pod_uid ON sandbox_k8s_executions
             FOR EACH ROW EXECUTE FUNCTION fail_test_pod_metadata_persist()",
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pod_is_deleted_when_post_create_persistence_fails() -> Result<(), BoxError> {
        // Arrange: make the metadata write fail after Kubernetes accepts the
        // Pod, at the database finalization seam.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "restoring").await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        install_test_pod_metadata_persist_failure(&pool).await?;
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let kubernetes_client =
            running_pod_kubernetes_client(Arc::clone(&captured_pod_creates), "127.0.0.1");
        let substrate = substrate_with_store_and_client(pool.clone(), kubernetes_client.clone());

        // Act.
        let error = substrate
            .ensure_pod_exists(
                &sandbox,
                None,
                None,
                Some("s3://space-test/suspend-snapshots/persist-failure?region=us-east-1"),
                PodMaterializationOwnership::default(),
            )
            .await
            .expect_err("injected metadata persistence failure must reach the caller");

        // Assert.
        assert!(
            matches!(error, SubstrateError::Failed { ref message } if message.contains("injected pod metadata persistence failure"))
        );
        let pod_name = captured_pod_creates
            .lock()
            .expect("capture mutex poisoned")
            .first()
            .and_then(|pod| pod.pointer("/metadata/name"))
            .and_then(serde_json::Value::as_str)
            .expect("created Pod must have a name")
            .to_string();
        let pods: Api<Pod> = Api::namespaced(kubernetes_client, "space");
        assert!(
            pods.get_opt(&pod_name).await?.is_none(),
            "a Pod whose database finalization failed must be deleted"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unconfirmed_delete_after_metadata_persist_failure_keeps_recovery_pending()
    -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let artifact_id = Uuid::now_v7();
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(artifact_id)
        .execute(&pool)
        .await?;
        insert_ready_full_snapshot_relative_to_lifecycle(
            &pool,
            sandbox_id,
            "s3://space-test/suspend-snapshots/unconfirmed-delete?region=us-east-1",
            0,
            artifact_id,
        )
        .await?;
        install_test_pod_metadata_persist_failure(&pool).await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let kubernetes_client = running_pod_kubernetes_client_with_unconfirmed_delete(
            Arc::clone(&captured_pod_creates),
            "127.0.0.1",
        );
        let mut substrate =
            substrate_with_store_and_client(pool.clone(), kubernetes_client.clone());
        substrate.k8s.k8s_blocking_delete_timeout = Duration::from_millis(10);

        let error = substrate
            .wake_execution_with_metric_identity(
                &sandbox,
                Duration::from_secs(1),
                SandboxApiMetricIdentity::ecw("connect"),
                crate::controllers::substrate::RecoveryCharge::Exempt,
            )
            .await
            .expect_err("unconfirmed Pod deletion must keep recovery retryable");

        assert!(
            matches!(error, SubstrateError::RestoreInFlight { .. }),
            "unconfirmed Pod deletion must preserve recovery ownership: {error:?}"
        );
        let lifecycle: String =
            sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
                .bind(sandbox_id)
                .fetch_one(&pool)
                .await?;
        assert_eq!(lifecycle, "started");
        let execution: (String, bool) = sqlx::query_as(sql!(
            "SELECT state, active
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(execution, ("recovering".to_string(), true));
        let pod_name = captured_pod_creates
            .lock()
            .expect("capture mutex poisoned")
            .first()
            .and_then(|pod| pod.pointer("/metadata/name"))
            .and_then(serde_json::Value::as_str)
            .expect("created Pod must have a name")
            .to_string();
        let pods: Api<Pod> = Api::namespaced(kubernetes_client, "space");
        assert!(
            pods.get_opt(&pod_name).await?.is_some(),
            "the mock must retain the Pod after accepting delete"
        );

        Ok(())
    }

    fn substrate_with_node_cache(nodes: &[Arc<Node>], nodes_ready: bool) -> KubernetesSubstrate {
        substrate_with_node_cache_and_config(nodes, nodes_ready, SandboxPodConfigValues::default())
    }

    /// [`substrate_with_node_cache_and_config`] with the node-group placement
    /// gate **open** against a real database, for the paths whose behavior only
    /// exists once a candidate set resolves.
    fn substrate_with_placement_gate_open(
        nodes: &[Arc<Node>],
        sandbox_pod_config: SandboxPodConfigValues,
        pool: PgPool,
    ) -> KubernetesSubstrate {
        let substrate = substrate_with_node_cache_and_config(nodes, true, sandbox_pod_config);
        KubernetesSubstrate {
            store: K8sStore::new(pool.clone(), "space".to_string()),
            node_group_placement: NodeGroupPlacementResolver::new(pool, true),
            ..substrate
        }
    }

    fn substrate_with_node_cache_and_config(
        nodes: &[Arc<Node>],
        nodes_ready: bool,
        sandbox_pod_config: SandboxPodConfigValues,
    ) -> KubernetesSubstrate {
        let shutdown_token = CancellationToken::new();
        let task_tracker = tokio_util::task::TaskTracker::new();
        use kube::runtime::watcher::Event;
        let (node_store, mut node_writer) = reflector::store::<Node>();
        node_writer.apply_watcher_event(&Event::Init);
        for node in nodes {
            node_writer.apply_watcher_event(&Event::InitApply((**node).clone()));
        }
        node_writer.apply_watcher_event(&Event::InitDone);

        let (pod_store, _) = reflector::store::<Pod>();
        let caches = Arc::new(K8sCaches::for_test(
            pod_store.clone(),
            node_store,
            nodes_ready,
        ));
        // The reflector-based count never touches Postgres; a lazy pool keeps
        // this test Docker-free.
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .expect("lazy pool builds without connecting");
        KubernetesSubstrate {
            k8s: KubernetesState {
                client: inert_kubernetes_client(),
                sandbox_namespace: "space".to_string(),
                sandbox_proxy_port: DEFAULT_SANDBOX_PROXY_PORT,
                node_api_port: DEFAULT_SANDBOX_PROXY_PORT,
                sandbox_pod_config,
                create_command_ready_timeout: Duration::from_secs(1),
                create_data_plane_ready_timeout: Duration::from_secs(1),
                k8s_blocking_delete_timeout: Duration::from_secs(1),
                caches,
            },
            restore_waiters: RestoreWaiters::new(
                K8sStore::new(pool.clone(), "space".to_string()),
                shutdown_token.clone(),
                task_tracker.clone(),
            ),
            wake_coalescer: Arc::new(
                pplx_space_common_work_coalescer::WorkCoalescer::with_tracker(task_tracker.clone()),
            ),
            store: K8sStore::new(pool.clone(), "space".to_string()),
            pod_store,
            pod_events: pod_reflector::PodEvents::for_test(),
            pod_reflector_ready: ReflectorReady::always_ready(),
            cancel_tracker: Arc::new(CancelRateTracker::with_budget(64)),
            // Gate closed: these tests exercise pod lifecycle, not placement,
            // and a closed gate issues no query — so the lazily-connected pool
            // stays undialed. Placement rendering is covered by the pod-builder
            // snapshots; resolution by `node_group_placement`'s own tests.
            node_group_placement: NodeGroupPlacementResolver::new(pool, false),
            recovery_join_backoff: RecoveryJoinBackoffPolicy::default(),
            shutdown_token,
            task_tracker,
        }
    }

    /// A Ready, schedulable, selector-matching worker whose node-local service
    /// pods carry the given readiness verdict (`None` = unlabeled).
    fn node_with_services_ready(
        name: &str,
        config: &SandboxPodConfigValues,
        services_ready: Option<bool>,
    ) -> Arc<Node> {
        let mut labels = config.base_node_selector().into_labels();
        if let Some(ready) = services_ready {
            labels.insert(
                pplx_space_common_core::kubernetes::NODE_SERVICES_READY_LABEL.to_string(),
                if ready {
                    pplx_space_common_core::kubernetes::NODE_SERVICES_READY_VALUE
                } else {
                    pplx_space_common_core::kubernetes::NODE_SERVICES_NOT_READY_VALUE
                }
                .to_string(),
            );
        }
        node_with_network_slots(name, labels, Some(8), None, true, false)
    }

    fn services_ready_config() -> SandboxPodConfigValues {
        SandboxPodConfigValues {
            node_services_ready_affinity: true,
            ..SandboxPodConfigValues::default()
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn restore_shortlist_drops_nodes_with_not_ready_node_services() {
        let config = services_ready_config();
        let nodes = [
            node_with_services_ready("fallback-ready", &config, Some(true)),
            node_with_services_ready("fallback-rolling", &config, Some(false)),
            node_with_services_ready("fallback-unlabeled", &config, None),
        ];
        let substrate = substrate_with_node_cache_and_config(&nodes, true, config);

        let shortlist = substrate.restore_node_shortlist(SandboxId::new(), "source-node", None);

        assert_eq!(shortlist[0], "source-node", "the source stays pinned first");
        assert!(
            !shortlist.iter().any(|node| node == "fallback-rolling"),
            "a node mid-DaemonSet-restart must not be a restore fallback: {shortlist:?}"
        );
        // An unlabeled node is not evidence of a problem — no writer, or a node
        // that just joined — so it stays a candidate.
        assert!(shortlist.iter().any(|node| node == "fallback-unlabeled"));
        assert!(shortlist.iter().any(|node| node == "fallback-ready"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn restore_shortlist_keeps_not_ready_nodes_when_the_bias_is_off() {
        let config = SandboxPodConfigValues::default();
        assert!(!config.node_services_ready_affinity);
        let nodes = [node_with_services_ready(
            "fallback-rolling",
            &config,
            Some(false),
        )];
        let substrate = substrate_with_node_cache_and_config(&nodes, true, config);

        let shortlist = substrate.restore_node_shortlist(SandboxId::new(), "source-node", None);

        assert!(shortlist.iter().any(|node| node == "fallback-rolling"));
    }

    /// The local-only restore pre-flight is **wired**, not merely written.
    ///
    /// `check_local_restore_source` is a pure function with its own unit tests,
    /// and `PodCreateContext::direct_node_name` is a pure mapping with its own.
    /// Neither says anything about whether the resolve path still calls them:
    /// deleting the `if let (Some(placement), Some(node_name))` block in
    /// `resolve_node_group_placement` passes both of those and every other test
    /// on the branch. This drives the real path.
    ///
    /// The property: a sandbox whose only copy sits on a node the boundary now
    /// excludes must be refused before a pod exists, with an error that names
    /// both ways out — not scheduled onto a node holding none of its bytes, and
    /// not left for kubelet to reject terminally after it has taken a network
    /// slot.
    #[tokio::test(flavor = "current_thread")]
    async fn a_local_restore_from_an_out_of_set_source_is_refused_before_any_pod()
    -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        sqlx::query(sql!(
            "INSERT INTO templates (id, name, org_id, visibility) VALUES ($1, 'seed', NULL, 'generic')"
        ))
        .bind(Uuid::now_v7())
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO node_groups (id, name, org_id, template_policy, is_default)
             VALUES ($1, 'workers', NULL, 'default', TRUE)"
        ))
        .bind(Uuid::now_v7())
        .execute(&db.pool)
        .await?;
        let config = SandboxPodConfigValues::default();
        // The source node is in a group the candidate set does not contain.
        let source = node_with_node_group("source-node", &config, Some("org-b-1"));
        let substrate = substrate_with_placement_gate_open(&[source], config, db.pool.clone());
        let sandbox = test_sandbox_with_lifecycle("suspended");
        // Local-only: no storage URI, so the pin is the sandbox's only route
        // back and dropping it is not an option.
        let local_only = PodCreateContext::Restore(RestoreContext::UserRestore {
            suspend_restore_storage_uri: None,
            suspend_restore_source_node: Some("source-node"),
            direct_node_name: Some("source-node"),
            preferred_node_name: None,
        });

        let error = substrate
            .resolve_node_group_placement(&sandbox, &local_only)
            .await
            .expect_err("a source node outside the candidate set must refuse the restore");

        // Both remedies have to be in the text. Naming only the relabel would
        // point an operator at the fleet when the sandbox's own snapshot can be
        // published instead, and naming only the publish would hide that a
        // mislabeled node is the likelier cause.
        assert!(
            matches!(&error, SubstrateError::Conflict { message }
                if message.contains("org-b-1")
                    && message.contains("remote storage")
                    && message.contains("compatible group")),
            "the error must name the group to migrate off and both remedies: {error:?}"
        );

        // The control: the same everything with the source node labeled into
        // the candidate set resolves, so the refusal above is the pre-flight's
        // doing and not a resolution failure.
        let in_set = node_with_node_group(
            "source-node",
            &SandboxPodConfigValues::default(),
            Some("workers"),
        );
        let ok_substrate = substrate_with_placement_gate_open(
            &[in_set],
            SandboxPodConfigValues::default(),
            db.pool.clone(),
        );
        let resolved = ok_substrate
            .resolve_node_group_placement(&sandbox, &local_only)
            .await
            .expect("an in-set source keeps its pin");
        assert!(resolved.placement().is_some());
        Ok(())
    }

    /// The readiness filter is disabled when the signal is untrustworthy
    /// *within the candidate set*, even though it looks fine fleet-wide.
    ///
    /// The property: filtering on `node-services-ready` **removes** shortlist
    /// candidates, so a stale or wedged signal must never be allowed to remove
    /// all of them. A fleet-wide-only reading misses this entirely — a handful
    /// of not-ready nodes is a rounding error against hundreds — and yet those
    /// same nodes can be the whole of a small candidate set, in which case
    /// filtering collapses the shortlist to the bare source pin.
    #[tokio::test(flavor = "current_thread")]
    async fn restore_shortlist_keeps_in_set_nodes_when_the_group_reads_degraded() {
        let config = SandboxPodConfigValues {
            node_services_ready_affinity: true,
            ..SandboxPodConfigValues::default()
        };
        let mut nodes = Vec::new();
        // Three of the four in-set nodes read not-ready: 3/4 crosses the
        // threshold within the set.
        for (name, ready) in [
            ("in-set-a", false),
            ("in-set-b", false),
            ("in-set-c", false),
            ("in-set-d", true),
        ] {
            nodes.push(node_with_group_and_services(
                name,
                &config,
                Some("workers"),
                Some(ready),
            ));
        }
        // Twenty ready nodes in another group. They are in the *eligible*
        // population, so the fleet-wide reading is 3/24 — comfortably usable —
        // but they are filtered out of the candidate set.
        for index in 0..20 {
            nodes.push(node_with_group_and_services(
                &format!("other-group-{index}"),
                &config,
                Some("org-b-1"),
                Some(true),
            ));
        }
        let substrate = substrate_with_node_cache_and_config(&nodes, true, config);
        let template = TemplateCompatSpec {
            name: TemplateName::parse("seed").expect("valid template name"),
            org_id: None,
            visibility: TemplateVisibility::Generic,
        };
        let placement = NodeGroupPlacement::for_caller(
            Some(OrgId::new()),
            &template,
            &[NodeGroup {
                id: NodeGroupId::new(),
                name: NodeGroupName::parse("workers").expect("valid group name"),
                policy: NodeGroupPolicy::Generic { is_default: true },
                template_allowlist: std::collections::BTreeSet::new(),
            }],
        )
        .expect("a generic group admits a generic template");

        let shortlist =
            substrate.restore_node_shortlist(SandboxId::new(), "source-node", Some(&placement));

        for name in ["in-set-a", "in-set-b", "in-set-c", "in-set-d"] {
            assert!(
                shortlist.iter().any(|node| node == name),
                "{name} must survive: discarding a signal that reads degraded across the \
                 whole candidate set is cheaper than removing every fallback: {shortlist:?}"
            );
        }
        assert!(
            !shortlist
                .iter()
                .any(|node| node.starts_with("other-group-")),
            "and the node-group filter still applies: {shortlist:?}"
        );
    }

    /// The candidate set narrows the restore-fallback shortlist, not only the
    /// rendered pod.
    ///
    /// The property, stated without reference to the code: a fallback the
    /// boundary excludes must not occupy a slot in a list whose whole purpose
    /// is to give a busy source somewhere else to land. The pod builder repeats
    /// the required node-group term into every ORed `metadata.name` term, so an
    /// out-of-set entry renders as a term no node satisfies — it consumes a
    /// shortlist slot and can never be chosen, and once every fallback is
    /// out-of-set the restore goes `Unschedulable` where an unfiltered
    /// shortlist would have said "no capacity".
    #[tokio::test(flavor = "current_thread")]
    async fn restore_shortlist_drops_fallbacks_outside_the_candidate_set() {
        let config = SandboxPodConfigValues::default();
        let in_set = node_with_node_group("fallback-in-set", &config, Some("workers"));
        let out_of_set = node_with_node_group("fallback-out-of-set", &config, Some("org-b-1"));
        let unlabeled = node_with_node_group("fallback-unlabeled", &config, None);
        let substrate =
            substrate_with_node_cache_and_config(&[in_set, out_of_set, unlabeled], true, config);
        let template = TemplateCompatSpec {
            name: TemplateName::parse("seed").expect("valid template name"),
            org_id: None,
            visibility: TemplateVisibility::Generic,
        };
        let placement = NodeGroupPlacement::for_caller(
            Some(OrgId::new()),
            &template,
            &[NodeGroup {
                id: NodeGroupId::new(),
                name: NodeGroupName::parse("workers").expect("valid group name"),
                policy: NodeGroupPolicy::Generic { is_default: true },
                template_allowlist: std::collections::BTreeSet::new(),
            }],
        )
        .expect("a generic group admits a generic template");

        // Ungated, every fallback is a candidate — so the exclusions below are
        // this filter's doing and not some other narrowing in the chain.
        let ungated = substrate.restore_node_shortlist(SandboxId::new(), "source-node", None);
        for name in [
            "fallback-in-set",
            "fallback-out-of-set",
            "fallback-unlabeled",
        ] {
            assert!(
                ungated.iter().any(|node| node == name),
                "{name} must be a candidate with no placement: {ungated:?}"
            );
        }

        let shortlist =
            substrate.restore_node_shortlist(SandboxId::new(), "source-node", Some(&placement));

        assert_eq!(shortlist[0], "source-node", "the source stays pinned first");
        assert!(
            shortlist.iter().any(|node| node == "fallback-in-set"),
            "an in-set fallback survives: {shortlist:?}"
        );
        assert!(
            !shortlist.iter().any(|node| node == "fallback-out-of-set"),
            "a fallback in another group must not occupy a shortlist slot: {shortlist:?}"
        );
        // An `In (…)` term does not match a node lacking the key, so an
        // unlabeled node is outside the set exactly as a foreign-labeled one is
        // — unlike the node-services signal, where unlabeled means "no
        // evidence" and stays a candidate.
        assert!(
            !shortlist.iter().any(|node| node == "fallback-unlabeled"),
            "an unlabeled fallback cannot satisfy the boundary: {shortlist:?}"
        );
    }

    fn node_with_group_and_services(
        name: &str,
        config: &SandboxPodConfigValues,
        group: Option<&str>,
        services_ready: Option<bool>,
    ) -> Arc<Node> {
        let mut labels = config.base_node_selector().into_labels();
        if let Some(group) = group {
            labels.insert(SANDBOX_NODE_GROUP_LABEL.to_string(), group.to_string());
        }
        if let Some(ready) = services_ready {
            labels.insert(
                pplx_space_common_core::kubernetes::NODE_SERVICES_READY_LABEL.to_string(),
                if ready {
                    pplx_space_common_core::kubernetes::NODE_SERVICES_READY_VALUE
                } else {
                    pplx_space_common_core::kubernetes::NODE_SERVICES_NOT_READY_VALUE
                }
                .to_string(),
            );
        }
        node_with_network_slots(name, labels, Some(8), None, true, false)
    }

    fn node_with_node_group(
        name: &str,
        config: &SandboxPodConfigValues,
        group: Option<&str>,
    ) -> Arc<Node> {
        let mut labels = config.base_node_selector().into_labels();
        if let Some(group) = group {
            labels.insert(SANDBOX_NODE_GROUP_LABEL.to_string(), group.to_string());
        }
        node_with_network_slots(name, labels, Some(8), None, true, false)
    }

    /// A stale or wedged writer can leave most of the fleet labelled
    /// `false`. Filtering on that would collapse restore-fallback capacity, so
    /// past the trust threshold the signal is ignored entirely.
    #[tokio::test(flavor = "current_thread")]
    async fn restore_shortlist_ignores_the_signal_when_most_of_the_fleet_reads_degraded() {
        let config = services_ready_config();
        let nodes = [
            node_with_services_ready("fallback-a", &config, Some(false)),
            node_with_services_ready("fallback-b", &config, Some(false)),
            node_with_services_ready("fallback-c", &config, Some(false)),
            node_with_services_ready("fallback-d", &config, Some(true)),
        ];
        let substrate = substrate_with_node_cache_and_config(&nodes, true, config);

        let shortlist = substrate.restore_node_shortlist(SandboxId::new(), "source-node", None);

        assert_eq!(
            shortlist.len(),
            5,
            "every candidate must survive when the signal is untrustworthy: {shortlist:?}"
        );
    }

    // Async so the lazy `PgPool` (unused by the reflector-based count) can be
    // built under a Tokio context; the count call itself is synchronous.
    #[tokio::test(flavor = "current_thread")]
    async fn eligible_start_node_count_reads_live_node_cache() {
        let config = SandboxPodConfigValues::default();

        // Cold cache — the node reflector's initial LIST has not completed, so
        // an empty store means "capacity unknown", not "zero nodes". Must fail
        // open (None) rather than reject a create during a total node outage.
        assert_eq!(
            substrate_with_node_cache(&[], false).eligible_start_node_count(),
            None,
            "cold node cache must be unknown (None), never a confident empty",
        );

        // Warm cache that genuinely holds zero eligible nodes → confident
        // empty (Some(0)), which the limiter rejects as empty_cluster. A bare
        // `is_empty()` check would collapse this into the cold None above.
        assert_eq!(
            substrate_with_node_cache(&[], true).eligible_start_node_count(),
            Some(0),
            "warm empty cache is a confident zero, not unknown",
        );

        // Warm cache with a Ready, schedulable, selector-matching node → capacity.
        let ready = node_with_network_slots(
            "ready",
            config.base_node_selector().into_labels(),
            Some(8),
            None,
            true,
            false,
        );
        assert_eq!(
            substrate_with_node_cache(&[ready], true).eligible_start_node_count(),
            Some(1),
        );

        // Warm cache whose only node is NotReady → confident empty.
        let not_ready = node_with_network_slots(
            "down",
            config.base_node_selector().into_labels(),
            Some(8),
            None,
            false,
            false,
        );
        assert_eq!(
            substrate_with_node_cache(&[not_ready], true).eligible_start_node_count(),
            Some(0),
        );

        // Warm cache whose only Ready node is hard placement-blocked
        // (ssd-placement-blocked=true). `build_sandbox_pod`'s required NotIn
        // affinity forbids scheduling there, so it must not count — a cluster
        // where every node is placement-blocked is a confident empty, not
        // spare capacity the scheduler can't actually use.
        let mut blocked_labels = config.base_node_selector().into_labels();
        blocked_labels.insert(
            pplx_space_common_core::kubernetes::SSD_PLACEMENT_BLOCKED_LABEL.to_string(),
            pplx_space_common_core::kubernetes::SSD_PLACEMENT_BLOCKED_VALUE.to_string(),
        );
        let blocked =
            node_with_network_slots("blocked", blocked_labels, Some(8), None, true, false);
        assert_eq!(
            substrate_with_node_cache(std::slice::from_ref(&blocked), true)
                .eligible_start_node_count(),
            Some(0),
            "placement-blocked nodes must not count when the SSD-headroom-tier affinity is on",
        );

        // With the SSD-headroom-tier affinity disabled, `build_sandbox_pod`
        // stamps no NotIn exclusion, so placement-blocked nodes stay schedulable
        // and must count — otherwise admission reports a false empty_cluster.
        let affinity_off = SandboxPodConfigValues {
            ssd_headroom_tier_affinity: false,
            ..SandboxPodConfigValues::default()
        };
        assert_eq!(
            substrate_with_node_cache_and_config(&[blocked], true, affinity_off)
                .eligible_start_node_count(),
            Some(1),
            "with the affinity off, placement-blocked nodes remain schedulable and count",
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn recovery_follower_does_not_charge_cluster_admission() -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active)
             VALUES ($1, 1, 'space-sbx-recovering-g1', 'space', 'recovering', TRUE)",
        )
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
        );
        let admission = std::sync::Arc::new(
            crate::cluster_admission::ClusterSandboxAdmission::with_config(
                Substrate::Direct(Box::new(
                    crate::controllers::substrate::direct::DirectSubstrate::new(pool),
                )),
                crate::cluster_admission::ClusterSandboxAdmissionConfig {
                    enabled: true,
                    refill_rps: 6.0,
                    burst: 60.0,
                    redis_url: None,
                },
                pplx_space_feature_flags::FeatureFlagClient::disabled(),
            ),
        );

        // Act.
        let error = substrate
            .ensure_execution_ready_for_access(
                &sandbox,
                SandboxApiMetricIdentity::native("runtime_client"),
                Duration::from_secs(1),
                crate::controllers::substrate::RecoveryCharge::enforce(Some(&admission)),
                None,
            )
            .await
            .expect_err("a follower must wait for the active recovery owner");

        // Assert.
        assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            error.retry_after_cause(),
            Some(crate::controllers::ControllerRetryAfterCause::RestoreInFlight),
            "a follower must join recovery before cluster admission is charged",
        );
        assert!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .is_empty(),
            "a recovery follower must not create a Pod",
        );

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn suspending_access_defers_before_charging_cluster_admission() -> Result<(), BoxError> {
        // Arrange: the exact execution has been suspended and its pod is gone,
        // while teardown still owns finalization of the central lifecycle.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_suspending_finalization_gap(&pool, sandbox_id, "space-sbx-suspending-access-g1")
            .await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
        );
        let admission = std::sync::Arc::new(
            crate::cluster_admission::ClusterSandboxAdmission::with_config(
                Substrate::Direct(Box::new(
                    crate::controllers::substrate::direct::DirectSubstrate::new(pool.clone()),
                )),
                crate::cluster_admission::ClusterSandboxAdmissionConfig {
                    enabled: true,
                    refill_rps: 6.0,
                    burst: 60.0,
                    redis_url: None,
                },
                pplx_space_feature_flags::FeatureFlagClient::disabled(),
            ),
        );

        // Act.
        let error = substrate
            .ensure_execution_ready_for_access(
                &sandbox,
                SandboxApiMetricIdentity::native("runtime_client"),
                Duration::from_secs(1),
                crate::controllers::substrate::RecoveryCharge::enforce(Some(&admission)),
                None,
            )
            .await
            .expect_err("access must join suspend finalization before activation admission");

        // Assert: an empty-cluster admission would return 429 if access reached
        // the activation charge. Suspend ownership must win first and remain
        // retryable without consuming another generation.
        assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            error.retry_after_cause(),
            Some(crate::controllers::ControllerRetryAfterCause::RestoreInFlight),
        );
        let max_generation: i64 = sqlx::query_scalar(sql!(
            "SELECT MAX(generation)
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(max_generation, 1);
        assert!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .is_empty(),
            "suspending access must not create a replacement pod",
        );

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rejected_admission_blocks_pod_recreation_for_started_sandbox_with_lost_pod()
    -> Result<(), BoxError> {
        // A `started` sandbox whose pod is gone drives `runtime_client`'s
        // recovery branch (`ensure_pod_exists`), which recreates the pod — a
        // real cluster activation that `implicit_activation_class` classifies as
        // a no-op reconnect. When the shared limiter rejects, the charge must
        // fire BEFORE `ensure_pod_exists` so no replacement pod is created.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        // No sandbox_k8s_executions row → `resolve_pod_ip` misses → recovery.
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;

        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
        );

        // Empty nodes inventory → the admission capacity probe reports zero
        // eligible nodes → EmptyCluster rejection (no Redis needed).
        let admission = std::sync::Arc::new(
            crate::cluster_admission::ClusterSandboxAdmission::with_config(
                Substrate::Direct(Box::new(
                    crate::controllers::substrate::direct::DirectSubstrate::new(pool.clone()),
                )),
                crate::cluster_admission::ClusterSandboxAdmissionConfig {
                    enabled: true,
                    refill_rps: 6.0,
                    burst: 60.0,
                    redis_url: None,
                },
                pplx_space_feature_flags::FeatureFlagClient::disabled(),
            ),
        );

        let result = substrate
            .runtime_client(
                sandbox_id,
                crate::controllers::substrate::RecoveryCharge::enforce(Some(&admission)),
            )
            .await;
        let execution_state: String = sqlx::query_scalar(
            "SELECT state FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;

        assert!(
            result.is_err(),
            "rejected admission must fail the recovery instead of recreating the pod",
        );
        assert!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .is_empty(),
            "no replacement pod may be created when admission rejects the activation",
        );
        assert_eq!(
            execution_state, "recovery_failed",
            "admission rejection must release the recovery claim for a later retry",
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn no_activity_spaced_path_charges_recovery_for_lost_pod() -> Result<(), BoxError> {
        // Public-path regression: the no-activity spaced client path
        // (`spaced_get_status` / `spaced_wait_ready`, and `reconnect_node_api`'s
        // no-activity reconnect) recovers a lost pod through the plain
        // `runtime_client`. All of them route through this exact
        // `connect_spaced_runtime_client(…, None)` call, so a rejecting
        // limiter must block the recreate — the callee-level test above can't
        // catch a public call site that forgets to enforce.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;

        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let substrate = Substrate::Kubernetes(Box::new(substrate_with_store_and_client(
            pool.clone(),
            mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
        )));
        let admission = std::sync::Arc::new(
            crate::cluster_admission::ClusterSandboxAdmission::with_config(
                Substrate::Direct(Box::new(
                    crate::controllers::substrate::direct::DirectSubstrate::new(pool.clone()),
                )),
                crate::cluster_admission::ClusterSandboxAdmissionConfig {
                    enabled: true,
                    refill_rps: 6.0,
                    burst: 60.0,
                    redis_url: None,
                },
                pplx_space_feature_flags::FeatureFlagClient::disabled(),
            ),
        );

        let mut sandbox = test_sandbox_with_lifecycle("started");
        sandbox.id = sandbox_id;

        let result = crate::controllers::sandbox::spaced::connect_spaced_runtime_client(
            &substrate,
            Some(&admission),
            true,
            &pool,
            &sandbox,
            SandboxApiMetricIdentity::ecw("spaced"),
            None, // no-activity branch → plain runtime_client recovery
        )
        .await;

        assert!(
            result.is_err(),
            "no-activity spaced recovery must be shed by the limiter",
        );
        assert!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .is_empty(),
            "no pod may be recreated when the no-activity spaced path is admission-rejected",
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn control_runtime_client_propagates_admission_rejection_without_recreating_pod()
    -> Result<(), BoxError> {
        // The control-plane path's fallback arm must not swallow the recovery
        // admission rejection: for a `started` k8s sandbox with a lost pod, the
        // first runtime_client charges and is rejected (429). If that 429 fell
        // into the dormant-wake fallback, `charge_control_wake_activation` is a
        // no-op for `started` and `wake_execution` would recreate the pod
        // anyway — re-opening the exact bypass this PR closes.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;

        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let substrate = Substrate::Kubernetes(Box::new(substrate_with_store_and_client(
            pool.clone(),
            mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
        )));
        let admission = crate::cluster_admission::ClusterSandboxAdmission::with_config(
            Substrate::Direct(Box::new(
                crate::controllers::substrate::direct::DirectSubstrate::new(pool.clone()),
            )),
            crate::cluster_admission::ClusterSandboxAdmissionConfig {
                enabled: true,
                refill_rps: 6.0,
                burst: 60.0,
                redis_url: None,
            },
            pplx_space_feature_flags::FeatureFlagClient::disabled(),
        );
        let controller = crate::controllers::sandbox::SandboxController::with_cluster_admission(
            substrate, admission,
        );

        let mut sandbox = test_sandbox_with_lifecycle("started");
        sandbox.id = sandbox_id;
        sandbox.managed_by = "k8s".to_string();

        let result = controller
            .control_runtime_client(
                &pool,
                &sandbox,
                crate::controllers::sandbox::lifecycle::SandboxAction::Update,
            )
            .await;

        assert!(
            result.is_err(),
            "control path must surface the admission rejection, not fall back to wake",
        );
        assert_eq!(
            result.err().map(|err| err.status()),
            Some(axum::http::StatusCode::TOO_MANY_REQUESTS),
            "the rejection must propagate as a 429, not be swallowed by the wake fallback",
        );
        assert!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .is_empty(),
            "no pod may be recreated when the control-path recovery is admission-rejected",
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ensure_pod_exists_preserves_observed_invalid_stateful_generation_create_claim()
    -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let artifact_id = Uuid::now_v7();
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(
            "UPDATE sandboxes
                SET current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1",
        )
        .bind(sandbox_id)
        .bind(artifact_id)
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active)
             VALUES
                 ($1, 1, 'space-sbx-old-g1', 'space', 'replaced', FALSE),
                 ($1, 2, 'space-sbx-old-g2', 'space', 'creating', TRUE)",
        )
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
        let substrate = substrate_with_store(pool.clone());
        let mut sandbox = test_sandbox_with_lifecycle("started");
        sandbox.id = sandbox_id;

        let err = substrate
            .ensure_pod_exists(
                &sandbox,
                None,
                None,
                None,
                PodMaterializationOwnership::default(),
            )
            .await
            .expect_err("stateful generation create claim should return retryable timeout");

        assert!(matches!(
            err,
            SubstrateError::RestoreContextUnavailable { message }
                if message.contains("refusing fresh Kubernetes pod")
        ));
        let active_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            active_count, 1,
            "observed create claim must remain active for its owner to finish"
        );

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn observed_create_claim_replaced_before_return_is_retryable_not_bad_gateway()
    -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active)
             VALUES
                 ($1, 1, 'space-sbx-observed-g1', 'space', 'creating', TRUE)",
        )
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
        let substrate = substrate_with_store(pool.clone());
        let observed_claim = substrate
            .store
            .ensure_active_execution_claim(sandbox_id)
            .await?;
        assert!(
            !observed_claim.created,
            "test must model a request that observed another request's create claim"
        );

        sqlx::query(
            "UPDATE sandbox_k8s_executions
                SET active = FALSE,
                    state = 'replaced',
                    updated_at = NOW(),
                    terminated_at = NOW()
              WHERE sandbox_id = $1
                AND generation = $2",
        )
        .bind(sandbox_id)
        .bind(observed_claim.row.generation)
        .execute(&pool)
        .await?;
        let mut sandbox = test_sandbox_with_lifecycle("started");
        sandbox.id = sandbox_id;

        let err = substrate
            .require_observed_create_claim_in_flight(&sandbox, &observed_claim)
            .await
            .expect_err("replaced observed claim should not return success");

        assert!(matches!(
            err,
            SubstrateError::Timeout { ref message, phase: None }
                if message.contains("was replaced before pod creation")
        ));
        let controller_error = ControllerError::from(err);
        assert_eq!(controller_error.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_ne!(controller_error.status(), StatusCode::BAD_GATEWAY);

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failed_create_rejects_stale_pod_loaded_by_recovery_get() -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let storage_uri = "s3://space-test/suspend-snapshots/current?region=us-east-1";
        insert_test_sandbox_row(&pool, sandbox_id, "restoring").await?;
        let sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        let deleted_pods = Arc::new(AtomicUsize::new(0));
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            failed_create_then_stale_restore_pod_kubernetes_client(Arc::clone(&deleted_pods)),
        );

        // Act.
        substrate
            .ensure_pod_exists(
                &sandbox,
                None,
                None,
                Some(storage_uri),
                PodMaterializationOwnership::default(),
            )
            .await?;

        // Assert.
        assert_eq!(deleted_pods.load(Ordering::SeqCst), 1);
        let active_executions: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(active_executions, 0);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ensure_pod_exists_recovers_started_after_final_replaced_conflict()
    -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let stale_artifact_id = Uuid::now_v7();
        let current_artifact_id = Uuid::now_v7();
        let current_uri = "s3://space-test/suspend-snapshots/current?region=us-east-1";

        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(
            "UPDATE sandboxes
                SET placement = 'unplaced',
                    active_node_id = NULL,
                    target_node_id = NULL,
                    current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1",
        )
        .bind(sandbox_id)
        .bind(stale_artifact_id)
        .execute(&pool)
        .await?;
        insert_ready_full_snapshot_relative_to_lifecycle(
            &pool,
            sandbox_id,
            "s3://space-test/suspend-snapshots/stale?region=us-east-1",
            -60,
            stale_artifact_id,
        )
        .await?;
        insert_ready_full_snapshot_relative_to_lifecycle(
            &pool,
            sandbox_id,
            current_uri,
            60,
            current_artifact_id,
        )
        .await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active)
             VALUES
                 ($1, 7, 'space-sbx-conflict-g7', 'space', 'running', TRUE)",
        )
        .bind(sandbox_id)
        .execute(&pool)
        .await?;

        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            conflict_then_created_kubernetes_client(Arc::clone(&captured_pod_creates)),
        );
        let mut sandbox = test_sandbox_with_lifecycle("started");
        sandbox.id = sandbox_id;

        substrate
            .ensure_pod_exists(
                &sandbox,
                None,
                None,
                Some(current_uri),
                PodMaterializationOwnership::default(),
            )
            .await?;

        {
            let created_pods = captured_pod_creates.lock().expect("capture mutex poisoned");
            assert_eq!(
                created_pods.len(),
                1,
                "the stale conflict path should not create a replacement pod after reverting to suspended"
            );
        }

        let (lifecycle, current_artifact_id): (String, Option<LocalArtifactId>) = sqlx::query_as(
            "SELECT lifecycle, current_local_artifact_id
             FROM sandboxes
             WHERE id = $1",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(lifecycle, "suspended");
        assert_eq!(
            current_artifact_id, None,
            "stale marker must be cleared so restore selects the newest durable snapshot"
        );

        let active_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(active_count, 0);

        let terminal_state: String = sqlx::query_scalar(
            "SELECT state FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND generation = 7",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(terminal_state, "replaced");

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn mock_api_repro_stale_create_replacement_restores_from_source_snapshot()
    -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let source_artifact_id = Uuid::now_v7();
        let late_artifact_id = Uuid::now_v7();
        let source_uri = "s3://space-test/suspend-snapshots/g2-source?region=us-east-1";
        let late_branch_uri = "s3://space-test/suspend-snapshots/g3-late?region=us-east-1";

        insert_test_sandbox_row(&pool, sandbox_id, "restoring").await?;
        sqlx::query(
            "UPDATE sandboxes
                SET current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1",
        )
        .bind(sandbox_id)
        .bind(late_artifact_id)
        .execute(&pool)
        .await?;
        insert_ready_full_snapshot_relative_to_lifecycle(
            &pool,
            sandbox_id,
            source_uri,
            -60,
            source_artifact_id,
        )
        .await?;
        insert_ready_full_snapshot_relative_to_lifecycle(
            &pool,
            sandbox_id,
            late_branch_uri,
            60,
            late_artifact_id,
        )
        .await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active,
                  created_at, updated_at)
             VALUES
                 ($1, 2, 'space-sbx-old-g2', 'space', 'replaced', FALSE,
                  NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes'),
                 ($1, 3, 'space-sbx-old-g3', 'space', 'creating', TRUE,
                  NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes')",
        )
        .bind(sandbox_id)
        .execute(&pool)
        .await?;

        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
        );
        let mut sandbox = test_sandbox_with_lifecycle("restoring");
        sandbox.id = sandbox_id;

        substrate
            .ensure_pod_exists(
                &sandbox,
                None,
                None,
                None,
                PodMaterializationOwnership::default(),
            )
            .await?;

        {
            let created_pods = captured_pod_creates.lock().expect("capture mutex poisoned");
            assert_eq!(created_pods.len(), 1);
            let annotations = created_pods[0]
                .pointer("/metadata/annotations")
                .and_then(serde_json::Value::as_object)
                .expect("created pod request should carry annotations");
            assert_eq!(
                annotations
                    .get(SANDBOX_EXECUTION_GENERATION_ANNOTATION)
                    .and_then(serde_json::Value::as_str),
                Some("4")
            );
            assert_eq!(
                annotations
                    .get(SANDBOX_CHECKPOINT_ANNOTATION)
                    .and_then(serde_json::Value::as_str),
                Some("suspend"),
                "replacement pod must be a restore, not an on-demand fresh boot"
            );
            assert_eq!(
                annotations
                    .get(SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION)
                    .and_then(serde_json::Value::as_str),
                Some(source_uri),
                "replacement pod must restore from the pre-restore g2 source snapshot"
            );
            assert_ne!(
                annotations
                    .get(SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION)
                    .and_then(serde_json::Value::as_str),
                Some(late_branch_uri),
                "replacement pod must ignore late artifacts from the stale g3 branch"
            );
        }

        let active_generation: i64 = sqlx::query_scalar(
            "SELECT generation FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(active_generation, 4);

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn expired_restore_redrive_with_vanished_source_uses_remote_snapshot_without_hard_pin()
    -> Result<(), BoxError> {
        // Arrange.
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let vanished_node_id = NodeId::new();
        let storage_uri = "s3://space-test/suspend-snapshots/portable?region=us-east-1";

        insert_test_sandbox_row(&pool, sandbox_id, "restoring").await?;
        sqlx::query(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '10.0.0.10', 'worker', 'k8s', 'ready')",
        )
        .bind(vanished_node_id)
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO node_k8s (node_id, k8s_node_name)
             VALUES ($1, 'vanished-source-node')",
        )
        .bind(vanished_node_id)
        .execute(&pool)
        .await?;
        sqlx::query(
            "UPDATE sandboxes
                SET active_node_id = $2,
                    restore_lease_expires_at = NOW() - INTERVAL '1 second',
                    restore_lease_token = $3
              WHERE id = $1",
        )
        .bind(sandbox_id)
        .bind(vanished_node_id)
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await?;
        insert_ready_full_snapshot_relative_to_lifecycle(
            &pool,
            sandbox_id,
            storage_uri,
            -60,
            Uuid::now_v7(),
        )
        .await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, k8s_node_name,
                  state, active, created_at, updated_at)
             VALUES
                 ($1, 1, 'space-sbx-dead-source-g1', 'space',
                  'vanished-source-node', 'creating', TRUE,
                  NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes')",
        )
        .bind(sandbox_id)
        .execute(&pool)
        .await?;

        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
        );
        let stale_sandbox = fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");

        // Act.
        let restore_claim = substrate
            .store
            .begin_restore_lifecycle(sandbox_id, false)
            .await?;
        let prepared = substrate
            .store
            .prepare_wake_sandbox(&stale_sandbox, sandbox_id, Some(restore_claim))
            .await?;
        let restore_plan = substrate
            .suspend_restore_plan(&prepared.sandbox, false)
            .await?;
        substrate
            .ensure_pod_exists(
                &prepared.sandbox,
                None,
                restore_plan.source_node_name.as_deref(),
                restore_plan.storage_uri.as_deref(),
                PodMaterializationOwnership::default(),
            )
            .await?;

        // Assert.
        assert!(matches!(
            restore_claim,
            RestoreClaim::RedroveRestoring { .. }
        ));
        assert_eq!(
            restore_plan.source_node_name.as_deref(),
            Some("vanished-source-node")
        );
        assert_eq!(restore_plan.storage_uri.as_deref(), Some(storage_uri));
        {
            let created_pods = captured_pod_creates.lock().expect("capture mutex poisoned");
            assert_eq!(created_pods.len(), 1);
            let pod = &created_pods[0];
            assert_eq!(
                pod.pointer("/metadata/annotations/space.pplx.net~1suspend-restore-storage-uri")
                    .and_then(serde_json::Value::as_str),
                Some(storage_uri)
            );
            assert!(
                pod.pointer("/spec/nodeName").is_none(),
                "portable restore must not hard-pin the vanished source node"
            );
            let preferred_affinity = pod
                .pointer(
                    "/spec/affinity/nodeAffinity/preferredDuringSchedulingIgnoredDuringExecution",
                )
                .expect("portable restore should retain the source as a preference")
                .to_string();
            assert!(preferred_affinity.contains("vanished-source-node"));
            let required_affinity = pod
                .pointer(
                    "/spec/affinity/nodeAffinity/requiredDuringSchedulingIgnoredDuringExecution",
                )
                .expect("sandbox pod should retain its ordinary required affinity")
                .to_string();
            assert!(!required_affinity.contains("vanished-source-node"));
        }

        let (generation, state): (i64, String) = sqlx::query_as(
            "SELECT generation, state
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(generation, 2);
        assert_eq!(state, "created");

        Ok(())
    }

    #[test]
    fn on_demand_pod_create_allows_initial_generation() {
        let sandbox_id = SandboxId::new();

        let result = parse_pod_create_context(sandbox_id, 1, "started", false, None, None, None);

        assert!(matches!(result, Ok(PodCreateContext::OnDemand)));
    }

    #[test]
    fn on_demand_pod_create_rejects_stopped_restart_after_initial_generation() {
        let sandbox_id = SandboxId::new();

        let result = parse_pod_create_context(sandbox_id, 2, "stopped", true, None, None, None);

        assert!(matches!(
            result,
            Err(SubstrateError::RestoreContextUnavailable { message })
                if message.contains("refusing fresh Kubernetes pod")
        ));
    }

    #[test]
    fn stopped_pod_create_preserves_node_local_restore_intent() {
        let sandbox_id = SandboxId::new();

        let result = parse_pod_create_context(
            sandbox_id,
            2,
            "stopped",
            true,
            None,
            Some("source-node"),
            None,
        );

        assert!(matches!(
            result,
            Ok(PodCreateContext::Restore(RestoreContext::UserRestore {
                suspend_restore_source_node: Some("source-node"),
                direct_node_name: Some("source-node"),
                preferred_node_name: None,
                ..
            }))
        ));
    }

    /// The decision `stopped_restore_plan` makes, at the layer that renders
    /// it: carrying both the source node and the evacuated artifact's URI is
    /// not a contradiction the builder has to break, it selects a third
    /// placement — the source node as a *preference* with S3 as the fallback
    /// any other node restores from. Contrast
    /// `stopped_pod_create_preserves_node_local_restore_intent` above, where
    /// the same source node with no URI pre-binds the pod via `nodeName`.
    #[test]
    fn stopped_pod_create_prefers_the_source_node_once_the_disk_artifact_is_durable() {
        let sandbox_id = SandboxId::new();

        let result = parse_pod_create_context(
            sandbox_id,
            2,
            "stopped",
            true,
            None,
            Some("source-node"),
            Some("s3://space-test/stopped-disk?region=us-east-1"),
        );

        assert!(
            matches!(
                result,
                Ok(PodCreateContext::Restore(RestoreContext::UserRestore {
                    suspend_restore_storage_uri: Some(
                        "s3://space-test/stopped-disk?region=us-east-1"
                    ),
                    suspend_restore_source_node: Some("source-node"),
                    direct_node_name: None,
                    preferred_node_name: Some("source-node"),
                }))
            ),
            "a durable disk artifact must downgrade the stopped restart's node pin to a \
             preference rather than drop either input"
        );
    }

    #[test]
    fn on_demand_pod_create_rejects_live_recovery_after_initial_generation() {
        let sandbox_id = SandboxId::new();

        let result = parse_pod_create_context(sandbox_id, 2, "started", true, None, None, None);

        assert!(matches!(
            result,
            Err(SubstrateError::RestoreContextUnavailable { message })
                if message.contains("refusing fresh Kubernetes pod")
        ));
    }

    #[test]
    fn on_demand_pod_create_rejects_transient_lifecycle_after_initial_generation() {
        let sandbox_id = SandboxId::new();

        for lifecycle in ["creating", "created", "starting"] {
            let result = parse_pod_create_context(sandbox_id, 2, lifecycle, true, None, None, None);

            assert!(
                matches!(
                    result,
                    Err(SubstrateError::RestoreContextUnavailable { message })
                        if message.contains("refusing fresh Kubernetes pod")
                ),
                "{lifecycle} after initial generation must not create an on-demand pod"
            );
        }
    }

    #[test]
    fn pod_create_context_preserves_restore_intent_after_initial_generation() {
        let sandbox_id = SandboxId::new();

        let result = parse_pod_create_context(
            sandbox_id,
            2,
            "started",
            true,
            None,
            Some("source-node"),
            None,
        );

        assert!(matches!(
            result,
            Ok(PodCreateContext::Restore(RestoreContext::UserRestore {
                suspend_restore_source_node: Some("source-node"),
                direct_node_name: Some("source-node"),
                preferred_node_name: None,
                ..
            }))
        ));
    }

    #[test]
    fn pod_create_context_allows_storage_only_restore_after_initial_generation() {
        let sandbox_id = SandboxId::new();

        let result = parse_pod_create_context(
            sandbox_id,
            2,
            "started",
            true,
            None,
            None,
            Some("s3://space-test/g2-source?region=us-east-1"),
        );

        assert!(matches!(
            result,
            Ok(PodCreateContext::Restore(RestoreContext::UserRestore {
                suspend_restore_storage_uri: Some("s3://space-test/g2-source?region=us-east-1"),
                suspend_restore_source_node: None,
                direct_node_name: None,
                preferred_node_name: None,
            }))
        ));
    }

    fn pod_with_annotation(key: &str, value: &str) -> Pod {
        let mut annotations = BTreeMap::new();
        annotations.insert(key.to_string(), value.to_string());
        Pod {
            metadata: kube::api::ObjectMeta {
                annotations: Some(annotations),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// The sandbox lifecycle reads `suspended` for the whole restore window
    /// (it only flips once a restore finalizes). Concurrent restore requests
    /// fanned across `space-api` replicas all load that suspended sandbox and
    /// inspect the active execution's own in-progress restore pod. Treating
    /// that pod as `SandboxSuspended`-stale makes each restorer delete it,
    /// finish the execution row, and allocate a fresh generation — producing
    /// duplicate pods (g2, g3, …) on different nodes and violating the
    /// one-pod-per-sandbox invariant. A pod carrying restore annotations is
    /// the restore-in-progress, not a pre-suspend husk, so it must not be
    /// deemed stale by suspension alone.
    #[test]
    fn stale_existing_pod_reason_keeps_in_progress_restore_pod_for_suspended_sandbox() {
        let sandbox = test_sandbox_with_lifecycle("suspended");

        let s3_restore_pod = pod_with_annotation(
            SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION,
            "s3://snap/current",
        );
        assert_eq!(
            stale_existing_pod_reason(&sandbox, &s3_restore_pod, Some("s3://snap/current")),
            None
        );

        let local_restore_pod = pod_with_annotation(
            SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION,
            "source-node",
        );
        assert_eq!(
            stale_existing_pod_reason(&sandbox, &local_restore_pod, None),
            None
        );
    }

    #[test]
    fn stale_existing_pod_reason_replaces_pending_local_restore_when_s3_becomes_ready() {
        let sandbox = test_sandbox_with_lifecycle("restoring");
        let mut annotations = BTreeMap::new();
        annotations.insert(
            SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION.to_string(),
            "source-node".to_string(),
        );
        let pod = Pod {
            metadata: kube::api::ObjectMeta {
                annotations: Some(annotations),
                ..Default::default()
            },
            ..Default::default()
        };

        assert_eq!(stale_existing_pod_reason(&sandbox, &pod, None), None);
        assert_eq!(
            stale_existing_pod_reason(&sandbox, &pod, Some("s3://snap/current")),
            Some(StalePodReason::LocalOnlyRestoreNowPortable)
        );
    }

    #[test]
    fn stale_existing_pod_reason_treats_terminal_pod_as_stale() {
        let sandbox = test_sandbox_with_lifecycle("started");
        let pod = Pod {
            status: Some(PodStatus {
                phase: Some("Succeeded".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(
            stale_existing_pod_reason(&sandbox, &pod, None),
            Some(StalePodReason::TerminalPhase(TerminalPodPhase::Succeeded))
        );
    }

    /// Regression for the orphan-reconciler jam: `delete_execution` must not
    /// short-circuit when the tracked pod is `Pending+Unschedulable`. That pod
    /// shape makes `resolve_pod_ip` return `SubstrateError::Unavailable`; the
    /// delete path has to treat it as "no live runtime", skip the in-VM
    /// stop/delete gRPC calls, and proceed to delete the pod object.
    ///
    /// A full `delete_execution` test would need a Postgres-backed `K8sStore`
    /// and a kube client fixture, neither of which exists in these unit tests,
    /// so we assert the decision predicate against the exact error
    /// `resolve_pod_ip` builds for an unschedulable pod.
    #[test]
    fn delete_skips_runtime_cleanup_for_unschedulable_pod() {
        let status = PodStatus {
            phase: Some("Pending".to_string()),
            conditions: Some(vec![k8s_openapi::api::core::v1::PodCondition {
                type_: "PodScheduled".to_string(),
                status: "False".to_string(),
                reason: Some("Unschedulable".to_string()),
                message: Some(
                    "0/109 nodes are available: 109 node(s) didn't match Pod's node affinity/selector. [NodeAffinity]"
                        .to_string(),
                ),
                ..Default::default()
            }]),
            ..Default::default()
        };
        let message =
            pod_unschedulable_message(Some(&status)).expect("pod should be unschedulable");
        let err = SubstrateError::Unavailable {
            message: format!("sandbox pod is unschedulable: {message}"),
        };

        assert!(delete_can_skip_runtime_cleanup(&err));
    }

    /// Genuine infra faults surfaced by `resolve_pod_ip` (DB error, kube-API
    /// query failure, malformed pod IP) must still propagate out of the delete
    /// path — the pod-delete loop would hit the same fault.
    #[test]
    fn delete_propagates_genuine_infra_errors() {
        let propagating = [
            SubstrateError::Failed {
                message: "failed to query K8s pod: connection refused".to_string(),
            },
            SubstrateError::NotFound {
                message: "pod not found".to_string(),
            },
            SubstrateError::Timeout {
                message: "kube-API timeout".to_string(),
                phase: None,
            },
            SubstrateError::Conflict {
                message: "conflict".to_string(),
            },
        ];
        for err in &propagating {
            assert!(
                !delete_can_skip_runtime_cleanup(err),
                "{err:?} must propagate"
            );
        }
    }

    /// `retire_execution` exists so failure reconcilers can release pods
    /// without destroying node-local sandbox state — a row that failed
    /// mid-suspend may hold the only up-to-date copy of the sandbox on the
    /// node. The teardown-mode predicate is the single switch that decides
    /// whether the in-VM cleanup sends the sbx-mgr `Delete`; `Retire` must
    /// never do so. (A full `retire_execution` test has the same missing
    /// fixture problem as `delete_execution`, documented above, so we pin
    /// the decision predicates.)
    #[test]
    fn retire_teardown_never_sends_sandbox_delete() {
        assert!(!ExecutionTeardown::Retire.sends_sandbox_delete());
        assert!(ExecutionTeardown::Delete.sends_sandbox_delete());
    }

    /// A retire that proceeds past failed preservation work would delete the
    /// pod and forget pod tracking, marking the execution inactive — which
    /// drops the sandbox from the failed-cleanup retry scan
    /// (`list_failed_active_executions` only lists active executions) and
    /// silently skips stopped-disk backup. `Retire` must require preservation
    /// success and therefore sends the stop; `Delete` skips that pre-stop so
    /// sbx-mgr can guard an in-flight suspend before deleting.
    #[test]
    fn retire_teardown_requires_stop_success() {
        assert!(ExecutionTeardown::Retire.requires_stop_success());
        assert!(!ExecutionTeardown::Delete.requires_stop_success());
        assert!(ExecutionTeardown::Retire.sends_sandbox_stop());
        assert!(!ExecutionTeardown::Delete.sends_sandbox_stop());
    }

    /// A retryable refusal from sbx-mgr means it deliberately retained the
    /// node-local row. Kubernetes teardown must stop before deleting the pod
    /// or forgetting execution tracking, otherwise the publisher disappears
    /// and the retry can never observe the retained suspend outcome.
    #[test]
    fn retryable_sbxmgr_delete_refusal_defers_kubernetes_teardown() {
        assert_eq!(
            classify_sandbox_delete_rpc_error(
                &pplx_space_sbx_mgr_client::suspend_publication_pending_status(
                    "suspend publication is still pending",
                ),
            ),
            SandboxDeleteRpcErrorDisposition::DeferTeardown
        );
        assert_eq!(
            classify_sandbox_delete_rpc_error(
                &pplx_space_sbx_mgr_client::retained_filesystem_only_copy_status(
                    "retained filesystem is the only copy",
                ),
            ),
            SandboxDeleteRpcErrorDisposition::RetainedFilesystemOnlyCopy
        );
        assert_eq!(
            classify_sandbox_delete_rpc_error(&tonic::Status::aborted("another cleanup conflict",)),
            SandboxDeleteRpcErrorDisposition::ContinueTeardown
        );
        assert_eq!(
            classify_sandbox_delete_rpc_error(&tonic::Status::not_found("already gone")),
            SandboxDeleteRpcErrorDisposition::ContinueTeardown
        );
        assert_eq!(
            classify_sandbox_delete_rpc_error(&tonic::Status::internal("cleanup failed")),
            SandboxDeleteRpcErrorDisposition::ContinueTeardown
        );
    }

    /// Retire may proceed past the backup enqueue only when there is provably
    /// nothing it can preserve: the sbx-mgr sandbox row is missing, or the
    /// refusal carries one of the terminal `artifact_backup_signals` markers
    /// (a `failed` local state can never take a current-instance backup; a
    /// suspended instance's current artifact is the full snapshot whose
    /// durability the suspend machinery owns). Aborting on those would wedge
    /// the failure reconciler's retry loop forever while the leftover pod
    /// keeps feeding kubelet CreatePodSandbox retries. Every other enqueue
    /// failure leaves preservation uncertain (or convergeable, e.g. a
    /// still-stopping row) and must keep the execution active for retry.
    #[test]
    fn retire_skips_backup_for_missing_source_or_terminal_refusal() {
        use pplx_space_common_core::artifact_backup_signals::{
            CURRENT_INSTANCE_BACKUP_PUBLISHES_FULL_MARKER,
            CURRENT_INSTANCE_BACKUP_SANDBOX_FAILED_MARKER,
        };

        let skippable = [
            SubstrateError::NotFound {
                message: "sandbox row is gone".to_string(),
            },
            SubstrateError::Conflict {
                message: format!(
                    "node-api refused artifact backup: cannot back up current instance: \
                     {CURRENT_INSTANCE_BACKUP_SANDBOX_FAILED_MARKER}"
                ),
            },
            SubstrateError::Conflict {
                message: format!(
                    "node-api refused artifact backup: requested disk backup but \
                     {CURRENT_INSTANCE_BACKUP_PUBLISHES_FULL_MARKER}"
                ),
            },
        ];
        for err in &skippable {
            assert!(
                retire_can_skip_backup_enqueue_error(err),
                "{err:?} must let retire proceed"
            );
        }

        let retryable = [
            SubstrateError::Failed {
                message: "cluster-api lookup failed".to_string(),
            },
            SubstrateError::Conflict {
                message: "sandbox is still running".to_string(),
            },
            SubstrateError::Conflict {
                message: "cannot back up current instance: sandbox is stopping, \
                          must be stopped or suspended"
                    .to_string(),
            },
            SubstrateError::Unavailable {
                message: "pod worker unavailable".to_string(),
            },
            SubstrateError::Timeout {
                message: "enqueue timed out".to_string(),
                phase: None,
            },
        ];
        for err in &retryable {
            assert!(
                !retire_can_skip_backup_enqueue_error(err),
                "{err:?} must keep retire retryable"
            );
        }
    }

    #[test]
    fn pod_unschedulable_message_extracts_scheduler_reason() {
        let status = PodStatus {
            conditions: Some(vec![k8s_openapi::api::core::v1::PodCondition {
                type_: "PodScheduled".to_string(),
                status: "False".to_string(),
                reason: Some("Unschedulable".to_string()),
                message: Some("0/3 nodes are available".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        };

        assert_eq!(
            pod_unschedulable_message(Some(&status)),
            Some("0/3 nodes are available")
        );
    }

    fn test_sandbox_id() -> SandboxId {
        SandboxId::from_uuid(Uuid::nil())
    }

    fn test_cancel_tracker() -> Arc<CancelRateTracker> {
        Arc::new(CancelRateTracker::with_budget(64))
    }

    /// A defused CancelGuard does not spawn cleanup on drop.
    #[test]
    fn cancel_guard_defused_is_noop() {
        let guard = CancelGuard::new(
            test_sandbox_id(),
            "space-sbx-test".into(),
            1,
            pplx_space_common_core::types::SandboxActivationAttemptId::new(),
            test_cancel_tracker(),
        );
        guard.defuse();
    }

    /// An unarmed CancelGuard (never armed with k8s/pool) is a no-op on drop,
    /// covering the path where the future is cancelled before pod creation.
    #[test]
    fn cancel_guard_unarmed_drop_is_noop() {
        let guard = CancelGuard::new(
            test_sandbox_id(),
            "space-sbx-test".into(),
            1,
            pplx_space_common_core::types::SandboxActivationAttemptId::new(),
            test_cancel_tracker(),
        );
        assert!(guard.is_unarmed());
        drop(guard);
    }

    #[test]
    fn is_nodemgr_unreachable_matches_transport_signatures() {
        // tonic Code::Unavailable shows up rendered as the variant name.
        assert!(is_nodemgr_unreachable(&SubstrateError::Unavailable {
            message: "rpc failed: status: Unavailable, message: ...".into(),
        }));
        assert!(is_nodemgr_unreachable(&SubstrateError::Unavailable {
            message: "transport error: connection refused".into(),
        }));
        assert!(is_nodemgr_unreachable(&SubstrateError::Unavailable {
            message: "tcp connect error: ...".into(),
        }));
    }

    #[test]
    fn is_nodemgr_unreachable_matches_timeout_after_unavailable_probe() {
        // The command-ready wait helpers exit with `Timeout` after
        // swallowing `Code::Unavailable` probes; the message names node-mgr
        // only when a probe really returned `Unavailable`, and that suffix
        // is what classifies the brownout.
        assert!(is_nodemgr_unreachable(&command_ready_timeout_error(
            WaitReadyTimeoutPhase::GuestNotReady,
            true,
        )));
        // A vanilla timeout (no node-mgr suffix) is not a brownout signal.
        assert!(!is_nodemgr_unreachable(&SubstrateError::Timeout {
            message: "timed out waiting for sandbox command readiness".into(),
            phase: None,
        }));
    }

    #[test]
    fn phase_only_timeout_messages_do_not_name_nodemgr() {
        // A readiness timeout that saw no `Unavailable` probe carries only
        // the phase that stalled — it must not classify as a node-mgr
        // brownout. (The create path still defers cleanup for these: its
        // defer decision treats every `Timeout` as defer-eligible on its
        // own, so dropping the blanket node-mgr label changes no behavior.)
        for phase in [
            WaitReadyTimeoutPhase::PodNeverReady,
            WaitReadyTimeoutPhase::ProxyUnreachable,
            WaitReadyTimeoutPhase::GuestNotReady,
        ] {
            let err = command_ready_timeout_error(phase, false);
            assert!(!is_nodemgr_unreachable(&err), "{err:?}");
        }
    }

    #[test]
    fn ready_wait_bounds_phase_deadline_fires_before_the_outer_backstop() {
        // Regression guard for the budget-swallow bug: the default
        // `create_data_plane_ready_timeout` (120s) and the evacuation
        // command-ready wait (~600s) both exceed `CREATE_READY_BUDGET`
        // (105s), so without the inner clamp the outer budget always
        // cancelled the inner readiness wait first and every budget-bounded
        // readiness timeout lost its phase attribution (it surfaced as a
        // generic phase-less `Timeout`, which the node-drain metric then
        // tagged with the blanket `restore`).
        let now = Instant::now();
        let (outer, phase_deadline) = ready_wait_bounds(Duration::from_secs(600));
        assert!(phase_deadline < now + outer);
        assert_eq!(outer, CREATE_READY_BUDGET);
        // The clamped data-plane deadline also lands inside the backstop
        // even though its configured default exceeds it.
        let clamped = (now + DEFAULT_CREATE_DATA_PLANE_READY_TIMEOUT).min(phase_deadline);
        assert!(clamped < now + outer);
    }

    #[test]
    fn caller_ready_budget_is_not_shortened_by_phase_margin() {
        // Review-pointed regression: the phase margin must be reserved ABOVE
        // a caller-supplied readiness budget (extending the outer backstop),
        // not carved out of it — a wake with `ready_timeout_secs=30` must
        // still get 30s of effective readiness waiting, and a sub-margin
        // request keeps its full (tiny) wait instead of collapsing to zero.
        for requested in [Duration::from_secs(30), Duration::from_secs(3)] {
            let now = Instant::now();
            let (outer, phase_deadline) = ready_wait_bounds(requested);
            assert!(
                phase_deadline >= now + requested,
                "caller's requested {requested:?} wait was shortened by the phase margin"
            );
            assert_eq!(outer, requested + READY_WAIT_PHASE_MARGIN);
            // The backstop never exceeds the channel-safe budget.
            assert!(outer <= CREATE_READY_BUDGET);
        }
        // A deadline that has already passed yields a zero remaining window.
        assert_eq!(remaining_ready_window(Instant::now()), Duration::ZERO);
    }

    #[test]
    fn warm_pool_setup_rpc_timeout_stays_below_lifecycle_wait_budget() {
        assert_eq!(adapter::WARM_POOL_SETUP_RPC_TIMEOUT, Duration::from_secs(5));
        assert!(adapter::WARM_POOL_SETUP_RPC_TIMEOUT < NODE_RPC_TIMEOUT);
        assert!(
            adapter::WARM_POOL_SETUP_RPC_TIMEOUT
                < crate::controllers::substrate::NODE_RPC_BLOCKING_BUDGET
        );
    }

    #[test]
    fn command_ready_timeout_error_carries_typed_phase_and_phase_label() {
        for (wait_phase, label) in [
            (WaitReadyTimeoutPhase::PodNeverReady, "pod_never_ready"),
            (WaitReadyTimeoutPhase::ProxyUnreachable, "proxy_unreachable"),
            (WaitReadyTimeoutPhase::GuestNotReady, "guest_not_ready"),
        ] {
            let err = command_ready_timeout_error(wait_phase, false);
            assert_eq!(err.wait_ready_timeout_phase(), Some(wait_phase));
            let SubstrateError::Timeout { message, .. } = &err else {
                panic!("expected Timeout, got {err:?}");
            };
            assert!(message.contains(label), "{message}");
            assert!(!message.contains("node-mgr"), "{message}");
        }
        let unavailable = command_ready_timeout_error(WaitReadyTimeoutPhase::GuestNotReady, true);
        let SubstrateError::Timeout { message, .. } = &unavailable else {
            panic!("expected Timeout, got {unavailable:?}");
        };
        assert!(message.contains("guest_not_ready"), "{message}");
        assert!(message.contains("node-mgr"), "{message}");
    }

    #[test]
    fn is_nodemgr_unreachable_does_not_match_other_variants() {
        // Failed/NotFound/etc. should never trigger the defer path even if
        // their message happens to contain the word "transport".
        assert!(!is_nodemgr_unreachable(&SubstrateError::Failed {
            message: "transport error".into(),
        }));
        assert!(!is_nodemgr_unreachable(&SubstrateError::Unavailable {
            message: "sandbox has no active Kubernetes execution".into(),
        }));
    }

    #[test]
    fn is_nodemgr_unreachable_does_not_match_unrelated_connect_substring() {
        // Tightened from bare "connect" → "connect error" / "connection refused"
        // so unrelated phrases like "reconnect", "interconnect", or any
        // arbitrary message containing "connection" don't route to the
        // defer-cleanup path.
        assert!(!is_nodemgr_unreachable(&SubstrateError::Unavailable {
            message: "failed to reconnect to kube apiserver".into(),
        }));
        assert!(!is_nodemgr_unreachable(&SubstrateError::Unavailable {
            message: "interconnect health probe degraded".into(),
        }));
        assert!(!is_nodemgr_unreachable(&SubstrateError::Unavailable {
            message: "unexpected connection state".into(),
        }));
    }

    #[test]
    fn terminal_pod_phase_matches_only_succeeded_and_failed() {
        let succeeded = PodStatus {
            phase: Some("Succeeded".to_string()),
            ..Default::default()
        };
        let failed = PodStatus {
            phase: Some("Failed".to_string()),
            ..Default::default()
        };
        let running = PodStatus {
            phase: Some("Running".to_string()),
            ..Default::default()
        };

        assert_eq!(
            terminal_pod_phase(Some(&succeeded)),
            Some(TerminalPodPhase::Succeeded)
        );
        assert_eq!(
            terminal_pod_phase(Some(&failed)),
            Some(TerminalPodPhase::Failed)
        );
        assert_eq!(terminal_pod_phase(Some(&running)), None);
        assert_eq!(terminal_pod_phase(None), None);
    }

    /// The pooled pod a [`pool_claim_fixture`] puts in front of the claim.
    struct PooledSlotPod<'a> {
        /// The pod's phase. Anything but `Running` makes the claimed pod
        /// unusable.
        phase: &'a str,
        /// The node the pod is bound to, or `None` for an unscheduled pod.
        node_name: Option<&'a str>,
        /// The nodes the reflector holds, as `(name, node-group label)`. A node
        /// the pod names but this list omits is a reflector miss.
        nodes: &'a [(&'a str, Option<&'a str>)],
    }

    /// One ready warm-pool slot, its pod, and a caller — enough to drive the
    /// real `claim_pooled_execution` against a real database with the
    /// node-group gate on.
    ///
    /// The catalog is fixed: a generic `seed` template and one default generic
    /// `workers` group, so the resolver always produces a set that admits
    /// shared capacity. What each test varies is the pod and the fleet, which
    /// is what the claim's decisions are actually about.
    struct PoolClaimFixture {
        db: pplx_space_db_test_fixtures::TestDb,
        substrate: KubernetesSubstrate,
        slot_id: Uuid,
        release_id: Uuid,
        actor: crate::controllers::substrate::ActorRef,
        env: BTreeMap<String, String>,
        auto_lifecycle: pplx_space_common_core::types::AutoLifecyclePolicy,
        network_policy: pplx_space_net_common::NetworkPolicyV1,
    }

    impl PoolClaimFixture {
        fn request(&self) -> crate::controllers::substrate::PooledExecutionClaimRequest<'_> {
            crate::controllers::substrate::PooledExecutionClaimRequest {
                metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity {
                    op: "create",
                    surface: pplx_space_metrics::sandbox::api::SandboxApiSurface::Native,
                    client_class: pplx_space_metrics::sandbox::api::ClientClass::Other,
                },
                actor: &self.actor,
                template: "seed",
                content_hash: None,
                template_release_id: Some(self.release_id),
                runtime: "firecracker",
                vcpu: Some(2),
                memory_mib: Some(1024),
                env: &self.env,
                auto_lifecycle: &self.auto_lifecycle,
                network_policy_override: None,
                network_policy_generation: 1,
                template_network_policy: &self.network_policy,
                template_network_policy_generation: 1,
                volume_encryption: false,
            }
        }

        /// The slot's `(state, claim_id)`, which is what tells release from
        /// invalidate: `release` moves the state back to `ready`, while
        /// `invalidate` leaves it `claimed` with the claim id stripped so no
        /// later claim can pick it up.
        ///
        /// This is also the positive control for the `claim.is_none()`
        /// assertions above it. A fixture that never produced a claimable slot
        /// would satisfy `is_none()` trivially — and would leave the row
        /// `("ready", None)` here, which no caller of this accepts. So the two
        /// assertions together say "a slot was claimed and then refused",
        /// not merely "nothing came back".
        async fn slot_disposition(&self) -> Result<(String, Option<Uuid>), BoxError> {
            let row = sqlx::query_as(sql!(
                "SELECT state, claim_id FROM kubernetes_pool_slots WHERE id = $1"
            ))
            .bind(self.slot_id)
            .fetch_one(&self.db.pool)
            .await?;
            Ok(row)
        }
    }

    async fn pool_claim_fixture(pod: PooledSlotPod<'_>) -> Result<PoolClaimFixture, BoxError> {
        use pplx_space_cluster_pool::{NewPoolSlot, WarmPoolStore};
        use pplx_space_common_core::types::{ContentHash, Digest, MemorySize, VcpuCount};

        use crate::controllers::substrate::ActorRef;

        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let org_id = OrgId::new();
        let user_id = UserId::new();
        sqlx::query(sql!(
            "INSERT INTO organizations (id, name, slug) VALUES ($1, 'Pool Claim Test', $2)"
        ))
        .bind(org_id)
        .bind(format!("pool-claim-test-{org_id}"))
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
             VALUES ($1, $2, 'x', 'Pool Claim', $3, 'member')"
        ))
        .bind(user_id)
        .bind(format!("pool-claim-{user_id}@example.com"))
        .bind(org_id)
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO templates (id, name, org_id, visibility)
             VALUES ($1, 'seed', NULL, 'generic')"
        ))
        .bind(Uuid::now_v7())
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO node_groups (id, name, org_id, template_policy, is_default)
             VALUES ($1, 'workers', NULL, 'default', TRUE)"
        ))
        .bind(Uuid::now_v7())
        .execute(&db.pool)
        .await?;
        let release_id = Uuid::now_v7();
        let content_hash = ContentHash::new(Digest::sha256([0x77; 32]));
        sqlx::query(sql!(
            "INSERT INTO template_releases (id, template_name, type, hash)
             VALUES ($1, 'seed', 'fc', $2)"
        ))
        .bind(release_id)
        .bind(content_hash.to_string())
        .execute(&db.pool)
        .await?;
        let config_id = Uuid::now_v7();
        sqlx::query(sql!(
            "INSERT INTO kubernetes_pool_configs (
                 id, template_release_id, vcpu, memory_mib, target_ready_slots,
                 refill_per_tick, max_warming_slots, namespace, enabled
             ) VALUES ($1, $2, 2, 1024, 1, 1, 1, 'space', TRUE)"
        ))
        .bind(config_id)
        .bind(release_id)
        .execute(&db.pool)
        .await?;

        let store = WarmPoolStore::new(db.pool.clone());
        let reserved = SandboxId::new();
        let slot = store
            .insert_warming_slot(NewPoolSlot {
                config_id,
                template_release_id: release_id,
                reserved_sandbox_id: reserved,
                template_name: &TemplateName::parse("seed")?,
                content_hash,
                vcpu: VcpuCount::try_from_u32(2)?,
                memory: MemorySize::mib(1024)?,
                pod_name: "space-sbx-pooled-g1",
                pod_namespace: "space",
            })
            .await?;
        store
            .mark_slot_ready(
                slot.id,
                "pool-pod-uid",
                "10.90.0.7",
                pod.node_name.unwrap_or("unscheduled"),
            )
            .await?;

        let config = SandboxPodConfigValues::default();
        let pooled_pod = Pod {
            metadata: kube::api::ObjectMeta {
                name: Some("space-sbx-pooled-g1".to_string()),
                namespace: Some("space".to_string()),
                uid: Some("pool-pod-uid".to_string()),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::core::v1::PodSpec {
                node_name: pod.node_name.map(str::to_string),
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(pod.phase.to_string()),
                pod_ip: Some("10.90.0.7".to_string()),
                ..Default::default()
            }),
        };
        let nodes = pod
            .nodes
            .iter()
            .map(|(name, group)| (*node_with_node_group(name, &config, *group)).clone())
            .collect();
        let substrate = KubernetesSubstrate {
            store: K8sStore::new(db.pool.clone(), "space".to_string()),
            node_group_placement: NodeGroupPlacementResolver::new(db.pool.clone(), true),
            ..substrate_with_inventory_caches(vec![pooled_pod], nodes)
        };

        Ok(PoolClaimFixture {
            db,
            substrate,
            slot_id: slot.id,
            release_id,
            actor: ActorRef::new(
                user_id,
                org_id,
                pplx_space_common_core::types::Email::parse("pool-claim@example.com")?,
            ),
            env: BTreeMap::new(),
            auto_lifecycle: pplx_space_common_core::types::AutoLifecyclePolicy::disabled(),
            network_policy: pplx_space_net_common::NetworkPolicyV1::default(),
        })
    }

    /// A claimed warm slot the candidate set refuses, driven through the real
    /// `claim_pooled_execution`.
    ///
    /// `pooled_pod_node_admission` has its own unit test, and that test says
    /// nothing about whether the claim still consults it. Deleting the call or
    /// swapping `invalidate` for `release` both leave every other test on this
    /// branch green.
    ///
    /// The properties, in operator terms: a warm pod on a node this sandbox may
    /// not use is never handed to it, and the slot does not sit there poisoning
    /// every subsequent create. The second half is why the refusal invalidates
    /// rather than releases — `release` leaves `ready_at` alone and the claim
    /// orders by `ready_at ASC`, so a released slot is re-picked first, forever.
    #[tokio::test(flavor = "current_thread")]
    async fn claiming_a_warm_slot_consults_the_candidate_set() -> Result<(), BoxError> {
        let fixture = pool_claim_fixture(PooledSlotPod {
            phase: "Running",
            node_name: Some("reassigned-worker"),
            nodes: &[("reassigned-worker", Some("org-b-1"))],
        })
        .await?;

        let attempt = fixture
            .substrate
            .claim_pooled_execution(&fixture.request())
            .await?;

        assert!(
            attempt.claim.is_none(),
            "a slot on a node outside the candidate set must not be adopted"
        );

        // Invalidated, not released: the claim id is stripped so the slot can
        // never satisfy another claim, which is what stops it being re-picked
        // first on every subsequent create.
        let (state, claim_id) = fixture.slot_disposition().await?;
        assert_eq!(
            (state.as_str(), claim_id),
            ("claimed", None),
            "the refused slot must be invalidated so it leaves the pool, not released \
             back into it"
        );
        Ok(())
    }

    /// A sandbox that admits only dedicated capacity reports a *refusal*, not a
    /// bare miss.
    ///
    /// The distinction exists for one caller: `boot_mode=pool`. Every other
    /// boot mode builds cold and resolves the same boundary again, so a miss is
    /// enough. A strict pool create has nowhere to fall through to —
    /// `queries.rs` turns the miss into a 503 "no warm-pool sandbox is
    /// available" — and for a boundary that admits no pooled pod, no retry ever
    /// clears it, so 503 tells the client to loop forever on a request that
    /// cannot succeed. The refusal is what lets the controller answer 409.
    ///
    /// Asserted on the substrate's own return value rather than through the
    /// controller, because that is the layer that decides *whether* a miss is
    /// permanent; the controller decides what a permanent one means for a
    /// given boot mode.
    #[tokio::test(flavor = "current_thread")]
    async fn a_dedicated_only_sandbox_reports_a_refusal_not_a_bare_miss() -> Result<(), BoxError> {
        let fixture = pool_claim_fixture(PooledSlotPod {
            phase: "Running",
            node_name: Some("in-set-worker"),
            nodes: &[("in-set-worker", Some("workers"))],
        })
        .await?;
        // Make this caller's set dedicated-only: give its org a dedicated group
        // that admits the generic template, and remove the generic group so no
        // shared candidate survives.
        sqlx::query(sql!("DELETE FROM node_groups WHERE name = 'workers'"))
            .execute(&fixture.db.pool)
            .await?;
        sqlx::query(sql!(
            "INSERT INTO node_groups (id, name, org_id, template_policy, is_default)
             VALUES ($1, 'org-dedicated', $2, 'default', FALSE)"
        ))
        .bind(Uuid::now_v7())
        .bind(fixture.actor.org_id.as_uuid())
        .execute(&fixture.db.pool)
        .await?;

        let attempt = fixture
            .substrate
            .claim_pooled_execution(&fixture.request())
            .await?;

        assert!(attempt.claim.is_none(), "no pooled pod can satisfy it");
        let Some(crate::controllers::substrate::PooledClaimRefusal::PlacementIncompatible {
            message,
        }) = attempt.refusal.as_ref()
        else {
            panic!(
                "a dedicated-only boundary is a permanent refusal, not a retryable miss: {:?}",
                attempt.refusal
            );
        };
        // This arm's cause is the *cluster's* missing shared group, not the
        // template being org-private — telling this caller its template is
        // private would be false, since `seed` is generic.
        assert!(
            message.contains("no shared node group"),
            "the refusal has to name the real cause, since it becomes the caller's 409: \
             {message}"
        );
        assert!(
            !message.contains("may run only on node groups dedicated"),
            "and must not claim a generic template is org-private: {message}"
        );
        // Released, not invalidated, and this is the whole of the distinction:
        // `seed` is **generic**, so the pool's own org-less resolve reports
        // `unprovisioned` and keeps filling, and a caller owning no dedicated
        // capacity adopts this very slot. Invalidating would destroy a usable
        // pod once per create from this org, forever, with the reconciler
        // rebuilding it just as fast.
        let (state, claim_id) = fixture.slot_disposition().await?;
        assert_eq!(
            (state.as_str(), claim_id),
            ("ready", None),
            "a slot another caller can still adopt must go back into the pool"
        );

        // The control, and it has to run against **this** database and **this**
        // slot to be one: `pg_test_db()` hands out a freshly cloned database per
        // call, so claiming from a second fixture would only prove the vanilla
        // fixture works. Because the refusal above *released* the slot it is
        // still `ready` and claimable, so putting the generic group back changes
        // exactly one row and flips the outcome for the same slot.
        sqlx::query(sql!(
            "INSERT INTO node_groups (id, name, org_id, template_policy, is_default)
             VALUES ($1, 'workers', NULL, 'default', TRUE)"
        ))
        .bind(Uuid::now_v7())
        .execute(&fixture.db.pool)
        .await?;
        let hit = fixture
            .substrate
            .claim_pooled_execution(&fixture.request())
            .await?;
        assert!(
            hit.claim.is_some(),
            "with shared capacity available the same slot is adopted, so the refusal above \
             was the boundary's doing and not the fixture's"
        );
        assert!(hit.refusal.is_none(), "a hit is never a refusal");
        Ok(())
    }

    /// An unlabeled node refuses the adoption but **keeps** the warm pod.
    ///
    /// The distinction this pins is worth its own test because the cost of
    /// getting it wrong is fleet-wide. An unrelated MNG change can strip an
    /// MNG-set `node-restriction.*` label until the labeler repairs it.
    /// Treating that as evidence about the *slot* would invalidate up to three
    /// healthy warm pods per create for the length of the window, draining the
    /// pool in response to a label write and booking it as a group
    /// reassignment that never happened.
    ///
    /// So: refuse (an `In (…)` term excludes an unlabeled node, and isolation
    /// holds because the sandbox builds cold), but release the slot.
    #[tokio::test(flavor = "current_thread")]
    async fn an_unlabeled_node_releases_the_slot_instead_of_draining_the_pool()
    -> Result<(), BoxError> {
        let fixture = pool_claim_fixture(PooledSlotPod {
            phase: "Running",
            node_name: Some("stripped-worker"),
            // In the reflector, and deliberately carrying no group label.
            nodes: &[("stripped-worker", None)],
        })
        .await?;

        let attempt = fixture
            .substrate
            .claim_pooled_execution(&fixture.request())
            .await?;

        assert!(
            attempt.claim.is_none(),
            "an unlabeled node is outside the required term, so the adoption is refused"
        );
        let (state, claim_id) = fixture.slot_disposition().await?;
        assert_eq!(
            (state.as_str(), claim_id),
            ("ready", None),
            "the pod must survive the labeler's repair window; invalidating here drains the \
             pool fleet-wide on a label strip"
        );
        Ok(())
    }

    /// With no generic group provisioned, a slot on a *labeled* node is
    /// refused rather than adopted blind.
    ///
    /// `Unprovisioned` means no generic group admits this template — not that
    /// the cluster has no groups. Nothing orders `create_node_group`, so a
    /// dedicated group can exist without a default generic one, and in that
    /// state there is no candidate set to test a node against. Adopting anyway
    /// would hand this sandbox a pod already running on capacity reserved for
    /// another org, with no check at all. A labeled node is disqualifying by
    /// elimination: the labeler is the only writer, and the only groups that
    /// exist are dedicated ones.
    #[tokio::test(flavor = "current_thread")]
    async fn an_unprovisioned_cluster_refuses_a_slot_on_a_labeled_node() -> Result<(), BoxError> {
        let fixture = pool_claim_fixture(PooledSlotPod {
            phase: "Running",
            node_name: Some("org-a-worker"),
            nodes: &[("org-a-worker", Some("org-a-1"))],
        })
        .await?;
        // No generic group, but a dedicated one exists — the provisioning-order
        // fault the runbook's stage order forbids and the API does not prevent.
        sqlx::query(sql!("DELETE FROM node_groups WHERE name = 'workers'"))
            .execute(&fixture.db.pool)
            .await?;
        let other_org = Uuid::now_v7();
        sqlx::query(sql!(
            "INSERT INTO organizations (id, name, slug) VALUES ($1, 'Org A', $2)"
        ))
        .bind(other_org)
        .bind(format!("org-a-{other_org}"))
        .execute(&fixture.db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO node_groups (id, name, org_id, template_policy, is_default)
             VALUES ($1, 'org-a-1', $2, 'default', FALSE)"
        ))
        .bind(Uuid::now_v7())
        .bind(other_org)
        .execute(&fixture.db.pool)
        .await?;

        let attempt = fixture
            .substrate
            .claim_pooled_execution(&fixture.request())
            .await?;

        assert!(
            attempt.claim.is_none(),
            "a pod on another org's dedicated node must not be adopted just because this \
             sandbox resolved to no boundary"
        );
        let (state, claim_id) = fixture.slot_disposition().await?;
        assert_eq!(
            (state.as_str(), claim_id),
            ("ready", None),
            "the slot is released, not destroyed: the fault is provisioning order, not the pod"
        );
        Ok(())
    }

    /// A cluster with the gate on and **no** node groups still adopts warm
    /// slots.
    ///
    /// This is the state every cluster is in the moment the gate flips ahead of
    /// stage 0, and it is the one an over-eager boundary would break most
    /// visibly: if `Unprovisioned` were treated as "refuse", the flip would
    /// take the warm pool to a zero hit rate fleet-wide while every pod stayed
    /// healthy. The resolution imposes no boundary, so the node is not read at
    /// all and the slot is adopted unconstrained.
    #[tokio::test(flavor = "current_thread")]
    async fn an_unprovisioned_cluster_still_adopts_warm_slots() -> Result<(), BoxError> {
        let fixture = pool_claim_fixture(PooledSlotPod {
            phase: "Running",
            // An **unlabeled** node, which is what a genuinely unprovisioned
            // cluster has: the labeler is the only writer of this label and
            // with no groups to assign it writes nothing. A *labeled* node in
            // this same state means a dedicated group exists without a generic
            // one, and that is refused — see
            // `an_unprovisioned_cluster_refuses_a_slot_on_a_labeled_node`.
            node_name: Some("unprovisioned-worker"),
            nodes: &[("unprovisioned-worker", None)],
        })
        .await?;
        sqlx::query(sql!("DELETE FROM node_groups"))
            .execute(&fixture.db.pool)
            .await?;

        // The guard is held across the await deliberately: capture storage is
        // thread-local and this is a `current_thread` runtime, so the emission
        // inside `claim_pooled_execution` lands in this buffer.
        let capture = pplx_space_metrics::test_capture::capture_counters_guard();
        let attempt = fixture
            .substrate
            .claim_pooled_execution(&fixture.request())
            .await?;
        let counters = capture.finish();

        assert!(
            attempt.claim.is_some(),
            "an unprovisioned cluster has no boundary to enforce, so the pool keeps working"
        );
        assert!(attempt.refusal.is_none(), "an adoption is not a refusal");
        // Adoption must be counted even before a generic group is provisioned:
        // every resolution is `unprovisioned`, so `verdict:admitted` is zero and
        // this verdict is the evidence that the pool is adopting.
        let verdicts: Vec<&str> = counters
            .iter()
            .filter(|counter| counter.name == "node_group_placement.pool_claim_admission")
            .flat_map(|counter| counter.tags.iter().map(String::as_str))
            .collect();
        assert_eq!(
            verdicts,
            vec!["verdict:unconstrained"],
            "an unprovisioned adoption books exactly one verdict, and it is not `admitted`"
        );
        Ok(())
    }

    /// A claim whose candidate set cannot be resolved returns `Ok`, never an
    /// error, so the create goes on to build cold — and the slot is disposed of
    /// according to whether the failure rules out every future caller.
    ///
    /// This is the arm with the sharpest failure mode if it regresses.
    /// `queries.rs` treats an `Err` from `claim_pooled_execution` as terminal —
    /// it books `warm_pool_claim`/`claim_failed` and returns — so returning
    /// `Err` here aborts the create inside the claim step rather than letting
    /// the cold path resolve the same boundary and own the error. Nothing else
    /// on the branch catches that: the user-visible status is the same 409
    /// either way, and only the step attribution and the cold-create fallback
    /// differ.
    ///
    /// The state: an org-specific template whose org has **no** compatible
    /// dedicated group, which is every cluster before the dedicated node
    /// groups exist. The resolve refuses, so the slot is invalidated (its
    /// template is unhostable by any pooled pod) and the attempt carries a
    /// refusal so a strict `boot_mode=pool` create can answer 409 instead of
    /// "no slot available yet".
    #[tokio::test(flavor = "current_thread")]
    async fn an_unresolvable_candidate_set_never_errors_out_of_the_claim_step()
    -> Result<(), BoxError> {
        let fixture = pool_claim_fixture(PooledSlotPod {
            phase: "Running",
            node_name: Some("in-set-worker"),
            nodes: &[("in-set-worker", Some("workers"))],
        })
        .await?;
        // Org-specific template, and no dedicated group anywhere — so the
        // caller's own resolve fails outright rather than producing a
        // dedicated-only set.
        sqlx::query(sql!(
            "UPDATE templates SET org_id = $1, visibility = 'org_specific' WHERE name = 'seed'"
        ))
        .bind(fixture.actor.org_id.as_uuid())
        .execute(&fixture.db.pool)
        .await?;

        // The assertion that matters most is simply that this is `Ok`: a
        // `?`-propagated error here would abort the create.
        let attempt = fixture
            .substrate
            .claim_pooled_execution(&fixture.request())
            .await?;

        assert!(
            attempt.claim.is_none(),
            "an unresolvable set adopts nothing"
        );
        assert!(
            matches!(
                attempt.refusal,
                Some(
                    crate::controllers::substrate::PooledClaimRefusal::PlacementIncompatible { .. }
                )
            ),
            "a standing refusal has to be distinguishable from a retryable miss: {:?}",
            attempt.refusal
        );
        let (state, claim_id) = fixture.slot_disposition().await?;
        assert_eq!(
            (state.as_str(), claim_id),
            ("claimed", None),
            "no pooled pod may host an org-specific template, so the slot leaves the pool \
             rather than being handed to the next create"
        );
        Ok(())
    }

    /// A pool config for a template **no** pooled pod may host drains; one for
    /// a template that merely excludes *this* caller does not.
    ///
    /// This is the other half of
    /// `a_dedicated_only_sandbox_reports_a_refusal_not_a_bare_miss`, and
    /// together they are the only thing keeping the two apart. Both reach the
    /// same `!admits_shared_capacity()` arm with the same dedicated-only set;
    /// what differs is the *template's* visibility, which the caller's own
    /// candidate set does not record — so the claim re-resolves for the pool's
    /// org-less subject to decide, and this test is what proves it uses the
    /// answer.
    ///
    /// Here the template is org-specific: an org-less pod may never host it, so
    /// no caller can adopt any slot under this config, `reconcile_config`'s own
    /// org-less resolve fails and suppresses refill, and the slots hold a
    /// private rootfs on shared workers. Invalidate.
    #[tokio::test(flavor = "current_thread")]
    async fn an_org_specific_pool_config_drains_instead_of_recycling() -> Result<(), BoxError> {
        let fixture = pool_claim_fixture(PooledSlotPod {
            phase: "Running",
            node_name: Some("in-set-worker"),
            nodes: &[("in-set-worker", Some("workers"))],
        })
        .await?;
        // Make `seed` org-specific to the caller's org, and give that org a
        // dedicated group so its own resolve succeeds dedicated-only rather
        // than failing outright — the state that reaches the arm under test.
        sqlx::query(sql!(
            "UPDATE templates SET org_id = $1, visibility = 'org_specific' WHERE name = 'seed'"
        ))
        .bind(fixture.actor.org_id.as_uuid())
        .execute(&fixture.db.pool)
        .await?;
        sqlx::query(sql!("DELETE FROM node_groups WHERE name = 'workers'"))
            .execute(&fixture.db.pool)
            .await?;
        sqlx::query(sql!(
            "INSERT INTO node_groups (id, name, org_id, template_policy, is_default)
             VALUES ($1, 'org-dedicated', $2, 'default', FALSE)"
        ))
        .bind(Uuid::now_v7())
        .bind(fixture.actor.org_id.as_uuid())
        .execute(&fixture.db.pool)
        .await?;

        let attempt = fixture
            .substrate
            .claim_pooled_execution(&fixture.request())
            .await?;

        assert!(attempt.claim.is_none(), "no pooled pod may host it");
        let (state, claim_id) = fixture.slot_disposition().await?;
        assert_eq!(
            (state.as_str(), claim_id),
            ("claimed", None),
            "a slot no caller can ever adopt must leave the pool so the config drains"
        );
        Ok(())
    }

    /// Usability is decided **before** the candidate set, and the order is the
    /// whole point.
    ///
    /// The two refusals give the slot up in opposite ways: an unusable pod is
    /// invalidated, because the pod is gone and the slot is worthless; an
    /// unreadable node is released, because the slot is probably fine and the
    /// next request may be able to read it. Swap the order and a claim on a
    /// dead pod whose node cannot be read takes the *release* path — the slot
    /// goes back into the pool, keeps its `ready_at`, and is re-picked first by
    /// every subsequent create, none of which can use it either.
    ///
    /// The fixture makes both conditions true at once, which is the only way
    /// the order is observable: the pod has finished (so it is unusable) and
    /// names a node no reflector holds (so its group is unreadable). Neither
    /// alone distinguishes the two orders. Moving the usability check into the
    /// `Admitted` arm — judge the node, then the pod — leaves the slot
    /// `("ready", None)` here instead of `("claimed", None)`.
    ///
    /// What this does *not* pin, deliberately: where the **resolve** sits.
    /// Hoisting it above the usability check keeps this test green, and
    /// correctly so — an unusable pod would then pay one catalog read before
    /// being invalidated, which is waste, not a defect. The order that matters
    /// is usability before *admission*.
    #[tokio::test(flavor = "current_thread")]
    async fn an_unusable_warm_pod_is_judged_before_its_node() -> Result<(), BoxError> {
        let fixture = pool_claim_fixture(PooledSlotPod {
            phase: "Succeeded",
            node_name: Some("vanished-worker"),
            nodes: &[("in-set-worker", Some("workers"))],
        })
        .await?;

        let attempt = fixture
            .substrate
            .claim_pooled_execution(&fixture.request())
            .await?;

        assert!(
            attempt.claim.is_none(),
            "a claimed pod that is no longer running must not be adopted"
        );
        let (state, claim_id) = fixture.slot_disposition().await?;
        assert_eq!(
            (state.as_str(), claim_id),
            ("claimed", None),
            "the stale claim must be invalidated, not released — an unreadable node is \
             not a reason to hand a dead pod's slot back to the pool"
        );
        Ok(())
    }

    /// A warm pod already running on a node the claiming sandbox's boundary
    /// excludes must not be adopted.
    ///
    /// The scenario this exists for: a group is generic when the slot is
    /// filled, and dedicated by the time it is claimed. Reassignment is allowed
    /// with ready slots resident — group occupancy counts `sandboxes` rows and
    /// a warm slot has none until it is claimed — and the dedicated
    /// `NoSchedule` taint does not evict a pod that is already running, so the
    /// warm pod stays put on what is now another org's hardware. A
    /// template-level check cannot see it; only the node's own current label
    /// can.
    ///
    /// Every uncertainty is a refusal, because the cost of one is a cold create
    /// and the cost of the other is a tenant's sandbox on reserved hardware.
    #[tokio::test(flavor = "current_thread")]
    async fn a_warm_pod_outside_the_candidate_set_is_not_adopted() {
        let config = SandboxPodConfigValues::default();
        let in_set = node_with_node_group("in-set-worker", &config, Some("workers"));
        let reassigned = node_with_node_group("reassigned-worker", &config, Some("org-b-1"));
        let unlabeled = node_with_node_group("unlabeled-worker", &config, None);
        let substrate =
            substrate_with_node_cache_and_config(&[in_set, reassigned, unlabeled], true, config);
        let template = TemplateCompatSpec {
            name: TemplateName::parse("seed").expect("valid template name"),
            org_id: None,
            visibility: TemplateVisibility::Generic,
        };
        let placement = NodeGroupPlacement::for_caller(
            Some(OrgId::new()),
            &template,
            &[NodeGroup {
                id: NodeGroupId::new(),
                name: NodeGroupName::parse("workers").expect("valid group name"),
                policy: NodeGroupPolicy::Generic { is_default: true },
                template_allowlist: std::collections::BTreeSet::new(),
            }],
        )
        .expect("a generic group admits a generic template");
        let pod_on = |node: Option<&str>| Pod {
            spec: Some(k8s_openapi::api::core::v1::PodSpec {
                node_name: node.map(str::to_string),
                ..Default::default()
            }),
            ..Default::default()
        };

        // The control first: an in-set node is adopted, so every refusal below
        // is the check's doing and not a fixture that can only say no.
        assert_eq!(
            pooled_pod_node_admission(
                &substrate.k8s.caches,
                &placement,
                Some(&pod_on(Some("in-set-worker"))),
            ),
            PooledPodAdmission::Admitted,
            "a pod on an in-set node is adoptable"
        );

        // `OutsideSet` and `Unknown` are not interchangeable: the first is a
        // property of the node, and the slot leaves the pool; the second is a
        // property of our reading, and the slot stays. Distinguishing them is
        // what keeps a cold reflector from draining the pool and a reassigned
        // group from tarpitting it.
        for (node, want, why) in [
            (
                Some("reassigned-worker"),
                PooledPodAdmission::OutsideSet,
                "its group is now another org's",
            ),
            (
                // Refused just the same — an `In (…)` term does not match a
                // node with no label — but a *separate* verdict, because the
                // remedy differs: EKS strips this label from live nodes on an
                // unrelated MNG change, and destroying warm pods for the length
                // of the labeler's repair window would drain the pool
                // fleet-wide in response to a label write.
                Some("unlabeled-worker"),
                PooledPodAdmission::LabelAbsent,
                "an unlabeled node is refused, but as the labeler's problem rather than the                  node's",
            ),
            (
                Some("vanished-worker"),
                PooledPodAdmission::Unknown,
                "the reflector cannot resolve it, which says nothing about the node",
            ),
            (
                None,
                PooledPodAdmission::Unknown,
                "the pod is not scheduled, so there is no node to judge",
            ),
        ] {
            assert_eq!(
                pooled_pod_node_admission(&substrate.k8s.caches, &placement, Some(&pod_on(node))),
                want,
                "{why}"
            );
        }
        assert_eq!(
            pooled_pod_node_admission(&substrate.k8s.caches, &placement, None),
            PooledPodAdmission::Unknown,
            "no pod at all is unreadable, not evidence against the slot"
        );

        // A store that holds the node is read for what it holds, even with the
        // readiness flag still unset. That pair is a real state, though a
        // narrow one: `reflector::Store` swaps the whole initial LIST in at
        // once, so this models the window between that swap and the readiness
        // task marking the cache ready — not a half-loaded cache, which does
        // not exist. What a cold store cannot do is turn a *miss* into
        // evidence: the node may simply not be loaded yet, so the slot
        // survives the window instead of being destroyed by it.
        let cold = substrate_with_node_cache_and_config(
            &[node_with_node_group(
                "in-set-worker",
                &SandboxPodConfigValues::default(),
                Some("workers"),
            )],
            false,
            SandboxPodConfigValues::default(),
        );
        assert_eq!(
            pooled_pod_node_admission(
                &cold.k8s.caches,
                &placement,
                Some(&pod_on(Some("in-set-worker"))),
            ),
            PooledPodAdmission::Admitted,
            "a cold store's own entry is evidence: an in-set node is adoptable"
        );
        assert_eq!(
            pooled_pod_node_admission(
                &cold.k8s.caches,
                &placement,
                Some(&pod_on(Some("not-loaded-yet-worker"))),
            ),
            PooledPodAdmission::Unknown,
            "a miss against a cold store must be Unknown — never OutsideSet, or a \
             filling cache would drain the pool"
        );
    }

    #[test]
    fn pooled_claim_pod_is_usable_requires_present_matching_pod() {
        let claim = PooledExecutionClaim {
            claim_id: Uuid::now_v7(),
            pool_slot_id: Some(Uuid::now_v7()),
            sandbox_id: SandboxId::new(),
            content_hash: "sha256:pooled".to_string(),
            pod_name: "space-sbx-pooled-g1".to_string(),
            pod_uid: Some(PodUid::parse("pod-uid").unwrap()),
            execution_generation: 1,
            pod_ip: "10.90.0.11".to_string(),
            vcpu: Some(4),
            memory_mib: Some(2048),
            disk_mib: None,
            disk_io_limits: Default::default(),
            active_node_id: None,
            route_identity: None,
        };
        let mut pod = Pod {
            metadata: kube::api::ObjectMeta {
                uid: Some("pod-uid".to_string()),
                ..Default::default()
            },
            status: Some(PodStatus {
                phase: Some("Running".to_string()),
                pod_ip: Some("10.90.0.11".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(pooled_claim_pod_is_usable(&claim, Some(&pod)));
        assert!(!pooled_claim_pod_is_usable(&claim, None));

        pod.metadata.uid = Some("replacement-pod-uid".to_string());
        assert!(!pooled_claim_pod_is_usable(&claim, Some(&pod)));
    }

    #[test]
    fn pooled_adoption_request_persists_cluster_route_without_resuming() {
        let claim_id = Uuid::now_v7();
        let pool_slot_id = Uuid::now_v7();
        let sandbox_id = SandboxId::new();
        let active_node_id = NodeId::new();
        let claim = PooledExecutionClaim {
            claim_id,
            pool_slot_id: Some(pool_slot_id),
            sandbox_id,
            content_hash: "sha256:pooled".to_string(),
            pod_name: "space-sbx-pooled-g1".to_string(),
            pod_uid: Some(PodUid::parse("pod-uid").unwrap()),
            execution_generation: 1,
            pod_ip: "10.90.0.11".to_string(),
            vcpu: Some(4),
            memory_mib: Some(2048),
            disk_mib: None,
            disk_io_limits: Default::default(),
            active_node_id: Some(active_node_id),
            route_identity: None,
        };

        let request = super::adapter::pooled_route_identity_adoption_request(&claim).unwrap();
        let identity = request
            .start_context
            .expect("route adoption needs start context")
            .route_identity
            .expect("route adoption needs route identity");

        assert!(request.blocking);
        assert_eq!(
            request.target_state,
            pplx_space_sbx_mgr_client::proto::WakeTargetState::Paused as i32
        );
        assert_eq!(identity.route_generation, Some(1));
        assert_eq!(
            identity.current_node_id,
            Some(active_node_id.to_string()),
            "the unstripped claim identity is retained for acknowledgement validation"
        );
        assert_eq!(identity.execution_generation, Some(1));
        assert_eq!(identity.pod_uid.as_deref(), Some("pod-uid"));
        assert_eq!(
            identity.activation_attempt_id,
            Some(pool_slot_id.to_string())
        );
        assert_ne!(identity.activation_attempt_id, Some(claim_id.to_string()));
        assert_eq!(
            identity.substrate_kind,
            pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::Kubernetes as i32
        );
    }

    fn kubernetes_client_returning_live_pod(pod: Pod) -> Client {
        let pod = Arc::new(pod);
        let service = tower::service_fn(move |req: Request<Body>| {
            let pod = Arc::clone(&pod);
            async move {
                let path = req.uri().path();
                if req.method() == Method::GET && path.starts_with("/api/v1/namespaces/space/pods/")
                {
                    return Ok::<_, BoxError>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .header("content-type", "application/json")
                            .body(Body::from(serde_json::to_vec(pod.as_ref())?))?,
                    );
                }
                let body = serde_json::json!({
                    "kind": "Status",
                    "apiVersion": "v1",
                    "status": "Failure",
                    "reason": "BadRequest",
                    "code": 400
                });
                Ok::<_, BoxError>(
                    Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&body)?))?,
                )
            }
        });
        ClientBuilder::new(service, "space").build()
    }

    #[derive(Clone, Copy)]
    enum SuspendEvidenceCase {
        Current,
        OtherArtifact,
        FailedBackup,
        CompletedBackup,
        ReplacementPod,
        MismatchedPublishedGeneration,
        OldPublishedUid,
        OtherNode,
        MissingPublication,
        NewerActivity,
        ReplacedExecution,
        ReplacedRecordedUid,
        ClearedByRunning,
        MismatchedPublisherCannotClear,
    }

    #[rstest::rstest]
    #[case::current(SuspendEvidenceCase::Current)]
    #[case::other_artifact(SuspendEvidenceCase::OtherArtifact)]
    #[case::failed_backup(SuspendEvidenceCase::FailedBackup)]
    #[case::completed_backup(SuspendEvidenceCase::CompletedBackup)]
    #[case::replacement_pod(SuspendEvidenceCase::ReplacementPod)]
    #[case::mismatched_published_generation(SuspendEvidenceCase::MismatchedPublishedGeneration)]
    #[case::old_published_uid(SuspendEvidenceCase::OldPublishedUid)]
    #[case::other_node(SuspendEvidenceCase::OtherNode)]
    #[case::missing_publication(SuspendEvidenceCase::MissingPublication)]
    #[case::newer_activity(SuspendEvidenceCase::NewerActivity)]
    #[case::replaced_execution(SuspendEvidenceCase::ReplacedExecution)]
    #[case::replaced_recorded_uid(SuspendEvidenceCase::ReplacedRecordedUid)]
    #[case::cleared_by_running(SuspendEvidenceCase::ClearedByRunning)]
    #[case::mismatched_publisher_cannot_clear(SuspendEvidenceCase::MismatchedPublisherCannotClear)]
    #[tokio::test(flavor = "current_thread")]
    async fn terminal_selected_pod_requires_current_suspend_evidence(
        #[case] case: SuspendEvidenceCase,
    ) -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let sandbox_id = SandboxId::new();
        let artifact_id = Uuid::now_v7();
        let snapshot_id = SnapshotId::new();
        let node_id = NodeId::new();
        let route = SandboxActivationAttemptId::new();
        insert_test_sandbox_row(&db.pool, sandbox_id, "started").await?;
        pplx_space_db_test_fixtures::insert_ready_worker_node(&db.pool, node_id).await?;
        sqlx::query(sql!(
            "UPDATE sandboxes SET current_local_artifact_id = $2,
                 current_local_artifact_kind = 'full', current_local_artifact_origin = 'suspend',
                 active_node_id = $3, target_node_id = $3 WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(artifact_id)
        .bind(node_id)
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp, org_id,
                 local_artifact_id, artifact_kind, artifact_origin, source_node_id)
             VALUES ($1, $2, 'full', 'creating', NOW(),
                 '00000000-0000-0000-0000-000000000001', $3, 'full', 'suspend', $4)"
        ))
        .bind(snapshot_id)
        .bind(sandbox_id)
        .bind(artifact_id)
        .bind(node_id)
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions (sandbox_id, generation, pod_name, pod_namespace,
                 pod_uid, pod_ip, state, active, activation_attempt_id, route_activation_id)
             VALUES ($1, 1, 'selected-suspend-pod', 'space', 'selected-pod-uid',
                 '127.0.0.1', 'recovering', TRUE, $2, $2)"
        ))
        .bind(sandbox_id)
        .bind(route.as_uuid())
        .execute(&db.pool)
        .await?;
        // The observation must merge with other data owned by this facet.
        sqlx::query(sql!(
            "INSERT INTO node_facet_observed_status (node_id, facet, element, observed_payload, observed_at)
             VALUES ($1, $2, $3, '{\"preserve\":true}'::JSONB, NOW())"
        ))
        .bind(node_id)
        .bind(pplx_space_common_svc::types::FACET_SANDBOX_DESIRED_STATE)
        .bind(sandbox_id.to_string())
        .execute(&db.pool)
        .await?;
        let mut published =
            local_suspend_publication(sandbox_id, artifact_id, 1, "selected-pod-uid");
        let mut publishing_node = node_id;
        match case {
            SuspendEvidenceCase::OtherArtifact => {
                published.current_local_artifact_id = Some(Uuid::now_v7().to_string())
            }
            SuspendEvidenceCase::MismatchedPublishedGeneration => {
                published.execution_generation = Some(2)
            }
            SuspendEvidenceCase::OldPublishedUid => {
                published.pod_uid = Some("old-pod-uid".to_string())
            }
            SuspendEvidenceCase::OtherNode => {
                publishing_node = NodeId::new();
                pplx_space_db_test_fixtures::insert_ready_worker_node(&db.pool, publishing_node)
                    .await?;
            }
            _ => {}
        }
        if !matches!(case, SuspendEvidenceCase::MissingPublication) {
            publish_sandbox_states(
                &mut *db.pool.acquire().await?,
                publishing_node,
                std::slice::from_ref(&published),
            )
            .await?;
        }
        match case {
            SuspendEvidenceCase::FailedBackup | SuspendEvidenceCase::CompletedBackup => {
                sqlx::query(sql!("UPDATE snapshots SET state = $2 WHERE id = $1"))
                    .bind(snapshot_id)
                    .bind(if matches!(case, SuspendEvidenceCase::FailedBackup) {
                        "failed"
                    } else {
                        "ready"
                    })
                    .execute(&db.pool)
                    .await?;
            }
            SuspendEvidenceCase::NewerActivity => {
                sqlx::query(sql!("UPDATE sandboxes SET last_command_at = NOW() + INTERVAL '1 second' WHERE id = $1"))
                    .bind(sandbox_id).execute(&db.pool).await?;
            }
            SuspendEvidenceCase::ReplacedExecution => {
                sqlx::query(sql!(
                    "UPDATE sandbox_k8s_executions SET generation = 2 WHERE sandbox_id = $1"
                ))
                .bind(sandbox_id)
                .execute(&db.pool)
                .await?;
            }
            SuspendEvidenceCase::ReplacedRecordedUid => {
                sqlx::query(sql!("UPDATE sandbox_k8s_executions SET pod_uid = 'replacement-pod-uid' WHERE sandbox_id = $1"))
                    .bind(sandbox_id).execute(&db.pool).await?;
            }
            SuspendEvidenceCase::ClearedByRunning
            | SuspendEvidenceCase::MismatchedPublisherCannotClear => {
                published.status = Some("running".to_string());
                if matches!(case, SuspendEvidenceCase::MismatchedPublisherCannotClear) {
                    published.execution_generation = Some(2);
                }
                publish_sandbox_states(&mut *db.pool.acquire().await?, node_id, &[published])
                    .await?;
            }
            _ => {}
        }
        let live_uid = if matches!(
            case,
            SuspendEvidenceCase::ReplacementPod | SuspendEvidenceCase::ReplacedRecordedUid
        ) {
            "replacement-pod-uid"
        } else {
            "selected-pod-uid"
        };
        let substrate = substrate_with_store_and_client(
            db.pool.clone(),
            kubernetes_client_returning_live_pod(Pod {
                metadata: kube::api::ObjectMeta {
                    name: Some("selected-suspend-pod".to_string()),
                    namespace: Some("space".to_string()),
                    uid: Some(live_uid.to_string()),
                    ..Default::default()
                },
                status: Some(PodStatus {
                    phase: Some("Succeeded".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        );
        let execution = substrate
            .store
            .active_execution_status(sandbox_id)
            .await?
            .unwrap();
        let expected = k8s_execution_route_identity(&execution)?;
        let result = substrate.selected_wake_pod_ip(sandbox_id, &expected).await;
        if matches!(
            case,
            SuspendEvidenceCase::Current | SuspendEvidenceCase::MismatchedPublisherCannotClear
        ) {
            assert!(
                matches!(result, Err(SubstrateError::SuspendInFlight { .. })),
                "{result:?}"
            );
        } else {
            assert!(
                matches!(result, Err(SubstrateError::Unavailable { .. })),
                "{result:?}"
            );
        }
        let after = substrate
            .store
            .active_execution_status(sandbox_id)
            .await?
            .unwrap();
        assert_eq!(after.route_activation_id, execution.route_activation_id);
        assert_eq!(after.pod_uid, execution.pod_uid);
        assert_eq!(after.pod_ip, execution.pod_ip);
        let payload: serde_json::Value = sqlx::query_scalar(sql!(
            "SELECT observed_payload FROM node_facet_observed_status WHERE node_id = $1 AND facet = $2 AND element = $3"
        ))
        .bind(node_id).bind(pplx_space_common_svc::types::FACET_SANDBOX_DESIRED_STATE)
        .bind(sandbox_id.to_string()).fetch_one(&db.pool).await?;
        assert_eq!(
            payload.get("preserve"),
            Some(&serde_json::Value::Bool(true))
        );
        if matches!(case, SuspendEvidenceCase::Current) {
            let sandbox = fetch_sandbox(&db.pool, sandbox_id)
                .await?
                .expect("seeded sandbox remains visible");
            let error = substrate
                .ensure_pod_exists(
                    &sandbox,
                    None,
                    None,
                    None,
                    PodMaterializationOwnership::new(
                        Some(StartedRecoveryOwnership::ExistingExecution { generation: 1 }),
                        None,
                        None,
                    ),
                )
                .await
                .expect_err("pre-finalization recovery must preserve generation one");
            assert!(matches!(error, SubstrateError::SuspendInFlight { .. }));
            let active_generation: i64 = sqlx::query_scalar(sql!(
                "SELECT generation
                   FROM sandbox_k8s_executions
                  WHERE sandbox_id = $1 AND active"
            ))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
            assert_eq!(active_generation, 1);
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn selected_wake_uses_authoritative_pod_uid_and_ip() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let route = SandboxActivationAttemptId::new();
        let operation = SandboxActivationAttemptId::new_after(route).unwrap();
        let pod_name = "space-sbx-selected-wake-g7";
        insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip,
                  state, active, activation_attempt_id, route_activation_id)
             VALUES ($1, 7, $2, 'space', 'selected-pod-uid', '10.90.0.7',
                     'running', TRUE, $3, $4)",
        )
        .bind(sandbox_id)
        .bind(pod_name)
        .bind(operation.as_uuid())
        .bind(route.as_uuid())
        .execute(&pool)
        .await?;

        let live_pod = Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(pod_name.to_string()),
                namespace: Some("space".to_string()),
                uid: Some("selected-pod-uid".to_string()),
                ..Default::default()
            },
            status: Some(PodStatus {
                phase: Some("Running".to_string()),
                pod_ip: Some("10.90.0.8".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            kubernetes_client_returning_live_pod(live_pod.clone()),
        );
        let execution = substrate
            .store
            .active_execution_status(sandbox_id)
            .await?
            .expect("seeded execution");
        let expected = k8s_execution_route_identity(&execution)?;

        let selected = substrate
            .selected_wake_pod_ip(sandbox_id, &expected)
            .await?;
        assert_eq!(
            selected,
            Some(PodIp::parse("10.90.0.8")?),
            "Wake must ignore the cached database IP and select the live Pod IP"
        );
        let persisted_ip: String = sqlx::query_scalar(
            "SELECT HOST(pod_ip)::TEXT
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(persisted_ip, "10.90.0.8");

        let mut replacement_pod = live_pod;
        replacement_pod.metadata.uid = Some("replacement-pod-uid".to_string());
        let replacement_substrate = substrate_with_store_and_client(
            pool.clone(),
            kubernetes_client_returning_live_pod(replacement_pod),
        );
        assert!(matches!(
            replacement_substrate
                .selected_wake_pod_ip(sandbox_id, &expected)
                .await,
            Err(SubstrateError::Unavailable { message }) if message.contains("UID changed")
        ));
        let preserved_ip: String = sqlx::query_scalar(
            "SELECT HOST(pod_ip)::TEXT
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(preserved_ip, "10.90.0.8");

        let selected_pod_ip = selected.expect("selected live pod");
        let replacement_route = SandboxActivationAttemptId::new_after(operation).unwrap();
        sqlx::query(
            "UPDATE sandbox_k8s_executions
                SET route_activation_id = $2
              WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .bind(replacement_route.as_uuid())
        .execute(&pool)
        .await?;
        assert!(matches!(
            substrate
                .selected_wake_binding(
                    sandbox_id,
                    &expected,
                    selected_pod_ip,
                )
                .await,
            Err(SubstrateError::Unavailable { message })
                if message.contains("changed before readiness publication")
        ));

        Ok(())
    }

    #[test]
    fn wake_action_preserves_fence_intent_and_separates_split_receipts() {
        let route = SandboxActivationAttemptId::new();
        let operation = SandboxActivationAttemptId::new_after(route).unwrap();
        for (lifecycle, state, target, selected, expected_policy, split_receipt) in [
            (
                "started",
                ActiveExecutionState::Running,
                SandboxLifecycle::Started,
                Some(route),
                WakeWaitPolicy::ResidentProbe,
                true,
            ),
            (
                "paused",
                ActiveExecutionState::Running,
                SandboxLifecycle::Started,
                Some(route),
                WakeWaitPolicy::RequireLifecycle,
                true,
            ),
            (
                "stopped",
                ActiveExecutionState::Running,
                SandboxLifecycle::Started,
                Some(route),
                WakeWaitPolicy::RequireLifecycle,
                true,
            ),
            (
                "created",
                ActiveExecutionState::Running,
                SandboxLifecycle::Started,
                Some(route),
                WakeWaitPolicy::RequireLifecycle,
                true,
            ),
            (
                "restoring",
                ActiveExecutionState::Running,
                SandboxLifecycle::Started,
                Some(route),
                WakeWaitPolicy::RequireLifecycle,
                true,
            ),
            (
                "started",
                ActiveExecutionState::Recovering,
                SandboxLifecycle::Started,
                Some(route),
                WakeWaitPolicy::RequireLifecycle,
                true,
            ),
            (
                "started",
                ActiveExecutionState::Creating,
                SandboxLifecycle::Started,
                Some(route),
                WakeWaitPolicy::RequireLifecycle,
                true,
            ),
            (
                "started",
                ActiveExecutionState::Running,
                SandboxLifecycle::Paused,
                Some(route),
                WakeWaitPolicy::RequireLifecycle,
                true,
            ),
            (
                "started",
                ActiveExecutionState::Running,
                SandboxLifecycle::Started,
                Some(operation),
                WakeWaitPolicy::RequireIdentity,
                false,
            ),
            (
                "started",
                ActiveExecutionState::Running,
                SandboxLifecycle::Started,
                None,
                WakeWaitPolicy::RequireIdentity,
                false,
            ),
        ] {
            let execution = super::store::ActiveExecutionStatusRow {
                generation: 1,
                pod_name: "resident-g1".to_string(),
                pod_uid: Some(PodUid::parse("resident-uid").unwrap()),
                pod_ip: Some("127.0.0.1".to_string()),
                k8s_node_name: None,
                current_node_id: None,
                state,
                lifecycle: lifecycle.to_string(),
                create_age_expired: false,
                activation_attempt_id: Some(operation),
                route_activation_id: selected,
            };
            let action = activation_wake_action(
                &execution,
                ActivationFenceOwnership::Owned(operation),
                target,
            )
            .expect("active operation has a Wake action");
            let ActivationWakeAction::Execute {
                policy,
                require_split_receipt_capability,
                publish_legacy_route_after_ack,
                ..
            } = action
            else {
                panic!("owned operation must execute Wake")
            };
            assert_eq!(policy, expected_policy);
            assert_eq!(require_split_receipt_capability, split_receipt);
            assert_eq!(
                publish_legacy_route_after_ack,
                selected.is_none().then_some(operation)
            );
        }

        let completed = |route_activation_id| super::store::ActiveExecutionStatusRow {
            generation: 1,
            pod_name: "resident-g1".to_string(),
            pod_uid: Some(PodUid::parse("resident-uid").unwrap()),
            pod_ip: Some("127.0.0.1".to_string()),
            k8s_node_name: None,
            current_node_id: None,
            state: ActiveExecutionState::Running,
            lifecycle: "started".to_string(),
            create_age_expired: false,
            activation_attempt_id: None,
            route_activation_id,
        };
        assert!(matches!(
            activation_wake_action(
                &completed(None),
                ActivationFenceOwnership::Completed,
                SandboxLifecycle::Started,
            )
            .unwrap(),
            ActivationWakeAction::SkipCompleted {
                route_identity: None
            }
        ));
        assert!(matches!(
            activation_wake_action(
                &completed(Some(route)),
                ActivationFenceOwnership::Completed,
                SandboxLifecycle::Started,
            )
            .unwrap(),
            ActivationWakeAction::Execute {
                policy: WakeWaitPolicy::ResidentProbe,
                require_split_receipt_capability: false,
                ..
            }
        ));
        assert!(matches!(
            activation_wake_action(
                &completed(Some(route)),
                ActivationFenceOwnership::Completed,
                SandboxLifecycle::Paused,
            )
            .unwrap(),
            ActivationWakeAction::SkipCompleted {
                route_identity: Some(_)
            }
        ));
    }

    #[test]
    fn wake_ack_requires_the_complete_expected_route_identity() {
        let sandbox_id = SandboxId::new().to_string();
        let expected = pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity {
            sandbox_id: None,
            route_generation: Some(7),
            current_node_id: Some(Uuid::now_v7().to_string()),
            substrate_kind: pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::Kubernetes
                as i32,
            execution_generation: Some(7),
            activation_attempt_id: Some(Uuid::now_v7().to_string()),
            pod_uid: Some("pod-uid".to_string()),
        };

        let matching = pplx_space_sbx_mgr_client::proto::WakeResponse {
            sandbox_id: sandbox_id.clone(),
            route_identity: Some(expected.clone()),
            ..Default::default()
        };
        validate_wake_route_identity(&matching, &sandbox_id, &expected)
            .expect("matching identity acknowledgement");

        let mut canonical_equivalent = matching.clone();
        canonical_equivalent.sandbox_id.make_ascii_uppercase();
        let canonical_identity = canonical_equivalent
            .route_identity
            .as_mut()
            .expect("test identity");
        canonical_identity
            .current_node_id
            .as_mut()
            .expect("test node id")
            .make_ascii_uppercase();
        canonical_identity
            .activation_attempt_id
            .as_mut()
            .expect("test activation id")
            .make_ascii_uppercase();
        validate_wake_route_identity(&canonical_equivalent, &sandbox_id, &expected)
            .expect("UUID formatting does not change the canonical identity");

        let mut legacy_missing_node = matching.clone();
        legacy_missing_node
            .route_identity
            .as_mut()
            .expect("test identity")
            .current_node_id = None;
        assert!(
            validate_wake_route_identity_if_present(
                &legacy_missing_node,
                &sandbox_id,
                &expected,
                false,
            )
            .expect("optional legacy acknowledgement may omit only its node identity")
        );
        assert!(matches!(
            validate_wake_route_identity_if_present(
                &legacy_missing_node,
                &sandbox_id,
                &expected,
                true,
            ),
            Err(SubstrateError::Unavailable { .. })
        ));

        let mut runtime_authoritative_node = matching.clone();
        runtime_authoritative_node
            .route_identity
            .as_mut()
            .expect("test identity")
            .current_node_id = Some(Uuid::now_v7().to_string());
        assert!(matches!(
            validate_wake_route_identity(&runtime_authoritative_node, &sandbox_id, &expected),
            Err(SubstrateError::Unavailable { .. })
        ));
        validate_wake_route_identity_with_node_requirement(
            &runtime_authoritative_node,
            &sandbox_id,
            &expected,
            false,
        )
        .expect_err("a present wrong node is never a mixed-version compatibility case");

        let mut centrally_unresolved_node = expected.clone();
        centrally_unresolved_node.current_node_id = None;
        validate_wake_route_identity(
            &runtime_authoritative_node,
            &sandbox_id,
            &centrally_unresolved_node,
        )
        .expect("sandbox-manager may fill a centrally unresolved current node");

        let missing = pplx_space_sbx_mgr_client::proto::WakeResponse::default();
        assert!(matches!(
            validate_wake_route_identity(&missing, &sandbox_id, &expected),
            Err(SubstrateError::Unavailable { .. })
        ));
        assert!(
            !validate_wake_route_identity_if_present(&missing, &sandbox_id, &expected, false)
                .expect("unchanged resident route does not require new identity evidence")
        );
        assert!(matches!(
            validate_wake_route_identity_if_present(&missing, &sandbox_id, &expected, true),
            Err(SubstrateError::Unavailable { .. })
        ));

        for dimension in [
            "sandbox_id",
            "route_identity.sandbox_id",
            "route_generation",
            "current_node_id",
            "substrate_kind",
            "execution_generation",
            "activation_attempt_id",
            "pod_uid",
        ] {
            let mut actual = expected.clone();
            let mut actual_sandbox_id = sandbox_id.clone();
            match dimension {
                "sandbox_id" => actual_sandbox_id = Uuid::now_v7().to_string(),
                "route_identity.sandbox_id" => actual.sandbox_id = Some(Uuid::now_v7().to_string()),
                "route_generation" => actual.route_generation = Some(8),
                "current_node_id" => actual.current_node_id = Some(Uuid::now_v7().to_string()),
                "substrate_kind" => {
                    actual.substrate_kind =
                        pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::Direct as i32
                }
                "execution_generation" => actual.execution_generation = Some(8),
                "activation_attempt_id" => {
                    actual.activation_attempt_id = Some(Uuid::now_v7().to_string())
                }
                "pod_uid" => actual.pod_uid = Some("different-pod-uid".to_string()),
                _ => unreachable!(),
            }
            let response = pplx_space_sbx_mgr_client::proto::WakeResponse {
                sandbox_id: actual_sandbox_id,
                route_identity: Some(actual),
                ..Default::default()
            };
            let error = validate_wake_route_identity(&response, &sandbox_id, &expected)
                .expect_err("a mismatched route dimension must fail closed");
            assert!(
                matches!(&error, SubstrateError::Unavailable { message } if message.contains(dimension)),
                "unexpected error for {dimension}: {error:?}"
            );
            assert!(
                validate_wake_route_identity_if_present(&response, &sandbox_id, &expected, false,)
                    .is_err(),
                "optional evidence must still fail closed when {dimension} mismatches"
            );
        }

        for current_node_id in [None, Some("not-a-node-id".to_string())] {
            let mut actual = expected.clone();
            let missing_current_node_id = current_node_id.is_none();
            actual.current_node_id = current_node_id;
            let response = pplx_space_sbx_mgr_client::proto::WakeResponse {
                sandbox_id: sandbox_id.clone(),
                route_identity: Some(actual),
                ..Default::default()
            };
            if missing_current_node_id {
                validate_wake_route_identity_with_node_requirement(
                    &response,
                    &sandbox_id,
                    &expected,
                    false,
                )
                .expect("the prerequisite may accept only an old worker's absent node");
            }
            let error = validate_wake_route_identity(&response, &sandbox_id, &expected)
                .expect_err("the authoritative node must be present and canonical");
            assert!(
                matches!(&error, SubstrateError::Unavailable { message } if message.contains("current_node_id")),
                "unexpected current-node error: {error:?}"
            );
        }

        assert!(!WakeWaitPolicy::ResidentProbe.requires_route_identity());
        assert!(WakeWaitPolicy::RequireLifecycle.requires_route_identity());
        assert!(WakeWaitPolicy::RequireIdentity.requires_route_identity());
    }

    #[test]
    fn route_publication_classification_uses_materialization_evidence() {
        let row = |state, pod_materialized, route_initialized: bool| SandboxExecutionRow {
            generation: 1,
            pod_name: "space-sbx-test-g1".to_string(),
            pod_materialized,
            state,
            create_age_expired: false,
            activation_attempt_id: None,
            route_activation_id: route_initialized.then(SandboxActivationAttemptId::new),
        };

        for (name, execution, created, expected) in [
            (
                "new create",
                row(ActiveExecutionState::Creating, false, false),
                true,
                ActivationRoutePublication::InitializeFirst,
            ),
            (
                "preclaimed recovery",
                row(ActiveExecutionState::Recovering, false, false),
                false,
                ActivationRoutePublication::InitializeFirst,
            ),
            (
                "resident running route",
                row(ActiveExecutionState::Running, true, true),
                false,
                ActivationRoutePublication::PreserveExisting,
            ),
            (
                "existing transition route",
                row(ActiveExecutionState::Recovering, true, true),
                false,
                ActivationRoutePublication::PreserveExisting,
            ),
        ] {
            assert_eq!(
                classify_activation_route_publication(&execution, created)
                    .expect("classified route publication"),
                expected,
                "unexpected classification for {name}"
            );
        }

        let route_less_resident = row(ActiveExecutionState::Running, true, false);
        assert_eq!(
            classify_activation_route_publication(&route_less_resident, false)
                .expect("legacy resident needs acknowledged one-time adoption"),
            ActivationRoutePublication::AdoptLegacyResident
        );

        // Exhaust the policy, including states not admitted by ordinary
        // materialization paths. No residual branch may imply residency.
        for state in [
            ActiveExecutionState::Creating,
            ActiveExecutionState::Created,
            ActiveExecutionState::Recovering,
            ActiveExecutionState::RecoveryFailed,
            ActiveExecutionState::Running,
            ActiveExecutionState::Suspended,
        ] {
            for materialized in [false, true] {
                for initialized in [false, true] {
                    for created in [false, true] {
                        let capture = pplx_space_metrics::test_capture::capture_counters_guard();
                        let result = classify_activation_route_publication(
                            &row(state, materialized, initialized),
                            created,
                        );
                        let counters = capture.finish();
                        let expected = if initialized {
                            Some(ActivationRoutePublication::PreserveExisting)
                        } else if materialized {
                            Some(ActivationRoutePublication::AdoptLegacyResident)
                        } else if created
                            || matches!(
                                state,
                                ActiveExecutionState::Creating | ActiveExecutionState::Recovering
                            )
                        {
                            Some(ActivationRoutePublication::InitializeFirst)
                        } else {
                            None
                        };
                        if let Some(expected) = expected {
                            assert_eq!(result.unwrap(), expected);
                            assert!(counters.is_empty());
                        } else {
                            assert!(matches!(result, Err(SubstrateError::Unavailable { .. })));
                            assert_eq!(counters.len(), 1);
                            assert_eq!(counters[0].name, "sandbox.api.k8s.route_identity.decision");
                            assert_eq!(counters[0].tags, ["reason:rotation_required_unclassified"]);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn parse_create_command_ready_timeout_applies_default_for_missing_or_invalid() {
        let default = DEFAULT_CREATE_COMMAND_READY_TIMEOUT;
        assert_eq!(parse_create_command_ready_timeout(None), default);
        assert_eq!(parse_create_command_ready_timeout(Some("")), default);
        assert_eq!(
            parse_create_command_ready_timeout(Some("not-a-number")),
            default
        );
        // Zero is rejected — a zero deadline would time out every create
        // immediately, which is never the intent of the env var.
        assert_eq!(parse_create_command_ready_timeout(Some("0")), default);
    }

    #[test]
    fn parse_create_command_ready_timeout_honors_explicit_value() {
        assert_eq!(
            parse_create_command_ready_timeout(Some("45")),
            Duration::from_secs(45)
        );
        assert_eq!(
            parse_create_command_ready_timeout(Some("120")),
            Duration::from_secs(120)
        );
    }

    #[test]
    fn parse_create_data_plane_ready_timeout_defaults_to_120s() {
        let default = DEFAULT_CREATE_DATA_PLANE_READY_TIMEOUT;
        assert_eq!(parse_create_data_plane_ready_timeout(None), default);
        assert_eq!(parse_create_data_plane_ready_timeout(Some("")), default);
        assert_eq!(
            parse_create_data_plane_ready_timeout(Some("not-a-number")),
            default
        );
        assert_eq!(parse_create_data_plane_ready_timeout(Some("0")), default);
    }

    #[test]
    fn parse_create_data_plane_ready_timeout_honors_explicit_value() {
        assert_eq!(
            parse_create_data_plane_ready_timeout(Some("60")),
            Duration::from_secs(60)
        );
        assert_eq!(
            parse_create_data_plane_ready_timeout(Some("180")),
            Duration::from_secs(180)
        );
    }

    #[test]
    fn restore_cleanup_requires_clean_error_and_proof_wake_was_not_dispatched() {
        use super::super::execution::wake_failure_should_revert_to_suspended;
        use super::super::pod_ops::WakeDispatchStatus;
        use crate::controllers::substrate::SubstrateError;

        // A timeout may have left the wake RPC in flight with a live pod/VM.
        // Reverting to `suspended` (which neither the orphaned-sandbox nor the
        // transient-lifecycle sweeper touches) would orphan it, so we must NOT
        // revert — leave the row `restoring` to converge / be swept.
        assert!(!wake_failure_should_revert_to_suspended(
            Some(&SubstrateError::Timeout {
                message: "wake budget elapsed".to_string(),
                phase: None,
            }),
            WakeDispatchStatus::MayHaveLanded,
        ));
        // Clean failures revert: the restore definitively didn't take, so the
        // durable `suspended` state is accurate and a retry re-drives — but
        // only when dispatch evidence proves no runtime work can exist.
        assert!(wake_failure_should_revert_to_suspended(
            Some(&SubstrateError::Conflict {
                message: "wake rpc failed".to_string(),
            }),
            WakeDispatchStatus::NotDispatched,
        ));
        for post_dispatch_error in [
            SubstrateError::Conflict {
                message: "legacy route publication lost its CAS".to_string(),
            },
            SubstrateError::Unavailable {
                message: "sbx-mgr unreachable".to_string(),
            },
        ] {
            assert!(!wake_failure_should_revert_to_suspended(
                Some(&post_dispatch_error),
                WakeDispatchStatus::MayHaveLanded,
            ));
        }

        // Success never reverts.
        assert!(!wake_failure_should_revert_to_suspended(
            None,
            WakeDispatchStatus::NotDispatched,
        ));
    }

    #[test]
    fn missing_runtime_context_wake_requires_identity_ack_retry() {
        use super::super::support::{WakeRpcErrorDisposition, classify_wake_rpc_error};

        let error = tonic::Status::failed_precondition(
            pplx_space_common_core::wake_signals::MISSING_RUNTIME_CONTEXT_MARKER,
        );

        let disposition = classify_wake_rpc_error(&error);

        assert_eq!(
            disposition,
            WakeRpcErrorDisposition::ReadinessPoll("missing_runtime_context")
        );
    }

    #[test]
    fn command_ready_poll_backoff_preserves_fast_window_then_grows_and_caps() {
        let far_deadline = Instant::now() + Duration::from_secs(3_600);
        let mut fast_backoff = CommandReadyPollBackoff::new(Instant::now(), far_deadline);
        let started_at = Instant::now() - START_WAIT_FAST_POLL_WINDOW;
        let mut backoff = CommandReadyPollBackoff::new(started_at, far_deadline);

        let delays = (0..6).map(|_| backoff.next_delay()).collect::<Vec<_>>();
        let expected_bounds = [
            (Duration::from_millis(800), Duration::from_secs(1)),
            (Duration::from_millis(1_600), Duration::from_secs(2)),
            (Duration::from_millis(3_200), Duration::from_secs(4)),
            (Duration::from_millis(6_400), Duration::from_secs(8)),
            (Duration::from_secs(8), Duration::from_secs(10)),
            (Duration::from_secs(8), Duration::from_secs(10)),
        ];

        assert_eq!(fast_backoff.next_delay(), START_WAIT_FAST_POLL_INTERVAL);
        assert!(
            delays
                .iter()
                .zip(expected_bounds)
                .all(|(delay, (lower, upper))| (lower..=upper).contains(delay))
        );
        assert_eq!(
            jittered_command_ready_poll_delay(Duration::from_secs(10), 0.0),
            Duration::from_secs(10)
        );
        assert_eq!(
            jittered_command_ready_poll_delay(
                Duration::from_secs(10),
                COMMAND_READY_POLL_JITTER_FACTOR,
            ),
            Duration::from_secs(8)
        );
    }

    #[test]
    fn command_ready_poll_backoff_clamps_delays_to_deadline() {
        let started_at = Instant::now() - START_WAIT_FAST_POLL_WINDOW;
        let mut backoff = CommandReadyPollBackoff::new(started_at, Instant::now());
        assert_eq!(backoff.next_delay(), Duration::ZERO);
    }

    /// Insert one `sandbox_k8s_executions` row. The fenced-retire tests differ
    /// only in these six values, so they share this rather than each carrying
    /// its own copy of the INSERT.
    async fn insert_k8s_execution(
        pool: &PgPool,
        sandbox_id: SandboxId,
        generation: i64,
        pod_name: &str,
        pod_uid: &str,
        pod_ip: Option<&str>,
        active: bool,
    ) -> Result<(), BoxError> {
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip, state, active)
             VALUES ($1, $2, $3, 'space', $4, $5::INET, 'running', $6)",
        )
        .bind(sandbox_id)
        .bind(generation)
        .bind(pod_name)
        .bind(pod_uid)
        .bind(pod_ip)
        .bind(active)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Seed a sandbox with one active execution whose pod is `pod_name`/
    /// `recorded_uid`, and hand back a substrate whose reflector already holds
    /// a pod at that name with `live_uid`. The kube client 400s on anything
    /// other than a pod GET/POST, so any pod DELETE this teardown attempts
    /// turns the result into an `Err` — which is exactly the assertion these
    /// tests need: a refused retire must reach the apiserver not at all.
    async fn fenced_retire_subject(
        pool: &PgPool,
        sandbox_id: SandboxId,
        pod_name: &str,
        recorded_uid: &str,
        live_uid: &str,
    ) -> Result<KubernetesSubstrate, BoxError> {
        insert_test_sandbox_row(pool, sandbox_id, "failed").await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active)
             VALUES ($1, 1, $2, 'space', $3, 'running', TRUE)",
        )
        .bind(sandbox_id)
        .bind(pod_name)
        .bind(recorded_uid)
        .execute(pool)
        .await?;
        let live = Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(pod_name.to_string()),
                namespace: Some("space".to_string()),
                uid: Some(live_uid.to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        Ok(substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            mock_kubernetes_client(Arc::new(Mutex::new(Vec::new()))),
            DEFAULT_SANDBOX_PROXY_PORT,
            Some(live),
        ))
    }

    /// A kube client that records the body of every pod `DELETE`. The
    /// preconditions a fenced delete carries ride in that body
    /// (`DeleteOptions`), so this is what proves they were actually sent
    /// rather than merely computed.
    fn delete_capturing_kubernetes_client(
        captured_deletes: Arc<Mutex<Vec<(String, serde_json::Value)>>>,
        live_pod: Option<Pod>,
    ) -> Client {
        let live_pod = Arc::new(live_pod);
        let service = tower::service_fn(move |req: Request<Body>| {
            let captured_deletes = Arc::clone(&captured_deletes);
            let live_pod = Arc::clone(&live_pod);
            async move {
                let method = req.method().clone();
                let path = req.uri().path().to_string();
                // The teardown's own pod read, before the fence's. Answering it
                // with the same object the reflector holds keeps this fixture
                // to one source of truth.
                if method == Method::GET && path.starts_with("/api/v1/namespaces/space/pods/") {
                    let (status, body) = match live_pod.as_ref() {
                        Some(pod) => (StatusCode::OK, serde_json::to_value(pod)?),
                        None => (
                            StatusCode::NOT_FOUND,
                            serde_json::json!({
                                "kind": "Status",
                                "apiVersion": "v1",
                                "status": "Failure",
                                "reason": "NotFound",
                                "code": 404
                            }),
                        ),
                    };
                    return Ok::<_, BoxError>(
                        Response::builder()
                            .status(status)
                            .header("content-type", "application/json")
                            .body(Body::from(serde_json::to_vec(&body)?))?,
                    );
                }
                if method == Method::DELETE
                    && let Some(pod_name) = path.strip_prefix("/api/v1/namespaces/space/pods/")
                {
                    let pod_name = pod_name.to_string();
                    let body = req.into_body().collect().await?.to_bytes();
                    let options: serde_json::Value = if body.is_empty() {
                        serde_json::Value::Null
                    } else {
                        serde_json::from_slice(&body)?
                    };
                    captured_deletes
                        .lock()
                        .expect("capture mutex poisoned")
                        .push((pod_name, options));
                    let status = serde_json::json!({
                        "kind": "Status",
                        "apiVersion": "v1",
                        "status": "Success",
                        "code": 200
                    });
                    return Ok::<_, BoxError>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .header("content-type", "application/json")
                            .body(Body::from(serde_json::to_vec(&status)?))?,
                    );
                }
                let body = serde_json::json!({
                    "kind": "Status",
                    "apiVersion": "v1",
                    "status": "Failure",
                    "message": format!("unexpected mock Kubernetes request: {method} {path}"),
                    "reason": "BadRequest",
                    "code": 400
                });
                Ok::<_, BoxError>(
                    Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&body)?))?,
                )
            }
        });
        ClientBuilder::new(service, "space").build()
    }

    /// The in-VM stop addresses the *sandbox*, not the pod, so the address it
    /// uses decides which VM gets stopped. A fenced teardown derives that
    /// address from the pod the fence verified — never from the execution row,
    /// and never by re-resolving whatever is active by then, which on a race
    /// is the successor. Here the row advertises a runtime the verified pod
    /// does not have: a teardown that trusted the row would try to stop
    /// 127.0.0.1 and fail the retire; this one sees no runtime to stop and
    /// proceeds straight to the pod delete.
    #[tokio::test(flavor = "current_thread")]
    async fn fenced_retire_ignores_a_runtime_the_fenced_pod_does_not_have() -> Result<(), BoxError>
    {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
        insert_k8s_execution(
            &pool,
            sandbox_id,
            1,
            &pod_name,
            "uid-observed",
            Some("127.0.0.1"),
            true,
        )
        .await?;
        // The verified pod carries no IP; the execution row claims one.
        let fenced_pod = Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(pod_name.clone()),
                namespace: Some("space".to_string()),
                uid: Some("uid-observed".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let captured_deletes = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            delete_capturing_kubernetes_client(
                Arc::clone(&captured_deletes),
                Some(fenced_pod.clone()),
            ),
            DEFAULT_SANDBOX_PROXY_PORT,
            Some(fenced_pod),
        );

        let outcome = substrate
            .retire_execution(RetireExecutionRequest::fenced(
                sandbox_id,
                PodIdentityFence::new(PodName::parse(&pod_name)?)
                    .with_pod_uid(PodUid::parse("uid-observed")?),
            ))
            .await?;

        assert_eq!(outcome, RetireExecutionOutcome::Retired);
        assert_eq!(
            captured_deletes
                .lock()
                .expect("capture mutex poisoned")
                .len(),
            1,
            "the fenced pod is still deleted; only the in-VM stop was skipped"
        );
        Ok(())
    }

    /// A fenced teardown still collects strictly-older generations. Nothing
    /// else reliably does: once the retire succeeds the sandbox has no active
    /// execution and drops out of the failed-cleanup scan, so a prior-
    /// generation pod skipped here is a pod that lives until its node does.
    #[tokio::test(flavor = "current_thread")]
    async fn fenced_retire_collects_prior_generation_pods() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let stale_pod = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        let pod_name = format!("space-sbx-{}-g2", sandbox_id.as_uuid().simple());
        insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
        insert_k8s_execution(&pool, sandbox_id, 1, &stale_pod, "uid-stale", None, false).await?;
        insert_k8s_execution(&pool, sandbox_id, 2, &pod_name, "uid-observed", None, true).await?;
        let live = Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(pod_name.clone()),
                namespace: Some("space".to_string()),
                uid: Some("uid-observed".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let captured_deletes = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            delete_capturing_kubernetes_client(Arc::clone(&captured_deletes), Some(live.clone())),
            DEFAULT_SANDBOX_PROXY_PORT,
            Some(live),
        );

        let outcome = substrate
            .retire_execution(RetireExecutionRequest::fenced(
                sandbox_id,
                PodIdentityFence::new(PodName::parse(&pod_name)?)
                    .with_pod_uid(PodUid::parse("uid-observed")?),
            ))
            .await?;

        assert_eq!(outcome, RetireExecutionOutcome::Retired);
        let deleted: std::collections::BTreeSet<String> = captured_deletes
            .lock()
            .expect("capture mutex poisoned")
            .iter()
            .map(|(name, _)| name.clone())
            .collect();
        assert_eq!(
            deleted,
            std::collections::BTreeSet::from([stale_pod, pod_name]),
            "the fenced pod and every strictly-older generation must be deleted"
        );
        Ok(())
    }

    /// `resolve_pod_ip` releases pod tracking itself when it finds the pod
    /// authoritatively absent, so an ordinary already-gone pod reaches the
    /// generation CAS with nothing left to claim. That is a completed retire,
    /// not a pod-identity race — reporting a defer there would tell the
    /// sweeper nothing was torn down while the execution had just been
    /// retired, and would meter a race that never happened.
    #[tokio::test(flavor = "current_thread")]
    async fn fenced_retire_of_an_already_absent_pod_reports_retired() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
        insert_k8s_execution(&pool, sandbox_id, 1, &pod_name, "uid-observed", None, true).await?;
        // No pod in the reflector and a 404 from the apiserver: the object is
        // authoritatively gone.
        let captured_deletes = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            delete_capturing_kubernetes_client(Arc::clone(&captured_deletes), None),
            DEFAULT_SANDBOX_PROXY_PORT,
            None,
        );

        let outcome = substrate
            .retire_execution(RetireExecutionRequest::fenced(
                sandbox_id,
                PodIdentityFence::new(PodName::parse(&pod_name)?)
                    .with_pod_uid(PodUid::parse("uid-observed")?),
            ))
            .await?;

        assert_eq!(outcome, RetireExecutionOutcome::Retired);
        assert!(
            active_execution_generations(&pool, sandbox_id)
                .await
                .is_empty(),
            "tracking really was released; the outcome must say so"
        );
        Ok(())
    }

    /// A name-only fence reaching the teardown means the pod was absent at
    /// check time. It must issue no pod delete at all: between that check and
    /// the delete the pod can materialize, and with no UID the request would
    /// go out unconditional and remove a live pod nobody identified. The rest
    /// of the retire still runs — that is the cleanup this fence shape exists
    /// to perform.
    #[tokio::test(flavor = "current_thread")]
    async fn a_name_only_fenced_retire_deletes_no_pod() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
        // The pre-persistence state: an active execution with no recorded UID.
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active)
             VALUES ($1, 1, $2, 'space', 'running', TRUE)",
        )
        .bind(sandbox_id)
        .bind(&pod_name)
        .execute(&pool)
        .await?;
        let captured_deletes = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            delete_capturing_kubernetes_client(Arc::clone(&captured_deletes), None),
            DEFAULT_SANDBOX_PROXY_PORT,
            None,
        );

        let outcome = substrate
            .retire_execution(RetireExecutionRequest::fenced(
                sandbox_id,
                PodIdentityFence::new(PodName::parse(&pod_name)?),
            ))
            .await?;

        assert_eq!(outcome, RetireExecutionOutcome::Retired);
        assert!(
            captured_deletes
                .lock()
                .expect("capture mutex poisoned")
                .is_empty(),
            "a fence with no UID has nothing safe to delete"
        );
        assert!(
            active_execution_generations(&pool, sandbox_id)
                .await
                .is_empty(),
            "the rest of the retire still runs: tracking is released"
        );
        Ok(())
    }

    /// A cached pod that would authorize the blocking in-VM stop must be
    /// re-read authoritatively first. The reflector can lag a same-name
    /// replacement, and a stale hit matches a stale fence — so the teardown
    /// would stop a live VM and only learn better when the delete precondition
    /// came back 409, long after the stop landed. Here the cache holds the
    /// observed pod with a running IP and the apiserver holds its replacement.
    #[tokio::test(flavor = "current_thread")]
    async fn a_fenced_stop_never_rides_a_stale_cache() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
        insert_k8s_execution(&pool, sandbox_id, 1, &pod_name, "uid-observed", None, true).await?;
        let running = |uid: &str| Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(pod_name.clone()),
                namespace: Some("space".to_string()),
                uid: Some(uid.to_string()),
                ..Default::default()
            },
            status: Some(PodStatus {
                phase: Some("Running".to_string()),
                pod_ip: Some("127.0.0.1".to_string()),
                pod_ips: Some(vec![PodIP {
                    ip: "127.0.0.1".to_string(),
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let captured_deletes = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            // The apiserver's answer: the name now belongs to a different pod.
            delete_capturing_kubernetes_client(
                Arc::clone(&captured_deletes),
                Some(running("uid-successor")),
            ),
            DEFAULT_SANDBOX_PROXY_PORT,
            // The reflector's answer: still the pod the sweep observed.
            Some(running("uid-observed")),
        );

        let outcome = substrate
            .retire_execution(RetireExecutionRequest::fenced(
                sandbox_id,
                PodIdentityFence::new(PodName::parse(&pod_name)?)
                    .with_pod_uid(PodUid::parse("uid-observed")?),
            ))
            .await?;

        assert_eq!(
            outcome,
            RetireExecutionOutcome::PodIdentityChanged {
                mismatch: PodFenceMismatch::PodUidChanged,
                progress: TeardownProgress::NothingMutated,
            },
            "the authoritative read decides, not the cache"
        );
        assert!(
            captured_deletes
                .lock()
                .expect("capture mutex poisoned")
                .is_empty(),
            "a refusal tears nothing down"
        );
        Ok(())
    }

    /// Refusing a live pod a name-only fence cannot describe is correct but not
    /// on its own convergent: if the create died before persisting pod
    /// metadata, nothing else ever writes that UID and every later candidate
    /// carries the same unusable fence. The refusal records the UID it saw, so
    /// the next sweep has a fence it can enforce.
    #[tokio::test(flavor = "current_thread")]
    async fn a_name_only_refusal_records_the_uid_it_could_not_verify() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active)
             VALUES ($1, 1, $2, 'space', 'running', TRUE)",
        )
        .bind(sandbox_id)
        .bind(&pod_name)
        .execute(&pool)
        .await?;
        let live = Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(pod_name.clone()),
                namespace: Some("space".to_string()),
                uid: Some("uid-never-persisted".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let captured_deletes = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            delete_capturing_kubernetes_client(Arc::clone(&captured_deletes), Some(live.clone())),
            DEFAULT_SANDBOX_PROXY_PORT,
            Some(live),
        );

        let outcome = substrate
            .retire_execution(RetireExecutionRequest::fenced(
                sandbox_id,
                PodIdentityFence::new(PodName::parse(&pod_name)?),
            ))
            .await?;

        assert_eq!(
            outcome,
            RetireExecutionOutcome::PodIdentityChanged {
                mismatch: PodFenceMismatch::PodIdentityUnverified,
                progress: TeardownProgress::NothingMutated,
            }
        );
        let recorded: Option<String> = sqlx::query_scalar(
            "SELECT pod_uid FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            recorded.as_deref(),
            Some("uid-never-persisted"),
            "without this write the row can never be fenced, so it can never be cleaned up"
        );
        Ok(())
    }

    /// A caller that observed *no* execution must not get the unfenced
    /// teardown. That path deletes every tracked pod name and releases
    /// tracking unconditionally, and a row with no execution is exactly the
    /// one a concurrent create or wake is most likely to give one — so having
    /// seen nothing has to mean "re-prove it", not "nothing to protect".
    #[tokio::test(flavor = "current_thread")]
    async fn a_no_execution_fence_refuses_an_execution_that_appeared() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
        // The wake that landed after the sweep selected a row with no execution.
        insert_k8s_execution(&pool, sandbox_id, 1, &pod_name, "uid-successor", None, true).await?;
        let captured_deletes = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            delete_capturing_kubernetes_client(Arc::clone(&captured_deletes), None),
            DEFAULT_SANDBOX_PROXY_PORT,
            None,
        );

        let outcome = substrate
            .retire_execution(RetireExecutionRequest {
                sandbox_id,
                fence: RetireFence::NoExecution,
            })
            .await?;

        assert_eq!(
            outcome,
            RetireExecutionOutcome::PodIdentityChanged {
                mismatch: PodFenceMismatch::ExecutionMissing,
                progress: TeardownProgress::NothingMutated,
            }
        );
        assert!(
            captured_deletes
                .lock()
                .expect("capture mutex poisoned")
                .is_empty(),
            "the successor's pod must survive"
        );
        assert_eq!(
            active_execution_generations(&pool, sandbox_id).await,
            vec![1],
            "and so must its execution row"
        );
        Ok(())
    }

    /// The absence check and the tracking clear have to be the same
    /// transaction. Checking, then falling into the unfenced tail, re-creates
    /// the race the variant exists to close: an execution created in between
    /// gets stopped, deleted and cleared by a caller that never saw it. Here
    /// the row appears between the caller's observation and the teardown.
    #[tokio::test(flavor = "current_thread")]
    async fn a_no_execution_fence_never_clears_a_row_it_did_not_see() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
        insert_k8s_execution(&pool, sandbox_id, 1, &pod_name, "uid-successor", None, true).await?;
        let captured_deletes = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            delete_capturing_kubernetes_client(Arc::clone(&captured_deletes), None),
            DEFAULT_SANDBOX_PROXY_PORT,
            None,
        );

        let outcome = substrate
            .retire_execution(RetireExecutionRequest {
                sandbox_id,
                fence: RetireFence::NoExecution,
            })
            .await?;

        assert_eq!(
            outcome,
            RetireExecutionOutcome::PodIdentityChanged {
                mismatch: PodFenceMismatch::ExecutionMissing,
                progress: TeardownProgress::NothingMutated,
            }
        );
        assert_eq!(
            active_execution_generations(&pool, sandbox_id).await,
            vec![1],
            "the execution that appeared must keep its tracking"
        );
        assert!(
            captured_deletes
                .lock()
                .expect("capture mutex poisoned")
                .is_empty(),
            "and its pod must not be swept by a tracked-pod list the caller never observed"
        );
        Ok(())
    }

    /// The UID backfill has one field to contribute. `persist_pod_metadata`
    /// assigns rather than patches — it would set `pod_ip` to NULL and
    /// recompute `state` from it — so a row the create path completed while
    /// the fence was reading must come through untouched, and a UID that is
    /// already recorded must win.
    /// The identity a backfill binds: generation and attempt as the row holds
    /// them, with no pod observation (the backfill exists precisely because
    /// the UID is missing).
    fn backfill_identity(pod_name: &str) -> ExecutionIdentity {
        ExecutionIdentity {
            pod_name: PodName::parse(pod_name).expect("pod name"),
            generation: Some(1),
            activation_attempt_id: None,
            recorded: RecordedExecution::Read {
                pod_uid: None,
                route_activation_id: None,
            },
            pod: ObservedPod::Unobserved,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn the_uid_backfill_touches_nothing_it_should_not() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let store = K8sStore::new(pool.clone(), "space".to_string());
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
        insert_k8s_execution(
            &pool,
            sandbox_id,
            1,
            &pod_name,
            "uid-from-create",
            Some("10.4.0.7"),
            true,
        )
        .await?;

        assert!(
            !store
                .backfill_missing_pod_uid(sandbox_id, &backfill_identity(&pod_name), "uid-observed")
                .await?,
            "a recorded UID must not be overwritten"
        );
        let (uid, ip, state): (Option<String>, Option<String>, String) = sqlx::query_as(
            "SELECT pod_uid, host(pod_ip), state
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(uid.as_deref(), Some("uid-from-create"));
        assert_eq!(ip.as_deref(), Some("10.4.0.7"), "the IP must survive");
        assert_eq!(state, "running", "and so must the state");

        // The case it exists for: a genuinely missing UID gets filled.
        sqlx::query("UPDATE sandbox_k8s_executions SET pod_uid = NULL WHERE sandbox_id = $1")
            .bind(sandbox_id)
            .execute(&pool)
            .await?;
        assert!(
            store
                .backfill_missing_pod_uid(sandbox_id, &backfill_identity(&pod_name), "uid-observed")
                .await?
        );
        let (uid, ip, state): (Option<String>, Option<String>, String) = sqlx::query_as(
            "SELECT pod_uid, host(pod_ip), state
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(uid.as_deref(), Some("uid-observed"));
        assert_eq!(ip.as_deref(), Some("10.4.0.7"));
        assert_eq!(state, "running");
        Ok(())
    }

    /// The same fence when the absence still holds: there is nothing to refuse,
    /// so the teardown runs and clears the leftover placement state.
    #[tokio::test(flavor = "current_thread")]
    async fn a_no_execution_fence_retires_when_the_absence_holds() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
        let captured_deletes = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            delete_capturing_kubernetes_client(Arc::clone(&captured_deletes), None),
            DEFAULT_SANDBOX_PROXY_PORT,
            None,
        );

        let outcome = substrate
            .retire_execution(RetireExecutionRequest {
                sandbox_id,
                fence: RetireFence::NoExecution,
            })
            .await?;

        assert_eq!(outcome, RetireExecutionOutcome::Retired);
        Ok(())
    }

    /// The rows naming these pods are what a later attempt reads to find them
    /// again, so a failed delete must leave them in place. Clearing tracking
    /// first would strand the pod with nothing pointing at it: the
    /// failed-cleanup pass selects active rows only, so the VM would hold node
    /// capacity until the node is recycled.
    #[tokio::test(flavor = "current_thread")]
    async fn a_failed_orphan_delete_keeps_the_row_that_names_it() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let orphan = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
        insert_k8s_execution(&pool, sandbox_id, 1, &orphan, "uid-orphan", None, false).await?;

        // Every DELETE fails with a retryable server error.
        let service = tower::service_fn(move |req: Request<Body>| async move {
            if req.method() == Method::DELETE {
                let body = serde_json::json!({
                    "kind": "Status",
                    "apiVersion": "v1",
                    "status": "Failure",
                    "reason": "InternalError",
                    "code": 500
                });
                return Ok::<_, BoxError>(
                    Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&body)?))?,
                );
            }
            let not_found = serde_json::json!({
                "kind": "Status",
                "apiVersion": "v1",
                "status": "Failure",
                "reason": "NotFound",
                "code": 404
            });
            Ok::<_, BoxError>(
                Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&not_found)?))?,
            )
        });
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            ClientBuilder::new(service, "space").build(),
            DEFAULT_SANDBOX_PROXY_PORT,
            None,
        );

        let outcome = substrate
            .retire_execution(RetireExecutionRequest {
                sandbox_id,
                fence: RetireFence::NoExecution,
            })
            .await;
        assert!(outcome.is_err(), "a failed orphan delete must surface");

        let still_named: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM sandbox_k8s_executions WHERE sandbox_id = $1)",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert!(
            still_named,
            "the row naming the undeleted pod is the only way a later pass finds it"
        );
        let placement: String = sqlx::query_scalar("SELECT placement FROM sandboxes WHERE id = $1")
            .bind(sandbox_id)
            .fetch_one(&pool)
            .await?;
        assert_eq!(
            placement, "on_node",
            "and placement must not be cleared ahead of a delete that failed"
        );
        Ok(())
    }

    /// The same fence must still collect pods on *inactive* rows. A create
    /// cleanup that marks its row inactive and then fails its Kubernetes
    /// delete leaves an orphan no other pass retries — they all select active
    /// rows — and no successor can own one of these, because a successor's pod
    /// is recorded on the active row.
    #[tokio::test(flavor = "current_thread")]
    async fn a_no_execution_fence_collects_orphans_on_inactive_rows() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let orphan = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
        insert_k8s_execution(&pool, sandbox_id, 1, &orphan, "uid-orphan", None, false).await?;
        let captured_deletes = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            delete_capturing_kubernetes_client(Arc::clone(&captured_deletes), None),
            DEFAULT_SANDBOX_PROXY_PORT,
            None,
        );

        let outcome = substrate
            .retire_execution(RetireExecutionRequest {
                sandbox_id,
                fence: RetireFence::NoExecution,
            })
            .await?;

        assert_eq!(outcome, RetireExecutionOutcome::Retired);
        let deleted = captured_deletes.lock().unwrap().clone();
        assert!(
            deleted.iter().any(|(path, _)| path.contains(&orphan)),
            "the inactive row's pod must still be collected, got {deleted:?}"
        );
        Ok(())
    }

    /// A name-only fence whose pod materialized after the fence check must not
    /// release tracking: a live pod with no active execution row is an orphan
    /// the failed-cleanup pass can no longer collect. Absence is re-proven as
    /// late as possible, and the pod that showed up has its UID recorded so the
    /// next sweep can fence on it.
    #[tokio::test(flavor = "current_thread")]
    async fn a_name_only_retire_defers_to_a_pod_that_materialized() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active)
             VALUES ($1, 1, $2, 'space', 'running', TRUE)",
        )
        .bind(sandbox_id)
        .bind(&pod_name)
        .execute(&pool)
        .await?;

        // Absent for the fence check, present for the re-proof: the in-flight
        // create landed in between.
        let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let reads_in_service = Arc::clone(&reads);
        let materialized_name = pod_name.clone();
        let service = tower::service_fn(move |req: Request<Body>| {
            let reads_in_service = Arc::clone(&reads_in_service);
            let materialized_name = materialized_name.clone();
            async move {
                let path = req.uri().path().to_string();
                if req.method() == Method::GET
                    && path.starts_with("/api/v1/namespaces/space/pods/")
                    && reads_in_service.fetch_add(1, std::sync::atomic::Ordering::SeqCst) > 0
                {
                    let pod = serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "name": materialized_name,
                            "namespace": "space",
                            "uid": "uid-materialized"
                        }
                    });
                    return Ok::<_, BoxError>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .header("content-type", "application/json")
                            .body(Body::from(serde_json::to_vec(&pod)?))?,
                    );
                }
                let not_found = serde_json::json!({
                    "kind": "Status",
                    "apiVersion": "v1",
                    "status": "Failure",
                    "reason": "NotFound",
                    "code": 404
                });
                Ok::<_, BoxError>(
                    Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&not_found)?))?,
                )
            }
        });
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            ClientBuilder::new(service, "space").build(),
            DEFAULT_SANDBOX_PROXY_PORT,
            None,
        );

        let outcome = substrate
            .retire_execution(RetireExecutionRequest::fenced(
                sandbox_id,
                PodIdentityFence::new(PodName::parse(&pod_name)?),
            ))
            .await?;

        assert_eq!(
            outcome,
            RetireExecutionOutcome::PodIdentityChanged {
                mismatch: PodFenceMismatch::PodIdentityUnverified,
                progress: TeardownProgress::NothingMutated,
            }
        );
        assert_eq!(
            active_execution_generations(&pool, sandbox_id).await,
            vec![1],
            "releasing tracking here would orphan the pod that just appeared"
        );
        let recorded: Option<String> = sqlx::query_scalar(
            "SELECT pod_uid FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            recorded.as_deref(),
            Some("uid-materialized"),
            "the next sweep needs a fence it can enforce"
        );
        Ok(())
    }

    /// A generation is not a unique execution identity — activation retries and
    /// cross-node redrives reuse one, which is why the attempt-scoped tracking
    /// cleanup exists at all. So the fenced tracking release carries the pod
    /// UID too: a replacement that reused the generation and persisted its own
    /// UID after the fenced delete must not be marked inactive by a teardown
    /// that never saw it. The mock performs the replacement at the DELETE,
    /// which is exactly where that window is.
    #[tokio::test(flavor = "current_thread")]
    async fn fenced_retire_spares_a_same_generation_replacement() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
        insert_k8s_execution(&pool, sandbox_id, 1, &pod_name, "uid-observed", None, true).await?;
        let live = Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(pod_name.clone()),
                namespace: Some("space".to_string()),
                uid: Some("uid-observed".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        let race_pool = pool.clone();
        let service = tower::service_fn(move |req: Request<Body>| {
            let race_pool = race_pool.clone();
            async move {
                let path = req.uri().path().to_string();
                if req.method() == Method::DELETE
                    && path.starts_with("/api/v1/namespaces/space/pods/")
                {
                    // A retry recreated the pod under the same generation and
                    // persisted its identity.
                    sqlx::query(
                        "UPDATE sandbox_k8s_executions
                            SET pod_uid = 'uid-replacement'
                          WHERE sandbox_id = $1 AND active",
                    )
                    .bind(sandbox_id)
                    .execute(&race_pool)
                    .await?;
                }
                let body = serde_json::json!({
                    "kind": "Status",
                    "apiVersion": "v1",
                    "status": "Success",
                    "code": 200
                });
                Ok::<_, BoxError>(
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&body)?))?,
                )
            }
        });
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            ClientBuilder::new(service, "space").build(),
            DEFAULT_SANDBOX_PROXY_PORT,
            Some(live),
        );

        let outcome = substrate
            .retire_execution(RetireExecutionRequest::fenced(
                sandbox_id,
                PodIdentityFence::new(PodName::parse(&pod_name)?)
                    .with_pod_uid(PodUid::parse("uid-observed")?),
            ))
            .await?;

        assert_eq!(
            outcome,
            RetireExecutionOutcome::PodIdentityChanged {
                mismatch: PodFenceMismatch::ExecutionSuperseded,
                // The observed pod was deleted before the tracking CAS lost.
                progress: TeardownProgress::PodDeleted,
            }
        );
        assert_eq!(
            active_execution_generations(&pool, sandbox_id).await,
            vec![1],
            "the replacement kept the generation, so only its UID distinguishes it"
        );
        Ok(())
    }

    /// The last thing that can look identical: a retry that reuses the
    /// generation *and* the deterministic pod name, and has not materialized a
    /// pod yet — so both attempts also share an absent UID. Only the
    /// activation attempt id separates them, and the fence refuses on it
    /// rather than tearing down a replacement it never selected.
    #[tokio::test(flavor = "current_thread")]
    async fn fenced_retire_refuses_a_replacement_activation_attempt() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        let selected_attempt = SandboxActivationAttemptId::new();
        let replacement_attempt = SandboxActivationAttemptId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active,
                  activation_attempt_id)
             VALUES ($1, 1, $2, 'space', 'running', TRUE, $3)",
        )
        .bind(sandbox_id)
        .bind(&pod_name)
        .bind(replacement_attempt)
        .execute(&pool)
        .await?;
        let captured_deletes = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            delete_capturing_kubernetes_client(Arc::clone(&captured_deletes), None),
            DEFAULT_SANDBOX_PROXY_PORT,
            None,
        );

        let outcome = substrate
            .retire_execution(RetireExecutionRequest::fenced(
                sandbox_id,
                PodIdentityFence::new(PodName::parse(&pod_name)?)
                    .with_activation_attempt(selected_attempt),
            ))
            .await?;

        assert_eq!(
            outcome,
            RetireExecutionOutcome::PodIdentityChanged {
                mismatch: PodFenceMismatch::ActivationAttemptChanged,
                progress: TeardownProgress::NothingMutated,
            },
            "same generation, same pod name, same absent UID — only the attempt differs"
        );
        assert_eq!(
            active_execution_generations(&pool, sandbox_id).await,
            vec![1],
            "the replacement attempt must keep its execution"
        );
        assert!(
            captured_deletes
                .lock()
                .expect("capture mutex poisoned")
                .is_empty()
        );
        Ok(())
    }

    /// The common shape of the same hazard: a *completed* execution's attempt
    /// is NULL, so a fence built from one carries `None`. Skipping the
    /// comparison for that — reading it as "no expectation" rather than as the
    /// value the caller saw — fails open on exactly the state a same-generation
    /// recovery is most likely to take over.
    #[tokio::test(flavor = "current_thread")]
    async fn fenced_retire_refuses_an_attempt_that_appeared_where_none_was_observed()
    -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
        // A recovery claimed the generation and stamped its attempt after the
        // sweep selected a row that had none.
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active,
                  activation_attempt_id)
             VALUES ($1, 1, $2, 'space', 'running', TRUE, $3)",
        )
        .bind(sandbox_id)
        .bind(&pod_name)
        .bind(SandboxActivationAttemptId::new())
        .execute(&pool)
        .await?;
        let captured_deletes = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            delete_capturing_kubernetes_client(Arc::clone(&captured_deletes), None),
            DEFAULT_SANDBOX_PROXY_PORT,
            None,
        );

        let outcome = substrate
            .retire_execution(RetireExecutionRequest::fenced(
                sandbox_id,
                // No attempt: what the caller observed was NULL.
                PodIdentityFence::new(PodName::parse(&pod_name)?),
            ))
            .await?;

        assert_eq!(
            outcome,
            RetireExecutionOutcome::PodIdentityChanged {
                mismatch: PodFenceMismatch::ActivationAttemptChanged,
                progress: TeardownProgress::NothingMutated,
            }
        );
        assert_eq!(
            active_execution_generations(&pool, sandbox_id).await,
            vec![1],
            "an observed NULL is an expectation, not a wildcard"
        );
        Ok(())
    }

    /// The name-only fence's whole job is cleaning up rows whose pod is gone —
    /// and the common shape of that is a reflector that already dropped the
    /// pod while `sandbox_k8s_executions.pod_uid` still holds its identity.
    /// The release CAS has to match the row *as it is*, not as the delete
    /// fence describes it: guarding on the fence's absent UID would require
    /// `pod_uid IS NULL`, claim nothing, and defer this row forever.
    #[tokio::test(flavor = "current_thread")]
    async fn a_name_only_retire_releases_a_row_that_still_records_a_uid() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
        // The pod is gone from the reflector, but its UID was persisted while
        // it lived.
        insert_k8s_execution(&pool, sandbox_id, 1, &pod_name, "uid-persisted", None, true).await?;
        let captured_deletes = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            delete_capturing_kubernetes_client(Arc::clone(&captured_deletes), None),
            DEFAULT_SANDBOX_PROXY_PORT,
            None,
        );

        let outcome = substrate
            .retire_execution(RetireExecutionRequest::fenced(
                sandbox_id,
                PodIdentityFence::new(PodName::parse(&pod_name)?),
            ))
            .await?;

        assert_eq!(
            outcome,
            RetireExecutionOutcome::Retired,
            "an absent pod whose UID was recorded must still be cleanable"
        );
        assert!(
            active_execution_generations(&pool, sandbox_id)
                .await
                .is_empty(),
            "deferring here would strand the row in every cleanup pass forever"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reachable_runtime_probe_leaves_missing_and_terminal_pods_for_recovery()
    -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        for terminal in [false, true] {
            let pool = db.pool.clone();
            let sandbox_id = SandboxId::new();
            let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
            insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
            insert_k8s_execution(
                &pool,
                sandbox_id,
                1,
                &pod_name,
                "uid-observed",
                Some("192.0.2.1"),
                true,
            )
            .await?;
            let pod = terminal.then(|| Pod {
                metadata: kube::api::ObjectMeta {
                    name: Some(pod_name.clone()),
                    namespace: Some("space".to_string()),
                    uid: Some("uid-observed".to_string()),
                    ..Default::default()
                },
                status: Some(PodStatus {
                    phase: Some("Failed".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            });
            let deletes = Arc::new(Mutex::new(Vec::new()));
            let substrate = substrate_with_store_client_proxy_port_and_pod(
                pool,
                delete_capturing_kubernetes_client(deletes.clone(), pod.clone()),
                DEFAULT_SANDBOX_PROXY_PORT,
                pod,
            );
            let before = substrate
                .store
                .active_execution_status(sandbox_id)
                .await?
                .unwrap();
            assert!(substrate.probe_runtime_client(sandbox_id).await?.is_none());
            assert!(deletes.lock().unwrap().is_empty());
            let after = substrate
                .store
                .active_execution_status(sandbox_id)
                .await?
                .unwrap();
            assert_eq!(
                (
                    after.generation,
                    after.state,
                    after.lifecycle,
                    after.pod_uid,
                    after.pod_ip
                ),
                (
                    before.generation,
                    before.state,
                    before.lifecycle,
                    before.pod_uid,
                    before.pod_ip
                ),
            );
        }
        Ok(())
    }

    /// A terminal fenced pod is the case where using the general resolver is
    /// observably wrong. `resolve_pod_ip` reacts to a terminal pod by deleting
    /// it *unconditionally* and releasing generation tracking — a destructive
    /// act with no precondition, run before the fence's own delete gets a say.
    /// The fenced path must reach the same "no runtime to stop" conclusion
    /// without any of that: exactly one delete, carrying the precondition.
    #[tokio::test(flavor = "current_thread")]
    async fn fenced_retire_deletes_a_terminal_pod_only_under_its_precondition()
    -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
        insert_k8s_execution(&pool, sandbox_id, 1, &pod_name, "uid-observed", None, true).await?;
        let terminal_pod = Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(pod_name.clone()),
                namespace: Some("space".to_string()),
                uid: Some("uid-observed".to_string()),
                ..Default::default()
            },
            status: Some(PodStatus {
                phase: Some("Failed".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let captured_deletes = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            delete_capturing_kubernetes_client(
                Arc::clone(&captured_deletes),
                Some(terminal_pod.clone()),
            ),
            DEFAULT_SANDBOX_PROXY_PORT,
            Some(terminal_pod),
        );

        let outcome = substrate
            .retire_execution(RetireExecutionRequest::fenced(
                sandbox_id,
                PodIdentityFence::new(PodName::parse(&pod_name)?)
                    .with_pod_uid(PodUid::parse("uid-observed")?),
            ))
            .await?;

        assert_eq!(outcome, RetireExecutionOutcome::Retired);
        let deletes = captured_deletes.lock().expect("capture mutex poisoned");
        let (_, options) = match deletes.as_slice() {
            [single] => single,
            other => panic!(
                "the fenced pod must be deleted exactly once, saw {} — an extra delete means \
                 something other than the fenced path removed it",
                other.len()
            ),
        };
        assert_eq!(
            options.pointer("/preconditions/uid"),
            Some(&serde_json::json!("uid-observed")),
            "the only delete must be the preconditioned one"
        );
        Ok(())
    }

    /// The race the fence check alone cannot win: a successor execution
    /// appears *after* the check passes. The pod DELETE is the point in the
    /// teardown where that is possible, so this mock installs generation 2
    /// exactly there. The tracking release is CAS-guarded on the generation
    /// the fence proved, so it claims nothing and the teardown reports the
    /// supersession instead of retiring the successor's execution row out
    /// from under it.
    #[tokio::test(flavor = "current_thread")]
    async fn fenced_retire_refuses_when_a_successor_lands_mid_teardown() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        let successor_pod = format!("space-sbx-{}-g2", sandbox_id.as_uuid().simple());
        insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
        insert_k8s_execution(&pool, sandbox_id, 1, &pod_name, "uid-observed", None, true).await?;
        let live = Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(pod_name.clone()),
                namespace: Some("space".to_string()),
                uid: Some("uid-observed".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        let race_pool = pool.clone();
        let race_successor = successor_pod.clone();
        let service = tower::service_fn(move |req: Request<Body>| {
            let race_pool = race_pool.clone();
            let race_successor = race_successor.clone();
            async move {
                let method = req.method().clone();
                let path = req.uri().path().to_string();
                if method == Method::GET && path.starts_with("/api/v1/namespaces/space/pods/") {
                    let not_found = serde_json::json!({
                        "kind": "Status",
                        "apiVersion": "v1",
                        "status": "Failure",
                        "reason": "NotFound",
                        "code": 404
                    });
                    return Ok::<_, BoxError>(
                        Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .header("content-type", "application/json")
                            .body(Body::from(serde_json::to_vec(&not_found)?))?,
                    );
                }
                if method == Method::DELETE && path.starts_with("/api/v1/namespaces/space/pods/") {
                    // A wake wins the sandbox while the teardown is mid-flight.
                    sqlx::query(
                        "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1",
                    )
                    .bind(sandbox_id)
                    .execute(&race_pool)
                    .await?;
                    sqlx::query(
                        "INSERT INTO sandbox_k8s_executions
                             (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active)
                         VALUES ($1, 2, $2, 'space', 'uid-successor', 'running', TRUE)",
                    )
                    .bind(sandbox_id)
                    .bind(&race_successor)
                    .execute(&race_pool)
                    .await?;
                }
                let body = serde_json::json!({
                    "kind": "Status",
                    "apiVersion": "v1",
                    "status": "Success",
                    "code": 200
                });
                Ok::<_, BoxError>(
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&body)?))?,
                )
            }
        });
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            ClientBuilder::new(service, "space").build(),
            DEFAULT_SANDBOX_PROXY_PORT,
            Some(live),
        );

        let outcome = substrate
            .retire_execution(RetireExecutionRequest::fenced(
                sandbox_id,
                PodIdentityFence::new(PodName::parse(&pod_name)?)
                    .with_pod_uid(PodUid::parse("uid-observed")?),
            ))
            .await?;

        assert_eq!(
            outcome,
            RetireExecutionOutcome::PodIdentityChanged {
                mismatch: PodFenceMismatch::ExecutionSuperseded,
                // The observed pod was deleted before the tracking CAS lost,
                // and saying otherwise would let a caller restore a lifecycle
                // for a sandbox whose pod is gone.
                progress: TeardownProgress::PodDeleted,
            }
        );
        assert!(
            outcome.permits_undo(),
            "a successor owns the sandbox and this teardown never reached it, so the caller \
             may still undo its own transition"
        );
        assert_eq!(
            active_execution_generations(&pool, sandbox_id).await,
            vec![2],
            "the successor execution must still be active; retiring it is the bug"
        );
        Ok(())
    }

    /// A caller-side probe cannot trust the reflector. Its answer authorizes a
    /// state transition that commits before any delete runs, so unlike the
    /// teardown — whose delete precondition catches a stale read before
    /// mutating anything — a cached pod the apiserver has already replaced
    /// would approve a fence the transition then cannot be followed through
    /// on.
    #[tokio::test(flavor = "current_thread")]
    async fn the_caller_side_probe_does_not_trust_the_reflector() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
        insert_k8s_execution(&pool, sandbox_id, 1, &pod_name, "uid-observed", None, true).await?;

        // Cached: the pod the caller saw, with no IP, so nothing about it
        // triggers the existing authoritative re-read.
        let cached = Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(pod_name.clone()),
                namespace: Some("space".to_string()),
                uid: Some("uid-observed".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        // Live: a replacement already carries that name at the apiserver.
        let replacement = serde_json::json!({
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {
                "name": pod_name,
                "namespace": "space",
                "uid": "uid-replacement",
                "resourceVersion": "9001"
            }
        });
        let service = tower::service_fn(move |_req: Request<Body>| {
            let replacement = replacement.clone();
            async move {
                Ok::<_, BoxError>(
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&replacement)?))?,
                )
            }
        });
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            ClientBuilder::new(service, "space").build(),
            DEFAULT_SANDBOX_PROXY_PORT,
            Some(cached),
        );

        let fence = RetireFence::Pod(
            PodIdentityFence::new(PodName::parse(&pod_name)?)
                .with_pod_uid(PodUid::parse("uid-observed")?),
        );
        assert_eq!(
            substrate.retire_fence_mismatch(sandbox_id, &fence).await?,
            Some(PodFenceMismatch::PodUidChanged),
            "the probe must read through to the apiserver, which holds a different pod"
        );
        Ok(())
    }

    /// The last-mile half of the fence. The pre-check and the delete are two
    /// round trips, so the delete has to carry the precondition itself —
    /// otherwise a pod replaced in between is deleted by a request that had
    /// every chance to say "only if it is still the one I saw" and didn't.
    #[tokio::test(flavor = "current_thread")]
    async fn fenced_retire_sends_the_identity_precondition_with_the_delete() -> Result<(), BoxError>
    {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
        insert_k8s_execution(&pool, sandbox_id, 1, &pod_name, "uid-observed", None, true).await?;
        let live = Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(pod_name.clone()),
                namespace: Some("space".to_string()),
                uid: Some("uid-observed".to_string()),
                resource_version: Some("4242".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let captured_deletes = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            delete_capturing_kubernetes_client(Arc::clone(&captured_deletes), Some(live.clone())),
            DEFAULT_SANDBOX_PROXY_PORT,
            Some(live),
        );

        let outcome = substrate
            .retire_execution(RetireExecutionRequest::fenced(
                sandbox_id,
                PodIdentityFence::new(PodName::parse(&pod_name)?)
                    .with_pod_uid(PodUid::parse("uid-observed")?)
                    .with_resource_version(PodResourceVersion::parse("4242")?),
            ))
            .await?;

        assert_eq!(outcome, RetireExecutionOutcome::Retired);
        let deletes = captured_deletes.lock().expect("capture mutex poisoned");
        let fenced_deletes: Vec<_> = deletes
            .iter()
            .filter(|(name, _)| name == &pod_name)
            .collect();
        // Exactly one. The tracked-pod loop deletes every recorded pod name
        // without preconditions, so a second pass over the fenced name would
        // hand the apiserver an unconditional delete for the object the
        // precondition just protected.
        let (_, options) = match fenced_deletes.as_slice() {
            [single] => single,
            other => panic!(
                "the fenced pod must be deleted exactly once, saw {}",
                other.len()
            ),
        };
        assert_eq!(
            options.pointer("/preconditions/uid"),
            Some(&serde_json::json!("uid-observed")),
            "the delete must carry the observed UID as a precondition"
        );
        assert_eq!(
            options.pointer("/preconditions/resourceVersion"),
            Some(&serde_json::json!("4242")),
            "a caller that observed a resourceVersion must have it enforced too"
        );
        Ok(())
    }

    async fn active_execution_generations(pool: &PgPool, sandbox_id: SandboxId) -> Vec<i64> {
        sqlx::query_scalar(
            "SELECT generation FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .fetch_all(pool)
        .await
        .expect("active execution generations must be queryable")
    }

    /// End to end: a retire whose fenced pod was replaced must tear down
    /// nothing. Not the pod (the caller never saw *this* pod), and not the pod
    /// tracking — releasing that marks the execution inactive, which drops the
    /// sandbox out of the failed-cleanup retry scan and hands the successor a
    /// control plane that no longer believes it exists.
    #[tokio::test(flavor = "current_thread")]
    async fn fenced_retire_refuses_a_replaced_pod_and_keeps_the_execution() -> Result<(), BoxError>
    {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        let substrate = fenced_retire_subject(
            &pool,
            sandbox_id,
            &pod_name,
            "uid-observed",
            "uid-successor",
        )
        .await?;

        let outcome = substrate
            .retire_execution(RetireExecutionRequest::fenced(
                sandbox_id,
                PodIdentityFence::new(PodName::parse(&pod_name)?)
                    .with_pod_uid(PodUid::parse("uid-observed")?),
            ))
            .await?;

        assert_eq!(
            outcome,
            RetireExecutionOutcome::PodIdentityChanged {
                mismatch: PodFenceMismatch::PodUidChanged,
                progress: TeardownProgress::NothingMutated,
            }
        );
        assert_eq!(
            active_execution_generations(&pool, sandbox_id).await,
            vec![1],
            "a refused retire must leave pod tracking alone"
        );
        Ok(())
    }

    /// The successor case, which no pod-level precondition can catch: the new
    /// generation's pod has a different *name*, so a UID precondition on the
    /// observed pod would pass while the teardown deleted the successor's pod
    /// alongside it. The control-plane half of the fence is what stops it.
    #[tokio::test(flavor = "current_thread")]
    async fn fenced_retire_refuses_when_a_newer_execution_took_over() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        let substrate =
            fenced_retire_subject(&pool, sandbox_id, &pod_name, "uid-observed", "uid-observed")
                .await?;

        // A wake takes over between the sweep's observation and its retire.
        sqlx::query("UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1")
            .bind(sandbox_id)
            .execute(&pool)
            .await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active)
             VALUES ($1, 2, $2, 'space', 'uid-successor', 'running', TRUE)",
        )
        .bind(sandbox_id)
        .bind(format!("space-sbx-{}-g2", sandbox_id.as_uuid().simple()))
        .execute(&pool)
        .await?;

        let outcome = substrate
            .retire_execution(RetireExecutionRequest::fenced(
                sandbox_id,
                PodIdentityFence::new(PodName::parse(&pod_name)?)
                    .with_pod_uid(PodUid::parse("uid-observed")?),
            ))
            .await?;

        assert_eq!(
            outcome,
            RetireExecutionOutcome::PodIdentityChanged {
                mismatch: PodFenceMismatch::ExecutionSuperseded,
                progress: TeardownProgress::NothingMutated,
            }
        );
        assert_eq!(
            active_execution_generations(&pool, sandbox_id).await,
            vec![2],
            "the successor execution must survive the refused retire"
        );
        Ok(())
    }

    fn observed(
        uid: Option<&str>,
        resource_version: Option<&str>,
        attempt: Option<SandboxActivationAttemptId>,
        generation: Option<i64>,
    ) -> ExecutionIdentity {
        ExecutionIdentity {
            pod_name: PodName::parse("space-sbx-fenced-g1").expect("pod name"),
            generation,
            activation_attempt_id: attempt,
            recorded: RecordedExecution::Read {
                pod_uid: uid.map(|uid| PodUid::parse(uid).expect("pod uid")),
                route_activation_id: None,
            },
            pod: match uid {
                Some(uid) => ObservedPod::Seen {
                    pod_uid: PodUid::parse(uid).expect("pod uid"),
                    resource_version: resource_version
                        .map(|v| PodResourceVersion::parse(v).expect("resource version")),
                    terminal: false,
                },
                None => ObservedPod::Unobserved,
            },
        }
    }

    /// **The test that makes the recurring bug class unshippable.**
    ///
    /// "Is this still the execution I observed?" used to be answered at six
    /// places that each picked their own subset of the identity, and every
    /// review finding against this fence was one of them omitting a field.
    /// This walks the identity field by field: change exactly one, and the
    /// single comparison must reject. A field that gets added to
    /// `ExecutionIdentity` and left out of the comparison already fails to
    /// compile (`reconcile` destructures exhaustively); this catches the
    /// weaker mistake of comparing it wrongly.
    #[test]
    fn every_field_of_the_identity_is_part_of_the_comparison() {
        let attempt = SandboxActivationAttemptId::new();
        let baseline = observed(Some("uid-observed"), Some("100"), Some(attempt), Some(7));
        assert_eq!(baseline.reconcile(&baseline), Ok(()), "identical must hold");

        let mut different_name = baseline.clone();
        different_name.pod_name = PodName::parse("space-sbx-other-g1").expect("pod name");
        let mut different_generation = baseline.clone();
        different_generation.generation = Some(8);
        let mut different_attempt = baseline.clone();
        different_attempt.activation_attempt_id = Some(SandboxActivationAttemptId::new());
        let cleared_attempt = ExecutionIdentity {
            activation_attempt_id: None,
            ..baseline.clone()
        };
        let different_uid = observed(Some("uid-replacement"), Some("100"), Some(attempt), Some(7));
        let different_version = observed(Some("uid-observed"), Some("101"), Some(attempt), Some(7));

        for (label, current, expected) in [
            (
                "pod name",
                different_name,
                PodFenceMismatch::ExecutionSuperseded,
            ),
            (
                "generation",
                different_generation,
                PodFenceMismatch::ExecutionSuperseded,
            ),
            (
                "activation attempt",
                different_attempt,
                PodFenceMismatch::ActivationAttemptChanged,
            ),
            (
                "activation attempt cleared to NULL",
                cleared_attempt,
                PodFenceMismatch::ActivationAttemptChanged,
            ),
            ("pod uid", different_uid, PodFenceMismatch::PodUidChanged),
            (
                "resource version",
                different_version,
                PodFenceMismatch::PodResourceVersionChanged,
            ),
        ] {
            assert_eq!(
                baseline.reconcile(&current),
                Err(expected),
                "changing the {label} alone must be rejected"
            );
        }
    }

    /// The two asymmetries in that comparison, which are deliberate and were
    /// each got wrong at least once before the rule lived in one place.
    #[test]
    fn absence_is_a_value_except_where_it_is_unobservable() {
        let attempt = SandboxActivationAttemptId::new();
        let seen = observed(Some("uid-observed"), Some("100"), Some(attempt), Some(7));

        // A pod that is gone holds: there is nothing left to protect, the
        // delete would 404, and refusing would strand every row whose pod is
        // genuinely absent.
        let gone = ExecutionIdentity {
            pod: ObservedPod::Absent,
            ..seen.clone()
        };
        assert_eq!(seen.reconcile(&gone), Ok(()));

        // A caller that never saw the pod can prove nothing about a live one.
        let never_saw = ExecutionIdentity {
            pod: ObservedPod::Unobserved,
            ..seen.clone()
        };
        assert_eq!(
            never_saw.reconcile(&seen),
            Err(PodFenceMismatch::PodIdentityUnverified)
        );
        assert_eq!(never_saw.reconcile(&gone), Ok(()));

        // A resourceVersion the caller never observed constrains nothing — it
        // is only available to a caller that read the object.
        let no_version = observed(Some("uid-observed"), None, Some(attempt), Some(7));
        assert_eq!(no_version.reconcile(&seen), Ok(()));

        // A generation the caller never recorded likewise constrains nothing,
        // but one it did record must match.
        let no_generation = observed(Some("uid-observed"), Some("100"), Some(attempt), None);
        assert_eq!(no_generation.reconcile(&seen), Ok(()));
    }

    /// A fenced retire that finds no active execution has had its goal met by
    /// someone else — another teardown, a suspend finalizer — so it succeeded.
    /// Reporting supersession there would make a compensating caller undo a
    /// lifecycle transition that was correct, which is the opposite of what
    /// the fence is for.
    #[tokio::test(flavor = "current_thread")]
    async fn a_retire_with_nothing_left_to_retire_succeeded() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        insert_test_sandbox_row(&pool, sandbox_id, "restoring").await?;
        // No *active* execution: the retire ran, or someone else's did. The
        // inactive row it left behind still names a pod whose delete may have
        // failed, and no other pass selects it.
        insert_k8s_execution(&pool, sandbox_id, 1, &pod_name, "uid-observed", None, false).await?;
        let captured_deletes = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            delete_capturing_kubernetes_client(Arc::clone(&captured_deletes), None),
            DEFAULT_SANDBOX_PROXY_PORT,
            None,
        );

        let fence = RetireFence::Pod(
            PodIdentityFence::new(PodName::parse(&pod_name)?)
                .with_pod_uid(PodUid::parse("uid-observed")?),
        );
        assert_eq!(
            substrate
                .retire_execution(RetireExecutionRequest {
                    sandbox_id,
                    fence: fence.clone(),
                })
                .await?,
            RetireExecutionOutcome::Retired
        );
        let deleted = captured_deletes.lock().unwrap().clone();
        assert!(
            deleted.iter().any(|(path, _)| path.contains(&pod_name)),
            "reaching this through the fence must collect the same orphans the \
             no-execution fence does, got {deleted:?}"
        );
        let placement: String = sqlx::query_scalar("SELECT placement FROM sandboxes WHERE id = $1")
            .bind(sandbox_id)
            .fetch_one(&pool)
            .await?;
        assert_eq!(
            placement, "unplaced",
            "and must clear the placement too — a revert that left this behind would leave a \
             dormant row still holding a node and a pool slot"
        );
        assert_eq!(
            substrate.retire_fence_mismatch(sandbox_id, &fence).await?,
            None,
            "and a caller asking first must not be told to defer"
        );
        Ok(())
    }

    /// End to end for the same case: a terminal pod the reflector cannot show,
    /// reached through a name-only fence, must be deleted under the UID its row
    /// records — not refused. Refusing leaves the pod, its execution row and
    /// its placement behind on every subsequent sweep.
    #[tokio::test(flavor = "current_thread")]
    async fn a_terminal_pod_behind_a_name_only_fence_is_retired() -> Result<(), BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
        insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
        insert_k8s_execution(&pool, sandbox_id, 1, &pod_name, "uid-terminal", None, true).await?;

        let terminal = Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(pod_name.clone()),
                namespace: Some("space".to_string()),
                uid: Some("uid-terminal".to_string()),
                ..Default::default()
            },
            status: Some(k8s_openapi::api::core::v1::PodStatus {
                phase: Some("Failed".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let captured_deletes = Arc::new(Mutex::new(Vec::new()));
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            delete_capturing_kubernetes_client(Arc::clone(&captured_deletes), Some(terminal)),
            DEFAULT_SANDBOX_PROXY_PORT,
            // Not in the reflector: that is the whole point.
            None,
        );

        let outcome = substrate
            .retire_execution(RetireExecutionRequest::fenced(
                sandbox_id,
                // Exactly what the sweeper's candidate select produces for a
                // pod the reflector cannot see: no observation, but the row's
                // own claim about which pod is its.
                PodIdentityFence::new(PodName::parse(&pod_name)?).with_recorded(
                    RecordedExecution::Read {
                        pod_uid: PodUid::parse("uid-terminal").ok(),
                        route_activation_id: None,
                    },
                ),
            ))
            .await?;

        assert_eq!(outcome, RetireExecutionOutcome::Retired);
        let deleted = captured_deletes.lock().unwrap().clone();
        let precondition = deleted
            .iter()
            .find(|(path, _)| path.contains(&pod_name))
            .map(|(_, body)| body.clone())
            .expect("the terminal pod must be deleted");
        assert_eq!(
            precondition["preconditions"]["uid"], "uid-terminal",
            "and under the UID its row records, not unconditionally"
        );
        assert!(
            active_execution_generations(&pool, sandbox_id)
                .await
                .is_empty(),
            "its execution row must be retired too"
        );
        Ok(())
    }

    /// The filtered reflector drops terminal pods, so no caller can ever
    /// observe one and every terminal pod arrives behind a name-only fence.
    /// Refusing those would defer their execution, placement and pod forever.
    /// A terminal pod cannot recover, so the UID its own row records is a
    /// complete identification — which is exactly what a *live* pod's UID is
    /// not, because Pending going Running keeps it.
    #[test]
    fn a_terminal_pod_is_identified_by_the_row_that_records_it() {
        let unobserved = ExecutionIdentity {
            recorded: RecordedExecution::Read {
                pod_uid: PodUid::parse("uid-a").ok(),
                route_activation_id: None,
            },
            pod: ObservedPod::Unobserved,
            ..observed(None, None, None, Some(7))
        };
        let live = |terminal: bool, uid: &str| ExecutionIdentity {
            pod: ObservedPod::Seen {
                pod_uid: PodUid::parse(uid).expect("pod uid"),
                resource_version: PodResourceVersion::parse("55").ok(),
                terminal,
            },
            recorded: RecordedExecution::Read {
                pod_uid: PodUid::parse("uid-a").ok(),
                route_activation_id: None,
            },
            ..observed(None, None, None, Some(7))
        };

        assert_eq!(
            unobserved.reconcile(&live(true, "uid-a")),
            Ok(()),
            "a terminal pod its own row claims is fully identified"
        );
        assert_eq!(
            unobserved.reconcile(&live(false, "uid-a")),
            Err(PodFenceMismatch::PodIdentityUnverified),
            "a running pod may be the recovery this sweep must not destroy"
        );
    }

    /// The apiserver returns one 409 whether the observed pod moved or a
    /// different pod took its name, and those call for opposite decisions from
    /// a caller that has already stopped a runtime: the first says it stopped
    /// the live sandbox, the second says it stopped a predecessor. Picking a
    /// default is wrong half the time, so the 409 is resolved by reading the
    /// object.
    #[tokio::test(flavor = "current_thread")]
    async fn a_rejected_delete_precondition_is_resolved_by_reading_the_pod() -> Result<(), BoxError>
    {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();

        for (uid_after_409, expected) in [
            ("uid-observed", PodFenceMismatch::PodResourceVersionChanged),
            ("uid-replacement", PodFenceMismatch::PodUidChanged),
        ] {
            let sandbox_id = SandboxId::new();
            let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
            insert_test_sandbox_row(&pool, sandbox_id, "failed").await?;
            insert_k8s_execution(&pool, sandbox_id, 1, &pod_name, "uid-observed", None, true)
                .await?;

            // The fence's own read sees the pod it observed, so the check
            // passes and the teardown reaches the delete. The read *after* the
            // 409 sees whatever the race left behind.
            let deleted = Arc::new(AtomicBool::new(false));
            let served_name = pod_name.clone();
            let service = tower::service_fn(move |req: Request<Body>| {
                let deleted = Arc::clone(&deleted);
                let served_name = served_name.clone();
                async move {
                    if req.method() == Method::DELETE {
                        deleted.store(true, Ordering::SeqCst);
                        let conflict = serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "reason": "Conflict",
                            "code": 409
                        });
                        return Ok::<_, BoxError>(
                            Response::builder()
                                .status(StatusCode::CONFLICT)
                                .header("content-type", "application/json")
                                .body(Body::from(serde_json::to_vec(&conflict)?))?,
                        );
                    }
                    let uid = if deleted.load(Ordering::SeqCst) {
                        uid_after_409
                    } else {
                        "uid-observed"
                    };
                    let pod = serde_json::json!({
                        "kind": "Pod",
                        "apiVersion": "v1",
                        "metadata": {
                            "name": served_name,
                            "namespace": "space",
                            "uid": uid,
                            "resourceVersion": "8000"
                        }
                    });
                    Ok::<_, BoxError>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .header("content-type", "application/json")
                            .body(Body::from(serde_json::to_vec(&pod)?))?,
                    )
                }
            });
            let substrate = substrate_with_store_client_proxy_port_and_pod(
                pool.clone(),
                ClientBuilder::new(service, "space").build(),
                DEFAULT_SANDBOX_PROXY_PORT,
                None,
            );

            let outcome = substrate
                .retire_execution(RetireExecutionRequest::fenced(
                    sandbox_id,
                    PodIdentityFence::new(PodName::parse(&pod_name)?)
                        .with_pod_uid(PodUid::parse("uid-observed")?)
                        .with_resource_version(PodResourceVersion::parse("8000")?),
                ))
                .await?;

            let RetireExecutionOutcome::PodIdentityChanged { mismatch, .. } = outcome else {
                panic!("a rejected precondition must refuse, got {outcome:?}");
            };
            assert_eq!(
                mismatch, expected,
                "a 409 whose pod then reads as {uid_after_409} must resolve to {expected}"
            );
        }
        Ok(())
    }

    /// Three sweeper passes ask "may I undo my own transition?", so the answer
    /// lives in one place. Both facts have to be wrong before an undo is
    /// refused: something destructive must already have happened, *and* what
    /// owns the sandbox now must be what this teardown reached. A successful
    /// pod delete counts as destructive — reporting otherwise would let a
    /// caller restore a lifecycle for a sandbox whose pod is gone.
    #[test]
    fn one_place_answers_whether_a_caller_may_undo() {
        let refused =
            |progress, mismatch| RetireExecutionOutcome::PodIdentityChanged { mismatch, progress };

        // A retire that worked is not an undo case at all.
        assert!(!RetireExecutionOutcome::Retired.permits_undo());

        // Nothing done yet: always safe.
        for mismatch in [
            PodFenceMismatch::PodResourceVersionChanged,
            PodFenceMismatch::ExecutionSuperseded,
        ] {
            assert!(refused(TeardownProgress::NothingMutated, mismatch).permits_undo());
        }

        // Something done, and it reached what is still live: undoing would
        // describe a sandbox that no longer matches the row.
        for progress in [
            TeardownProgress::PodDeleted,
            TeardownProgress::RuntimeStopped,
        ] {
            assert!(
                !refused(progress, PodFenceMismatch::PodResourceVersionChanged).permits_undo(),
                "{progress:?} reached the same pod the caller observed"
            );
            // Something done, but a successor owns the sandbox now and this
            // teardown never touched it.
            assert!(
                refused(progress, PodFenceMismatch::ExecutionSuperseded).permits_undo(),
                "{progress:?} says nothing about an execution that took over since"
            );
        }
    }

    /// Only one mismatch proves the in-VM stop reached what is live now: the
    /// same pod object at a later resourceVersion. Everything else may be
    /// something the stop never touched, and answering otherwise leaves a
    /// dormant or terminal lifecycle over a running sandbox. The default is
    /// therefore "a successor may own this", and each exception has to earn
    /// itself — this pins which one has.
    #[test]
    fn only_the_same_pod_moving_proves_the_stop_reached_what_is_live() {
        for mismatch in [
            PodFenceMismatch::ExecutionSuperseded,
            PodFenceMismatch::ExecutionMissing,
            PodFenceMismatch::PodUidChanged,
            PodFenceMismatch::ActivationAttemptChanged,
            PodFenceMismatch::DeletePreconditionRejected,
            PodFenceMismatch::PodIdentityUnverified,
        ] {
            assert!(
                mismatch.successor_took_over(),
                "{mismatch} may describe something the stop never reached, so a caller must \
                 undo its own transition rather than strand a live sandbox"
            );
        }
        assert!(
            !PodFenceMismatch::PodResourceVersionChanged.successor_took_over(),
            "the same pod at a later resourceVersion is the object the stop reached"
        );
    }

    /// Completing an activation clears the ephemeral attempt id back to NULL
    /// and leaves the durable one behind. Two activations that have both
    /// completed therefore agree on every other field — same generation, same
    /// pod name, no attempt on either side, and a wake need not change the
    /// pod's resourceVersion — so the durable id is the only thing left that
    /// says the execution has a different owner than the caller observed.
    #[test]
    fn a_completed_replacement_activation_is_a_different_owner() {
        let route = SandboxActivationAttemptId::new();
        let successor_route = SandboxActivationAttemptId::new();
        let with_route = |route: SandboxActivationAttemptId| ExecutionIdentity {
            recorded: RecordedExecution::Read {
                pod_uid: PodUid::parse("uid-a").ok(),
                route_activation_id: Some(route),
            },
            ..observed(Some("uid-a"), Some("100"), None, Some(7))
        };

        assert_eq!(
            with_route(route).reconcile(&with_route(successor_route)),
            Err(PodFenceMismatch::ActivationAttemptChanged)
        );
        assert_eq!(with_route(route).reconcile(&with_route(route)), Ok(()));
    }

    /// A replacement claim clears the execution row's UID and moves it to
    /// `creating` while keeping the generation, pod name and activation
    /// attempt — and before its pod exists there is no live object left to
    /// compare either. Every other field still matches, so the row's own
    /// record of which pod is its is the only thing that can notice, which is
    /// why it is compared rather than merely bound into the SQL guards.
    #[test]
    fn a_re_claimed_row_is_caught_even_with_no_pod_left_to_compare() {
        let attempt = SandboxActivationAttemptId::new();
        let seen = observed(Some("uid-a"), Some("100"), Some(attempt), Some(7));

        let re_claimed = ExecutionIdentity {
            recorded: RecordedExecution::Read {
                pod_uid: None,
                route_activation_id: None,
            },
            pod: ObservedPod::Absent,
            ..seen.clone()
        };
        assert_eq!(
            seen.reconcile(&re_claimed),
            Err(PodFenceMismatch::PodUidChanged),
            "a row that dropped the UID the caller selected has been re-claimed"
        );

        // The successor's own pod having come and gone is the same story told
        // later, and must not read as an ordinary absent-pod cleanup.
        let replaced = ExecutionIdentity {
            recorded: RecordedExecution::Read {
                pod_uid: PodUid::parse("uid-b").ok(),
                route_activation_id: None,
            },
            pod: ObservedPod::Absent,
            ..seen.clone()
        };
        assert_eq!(
            seen.reconcile(&replaced),
            Err(PodFenceMismatch::PodUidChanged)
        );

        // A row that still claims the pod the caller selected, whose pod is
        // simply gone, is the ordinary cleanup this fence must not block.
        let gone = ExecutionIdentity {
            pod: ObservedPod::Absent,
            ..seen
        };
        assert_eq!(
            observed(Some("uid-a"), Some("100"), Some(attempt), Some(7)).reconcile(&gone),
            Ok(())
        );
    }

    /// A fenced delete always carries the UID as a precondition, and the
    /// resourceVersion too when the caller observed one. The UID is a required
    /// argument rather than an `Option` so that "delete this pod with no
    /// identity constraint" has no spelling here — that request belongs to the
    /// unfenced path, and a name-only fence must not delete at all.
    #[test]
    fn a_fenced_delete_always_preconditions_on_the_uid() {
        let uid = PodUid::parse("uid-observed").expect("pod uid");

        let uid_only = pod_delete_params(&uid, None);
        let preconditions = uid_only
            .preconditions
            .expect("a UID must become a precondition");
        assert_eq!(preconditions.uid.as_deref(), Some("uid-observed"));
        assert_eq!(preconditions.resource_version, None);

        let version = PodResourceVersion::parse("4242").expect("resource version");
        let both = pod_delete_params(&uid, Some(&version));
        let preconditions = both.preconditions.expect("both must survive");
        assert_eq!(preconditions.uid.as_deref(), Some("uid-observed"));
        assert_eq!(preconditions.resource_version.as_deref(), Some("4242"));
    }

    /// The apiserver's two expected answers to a preconditioned delete. 404 is
    /// benign — someone else already removed the pod. 409 is the precondition
    /// being rejected, and must never read as a completed delete: the caller
    /// stops before releasing pod tracking. Anything else is a real error and
    /// propagates.
    #[test]
    fn fenced_pod_delete_maps_404_benign_and_409_to_precondition_failure() {
        let api_error = |code: u16| {
            kube::Error::Api(Box::new(kube::core::Status {
                code,
                message: "pod".to_string(),
                reason: "Conflict".to_string(),
                ..Default::default()
            }))
        };

        assert_eq!(
            classify_fenced_pod_delete_error(&api_error(404)),
            Some(FencedPodDeleteOutcome::NotFound)
        );
        assert_eq!(
            classify_fenced_pod_delete_error(&api_error(409)),
            Some(FencedPodDeleteOutcome::PreconditionFailed)
        );
        assert_eq!(classify_fenced_pod_delete_error(&api_error(500)), None);
    }
}
