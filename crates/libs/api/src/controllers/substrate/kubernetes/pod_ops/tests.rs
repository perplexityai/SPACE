use std::ffi::OsStr;

use pplx_space_common_grpc::internal_transport::{CA_ENV, INSECURE_DEV_ENV};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Identity, Server, ServerTlsConfig};

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId};

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use k8s_openapi::api::core::v1::PodStatus;
use kube::error::Status;
use pplx_space_common_core::kubernetes::{
    SANDBOX_EXECUTION_GENERATION_ANNOTATION, SANDBOX_ID_ANNOTATION,
};

use super::*;

#[test]
fn activation_assignment_retains_the_database_canonical_id_across_clones() {
    let requested_id = SandboxActivationAttemptId::from_uuid(
        Uuid::parse_str("019c0000-0000-7000-8000-000000000001").unwrap(),
    );
    let assigned_id = SandboxActivationAttemptId::from_uuid(
        Uuid::parse_str("019c0000-0000-7000-8000-000000000002").unwrap(),
    );
    let assignment =
        ActivationAttemptAssignment::new(requested_id, SandboxTransitionTargetState::Started);

    assignment.clone().record_assigned(assigned_id, 1);

    assert_eq!(assignment.assigned_id(), Some(assigned_id));
    assert_eq!(assignment.assigned(), Some((assigned_id, 1)));
    assert_eq!(assignment.assigned_or_requested(), assigned_id);

    let replacement_id = SandboxActivationAttemptId::new_after(assigned_id)
        .expect("test activation identity has a successor");
    assignment.clone().record_assigned(replacement_id, 2);
    assert_eq!(assignment.assigned_id(), Some(replacement_id));
    assert_eq!(assignment.assigned(), Some((replacement_id, 2)));
    assert_eq!(assignment.assigned_or_requested(), replacement_id);
}

#[test]
fn lingering_sandbox_pod_names_selects_only_strictly_older_generations() {
    let sandbox_id = SandboxId::new();
    let other_sandbox = Uuid::now_v7();
    let annotated = |name: &str, id: SandboxId, generation: Option<&str>| {
        let mut annotations =
            std::collections::BTreeMap::from([(SANDBOX_ID_ANNOTATION.to_string(), id.to_string())]);
        if let Some(generation) = generation {
            annotations.insert(
                SANDBOX_EXECUTION_GENERATION_ANNOTATION.to_string(),
                generation.to_string(),
            );
        }
        SandboxPodView::from(Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(name.to_string()),
                annotations: Some(annotations),
                ..Default::default()
            },
            ..Default::default()
        })
    };

    let ours_current = annotated("space-sbx-x-g3", sandbox_id, Some("3"));
    let ours_stale = annotated("space-sbx-x-g2", sandbox_id, Some("2"));
    // A concurrent request superseded this claim: its newer pod must
    // survive the sweep (a name-only exclusion would delete it).
    let ours_newer = annotated("space-sbx-x-g4", sandbox_id, Some("4"));
    // No parseable generation: conservatively skipped.
    let ours_ungenerationed = annotated("space-sbx-x-mystery", sandbox_id, None);
    let foreign = annotated(
        "space-sbx-y-g1",
        SandboxId::from_uuid(other_sandbox),
        Some("1"),
    );

    let lingering = lingering_sandbox_pod_names(
        [
            &ours_current,
            &ours_stale,
            &ours_newer,
            &ours_ungenerationed,
            &foreign,
        ],
        sandbox_id,
        3,
    );

    assert_eq!(lingering, vec!["space-sbx-x-g2".to_string()]);
}

#[test]
fn classify_precondition_reread_covers_all_post_conflict_shapes() {
    use k8s_openapi::api::core::v1::{
        Affinity, NodeAffinity, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm,
        PodCondition, PodSpec, PodStatus,
    };

    let required_pin = || {
        Some(Affinity {
            node_affinity: Some(NodeAffinity {
                required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                    node_selector_terms: vec![NodeSelectorTerm {
                        match_fields: Some(vec![NodeSelectorRequirement {
                            key: "metadata.name".to_string(),
                            operator: "In".to_string(),
                            values: Some(vec!["source-node".to_string()]),
                        }]),
                        ..Default::default()
                    }],
                }),
                ..Default::default()
            }),
            ..Default::default()
        })
    };
    let unschedulable_status = || {
        Some(PodStatus {
            conditions: Some(vec![PodCondition {
                type_: "PodScheduled".to_string(),
                status: "False".to_string(),
                reason: Some("Unschedulable".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        })
    };
    let pod = |node_name: Option<&str>, pinned: bool, unschedulable: bool| Pod {
        metadata: kube::api::ObjectMeta {
            uid: Some("uid-2".to_string()),
            resource_version: Some("rv-2".to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            node_name: node_name.map(str::to_string),
            affinity: pinned.then(required_pin).flatten(),
            ..Default::default()
        }),
        status: unschedulable.then(unschedulable_status).flatten(),
    };

    // Absent -> fall through to the successor POST.
    assert_eq!(
        classify_precondition_reread(None, Some("source-node")),
        PreconditionReread::Absent
    );
    // Bound on the source -> retain, same-node.
    assert_eq!(
        classify_precondition_reread(
            Some(&SandboxPodView::from(pod(Some("source-node"), true, false))),
            Some("source-node")
        ),
        PreconditionReread::Retain { cross_node: false }
    );
    // Bound on a shortlist fallback -> retain, cross-node.
    assert_eq!(
        classify_precondition_reread(
            Some(&SandboxPodView::from(pod(Some("other-node"), true, false))),
            Some("source-node")
        ),
        PreconditionReread::Retain { cross_node: true }
    );
    // Unpinned, unbound -> concurrent cross-node successor, retain.
    assert_eq!(
        classify_precondition_reread(
            Some(&SandboxPodView::from(pod(None, false, false))),
            Some("source-node")
        ),
        PreconditionReread::Retain { cross_node: true }
    );
    // Still required-pinned + Unschedulable -> retry with fresh identity,
    // never retained (regression guard for the status-only RV bump).
    assert_eq!(
        classify_precondition_reread(
            Some(&SandboxPodView::from(pod(None, true, true))),
            Some("source-node")
        ),
        PreconditionReread::RetryDelete {
            uid: "uid-2".to_string(),
            resource_version: "rv-2".to_string(),
        }
    );
    // Still required-pinned but undecided -> also retry, never retained.
    assert_eq!(
        classify_precondition_reread(
            Some(&SandboxPodView::from(pod(None, true, false))),
            Some("source-node")
        ),
        PreconditionReread::RetryDelete {
            uid: "uid-2".to_string(),
            resource_version: "rv-2".to_string(),
        }
    );
    // Still required-pinned but the apiserver exposed no UID/RV to
    // precondition on -> Indeterminate, never Absent: deferring is
    // fail-closed, whereas treating it as Absent would POST a successor
    // past a pod that may still exist.
    let pinned_without_identity = |uid: Option<&str>, resource_version: Option<&str>| Pod {
        metadata: kube::api::ObjectMeta {
            uid: uid.map(str::to_string),
            resource_version: resource_version.map(str::to_string),
            ..Default::default()
        },
        spec: Some(PodSpec {
            affinity: required_pin(),
            ..Default::default()
        }),
        status: None,
    };
    assert_eq!(
        classify_precondition_reread(
            Some(&SandboxPodView::from(pinned_without_identity(
                Some("uid-2"),
                None
            ))),
            Some("source-node")
        ),
        PreconditionReread::Indeterminate,
        "a pinned, unbound pod missing its resourceVersion must defer, not POST a successor"
    );
    assert_eq!(
        classify_precondition_reread(
            Some(&SandboxPodView::from(pinned_without_identity(
                None,
                Some("rv-2")
            ))),
            Some("source-node")
        ),
        PreconditionReread::Indeterminate,
        "a pinned, unbound pod missing its UID must defer, not POST a successor"
    );
}

#[test]
fn restore_placement_sample_reflects_pin_shape() {
    use pplx_space_metrics::sandbox::restore::{RestorePlacement, SourcePin};

    // On-demand carries no placement decision.
    assert!(restore_placement_sample(&PodCreateContext::OnDemand, true).is_none());

    // Portable restore, hard-pin on -> required affinity, same node.
    let portable = PodCreateContext::Restore(RestoreContext::UserRestore {
        suspend_restore_storage_uri: Some("s3://bucket/key"),
        suspend_restore_source_node: Some("source-node"),
        direct_node_name: None,
        preferred_node_name: Some("source-node"),
    });
    let required = restore_placement_sample(&portable, true).unwrap();
    assert_eq!(required.placement, RestorePlacement::SameNode);
    assert_eq!(required.pin, SourcePin::RequiredAffinity);
    assert!(required.local_snapshot_present);

    // Same inputs, hard-pin off -> preferred affinity.
    let preferred = restore_placement_sample(&portable, false).unwrap();
    assert_eq!(preferred.pin, SourcePin::PreferredAffinity);
    assert_eq!(preferred.placement, RestorePlacement::SameNode);

    // Local-only restore (no S3) -> spec.nodeName pin.
    let local = PodCreateContext::Restore(RestoreContext::UserRestore {
        suspend_restore_storage_uri: None,
        suspend_restore_source_node: Some("source-node"),
        direct_node_name: Some("source-node"),
        preferred_node_name: None,
    });
    let local_sample = restore_placement_sample(&local, true).unwrap();
    assert_eq!(local_sample.pin, SourcePin::NodeName);
    assert_eq!(local_sample.placement, RestorePlacement::SameNode);

    // Storage-only restore (source unknown) -> cross-node, no pin.
    let storage_only = PodCreateContext::Restore(RestoreContext::UserRestore {
        suspend_restore_storage_uri: Some("s3://bucket/key"),
        suspend_restore_source_node: None,
        direct_node_name: None,
        preferred_node_name: None,
    });
    let cross = restore_placement_sample(&storage_only, true).unwrap();
    assert_eq!(cross.placement, RestorePlacement::CrossNode);
    assert_eq!(cross.pin, SourcePin::None);
    assert!(!cross.local_snapshot_present);

    // Evacuation always drops the pin -> cross-node.
    let evac = PodCreateContext::Restore(RestoreContext::Evacuation {
        suspend_restore_storage_uri: Some("s3://bucket/key"),
        extra_tolerations: &[],
        extra_required_affinity: None,
    });
    let evac_sample = restore_placement_sample(&evac, true).unwrap();
    assert_eq!(evac_sample.placement, RestorePlacement::CrossNode);
    assert_eq!(evac_sample.pin, SourcePin::None);
}

fn pod_with_phase(phase: &str) -> Pod {
    Pod {
        status: Some(PodStatus {
            phase: Some(phase.to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn kube_api_error(code: u16) -> KubeError {
    KubeError::Api(
        Status::failure("synthetic kube API error", "Synthetic")
            .with_code(code)
            .boxed(),
    )
}

fn routable_pod(sandbox_id: SandboxId, generation: i64) -> Pod {
    let mut annotations = std::collections::BTreeMap::new();
    annotations.insert(SANDBOX_ID_ANNOTATION.to_string(), sandbox_id.to_string());
    annotations.insert(
        SANDBOX_EXECUTION_GENERATION_ANNOTATION.to_string(),
        generation.to_string(),
    );
    Pod {
        metadata: kube::api::ObjectMeta {
            annotations: Some(annotations),
            uid: Some("pod-uid-1".to_string()),
            ..Default::default()
        },
        status: Some(PodStatus {
            phase: Some("Running".to_string()),
            pod_ip: Some("10.90.0.9".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn cancelled_materialization_cleanup_requires_the_retired_execution_identity() {
    use pplx_space_common_core::{
        kubernetes::SANDBOX_MATERIALIZATION_ID_ANNOTATION, types::PodMaterializationId,
    };
    let sandbox = SandboxId::new();
    let token = PodMaterializationId::new();
    for expected in [None, Some(token)] {
        let mut pod = routable_pod(sandbox, 3);
        pod.metadata.name = Some("sandbox-generation-3".into());
        if let Some(token) = expected {
            pod.metadata.annotations.as_mut().unwrap().insert(
                SANDBOX_MATERIALIZATION_ID_ANNOTATION.into(),
                token.to_string(),
            );
        }
        let pod = SandboxPodView::from(pod);
        for (owner, generation, name, materialization, allowed) in [
            (sandbox, 3, "sandbox-generation-3", expected, true),
            (SandboxId::new(), 3, "sandbox-generation-3", expected, false),
            (sandbox, 4, "sandbox-generation-3", expected, false),
            (sandbox, 3, "different-name", expected, false),
            (
                sandbox,
                3,
                "sandbox-generation-3",
                Some(PodMaterializationId::new()),
                false,
            ),
        ] {
            assert_eq!(
                delete::retired_materialization_matches(
                    &pod,
                    owner,
                    generation,
                    name,
                    materialization
                )
                .unwrap(),
                allowed
            );
        }
    }
}

/// The full acceptance path: matching annotations, Running, not deleting,
/// IP present -> the identity the execution row is hydrated with.
#[test]
fn restored_pod_routability_accepts_matching_running_pod() {
    let sandbox_id = SandboxId::new();
    let identity = restored_pod_routability(
        &SandboxPodView::from(routable_pod(sandbox_id, 2)),
        sandbox_id,
        2,
    )
    .expect("matching Running pod with IP is routable");
    assert_eq!(identity.pod_ip, "10.90.0.9");
    assert_eq!(identity.pod_uid.as_deref(), Some("pod-uid-1"));
}

/// Every single-field violation must reject: the hydration write is
/// truth-copying, so any mismatch means there is no truth to copy.
#[test]
fn restored_pod_routability_rejects_every_mismatch() {
    let sandbox_id = SandboxId::new();

    // Wrong sandbox id (a name collision must never vouch).
    assert!(
        restored_pod_routability(
            &SandboxPodView::from(routable_pod(SandboxId::new(), 2)),
            sandbox_id,
            2
        )
        .is_none()
    );

    // Stale prior-generation pod.
    assert!(
        restored_pod_routability(
            &SandboxPodView::from(routable_pod(sandbox_id, 1)),
            sandbox_id,
            2
        )
        .is_none()
    );

    // Not Running.
    let mut pending = routable_pod(sandbox_id, 2);
    pending.status.as_mut().unwrap().phase = Some("Pending".to_string());
    assert!(
        restored_pod_routability(&SandboxPodView::from(pending.clone()), sandbox_id, 2).is_none()
    );

    // Being deleted.
    let mut deleting = routable_pod(sandbox_id, 2);
    deleting.metadata.deletion_timestamp =
        Some(serde_json::from_str("\"2026-07-14T00:00:00Z\"").unwrap());
    assert!(
        restored_pod_routability(&SandboxPodView::from(deleting.clone()), sandbox_id, 2).is_none()
    );

    // No IP yet.
    let mut no_ip = routable_pod(sandbox_id, 2);
    no_ip.status.as_mut().unwrap().pod_ip = None;
    assert!(
        restored_pod_routability(&SandboxPodView::from(no_ip.clone()), sandbox_id, 2).is_none()
    );

    // No annotations at all.
    let mut bare = routable_pod(sandbox_id, 2);
    bare.metadata.annotations = None;
    assert!(restored_pod_routability(&SandboxPodView::from(bare.clone()), sandbox_id, 2).is_none());

    // Unparseable generation annotation.
    let mut garbled = routable_pod(sandbox_id, 2);
    garbled.metadata.annotations.as_mut().unwrap().insert(
        SANDBOX_EXECUTION_GENERATION_ANNOTATION.to_string(),
        "x".to_string(),
    );
    assert!(
        restored_pod_routability(&SandboxPodView::from(garbled.clone()), sandbox_id, 2).is_none()
    );
}

#[test]
fn user_restore_without_ready_remote_snapshot_keeps_source_node_local_path() {
    match parse_pod_create_context(
        SandboxId::new(),
        2,
        "restoring",
        true,
        None,
        Some("source-node"),
        None,
    ) {
        Ok(PodCreateContext::Restore(RestoreContext::UserRestore {
            suspend_restore_source_node,
            direct_node_name,
            preferred_node_name,
            ..
        })) => {
            assert_eq!(suspend_restore_source_node, Some("source-node"));
            assert_eq!(direct_node_name, Some("source-node"));
            assert_eq!(preferred_node_name, None);
        }
        _ => panic!("local-only restore must use its captured source node"),
    }
}

/// The pre-flight that keeps a local-only restore off a node the boundary
/// excludes runs only where `direct_node_name` reports a pre-bound node, so a
/// mapping that lost the local-restore arm would silently disable the check
/// while every placement test still passed. Pinned per context, alongside the
/// metric path each one books — though see the note on the portable-restore
/// case, whose path assertion restates its match arm rather than pinning it
/// independently.
#[test]
fn pre_bound_node_and_metric_path_are_reported_per_context() {
    let on_demand = PodCreateContext::OnDemand;
    assert_eq!(on_demand.direct_node_name(), None);
    assert_eq!(
        on_demand.node_group_resolve_path(),
        NodeGroupResolvePath::Materialize
    );

    // The only shape that pre-binds: a restore with no portable snapshot, so
    // its bytes exist on the source node and nowhere else.
    let local_restore = PodCreateContext::Restore(RestoreContext::UserRestore {
        suspend_restore_storage_uri: None,
        suspend_restore_source_node: Some("source-node"),
        direct_node_name: Some("source-node"),
        preferred_node_name: None,
    });
    assert_eq!(local_restore.direct_node_name(), Some("source-node"));
    assert_eq!(
        local_restore.node_group_resolve_path(),
        NodeGroupResolvePath::Restore
    );

    // A portable restore prefers its source and is placed by the scheduler
    // inside the candidate set, so there is nothing to pre-check.
    let portable_restore = PodCreateContext::Restore(RestoreContext::UserRestore {
        suspend_restore_storage_uri: Some("s3://bucket/key"),
        suspend_restore_source_node: Some("source-node"),
        direct_node_name: None,
        preferred_node_name: Some("source-node"),
    });
    assert_eq!(portable_restore.direct_node_name(), None);
    // Both restore shapes book the same path: the pre-flight is what differs
    // between them, not the resolve. Not an independent pin — the mapping
    // matches `UserRestore { .. }` without reading a field, so this cannot fail
    // while the assertion above passes. It is here to fail loudly if that arm
    // is ever split per shape.
    assert_eq!(
        portable_restore.node_group_resolve_path(),
        NodeGroupResolvePath::Restore
    );

    let evacuation = PodCreateContext::Restore(RestoreContext::Evacuation {
        suspend_restore_storage_uri: Some("s3://bucket/key"),
        extra_tolerations: &[],
        extra_required_affinity: None,
    });
    assert_eq!(evacuation.direct_node_name(), None);
    assert_eq!(
        evacuation.node_group_resolve_path(),
        NodeGroupResolvePath::Evacuation
    );
}

#[test]
fn created_sandbox_without_preferred_node_renders_on_demand_pod() {
    match parse_pod_create_context(SandboxId::new(), 1, "created", false, None, None, None) {
        Ok(PodCreateContext::OnDemand) => {}
        Ok(PodCreateContext::Restore(_)) => {
            panic!("created sandbox must not carry restore pod intent")
        }
        Err(err) => panic!("created sandbox pod rendering failed: {err}"),
    }
}

#[test]
fn terminal_cleanup_requires_uncached_terminal_or_missing_pod() {
    let running = pod_with_phase("Running");
    let failed = pod_with_phase("Failed");

    assert_eq!(
        terminal_cleanup_phase_after_uncached_check(
            TerminalPodPhase::Succeeded,
            Some(&SandboxPodView::from(running.clone())),
        ),
        None,
        "a stale cached terminal phase must not clear tracking for an authoritative running pod"
    );
    assert_eq!(
        terminal_cleanup_phase_after_uncached_check(
            TerminalPodPhase::Succeeded,
            Some(&SandboxPodView::from(failed.clone()))
        ),
        Some(TerminalPodPhase::Failed),
        "authoritative terminal truth should drive cleanup/logging"
    );
    assert_eq!(
        terminal_cleanup_phase_after_uncached_check(TerminalPodPhase::Succeeded, None),
        Some(TerminalPodPhase::Succeeded),
        "an authoritatively missing pod can safely clear stale tracking"
    );
}

#[test]
fn successful_terminal_publication_grace_is_bounded() {
    let pod: Pod = serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "sandbox", "namespace": "space" },
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
                        "finishedAt": "2026-08-06T00:00:00Z"
                    }
                }
            }]
        }
    }))
    .expect("test pod must decode");
    let within_grace = "2026-08-06T00:02:00Z"
        .parse::<jiff::Timestamp>()
        .expect("test time must parse");
    let after_grace = "2026-08-06T00:03:01Z"
        .parse::<jiff::Timestamp>()
        .expect("test time must parse");

    assert_eq!(
        succeeded_pod_publication_grace_remaining(&SandboxPodView::from(pod.clone()), within_grace),
        Some(Duration::from_secs(60))
    );
    assert_eq!(
        succeeded_pod_publication_grace_remaining(&SandboxPodView::from(pod.clone()), after_grace),
        None,
        "an old successful pod must fall through to genuine missing-source recovery"
    );
}

#[test]
fn kube_api_throttling_maps_to_retryable_substrate_timeout() {
    let err = kube_error_for_substrate("failed to verify cached sandbox pod", kube_api_error(429));

    assert!(
        matches!(
            err,
            SubstrateError::Timeout {
                ref message,
                phase: None
            } if message.contains("Kubernetes API throttling")
        ),
        "Kube 429 must stay retryable instead of becoming opaque 500: {err:?}"
    );
}

#[test]
fn kube_api_not_found_is_not_throttling() {
    assert!(!is_kube_api_throttling_error(&kube_api_error(404)));
}

#[test]
fn non_transient_kube_api_error_maps_to_failed_substrate_error() {
    let err = kube_error_for_substrate("failed to query K8s pod", kube_api_error(422));

    assert!(
        matches!(err, SubstrateError::Failed { .. }),
        "non-transient kube errors should still surface as substrate failures: {err:?}"
    );
}

#[test]
fn kube_api_server_error_maps_to_retryable_substrate_timeout() {
    let err = kube_error_for_substrate("failed to resolve selected Wake pod", kube_api_error(503));

    assert!(
        matches!(
            err,
            SubstrateError::Timeout {
                ref message,
                phase: None
            } if message.contains("transient Kubernetes API failure")
        ),
        "Kube 5xx must retry through the lifecycle deadline: {err:?}"
    );
}

#[tokio::test]
async fn delete_throttling_retry_retries_429_until_success() {
    let calls = Arc::new(AtomicUsize::new(0));

    let result = retry_kube_delete_throttling("test_delete", || {
        let calls = Arc::clone(&calls);
        async move {
            let prior = calls.fetch_add(1, Ordering::SeqCst);
            if prior + 1 < K8S_THROTTLE_DELETE_RETRY_ATTEMPTS {
                Err(kube_api_error(429))
            } else {
                Ok(())
            }
        }
    })
    .await;

    result.unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        K8S_THROTTLE_DELETE_RETRY_ATTEMPTS
    );
}

#[tokio::test]
async fn delete_throttling_retry_stops_after_bounded_429_budget() {
    let calls = Arc::new(AtomicUsize::new(0));

    let result = retry_kube_delete_throttling("test_delete", || {
        let calls = Arc::clone(&calls);
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(kube_api_error(429))
        }
    })
    .await;

    assert!(matches!(result, Err(ref err) if is_kube_api_throttling_error(err)));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        K8S_THROTTLE_DELETE_RETRY_ATTEMPTS
    );
}

#[tokio::test]
async fn delete_throttling_retry_does_not_retry_non_429_errors() {
    let calls = Arc::new(AtomicUsize::new(0));

    let result = retry_kube_delete_throttling("test_delete", || {
        let calls = Arc::clone(&calls);
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(kube_api_error(422))
        }
    })
    .await;

    assert!(matches!(result, Err(ref err) if !is_kube_api_throttling_error(err)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// The `stopped` restore plan reads only the store, so the kube client here is
/// a capture-only mock that never receives a request.
mod stopped_restore_plan_tests {

    use super::*;
    use crate::controllers::sandbox::fetch_sandbox;
    use crate::controllers::substrate::kubernetes::tests::{
        mock_kubernetes_client, substrate_with_store_and_client,
    };
    use pplx_space_common_core::types::NodeRole;
    use pplx_space_common_core::types::{LocalArtifactId, SandboxId};
    use pplx_space_db_test_fixtures::{
        SEED_ORG_ID, SEED_USER_ID, insert_ready_worker_node, publish_ready_disk_artifact,
    };
    use pplx_space_test_env::TestResult;
    use sqlx::PgPool;
    use std::sync::Mutex;

    async fn insert_stopped_sandbox(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
        sqlx::query(
            "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, placement, managed_by,
                                    network_policy_override_json, network_policy_generation)
             VALUES ($1, $2, $3, 'stopped', 'unplaced', 'k8s', $4, 1)",
        )
        .bind(sandbox_id)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(sqlx::types::Json(
            &pplx_space_net_common::NetworkPolicyV1::default(),
        ))
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Give the sandbox a live `active_node_id` -> `node_k8s` mapping.
    /// `restore_source_node_name` resolves that through its placement branch,
    /// which reads `target_node_id.or(active_node_id)` — so this fixture takes
    /// the `active_node_id` half of it, with `target_node_id` left NULL.
    async fn place_on_node(pool: &PgPool, sandbox_id: SandboxId, node_name: &str) -> TestResult {
        let node_id = NodeId::new();
        insert_ready_worker_node(pool, node_id).await?;
        sqlx::query("INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)")
            .bind(node_id)
            .bind(node_name)
            .execute(pool)
            .await?;
        sqlx::query("UPDATE sandboxes SET active_node_id = $2 WHERE id = $1")
            .bind(sandbox_id)
            .bind(node_id)
            .execute(pool)
            .await?;
        Ok(())
    }

    /// The three plan shapes a stopped restart can resolve to, plus the one it
    /// must refuse. The operator-visible properties:
    ///
    /// * a stopped sandbox whose node still holds it restarts there;
    /// * once its rootfs is durable in S3 it can restart anywhere, and still
    ///   prefers the node that has the bytes locally;
    /// * with the node gone but the bytes durable it restarts elsewhere;
    /// * with neither, `start` is told to retry — not that the sandbox failed.
    ///
    /// Each case is one fixture mutation from a neighbour, so no assertion
    /// holds because the fixture was missing something unrelated.
    #[tokio::test(flavor = "current_thread")]
    async fn stopped_restore_plan_resolves_node_and_evacuated_artifact_independently() -> TestResult
    {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let substrate = substrate_with_store_and_client(
            pool.clone(),
            mock_kubernetes_client(Arc::new(Mutex::new(Vec::new()))),
        );
        let uri = "s3://space-test/stopped-plan-disk?region=us-east-1";

        // Neither: the evacuation window before the artifact is durable, and
        // the state a `start` hits when its node is already gone.
        let stranded = SandboxId::new();
        insert_stopped_sandbox(&pool, stranded).await?;
        let sandbox = fetch_sandbox(&pool, stranded)
            .await?
            .expect("stranded sandbox should exist");
        let Err(error) = substrate.stopped_restore_plan(&sandbox).await else {
            panic!("no source and no artifact must not resolve a plan");
        };
        let SubstrateError::Timeout { message, phase } = &error else {
            panic!("a stopped restart with nothing to restore from must stay a retryable 503");
        };
        // A guard on the constructor, not coverage of the mapping: `phase` is a
        // literal here, so no fixture state could make it `Some`. The
        // `Timeout` -> 503 + `Retry-After` translation itself lives in the
        // substrate error conversion and is not asserted by this suite.
        assert!(
            phase.is_none(),
            "the unphased Timeout variant is what the 503 + Retry-After mapping keys on"
        );
        // The variant is what produces the 503; the message is the only part
        // of this arm the change is visible in, so assert it names both
        // sources rather than only the node it used to name.
        assert!(
            message.contains("source node") && message.contains("disk artifact"),
            "the retry message must name both restore sources, got {message:?}"
        );

        // Node only: unchanged from before this selector existed.
        let node_only = SandboxId::new();
        insert_stopped_sandbox(&pool, node_only).await?;
        place_on_node(&pool, node_only, "holder-node").await?;
        let sandbox = fetch_sandbox(&pool, node_only)
            .await?
            .expect("node-only sandbox should exist");
        let plan = substrate.stopped_restore_plan(&sandbox).await?;
        assert_eq!(
            plan.source_node_name.as_deref(),
            Some("holder-node"),
            "a stopped sandbox whose node still holds it must restart there"
        );
        assert_eq!(
            plan.storage_uri, None,
            "with no published disk artifact the plan must not invent a remote source"
        );

        // Both: the node preference survives the artifact becoming durable.
        publish_ready_disk_artifact(&pool, node_only, LocalArtifactId::new(), uri).await?;
        let sandbox = fetch_sandbox(&pool, node_only)
            .await?
            .expect("node-only sandbox should still exist");
        let plan = substrate.stopped_restore_plan(&sandbox).await?;
        assert_eq!(
            plan.source_node_name.as_deref(),
            Some("holder-node"),
            "a durable artifact must not suppress the source-node preference"
        );
        assert_eq!(
            plan.storage_uri.as_deref(),
            Some(uri),
            "the durable artifact must reach the plan so a fallback node can restore from it"
        );

        // Artifact only. Reaching this shape takes a sandbox with *no*
        // surviving node history: `restore_source_node_name` falls back to the
        // newest `sandbox_k8s_executions` row whose `k8s_node_name` is set, and
        // that fallback has no liveness or age filter. This fixture seeds no
        // execution row at all, so it exercises the URI-only branch of the plan
        // — but it is deliberately *not* the shape a drained sandbox lands in;
        // see the next case for that one.
        let artifact_only = SandboxId::new();
        insert_stopped_sandbox(&pool, artifact_only).await?;
        publish_ready_disk_artifact(&pool, artifact_only, LocalArtifactId::new(), uri).await?;
        let sandbox = fetch_sandbox(&pool, artifact_only)
            .await?
            .expect("artifact-only sandbox should exist");
        let plan = substrate.stopped_restore_plan(&sandbox).await?;
        assert_eq!(
            plan.source_node_name, None,
            "with no node history at all there is no source node to prefer"
        );
        assert_eq!(
            plan.storage_uri.as_deref(),
            Some(uri),
            "the evacuated artifact must reach the plan as the sole restore source rather \
             than timing out"
        );

        // The post-evacuation shape, which is the one an evacuated stopped
        // sandbox actually presents: placement pointers cleared, but the
        // execution row still names the node it last ran on. Because that
        // fallback filters on nothing but `k8s_node_name IS NOT NULL`, the plan
        // resolves *both* — and the node it names may already be terminated.
        // Asserting it here so the shape is on record rather than discovered
        // by whichever change makes the URI half live.
        let evacuated = SandboxId::new();
        insert_stopped_sandbox(&pool, evacuated).await?;
        // Build the execution row through the store rather than by hand, so it
        // carries every column production sets, then clear the placement
        // pointers the way an evacuation does. What survives is the execution
        // row's `k8s_node_name`.
        let node_id = NodeId::new();
        insert_ready_worker_node(&pool, node_id).await?;
        substrate
            .store
            .ensure_active_execution_row(evacuated)
            .await?;
        substrate
            .store
            .persist_pod_node_placement(
                evacuated,
                "since-terminated-node",
                "since-terminated-node-uid",
                node_id,
                NodeRole::Worker,
                None,
            )
            .await?;
        sqlx::query(
            "UPDATE sandboxes SET active_node_id = NULL, target_node_id = NULL WHERE id = $1",
        )
        .bind(evacuated)
        .execute(&pool)
        .await?;
        publish_ready_disk_artifact(&pool, evacuated, LocalArtifactId::new(), uri).await?;
        let sandbox = fetch_sandbox(&pool, evacuated)
            .await?
            .expect("evacuated sandbox should exist");
        let plan = substrate.stopped_restore_plan(&sandbox).await?;
        assert_eq!(
            plan.source_node_name.as_deref(),
            Some("since-terminated-node"),
            "execution history outlives placement, so an evacuated sandbox still resolves a \
             source node — a dead one"
        );
        assert_eq!(
            plan.storage_uri.as_deref(),
            Some(uri),
            "and it must carry the artifact too, so a node that is not the dead one has \
             something to restore from"
        );

        Ok(())
    }
}

#[tokio::test]
async fn pod_worker_channel_verifies_tls_for_raw_pod_ips() {
    let temp = tempfile::tempdir().unwrap();
    let ca_path = temp.path().join("ca.pem");
    if !pplx_space_test_env::enter_test_process(&[
        (CA_ENV, Some(ca_path.as_os_str())),
        (INSECURE_DEV_ENV, None),
        (
            "SPACE_NODE_API_TLS_SERVER_NAME",
            Some(OsStr::new("node.example")),
        ),
    ]) {
        return;
    }
    pplx_space_common_svc::install_default_rustls_crypto_provider();
    let ca_path = std::path::PathBuf::from(std::env::var_os(CA_ENV).unwrap());
    // The certificate has no IP SAN: dialing a Pod IP must use the configured
    // NodeApi identity as well as the mounted CA.
    let certificate = rcgen::generate_simple_self_signed(vec!["node.example".into()]).unwrap();
    let untrusted = rcgen::generate_simple_self_signed(vec!["node.example".into()]).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (reporter, health) = tonic_health::server::health_reporter();
    reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(
        Server::builder()
            .tls_config(ServerTlsConfig::new().identity(Identity::from_pem(
                certificate.cert.pem(),
                certificate.signing_key.serialize_pem(),
            )))
            .unwrap()
            .add_service(health)
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                shutdown.clone().cancelled_owned(),
            ),
    );
    for (trust, accepted) in [
        (Some(certificate.cert.pem()), true),
        (Some(untrusted.cert.pem()), false),
        (None, false),
    ] {
        if let Some(pem) = trust {
            tokio::fs::write(&ca_path, pem).await.unwrap();
        } else {
            tokio::fs::remove_file(&ca_path).await.unwrap();
        }
        let channel = tokio::time::timeout(
            Duration::from_secs(3),
            KubernetesSubstrate::pod_worker_channel(
                PodIp(address.ip()),
                address.port(),
                Duration::from_secs(2),
            ),
        )
        .await
        .expect("connection establishment must remain bounded");
        assert_eq!(channel.is_some(), accepted);
        if let Some(channel) = channel {
            let response = tokio::time::timeout(
                Duration::from_secs(3),
                tonic_health::pb::health_client::HealthClient::new(channel).check(
                    tonic_health::pb::HealthCheckRequest {
                        service: String::new(),
                    },
                ),
            )
            .await
            .expect("health RPC must remain bounded")
            .unwrap();
            assert_eq!(
                response.into_inner().status,
                tonic_health::ServingStatus::Serving as i32,
            );
        }
    }
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .expect("listener shutdown must remain bounded")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn obsolete_materialization_cleanup_revalidates_the_current_claim()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::controllers::substrate::kubernetes::tests::{
        insert_test_sandbox_row, substrate_with_store_and_client,
    };
    use pplx_space_common_core::kubernetes::SANDBOX_MATERIALIZATION_ID_ANNOTATION;
    use tonic::codegen::http;
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox, "started").await?;
    let deletes = Arc::new(AtomicUsize::new(0));
    let conflict = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let count = Arc::clone(&deletes);
    let respond_conflict = Arc::clone(&conflict);
    let client = kube::Client::new(
        tower::service_fn(move |req: http::Request<kube::client::Body>| {
            use http_body_util::BodyExt;
            let count = Arc::clone(&count);
            let conflict = respond_conflict.load(Ordering::SeqCst);
            async move {
                assert_eq!(req.method(), http::Method::DELETE);
                let body = req.into_body().collect().await?.to_bytes();
                let options: serde_json::Value = serde_json::from_slice(&body)?;
                assert_eq!(options["preconditions"]["uid"], "pod-uid-1");
                assert_eq!(options["preconditions"]["resourceVersion"], "1");
                count.fetch_add(1, Ordering::SeqCst);
                let code = if conflict { 409 } else { 200 };
                let body = serde_json::json!({"kind":"Status", "apiVersion":"v1", "status":if conflict {"Failure"} else {"Success"}, "reason":if conflict {"Conflict"} else {""}, "code":code});
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                    http::Response::builder()
                        .status(code)
                        .header("content-type", "application/json")
                        .body(kube::client::Body::from(serde_json::to_vec(&body)?))?,
                )
            }
        }),
        "space",
    );
    let substrate = substrate_with_store_and_client(db.pool.clone(), client);
    let original = substrate.store.ensure_active_execution_row(sandbox).await?;
    sqlx::query(sql!("UPDATE sandbox_k8s_executions SET pod_name='sandbox-generation-1' WHERE sandbox_id=$1 AND active"))
        .bind(sandbox).execute(&db.pool).await?;
    substrate
        .store
        .persist_observed_pod_metadata(
            sandbox,
            original.generation,
            Some("pod-uid-1"),
            None,
            original.materialization_id,
        )
        .await?;
    let predecessor = substrate.store.ensure_active_execution_row(sandbox).await?;
    substrate
        .store
        .claim_pod_materialization(sandbox, &predecessor, SandboxLifecycle::Started)
        .await?
        .unwrap();
    let successor = substrate.store.ensure_active_execution_row(sandbox).await?;
    for (expected, token, allowed, changed) in [
        (
            &predecessor,
            successor.materialization_id.unwrap().to_string(),
            false,
            false,
        ),
        (
            &successor,
            predecessor.materialization_id.unwrap().to_string(),
            true,
            false,
        ),
        (&successor, "malformed-token".to_string(), true, false),
        (
            &successor,
            predecessor.materialization_id.unwrap().to_string(),
            true,
            true,
        ),
    ] {
        let mut pod = routable_pod(sandbox, original.generation);
        pod.metadata.name = Some("sandbox-generation-1".into());
        pod.metadata.resource_version = Some("1".into());
        pod.metadata
            .annotations
            .as_mut()
            .unwrap()
            .insert(SANDBOX_MATERIALIZATION_ID_ANNOTATION.into(), token);
        conflict.store(changed, Ordering::SeqCst);
        let count = deletes.load(Ordering::SeqCst);
        let result = delete::delete_execution_pod(
            &substrate.store,
            sandbox,
            &substrate.k8s,
            &SandboxPodView::from(pod),
            expected,
        )
        .await;
        assert_eq!(result.is_ok(), allowed && !changed, "{result:?}");
        if changed {
            assert!(matches!(result, Err(SubstrateError::Unavailable { .. })));
        }
        assert_eq!(deletes.load(Ordering::SeqCst), count + usize::from(allowed));
    }
    Ok(())
}

#[tokio::test]
async fn interrupted_restore_checks_materialization_before_hydration()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::controllers::substrate::kubernetes::tests::{
        insert_test_sandbox_row, substrate_with_store_client_proxy_port_and_pod,
    };
    use pplx_space_common_core::{
        kubernetes::SANDBOX_MATERIALIZATION_ID_ANNOTATION, types::PodMaterializationId,
    };
    use tonic::codegen::http;
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    for (matches, malformed) in [(true, false), (false, false), (false, true)] {
        let sandbox = SandboxId::new();
        let pod_name = format!("restore-{sandbox}");
        let current = PodMaterializationId::new();
        let observed = if malformed {
            "invalid-token".into()
        } else if matches {
            current.to_string()
        } else {
            PodMaterializationId::new().to_string()
        };
        insert_test_sandbox_row(&db.pool, sandbox, "restoring").await?;
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                (sandbox_id,generation,pod_name,pod_namespace,state,active,materialization_id)
                VALUES ($1,1,$3,'space','creating',TRUE,$2)"
        ))
        .bind(sandbox)
        .bind(current)
        .bind(&pod_name)
        .execute(&db.pool)
        .await?;
        let mut pod = routable_pod(sandbox, 1);
        pod.metadata.name = Some(pod_name);
        pod.metadata.namespace = Some("space".into());
        pod.metadata.resource_version = Some("7".into());
        pod.metadata
            .annotations
            .as_mut()
            .unwrap()
            .insert(SANDBOX_MATERIALIZATION_ID_ANNOTATION.into(), observed);
        pod.spec = Some(k8s_openapi::api::core::v1::PodSpec::default());
        pod.status = Some(PodStatus {
            phase: Some("Pending".into()),
            ..Default::default()
        });
        let deletes = Arc::new(AtomicUsize::new(0));
        let count = deletes.clone();
        let client = kube::Client::new(
            tower::service_fn(move |request: http::Request<kube::client::Body>| {
                let count = count.clone();
                async move {
                    use http_body_util::BodyExt;
                    assert_eq!(request.method(), http::Method::DELETE);
                    let body = request.into_body().collect().await?.to_bytes();
                    let options: serde_json::Value = serde_json::from_slice(&body)?;
                    assert_eq!(options["preconditions"]["uid"], "pod-uid-1");
                    assert_eq!(options["preconditions"]["resourceVersion"], "7");
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(http::Response::builder()
                        .header("content-type", "application/json")
                        .body(kube::client::Body::from(r#"{"kind":"Status","apiVersion":"v1","status":"Success","code":200}"#.as_bytes().to_vec()))?)
                }
            }),
            "space",
        );
        let substrate = substrate_with_store_client_proxy_port_and_pod(
            db.pool.clone(),
            client,
            8080,
            Some(pod),
        );
        let row = substrate.store.require_sandbox(sandbox).await?;
        let result = substrate
            .ensure_pod_exists(
                &row,
                None,
                None,
                Some("s3://example/restore?region=us-east-1"),
                PodMaterializationOwnership::new(None, None, None),
            )
            .await;
        assert_eq!(
            deletes.load(Ordering::SeqCst),
            usize::from(!matches),
            "{result:?}"
        );
        if matches {
            assert!(result.is_ok(), "{result:?}");
        } else {
            assert!(
                matches!(result, Err(SubstrateError::Unavailable { .. })),
                "{result:?}"
            );
        }
        let after = substrate.store.ensure_active_execution_row(sandbox).await?;
        assert_eq!(after.materialization_id, Some(current));
        assert_eq!(
            after.observed_pod_uid.as_deref(),
            matches.then_some("pod-uid-1")
        );
    }
    Ok(())
}
