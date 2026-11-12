//! The environment a sandbox pod records and, while the environment
//! placement gate is on, requires a node label for: the binding of what it
//! boots.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{
    EnvironmentReference, NodeId, SandboxId, SandboxLifecycle, SandboxOwner,
};
use pplx_space_db_test_fixtures::{SEED_ORG_ID, insert_ready_worker_node};

use super::*;
use crate::controllers::substrate::environment_placement::EnvironmentPlacementGate;
use pplx_space_common_core::kubernetes::PodEnvironment;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const E0: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const E1: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const HASH: &str = "sha256:seed";
const URI: &str = "s3://synthetic-bucket/checkpoint";

fn reference(value: &str) -> EnvironmentReference {
    value.parse().unwrap()
}

/// `value` as on requires it.
fn required(value: &str) -> Option<PodEnvironment> {
    Some(PodEnvironment {
        reference: reference(value),
        required: true,
    })
}

/// A sandbox row and its in-memory model, on template `seed` at [`HASH`],
/// whose release is bound to `release_environment`, with a ready checkpoint
/// at [`URI`] produced under `checkpoint_environment` and a published
/// execution under `published_environment`.
async fn sandbox(
    pool: &sqlx::PgPool,
    lifecycle: &str,
    release_environment: Option<&str>,
    checkpoint_environment: Option<&str>,
    published_environment: Option<&str>,
) -> Result<Sandbox, BoxError> {
    let id = SandboxId::new();
    insert_test_sandbox_row(pool, id, lifecycle).await?;
    let binding = |environment: Option<&str>| {
        sqlx::types::Json(match environment {
            Some(environment) => serde_json::json!({"environment": environment, "components": []}),
            None => serde_json::json!({"components": []}),
        })
    };
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, org_id) VALUES ($1, 'seed', $2)
         ON CONFLICT (name) DO NOTHING"
    ))
    .bind(uuid::Uuid::now_v7())
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "DELETE FROM template_releases WHERE template_name = 'seed'"
    ))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_releases (template_name, type, hash, execution_metadata)
         VALUES ('seed', 'fc', $1, $2)"
    ))
    .bind(HASH)
    .bind(binding(release_environment))
    .execute(pool)
    .await?;
    let snapshot_id = pplx_space_common_core::types::SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp, org_id, execution_metadata)
         VALUES ($1, $2, 'full', 'ready', NOW(), $3, $4)"
    ))
    .bind(snapshot_id)
    .bind(id)
    .bind(SEED_ORG_ID)
    .bind(binding(checkpoint_environment))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, $3)"
    ))
    .bind(snapshot_id)
    .bind(URI)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    let node = NodeId::new();
    insert_ready_worker_node(pool, node).await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status, runtime, execution_metadata)
         VALUES ($1, $2, 'stopped', 'firecracker', $3)"
    ))
    .bind(id)
    .bind(node)
    .bind(binding(published_environment))
    .execute(pool)
    .await?;
    Ok(Sandbox {
        id,
        content_hash: Some(HASH.to_string()),
        ..test_sandbox_with_lifecycle(lifecycle)
    })
}

fn restore() -> PodCreateContext<'static> {
    PodCreateContext::Restore(RestoreContext::UserRestore {
        suspend_restore_storage_uri: Some(URI),
        suspend_restore_source_node: None,
        direct_node_name: None,
        preferred_node_name: None,
    })
}

#[tokio::test(flavor = "current_thread")]
async fn off_renders_no_environment_and_shadow_records_it_without_requiring_it()
-> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox = sandbox(&db.pool, "suspended", Some(E1), Some(E1), Some(E1)).await?;
    let mut substrate = substrate_with_store(db.pool.clone());
    for (gate, expected) in [
        (EnvironmentPlacementGate::Off, None),
        (
            EnvironmentPlacementGate::Shadow,
            Some(PodEnvironment {
                reference: reference(E1),
                required: false,
            }),
        ),
        (EnvironmentPlacementGate::On, required(E1)),
    ] {
        substrate.k8s.environment_placement = gate;
        for context in [PodCreateContext::OnDemand, restore()] {
            assert_eq!(
                substrate
                    .resolve_pod_environment(&sandbox, &context)
                    .await?,
                expected,
                "{gate}"
            );
        }
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn on_requires_the_binding_of_what_the_pod_boots() -> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let mut substrate = substrate_with_store(db.pool.clone());
    substrate.k8s.environment_placement = EnvironmentPlacementGate::On;

    // Each record is bound to a different environment, so the answer names
    // the one the pod boots.
    let suspended = sandbox(&db.pool, "suspended", Some(E1), Some(E0), None).await?;
    assert_eq!(
        substrate
            .resolve_pod_environment(&suspended, &PodCreateContext::OnDemand)
            .await?,
        required(E1),
        "an on-demand pod boots the template release"
    );
    assert_eq!(
        substrate
            .resolve_pod_environment(&suspended, &restore())
            .await?,
        required(E0),
        "a suspended sandbox restores its checkpoint"
    );
    let stopped = sandbox(&db.pool, "stopped", Some(E1), Some(E1), Some(E0)).await?;
    assert_eq!(
        substrate
            .resolve_pod_environment(&stopped, &restore())
            .await?,
        required(E0),
        "a stopped sandbox cold boots its published execution"
    );
    // The release is still bound to E1 here, so only the missing content
    // hash can make an on-demand pod unbound.
    let no_hash = Sandbox {
        content_hash: None,
        ..suspended
    };
    assert_eq!(
        substrate
            .resolve_pod_environment(&no_hash, &PodCreateContext::OnDemand)
            .await?,
        None,
        "a sandbox with no content hash names no release"
    );

    let unbound = sandbox(&db.pool, "suspended", None, None, None).await?;
    for context in [PodCreateContext::OnDemand, restore()] {
        assert_eq!(
            substrate
                .resolve_pod_environment(&unbound, &context)
                .await?,
            None,
            "unbound work requires no environment"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_invalid_binding_fails_rather_than_placing_as_unbound() -> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let mut substrate = substrate_with_store(db.pool.clone());
    substrate.k8s.environment_placement = EnvironmentPlacementGate::On;
    let sandbox = sandbox(&db.pool, "suspended", Some("E1"), None, None).await?;
    let error = substrate
        .resolve_pod_environment(&sandbox, &PodCreateContext::OnDemand)
        .await
        .expect_err("an unparseable binding");
    assert!(
        matches!(&error, SubstrateError::Failed { message } if message.contains("invalid environment")),
        "{error:?}"
    );
    Ok(())
}

/// Shadow places exactly as off, so a binding it can't parse places the pod
/// as unbound rather than fail it: only on refuses to place bound work it
/// can't name.
#[tokio::test(flavor = "current_thread")]
async fn shadow_places_work_with_an_invalid_binding_as_off_would() -> Result<(), BoxError> {
    use crate::controllers::substrate::environment_placement::pool_pod_environment;
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let mut substrate = substrate_with_store(db.pool.clone());
    substrate.k8s.environment_placement = EnvironmentPlacementGate::Shadow;
    let sandbox = sandbox(&db.pool, "suspended", Some("E1"), Some("E0"), None).await?;
    for context in [PodCreateContext::OnDemand, restore()] {
        assert_eq!(
            substrate
                .resolve_pod_environment(&sandbox, &context)
                .await?,
            None
        );
    }
    assert_eq!(
        pool_pod_environment(EnvironmentPlacementGate::Shadow, &db.pool, "seed", HASH).await?,
        None
    );
    assert!(
        pool_pod_environment(EnvironmentPlacementGate::On, &db.pool, "seed", HASH)
            .await
            .is_err(),
        "on fails it"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_pool_pod_records_its_release_binding_and_requires_it_only_when_on()
-> Result<(), BoxError> {
    use crate::controllers::substrate::environment_placement::pool_pod_environment;
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    sandbox(&db.pool, "suspended", Some(E1), None, None).await?;
    for (gate, expected) in [
        (EnvironmentPlacementGate::Off, None),
        (
            EnvironmentPlacementGate::Shadow,
            Some(PodEnvironment {
                reference: reference(E1),
                required: false,
            }),
        ),
        (EnvironmentPlacementGate::On, required(E1)),
    ] {
        assert_eq!(
            pool_pod_environment(gate, &db.pool, "seed", HASH).await?,
            expected,
            "{gate}"
        );
    }
    assert_eq!(
        pool_pod_environment(
            EnvironmentPlacementGate::On,
            &db.pool,
            "seed",
            "sha256:other"
        )
        .await?,
        None,
        "no such build"
    );
    Ok(())
}

/// A release that records no binding leaves template work the one every
/// node holding the build reports, as for a Direct pull-on-create, rather
/// than passing the gate as unbound.
#[tokio::test(flavor = "current_thread")]
async fn a_release_without_a_binding_takes_its_environment_from_the_build_inventory()
-> Result<(), BoxError> {
    use crate::controllers::substrate::environment_placement::pool_pod_environment;
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let mut substrate = substrate_with_store(db.pool.clone());
    substrate.k8s.environment_placement = EnvironmentPlacementGate::On;
    sandbox(&db.pool, "suspended", None, None, None).await?;
    assert_eq!(
        substrate.template_pod_environment("seed", HASH).await?,
        None,
        "nothing records a binding yet"
    );

    let hold = |environment: &'static str| {
        let pool = db.pool.clone();
        async move {
            let holder = NodeId::new();
            insert_ready_worker_node(&pool, holder).await?;
            sqlx::query(sql!(
                "INSERT INTO node_template_inventory (
                     node_id, template_name, content_hash, runtime, arch,
                     has_base_snapshot, vcpu, memory_mib, guest_os,
                     state_format_name, state_format_version, execution_metadata
                 )
                 VALUES ($1, 'seed', $2, 'fc', 'aarch64', TRUE, 1, 512, 'linux',
                         'firecracker-snapshot', '9.0.0',
                         jsonb_build_object('environment', $3::TEXT))"
            ))
            .bind(holder)
            .bind(HASH)
            .bind(environment)
            .execute(&pool)
            .await?;
            Ok::<_, BoxError>(())
        }
    };
    hold(E1).await?;
    assert_eq!(
        substrate.template_pod_environment("seed", HASH).await?,
        required(E1),
        "a cold create or an on-demand pod"
    );
    assert_eq!(
        pool_pod_environment(EnvironmentPlacementGate::On, &db.pool, "seed", HASH).await?,
        required(E1),
        "a warm-pool pod"
    );

    // A pod requires one label, so holders that disagree leave the work
    // unbound on Kubernetes.
    hold(E0).await?;
    assert_eq!(
        substrate.template_pod_environment("seed", HASH).await?,
        None,
        "the holders disagree"
    );
    Ok(())
}

/// The environment has to reach the PodSpec on every context
/// `sandbox_pod_request` renders, as the node-group boundary does
/// (`every_pod_build_path_renders_the_node_group_boundary`): each arm passes
/// `common` through, so a dropped field renders an unbound pod.
#[tokio::test(flavor = "current_thread")]
async fn every_pod_build_path_renders_the_environment() {
    use pplx_space_common_core::kubernetes::{
        SANDBOX_ENVIRONMENT_ANNOTATION, build_sandbox_pod, environment_label_key,
    };
    let substrate = substrate_with_unreachable_store();
    let sandbox = test_sandbox_with_lifecycle("started");
    let execution = SandboxExecutionRow {
        generation: 1,
        pod_name: "space-sbx-env-g1".to_string(),
        pod_materialized: false,
        state: ActiveExecutionState::Creating,
        create_age_expired: false,
        activation_attempt_id: None,
        route_activation_id: None,
    };
    let owner = SandboxOwner::platform_user(sandbox.user_id);
    for context in [
        PodCreateContext::OnDemand,
        PodCreateContext::Restore(RestoreContext::UserRestore {
            suspend_restore_storage_uri: None,
            suspend_restore_source_node: Some("source-node"),
            direct_node_name: Some("source-node"),
            preferred_node_name: None,
        }),
        PodCreateContext::Restore(RestoreContext::Evacuation {
            stopped_materialization: None,
            suspend_restore_storage_uri: Some("s3://bucket/key"),
            extra_tolerations: &[],
            extra_required_affinity: None,
        }),
    ] {
        let request = substrate.sandbox_pod_request(
            &sandbox,
            &execution,
            crate::controllers::substrate::kubernetes::pod_ops::SandboxPodRequestInputs {
                sandbox_owner: &owner,
                auto_lifecycle_json: "{}",
                network_policy_json: "{}",
                node_group_placement: None,
                environment: required(E1),
                client_id: None,
            },
            context,
        );
        let pod = build_sandbox_pod(&sandbox_pod_config(&substrate.k8s), request);
        assert_eq!(
            pod.metadata
                .annotations
                .as_ref()
                .unwrap()
                .get(SANDBOX_ENVIRONMENT_ANNOTATION)
                .map(String::as_str),
            Some(E1)
        );
        let key = environment_label_key(&reference(E1));
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
        assert!(!terms.is_empty());
        for term in terms {
            assert!(
                term.match_expressions
                    .unwrap_or_default()
                    .iter()
                    .any(|expression| expression.key == key)
            );
        }
    }
}

/// A drain's stopped re-placement claims the row to `restoring`
/// (`claim_evacuation_restore`), so the lifecycle no longer says the sandbox
/// is stopped while its pod cold boots the pinned disk artifact. The stopped
/// materialization is what names that work: the disk snapshot's binding, or,
/// where it records none, the execution the sandbox last published.
#[tokio::test(flavor = "current_thread")]
async fn a_stopped_replacement_requires_its_pinned_disk_environment_while_restoring()
-> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let mut substrate = substrate_with_store(db.pool.clone());
    substrate.k8s.environment_placement = EnvironmentPlacementGate::On;
    let restoring = sandbox(&db.pool, "restoring", Some(E1), Some(E1), Some(E0)).await?;
    let excluded: Vec<String> = Vec::new();
    let artifact_id = pplx_space_common_core::types::LocalArtifactId::new();
    let evacuation = |stopped| {
        PodCreateContext::Restore(RestoreContext::Evacuation {
            stopped_materialization: stopped,
            suspend_restore_storage_uri: Some(URI),
            extra_tolerations: &[],
            extra_required_affinity: None,
        })
    };
    let pinned = || {
        evacuation(Some(
            pplx_space_common_core::kubernetes::StoppedMaterialization {
                artifact_id,
                excluded_node_names: &excluded,
            },
        ))
    };
    assert_eq!(
        substrate
            .resolve_pod_environment(&restoring, &pinned())
            .await?,
        required(E0),
        "a disk that records no binding boots the published execution"
    );
    assert_eq!(
        substrate
            .resolve_pod_environment(&restoring, &evacuation(None))
            .await?,
        required(E1),
        "a suspended evacuation restores its checkpoint"
    );

    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp, org_id,
                                local_artifact_id, artifact_kind, artifact_origin,
                                execution_metadata)
         VALUES ($1, $2, 'disk', 'ready', NOW(), $3, $4, 'disk', 'stop', $5)"
    ))
    .bind(pplx_space_common_core::types::SnapshotId::new())
    .bind(restoring.id)
    .bind(SEED_ORG_ID)
    .bind(artifact_id.as_uuid())
    .bind(sqlx::types::Json(
        serde_json::json!({"environment": E1, "components": []}),
    ))
    .execute(&db.pool)
    .await?;
    // The source node's deletion takes the published execution with it.
    sqlx::query(sql!(
        "DELETE FROM sandbox_observed_states WHERE sandbox_id = $1"
    ))
    .bind(restoring.id)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        substrate
            .resolve_pod_environment(&restoring, &pinned())
            .await?,
        required(E1),
        "the pinned disk's own binding outlives its source node"
    );
    Ok(())
}

/// Off renders no environment even for a caller that doesn't check the gate
/// first.
#[test]
fn pod_environment_follows_the_gate() {
    use crate::controllers::substrate::environment_placement::pod_environment;
    assert_eq!(
        pod_environment(EnvironmentPlacementGate::Off, reference(E1)),
        None
    );
    assert_eq!(
        pod_environment(EnvironmentPlacementGate::Shadow, reference(E1)),
        Some(PodEnvironment {
            reference: reference(E1),
            required: false,
        })
    );
    assert_eq!(
        pod_environment(EnvironmentPlacementGate::On, reference(E1)),
        required(E1)
    );
}

/// The environment of `pod`, a captured pod create: its annotation, and
/// whether every required node-affinity term requires its label.
pub(in crate::controllers::substrate::kubernetes) fn created_pod_environment(
    pod: &serde_json::Value,
) -> (Option<String>, bool) {
    use pplx_space_common_core::kubernetes::SANDBOX_ENVIRONMENT_ANNOTATION;
    let annotation = pod
        .pointer("/metadata/annotations")
        .and_then(|annotations| annotations.get(SANDBOX_ENVIRONMENT_ANNOTATION))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let terms = pod
        .pointer(
            "/spec/affinity/nodeAffinity/requiredDuringSchedulingIgnoredDuringExecution/nodeSelectorTerms",
        )
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let required = annotation.as_deref().is_some_and(|recorded| {
        let key = pplx_space_common_core::kubernetes::environment_label_key(&reference(recorded));
        !terms.is_empty()
            && terms.iter().all(|term| {
                term.get("matchExpressions")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|expressions| {
                        expressions.iter().any(|expression| {
                            expression.get("key").and_then(serde_json::Value::as_str)
                                == Some(key.as_str())
                        })
                    })
            })
    });
    (annotation, required)
}

/// `ensure_pod_exists` resolves the environment and hands it to the pod it
/// creates: a started sandbox with no prior state materializes on demand,
/// which boots the template release.
#[tokio::test(flavor = "current_thread")]
async fn a_materialized_on_demand_pod_requires_its_release_environment() -> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let seeded = sandbox(&db.pool, "started", Some(E1), None, None).await?;
    // No prior state, so the recovery materializes on demand rather than
    // restoring: only the release binding is left.
    for statement in [
        sql!(
            "DELETE FROM snapshot_storage_locations
              WHERE snapshot_id IN (SELECT id FROM snapshots WHERE sandbox_id = $1)"
        ),
        sql!("DELETE FROM snapshots WHERE sandbox_id = $1"),
        sql!("DELETE FROM sandbox_observed_states WHERE sandbox_id = $1"),
    ] {
        sqlx::query(statement)
            .bind(seeded.id)
            .execute(&db.pool)
            .await?;
    }
    // `ensure_pod_exists` builds from the row it re-reads.
    sqlx::query(sql!(
        "UPDATE sandboxes SET template_id = 'seed', content_hash = $2 WHERE id = $1"
    ))
    .bind(seeded.id)
    .bind(HASH)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, state, active)
         VALUES ($1, 2, 'space-sbx-environment-g2', 'space', 'running', TRUE)"
    ))
    .bind(seeded.id)
    .execute(&db.pool)
    .await?;
    let sandbox = crate::controllers::sandbox::fetch_sandbox(&db.pool, seeded.id)
        .await?
        .expect("the seeded sandbox");
    let generation = match substrate_with_store(db.pool.clone())
        .store
        .claim_started_recovery(sandbox.id)
        .await?
    {
        StartedRecoveryClaim::ExistingExecution { generation } => generation,
        claim => panic!("expected the tracked execution's recovery claim, got {claim:?}"),
    };
    let creates = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut substrate = substrate_with_store_and_client(
        db.pool.clone(),
        mock_kubernetes_client(std::sync::Arc::clone(&creates)),
    );
    substrate.k8s.environment_placement = EnvironmentPlacementGate::On;

    substrate
        .ensure_pod_exists(
            &sandbox,
            None,
            None,
            None,
            PodMaterializationOwnership::new(
                Some(StartedRecoveryOwnership::ExistingExecution { generation }),
                None,
                None,
            ),
        )
        .await?;

    let creates = creates.lock().unwrap();
    let [pod] = creates.as_slice() else {
        panic!("expected one pod create, got {}", creates.len());
    };
    assert_eq!(
        pod.pointer("/metadata/annotations")
            .and_then(|annotations| {
                annotations.get(pplx_space_common_core::kubernetes::SANDBOX_CHECKPOINT_ANNOTATION)
            }),
        None,
        "an on-demand pod restores no checkpoint"
    );
    assert_eq!(created_pod_environment(pod), (Some(E1.to_string()), true));
    Ok(())
}

/// The cross-node re-drive builds its successor from scratch, so it resolves
/// the environment itself: the successor of a suspended sandbox's restore
/// requires the checkpoint's environment.
#[tokio::test(flavor = "current_thread")]
async fn a_cross_node_redrive_successor_requires_its_checkpoint_environment() -> Result<(), BoxError>
{
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox = sandbox(&db.pool, "restoring", Some(E1), Some(E0), Some(E1)).await?;
    let creates = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut substrate = substrate_with_store_and_client(
        db.pool.clone(),
        mock_kubernetes_client(std::sync::Arc::clone(&creates)),
    );
    substrate.k8s.environment_placement = EnvironmentPlacementGate::On;
    let claim = substrate
        .store
        .ensure_active_execution_claim(sandbox.id)
        .await?;

    // The pinned predecessor is already gone (the GET answers 404), so the
    // re-drive goes straight to the successor.
    let _ = substrate
        .redrive_unschedulable_restore_cross_node_recorded(
            crate::metrics::sandbox::restore::RedrivePath::Retry,
            &sandbox,
            &claim,
            SandboxLifecycle::Restoring,
            Some("source-node"),
            Some(URI),
            None,
            None,
        )
        .await;

    let creates = creates.lock().unwrap();
    let [pod] = creates.as_slice() else {
        panic!("expected one successor create, got {}", creates.len());
    };
    assert_eq!(created_pod_environment(pod), (Some(E0.to_string()), true));
    Ok(())
}

/// A failed read of the work's record places the pod as off would in
/// shadow, and fails it on, for template work and a restore alike.
#[tokio::test(flavor = "current_thread")]
async fn shadow_places_work_whose_record_it_cannot_read_as_off_would() {
    let mut substrate = substrate_with_unreachable_store();
    let sandbox = Sandbox {
        content_hash: Some(HASH.to_string()),
        ..test_sandbox_with_lifecycle("suspended")
    };
    for context in [PodCreateContext::OnDemand, restore()] {
        substrate.k8s.environment_placement = EnvironmentPlacementGate::Shadow;
        assert_eq!(
            substrate
                .resolve_pod_environment(&sandbox, &context)
                .await
                .expect("shadow places as off"),
            None
        );
        substrate.k8s.environment_placement = EnvironmentPlacementGate::On;
        assert!(
            substrate
                .resolve_pod_environment(&sandbox, &context)
                .await
                .is_err(),
            "on fails the build"
        );
    }
}

/// A stopped sandbox's own start boots its current disk artifact, whose
/// record outlives the node the sandbox stopped on: deleting that node
/// removes the published execution, and the disk's binding still applies.
#[tokio::test(flavor = "current_thread")]
async fn a_stopped_start_requires_its_disk_environment_after_the_published_execution_is_gone()
-> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let mut substrate = substrate_with_store(db.pool.clone());
    substrate.k8s.environment_placement = EnvironmentPlacementGate::On;
    let stopped = sandbox(&db.pool, "stopped", Some(E0), Some(E0), Some(E0)).await?;
    let artifact_id = pplx_space_common_core::types::LocalArtifactId::new();
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'disk',
                current_local_artifact_origin = 'stop'
          WHERE id = $1"
    ))
    .bind(stopped.id)
    .bind(artifact_id.as_uuid())
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp, org_id,
                                local_artifact_id, artifact_kind, artifact_origin,
                                execution_metadata)
         VALUES ($1, $2, 'disk', 'ready', NOW(), $3, $4, 'disk', 'stop', $5)"
    ))
    .bind(pplx_space_common_core::types::SnapshotId::new())
    .bind(stopped.id)
    .bind(SEED_ORG_ID)
    .bind(artifact_id.as_uuid())
    .bind(sqlx::types::Json(
        serde_json::json!({"environment": E1, "components": []}),
    ))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "DELETE FROM sandbox_observed_states WHERE sandbox_id = $1"
    ))
    .bind(stopped.id)
    .execute(&db.pool)
    .await?;

    assert_eq!(
        substrate
            .resolve_pod_environment(&stopped, &restore())
            .await?,
        required(E1)
    );
    Ok(())
}

/// A restore pinned to its source node skips kube-scheduler, and kubelet's
/// refusal there is terminal, so with the gate on the restore is refused
/// before any pod exists when the reflector shows the source node not
/// publishing the environment. A node the cache doesn't have keeps the pin.
#[tokio::test(flavor = "current_thread")]
async fn a_pinned_restore_is_refused_before_its_pod_when_the_source_node_lacks_the_environment()
-> Result<(), BoxError> {
    use pplx_space_common_core::kubernetes::{
        ENVIRONMENT_USABLE_LABEL_VALUE, SandboxPodConfigValues, environment_label_key,
    };
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let suspended = sandbox(&db.pool, "suspended", Some(E1), Some(E1), None).await?;
    let node = |name: &str, publishes: Option<&str>| {
        let mut labels = SandboxPodConfigValues::default()
            .base_node_selector()
            .into_labels();
        if let Some(environment) = publishes {
            labels.insert(
                environment_label_key(&reference(environment)),
                ENVIRONMENT_USABLE_LABEL_VALUE.to_string(),
            );
        }
        (*node_with_network_slots(name, labels, Some(8), None, true, false)).clone()
    };
    let mut substrate = substrate_with_inventory_caches_and_pool(
        Vec::new(),
        vec![
            node("publishes-e1", Some(E1)),
            node("publishes-e0", Some(E0)),
        ],
        db.pool.clone(),
    );
    substrate.k8s.environment_placement = EnvironmentPlacementGate::On;
    let pinned = |node_name| {
        PodCreateContext::Restore(RestoreContext::UserRestore {
            suspend_restore_storage_uri: Some(URI),
            suspend_restore_source_node: Some(node_name),
            direct_node_name: Some(node_name),
            preferred_node_name: None,
        })
    };

    assert_eq!(
        substrate
            .resolve_pod_environment(&suspended, &pinned("publishes-e1"))
            .await?,
        required(E1)
    );
    let refused = substrate
        .resolve_pod_environment(&suspended, &pinned("publishes-e0"))
        .await
        .expect_err("the source node doesn't publish E1");
    assert!(
        matches!(&refused, SubstrateError::Conflict { message } if message.contains("publishes-e0")),
        "{refused:?}"
    );
    assert_eq!(
        substrate
            .resolve_pod_environment(&suspended, &pinned("not-in-the-cache"))
            .await?,
        required(E1),
        "kubelet stays the backstop for a node the cache doesn't have"
    );
    substrate.k8s.environment_placement = EnvironmentPlacementGate::Shadow;
    assert!(
        substrate
            .resolve_pod_environment(&suspended, &pinned("publishes-e0"))
            .await
            .is_ok(),
        "shadow places as off"
    );
    Ok(())
}
