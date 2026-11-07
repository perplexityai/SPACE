//! A user restore's or a stopped start's pod boots the runtime upgrade the
//! policy and catalog decide, only while the decision is enabled, and its
//! Wake never carries one; a stranded sandbox is refused before any pod or
//! Wake.

use std::time::Duration;

use crate::metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{SandboxId, SandboxLifecycle};

use super::stopped_runtime_upgrade::fixture::{
    Cluster, Report, backup_enqueued, seed_sandbox_on_e0,
};
use super::*;
use crate::controllers::substrate::runtime_upgrade::RuntimeUpgradeAtActivation;
use crate::controllers::substrate::{ActivationOptions, RecoveryCharge, SubstrateAdapter};
use pplx_space_common_core::kubernetes::{
    RUNTIME_UPGRADE_CHECKPOINT, SANDBOX_CHECKPOINT_ANNOTATION,
    SANDBOX_RUNTIME_UPGRADE_AUTHORIZATION_ID_ANNOTATION,
    SANDBOX_RUNTIME_UPGRADE_CHECKPOINT_LOCAL_ARTIFACT_ID_ANNOTATION,
    SANDBOX_RUNTIME_UPGRADE_MEMORY_LOSS_ANNOTATION,
    SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION,
    SANDBOX_RUNTIME_UPGRADE_TARGET_ENVIRONMENT_ANNOTATION,
    SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION,
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub(super) const E0: &str =
    "sha256:0000000000000000000000000000000000000000000000000000000000000000";
pub(super) const E1: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";

/// The cluster default is E1; E0, which the sandbox's checkpoint was produced
/// by, is blocked. `e0_upgrade_edges` are E0's certified upgrade edges.
pub(super) async fn block_e0_for_e1(
    pool: &sqlx::PgPool,
    e0_upgrade_edges: &[&str],
) -> Result<(), BoxError> {
    use pplx_space_runtime_environment_policy_store::{
        PolicyChange, PolicyChangeReason, RuntimeEnvironmentState, set_default,
        set_environment_state,
    };
    let change = PolicyChange {
        actor: None,
        reason: PolicyChangeReason::new("test policy")?,
    };
    let mut tx = pool.begin().await?;
    set_default(&mut tx, E1.parse()?, &change).await?;
    set_environment_state(
        &mut tx,
        E0.parse()?,
        RuntimeEnvironmentState::Blocked,
        &change,
    )
    .await?;
    tx.commit().await?;
    for (reference, version, format, edges) in [
        (E0, "1.15.1", "9.0.0", e0_upgrade_edges.to_vec()),
        (E1, "1.17.0", "12.0.0", Vec::new()),
    ] {
        sqlx::query(sql!(
            "INSERT INTO runtime_environments
                 (environment_ref, runtime, runtime_version, kernel_version,
                  state_format_name, state_format_version, upgrade_edges)
             VALUES ($1, 'firecracker', $2, '6.1.155', 'firecracker-snapshot', $3, $4)"
        ))
        .bind(reference)
        .bind(version)
        .bind(format)
        .bind(edges)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// What the node publishes for a stopped sandbox: the environment it last
/// ran under.
pub(super) async fn publish_environment(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    node_id: pplx_space_common_core::types::NodeId,
    environment: &str,
) -> Result<(), BoxError> {
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status, execution_metadata)
         VALUES ($1, $2, 'stopped', jsonb_build_object('environment', $3::text))
         ON CONFLICT (sandbox_id) DO UPDATE
            SET status = EXCLUDED.status,
                execution_metadata = EXCLUDED.execution_metadata,
                updated_at = NOW()"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(environment)
    .execute(pool)
    .await?;
    Ok(())
}

/// Record `environment` as the producer of every snapshot of `sandbox_id`.
async fn bind_snapshots_to(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    environment: &str,
) -> Result<(), BoxError> {
    sqlx::query(sql!(
        "UPDATE snapshots
            SET execution_metadata = jsonb_build_object('environment', $2::text,
                                                        'components', '[]'::jsonb)
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .bind(environment)
    .execute(pool)
    .await?;
    Ok(())
}

/// A suspended sandbox whose checkpoint E0 produced, restored by a user
/// rather than evacuated, on a scripted node that reports every pod's
/// upgrade done.
async fn user_restore_subject(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
) -> Result<(KubernetesSubstrate, Cluster), BoxError> {
    // Only the rows: the substrate and node below replace its own.
    let _ = paused_restore_test_subject(pool, sandbox_id, Some(1), true).await?;
    let cluster = Cluster::start(
        pool,
        pplx_space_k8s_node_id::space_k8s_node_id("node-a", "worker"),
        backup_enqueued(),
    )
    .await?;
    let substrate = cluster.substrate(pool);
    sqlx::query(sql!(
        "UPDATE sandboxes SET evacuation_restore_target = NULL WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    // A user restore claims only once the old execution has gone quiet.
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET updated_at = NOW() - INTERVAL '1 hour',
                k8s_node_name = 'node-a'
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    bind_snapshots_to(pool, sandbox_id, E0).await?;
    // A suspend pins its checkpoint as the sandbox's current local artifact,
    // which a restore without a storage URI loads.
    sqlx::query(sql!(
        "UPDATE sandboxes s
            SET current_local_artifact_id = sn.local_artifact_id,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
           FROM snapshots sn
          WHERE s.id = $1 AND sn.sandbox_id = s.id"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    Ok((substrate, cluster))
}

/// Every Wake `cluster`'s node received.
fn wakes(cluster: &Cluster) -> Vec<pplx_space_sbx_mgr_client::proto::WakeRequest> {
    cluster.node.wakes.lock().unwrap().clone()
}

async fn restore(
    substrate: &KubernetesSubstrate,
    sandbox_id: SandboxId,
) -> Result<crate::controllers::substrate::ActivationCompletion, SubstrateError> {
    restore_within(substrate, sandbox_id, Duration::from_secs(2)).await
}

/// A restore under `ready_timeout`; one the test expects to run to its
/// deadline takes a short one.
async fn restore_within(
    substrate: &KubernetesSubstrate,
    sandbox_id: SandboxId,
    ready_timeout: Duration,
) -> Result<crate::controllers::substrate::ActivationCompletion, SubstrateError> {
    substrate
        .restore_execution(
            SandboxApiMetricIdentity::native("restore"),
            sandbox_id,
            ActivationOptions {
                blocking: true,
                ready_timeout,
            },
            RecoveryCharge::Exempt,
        )
        .await
}

#[tokio::test(flavor = "current_thread")]
async fn a_user_restore_off_a_blocked_environment_boots_its_pod_by_the_upgrade()
-> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (mut substrate, cluster) = user_restore_subject(&db.pool, sandbox_id).await?;
    block_e0_for_e1(&db.pool, &[E1]).await?;
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;

    let result = restore(&substrate, sandbox_id).await;

    let checkpoints = cluster
        .pods
        .created_annotations(SANDBOX_CHECKPOINT_ANNOTATION);
    assert!(!checkpoints.is_empty(), "no pod: {result:?}");
    assert!(
        checkpoints
            .iter()
            .all(|checkpoint| checkpoint.as_deref() == Some(RUNTIME_UPGRADE_CHECKPOINT)),
        "{checkpoints:?}"
    );
    let annotation = |key| cluster.pods.created_annotations(key)[0].clone();
    assert_eq!(
        annotation(SANDBOX_RUNTIME_UPGRADE_TARGET_ENVIRONMENT_ANNOTATION).as_deref(),
        Some(E1)
    );
    assert_eq!(
        annotation(SANDBOX_RUNTIME_UPGRADE_MEMORY_LOSS_ANNOTATION).as_deref(),
        Some("true")
    );
    assert!(
        annotation(SANDBOX_RUNTIME_UPGRADE_AUTHORIZATION_ID_ANNOTATION)
            .is_some_and(|authorization| authorization.contains(E0))
    );
    // The node boots only the checkpoint the decision read.
    let pinned: Option<uuid::Uuid> = sqlx::query_scalar(sql!(
        "SELECT current_local_artifact_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        annotation(SANDBOX_RUNTIME_UPGRADE_CHECKPOINT_LOCAL_ARTIFACT_ID_ANNOTATION),
        Some(pinned.expect("the subject pins its checkpoint").to_string())
    );
    // The restore keeps its placement: the node holding the checkpoint.
    assert_eq!(
        annotation(SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION).as_deref(),
        Some("node-a")
    );
    // Every pod of this restore names the same upgrade, and the node was
    // asked for it.
    let operations = cluster
        .pods
        .created_annotations(SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION);
    assert!(
        operations
            .iter()
            .all(|operation| *operation == operations[0]),
        "{operations:?}"
    );
    assert!(
        cluster
            .worker
            .first_call(super::stopped_runtime_upgrade::fixture::GET_RUNTIME_UPGRADE)
            .is_some()
    );
    // The Wake acknowledges the route and carries no upgrade, which the node
    // refuses on a Kubernetes Wake.
    let received = wakes(&cluster);
    assert!(!received.is_empty(), "no Wake: {result:?}");
    assert!(
        received.iter().all(|wake| wake.runtime_upgrade.is_none()),
        "{received:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_user_restore_wakes_as_recorded_while_the_decision_is_disabled() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (substrate, cluster) = user_restore_subject(&db.pool, sandbox_id).await?;
    block_e0_for_e1(&db.pool, &[E1]).await?;
    assert!(!substrate.k8s.runtime_upgrade_at_activation.is_enabled());

    let result = restore(&substrate, sandbox_id).await;

    let checkpoints = cluster
        .pods
        .created_annotations(SANDBOX_CHECKPOINT_ANNOTATION);
    assert!(!checkpoints.is_empty(), "no pod: {result:?}");
    assert!(
        checkpoints
            .iter()
            .all(|checkpoint| checkpoint.as_deref() != Some(RUNTIME_UPGRADE_CHECKPOINT)),
        "{checkpoints:?}"
    );
    let received = wakes(&cluster);
    assert!(!received.is_empty(), "{result:?}");
    assert!(
        received.iter().all(|wake| wake.runtime_upgrade.is_none()),
        "{received:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_stranded_sandbox_is_refused_before_any_wake_and_its_restore_released()
-> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (mut substrate, cluster) = user_restore_subject(&db.pool, sandbox_id).await?;
    // Blocked, with no edge to the default.
    block_e0_for_e1(&db.pool, &[]).await?;
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;

    let error = restore(&substrate, sandbox_id)
        .await
        .expect_err("a stranded sandbox is refused");

    let SubstrateError::FailedPrecondition { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains(E0) && message.contains(E1), "{message}");
    assert!(wakes(&cluster).is_empty());
    assert!(cluster.pods.created.lock().unwrap().is_empty());
    // The refusal reverts the restore claim, so the sandbox stays restorable
    // once an operator acts.
    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(lifecycle, SandboxLifecycle::Suspended.as_str());
    Ok(())
}

/// A sandbox in `lifecycle` whose last execution E0 ran on `node-a`, with E0
/// blocked (or, with `deprecated`, deprecated) in favor of E1, on a
/// scripted node, with the decision at activation enabled.
async fn start_subject(
    lifecycle: &str,
    deprecated: bool,
) -> Result<
    (
        pplx_space_db_test_fixtures::TestDb,
        SandboxId,
        KubernetesSubstrate,
        Cluster,
    ),
    BoxError,
> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = db.pool.clone();
    // The stopped disk's node is the one its start is placed on.
    let (sandbox_id, node_id) = seed_sandbox_on_e0(&pool, lifecycle).await?;
    if deprecated {
        use pplx_space_runtime_environment_policy_store::{
            PolicyChange, PolicyChangeReason, RuntimeEnvironmentState, set_environment_state,
        };
        let mut tx = pool.begin().await?;
        set_environment_state(
            &mut tx,
            E0.parse()?,
            RuntimeEnvironmentState::Deprecated,
            &PolicyChange {
                actor: None,
                reason: PolicyChangeReason::new("test deprecation")?,
            },
        )
        .await?;
        tx.commit().await?;
    }
    let cluster = Cluster::start(&pool, node_id, backup_enqueued()).await?;
    // The start's upgrade boots the sandbox; nothing publishes it stopped.
    cluster.node.report(Report::Upgraded { publish: false });
    let mut substrate = cluster.substrate(&pool);
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;
    Ok((db, sandbox_id, substrate, cluster))
}

async fn start(
    substrate: &KubernetesSubstrate,
    sandbox_id: SandboxId,
) -> Result<crate::controllers::substrate::ExecutionActivation, SubstrateError> {
    start_within(substrate, sandbox_id, Duration::from_secs(2)).await
}

/// A start under `ready_timeout`; one the test expects to run to its deadline
/// takes a short one.
async fn start_within(
    substrate: &KubernetesSubstrate,
    sandbox_id: SandboxId,
    ready_timeout: Duration,
) -> Result<crate::controllers::substrate::ExecutionActivation, SubstrateError> {
    substrate
        .start_execution(
            SandboxApiMetricIdentity::native("start"),
            sandbox_id,
            ActivationOptions {
                blocking: true,
                ready_timeout,
            },
            RecoveryCharge::Exempt,
        )
        .await
}

/// Only a stopped sandbox's start moves its runtime; a start that recovers a
/// live lifecycle keeps the environment its execution runs under.
/// A deprecated environment still executes, but a stopped sandbox loses
/// nothing by moving, so it moves too. The upgrade boots in the start's pod,
/// targeting `started`; its Wake only acknowledges the route.
#[rstest::rstest]
#[case::stopped("stopped", false, true)]
#[case::stopped_on_deprecated("stopped", true, true)]
#[case::started("started", false, false)]
#[tokio::test(flavor = "current_thread")]
async fn a_stopped_start_off_a_blocked_environment_boots_its_pod_by_the_upgrade(
    #[case] lifecycle: &str,
    #[case] deprecated: bool,
    #[case] upgrades: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let (_db, sandbox_id, substrate, cluster) = start_subject(lifecycle, deprecated).await?;

    let result = start(&substrate, sandbox_id).await;

    let received = wakes(&cluster);
    assert!(!received.is_empty(), "no Wake: {:?}", result.as_ref().err());
    assert!(
        received.iter().all(|wake| wake.runtime_upgrade.is_none()),
        "{received:?}"
    );
    let checkpoints = cluster
        .pods
        .created_annotations(SANDBOX_CHECKPOINT_ANNOTATION);
    assert!(!checkpoints.is_empty(), "no pod");
    assert!(
        checkpoints.iter().all(|checkpoint| (checkpoint.as_deref()
            == Some(RUNTIME_UPGRADE_CHECKPOINT))
            == upgrades),
        "{checkpoints:?}"
    );
    if upgrades {
        cluster.pods.assert_every_create_upgrades(
            cluster
                .pods
                .created_annotations(SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION)[0]
                .as_deref()
                .unwrap(),
            "started",
        );
        let annotation = |key| cluster.pods.created_annotations(key)[0].clone();
        assert_eq!(
            annotation(SANDBOX_RUNTIME_UPGRADE_TARGET_ENVIRONMENT_ANNOTATION).as_deref(),
            Some(E1)
        );
        // A stopped sandbox has no memory to lose.
        assert_eq!(
            annotation(SANDBOX_RUNTIME_UPGRADE_MEMORY_LOSS_ANNOTATION).as_deref(),
            Some("false")
        );
    }
    Ok(())
}

/// A start whose pod doesn't run the sandbox on the target is refused before
/// its Wake: a rollback or a call refused before it began settled it on the
/// old environment, and an upgrade the node hasn't settled is left to a
/// retry. The node's report is the upgrade's outcome, not the pod's: one it
/// upgraded, or found on the target, whose pod's create it refused (as it
/// refuses a retried operation's new pod once the sandbox left the execution
/// the old pod ran) answers a retryable refusal at once rather than a
/// timeout.
#[rstest::rstest]
#[case::rolled_back(Report::RolledBack, false, Some("rolled back"))]
#[case::not_started(Report::Absent, true, Some("refused before it began"))]
#[case::unsettled(Report::InProgress, false, None)]
#[case::upgraded_pod_refused(
    Report::Upgraded { publish: false },
    true,
    Some("create was refused, so the pod doesn't run it")
)]
#[case::on_target_pod_refused(
    Report::AlreadyOnTarget,
    true,
    Some("create was refused, so the pod doesn't run it; the operation already claimed its start")
)]
#[tokio::test(flavor = "current_thread")]
async fn a_start_whose_pod_doesnt_run_the_upgraded_sandbox_never_wakes(
    #[case] report: Report,
    #[case] container_ends: bool,
    #[case] refusal: Option<&str>,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let (_db, sandbox_id, substrate, cluster) = start_subject("stopped", false).await?;
    cluster.node.report(report);
    if container_ends {
        cluster.pods.end_container();
    }

    // An unsettled upgrade runs to the start's deadline.
    let ready_timeout = match refusal {
        Some(_) => Duration::from_secs(2),
        None => Duration::from_millis(800),
    };
    let Err(error) = start_within(&substrate, sandbox_id, ready_timeout).await else {
        panic!("the start was not refused");
    };

    match refusal {
        Some(reason) => {
            let SubstrateError::Unavailable { message } = &error else {
                panic!("{error:?}");
            };
            assert!(message.contains(reason), "{message}");
            // Only an operation the node says claimed its start says so.
            assert_eq!(
                message.contains("claimed"),
                reason.contains("claimed"),
                "{message}"
            );
        }
        None => assert!(matches!(error, SubstrateError::Timeout { .. }), "{error:?}"),
    }
    assert!(wakes(&cluster).is_empty(), "{:?}", wakes(&cluster));
    Ok(())
}

/// A start whose decided upgrade didn't take effect never reports it as
/// done: the sandbox runs on its old environment and the upgrade stays
/// visibly not done. The node rolls the start's pod back and keeps the
/// sandbox published on E0, so the start is refused and its retry boots the
/// same upgrade again rather than starting the sandbox plainly. Only once an
/// operator allows E0 does a start fall back to a plain start, which runs the
/// sandbox on E0; nothing it records names E1, so the next decision, once E0
/// is blocked again, still moves the sandbox.
#[tokio::test(flavor = "current_thread")]
async fn a_start_whose_upgrade_rolled_back_never_reports_it_done() -> Result<(), BoxError> {
    use pplx_space_runtime_environment_policy_store::{
        PolicyChange, PolicyChangeReason, RuntimeEnvironmentState, set_environment_state,
    };
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let (db, sandbox_id, substrate, cluster) = start_subject("stopped", false).await?;
    let set_e0 = |state| {
        let pool = db.pool.clone();
        async move {
            let mut tx = pool.begin().await?;
            set_environment_state(
                &mut tx,
                E0.parse()?,
                state,
                &PolicyChange {
                    actor: None,
                    reason: PolicyChangeReason::new("test policy")?,
                },
            )
            .await?;
            tx.commit().await?;
            Ok::<_, BoxError>(())
        }
    };
    cluster.node.report(Report::RolledBack);
    for attempt in ["first", "retry"] {
        let Err(SubstrateError::Unavailable { message }) = start(&substrate, sandbox_id).await
        else {
            panic!("the {attempt} start wasn't refused retryably");
        };
        assert!(message.contains("rolled back"), "{attempt}: {message}");
    }
    assert!(wakes(&cluster).is_empty(), "{:?}", wakes(&cluster));
    let operation = cluster
        .pods
        .created_annotations(SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION)[0]
        .clone()
        .unwrap();
    cluster
        .pods
        .assert_every_create_upgrades(&operation, "started");
    assert_eq!(cluster.pods.created_uids().len(), 2);

    // An operator allows E0: the decision keeps the sandbox there, and a
    // fresh substrate reads the policy uncached.
    set_e0(RuntimeEnvironmentState::Allowed).await?;
    cluster.pods.run_containers();
    let substrate = cluster.substrate(&db.pool);
    start(&substrate, sandbox_id).await?;
    assert_eq!(wakes(&cluster).len(), 1);
    let checkpoints = cluster
        .pods
        .created_annotations(SANDBOX_CHECKPOINT_ANNOTATION);
    assert_ne!(
        checkpoints.last().unwrap().as_deref(),
        Some(RUNTIME_UPGRADE_CHECKPOINT),
        "{checkpoints:?}"
    );

    set_e0(RuntimeEnvironmentState::Blocked).await?;
    let substrate = cluster.substrate(&db.pool);
    let decided = substrate
        .decide_runtime_upgrade(
            sandbox_id,
            crate::controllers::substrate::runtime_upgrade::UpgradeActivation::Start,
            None,
        )
        .await?
        .expect("the upgrade is still due");
    assert_eq!(decided.movement.from.to_string(), E0);
    assert_eq!(decided.movement.to.to_string(), E1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_evacuation_restore_keeps_the_sandboxs_environment() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (mut substrate, _wake_attempts, _pod_gets, worker) =
        paused_restore_test_subject(&db.pool, sandbox_id, Some(1), true).await?;
    bind_snapshots_to(&db.pool, sandbox_id, E0).await?;
    block_e0_for_e1(&db.pool, &[E1]).await?;
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;

    // A drain moves the sandbox as it is, even into the started lifecycle.
    let result = substrate
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
        .await;

    let received = worker.received.lock().unwrap().clone();
    assert!(!received.is_empty(), "{result:?}");
    assert!(
        received.iter().all(|wake| wake.runtime_upgrade.is_none()),
        "{received:?}"
    );
    Ok(())
}

/// The policy cache may lag a policy change by its TTL, which is fine for
/// what only moves a stopped disk, but a resume discards memory only on the
/// policy as it is now.
#[tokio::test(flavor = "current_thread")]
async fn a_resume_discards_memory_only_under_the_current_policy() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (mut substrate, _worker) = user_restore_subject(&db.pool, sandbox_id).await?;
    block_e0_for_e1(&db.pool, &[E1]).await?;
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;
    let decide = || {
        substrate.activation_runtime_upgrade(
            sandbox_id,
            crate::controllers::substrate::runtime_upgrade::UpgradeActivation::Restore,
            None,
        )
    };
    // Blocked: the decision upgrades, and the policy is now cached.
    assert!(decide().await?.is_some());

    // An operator allows E0 again within the cache's TTL.
    {
        use pplx_space_runtime_environment_policy_store::{
            PolicyChange, PolicyChangeReason, RuntimeEnvironmentState, set_environment_state,
        };
        let mut tx = db.pool.begin().await?;
        set_environment_state(
            &mut tx,
            E0.parse()?,
            RuntimeEnvironmentState::Allowed,
            &PolicyChange {
                actor: None,
                reason: PolicyChangeReason::new("test unblock")?,
            },
        )
        .await?;
        tx.commit().await?;
    }
    assert!(
        decide().await?.is_none(),
        "the resume must restore as recorded, keeping its memory"
    );
    Ok(())
}

/// A checkpoint from before execution metadata, on a cluster that permits
/// legacy unbound work: node-manager restores it as a historical contract
/// whatever E0's state, so the control plane neither strands it nor moves
/// it, even with E0 blocked.
#[tokio::test(flavor = "current_thread")]
async fn a_pre_metadata_checkpoint_restores_as_recorded_while_legacy_work_is_permitted()
-> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (mut substrate, _worker) = user_restore_subject(&db.pool, sandbox_id).await?;
    sqlx::query(sql!(
        "UPDATE snapshots
            SET execution_metadata = NULL, runtime = NULL, runtime_version = NULL,
                state_format_name = NULL, state_format_version = NULL
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    block_e0_for_e1(&db.pool, &[E1]).await?;
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;

    let upgrade = substrate
        .activation_runtime_upgrade(
            sandbox_id,
            crate::controllers::substrate::runtime_upgrade::UpgradeActivation::Restore,
            None,
        )
        .await?;
    assert_eq!(upgrade, None);
    Ok(())
}

/// A resume's upgrade must name the checkpoint the node may boot, by its
/// local artifact. A checkpoint without that identity is left to the node's
/// own restore rather than upgraded on a copy nothing can vouch for.
#[tokio::test(flavor = "current_thread")]
async fn a_resume_upgrades_only_a_checkpoint_with_a_local_artifact_identity() -> Result<(), BoxError>
{
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (mut substrate, _worker) = user_restore_subject(&db.pool, sandbox_id).await?;
    block_e0_for_e1(&db.pool, &[E1]).await?;
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;
    let storage_uri: String = sqlx::query_scalar(sql!(
        "SELECT ssl.storage_connection_string
           FROM snapshot_storage_locations ssl
           JOIN snapshots sn ON sn.id = ssl.snapshot_id
          WHERE sn.sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    let decide = || {
        substrate.activation_runtime_upgrade(
            sandbox_id,
            crate::controllers::substrate::runtime_upgrade::UpgradeActivation::Restore,
            Some(&storage_uri),
        )
    };
    assert!(
        decide().await?.is_some(),
        "the identified checkpoint upgrades"
    );

    sqlx::query(sql!(
        "UPDATE snapshots
            SET local_artifact_id = NULL, artifact_kind = NULL, artifact_origin = NULL
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    assert_eq!(decide().await?, None);
    Ok(())
}

/// Only a start's or resume's restore pod renders an upgrade. A cold create's
/// context can't carry one, so a materialization handed an upgrade creates
/// its pod without it, and nothing boots the upgrade there.
#[tokio::test(flavor = "current_thread")]
async fn an_upgrade_is_never_rendered_on_a_cold_create_pod() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    // A created sandbox with no prior state materializes on a cold create pod.
    insert_test_sandbox_row(&db.pool, sandbox_id, "created").await?;
    let node_id = pplx_space_k8s_node_id::space_k8s_node_id("node-a", "worker");
    let cluster = Cluster::start(&db.pool, node_id, backup_enqueued()).await?;
    let substrate = cluster.substrate(&db.pool);
    let sandbox = substrate.store.require_sandbox(sandbox_id).await?;
    let upgrade = super::super::pod_runtime_upgrade::PodUpgrade {
        operation_id: uuid::Uuid::now_v7(),
        target: E1.parse()?,
        authorization_id: format!("runtime-environment-blocked:{E0}"),
        authorization_reason: "test".to_string(),
        memory_loss: false,
        target_state: pplx_space_common_core::kubernetes::PodRuntimeUpgradeTargetState::Started,
        checkpoint_local_artifact_id: None,
    };

    substrate
        .ensure_pod_exists(
            &sandbox,
            None,
            None,
            None,
            super::super::PodMaterializationOwnership::new(None, None, None)
                .upgrading_runtime(Some(upgrade)),
        )
        .await?;

    let checkpoints = cluster
        .pods
        .created_annotations(SANDBOX_CHECKPOINT_ANNOTATION);
    assert_eq!(checkpoints.len(), 1, "{checkpoints:?}");
    assert_ne!(
        checkpoints[0].as_deref(),
        Some(RUNTIME_UPGRADE_CHECKPOINT),
        "a cold create pod booted the upgrade"
    );
    assert_eq!(
        cluster
            .pods
            .created_annotations(SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION),
        vec![None]
    );
    Ok(())
}

/// A resume whose pod doesn't run the sandbox on the target is refused
/// before its Wake, with a retryable refusal. A rollback left the sandbox on
/// its checkpoint, so it goes back to `suspended`, restorable. An upgrade
/// that reached the target before the pod failed booted the sandbox and
/// retired the checkpoint this restore read, which may exist nowhere else:
/// the restore stays `restoring`, placed on the node, never reverted, and
/// the node's publication of the sandbox stopped converges it there.
#[rstest::rstest]
#[case::rolled_back(Report::RolledBack, false, "rolled back", SandboxLifecycle::Suspended)]
#[case::pod_failed_after_the_target(
    Report::Upgraded { publish: false },
    true,
    "create was refused",
    SandboxLifecycle::Restoring
)]
#[case::create_refused_after_the_target(
    Report::PodRefused { already_on_target: false },
    false,
    "but its pod's create was refused",
    SandboxLifecycle::Restoring
)]
#[tokio::test(flavor = "current_thread")]
async fn a_resume_whose_pod_doesnt_run_the_upgraded_sandbox_never_wakes(
    #[case] report: Report,
    #[case] container_ends: bool,
    #[case] reason: &str,
    #[case] left: SandboxLifecycle,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (mut substrate, cluster) = user_restore_subject(&db.pool, sandbox_id).await?;
    block_e0_for_e1(&db.pool, &[E1]).await?;
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;
    cluster.node.report(report);
    if container_ends {
        cluster.pods.end_container();
    }

    let error = restore(&substrate, sandbox_id).await.unwrap_err();

    let SubstrateError::Unavailable { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains(reason), "{message}");
    assert!(wakes(&cluster).is_empty(), "{:?}", wakes(&cluster));
    let (lifecycle, active_node_id): (String, Option<pplx_space_common_core::types::NodeId>) =
        sqlx::query_as(sql!(
            "SELECT lifecycle, active_node_id FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(lifecycle, left.as_str());
    if left == SandboxLifecycle::Restoring {
        assert!(
            active_node_id.is_some(),
            "the restore left the upgraded sandbox on no node"
        );
    }
    Ok(())
}

/// A restore or a start that joins another attempt's claim on the execution,
/// one still inside its lease targeting `started`, reads the pod that
/// attempt places. While that attempt hasn't sent the pod's create, no pod
/// at the execution's name settles nothing: the owner's pod may still boot
/// the upgrade. So the joiner waits to its deadline and answers its
/// retryable timeout, which its cleanup treats as node work that may still
/// run: a restore keeps the sandbox `restoring` rather than reverting it to
/// `suspended`, and the owner's claim and execution stay as they were.
#[rstest::rstest]
#[case::restore(true)]
#[case::start(false)]
#[tokio::test(flavor = "current_thread")]
async fn a_joiner_doesnt_settle_on_a_pod_its_owner_hasnt_created(
    #[case] restores: bool,
) -> Result<(), BoxError> {
    use std::sync::atomic::Ordering;
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let (_db, pool, sandbox_id, mut substrate, cluster) = if restores {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let (substrate, cluster) = user_restore_subject(&pool, sandbox_id).await?;
        block_e0_for_e1(&pool, &[E1]).await?;
        (db, pool, sandbox_id, substrate, cluster)
    } else {
        let (db, sandbox_id, substrate, cluster) = start_subject("stopped", false).await?;
        let pool = db.pool.clone();
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions SET active = TRUE WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
        (db, pool, sandbox_id, substrate, cluster)
    };
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;
    // The owner claimed the execution and hasn't created its pod: nothing
    // recorded a pod UID or an address. A start's execution names no node
    // either, so without the pod the wait would read the operation as never
    // begun. A restore's keeps the node holding its checkpoint, which the
    // restore needs as its source, so without the pod the wait would read
    // the node's report as the pod's create refused.
    let owner: uuid::Uuid = sqlx::query_scalar(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = attempt.id,
                activation_attempt_target_state = 'started',
                activation_attempt_expires_at = NOW() + INTERVAL '5 minutes',
                state = 'creating', pod_uid = NULL, pod_ip = NULL,
                k8s_node_name = CASE WHEN $2 THEN k8s_node_name END
           FROM (SELECT gen_random_uuid() AS id) attempt
          WHERE sandbox_id = $1 AND active
      RETURNING activation_attempt_id"
    ))
    .bind(sandbox_id)
    .bind(restores)
    .fetch_one(&pool)
    .await?;
    *cluster.pods.pod.lock().unwrap() = None;

    let error = if restores {
        restore_within(&substrate, sandbox_id, Duration::from_millis(800))
            .await
            .map(drop)
    } else {
        start_within(&substrate, sandbox_id, Duration::from_millis(800))
            .await
            .map(drop)
    }
    .expect_err("the joiner settled");

    assert!(matches!(error, SubstrateError::Timeout { .. }), "{error:?}");
    assert_eq!(cluster.pods.creates.load(Ordering::SeqCst), 0);
    assert_eq!(cluster.pods.deletes.load(Ordering::SeqCst), 0);
    assert!(wakes(&cluster).is_empty(), "{:?}", wakes(&cluster));
    let (lifecycle, attempt, active): (String, Option<uuid::Uuid>, Option<bool>) =
        sqlx::query_as(sql!(
            "SELECT s.lifecycle, e.activation_attempt_id, e.active
               FROM sandboxes s
               LEFT JOIN sandbox_k8s_executions e
                      ON e.sandbox_id = s.id AND e.activation_attempt_id IS NOT NULL
              WHERE s.id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await?;
    if restores {
        assert_eq!(lifecycle, SandboxLifecycle::Restoring.as_str());
    }
    assert_eq!((attempt, active), (Some(owner), Some(true)));
    Ok(())
}

/// The materialization fails after it created the pod carrying the upgrade:
/// the pod's node is gone from the API by the time its placement is
/// recorded, or the pod's metadata can't be persisted. The pod's shim may be
/// running the upgrade, so the failure isn't a clean one and the pod isn't
/// deleted: a restore keeps `restoring` and its pod instead of reverting to
/// `suspended`, and a start keeps its attempt's claim on the execution until
/// its lease ends, as for an upgrade that hasn't settled.
#[rstest::rstest]
#[case::restore_node_gone(true, false)]
#[case::start_node_gone(false, false)]
#[case::restore_metadata_unpersisted(true, true)]
#[case::start_metadata_unpersisted(false, true)]
#[tokio::test(flavor = "current_thread")]
async fn a_materialization_failing_after_its_upgrade_pod_was_created_isnt_clean(
    #[case] restores: bool,
    #[case] metadata_unpersisted: bool,
) -> Result<(), BoxError> {
    use std::sync::atomic::Ordering;
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let (_db, pool, sandbox_id, cluster) = if restores {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let (_, cluster) = user_restore_subject(&pool, sandbox_id).await?;
        block_e0_for_e1(&pool, &[E1]).await?;
        (db, pool, sandbox_id, cluster)
    } else {
        let (db, sandbox_id, _, cluster) = start_subject("stopped", false).await?;
        let pool = db.pool.clone();
        (db, pool, sandbox_id, cluster)
    };
    let mut substrate = if metadata_unpersisted {
        // The store refuses to record the created pod's UID.
        sqlx::raw_sql(sql!(
            "CREATE FUNCTION refuse_pod_uid() RETURNS trigger AS $$
             BEGIN RAISE EXCEPTION 'pod metadata refused'; END $$ LANGUAGE plpgsql;
             CREATE TRIGGER refuse_pod_uid
                 BEFORE UPDATE OF pod_uid ON sandbox_k8s_executions
                 FOR EACH ROW WHEN (NEW.pod_uid IS NOT NULL)
                 EXECUTE FUNCTION refuse_pod_uid();"
        ))
        .execute(&pool)
        .await?;
        cluster.substrate(&pool)
    } else {
        // Placement reads the Node from the API, which no longer has it.
        cluster.pods.node_gone.store(true, Ordering::SeqCst);
        cluster.substrate_with_nodes(&pool, Vec::new())
    };
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;

    let error = if restores {
        restore(&substrate, sandbox_id).await.map(drop)
    } else {
        start(&substrate, sandbox_id).await.map(drop)
    }
    .expect_err("the materialization failed");

    // Retryable either way (503).
    assert!(
        matches!(
            error,
            SubstrateError::Unavailable { .. } | SubstrateError::RestoreInFlight { .. }
        ),
        "{error:?}"
    );
    assert_eq!(cluster.pods.creates.load(Ordering::SeqCst), 1);
    assert!(cluster.pods.exists(), "the upgrade's pod was deleted");
    assert!(wakes(&cluster).is_empty(), "{:?}", wakes(&cluster));
    let (lifecycle, attempt_live): (String, bool) = sqlx::query_as(sql!(
        "SELECT s.lifecycle,
                COALESCE(e.activation_attempt_id IS NOT NULL
                         AND e.activation_attempt_expires_at > NOW(), FALSE)
           FROM sandboxes s
           LEFT JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id AND e.active
          WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&pool)
    .await?;
    if restores {
        assert_eq!(lifecycle, SandboxLifecycle::Restoring.as_str());
    } else {
        assert!(attempt_live, "the start gave its claim up");
    }
    Ok(())
}

/// How a cross-node re-drive fails once it sent the create of a successor
/// that renders a runtime upgrade.
#[derive(Clone, Copy, Debug)]
enum RedriveFailure {
    /// The store refuses to record the successor's UID.
    MetadataUnpersisted,
    /// Kubernetes commits the successor but its answer is lost.
    CreateAnswerLost,
    /// The request is dropped while the create is in flight.
    Cancelled,
}

/// An upgrade-enabled restore whose source-pinned pod came back
/// unschedulable re-drives it cross-node, and the successor's shim runs the
/// upgrade at container create. A failure once that create was sent (the
/// successor's metadata can't be persisted, or Kubernetes committed it and
/// its answer was lost), or the request being dropped there, leaves the
/// successor and its `creating` claim for the operation's retry, which
/// hydrates the pod and reads the outcome. The failure answers retryably.
#[rstest::rstest]
#[case::metadata_unpersisted(RedriveFailure::MetadataUnpersisted)]
#[case::create_answer_lost(RedriveFailure::CreateAnswerLost)]
#[case::cancelled(RedriveFailure::Cancelled)]
#[tokio::test(flavor = "current_thread")]
async fn a_redrive_failing_after_its_upgrade_successor_was_sent_keeps_it(
    #[case] failure: RedriveFailure,
) -> Result<(), BoxError> {
    use std::sync::atomic::Ordering;

    use crate::controllers::substrate::kubernetes::pod_runtime_upgrade::PodUpgrade;
    use pplx_space_common_core::kubernetes::PodRuntimeUpgradeTargetState;
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (substrate, cluster) = user_restore_subject(&db.pool, sandbox_id).await?;
    // An earlier attempt of the restore created the source-pinned pod and
    // persisted its UID, and the scheduler then found the source full.
    sqlx::raw_sql(sql!(
        "UPDATE sandbox_k8s_executions SET state = 'created', pod_ip = NULL;
         UPDATE sandboxes SET lifecycle = 'restoring';"
    ))
    .execute(&db.pool)
    .await?;
    let claim = substrate
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?;
    *cluster.pods.pod.lock().unwrap() = Some(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": claim.row.pod_name,
            "namespace": "space",
            "uid": format!("{}-uid", claim.row.pod_name),
            "resourceVersion": "7",
        },
        "spec": {
            "affinity": { "nodeAffinity": { "requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [{ "matchFields": [{
                    "key": "metadata.name", "operator": "In", "values": ["node-a"],
                }] }],
            } } },
            "containers": [{ "name": "sandbox", "image": "sandbox" }],
        },
        "status": {
            "phase": "Pending",
            "conditions": [{
                "type": "PodScheduled", "status": "False", "reason": "Unschedulable",
                "message": "0/1 nodes are available",
            }],
        },
    }));
    match failure {
        RedriveFailure::MetadataUnpersisted => {
            sqlx::raw_sql(sql!(
                "CREATE FUNCTION refuse_pod_uid() RETURNS trigger AS $$
                 BEGIN RAISE EXCEPTION 'pod metadata refused'; END $$ LANGUAGE plpgsql;
                 CREATE TRIGGER refuse_pod_uid
                     BEFORE UPDATE OF pod_uid ON sandbox_k8s_executions
                     FOR EACH ROW WHEN (NEW.pod_uid IS NOT NULL)
                     EXECUTE FUNCTION refuse_pod_uid();"
            ))
            .execute(&db.pool)
            .await?;
        }
        RedriveFailure::CreateAnswerLost => cluster
            .pods
            .creates_fail_after_commit
            .store(true, Ordering::SeqCst),
        RedriveFailure::Cancelled => cluster
            .pods
            .creates_hang_after_commit
            .store(true, Ordering::SeqCst),
    }
    let sandbox = substrate.store.require_sandbox(sandbox_id).await?;
    let upgrade = PodUpgrade {
        operation_id: uuid::Uuid::now_v7(),
        target: E1.parse()?,
        authorization_id: "test-authorization".to_string(),
        authorization_reason: "test".to_string(),
        memory_loss: true,
        target_state: PodRuntimeUpgradeTargetState::Started,
        checkpoint_local_artifact_id: None,
    };

    let redrive = substrate.redrive_unschedulable_restore_cross_node_recorded(
        crate::metrics::sandbox::restore::RedrivePath::Retry,
        &sandbox,
        &claim,
        SandboxLifecycle::Restoring,
        Some("node-a"),
        None,
        None,
        Some(&upgrade),
    );
    if matches!(failure, RedriveFailure::Cancelled) {
        tokio::time::timeout(Duration::from_millis(300), redrive)
            .await
            .err()
            .expect("the re-drive finished while its create hung");
        // Let a cancellation cleanup, if one was spawned, run.
        tokio::time::sleep(Duration::from_millis(200)).await;
    } else {
        let error = match redrive.await {
            Err(error) => error,
            Ok(outcome) => panic!(
                "the re-drive ended {}",
                match outcome {
                    super::super::pod_ops::RedriveOutcome::Redriven(_) => "redriven",
                    super::super::pod_ops::RedriveOutcome::PodRetained { .. } =>
                        "with the pod retained",
                    super::super::pod_ops::RedriveOutcome::OwnershipLost => "with ownership lost",
                }
            ),
        };
        assert!(
            matches!(error, SubstrateError::RestoreInFlight { .. }),
            "{error:?}"
        );
    }

    assert_eq!(cluster.pods.creates.load(Ordering::SeqCst), 1);
    // Only the unschedulable pinned pod was deleted.
    assert_eq!(cluster.pods.deletes.load(Ordering::SeqCst), 1);
    assert!(cluster.pods.exists(), "the upgrade's successor was deleted");
    let claim_row: Option<(String, bool)> = sqlx::query_as(sql!(
        "SELECT state, active FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(claim.row.generation)
    .fetch_optional(&db.pool)
    .await?;
    assert_eq!(claim_row, Some(("creating".to_string(), true)));
    Ok(())
}

/// The live activation claim on the sandbox's executions, if any: its
/// attempt, and whether its lease still runs.
async fn live_claim(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
) -> Result<Option<(uuid::Uuid, bool)>, BoxError> {
    Ok(sqlx::query_as(sql!(
        "SELECT activation_attempt_id, activation_attempt_expires_at > NOW()
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active AND activation_attempt_id IS NOT NULL"
    ))
    .bind(sandbox_id)
    .fetch_optional(pool)
    .await?)
}

/// A start's or a restore's pod whose upgrade the node rolled back, or
/// refused before it began, for that pod, or a start's pod whose create the
/// node refused after it upgraded the sandbox or found it on the target (a
/// restore's is never reverted; see
/// `a_resume_whose_pod_doesnt_run_the_upgraded_sandbox_never_wakes`): the pod stays
/// pending in its create, as a failed sandbox create leaves it, and the
/// report naming it settles the activation anyway. The activation deletes that pod and gives
/// up its attempt, so the retry isn't held off by its lease and places a new
/// pod with a new UID, which the node takes as a new attempt, here one that
/// boots and Wakes.
#[rstest::rstest]
#[case::start_rolled_back(false, Report::RolledBack, "rolled back")]
#[case::start_refused(false, Report::Refused, "was refused before it began")]
#[case::restore_rolled_back(true, Report::RolledBack, "rolled back")]
#[case::restore_refused(true, Report::Refused, "was refused before it began")]
#[case::start_upgraded_pod_refused(
    false,
    Report::PodRefused { already_on_target: false },
    "but its pod's create was refused, so the pod doesn't run it (the sandbox runs another"
)]
#[case::start_on_target_pod_refused(
    false,
    Report::PodRefused { already_on_target: true },
    "where it already was, but its pod's create was refused"
)]
#[tokio::test(flavor = "current_thread")]
async fn a_settled_refusal_deletes_its_pod_and_the_retry_gets_a_new_one(
    #[case] restores: bool,
    #[case] report: Report,
    #[case] reason: &str,
) -> Result<(), BoxError> {
    use std::sync::atomic::Ordering;
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let (_db, pool, sandbox_id, mut substrate, cluster) = if restores {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let (substrate, cluster) = user_restore_subject(&pool, sandbox_id).await?;
        block_e0_for_e1(&pool, &[E1]).await?;
        (db, pool, sandbox_id, substrate, cluster)
    } else {
        let (db, sandbox_id, substrate, cluster) = start_subject("stopped", false).await?;
        let pool = db.pool.clone();
        (db, pool, sandbox_id, substrate, cluster)
    };
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;
    cluster.node.report(report);
    let activate = async |substrate: &KubernetesSubstrate| {
        if restores {
            restore(substrate, sandbox_id).await.map(drop)
        } else {
            start(substrate, sandbox_id).await.map(drop)
        }
    };

    let error = activate(&substrate)
        .await
        .expect_err("the activation settled");

    let SubstrateError::Unavailable { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains(reason), "{message}");
    assert!(wakes(&cluster).is_empty(), "{:?}", wakes(&cluster));
    assert!(!cluster.pods.exists(), "the settled pod is still there");
    assert_eq!(live_claim(&pool, sandbox_id).await?, None);

    cluster.pods.run_containers();
    cluster.node.report(Report::Upgraded { publish: false });
    if restores {
        // A user restore claims only once the old execution has gone quiet.
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET updated_at = NOW() - INTERVAL '1 hour'
              WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
    }
    // The retry's own outcome doesn't matter: only the pod it reads.
    let _ = activate(&substrate).await;

    assert_eq!(cluster.pods.creates.load(Ordering::SeqCst), 2);
    let uids = cluster.pods.created_uids();
    assert_ne!(uids[0], uids[1], "the retry reused the settled pod");
    assert!(!wakes(&cluster).is_empty(), "the retry never woke its pod");
    Ok(())
}

/// A rollback the node reports for another pod (an earlier attempt's) or
/// for no pod (a node that predates the pod field) is no proof the
/// activation's pod is done: the activation waits to its deadline, keeps its
/// pod, which it never deletes, and keeps its claim's lease, as for any
/// outcome it can't confirm. That holds even once the pod's container ended.
/// The same goes for a create the node refused for an earlier pod after it
/// upgraded the sandbox.
#[rstest::rstest]
#[case::start_earlier_pod(false, Report::EarlierRollback, false)]
#[case::start_old_node(false, Report::UnnamedRollback, false)]
#[case::start_old_node_ended(false, Report::UnnamedRollback, true)]
#[case::restore_earlier_pod(true, Report::EarlierRollback, false)]
#[case::restore_old_node_ended(true, Report::UnnamedRollback, true)]
#[case::start_earlier_pod_refused(false, Report::EarlierPodRefused, false)]
#[case::restore_earlier_pod_refused(true, Report::EarlierPodRefused, false)]
#[tokio::test(flavor = "current_thread")]
async fn a_rollback_naming_no_pod_or_another_never_settles_or_deletes(
    #[case] restores: bool,
    #[case] report: Report,
    #[case] container_ends: bool,
) -> Result<(), BoxError> {
    use std::sync::atomic::Ordering;
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let (_db, pool, sandbox_id, mut substrate, cluster) = if restores {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        let (substrate, cluster) = user_restore_subject(&pool, sandbox_id).await?;
        block_e0_for_e1(&pool, &[E1]).await?;
        (db, pool, sandbox_id, substrate, cluster)
    } else {
        let (db, sandbox_id, substrate, cluster) = start_subject("stopped", false).await?;
        let pool = db.pool.clone();
        (db, pool, sandbox_id, substrate, cluster)
    };
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;
    cluster.pods.keep_creating();
    cluster.node.report(report);
    if container_ends {
        cluster.pods.end_container();
    }

    let error = if restores {
        restore_within(&substrate, sandbox_id, Duration::from_millis(800))
            .await
            .map(drop)
    } else {
        start_within(&substrate, sandbox_id, Duration::from_millis(800))
            .await
            .map(drop)
    }
    .expect_err("the activation settled");

    assert!(matches!(error, SubstrateError::Timeout { .. }), "{error:?}");
    assert!(cluster.pods.exists(), "the pod was deleted");
    assert_eq!(cluster.pods.deletes.load(Ordering::SeqCst), 0);
    assert!(wakes(&cluster).is_empty(), "{:?}", wakes(&cluster));
    assert!(
        live_claim(&pool, sandbox_id)
            .await?
            .is_some_and(|(_, live)| live),
        "the activation let its claim go"
    );
    Ok(())
}
