//! A user restore's Wake carries the runtime upgrade the policy and catalog
//! decide, only while the decision is enabled, and a stranded sandbox is
//! refused before any Wake.

use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{SandboxId, SandboxLifecycle};
use pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity;

use super::*;
use crate::controllers::substrate::runtime_upgrade::RuntimeUpgradeAtActivation;
use crate::controllers::substrate::{ActivationOptions, RecoveryCharge, SubstrateAdapter};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const E0: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const E1: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";

/// The cluster default is E1; E0, which the sandbox's checkpoint was produced
/// by, is blocked. `e0_upgrade_edges` are E0's certified upgrade edges.
async fn block_e0_for_e1(pool: &sqlx::PgPool, e0_upgrade_edges: &[&str]) -> Result<(), BoxError> {
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
/// rather than evacuated.
async fn user_restore_subject(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
) -> Result<
    (
        KubernetesSubstrate,
        kubernetes_tests::SequencedWakeWorkerHandle,
    ),
    BoxError,
> {
    let (substrate, _wake_attempts, _pod_gets, worker) =
        paused_restore_test_subject(pool, sandbox_id, Some(1), true).await?;
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
    Ok((substrate, worker))
}

async fn restore(
    substrate: &KubernetesSubstrate,
    sandbox_id: SandboxId,
) -> Result<crate::controllers::substrate::ActivationCompletion, SubstrateError> {
    substrate
        .restore_execution(
            SandboxApiMetricIdentity::native("restore"),
            sandbox_id,
            ActivationOptions {
                blocking: true,
                ready_timeout: Duration::from_secs(2),
            },
            RecoveryCharge::Exempt,
        )
        .await
}

#[tokio::test(flavor = "current_thread")]
async fn a_user_restore_off_a_blocked_environment_wakes_by_the_upgrade() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (mut substrate, worker) = user_restore_subject(&db.pool, sandbox_id).await?;
    block_e0_for_e1(&db.pool, &[E1]).await?;
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;

    let result = restore(&substrate, sandbox_id).await;

    let received = worker.received.lock().unwrap().clone();
    let wake = received
        .first()
        .unwrap_or_else(|| panic!("no Wake: {result:?}"));
    let upgrade = wake
        .runtime_upgrade
        .as_ref()
        .unwrap_or_else(|| panic!("the Wake carries no upgrade: {wake:?}"));
    assert_eq!(upgrade.target_environment, E1);
    let authorization = upgrade.authorization.as_ref().unwrap();
    assert!(authorization.memory_loss, "{authorization:?}");
    // The node boots only the checkpoint the decision read.
    let pinned: Option<uuid::Uuid> = sqlx::query_scalar(sql!(
        "SELECT current_local_artifact_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        upgrade.checkpoint_local_artifact_id,
        Some(pinned.expect("the subject pins its checkpoint").to_string())
    );
    assert!(authorization.authorization_id.contains(E0));
    // The restore keeps its placement: the node holding the checkpoint.
    assert_eq!(
        wake.start_context
            .as_ref()
            .and_then(|context| context.suspend_restore_source_node.as_deref()),
        Some("node-a"),
        "{wake:?}"
    );
    // Every attempt of this restore names the same upgrade.
    assert!(
        received
            .iter()
            .all(|wake| wake.runtime_upgrade.as_ref() == Some(upgrade)),
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
    let (substrate, worker) = user_restore_subject(&db.pool, sandbox_id).await?;
    block_e0_for_e1(&db.pool, &[E1]).await?;
    assert!(!substrate.k8s.runtime_upgrade_at_activation.is_enabled());

    let result = restore(&substrate, sandbox_id).await;

    let received = worker.received.lock().unwrap().clone();
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
    let (mut substrate, worker) = user_restore_subject(&db.pool, sandbox_id).await?;
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
    assert!(worker.received.lock().unwrap().is_empty());
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

/// Only a stopped sandbox's start moves its runtime; a start that recovers a
/// live lifecycle keeps the environment its execution runs under.
/// A deprecated environment still executes, but a stopped sandbox loses
/// nothing by moving, so it moves too.
#[rstest::rstest]
#[case::stopped("stopped", false, true)]
#[case::stopped_on_deprecated("stopped", true, true)]
#[case::started("started", false, false)]
#[tokio::test(flavor = "current_thread")]
async fn a_stopped_start_off_a_blocked_environment_wakes_by_the_upgrade(
    #[case] lifecycle: &str,
    #[case] deprecated: bool,
    #[case] upgrades: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = db.pool.clone();
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&pool, sandbox_id, lifecycle).await?;
    let node_id = pplx_space_common_svc::types::k8s::space_k8s_node_id("node-a", "worker");
    pplx_space_db_test_fixtures::insert_ready_worker_node(&pool, node_id).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(&pool)
    .await?;
    // The stopped disk's node, which its start is placed on.
    let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
    insert_k8s_execution(&pool, sandbox_id, 1, &pod_name, "uid-stopped", None, false).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET k8s_node_name = 'node-a' WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(&pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status, execution_metadata)
         VALUES ($1, $2, 'stopped', jsonb_build_object('environment', $3::text))"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(E0)
    .execute(&pool)
    .await?;
    block_e0_for_e1(&pool, &[E1]).await?;
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
    let worker = start_sequenced_wake_worker(
        std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        Some(1),
        true,
    )
    .await?;
    let mut substrate = substrate_with_store_client_proxy_port_and_pod(
        pool.clone(),
        running_pod_kubernetes_client(std::sync::Arc::default(), "127.0.0.1"),
        worker.port,
        None,
    );
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;

    let result = substrate
        .start_execution(
            SandboxApiMetricIdentity::native("start"),
            sandbox_id,
            ActivationOptions {
                blocking: true,
                ready_timeout: Duration::from_secs(2),
            },
            RecoveryCharge::Exempt,
        )
        .await;

    let received = worker.received.lock().unwrap().clone();
    let wake = received
        .first()
        .unwrap_or_else(|| panic!("no Wake: {:?}", result.as_ref().err()));
    assert!(
        received
            .iter()
            .all(|wake| wake.runtime_upgrade.is_some() == upgrades),
        "{received:?}"
    );
    if upgrades {
        let upgrade = wake.runtime_upgrade.as_ref().unwrap();
        assert_eq!(upgrade.target_environment, E1);
        // A stopped sandbox has no memory to lose.
        assert!(!upgrade.authorization.as_ref().unwrap().memory_loss);
    }
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
