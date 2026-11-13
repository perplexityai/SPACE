//! Upgrading a stopped sandbox's runtime without starting it, against
//! Postgres, a scripted node and a Kubernetes API: the operation places the
//! sandbox's pod in its runtime-upgrade mode, reads the outcome from the
//! node's `GetRuntimeUpgrade`, and backs up and retires what reached the
//! target. Kubernetes only: Direct refuses the operation, under the
//! stopped-sandbox runtime upgrade row of the `direct.rs` gap table.

use std::sync::atomic::Ordering;
use std::time::Duration;

use pplx_space_common_core::kubernetes::{
    PodRuntimeUpgradeTargetState, SANDBOX_RUNTIME_UPGRADE_AUTHORIZATION_ID_ANNOTATION,
    SANDBOX_RUNTIME_UPGRADE_MEMORY_LOSS_ANNOTATION,
    SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION,
    SANDBOX_RUNTIME_UPGRADE_TARGET_ENVIRONMENT_ANNOTATION,
    SANDBOX_RUNTIME_UPGRADE_TARGET_STATE_ANNOTATION,
};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::SandboxTransitionTargetState;

use super::runtime_upgrade_dispatch::{E0, E1};
use super::*;
use crate::controllers::substrate::kubernetes::scripted_test_doubles::{
    BoxError, ScriptedReply, WORKER_NODE,
};
use crate::controllers::substrate::runtime_upgrade::{StoppedRuntimeUpgrade, UpgradeActivation};

pub(super) mod fixture;
use fixture::*;

mod backup;
mod claims;
mod outcomes;
mod pods;
mod prechecks;
mod retire;

/// Give the sandbox an active execution whose attempt targets `target` under
/// a lease ending `lease_secs` from now, with its pod in place: what a start
/// or an earlier upgrade leaves while it runs, or after it crashed.
async fn held_execution(s: &Subject, target: &str, lease_secs: i64) -> Result<(), BoxError> {
    let pod_name = format!("space-sbx-{}-g7", s.sandbox_id.as_uuid().simple());
    insert_k8s_execution(
        &s.db.pool,
        s.sandbox_id,
        7,
        &pod_name,
        &format!("{pod_name}-uid"),
        Some("127.0.0.1"),
        true,
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = attempt.id,
                route_activation_id = attempt.id,
                activation_attempt_target_state = $2,
                activation_attempt_expires_at = NOW() + make_interval(secs => $3::BIGINT),
                k8s_node_name = 'node-a'
           FROM (SELECT gen_random_uuid() AS id) attempt
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(s.sandbox_id)
    .bind(target)
    .bind(lease_secs)
    .execute(&s.db.pool)
    .await?;
    *s.pods().pod.lock().unwrap() = Some(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": pod_name, "namespace": "space", "uid": format!("{pod_name}-uid") },
        "spec": { "nodeName": "node-a" },
        "status": { "phase": "Running", "podIP": "127.0.0.1" }
    }));
    Ok(())
}

/// Leave the execution [`held_execution`] made as an activation that ended
/// between creating its pod and persisting it does: `creating`, with no pod
/// UID, address or node recorded.
async fn unpersist_held_execution(s: &Subject) -> Result<(), BoxError> {
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'creating', pod_uid = NULL, pod_ip = NULL, k8s_node_name = NULL
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    Ok(())
}

/// Record an upgrade to E1 on the execution [`held_execution`] left, as an
/// earlier attempt of the operation does before it places its pod. Answers
/// its operation.
async fn record_upgrade_on_held_execution(s: &Subject) -> Result<uuid::Uuid, BoxError> {
    let operation = uuid::Uuid::now_v7();
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET runtime_upgrade_operation_id = $2,
                runtime_upgrade_target_environment_ref = $3,
                runtime_upgrade_authorization_id = $4
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(s.sandbox_id)
    .bind(operation)
    .bind(E1)
    .bind(format!("runtime-environment-blocked:{E0}"))
    .execute(&s.db.pool)
    .await?;
    Ok(operation)
}

/// The attempt holding the claim a retry reads before its takeover.
async fn left_by(
    s: &Subject,
) -> Result<pplx_space_common_core::types::SandboxActivationAttemptId, BoxError> {
    Ok(s.substrate
        .store
        .stopped_runtime_upgrade_claim(s.sandbox_id)
        .await?
        .ok_or("no stopped runtime upgrade claim")?
        .activation_attempt_id)
}

/// A user start of `s`'s sandbox, with a short readiness budget.
async fn start_the_sandbox(
    s: &Subject,
) -> Result<crate::controllers::substrate::ExecutionActivation, SubstrateError> {
    s.substrate
        .start_execution(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("start"),
            s.sandbox_id,
            crate::controllers::substrate::ActivationOptions {
                blocking: true,
                ready_timeout: Duration::from_millis(800),
            },
            crate::controllers::substrate::RecoveryCharge::exempt_for_tests(),
        )
        .await
}

/// A drain's re-placement of `s`'s stopped sandbox, with a short readiness
/// budget.
async fn re_place_off_the_drained_node(s: &Subject) -> Result<(), SubstrateError> {
    s.substrate
        .restore_execution_inner(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("node_drain.evacuate"),
            s.sandbox_id,
            crate::controllers::substrate::ActivationOptions {
                blocking: true,
                ready_timeout: Duration::from_millis(800),
            },
            crate::controllers::substrate::kubernetes::restore_intent::RestoreIntent::drain(
                None,
                pplx_space_common_core::types::SandboxLifecycle::Stopped,
            ),
        )
        .await
        .map(drop)
}
