//! The resources branch of the update path against the Kubernetes
//! substrate, with a simulated worker behind the sandbox's pod.

use std::time::Duration;

use pplx_space_common_core::kubernetes::SANDBOX_VCPU_ANNOTATION;

use super::*;
use crate::controllers::substrate::{
    ActivationOptions, RecoveryCharge, RetireExecutionRequest, RetireFence, SubstrateAdapter,
};
use crate::test_support::kubernetes::{
    LiveExecution, RouteIdentity, insert_kubernetes_resident, insert_live_execution, kubernetes,
    start_kubernetes_sim_worker, substrate_over_pods,
};

/// Kubernetes twin of the direct substrate's test of the same name: a live
/// CPU quota update leaves the row's activation allocation alone, so the
/// cold restart, which on Kubernetes retires the pod and creates the next
/// generation's, requests the full allocation again, and a later update
/// resolves the new pod.
///
/// The fixture's reflectors are seeded once, so the retired pod stays in
/// them and the new one is not added; the substrate finds the new pod
/// through its uncached apiserver read, not through the cache production
/// would use. Every pod shares the simulator's address, so the worker cannot
/// tell the pods apart: the apiserver read of the new pod during the later
/// update is what shows the update resolved it.
#[tokio::test(flavor = "current_thread")]
async fn live_cpu_update_preserves_allocation_across_cold_restart() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let actor = insert_actor(pool).await?;
    let (worker, node_id) = start_kubernetes_sim_worker(pool).await?;
    let sandbox_id = seed_sandbox(pool, &actor).await?.id;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET active_node_id = $2, placement = 'on_node', lifecycle = 'started', vcpu = 8
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    let LiveExecution {
        pod,
        route_activation_id,
    } = insert_live_execution(pool, sandbox_id, RouteIdentity::Published).await?;
    insert_kubernetes_resident(worker.state(), sandbox_id, "running", route_activation_id).await?;
    let (substrate, log) = substrate_over_pods(pool, worker.port(), vec![pod])?;
    let controller = SandboxController::new(kubernetes(substrate));
    let pod_requests = || -> Vec<(axum::http::Method, String, Option<serde_json::Value>)> {
        log.lock()
            .expect("kube log mutex poisoned")
            .iter()
            .map(|request| {
                (
                    request.method.clone(),
                    request.path.clone(),
                    request.body.clone(),
                )
            })
            .collect()
    };
    let mut restarted_pod = None;
    for quota in [2, 8] {
        let requests_before = pod_requests().len();
        let sandbox = fetch_sandbox(pool, sandbox_id).await?.expect("sandbox row");
        controller
            .update(
                pool,
                &sandbox,
                sandbox_id,
                UpdateSandboxRequest {
                    metadata: None,
                    vcpu: Some(quota),
                    memory_mib: None,
                    pids_max: None,
                    auto_lifecycle: None,
                    network_policy: None,
                },
                None,
            )
            .await?;
        // The simulator supplies RPC completion; this test owns persistence,
        // not real cgroup enforcement, which the supervisor tests cover.
        assert_eq!(
            fetch_sandbox(pool, sandbox_id)
                .await?
                .expect("sandbox row")
                .vcpu,
            Some(8)
        );
        if let Some(pod_name) = &restarted_pod {
            let read = format!("/api/v1/namespaces/space/pods/{pod_name}");
            assert!(
                pod_requests()[requests_before..]
                    .iter()
                    .any(|(method, path, _)| *method == axum::http::Method::GET && *path == read),
                "the update after the restart resolves {pod_name}"
            );
        }
        if quota == 2 {
            controller
                .substrate
                .retire_execution(RetireExecutionRequest {
                    sandbox_id,
                    fence: RetireFence::Unobserved,
                })
                .await?;
            controller
                .substrate
                .start_execution(
                    pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native("start"),
                    sandbox_id,
                    ActivationOptions {
                        blocking: true,
                        ready_timeout: Duration::from_secs(5),
                    },
                    RecoveryCharge::Exempt,
                )
                .await?;
            let created: Vec<serde_json::Value> = pod_requests()
                .into_iter()
                .filter(|(method, _, _)| *method == axum::http::Method::POST)
                .filter_map(|(_, _, body)| body)
                .collect();
            assert_eq!(
                created
                    .iter()
                    .map(|pod| &pod["metadata"]["annotations"][SANDBOX_VCPU_ANNOTATION])
                    .collect::<Vec<_>>(),
                vec![&serde_json::json!("8")],
                "the restart creates one pod with the activation allocation"
            );
            let pod_name = created[0]["metadata"]["name"]
                .as_str()
                .ok_or("the created pod has a name")?
                .to_owned();
            assert!(pod_name.ends_with("-g2"), "{pod_name}");
            restarted_pod = Some(pod_name);
        }
    }
    Ok(())
}
