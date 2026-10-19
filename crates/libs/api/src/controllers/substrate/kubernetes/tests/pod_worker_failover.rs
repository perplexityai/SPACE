//! A kept-channel miss whose first dial fails re-resolves the sandbox's pod
//! instead of redialing the old address for the whole connect budget.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1::{Pod, PodIP, PodStatus};
use pplx_space_common_core::types::{PodUid, SandboxId};
use pplx_space_test_env::TestResult;

use super::*;

/// Well under the old ~10s of redials to an address nothing serves.
const FAILOVER_BOUND: Duration = Duration::from_secs(4);

/// The address an earlier lookup returned for a pod since replaced or
/// removed. Nothing listens there, so a dial is refused.
fn stale_instance() -> ResolvedPodInstance {
    ResolvedPodInstance {
        ip: PodIp::parse("127.0.0.2").unwrap(),
        pod_uid: Some(PodUid::parse("space-sbx-old-uid").unwrap()),
        generation: 1,
    }
}

fn running_pod(name: &str, ip: &str) -> Pod {
    Pod {
        metadata: kube::api::ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some("space".to_string()),
            uid: Some(format!("{name}-uid")),
            ..Default::default()
        },
        status: Some(PodStatus {
            phase: Some("Running".to_string()),
            pod_ip: Some(ip.to_string()),
            pod_ips: Some(vec![PodIP { ip: ip.to_string() }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_failed_first_dial_follows_the_pod_to_its_new_address() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "started").await?;
    let pod_name = "space-sbx-new";
    insert_k8s_execution(
        &db.pool,
        sandbox_id,
        2,
        pod_name,
        &format!("{pod_name}-uid"),
        Some("127.0.0.1"),
        true,
    )
    .await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let (_reporter, health) = tonic_health::server::health_reporter();
    let shutdown = CancellationToken::new();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(health)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                shutdown.clone().cancelled_owned(),
            ),
    );
    let substrate = substrate_with_store_client_proxy_port_and_pod(
        db.pool.clone(),
        mock_kubernetes_client(Arc::new(Mutex::new(Vec::new()))),
        port,
        Some(running_pod(pod_name, "127.0.0.1")),
    );

    let started = Instant::now();
    let client = substrate
        .kept_worker_client(sandbox_id, &stale_instance())
        .await;
    let elapsed = started.elapsed();
    shutdown.cancel();

    assert!(client.is_ok(), "the replacement pod is dialed: {client:?}");
    assert!(elapsed < FAILOVER_BOUND, "took {elapsed:?}");
    assert_eq!(
        substrate.pod_worker_channels.len(),
        1,
        "only the live pod's channel is kept"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_failed_first_dial_to_a_removed_pod_returns_promptly() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    // No active execution: the pod the caller resolved has since gone.
    insert_test_sandbox_row(&db.pool, sandbox_id, "started").await?;
    let substrate = substrate_with_store_client_proxy_port_and_pod(
        db.pool.clone(),
        mock_kubernetes_client(Arc::new(Mutex::new(Vec::new()))),
        9,
        None,
    );

    let started = Instant::now();
    let error = substrate
        .kept_worker_client(sandbox_id, &stale_instance())
        .await
        .expect_err("nothing serves the old address");
    let elapsed = started.elapsed();

    assert_eq!(error.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        elapsed < FAILOVER_BOUND,
        "the caller's recovery path starts without waiting out redials: {elapsed:?}"
    );
    assert_eq!(substrate.pod_worker_channels.len(), 0);
    Ok(())
}
