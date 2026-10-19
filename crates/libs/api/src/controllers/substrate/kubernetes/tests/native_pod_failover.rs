//! A Native data-plane request whose kept-channel dial gets no answer within
//! a second resolves the sandbox's pod again instead of waiting out the full
//! connect budget on the old address. Every other caller keeps waiting it
//! out.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1::{Pod, PodIP, PodStatus};
use pplx_space_common_core::types::{PodUid, SandboxId};
use pplx_space_test_env::TestResult;

use super::*;
use crate::controllers::substrate::native_access;

/// The Native first dial's one second, plus the re-resolution and a dial to
/// a live peer, with room for a slow CI host; well under the full budget.
const NATIVE_FAILOVER_BOUND: Duration = Duration::from_millis(2500);

/// A listener whose accept queue is full, so the kernel drops further SYNs
/// and a dial to it gets no answer: what a lost pod, or a SYN lost on the way
/// to a live one, looks like to the dialer. The returned connections fill
/// the queue and must outlive the dial.
async fn silent_listener(
    ip: &str,
    port: u16,
) -> TestResult<(tokio::net::TcpListener, Vec<tokio::net::TcpStream>)> {
    let address: std::net::SocketAddr = format!("{ip}:{port}").parse()?;
    let socket = tokio::net::TcpSocket::new_v4()?;
    socket.bind(address)?;
    let listener = socket.listen(0)?;
    let mut filling = Vec::new();
    loop {
        match tokio::time::timeout(
            Duration::from_millis(200),
            tokio::net::TcpStream::connect(address),
        )
        .await
        {
            Ok(Ok(stream)) => filling.push(stream),
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => return Ok((listener, filling)),
        }
        assert!(filling.len() < 64, "the accept queue never filled");
    }
}

/// The instance an earlier lookup returned, at an address that no longer
/// answers.
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

/// A sandbox whose pod was replaced: the stale address is silent and the
/// replacement, on the same proxy port, serves.
struct MovedPod {
    substrate: KubernetesSubstrate,
    sandbox_id: SandboxId,
    shutdown: CancellationToken,
    _silent: (tokio::net::TcpListener, Vec<tokio::net::TcpStream>),
    _db: pplx_space_db_test_fixtures::TestDb,
}

async fn moved_pod() -> TestResult<MovedPod> {
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
    let silent = silent_listener("127.0.0.2", port).await?;
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
    Ok(MovedPod {
        substrate,
        sandbox_id,
        shutdown,
        _silent: silent,
        _db: db,
    })
}

#[tokio::test(flavor = "current_thread")]
async fn a_native_request_follows_a_moved_pod_after_a_one_second_dial() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let moved = moved_pod().await?;

    let started = Instant::now();
    let client = native_access::scope(
        moved
            .substrate
            .access_kept_worker_client(moved.sandbox_id, &stale_instance()),
    )
    .await;
    let elapsed = started.elapsed();
    moved.shutdown.cancel();

    let client = client.expect("the replacement pod is dialed");
    assert!(
        elapsed >= Duration::from_millis(900),
        "the old address had its one-second dial first: {elapsed:?}"
    );
    assert!(elapsed < NATIVE_FAILOVER_BOUND, "took {elapsed:?}");
    assert!(
        client.report.is_some(),
        "a Native client reports a slow call"
    );
    assert_eq!(
        moved.substrate.pod_worker_channels.len(),
        1,
        "only the live pod's channel is kept"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_native_request_to_a_removed_pod_returns_after_its_first_dial() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    // No active execution: the pod the caller resolved has since gone.
    insert_test_sandbox_row(&db.pool, sandbox_id, "started").await?;
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = probe.local_addr()?.port();
    drop(probe);
    let _silent = silent_listener("127.0.0.2", port).await?;
    let substrate = substrate_with_store_client_proxy_port_and_pod(
        db.pool.clone(),
        mock_kubernetes_client(Arc::new(Mutex::new(Vec::new()))),
        port,
        None,
    );

    let started = Instant::now();
    let error =
        native_access::scope(substrate.access_kept_worker_client(sandbox_id, &stale_instance()))
            .await
            .expect_err("nothing answers the old address");
    let elapsed = started.elapsed();

    assert_eq!(error.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        elapsed < NATIVE_FAILOVER_BOUND,
        "the caller's recovery path starts without waiting out the budget: {elapsed:?}"
    );
    assert_eq!(substrate.pod_worker_channels.len(), 0);
    Ok(())
}

/// Outside a Native data-plane request (ECW, lifecycle, background work) the
/// kept-channel dial is the one every caller had: the old address gets the
/// full connect budget and is not re-resolved, so the caller sees its
/// failure and recovers as before.
#[tokio::test(flavor = "current_thread")]
async fn any_other_caller_waits_out_the_full_budget_on_the_old_address() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let moved = moved_pod().await?;

    let started = Instant::now();
    let result = moved
        .substrate
        .access_kept_worker_client(moved.sandbox_id, &stale_instance())
        .await;
    let elapsed = started.elapsed();
    moved.shutdown.cancel();

    let error = result.expect_err("the replacement is not looked up");
    assert_eq!(error.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        elapsed >= Duration::from_secs(9),
        "the default connect budget is spent on the old address: {elapsed:?}"
    );
    assert_eq!(moved.substrate.pod_worker_channels.len(), 0);
    Ok(())
}
