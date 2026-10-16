use std::time::Duration;

use super::*;
use crate::controllers::substrate::direct::DirectSubstrate;

async fn fixture() -> (RuntimeClient, Substrate) {
    let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy_with(sqlx::postgres::PgConnectOptions::new());
    // These cases must not resolve another worker or access the database.
    pool.close().await;
    (
        RuntimeClient::new(channel),
        Substrate::from_direct(DirectSubstrate::new(pool)),
    )
}

/// The `grpc.client.replay` counters `capture` saw, by their tags.
fn replay_counts(
    counters: &[pplx_space_metrics::test_capture::CapturedCounter],
) -> Vec<(u64, Vec<String>)> {
    counters
        .iter()
        .filter(|counter| counter.name == "grpc.client.replay")
        .map(|counter| {
            let mut tags = counter.tags.clone();
            tags.sort();
            (counter.value, tags)
        })
        .collect()
}

#[tokio::test]
async fn peer_errors_are_returned_without_replaying_the_request() {
    for status in [
        tonic::Status::unavailable("connection refused"),
        tonic::Status::unknown("transport error"),
        tonic::Status::internal("h2 protocol error"),
        tonic::Status::failed_precondition("execution changed"),
    ] {
        let (mut client, substrate) = fixture().await;
        let mut attempts = 0;
        let mut status = status;
        status
            .metadata_mut()
            .insert("retry-after", "1".parse().unwrap());
        let capture = pplx_space_metrics::test_capture::capture_counters_guard();

        let result: Result<(), _> = client
            .call(
                &substrate,
                SandboxId::new(),
                Instant::now() + Duration::from_secs(1),
                tonic::Request::new(()),
                |_, _| {
                    attempts += 1;
                    std::future::ready(Err(status.clone()))
                },
            )
            .await;

        let RuntimeRpcError::Rpc(returned) = result.unwrap_err() else {
            panic!("peer error must survive unchanged");
        };
        assert_eq!(attempts, 1);
        assert_eq!(replay_counts(&capture.finish()), Vec::new(), "{status:?}");
        assert_eq!(returned.code(), status.code());
        assert_eq!(returned.message(), status.message());
        assert_eq!(
            returned.metadata().get("retry-after"),
            status.metadata().get("retry-after")
        );
    }
}

#[tokio::test(start_paused = true)]
async fn deadline_bounds_the_rpc_and_prevents_expired_requests() {
    for budget in [Duration::ZERO, Duration::from_secs(1)] {
        let (mut client, substrate) = fixture().await;
        let started = Instant::now();
        let mut attempts = 0;

        let result: Result<(), _> = client
            .call(
                &substrate,
                SandboxId::new(),
                started + budget,
                tonic::Request::new(()),
                |_, _| {
                    attempts += 1;
                    std::future::pending()
                },
            )
            .await;

        assert_eq!(
            ControllerError::from(result.unwrap_err()).status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(attempts, usize::from(!budget.is_zero()));
        assert_eq!(started.elapsed(), budget);
    }
}

// A first attempt the local transport proves was never sent is a replay
// decision, counted with that proof, even when no fresh client can be found
// to replay it on; the first failure is then the call's.
#[tokio::test]
async fn a_never_sent_rpc_without_a_fresh_client_counts_its_replay_decision() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let refused = tonic_health::pb::health_client::HealthClient::new(
        tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect_lazy(),
    )
    .check(tonic_health::pb::HealthCheckRequest::default())
    .await
    .expect_err("nothing listens on a released port");
    let (mut client, substrate) = fixture().await;
    let mut attempts = 0;
    let capture = pplx_space_metrics::test_capture::capture_counters_guard();

    let result: Result<(), _> = client
        .call(
            &substrate,
            SandboxId::new(),
            Instant::now() + Duration::from_secs(1),
            tonic::Request::new(()),
            |_, _| {
                attempts += 1;
                std::future::ready(Err(refused.clone()))
            },
        )
        .await;

    assert!(result.is_err());
    assert_eq!(attempts, 1);
    assert_eq!(
        replay_counts(&capture.finish()),
        vec![(
            1,
            vec![
                "cause:refused".to_owned(),
                "outcome:no_fresh_client".to_owned(),
                "service:worker-node-api".to_owned(),
            ]
        )]
    );
}
