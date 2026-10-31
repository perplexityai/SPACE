use super::*;

#[test]
fn cpu_stat_usage_usec_parses_the_usage_line() {
    let content = "usage_usec 41398047\nuser_usec 40000000\nsystem_usec 1398047\n";
    assert_eq!(cpu_stat_usage_usec(content), Some(41_398_047));
    assert_eq!(cpu_stat_usage_usec("user_usec 1\n"), None);
}

#[test]
fn inspect_warnings_are_rate_limited_per_sandbox() {
    let warnings = PerSandboxWarnings::new(Duration::from_secs(60));
    let start = Instant::now();
    assert!(warnings.admit("a", start));
    assert!(!warnings.admit("a", start + Duration::from_secs(59)));
    // Another sandbox is not held back by the first one's warning.
    assert!(warnings.admit("b", start + Duration::from_secs(1)));
    assert!(warnings.admit("a", start + Duration::from_secs(60)));
    // Admitting a warning drops every expired entry.
    assert!(warnings.admit("c", start + Duration::from_secs(120)));
    assert_eq!(warnings.last_warned.lock().unwrap().len(), 1);
}

#[test]
fn a_node_manager_outage_is_logged_when_it_starts_while_it_lasts_and_when_it_ends() {
    let outage = NodeManagerOutage::new(Duration::from_secs(60));
    let start = Instant::now();
    assert_eq!(outage.answered(start), None, "no outage to end");
    assert_eq!(outage.unreachable(start), Some(OutageLog::Started));
    // Every other sandbox polled within the interval is part of the same
    // outage.
    assert_eq!(outage.unreachable(start + Duration::from_secs(1)), None);
    assert_eq!(outage.unreachable(start + Duration::from_secs(59)), None);
    assert_eq!(
        outage.unreachable(start + Duration::from_secs(60)),
        Some(OutageLog::Continuing {
            lasted: Duration::from_secs(60)
        })
    );
    assert_eq!(outage.unreachable(start + Duration::from_secs(119)), None);
    assert_eq!(
        outage.answered(start + Duration::from_secs(90)),
        Some(Duration::from_secs(90))
    );
    assert_eq!(outage.answered(start + Duration::from_secs(91)), None);
    // The next failure is a new outage.
    assert_eq!(
        outage.unreachable(start + Duration::from_secs(92)),
        Some(OutageLog::Started)
    );
}

/// The error `Stats` gets back from its inspect when node-manager is
/// reached through `connector`, retried and bounded as `Stats` does.
async fn stats_inspect_error(
    connector: crate::node_manager_transport::NodeManagerConnector,
) -> Status {
    let node_manager = connector.lazy();
    pplx_space_common_grpc::retry_grpc_status_with_context(
        "node-manager",
        "InspectSandbox",
        pplx_space_common_grpc::DEFAULT_LOCAL_GRPC_RETRY_POLICY,
        pplx_space_common_grpc::GrpcRetryContext::with_timeout(STATS_INSPECT_TIMEOUT),
        || {
            let mut client = node_manager.clone();
            async move {
                client
                    .inspect_sandbox(
                        pplx_space_node_mgr_client::proto::InspectSandboxRequest::default(),
                    )
                    .await
            }
        },
    )
    .await
    .expect_err("the inspect must fail")
}

/// Only a node-manager that is not listening counts as unreachable: one that
/// answers with an error, even `Unavailable`, or never answers, is a fault
/// in that sandbox's inspect and keeps its per-sandbox warning.
#[tokio::test]
async fn only_a_node_manager_that_is_not_listening_is_unreachable() {
    use crate::node_manager_transport::fake::{FakeNodeManager, unbound};
    use pplx_space_common_grpc::ClientConnectFailureCause;
    use pplx_space_node_mgr_client::proto::{InspectSandboxRequest, InspectSandboxResponse};

    let sockets = tempfile::tempdir().unwrap();
    let absent = sockets.path().join("absent.sock");
    // A socket file whose listener has gone, as while node-manager restarts.
    let refused = sockets.path().join("refused.sock");
    drop(tokio::net::UnixListener::bind(&refused).unwrap());

    let error = stats_inspect_error(unbound(&absent)).await;
    assert_eq!(
        node_manager_unreachable(&error),
        Some(ClientConnectFailureCause::NotFound),
        "{error:?}"
    );
    let error = stats_inspect_error(unbound(&refused)).await;
    assert_eq!(
        node_manager_unreachable(&error),
        Some(ClientConnectFailureCause::Refused),
        "{error:?}"
    );

    let answering = FakeNodeManager::default()
        .unary(
            "InspectSandbox",
            |_: InspectSandboxRequest| -> Result<InspectSandboxResponse, Status> {
                Err(Status::unavailable("connection refused"))
            },
        )
        .serve();
    let error = stats_inspect_error(answering.connector()).await;
    assert_eq!(error.code(), tonic::Code::Unavailable, "{error:?}");
    assert_eq!(node_manager_unreachable(&error), None, "{error:?}");
}

#[tokio::test(start_paused = true)]
async fn a_node_manager_that_never_answers_is_not_unreachable() {
    let node_manager = crate::node_manager_transport::fake::StalledNodeManager::bind();
    let error = stats_inspect_error(node_manager.connector()).await;
    assert_eq!(error.code(), tonic::Code::DeadlineExceeded, "{error:?}");
    assert_eq!(node_manager_unreachable(&error), None, "{error:?}");
}
