use super::*;

#[tokio::test(start_paused = true)]
async fn rejected_or_unavailable_registry_never_invokes_the_authenticated_peer_connector() {
    let requested = transfer_source_uri("http://192.0.2.10:9500").unwrap();
    for expected in [
        tonic::Code::PermissionDenied,
        tonic::Code::Unavailable,
        tonic::Code::DeadlineExceeded,
    ] {
        let connected = std::cell::Cell::new(false);
        let result = connect_after_transfer_admission(
            &requested,
            async {
                match expected {
                    tonic::Code::PermissionDenied => Ok(vec![]),
                    tonic::Code::Unavailable => Err(Status::unavailable("registry unavailable")),
                    _ => std::future::pending().await,
                }
            },
            |_, _| async {
                connected.set(true);
                Ok(())
            },
        )
        .await;
        assert_eq!(result.unwrap_err().code(), expected);
        assert!(
            !connected.get(),
            "peer must not receive a connection or bearer before source admission"
        );
    }
    let connected = std::cell::Cell::new(false);
    let worker = crate::proto::NodeRecord {
        node_api_endpoint: requested.to_string(),
        role: crate::proto::NodeRole::Worker as i32,
        ..Default::default()
    };
    connect_after_transfer_admission(&requested, async { Ok(vec![worker]) }, |endpoint, role| {
        assert_eq!(endpoint, requested);
        assert_eq!(role, crate::proto::NodeRole::Worker);
        connected.set(true);
        std::future::ready(Ok(()))
    })
    .await
    .unwrap();
    assert!(connected.get());
}

#[test]
fn transfer_sources_require_unambiguous_registered_endpoint_and_role() {
    let requested = transfer_source_uri("http://192.0.2.10:9500").unwrap();
    let worker = crate::proto::NodeRecord {
        id: "registered-worker".to_owned(),
        node_api_endpoint: "http://192.0.2.10:9500".to_owned(),
        role: crate::proto::NodeRole::Worker as i32,
        ..Default::default()
    };
    for (nodes, expected) in [
        (vec![worker.clone()], Ok(crate::proto::NodeRole::Worker)),
        (
            vec![crate::proto::NodeRecord {
                role: crate::proto::NodeRole::Builder as i32,
                ..worker.clone()
            }],
            Ok(crate::proto::NodeRole::Builder),
        ),
        (vec![], Err(tonic::Code::PermissionDenied)),
        (
            vec![crate::proto::NodeRecord {
                node_api_endpoint: "https://192.0.2.10:9500".to_owned(),
                ..worker.clone()
            }],
            Err(tonic::Code::PermissionDenied),
        ),
        (
            vec![crate::proto::NodeRecord {
                deleted_at: Some("deleted".to_owned()),
                ..worker.clone()
            }],
            Err(tonic::Code::PermissionDenied),
        ),
        (
            vec![crate::proto::NodeRecord {
                node_api_endpoint: "http://unregistered-alias:9500".to_owned(),
                ..worker.clone()
            }],
            Err(tonic::Code::PermissionDenied),
        ),
        (
            vec![crate::proto::NodeRecord {
                role: crate::proto::NodeRole::Cluster as i32,
                ..worker.clone()
            }],
            Err(tonic::Code::PermissionDenied),
        ),
        (
            vec![crate::proto::NodeRecord {
                role: 999,
                ..worker.clone()
            }],
            Err(tonic::Code::FailedPrecondition),
        ),
        (
            vec![worker.clone(), worker.clone()],
            Err(tonic::Code::FailedPrecondition),
        ),
    ] {
        assert_eq!(
            registered_transfer_source(&requested, nodes)
                .map(|(_, role)| role)
                .map_err(|error| error.code()),
            expected
        );
    }
    assert_eq!(
        transfer_source_uri("http://192.0.2.10:9500/").unwrap(),
        requested
    );
    for endpoint in [
        "http://worker:9500/other",
        "http://worker:9500/?query",
        "http://user@worker:9500",
        "ftp://worker:9500",
        "http://",
    ] {
        assert_eq!(
            transfer_source_uri(endpoint).unwrap_err().code(),
            tonic::Code::InvalidArgument,
            "{endpoint}"
        );
    }
}
