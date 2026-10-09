use super::*;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use tonic::{Status, transport::Server};

#[tokio::test]
async fn probe_requires_both_denials_and_both_allowed_methods() {
    let methods = [
        "IssueIntermediateCertificate",
        "ResolveNetworkCredentials",
        "IssueSandboxArtifactDek",
        "UnwrapSandboxArtifactDek",
    ];
    let expected = [
        Code::PermissionDenied,
        Code::PermissionDenied,
        Code::InvalidArgument,
        Code::InvalidArgument,
    ];
    let mut cases = vec![(expected, true)];
    for (index, code) in [
        (0, Code::InvalidArgument),
        (1, Code::Unimplemented),
        (2, Code::PermissionDenied),
        (2, Code::Unauthenticated),
        (3, Code::Unavailable),
    ] {
        let mut replies = expected;
        replies[index] = code;
        cases.push((replies, false));
    }
    for (replies, success) in cases {
        let path = std::env::temp_dir().join(format!("volume-probe-{}.sock", uuid::Uuid::now_v7()));
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let calls = recorded.clone();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            Server::builder()
                .serve_with_incoming_shutdown(
                    tower::service_fn(
                        move |request: tonic::codegen::http::Request<tonic::body::Body>| {
                            let method = request.uri().path().rsplit('/').next().unwrap();
                            let index = methods.iter().position(|name| *name == method).unwrap();
                            calls.lock().unwrap().push(method.to_owned());
                            let code = replies[index];
                            async move {
                                Ok::<_, Infallible>(
                                    Status::new(code, "fixture response")
                                        .into_http::<tonic::body::Body>(),
                                )
                            }
                        },
                    ),
                    tokio_stream::wrappers::UnixListenerStream::new(listener),
                    async {
                        stopped.await.unwrap();
                    },
                )
                .await
                .unwrap();
        });
        let client = pplx_space_credmgr_client::connect(path.to_str().unwrap())
            .await
            .unwrap();
        let result = check_client(client).await;
        assert_eq!(result.is_ok(), success, "replies {replies:?}");
        if success {
            assert_eq!(*recorded.lock().unwrap(), methods);
        }
        stop.send(()).unwrap();
        server.await.unwrap();
        std::fs::remove_file(path).unwrap();
    }
}
