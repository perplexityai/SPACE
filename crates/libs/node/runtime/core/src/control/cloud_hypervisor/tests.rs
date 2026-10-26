use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn reads_states_and_refuses_unknown_or_failed_observations() {
    for (body, expected) in [
        (r#"{"state":"Created"}"#, Some(VmState::NotStarted)),
        (r#"{"state":"Running"}"#, Some(VmState::Running)),
        (r#"{"state":"Paused"}"#, Some(VmState::Paused)),
        (r#"{"state":"Shutdown"}"#, Some(VmState::Shutdown)),
        (r#"{"state":"BreakPoint"}"#, None),
        (r#"{"config":{}}"#, None),
    ] {
        let root = tempfile::tempdir().unwrap();
        let socket = root.path().join("control.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            let len = stream.read(&mut request).await.unwrap();
            assert!(request[..len].starts_with(b"GET /api/v1/vm.info HTTP/1.1\r\n"));
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        let client = Client::new(socket);
        let result = client.state().await;
        match expected {
            Some(expected) => assert_eq!(result.unwrap(), expected),
            None => assert!(matches!(
                result,
                Err(ControlError::CloudHypervisor(ApiError::Decode(_)))
            )),
        }
        server.await.unwrap();
    }
}

#[tokio::test]
async fn unsupported_mutations_do_not_connect_and_missing_socket_is_an_error() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::new(root.path().join("absent.sock"));
    assert!(matches!(
        client.pause().await,
        Err(ControlError::UnsupportedOperation { .. })
    ));
    assert!(matches!(
        client.resume().await,
        Err(ControlError::UnsupportedOperation { .. })
    ));
    assert!(matches!(
        client.state().await,
        Err(ControlError::CloudHypervisor(ApiError::Io(_)))
    ));
}

#[tokio::test(start_paused = true)]
async fn deadline_includes_a_stalled_response() {
    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("control.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let server = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.unwrap();
        std::future::pending::<()>().await;
    });
    let result = Client::new(socket).state().await;
    assert!(matches!(
        result,
        Err(ControlError::CloudHypervisor(ApiError::Timeout))
    ));
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn boot_orders_create_before_boot_and_never_replays_a_failure() {
    for statuses in [[204, 204], [400, 204], [204, 500]] {
        let root = tempfile::tempdir().unwrap();
        let socket = root.path().join("control.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            for (endpoint, status) in ["vm.create", "vm.boot"].into_iter().zip(statuses) {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                loop {
                    let byte = stream.read_u8().await.unwrap();
                    bytes.push(byte);
                    if bytes.ends_with(b"\r\n\r\n") {
                        break;
                    }
                }
                assert!(
                    bytes.starts_with(format!("PUT /api/v1/{endpoint} HTTP/1.1\r\n").as_bytes())
                );
                if endpoint == "vm.create" {
                    let mut body = [0; 2];
                    stream.read_exact(&mut body).await.unwrap();
                    assert_eq!(&body, b"{}");
                }
                stream
                    .write_all(
                        format!("HTTP/1.1 {status} Result\r\nContent-Length: 0\r\n\r\n").as_bytes(),
                    )
                    .await
                    .unwrap();
                if status >= 400 {
                    break;
                }
            }
            // Closing the listener makes any replay fail; the original status
            // must be preserved by the client rather than replaced by I/O.
        });
        let result = Client::new(socket).boot("{}").await;
        let failure = statuses.into_iter().find(|status| *status >= 400);
        match failure {
            None => result.unwrap(),
            Some(status) => assert!(
                matches!(result, Err(ControlError::CloudHypervisor(ApiError::Status(actual))) if actual.as_u16() == status)
            ),
        }
        server.await.unwrap();
    }
}
