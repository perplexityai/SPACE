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
