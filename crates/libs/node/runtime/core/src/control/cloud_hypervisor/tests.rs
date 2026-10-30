use super::*;
use crate::control::ControlError;
use pplx_space_common_core::types::RuntimeKind;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn reads_states_and_refuses_unknown_or_failed_observations() {
    for (code, body, expected) in [
        (200, r#"{"state":"Created"}"#, Some(VmState::NotStarted)),
        (200, r#"{"state":"Running"}"#, Some(VmState::Running)),
        (200, r#"{"state":"Paused"}"#, Some(VmState::Paused)),
        (200, r#"{"state":"Shutdown"}"#, Some(VmState::Shutdown)),
        (200, r#"{"state":"BreakPoint"}"#, None),
        (200, r#"{"config":{}}"#, None),
        (404, "", Some(VmState::NotStarted)),
        (500, "unavailable", None),
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
                "HTTP/1.1 {code} Result\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        let client = Client::new(socket, peer());
        let result = client.state().await;
        match expected {
            Some(expected) => assert_eq!(result.unwrap(), expected),
            None if code != 200 => assert!(matches!(
                result, Err(ControlError::CloudHypervisor(ApiError::Status { code: actual, .. })) if actual.as_u16() == code
            )),
            None => assert!(matches!(
                result,
                Err(ControlError::CloudHypervisor(ApiError::Decode(_)))
            )),
        }
        server.await.unwrap();
    }
}

#[tokio::test]
async fn missing_socket_is_an_error() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::new(root.path().join("absent.sock"), peer());
    assert!(matches!(
        client.state().await,
        Err(ControlError::CloudHypervisor(ApiError::Io(_)))
    ));
}

#[tokio::test(start_paused = true)]
async fn deadline_includes_a_stalled_response() {
    for (operation, expected_seconds) in [(0, 2), (1, 10), (2, 572)] {
        let root = tempfile::tempdir().unwrap();
        let socket = root.path().join("control.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });
        let client = Client::new(socket, peer());
        let started = tokio::time::Instant::now();
        let result = match operation {
            0 => client.state().await.map(|_| ()),
            1 => client.set_paused(true).await,
            2 => {
                client
                    .restore_state(StateWorkspace::Plain, 32 * 1024 * 1024 * 1024)
                    .await
            }
            _ => unreachable!(),
        };
        assert!(matches!(
            result,
            Err(ControlError::CloudHypervisor(ApiError::Timeout))
        ));
        assert!(result.unwrap_err().is_timeout());
        assert_eq!(started.elapsed(), Duration::from_secs(expected_seconds));
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
    }
}

#[tokio::test]
async fn boot_orders_create_before_boot_and_never_replays_a_failure() {
    for statuses in [[204, 204], [400, 204], [204, 500]] {
        let root = tempfile::tempdir().unwrap();
        let socket = root.path().join("control.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let configuration = crate::boot::validate_standalone(
            RuntimeKind::CloudHypervisor,
            &crate::boot::configuration(RuntimeKind::CloudHypervisor, &crate::boot::tests::input())
                .unwrap(),
            crate::boot::BootSource::LinuxDirect,
        )
        .unwrap();
        let expected_body = configuration.encode().unwrap();
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
                    let mut body = vec![0; expected_body.len()];
                    stream.read_exact(&mut body).await.unwrap();
                    assert_eq!(body, expected_body);
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
        let result = Client::new(socket, peer()).boot(&configuration).await;
        let failure = statuses.into_iter().find(|status| *status >= 400);
        match failure {
            None => result.unwrap(),
            Some(status) => assert!(
                matches!(result, Err(ControlError::CloudHypervisor(ApiError::Status { code: actual, .. })) if actual.as_u16() == status)
            ),
        }
        server.await.unwrap();
    }
}

fn peer() -> crate::launch::ProcessObserver {
    assert_eq!(
        nix::unistd::Uid::effective().as_raw(),
        0,
        "requires root listener fixture"
    );
    assert_eq!(
        nix::unistd::Gid::effective().as_raw(),
        0,
        "requires root listener fixture"
    );
    crate::launch::ProcessObserver::current().expect("pin test listener process")
}

#[tokio::test]
async fn refuses_a_listener_owned_by_a_different_process_before_sending_bytes() {
    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("control.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let mut child = std::process::Command::new("sleep")
        .arg("30")
        .spawn()
        .unwrap();
    let expected = crate::launch::ProcessHandle::open(child.id()).unwrap();
    let result = Client::new(socket, expected.into()).state().await;
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(matches!(
        result,
        Err(ControlError::CloudHypervisor(ApiError::PeerMismatch))
    ));
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await.unwrap();
    assert!(
        bytes.is_empty(),
        "no privileged request may reach an unattested listener"
    );
}

#[tokio::test]
async fn refuses_an_oversized_response() {
    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("control.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0; 1024];
        assert!(stream.read(&mut request).await.unwrap() > 0);
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
            MAX_RESPONSE_BYTES + 1
        );
        stream.write_all(header.as_bytes()).await.unwrap();
        // The client may reject Content-Length without reading the body.
    });
    assert!(matches!(
        Client::new(socket, peer()).state().await,
        Err(ControlError::CloudHypervisor(ApiError::Body(_)))
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn state_operations_use_fixed_workspaces_and_do_not_replay_refusals() {
    for (operation, endpoint, body) in [
        (0, "vm.pause", ""),
        (1, "vm.resume", ""),
        (
            2,
            "vm.snapshot",
            r#"{"destination_url":"file:///state/capture"}"#,
        ),
        (
            3,
            "vm.restore",
            r#"{"source_url":"file:///state/restore","prefault":false,"memory_restore_mode":"CopyOnWrite","resume":false}"#,
        ),
        (
            4,
            "vm.snapshot",
            r#"{"destination_url":"file:///state/encsnap/capture"}"#,
        ),
        (
            5,
            "vm.restore",
            r#"{"source_url":"file:///state/encsnap/restore","prefault":false,"memory_restore_mode":"CopyOnWrite","resume":false}"#,
        ),
    ] {
        for status in [204, 409] {
            let root = tempfile::tempdir().unwrap();
            let socket = root.path().join("control.sock");
            let listener = tokio::net::UnixListener::bind(&socket).unwrap();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut headers = Vec::new();
                while !headers.ends_with(b"\r\n\r\n") {
                    headers.push(stream.read_u8().await.unwrap());
                }
                assert!(
                    headers.starts_with(format!("PUT /api/v1/{endpoint} HTTP/1.1\r\n").as_bytes())
                );
                let mut payload = vec![0; body.len()];
                stream.read_exact(&mut payload).await.unwrap();
                assert_eq!(payload, body.as_bytes());
                stream
                    .write_all(
                        format!("HTTP/1.1 {status} Result\r\nContent-Length: 0\r\n\r\n").as_bytes(),
                    )
                    .await
                    .unwrap();
            });
            let client = Client::new(socket, peer());
            let result = match operation {
                0 => client.set_paused(true).await,
                1 => client.set_paused(false).await,
                2 => {
                    client
                        .capture_state(StateWorkspace::Plain, 1024 * 1024 * 1024)
                        .await
                }
                3 => {
                    client
                        .restore_state(StateWorkspace::Plain, 1024 * 1024 * 1024)
                        .await
                }
                4 => {
                    client
                        .capture_state(StateWorkspace::Encrypted, 1024 * 1024 * 1024)
                        .await
                }
                5 => {
                    client
                        .restore_state(StateWorkspace::Encrypted, 1024 * 1024 * 1024)
                        .await
                }
                _ => unreachable!(),
            };
            if status == 204 {
                result.unwrap();
            } else {
                assert!(
                    matches!(result, Err(ControlError::CloudHypervisor(ApiError::Status { code, .. })) if code.as_u16() == status)
                );
            }
            server.await.unwrap();
        }
    }
}
