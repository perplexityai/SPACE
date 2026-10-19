#![cfg(unix)]

use std::{process::Stdio, time::Duration};

use pplx_space_common_core::types::GuestUserName;
use pplx_space_spaced_guest_server::{
    FilesystemExecutionMode, HostConnectorType, SpacedGuestService, SpacedGuestServiceConfig,
};
use pplx_space_spaced_host_client::{
    SpacedHostClient, SpacedHostClientConfig, StartProcessOptions,
};
use pplx_space_spaced_host_server::{
    BridgeGuestConnector, SpacedHostPeerAuthorization, SpacedHostService, SpacedHostServiceConfig,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio_util::sync::CancellationToken;

// Real CLI -> host -> guest: output must be observable while the process is
// blocked, and binary data must not be replaced by UTF-8 decoding.
#[tokio::test]
async fn read_run_and_connect_preserve_bytes_without_waiting_for_process_exit() {
    let directory = tempfile::tempdir().unwrap();
    let guest_socket = directory.path().join("guest.sock");
    let host_socket = directory.path().join("host.sock");
    let shutdown = CancellationToken::new();
    let user = nix::unistd::User::from_uid(nix::unistd::Uid::current())
        .unwrap()
        .unwrap();
    let guest = SpacedGuestService::with_config(
        shutdown.clone(),
        SpacedGuestServiceConfig {
            default_operation_user: GuestUserName::parse(user.name).unwrap(),
            filesystem_execution: FilesystemExecutionMode::Direct,
            host_connector_type: HostConnectorType::Uds(directory.path().join("bridge-host.sock")),
            local_socket_path: None,
            bridge_socket_path: Some(guest_socket.clone()),
            ..Default::default()
        },
    )
    .expect("guest service");
    let guest_task = tokio::spawn(guest.clone().run());
    assert!(guest.wait_until_listening().await);
    let host = SpacedHostService::new_with_config(
        shutdown.clone(),
        SpacedHostServiceConfig {
            socket_path: host_socket.clone(),
            bridge_host_socket_path: directory.path().join("bridge-host.sock"),
            bridge_guest_connector: BridgeGuestConnector::Uds(guest_socket),
            host_control_socket_path: directory.path().join("unused-control.sock"),
            host_peer_authorization: SpacedHostPeerAuthorization::Disabled,
        },
    );
    let host_task = tokio::spawn(host.run());
    let client = SpacedHostClient::new_with_config(
        SpacedHostClientConfig::new().with_socket_path(&host_socket),
    )
    .await
    .unwrap();
    let cli = || {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_spaced-host"));
        command.arg("--socket").arg(&host_socket).kill_on_drop(true);
        command
    };
    let file = directory.path().join("binary");
    tokio::fs::write(&file, [0, 255, 226, 130, 172])
        .await
        .unwrap();
    let read = cli().arg("read").arg(&file).output().await.unwrap();

    let fifo = directory.path().join("gate");
    nix::unistd::mkfifo(
        &fifo,
        nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
    )
    .unwrap();
    let command = vec![
        "sh".into(),
        "-c".into(),
        "exec cat \"$1\"".into(),
        "spaced-test".into(),
        fifo.display().to_string(),
    ];
    let mut started = client
        .start_process_stream_with_options(StartProcessOptions {
            idempotency_key: Some(
                pplx_space_common_core::types::IdempotencyKey::parse("cli-output").unwrap(),
            ),
            ..pplx_space_spaced_host_client::StartProcessOptions::new(
                pplx_space_common_core::types::ProcessCommand::new(command.clone()).unwrap(),
            )
        })
        .await
        .unwrap();
    let process_id = started.message().await.unwrap().unwrap().process_id;
    drop(started);
    let mut input = tokio::fs::OpenOptions::new()
        .write(true)
        .open(&fifo)
        .await
        .unwrap();
    let mut observations = Vec::new();
    for connect in [false, true] {
        let mut process = cli();
        if connect {
            process.arg("connect").arg(&process_id);
        } else {
            process
                .args(["run", "--idempotency-key", "cli-output", "--"])
                .args(&command);
        }
        let mut process = process.stdout(Stdio::piped()).spawn().unwrap();
        let mut output = BufReader::new(process.stdout.take().unwrap());
        let observed = tokio::time::timeout(Duration::from_secs(3), async {
            let mut header = String::new();
            output.read_line(&mut header).await?;
            input.write_all(&[255, 226]).await?;
            input.flush().await?;
            let mut bytes = [0; 2];
            output.read_exact(&mut bytes).await?;
            Ok::<_, std::io::Error>((header, bytes))
        })
        .await;
        process.kill().await.unwrap();
        process.wait().await.unwrap();
        observations.push(observed);
    }
    client.kill_process(process_id).await.unwrap();
    shutdown.cancel();
    host_task.await.unwrap().unwrap();
    guest_task.await.unwrap().unwrap();
    assert!(read.status.success());
    assert_eq!(read.stdout, [0, 255, 226, 130, 172]);
    for (index, observed) in observations.into_iter().enumerate() {
        let (header, bytes) = observed.expect("incremental output before exit").unwrap();
        assert!(header.starts_with(if index == 0 { "started " } else { "connected " }));
        assert_eq!(bytes, [255, 226]);
    }
}
