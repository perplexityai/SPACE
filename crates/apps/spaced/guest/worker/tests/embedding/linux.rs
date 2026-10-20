use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::time::Duration;

use futures_util::StreamExt;
use pplx_space_common_core::types::GuestUserName;
use pplx_space_spaced_guest_execution::{
    ExecutionService, ExecutionServiceConfig, WorkerExecutable,
};
use pplx_space_spaced_guest_protocol::proto::bridge_guest::{
    StartRequest, StatRequest, WriteBatchEntry, WriteBatchRequest,
    filesystem_client::FilesystemClient, process_event::Event, process_exit::Outcome,
    process_server::Process,
};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Server};

type ServerTask = tokio::task::JoinHandle<Result<(), tonic::transport::Error>>;

async fn host(
    executable: WorkerExecutable,
) -> (
    ExecutionService,
    FilesystemClient<Channel>,
    CancellationToken,
    ServerTask,
) {
    let shutdown = CancellationToken::new();
    let service = ExecutionService::with_config(
        shutdown.clone(),
        ExecutionServiceConfig {
            default_operation_user: GuestUserName::parse("nobody").expect("account name"),
            worker_executable: Some(executable),
            ..Default::default()
        },
    )
    .expect("execution service");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener");
    let address = listener.local_addr().expect("listener address");
    let grpc = service.filesystem_grpc_service();
    let stop = shutdown.clone();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(grpc)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                stop.cancelled_owned(),
            )
            .await
    });
    let client = FilesystemClient::connect(format!("http://{address}"))
        .await
        .expect("connect host");
    (service, client, shutdown, server)
}

async fn stop(service: ExecutionService, shutdown: CancellationToken, server: ServerTask) {
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(3), service.drain())
        .await
        .expect("drain reaps workers promptly");
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .expect("server stops")
        .expect("server task")
        .expect("server result");
    assert!(
        children().is_empty(),
        "drain must reap owned worker processes"
    );
}

fn children() -> Vec<u32> {
    let mut children = Vec::new();
    for task in std::fs::read_dir("/proc/self/task").expect("tasks") {
        let path = task.expect("task").path().join("children");
        if let Ok(value) = std::fs::read_to_string(path) {
            children.extend(
                value
                    .split_whitespace()
                    .map(|pid| pid.parse::<u32>().expect("pid")),
            );
        }
    }
    children.sort_unstable();
    children.dedup();
    children
}

pub(super) async fn run() {
    let host_identity = super::identity::current_status();
    assert!(host_identity.len() > 1, "exercise a multithreaded host");
    let standalone = std::process::Command::new(env!("CARGO_BIN_EXE_spaced-filesystem-worker"))
        .env_clear()
        .output()
        .expect("worker without launch configuration");
    assert!(
        !standalone.status.success(),
        "worker must reject ordinary invocation"
    );
    let directory = tempfile::tempdir().expect("fixture directory");
    // Provisioned system account, not a root-only success path.
    let account = nix::unistd::User::from_name("nobody")
        .expect("account lookup")
        .expect("nobody account");
    assert!(!account.uid.is_root());
    nix::unistd::chown(directory.path(), Some(account.uid), Some(account.gid))
        .expect("fixture ownership");
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
        .expect("fixture mode");
    let path = directory.path().join("payload");
    let artifacts = tempfile::tempdir().expect("artifact directory");
    let artifact = artifacts.path().join("worker");
    std::fs::copy(env!("CARGO_BIN_EXE_spaced-filesystem-worker"), &artifact)
        .expect("worker artifact");
    let executable = WorkerExecutable::open(&artifact).expect("dedicated worker");
    let replacement = artifacts.path().join("replacement");
    std::fs::copy("/bin/true", &replacement).expect("replacement artifact");
    std::fs::rename(&replacement, &artifact).expect("atomic replacement");
    let (service, mut client, shutdown, server) = host(executable).await;
    super::identity::check_cwd(&service, &account).await;
    let write = WriteBatchRequest {
        entries: vec![WriteBatchEntry {
            path: path.display().to_string(),
            data: b"once".to_vec(),
            append: true,
        }],
        idempotency_key: Some("embedding-write".into()),
        user: None,
    };
    for _ in 0..2 {
        client
            .write_batch(write.clone())
            .await
            .expect("write and replay");
    }
    assert_eq!(std::fs::read(&path).expect("file"), b"once");
    let metadata = std::fs::metadata(&path).expect("metadata");
    assert_eq!(metadata.uid(), account.uid.as_raw());
    assert_eq!(metadata.gid(), account.gid.as_raw());
    assert_eq!(children().len(), 1, "one resident worker");
    let error = client
        .stat_info(StatRequest {
            path: "/".into(),
            user: Some("root".into()),
        })
        .await
        .expect_err("restricted root worker must be rejected");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        children().len(),
        1,
        "rejected identity must not spawn a worker"
    );
    for pid in children() {
        let status =
            std::fs::read_to_string(format!("/proc/{pid}/status")).expect("worker credentials");
        let status = super::identity::status(&status);
        let uid = status["Uid"]
            .split_whitespace()
            .next()
            .expect("uid")
            .parse()
            .expect("numeric uid");
        let selected = nix::unistd::User::from_uid(nix::unistd::Uid::from_raw(uid))
            .expect("worker account")
            .expect("installed account");
        assert_eq!(selected.uid, account.uid);
        super::identity::assert_restricted(&status, &selected);
        assert_eq!(
            std::fs::read_to_string(format!("/proc/{pid}/comm"))
                .expect("worker name")
                .trim(),
            "spaced-fsworker",
            "worker name must remain recognizable when executing a pinned descriptor",
        );
        let command_line =
            std::fs::read(format!("/proc/{pid}/cmdline")).expect("worker command line");
        assert_eq!(
            command_line.split(|byte| *byte == 0).next(),
            Some(b"spaced-fsworker".as_slice())
        );
        // The executable pin is CLOEXEC; only the worker channel is deliberately inherited.
        for entry in std::fs::read_dir(format!("/proc/{pid}/fd")).expect("worker descriptors") {
            let target =
                std::fs::read_link(entry.expect("descriptor").path()).expect("descriptor target");
            assert!(
                !target.starts_with(artifacts.path()),
                "executable pin leaked into worker"
            );
        }
    }
    // Native process calls and filesystem workers share the selected account.
    let mut events = Process::start(
        &service,
        tonic::Request::new(StartRequest {
            command: vec![
                "/bin/sh".into(),
                "-c".into(),
                "/usr/bin/id -u; exec /bin/cat \"$1\"".into(),
                "embedding".into(),
                path.display().to_string(),
            ],
            cwd: Some(directory.path().display().to_string()),
            idempotency_key: Some("embedding-process".into()),
            ..Default::default()
        }),
    )
    .await
    .expect("start process")
    .into_inner();
    let mut output = Vec::new();
    let mut exited = false;
    while let Some(event) = events.next().await {
        match event.expect("process event").event {
            Some(Event::Stdout(chunk)) => output.extend(chunk.data),
            Some(Event::Exit(exit)) => {
                assert!(matches!(exit.outcome, Some(Outcome::Exited(exit)) if exit.exit_code == 0));
                exited = true;
            }
            _ => {}
        }
    }
    assert!(exited);
    assert_eq!(output, format!("{}\nonce", account.uid).as_bytes());
    super::identity::check_processes(&service, &account).await;
    drop(client);
    stop(service, shutdown, server).await;

    // Valid executable shape, wrong worker protocol: fail startup and reap.
    let (service, mut client, shutdown, server) =
        host(WorkerExecutable::open("/bin/true").expect("wrong artifact")).await;
    let error = client
        .stat_info(StatRequest {
            path: "/".into(),
            user: None,
        })
        .await
        .expect_err("non-worker must fail startup");
    assert_eq!(error.code(), tonic::Code::Unavailable);
    drop(client);
    stop(service, shutdown, server).await;

    let incompatible = artifacts.path().join("incompatible-worker");
    std::fs::copy(
        std::env::current_exe().expect("test executable"),
        &incompatible,
    )
    .expect("incompatible artifact");
    let (service, mut client, shutdown, server) =
        host(WorkerExecutable::open(&incompatible).expect("incompatible artifact")).await;
    let error = client
        .stat_info(StatRequest {
            path: "/".into(),
            user: None,
        })
        .await
        .expect_err("mismatched protocol must fail before dispatch");
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(error.message(), "incompatible filesystem worker protocol");
    drop(client);
    stop(service, shutdown, server).await;

    // Caller cancellation must not detach startup ownership. A worker that
    // never speaks gRPC must be killed and reaped promptly on host shutdown.
    let (service, mut client, shutdown, server) =
        host(WorkerExecutable::open("/proc/self/exe").expect("unresponsive artifact")).await;
    let request = tokio::spawn(async move {
        client
            .stat_info(StatRequest {
                path: "/".into(),
                user: None,
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while children().is_empty() {
            // Poll an external process event without busy-reading procfs.
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("worker spawned");
    request.abort();
    assert!(request.await.expect_err("caller cancelled").is_cancelled());
    stop(service, shutdown, server).await;
    super::identity::assert_host_unchanged(&host_identity);
}
