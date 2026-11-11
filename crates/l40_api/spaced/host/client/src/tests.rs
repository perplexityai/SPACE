use super::*;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio_stream::Stream;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Status};

type EventStream = Pin<Box<dyn Stream<Item = Result<proto::ProcessEvent, Status>> + Send>>;
type ProcessInfoStream = Pin<Box<dyn Stream<Item = Result<proto::ProcessInfo, Status>> + Send>>;

const INVALID_PROCESS_ID: &str = "";
const SECRET_INPUT: &[u8] = b"secret";

#[test]
fn process_options_debug_redacts_environment_values() {
    let mut options =
        StartProcessOptions::new(ProcessCommand::new(vec!["true".to_owned()]).expect("command"));
    options.env = pplx_space_common_core::types::EnvMap::try_from(std::collections::HashMap::from(
        [("TOKEN".to_owned(), "spaced-test-secret-value".to_owned())],
    ))
    .expect("valid environment");
    let rendered = format!("{options:?}");
    assert!(!rendered.contains("spaced-test-secret-value"));
}

#[derive(Clone)]
struct RejectingControlService {
    calls: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl proto::process_server::Process for RejectingControlService {
    type StartStream = EventStream;
    type ConnectProcessStream = EventStream;
    type ListProcessesStream = ProcessInfoStream;

    async fn start(
        &self,
        _request: Request<proto::StartRequest>,
    ) -> Result<tonic::Response<Self::StartStream>, Status> {
        Err(Status::unimplemented("unused"))
    }

    async fn connect_process(
        &self,
        _request: Request<proto::ConnectProcessRequest>,
    ) -> Result<tonic::Response<Self::ConnectProcessStream>, Status> {
        Err(Status::unimplemented("unused"))
    }

    async fn list_processes(
        &self,
        _request: Request<proto::ListProcessesRequest>,
    ) -> Result<tonic::Response<Self::ListProcessesStream>, Status> {
        Err(Status::unimplemented("unused"))
    }

    async fn send_signal(
        &self,
        _request: Request<proto::SendSignalRequest>,
    ) -> Result<tonic::Response<proto::SendSignalResponse>, Status> {
        self.reject()
    }

    async fn send_input(
        &self,
        _request: Request<proto::SendInputRequest>,
    ) -> Result<tonic::Response<proto::SendInputResponse>, Status> {
        self.reject()
    }

    async fn close_stdin(
        &self,
        _request: Request<proto::CloseStdinRequest>,
    ) -> Result<tonic::Response<proto::CloseStdinResponse>, Status> {
        self.reject()
    }

    async fn resize_pty(
        &self,
        _request: Request<proto::ResizePtyRequest>,
    ) -> Result<tonic::Response<proto::ResizePtyResponse>, Status> {
        self.reject()
    }

    async fn kill(
        &self,
        _request: Request<proto::KillRequest>,
    ) -> Result<tonic::Response<proto::KillResponse>, Status> {
        self.reject()
    }
}

#[tonic::async_trait]
impl proto::networking_server::Networking for RejectingControlService {
    async fn open_loopback_bridge(
        &self,
        _request: Request<proto::OpenLoopbackBridgeRequest>,
    ) -> Result<tonic::Response<proto::OpenLoopbackBridgeResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }

    async fn close_loopback_bridge(
        &self,
        _request: Request<proto::CloseLoopbackBridgeRequest>,
    ) -> Result<tonic::Response<proto::CloseLoopbackBridgeResponse>, Status> {
        self.reject()
    }
}

impl RejectingControlService {
    fn reject<T>(&self) -> Result<tonic::Response<T>, Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(Status::unavailable("ambiguous transport failure"))
    }
}

#[test]
fn connect_process_request_preserves_output_cursor() {
    let process_id = ProcessId::new();

    let request = connect_process_request(&process_id, None, Some(7), true);

    assert_eq!(request.process_id, process_id.as_str());
    assert_eq!(request.output_cursor, Some(7));
    assert!(request.resume_from_oldest_retained);
}

#[tokio::test]
async fn single_attempt_controls_do_not_replay_retryable_failures() {
    let temp_dir = tempfile::tempdir().unwrap();
    let socket_path = temp_dir.path().join("spaced-host.sock");
    let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let service = RejectingControlService {
        calls: Arc::clone(&calls),
    };
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(proto::process_server::ProcessServer::new(service.clone()))
            .add_service(proto::networking_server::NetworkingServer::new(service))
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await
            .unwrap();
    });
    let retry_policy = RetryPolicy {
        min_delay: std::time::Duration::ZERO,
        max_delay: std::time::Duration::ZERO,
        steps: 1,
        max_retry_count: 3,
    };
    let client = SpacedHostClient::new_with_config(
        SpacedHostClientConfig::default()
            .with_socket_path(socket_path)
            .with_command_retry_policy(retry_policy),
    )
    .await
    .unwrap();
    let process_id = ProcessId::new().to_string();

    let malformed_process_id_error = client
        .send_input(INVALID_PROCESS_ID.to_string(), SECRET_INPUT.to_vec())
        .await
        .unwrap_err();
    assert_eq!(
        malformed_process_id_error.code(),
        tonic::Code::InvalidArgument
    );

    for error in [
        client
            .send_input(process_id.clone(), SECRET_INPUT.to_vec())
            .await
            .expect_err("input failure"),
        client
            .close_stdin(process_id.clone())
            .await
            .expect_err("close failure"),
        client
            .send_signal(process_id.clone(), 15)
            .await
            .expect_err("signal failure"),
        client
            .resize_pty(process_id.clone(), proto::PtySize { cols: 80, rows: 24 })
            .await
            .expect_err("resize failure"),
        client
            .kill_process(process_id)
            .await
            .expect_err("kill failure"),
        client
            .close_loopback_bridge(8080)
            .await
            .expect_err("loopback close failure"),
    ] {
        assert_eq!(error.code(), tonic::Code::Unavailable);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 6);

    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}
