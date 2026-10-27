//! Fenced guest access. No request can select a host address, file or VMM socket.
use super::*;
use crate::proto::*;
use std::{
    num::NonZeroU16,
    pin::Pin,
    sync::{Arc, LazyLock},
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_stream::{Stream, wrappers::ReceiverStream};

const CHUNK_LIMIT: usize = 64 * 1024;
static CHANNELS: LazyLock<Arc<tokio::sync::Semaphore>> =
    LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(32)));

fn port(value: u32) -> Result<NonZeroU16, Status> {
    u16::try_from(value)
        .ok()
        .and_then(NonZeroU16::new)
        .ok_or_else(|| Status::invalid_argument("guest TCP port must be 1-65535"))
}
trait GuestIo: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> GuestIo for T {}

pub struct ChannelStream {
    receiver: ReceiverStream<Result<OpenGuestChannelResponse, Status>>,
    task: tokio::task::JoinHandle<()>,
}
impl Stream for ChannelStream {
    type Item = Result<OpenGuestChannelResponse, Status>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_next(cx)
    }
}
impl Drop for ChannelStream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl NodeManagerService {
    pub(super) async fn request_guest_shutdown(
        &self,
        request: Request<RequestGuestShutdownRequest>,
    ) -> Result<Response<RequestGuestShutdownResponse>, Status> {
        self.authorize_peer(&request, "RequestGuestShutdown", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        let req = request.into_inner();
        let id = req
            .sandbox_id
            .parse::<SandboxId>()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        if req.execution_fence.is_none() {
            return Err(Status::invalid_argument("execution fence required"));
        }
        let service = self.clone();
        self.run_owned_sandbox_work(&id.to_string(), async move {
            let _gate = service.lock_sandbox_vm_cgroup_launch(&req.sandbox_id).await;
            service.check_execution(&req.sandbox_id, req.execution_fence.as_ref(), false)?;
            let execution = service
                .recorded_execution(req.sandbox_id)
                .await?
                .ok_or_else(|| Status::failed_precondition("execution metadata required"))?;
            let runtime = execution.runtime().kind;
            let socket = pplx_space_node_runtime_core::control::socket_path(runtime, id)
                .map_err(|e| Status::failed_precondition(e.to_string()))?;
            let control = pplx_space_node_runtime_core::control::connect(runtime, socket)
                .map_err(|e| Status::failed_precondition(e.to_string()))?;
            control
                .request_guest_shutdown()
                .await
                .map_err(|e| Status::failed_precondition(e.to_string()))?;
            Ok(Response::new(RequestGuestShutdownResponse {}))
        })
        .await
    }

    async fn connect_guest_tcp(
        &self,
        id: &str,
        fence: Option<&ExecutionFence>,
        port: NonZeroU16,
    ) -> Result<tokio::net::TcpStream, Status> {
        if fence.is_none() {
            return Err(Status::invalid_argument("execution fence required"));
        }
        let _gate = self.lock_sandbox_vm_cgroup_launch(id).await;
        self.check_execution(id, fence, false)?;
        let lease = self
            .pool
            .get(id)
            .map_err(slot_store_status)?
            .ok_or_else(|| Status::failed_precondition("execution has no network lease"))?;
        let namespace = lease.ns_name();
        let stream = self
            .off_runtime("connect guest TCP", move || {
                net::guest_tcp_stream(&namespace, port)
                    .map_err(|e| Status::unavailable(format!("guest TCP connect: {e}")))
            })
            .await?;
        self.check_execution(id, fence, false)?;
        tokio::net::TcpStream::from_std(stream).map_err(|e| Status::unavailable(e.to_string()))
    }

    pub(super) async fn probe_guest_service(
        &self,
        request: Request<ProbeGuestServiceRequest>,
    ) -> Result<Response<ProbeGuestServiceResponse>, Status> {
        self.authorize_peer(&request, "ProbeGuestService", ALLOW_NODE_ADMIN_OR_SBXMGR)?;
        let req = request.into_inner();
        let port = port(req.port)?;
        let _permit = CHANNELS
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("guest channel capacity exhausted"))?;
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            self.connect_guest_tcp(&req.sandbox_id, req.execution_fence.as_ref(), port),
        )
        .await;
        let reachable = match result {
            Ok(Ok(_)) => true,
            Ok(Err(error)) if error.code() != tonic::Code::Unavailable => return Err(error),
            Ok(Err(_)) | Err(_) => false,
        };
        Ok(Response::new(ProbeGuestServiceResponse { reachable }))
    }

    pub(super) async fn open_guest_channel(
        &self,
        request: Request<tonic::Streaming<OpenGuestChannelRequest>>,
    ) -> Result<Response<ChannelStream>, Status> {
        self.authorize_peer(&request, "OpenGuestChannel", ALLOW_NODE_ADMIN)?;
        let permit = CHANNELS
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("guest channel capacity exhausted"))?;
        let mut input = request.into_inner();
        let first = tokio::time::timeout(Duration::from_secs(2), input.message())
            .await
            .map_err(|_| Status::deadline_exceeded("guest channel selector timed out"))??;
        let Some(OpenGuestChannelRequest {
            frame: Some(open_guest_channel_request::Frame::Selector(selector)),
        }) = first
        else {
            return Err(Status::invalid_argument(
                "first frame must select a guest endpoint",
            ));
        };
        let id = selector
            .sandbox_id
            .parse::<SandboxId>()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        if selector.execution_fence.is_none() {
            return Err(Status::invalid_argument("execution fence required"));
        }
        let io: Box<dyn GuestIo> = match selector.endpoint {
            Some(guest_channel_selector::Endpoint::GuestTcpPort(value)) => Box::new(
                self.connect_guest_tcp(
                    &id.to_string(),
                    selector.execution_fence.as_ref(),
                    port(value)?,
                )
                .await?,
            ),
            Some(guest_channel_selector::Endpoint::Serial(_)) => {
                let _gate = self
                    .lock_sandbox_vm_cgroup_launch(&selector.sandbox_id)
                    .await;
                self.check_execution(
                    &selector.sandbox_id,
                    selector.execution_fence.as_ref(),
                    false,
                )?;
                let execution = self
                    .recorded_execution(selector.sandbox_id.clone())
                    .await?
                    .ok_or_else(|| Status::failed_precondition("execution metadata required"))?;
                if pplx_space_node_runtime_core::boot::BootSource::from_execution(&execution)
                    .map_err(|e| Status::failed_precondition(e.to_string()))?
                    != pplx_space_node_runtime_core::boot::BootSource::Uefi
                {
                    return Err(Status::failed_precondition(
                        "interactive serial is not qualified for this execution",
                    ));
                }
                let root = pplx_space_node_runtime_core::control::isolation_root(
                    execution.runtime().kind,
                    &selector.sandbox_id,
                )
                .map_err(|e| Status::failed_precondition(e.to_string()))?;
                let stream = tokio::time::timeout(
                    Duration::from_secs(2),
                    tokio::net::UnixStream::connect(root.join("serial.sock")),
                )
                .await
                .map_err(|_| Status::deadline_exceeded("serial connect timed out"))?
                .map_err(|e| Status::unavailable(e.to_string()))?;
                Box::new(stream)
            }
            None => return Err(Status::invalid_argument("guest endpoint required")),
        };
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let service = self.clone();
        let task = tokio::spawn(async move {
            let _permit = permit;
            let (mut reader, mut writer) = tokio::io::split(io);
            let upload = async {
                while let Some(frame) = input.message().await? {
                    let Some(open_guest_channel_request::Frame::Data(data)) = frame.frame else {
                        return Err(Status::invalid_argument("selector may only appear once"));
                    };
                    if data.len() > CHUNK_LIMIT {
                        return Err(Status::resource_exhausted(
                            "guest channel chunk exceeds 64 KiB",
                        ));
                    }
                    writer
                        .write_all(&data)
                        .await
                        .map_err(|e| Status::unavailable(e.to_string()))?;
                }
                writer
                    .shutdown()
                    .await
                    .map_err(|e| Status::unavailable(e.to_string()))
            };
            let download = async {
                let mut buffer = vec![0; CHUNK_LIMIT];
                loop {
                    let read = reader
                        .read(&mut buffer)
                        .await
                        .map_err(|e| Status::unavailable(e.to_string()))?;
                    if read == 0 {
                        return Ok::<_, Status>(());
                    }
                    let data = buffer
                        .get(..read)
                        .ok_or_else(|| Status::internal("invalid guest read size"))?
                        .to_vec();
                    if tx
                        .send(Ok(OpenGuestChannelResponse { data }))
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                }
            };
            let monitor = async {
                let mut interval = tokio::time::interval(Duration::from_millis(250));
                loop {
                    interval.tick().await;
                    service.check_execution(
                        &selector.sandbox_id,
                        selector.execution_fence.as_ref(),
                        false,
                    )?;
                    if service
                        .pool
                        .get(&selector.sandbox_id)
                        .map_err(slot_store_status)?
                        .is_none()
                    {
                        return Err(Status::aborted("execution lease released"));
                    }
                }
            };
            let result = tokio::select! {
                biased;
                _ = service.pool.shutdown_token.cancelled() => Ok(()),
                _ = tx.closed() => Ok(()),
                result = monitor => result,
                result = async {
                    tokio::pin!(upload, download);
                    tokio::select! {
                        result = &mut download => result,
                        result = &mut upload => { result?; download.await }
                    }
                } => result,
            };
            if let Err(error) = result
                && let Err(error) = tx.try_send(Err(error))
            {
                tracing::debug!(%error, "guest channel error receiver unavailable");
            }
        });
        Ok(Response::new(ChannelStream {
            receiver: ReceiverStream::new(rx),
            task,
        }))
    }
}
