//! Fenced guest access. No request can select a host address, file or VMM socket.
use super::*;
use crate::proto::*;
use std::{
    future::Future,
    num::NonZeroU16,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_stream::{Stream, wrappers::ReceiverStream};

const CHUNK_LIMIT: usize = 64 * 1024;
pub(super) struct GuestAccessBudgets {
    probes: Arc<tokio::sync::Semaphore>,
    channels: Arc<tokio::sync::Semaphore>,
}
impl Default for GuestAccessBudgets {
    fn default() -> Self {
        Self {
            probes: Arc::new(tokio::sync::Semaphore::new(32)),
            channels: Arc::new(tokio::sync::Semaphore::new(32)),
        }
    }
}

fn validate_serial_peer(
    stream: &tokio::net::UnixStream,
    uid: u32,
    owner: &pplx_space_node_runtime_core::launch::ProcessObserver,
) -> Result<(), Status> {
    let credentials = stream
        .peer_cred()
        .map_err(|error| Status::unavailable(error.to_string()))?;
    if credentials.uid() != uid
        || credentials.gid() != uid
        || credentials.pid().and_then(|pid| u32::try_from(pid).ok()) != Some(owner.pid())
        || owner
            .has_exited()
            .map_err(|error| Status::unavailable(error.to_string()))?
    {
        return Err(Status::failed_precondition(
            "serial listener is not the registered VMM",
        ));
    }
    Ok(())
}

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
    terminal: tokio::sync::oneshot::Receiver<Result<(), Status>>,
    terminal_delivered: bool,
}
impl Stream for ChannelStream {
    type Item = Result<OpenGuestChannelResponse, Status>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.receiver).poll_next(cx) {
            Poll::Ready(None) => {}
            other => return other,
        }
        if self.terminal_delivered {
            return Poll::Ready(None);
        }
        let result = match Pin::new(&mut self.terminal).poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Ok(result)) => result,
            Poll::Ready(Err(_)) => Err(Status::internal(
                "guest channel task ended without an outcome",
            )),
        };
        self.terminal_delivered = true;
        Poll::Ready(result.err().map(Err))
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
            let control = service.runtime_control(runtime, &id.to_string()).await?;
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
        let _permit = self
            .guest_access
            .probes
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("guest probe capacity exhausted"))?;
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
        let permit = self
            .guest_access
            .channels
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
                let owner = self.pin_runtime_owner(&selector.sandbox_id).await?;
                let lease = self
                    .pool
                    .get(&selector.sandbox_id)
                    .map_err(slot_store_status)?
                    .ok_or_else(|| Status::failed_precondition("serial execution has no lease"))?;
                let stream = tokio::time::timeout(
                    Duration::from_secs(2),
                    tokio::net::UnixStream::connect(root.join("serial.sock")),
                )
                .await
                .map_err(|_| Status::deadline_exceeded("serial connect timed out"))?
                .map_err(|e| Status::unavailable(e.to_string()))?;
                validate_serial_peer(&stream, lease.sandbox_uid, &owner)?;
                self.check_execution(
                    &selector.sandbox_id,
                    selector.execution_fence.as_ref(),
                    false,
                )?;
                Box::new(stream)
            }
            None => return Err(Status::invalid_argument("guest endpoint required")),
        };
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let (terminal_tx, terminal) = tokio::sync::oneshot::channel();
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
            // A slow reader may fill the data queue. Terminal status has its
            // own slot, so reporting a fence failure never blocks teardown.
            if terminal_tx.send(result).is_err() {
                tracing::debug!("guest channel receiver closed before terminal status");
            }
        });
        Ok(Response::new(ChannelStream {
            receiver: ReceiverStream::new(rx),
            task,
            terminal,
            terminal_delivered: false,
        }))
    }
}

#[cfg(test)]
mod tests;
