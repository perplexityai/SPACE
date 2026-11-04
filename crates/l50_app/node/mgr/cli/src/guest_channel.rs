//! Operator access to a single fenced guest endpoint; no external listener.
use pplx_space_node_mgr_client::{NodeManagerGrpcClient, proto};
use std::num::NonZeroU16;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(clap::Args)]
pub(super) struct Args {
    sandbox_id: pplx_space_common_core::types::SandboxId,
    /// Guest TCP service. Omit for the serial console.
    #[arg(long)]
    port: Option<NonZeroU16>,
    /// Accept one connection on 127.0.0.1 instead of forwarding stdin/stdout.
    #[arg(long, requires = "port")]
    listen: Option<NonZeroU16>,
}

pub(super) async fn run(
    client: &mut NodeManagerGrpcClient,
    args: Args,
) -> Result<(), tonic::Status> {
    let id = args.sandbox_id.to_string();
    let fence = super::operator_execution_fence(client, &id)
        .await?
        .ok_or_else(|| tonic::Status::failed_precondition("sandbox has no current execution"))?;
    let endpoint = match args.port {
        Some(port) => proto::guest_channel_selector::Endpoint::GuestTcpPort(u32::from(port.get())),
        None => proto::guest_channel_selector::Endpoint::Serial(proto::GuestSerialEndpoint {}),
    };
    let selector = proto::GuestChannelSelector {
        sandbox_id: id,
        execution_fence: Some(fence),
        endpoint: Some(endpoint),
    };
    match args.listen {
        Some(port) => {
            let listener =
                tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port.get()))
                    .await
                    .map_err(io_status)?;
            eprintln!("Waiting for one connection on 127.0.0.1:{port}");
            let (socket, _) = listener.accept().await.map_err(io_status)?;
            drop(listener);
            let (read, write) = socket.into_split();
            forward(client, selector, read, write).await
        }
        None => forward(client, selector, tokio::io::stdin(), tokio::io::stdout()).await,
    }
}
fn io_status(error: std::io::Error) -> tonic::Status {
    tonic::Status::unavailable(error.to_string())
}

async fn forward<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    client: &mut NodeManagerGrpcClient,
    selector: proto::GuestChannelSelector,
    mut reader: R,
    mut writer: W,
) -> Result<(), tonic::Status> {
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(proto::OpenGuestChannelRequest {
        frame: Some(proto::open_guest_channel_request::Frame::Selector(selector)),
    })
    .await
    .map_err(|_| tonic::Status::internal("guest channel closed before selector"))?;
    let mut stream = client
        .open_guest_channel(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await?
        .into_inner();
    let upload = async move {
        let mut bytes = vec![0; 64 * 1024];
        loop {
            let count = reader.read(&mut bytes).await.map_err(io_status)?;
            if count == 0 {
                return Ok::<(), tonic::Status>(());
            }
            let data = bytes
                .get(..count)
                .ok_or_else(|| tonic::Status::internal("invalid read size"))?
                .to_vec();
            tx.send(proto::OpenGuestChannelRequest {
                frame: Some(proto::open_guest_channel_request::Frame::Data(data)),
            })
            .await
            .map_err(|_| tonic::Status::unavailable("guest channel closed"))?;
        }
    };
    let download = async {
        while let Some(frame) = stream.message().await? {
            if frame.data.len() > 64 * 1024 {
                return Err(tonic::Status::data_loss("oversized guest channel response"));
            }
            writer.write_all(&frame.data).await.map_err(io_status)?;
            writer.flush().await.map_err(io_status)?;
        }
        writer.shutdown().await.map_err(io_status)
    };
    tokio::pin!(upload, download);
    tokio::select! {
        result = &mut download => result,
        result = &mut upload => { result?; download.await }
    }
}
