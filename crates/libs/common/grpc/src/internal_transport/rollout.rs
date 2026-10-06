//! Explicit listener compatibility for a staged plaintext-to-TLS migration.
//! Clients choose one protocol; failed TLS never becomes a plaintext connection.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::{Stream, StreamExt};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, server::TlsStream};
use tokio_util::sync::CancellationToken;
use tonic::transport::server::{Connected, TcpConnectInfo};

use super::{CERT_ENV, KEY_ENV, TransportError, file, insecure};
use pplx_space_common_core::env::{Environment, ProcessEnvironment};

pub const ALLOW_PLAINTEXT_ROLLOUT_ENV: &str = "SPACE_INTERNAL_RPC_ALLOW_PLAINTEXT_ROLLOUT";
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_PENDING_HANDSHAKES: usize = 64;

#[derive(Clone)]
pub struct ServerTransport {
    tls: Option<TlsAcceptor>,
    allow_plaintext: bool,
}

impl ServerTransport {
    pub fn from_env() -> Result<Self, TransportError> {
        Self::from_environment(&ProcessEnvironment)
    }

    pub fn from_environment(env: &dyn Environment) -> Result<Self, TransportError> {
        if insecure(env) && env.var_os(CERT_ENV).is_none() && env.var_os(KEY_ENV).is_none() {
            return Ok(Self {
                tls: None,
                allow_plaintext: true,
            });
        }
        // A compatibility flag never makes missing or invalid TLS identity acceptable.
        let certs =
            CertificateDer::pem_slice_iter(&file(env, CERT_ENV)?).collect::<Result<Vec<_>, _>>()?;
        let key = PrivateKeyDer::from_pem_slice(&file(env, KEY_ENV)?)?;
        let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
        config.alpn_protocols = vec![b"h2".to_vec()];
        let allow_plaintext = pplx_space_common_core::env::env_flag(
            env.var(ALLOW_PLAINTEXT_ROLLOUT_ENV).ok().as_deref(),
            false,
        );
        Ok(Self {
            tls: Some(TlsAcceptor::from(Arc::new(config))),
            allow_plaintext,
        })
    }

    pub fn server_builder(&self) -> crate::GrpcServer {
        // Incoming has already authenticated and decrypted TLS before tonic sees it.
        crate::grpc_server_builder()
    }

    pub fn incoming<S>(
        &self,
        incoming: S,
        shutdown: CancellationToken,
    ) -> impl Stream<Item = Result<Connection, io::Error>> + Send + 'static
    where
        S: Stream<Item = Result<TcpStream, io::Error>> + Send + 'static,
    {
        let transport = self.clone();
        let accepted = incoming
            .map(move |socket| {
                let transport = transport.clone();
                async move {
                    let socket = socket?;
                    let peer = socket.peer_addr().ok();
                    match tokio::time::timeout(HANDSHAKE_TIMEOUT, transport.accept(socket)).await {
                        Ok(Ok(connection)) => Ok(Some(connection)),
                        Ok(Err(error)) => {
                            // A bad peer is local to this connection, not a listener failure.
                            tracing::debug!(?peer, %error, "internal RPC connection rejected");
                            Ok(None)
                        }
                        Err(error) => {
                            tracing::debug!(?peer, %error, "internal RPC handshake timed out");
                            Ok(None)
                        }
                    }
                }
            })
            .buffer_unordered(MAX_PENDING_HANDSHAKES)
            .filter_map(|result| async move { result.transpose() });
        // Tonic keeps this stream until accepted connections drain, and
        // `take_until` would only stop polling it. Dropping it at shutdown
        // closes the listener and pending handshakes, so an `SO_REUSEPORT`
        // successor receives every new connection.
        let mut accepted = Some(Box::pin(accepted));
        let mut shutdown = Box::pin(shutdown.cancelled_owned());
        futures_util::stream::poll_fn(move |cx| {
            let Some(stream) = accepted.as_mut() else {
                return Poll::Ready(None);
            };
            if shutdown.as_mut().poll(cx).is_ready() {
                accepted = None;
                return Poll::Ready(None);
            }
            stream.as_mut().poll_next(cx)
        })
    }

    async fn accept(&self, socket: TcpStream) -> io::Result<Connection> {
        if self.tls.is_none() {
            return Ok(Connection::Plaintext(socket));
        }
        let mut first = [0];
        if socket.peek(&mut first).await? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        if first[0] == 0x16 {
            let tls = self.tls.as_ref().expect("TLS acceptor was checked above");
            return tls
                .accept(socket)
                .await
                .map(|socket| Connection::Tls(Box::new(socket)));
        }
        if self.allow_plaintext {
            Ok(Connection::Plaintext(socket))
        } else {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "plaintext internal RPC is disabled",
            ))
        }
    }
}

#[derive(Debug)]
pub enum Connection {
    Plaintext(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl Connected for Connection {
    type ConnectInfo = TcpConnectInfo;
    fn connect_info(&self) -> Self::ConnectInfo {
        match self {
            Self::Plaintext(socket) => socket.connect_info(),
            Self::Tls(socket) => socket.get_ref().0.connect_info(),
        }
    }
}

impl AsyncRead for Connection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plaintext(socket) => Pin::new(socket).poll_read(cx, buf),
            Self::Tls(socket) => Pin::new(socket).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Connection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plaintext(socket) => Pin::new(socket).poll_write(cx, buf),
            Self::Tls(socket) => Pin::new(socket).poll_write(cx, buf),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plaintext(socket) => Pin::new(socket).poll_flush(cx),
            Self::Tls(socket) => Pin::new(socket).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plaintext(socket) => Pin::new(socket).poll_shutdown(cx),
            Self::Tls(socket) => Pin::new(socket).poll_shutdown(cx),
        }
    }
}
