//! The HTTP client SPACE's S3 clients share: the SDK's default HTTPS client
//! with a connection-pool idle timeout below S3's idle close.
//!
//! The SDK's default client keeps hyper's 90 s pool idle timeout, so a
//! request that takes a pooled connection as S3 closes it fails with
//! "connection closed before message completed". Pass [`s3_http_client`] to
//! `http_client` on every S3 client config or `SdkConfig` loader.

mod client_span;

use std::time::Duration;

use aws_smithy_http_client::proxy::ProxyConfig;
use aws_smithy_http_client::{Builder, ConnectorBuilder, tls};
use aws_smithy_runtime_api::client::http::SharedHttpClient;

pub use client_span::S3ClientSpans;

/// How long an idle pooled connection stays eligible for reuse. S3
/// (us-east-1, measured 2026-10) closes an idle HTTPS keep-alive connection
/// 6.0 s after its last response, with a TLS close_notify and a FIN; reuse
/// after 5.8 s works and after 6.2 s fails. Half the server's window leaves
/// room for the time between S3 finishing a response and the client
/// returning the connection to the pool.
pub const DEFAULT_POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(3);

/// The SDK's default HTTPS client with `pool_idle_timeout` on its connection
/// pool.
///
/// Mirrors `default_https_client` in aws-smithy-runtime **1.14.0**:
///
/// - the rustls TLS provider with the AwsLc crypto mode;
/// - the connector settings (connect and read timeouts) each operation
///   passes in;
/// - the sleep impl from the operation's runtime components, which the
///   connect timeout runs on;
/// - `ProxyConfig::from_env()`. The SDK applies it for behavior versions
///   v2025_08_07 and later and disables proxies below that; this applies it
///   unconditionally, so use it only with `BehaviorVersion::latest()`.
///
/// An SDK bump requires re-diffing this against that function. The idle
/// timeout has to be set on the connector builder: a connector function
/// ignores the outer builder's pool settings.
pub fn s3_http_client(pool_idle_timeout: Duration) -> SharedHttpClient {
    Builder::new().build_with_connector_fn(move |settings, runtime_components| {
        let mut connector = ConnectorBuilder::default()
            .pool_idle_timeout(pool_idle_timeout)
            .tls_provider(tls::Provider::Rustls(
                tls::rustls_provider::CryptoMode::AwsLc,
            ));
        connector.set_connector_settings(settings.cloned());
        if let Some(components) = runtime_components {
            connector.set_sleep_impl(components.sleep_impl());
        }
        connector.set_proxy_config(Some(ProxyConfig::from_env()));
        connector.build()
    })
}
