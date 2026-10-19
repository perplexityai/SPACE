//! `assume_s3_vend_role` against a local fake STS endpoint, so the status a
//! caller sees comes from the SDK's real transport and error parsing.

use std::path::PathBuf;
use std::time::Duration;

use aws_config::provider_config::ProviderConfig;
use aws_config::retry::RetryConfig;
use aws_config::timeout::TimeoutConfig;
use aws_config::web_identity_token::{StaticConfiguration, WebIdentityTokenCredentialsProvider};
use aws_credential_types::Credentials;
use aws_credential_types::credential_fn::provide_credentials_fn;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_sts::error::ConnectorError;
use aws_smithy_http_client::tls;
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, SharedHttpClient, SharedHttpConnector,
    http_client_fn,
};
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use aws_types::os_shim_internal::{Env, Fs};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tonic::Code;

use super::*;

const ROLE_ARN: &str = "arn:aws:iam::123456789012:role/space-storage-vend";
const BASE_ROLE_ARN: &str = "arn:aws:iam::123456789012:role/space-credmgr-base";
const SESSION_POLICY: &str = r#"{"Version":"2012-10-17","Statement":[]}"#;
/// Bounds the silent-endpoint case only. Every other case runs without an
/// attempt timeout, as production does, so a slow runner cannot turn a
/// rejection into a timeout.
const ATTEMPT_TIMEOUT: Duration = Duration::from_millis(500);

/// How the fake STS endpoint answers. The container credential endpoint
/// reuses the transport failures.
#[derive(Clone, Copy, Debug)]
enum FakeSts {
    /// Nothing listens on the endpoint's port, which stays bound so that no
    /// other test can listen there.
    Refused,
    /// Accepts the connection and never answers.
    Silent,
    /// Reads the request, then closes the connection without answering.
    Hangup,
    /// Starts a success response, then closes before the promised body.
    Truncated,
    /// Answers success with a body that is not an STS response.
    Unparseable,
    /// Answers with an STS query-protocol error.
    Error { status: u16, code: &'static str },
}

/// A started fake endpoint. Keep it until the request finishes: a refused
/// endpoint holds its port only while this lives.
struct Endpoint {
    url: String,
    _reserved: Option<TcpSocket>,
}

impl FakeSts {
    async fn start(self) -> Endpoint {
        if matches!(self, Self::Refused) {
            // Bound but never listening: a connection is refused, and no
            // other socket can bind the port while this one holds it.
            let socket = TcpSocket::new_v4().unwrap();
            socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
            return Endpoint {
                url: format!("http://{}", socket.local_addr().unwrap()),
                _reserved: Some(socket),
            };
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(self.answer(stream));
            }
        });
        Endpoint {
            url,
            _reserved: None,
        }
    }

    async fn answer(self, mut stream: TcpStream) {
        read_request(&mut stream).await;
        let response = match self {
            Self::Error { status, code } => {
                let body = format!(
                    "<ErrorResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
                     <Error><Type>Sender</Type><Code>{code}</Code><Message>synthetic</Message></Error>\
                     <RequestId>request</RequestId></ErrorResponse>"
                );
                format!(
                    "HTTP/1.1 {status} Error\r\ncontent-type: text/xml\r\ncontent-length: {}\r\n\
                     connection: close\r\n\r\n{body}",
                    body.len()
                )
            }
            Self::Truncated => {
                "HTTP/1.1 200 OK\r\ncontent-type: text/xml\r\ncontent-length: 4096\r\n\
                 connection: close\r\n\r\n<AssumeRoleResponse"
                    .to_owned()
            }
            Self::Unparseable => {
                "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 7\r\n\
                 connection: close\r\n\r\nnot xml"
                    .to_owned()
            }
            Self::Hangup => return,
            // Hold the connection open without answering.
            Self::Refused | Self::Silent => std::future::pending().await,
        };
        stream.write_all(response.as_bytes()).await.unwrap();
    }
}

/// Read one HTTP/1.1 request with a `content-length` body.
async fn read_request(stream: &mut TcpStream) {
    let mut request = Vec::new();
    let mut buf = [0_u8; 4096];
    loop {
        let read = stream.read(&mut buf).await.unwrap_or(0);
        if read == 0 {
            return;
        }
        request.extend_from_slice(&buf[..read]);
        let Some(header_end) = request.windows(4).position(|w| w == b"\r\n\r\n") else {
            continue;
        };
        let headers = String::from_utf8_lossy(&request[..header_end]).to_ascii_lowercase();
        let content_length = headers
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or(0);
        if request.len() >= header_end + 4 + content_length {
            return;
        }
    }
}

/// Sends every request to `endpoint`. The web-identity provider builds its
/// own STS client and offers no endpoint override, so the test redirects its
/// transport instead.
#[derive(Debug)]
struct Redirect {
    endpoint: String,
    inner: SharedHttpConnector,
}

impl HttpConnector for Redirect {
    fn call(&self, mut request: HttpRequest) -> HttpConnectorFuture {
        request
            .set_uri(self.endpoint.as_str())
            .expect("the fake STS endpoint is a valid URI");
        self.inner.call(request)
    }
}

fn redirect(http_client: SharedHttpClient, endpoint: String) -> SharedHttpClient {
    http_client_fn(move |settings, components| {
        SharedHttpConnector::new(Redirect {
            endpoint: endpoint.clone(),
            inner: http_client.http_connector(settings, components),
        })
    })
}

fn static_credentials() -> SharedCredentialsProvider {
    SharedCredentialsProvider::new(Credentials::new(
        "synthetic-access-key",
        "synthetic-secret-key",
        None,
        None,
        "assume-role-test",
    ))
}

async fn sts_config(
    endpoint: String,
    base: SharedCredentialsProvider,
    attempt_timeout: Option<Duration>,
) -> aws_config::SdkConfig {
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .credentials_provider(base)
        .retry_config(RetryConfig::disabled());
    if let Some(timeout) = attempt_timeout {
        loader = loader.timeout_config(
            TimeoutConfig::builder()
                .operation_attempt_timeout(timeout)
                .build(),
        );
    }
    loader.load().await
}

/// Web-identity base credentials, as an IRSA deployment gives the authority,
/// whose `AssumeRoleWithWebIdentity` call goes to `endpoint`.
fn web_identity(endpoint: String, token_file: PathBuf) -> SharedCredentialsProvider {
    let http_client = aws_smithy_http_client::Builder::new()
        .tls_provider(tls::Provider::Rustls(
            tls::rustls_provider::CryptoMode::AwsLc,
        ))
        .build_https();
    SharedCredentialsProvider::new(
        WebIdentityTokenCredentialsProvider::builder()
            .static_configuration(StaticConfiguration {
                web_identity_token_file: token_file,
                role_arn: BASE_ROLE_ARN.to_owned(),
                session_name: "space-credmgr-base".to_owned(),
            })
            .configure(
                &ProviderConfig::without_region()
                    .with_region(Some(aws_config::Region::new("us-east-1")))
                    .with_http_client(redirect(http_client, endpoint))
                    .with_retry_config(RetryConfig::disabled()),
            )
            .build(),
    )
}

/// The default credentials chain with only the container credential
/// endpoint configured, as EKS Pod Identity and ECS give the authority.
async fn container_config(endpoint: String) -> aws_config::SdkConfig {
    let full_uri = format!("{endpoint}/credentials");
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .env(Env::from_slice(&[(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            full_uri.as_str(),
        )]))
        .fs(Fs::from_slice(&[]))
        .region(aws_config::Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .retry_config(RetryConfig::disabled())
        .load()
        .await
}

/// What fails, and where.
enum Case {
    /// The storage role's `AssumeRole` call.
    AssumeRole(FakeSts),
    /// The base identity's `AssumeRoleWithWebIdentity` call.
    WebIdentity(FakeSts),
    /// The base identity's web-identity token file does not exist.
    MissingTokenFile,
    /// The base identity's chained source-role `AssumeRole` call.
    SourceRole(FakeSts),
    /// The base identity's container credential endpoint call.
    Container(FakeSts),
    /// The base identity provider fails with this error, where no local
    /// endpoint can produce it cheaply.
    BaseProvider(fn() -> CredentialsError),
    /// The storage role's `AssumeRole` call fails with this SDK error, where
    /// no local socket can produce it cheaply.
    Sdk(fn() -> SdkError<AssumeRoleError>),
}

impl Case {
    async fn status(self) -> Status {
        let token_dir = tempfile::tempdir().unwrap();
        let token_file = token_dir.path().join("token");
        std::fs::write(&token_file, "synthetic-web-identity-token").unwrap();
        let sts = match &self {
            Self::AssumeRole(sts)
            | Self::WebIdentity(sts)
            | Self::SourceRole(sts)
            | Self::Container(sts) => *sts,
            Self::MissingTokenFile | Self::BaseProvider(_) => FakeSts::Refused,
            Self::Sdk(error) => {
                return assume_role_failure_status(&CredentialsError::provider_error(error()));
            }
        };
        let endpoint = sts.start().await;
        let url = endpoint.url.clone();
        let config = match self {
            Self::AssumeRole(sts) => {
                let timeout = matches!(sts, FakeSts::Silent).then_some(ATTEMPT_TIMEOUT);
                sts_config(url, static_credentials(), timeout).await
            }
            Self::WebIdentity(_) => {
                let base = web_identity(url.clone(), token_file);
                sts_config(url, base, None).await
            }
            Self::MissingTokenFile => {
                let base = web_identity(url.clone(), token_dir.path().join("missing"));
                sts_config(url, base, None).await
            }
            Self::SourceRole(_) => {
                let source = aws_config::sts::AssumeRoleProvider::builder(BASE_ROLE_ARN)
                    .session_name("space-credmgr-base")
                    .configure(&sts_config(url.clone(), static_credentials(), None).await)
                    .build()
                    .await;
                sts_config(url, SharedCredentialsProvider::new(source), None).await
            }
            Self::Container(_) => container_config(url).await,
            Self::BaseProvider(error) => {
                let base = provide_credentials_fn(move || async move { Err(error()) });
                sts_config(url, SharedCredentialsProvider::new(base), None).await
            }
            Self::Sdk(_) => unreachable!("answered without an endpoint"),
        };
        let status = assume_s3_vend_role(&config, ROLE_ARN, SESSION_POLICY, 900)
            .await
            .expect_err("the fake STS never issues credentials");
        drop(endpoint);
        status
    }
}

/// Transient STS and base-identity failures must stay retryable;
/// authorization and configuration rejections must not.
#[tokio::test]
async fn sts_failures_are_unavailable_only_when_transient() {
    let error = |status, code| FakeSts::Error { status, code };
    let cases = [
        (
            "connection refused",
            Case::AssumeRole(FakeSts::Refused),
            Code::Unavailable,
        ),
        (
            "attempt timeout",
            Case::AssumeRole(FakeSts::Silent),
            Code::Unavailable,
        ),
        (
            "connect timeout",
            Case::Sdk(|| {
                SdkError::dispatch_failure(ConnectorError::timeout("connect timed out".into()))
            }),
            Code::Unavailable,
        ),
        (
            "connection closed",
            Case::AssumeRole(FakeSts::Hangup),
            Code::Unavailable,
        ),
        (
            "truncated response",
            Case::AssumeRole(FakeSts::Truncated),
            Code::Unavailable,
        ),
        (
            "service unavailable",
            Case::AssumeRole(error(503, "ServiceUnavailable")),
            Code::Unavailable,
        ),
        (
            "throttling",
            Case::AssumeRole(error(400, "Throttling")),
            Code::Unavailable,
        ),
        (
            "request timeout",
            Case::AssumeRole(error(400, "RequestTimeout")),
            Code::Unavailable,
        ),
        (
            "not implemented",
            Case::AssumeRole(error(501, "NotImplemented")),
            Code::FailedPrecondition,
        ),
        (
            "unparseable response",
            Case::AssumeRole(FakeSts::Unparseable),
            Code::FailedPrecondition,
        ),
        (
            "access denied",
            Case::AssumeRole(error(403, "AccessDenied")),
            Code::FailedPrecondition,
        ),
        (
            "invalid client credentials",
            Case::AssumeRole(error(403, "InvalidClientTokenId")),
            Code::FailedPrecondition,
        ),
        (
            "malformed session policy",
            Case::AssumeRole(error(400, "MalformedPolicyDocument")),
            Code::FailedPrecondition,
        ),
        (
            "base: connection refused",
            Case::WebIdentity(FakeSts::Refused),
            Code::Unavailable,
        ),
        (
            "base: service unavailable",
            Case::WebIdentity(error(503, "ServiceUnavailable")),
            Code::Unavailable,
        ),
        (
            "base: identity provider unreachable",
            Case::WebIdentity(error(400, "IDPCommunicationError")),
            Code::Unavailable,
        ),
        (
            "base: access denied",
            Case::WebIdentity(error(403, "AccessDenied")),
            Code::FailedPrecondition,
        ),
        (
            "base: missing token file",
            Case::MissingTokenFile,
            Code::FailedPrecondition,
        ),
        (
            "base: source role throttled",
            Case::SourceRole(error(400, "Throttling")),
            Code::Unavailable,
        ),
        (
            "base: provider timed out",
            Case::BaseProvider(|| CredentialsError::provider_timed_out(Duration::from_secs(5))),
            Code::Unavailable,
        ),
        (
            "base: container endpoint refused",
            Case::Container(FakeSts::Refused),
            Code::Unavailable,
        ),
        (
            "base: container endpoint truncated response",
            Case::Container(FakeSts::Truncated),
            Code::Unavailable,
        ),
        (
            "base: no credentials configured",
            Case::BaseProvider(|| CredentialsError::not_loaded("no providers in chain")),
            Code::FailedPrecondition,
        ),
    ];

    let mut expected = Vec::new();
    let mut actual = Vec::new();
    let mut statuses = Vec::new();
    for (name, case, code) in cases {
        let status = case.status().await;
        expected.push((name, code));
        actual.push((name, status.code()));
        statuses.push((name, status));
    }
    assert_eq!(actual, expected);
    // The node agent relays only a reason-carrying `Unavailable` as the
    // authority's answer rather than a lost link.
    for (name, status) in statuses {
        assert_eq!(
            pplx_space_credmgr_client::status_is_sts_unavailable(&status),
            status.code() == Code::Unavailable,
            "{name}: only a transient failure carries the STS reason"
        );
    }
}
