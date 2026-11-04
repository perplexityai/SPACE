//! Fixture for the production gateway's host-control and ingress RPC boundaries.
//! Policy decisions and packet forwarding always happen in space-netgtw-svc.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, ensure};
use clap::{Parser, Subcommand, ValueEnum};
use pplx_space_common_core::types::{
    ClusterId, GuestPort, SandboxRouteIdentity, SpaceApiRouteIdentity,
};
use pplx_space_common_svc::space_api_route::SpaceApiRouteProtocol;
use pplx_space_common_svc::space_api_route_ed25519::{
    SpaceApiRouteEd25519Claims, SpaceApiRouteEd25519Issuer, SpaceApiRouteEd25519KeyId,
    SpaceApiRouteEd25519Signer, insert_space_api_route_ed25519_token,
};
use pplx_space_credential_injection_core::proto::ResolvedNetworkCredential;
use pplx_space_net_common::{
    CredentialHeaderBindingV1, EffectiveNetworkPolicyRef, EffectiveNetworkPolicyRefScope,
    EgressDefaultAction, EgressDestinationV1, EgressPolicyV1, NetworkCredentialDestinationV1,
    NetworkCredentialHttpMethodV1, NetworkCredentialRuleV1, NetworkPolicyV1, TlsInterception,
};
use pplx_space_net_gateway_client::proto::{
    self, net_gateway_control_client::NetGatewayControlClient,
};
use pplx_space_spaced_common::proto::{activity, host_control};
use serde::Deserialize;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

const SANDBOX: &str = "01990000-0000-7000-8000-000000000001";
const NODE: &str = "01990000-0000-7000-8000-000000000002";
const ATTEMPT: &str = "01990000-0000-7000-8000-000000000003";
const NETWORK_CREDENTIAL_REFERENCE: &str = "01990000-0000-7000-8000-000000000004";

#[derive(Parser)]
#[command(name = "netgtw-traffic-control")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Serve {
        policy: PathBuf,
        /// Public guest trust root; the disposable signing keys remain in this process.
        #[arg(long)]
        tls_root: Option<PathBuf>,
    },
    Refresh {
        socket: PathBuf,
        policy: PathBuf,
    },
    State {
        socket: PathBuf,
    },
    NoIdentity {
        socket: PathBuf,
    },
    Configure {
        socket: PathBuf,
        key: PathBuf,
    },
    Ingress {
        socket: PathBuf,
        key: PathBuf,
        port: u16,
        #[arg(value_enum)]
        token: Token,
        payload: String,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum Token {
    Valid,
    Missing,
    WrongPort,
    WrongSandbox,
    StaleRoute,
    InvalidSignature,
}

impl Token {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Valid => "valid",
            Self::Missing => "missing",
            Self::WrongPort => "wrong-port",
            Self::WrongSandbox => "wrong-sandbox",
            Self::StaleRoute => "stale-route",
            Self::InvalidSignature => "invalid-signature",
        }
    }
}

impl std::fmt::Display for Token {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for Token {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        <Self as ValueEnum>::from_str(value, false)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyDocument {
    default_action: EgressDefaultAction,
    allow: Vec<String>,
    deny: Vec<String>,
    tls_interception: TlsInterception,
    unavailable: bool,
    revision: u64,
    credential: Option<CredentialDocument>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialDocument {
    exact_host: pplx_space_net_common::DomainName,
    ports: Vec<u16>,
    path_prefix: String,
    methods: Vec<NetworkCredentialHttpMethodV1>,
    header_name: String,
    value_file: PathBuf,
}

fn policy_reference(bytes: &[u8]) -> anyhow::Result<EffectiveNetworkPolicyRef> {
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes);
    let sha = digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(EffectiveNetworkPolicyRef::local(
        EffectiveNetworkPolicyRefScope::Sandbox,
        SANDBOX,
        sha,
    )?)
}

async fn policy_response(path: &Path) -> anyhow::Result<host_control::GetNetworkPolicyResponse> {
    let bytes = tokio::fs::read(path).await?;
    let document: PolicyDocument = serde_json::from_slice(&bytes)?;
    let mut response = host_control::GetNetworkPolicyResponse::default();
    ensure!(
        !document.unavailable,
        "fixture policy source is unavailable"
    );
    ensure!(document.revision > 0, "policy revision must be positive");
    let parse = |text: String| -> anyhow::Result<EgressDestinationV1> {
        Ok(if let Ok(ip) = text.parse() {
            EgressDestinationV1::Ip(ip)
        } else if text.contains('/') {
            EgressDestinationV1::Cidr(text.parse()?)
        } else if text.starts_with("*.") {
            EgressDestinationV1::WildcardDomain(text.parse()?)
        } else {
            EgressDestinationV1::Domain(text.parse()?)
        })
    };
    let mut network_credential_rules = Vec::new();
    if let Some(credential) = document.credential {
        ensure!(
            !credential.ports.is_empty(),
            "credential fixture needs at least one port"
        );
        ensure!(
            !credential.methods.is_empty(),
            "credential fixture needs at least one method"
        );
        let reference_id = NETWORK_CREDENTIAL_REFERENCE.parse()?;
        for port in credential.ports {
            network_credential_rules.push(NetworkCredentialRuleV1 {
                rule_id: format!("traffic-{port}"),
                destination: NetworkCredentialDestinationV1::new(
                    credential.exact_host.clone(),
                    Some(u32::from(port)),
                    Some(credential.path_prefix.clone()),
                )?,
                credential_header_bindings: vec![CredentialHeaderBindingV1 {
                    header_name: credential.header_name.clone(),
                    credential_reference_id: reference_id,
                }],
                supported_http_methods: credential.methods.clone(),
            });
        }
        let value = tokio::fs::read(&credential.value_file)
            .await
            .context("read traffic credential")?;
        ensure!(!value.is_empty(), "traffic credential must not be empty");
        response
            .network_credentials
            .push(ResolvedNetworkCredential {
                credential_reference_id: NETWORK_CREDENTIAL_REFERENCE.to_owned(),
                value,
            });
    }
    let policy = NetworkPolicyV1 {
        version: Default::default(),
        tls_interception: document.tls_interception,
        tls_interception_exemptions: Vec::new(),
        network_credential_rules,
        egress: EgressPolicyV1::new(
            document.default_action,
            document
                .allow
                .into_iter()
                .map(parse)
                .collect::<anyhow::Result<_>>()?,
            document
                .deny
                .into_iter()
                .map(parse)
                .collect::<anyhow::Result<_>>()?,
        )?,
    };
    response.effective_policy = Some((&policy).into());
    response.effective_policy_ref = policy_reference(&bytes)?.to_string();
    Ok(response)
}

struct HostControl {
    policy: PathBuf,
    ca: Option<std::sync::Arc<FixtureCa>>,
}

struct FixtureCa {
    node: rcgen::Issuer<'static, rcgen::KeyPair>,
    certificate: Vec<u8>,
}

impl FixtureCa {
    fn new() -> anyhow::Result<(Self, String)> {
        let params = |name: &str, path_len| -> anyhow::Result<rcgen::CertificateParams> {
            let mut params = rcgen::CertificateParams::default();
            params
                .distinguished_name
                .push(rcgen::DnType::CommonName, name);
            params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Constrained(path_len));
            params.key_usages = vec![
                rcgen::KeyUsagePurpose::KeyCertSign,
                rcgen::KeyUsagePurpose::CrlSign,
            ];
            params.not_before = time::OffsetDateTime::now_utc() - time::Duration::minutes(1);
            params.not_after = time::OffsetDateTime::now_utc() + time::Duration::hours(2);
            Ok(params)
        };
        let root = params("traffic guest root", 2)?;
        let root_key = rcgen::KeyPair::generate()?;
        let root_certificate = root.self_signed(&root_key)?;
        let root = rcgen::Issuer::new(root, root_key);
        let node = params("traffic node CA", 1)?;
        let key = rcgen::KeyPair::generate()?;
        let certificate = node.signed_by(&key, &root)?.der().to_vec();
        Ok((
            Self {
                node: rcgen::Issuer::new(node, key),
                certificate,
            },
            root_certificate.pem(),
        ))
    }

    fn issue(
        &self,
        csr: Vec<u8>,
    ) -> anyhow::Result<host_control::IssueGatewayIntermediateCertificateResponse> {
        let mut request = rcgen::CertificateSigningRequestParams::from_der(&csr.into())?;
        request.params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Constrained(0));
        request.params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        request.params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
        request.params.not_before = time::OffsetDateTime::now_utc() - time::Duration::seconds(30);
        request.params.not_after = time::OffsetDateTime::now_utc() + time::Duration::hours(1);
        Ok(host_control::IssueGatewayIntermediateCertificateResponse {
            certificate_der: request.signed_by(&self.node)?.der().to_vec(),
            issuer_certificate_der: self.certificate.clone(),
        })
    }
}

#[tonic::async_trait]
impl host_control::host_control_server::HostControl for HostControl {
    async fn issue_gateway_intermediate_certificate(
        &self,
        request: Request<host_control::IssueGatewayIntermediateCertificateRequest>,
    ) -> Result<Response<host_control::IssueGatewayIntermediateCertificateResponse>, Status> {
        let ca = self
            .ca
            .clone()
            .ok_or_else(|| Status::failed_precondition("fixture CA disabled"))?;
        tokio::task::spawn_blocking(move || ca.issue(request.into_inner().csr))
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map(Response::new)
            .map_err(|error| Status::internal(error.to_string()))
    }

    async fn get_network_policy(
        &self,
        _: Request<host_control::GetNetworkPolicyRequest>,
    ) -> Result<Response<host_control::GetNetworkPolicyResponse>, Status> {
        policy_response(&self.policy)
            .await
            .map(Response::new)
            .map_err(|error| Status::unavailable(format!("{error:#}")))
    }

    async fn set_activity_state(
        &self,
        _: Request<activity::SetActivityStateRequest>,
    ) -> Result<Response<activity::SetActivityStateResponse>, Status> {
        Err(Status::unimplemented(
            "activity is outside the traffic fixture",
        ))
    }

    async fn request_active_for(
        &self,
        _: Request<activity::RequestActiveForRequest>,
    ) -> Result<Response<activity::RequestActiveForResponse>, Status> {
        Err(Status::unimplemented(
            "activity is outside the traffic fixture",
        ))
    }

    async fn request_shutdown(
        &self,
        _: Request<activity::RequestShutdownRequest>,
    ) -> Result<Response<activity::RequestShutdownResponse>, Status> {
        Err(Status::unimplemented(
            "lifecycle is outside the traffic fixture",
        ))
    }
}

async fn client(
    socket: &Path,
) -> anyhow::Result<NetGatewayControlClient<tonic::transport::Channel>> {
    let channel = pplx_space_spaced_common::connect_uds_channel("gateway-traffic", socket).await?;
    Ok(NetGatewayControlClient::new(channel))
}

fn identity(generation: u64, sandbox: &str) -> anyhow::Result<SandboxRouteIdentity> {
    Ok(SpaceApiRouteIdentity {
        sandbox_id: Some(sandbox.parse()?),
        route_generation: Some(generation.try_into()?),
        current_node_id: Some(NODE.parse()?),
        substrate_kind: Some(pplx_space_common_core::types::NodeManagedBy::Space),
        execution_generation: Some(pplx_space_common_core::types::ExecutionGeneration::new(1)?),
        activation_attempt_id: Some(ATTEMPT.parse()?),
        pod_uid: None,
    }
    .try_into_complete()?)
}

async fn signer(key: &Path) -> anyhow::Result<SpaceApiRouteEd25519Signer> {
    Ok(SpaceApiRouteEd25519Signer::from_pkcs8(
        SpaceApiRouteEd25519KeyId::parse("traffic")?,
        &tokio::fs::read(key).await?,
    )?)
}

async fn command(command: Command) -> anyhow::Result<()> {
    match command {
        Command::Serve { policy, tls_root } => {
            let ca = if let Some(path) = tls_root {
                let (ca, pem) = tokio::task::spawn_blocking(FixtureCa::new).await??;
                let temporary = path.with_extension("tmp");
                tokio::fs::write(&temporary, pem).await?;
                tokio::fs::rename(temporary, path).await?;
                Some(std::sync::Arc::new(ca))
            } else {
                None
            };
            let socket =
                Path::new(pplx_space_common_core::host::linux::paths::SANDBOX_HOST_CONTROL_SOCKET);
            tokio::fs::create_dir_all(socket.parent().context("host-control socket parent")?)
                .await?;
            let listener = UnixListener::bind(socket)?;
            let shutdown = CancellationToken::new();
            let signal = shutdown.clone();
            let signal_task = tokio::spawn(async move {
                if let Err(error) = tokio::signal::ctrl_c().await {
                    tracing::warn!(%error, "host-control signal handler failed");
                }
                signal.cancel();
            });
            let result = tonic::transport::Server::builder()
                .add_service(host_control::host_control_server::HostControlServer::new(
                    HostControl { policy, ca },
                ))
                .serve_with_incoming_shutdown(
                    UnixListenerStream::new(listener),
                    shutdown.cancelled(),
                )
                .await;
            signal_task.abort();
            result?;
        }
        Command::Refresh { socket, policy } => {
            let mut client = client(&socket).await?;
            // A new desired ref forces a pull even when the current policy is
            // ready. This includes the unavailable-source revocation case.
            let expected = policy_reference(&tokio::fs::read(&policy).await?)?.to_string();
            let response = client
                .refresh_network_policy(proto::RefreshNetworkPolicyRequest {
                    expected_policy_ref: expected.clone(),
                    reason: "traffic-test".into(),
                })
                .await?
                .into_inner();
            if response.state == proto::NetworkState::Ready as i32 {
                ensure!(
                    response.installed_policy_ref == expected,
                    "gateway installed a different policy"
                );
            }
            println!(
                "{}",
                serde_json::json!({
                    "state": response.state,
                    "interception_readiness": response.interception_readiness,
                    "error": response.last_error,
                })
            );
        }
        Command::State { socket } => {
            let response = client(&socket)
                .await?
                .get_network_state(proto::GetNetworkStateRequest {})
                .await?
                .into_inner();
            println!(
                "{}",
                serde_json::json!({
                    "state": response.state,
                    "interception_readiness": response.interception_readiness,
                    "error": response.last_error,
                })
            );
        }
        Command::NoIdentity { socket } => {
            let response = client(&socket)
                .await?
                .set_no_identity(proto::SetNoIdentityRequest {
                    reason: "traffic-test".into(),
                })
                .await?
                .into_inner();
            println!("{}", serde_json::json!({"state": response.state}));
        }
        Command::Configure { socket, key } => {
            let mut client = client(&socket).await?;
            let public_key = signer(&key).await?.public_key();
            client
                .set_route_verification_keys(proto::SetRouteVerificationKeysRequest {
                    generation: 1,
                    issuer: "traffic".into(),
                    cluster_id: "traffic".into(),
                    keys: vec![proto::RouteVerificationKey {
                        key_id: public_key.key_id.to_string(),
                        ed25519_public_key: public_key.public_key,
                    }],
                })
                .await?;
            client
                .set_route_identity(proto::SetRouteIdentityRequest {
                    identity: pplx_space_net_gateway::ingress::route_identity_to_proto(
                        &identity(2, SANDBOX)?.into(),
                    ),
                })
                .await?;
            println!("{{\"configured\":true}}");
        }
        Command::Ingress {
            socket,
            key,
            port,
            token,
            payload,
        } => {
            let mut client = client(&socket).await?;
            let payload_len = payload.len();
            let frames = vec![
                proto::ServicePortIngressFrame {
                    message: Some(proto::service_port_ingress_frame::Message::Start(
                        proto::ServicePortIngressStart {
                            sandbox_id: SANDBOX.into(),
                            port: u32::from(port),
                        },
                    )),
                },
                proto::ServicePortIngressFrame {
                    message: Some(proto::service_port_ingress_frame::Message::Data(
                        payload.into_bytes(),
                    )),
                },
            ];
            // Keep the request stream open until the guest echoes the payload;
            // closing it is the ingress protocol's cancellation signal.
            let (request_sender, request_receiver) = tokio::sync::mpsc::channel(2);
            for frame in frames {
                request_sender.send(frame).await?;
            }
            let mut request = Request::new(tokio_stream::wrappers::ReceiverStream::new(
                request_receiver,
            ));
            if !matches!(token, Token::Missing) {
                let route = identity(
                    if matches!(token, Token::StaleRoute) {
                        1
                    } else {
                        2
                    },
                    if matches!(token, Token::WrongSandbox) {
                        NODE
                    } else {
                        SANDBOX
                    },
                )?;
                let claims = SpaceApiRouteEd25519Claims::service_port_ingress(
                    SpaceApiRouteEd25519Issuer::parse("traffic")?,
                    ClusterId::parse("traffic")?,
                    route,
                    GuestPort::try_from_u16(if matches!(token, Token::WrongPort) {
                        port.checked_add(1)
                            .context("wrong-port token requires a port below 65535")?
                    } else {
                        port
                    })?,
                    SpaceApiRouteProtocol::Tcp,
                )?;
                let signer = if matches!(token, Token::InvalidSignature) {
                    let key = aws_lc_rs::signature::Ed25519KeyPair::generate_pkcs8(
                        &aws_lc_rs::rand::SystemRandom::new(),
                    )?;
                    SpaceApiRouteEd25519Signer::from_pkcs8(
                        SpaceApiRouteEd25519KeyId::parse("traffic")?,
                        key.as_ref(),
                    )?
                } else {
                    signer(&key).await?
                };
                insert_space_api_route_ed25519_token(
                    request.metadata_mut(),
                    &signer.mint(&claims)?,
                )?;
            }
            let result = async {
                let mut stream = client.service_port_ingress(request).await?.into_inner();
                let mut data = Vec::new();
                while let Some(frame) = stream.message().await? {
                    match frame.message {
                        Some(proto::service_port_ingress_frame::Message::Data(bytes)) => {
                            data.extend_from_slice(&bytes);
                            if data.len() >= payload_len {
                                break;
                            }
                        }
                        Some(proto::service_port_ingress_frame::Message::Eof(())) => break,
                        _ => {
                            return Err(Status::invalid_argument(
                                "unexpected ingress response frame",
                            ));
                        }
                    }
                }
                Ok::<_, Status>(data)
            }
            .await;
            drop(request_sender);
            match result {
                Ok(data) => println!(
                    "{}",
                    serde_json::json!({"code": 0, "data": String::from_utf8(data)?})
                ),
                Err(error) => println!(
                    "{}",
                    serde_json::json!({"code": error.code() as i32, "error": error.message()})
                ),
            }
        }
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    if matches!(cli.command, Command::Serve { .. }) {
        command(cli.command).await
    } else {
        tokio::time::timeout(Duration::from_secs(10), command(cli.command)).await?
    }
}
