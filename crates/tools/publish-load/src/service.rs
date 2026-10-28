use std::collections::BTreeMap;
use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use pplx_space_node_api_client::proto::cluster_api_client::ClusterApiClient;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::process::{Child, Command};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

use crate::Args;

pub struct LocalService {
    child: Child,
    channel: Channel,
    pub token: MetadataValue<Ascii>,
    _secrets: TempDir,
}

impl LocalService {
    pub async fn start(args: &Args, database_url: &str) -> Result<Self> {
        let secrets = tempfile::tempdir()?;
        let cert = secrets.path().join("cert.pem");
        let key = secrets.path().join("key.pem");
        let signing = secrets.path().join("signing.der");
        openssl(&[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1",
            "-addext",
            "basicConstraints=critical,CA:FALSE",
            "-addext",
            "extendedKeyUsage=serverAuth",
            "-out",
            path(&cert)?,
            "-keyout",
            path(&key)?,
        ])
        .await?;
        openssl(&[
            "genpkey",
            "-algorithm",
            "ED25519",
            "-outform",
            "DER",
            "-out",
            path(&signing)?,
        ])
        .await?;
        let password = std::env::var("PGPASSWORD").context("launcher PGPASSWORD required")?;
        let role_env = [
            ("SPACE_DB_BOOTSTRAP_USER_PASSWORD", password.as_str()),
            ("SPACE_DB_BOOTSTRAP_SUPERADMIN_PASSWORD", password.as_str()),
            ("SPACE_DB_BOOTSTRAP_RO_PASSWORD", password.as_str()),
            ("SPACE_DB_BOOTSTRAP_MIGRATOR_PASSWORD", password.as_str()),
            ("SPACE_DB_BOOTSTRAP_CREDS_PASSWORD", password.as_str()),
        ];
        ensure!(
            Command::new(&args.migrator_bin)
                .args(["bootstrap-roles", "--database-url", database_url])
                .env_clear()
                .envs(role_env)
                .env("RUST_LOG", "warn")
                .stdout(Stdio::null())
                .status()
                .await?
                .success(),
            "role bootstrap failed"
        );
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        drop(listener);
        let secret = uuid::Uuid::now_v7().to_string();
        let env = BTreeMap::from([
            ("SPACE_CLUSTER_ID", "publish-bench".to_owned()),
            ("SPACE_NODE_NAME", "publish-bench".to_owned()),
            ("SPACE_NODE_ID", uuid::Uuid::now_v7().to_string()),
            ("SPACE_INTERNAL_TOKEN", secret.clone()),
            ("SPACE_ECW_CREDENTIAL_KEY", secret.clone()),
            (
                "SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID",
                "synthetic".to_owned(),
            ),
            ("SPACE_API_ROUTE_ED25519_ISSUER", "publish-bench".to_owned()),
            (
                "SPACE_API_ROUTE_ED25519_SIGNING_KEY_PATH",
                path(&signing)?.to_owned(),
            ),
            ("SPACE_INTERNAL_RPC_TLS_CERT", path(&cert)?.to_owned()),
            ("SPACE_INTERNAL_RPC_TLS_KEY", path(&key)?.to_owned()),
            ("DATABASE_URL", database_url.to_owned()),
            ("SPACE_DB_USER_PASSWORD", password),
            ("POSTGRES_POOL_MIN_CONNECTIONS", "50".to_owned()),
            ("POSTGRES_POOL_MAX_CONNECTIONS", "50".to_owned()),
            ("POSTGRES_POOL_MAX_LIFETIME_SECS", "86400".to_owned()),
            ("POSTGRES_POOL_IDLE_TIMEOUT_SECS", "86400".to_owned()),
            ("RUST_LOG", "warn,space_publish_timing=trace".to_owned()),
            ("SPACE_LOG_FORMAT", "json".to_owned()),
            ("NO_COLOR", "1".to_owned()),
        ]);
        let log = tokio::fs::File::create(args.output.join("service.log"))
            .await?
            .into_std()
            .await;
        let mut child = Command::new(&args.service_bin)
            .args(["--listen", &format!("127.0.0.1:{port}")])
            .env_clear()
            .envs(env)
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .kill_on_drop(true)
            .spawn()?;
        let endpoint = Endpoint::from_shared(format!("https://localhost:{port}"))?
            .connect_timeout(Duration::from_secs(1))
            .timeout(Duration::from_secs(30))
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(tokio::fs::read(cert).await?)),
            )?;
        let start = Instant::now();
        let channel = loop {
            ensure!(
                child.try_wait()?.is_none(),
                "service exited; inspect service.log"
            );
            match endpoint.connect().await {
                Ok(channel) => break channel,
                Err(error) if start.elapsed() > Duration::from_secs(30) => return Err(error.into()),
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        };
        Ok(Self {
            child,
            channel,
            token: format!("Bearer {secret}").parse()?,
            _secrets: secrets,
        })
    }

    pub fn client(&self) -> ClusterApiClient<Channel> {
        ClusterApiClient::new(self.channel.clone())
    }

    pub fn pid(&self) -> Result<u32> {
        self.child.id().context("service is not running")
    }

    pub async fn stop(&mut self) -> Result<()> {
        self.child.kill().await?;
        Ok(())
    }
}

fn path(path: &Path) -> Result<&str> {
    path.to_str().context("path is not UTF-8")
}

async fn openssl(args: &[&str]) -> Result<()> {
    ensure!(
        Command::new("openssl")
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await?
            .success(),
        "openssl failed"
    );
    Ok(())
}
