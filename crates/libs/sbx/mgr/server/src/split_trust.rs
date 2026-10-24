//! Opt-in sandbox trust through existing credential, filesystem and process RPCs.

#[cfg(feature = "split-trust-prototype")]
use pplx_space_common_core::types::SandboxId;
#[cfg(feature = "split-trust-prototype")]
use pplx_space_credmgr_client::{
    CredentialManagerGrpcClient, proto::exchange_token_response::Payload, x509::CredentialKind,
};
use tonic::Status;

/// Chosen from the existing launch intent, never inferred from guest files.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Preparation {
    Initialize,
    Preserve,
}

pub(crate) fn requested() -> bool {
    matches!(
        std::env::var("SPACE_SPLIT_TRUST_PROTOTYPE").as_deref(),
        Ok("1" | "true")
    )
}

pub(crate) fn enabled() -> bool {
    cfg!(feature = "split-trust-prototype") && requested()
}

#[cfg(feature = "split-trust-prototype")]
pub(crate) async fn public_anchor(
    client: &mut CredentialManagerGrpcClient,
    sandbox_id: SandboxId,
) -> Result<Vec<u8>, Status> {
    let mut response = client
        .exchange_token(CredentialKind::Certificate.request(sandbox_id))
        .await?
        .into_inner();
    if response.issued_token_type != CredentialKind::Certificate.as_str()
        || response.token_type != "N_A"
        || response.expires_in.is_some()
        || response.scope.is_some()
        || response.refresh_token.is_some()
    {
        return Err(Status::failed_precondition("invalid public X.509 profile"));
    }
    let Some(Payload::X509CertificateDer(certificate)) = response.payload.as_mut() else {
        return Err(Status::failed_precondition(
            "node did not return public-only certificate payload",
        ));
    };
    if certificate.is_empty() || certificate.len() > 4096 {
        return Err(Status::failed_precondition(
            "invalid anchor certificate size",
        ));
    }
    Ok(std::mem::take(certificate))
}

/// Read-only on restore/resume and gateway issuance. Missing trust is reported
/// to the caller, which fails closed when scoped trust is enabled.
#[cfg(feature = "split-trust-prototype")]
pub(crate) async fn installed_anchor(
    client: &pplx_space_spaced_host_client::SpacedHostClient,
    sandbox_id: SandboxId,
) -> Result<Option<Vec<u8>>, Status> {
    async fn read(
        client: &pplx_space_spaced_host_client::SpacedHostClient,
        path: &str,
        limit: u64,
    ) -> Result<Vec<u8>, Status> {
        let root = pplx_space_common_core::types::GuestUserName::parse("root")
            .map_err(|_| Status::internal("invalid trust reader user"))?;
        let mut stream = client
            .read_stream_as_user(path.into(), 0, Some(limit), Some(root))
            .await?;
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.message().await? {
            bytes.extend(chunk.data);
            if bytes.len() as u64 >= limit {
                return Err(Status::failed_precondition(
                    "guest trust file exceeds size limit",
                ));
            }
        }
        Ok(bytes)
    }
    let operation = async {
        let certificate = match read(
            client,
            "/usr/local/share/ca-certificates/space-sandbox-anchor.crt",
            8192,
        )
        .await
        {
            Ok(bytes) => bytes,
            Err(status) if status.code() == tonic::Code::NotFound => return Ok(None),
            Err(status) => return Err(status),
        };
        let marker = read(client, "/var/lib/space/sandbox-trust-id", 128).await?;
        if marker != sandbox_id.to_string().as_bytes() {
            return Err(Status::failed_precondition(
                "guest anchor belongs to another sandbox",
            ));
        }
        let certificate = pem::parse(certificate)
            .map_err(|_| Status::failed_precondition("invalid installed anchor PEM"))?;
        if certificate.tag() != "CERTIFICATE" {
            return Err(Status::failed_precondition(
                "guest trust file is not a certificate",
            ));
        }
        Ok(Some(certificate.into_contents()))
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), operation)
        .await
        .map_err(|_| Status::deadline_exceeded("guest trust read timed out"))?
}

pub(crate) async fn gateway_anchor(
    endpoint: &pplx_space_credmgr_client::CredmgrEndpoint,
    sandbox_id: &str,
    socket_path: String,
    activation_complete: bool,
) -> Result<Option<Vec<u8>>, Status> {
    #[cfg(feature = "split-trust-prototype")]
    if enabled() {
        let id = sandbox_id
            .parse::<SandboxId>()
            .map_err(|_| Status::failed_precondition("invalid sandbox identity"))?;
        let config = pplx_space_spaced_host_client::SpacedHostClientConfig::default()
            .with_socket_path(socket_path);
        let client = pplx_space_spaced_host_client::SpacedHostClient::new_with_config(config)
            .await
            .map_err(|_| Status::unavailable("guest trust reader is unavailable"))?;
        let installed = installed_anchor(&client, id).await.map_err(|status| {
            if !activation_complete && status.code() == tonic::Code::FailedPrecondition {
                Status::unavailable("sandbox trust preparation is in progress")
            } else {
                status
            }
        })?;
        if let Some(installed) = installed {
            let mut credentials = endpoint
                .connect()
                .await
                .map_err(|_| Status::unavailable("node credential agent is unavailable"))?;
            let expected = public_anchor(&mut credentials, id).await?;
            if installed != expected {
                return Err(Status::failed_precondition(
                    "installed anchor does not match sandbox identity",
                ));
            }
            return Ok(Some(expected));
        }
        // A new template/fork can be in Restoring while the activation
        // barrier installs its identity. Never issue a shared CA in that gap.
        return Err(Status::unavailable("sandbox scoped trust is not prepared"));
    }
    #[cfg(not(feature = "split-trust-prototype"))]
    let _ = (endpoint, sandbox_id, socket_path, activation_complete);
    Ok(None)
}

/// Bound the whole file/process operation to 40s. The guest process has its own
/// 30s deadline, so caller cancellation cannot leave an unbounded installer.
#[cfg(feature = "split-trust-prototype")]
pub(crate) async fn install(
    client: &pplx_space_spaced_host_client::SpacedHostClient,
    sandbox_id: SandboxId,
    certificate_der: Vec<u8>,
) -> Result<(), Status> {
    use pplx_space_common_core::types::{GuestUserName, ProcessCommand};
    use pplx_space_spaced_host_client::{
        StartProcessOptions,
        proto::{process_event::Event, process_exit::Outcome},
    };
    use std::time::Duration;
    use x509_parser::prelude::{FromDer, X509Certificate};

    let (remaining, certificate) = X509Certificate::from_der(&certificate_der)
        .map_err(|_| Status::failed_precondition("invalid anchor DER"))?;
    let constraints = certificate
        .basic_constraints()
        .map_err(|_| Status::failed_precondition("invalid anchor constraints"))?
        .ok_or_else(|| Status::failed_precondition("missing anchor constraints"))?;
    let usage = certificate
        .key_usage()
        .map_err(|_| Status::failed_precondition("invalid anchor usage"))?
        .ok_or_else(|| Status::failed_precondition("missing anchor usage"))?;
    if !remaining.is_empty()
        || certificate.subject() != certificate.issuer()
        || !certificate.validity().is_valid()
        || !constraints.critical
        || !constraints.value.ca
        || constraints.value.path_len_constraint != Some(1)
        || !usage.value.key_cert_sign()
        || certificate.verify_signature(None).is_err()
    {
        return Err(Status::failed_precondition(
            "certificate is not a valid sandbox anchor",
        ));
    }

    // Fixed platform-owned paths. Arguments are validated identifiers, never
    // interpolated shell fragments. The lock also fences abandoned callers;
    // the identity marker is committed only after the trust-store update.
    const INSTALL: &str = r#"set -eu
exec 9>/run/space-anchor-install.lock
flock -w 20 9
target=/usr/local/share/ca-certificates/space-sandbox-anchor.crt
marker=/var/lib/space/sandbox-trust-id
shared_roots=false
for root in /usr/local/share/ca-certificates/space-mitm-root-*.crt; do
  if [ -e "$root" ] || [ -L "$root" ]; then shared_roots=true; break; fi
done
if cmp -s "$1" "$target" && [ "$(cat "$marker" 2>/dev/null || true)" = "$2" ] && [ "$shared_roots" = false ] && [ ! -d /usr/local/share/ca-certificates/space ]; then
  rm -f "$1"
  exit 0
fi
mkdir -p /usr/local/share/ca-certificates /var/lib/space
install -m 0644 "$1" "${target}.tmp"
mv "${target}.tmp" "$target"
rm -rf /usr/local/share/ca-certificates/space
rm -f /usr/local/share/ca-certificates/space-mitm-root-*.crt
update-ca-certificates --fresh
printf '%s' "$2" > "${marker}.tmp"
mv "${marker}.tmp" "$marker"
rm -f "$1"
"#;
    let operation = async {
        let root =
            GuestUserName::parse("root").map_err(|_| Status::internal("invalid installer user"))?;
        let temporary = format!("/run/space-anchor-{sandbox_id}.crt");
        let pem = pem::encode(&pem::Pem::new("CERTIFICATE", certificate_der));
        client
            .write_with_idempotency(
                temporary.clone(),
                pem.into_bytes(),
                false,
                None,
                Some(root.clone()),
            )
            .await?;
        let command = ProcessCommand::new(vec![
            "/bin/sh".into(),
            "-c".into(),
            INSTALL.into(),
            "space-trust-install".into(),
            temporary,
            sandbox_id.to_string(),
        ])
        .map_err(|_| Status::internal("invalid trust installation command"))?;
        let mut options = StartProcessOptions::new(command);
        options.user = Some(root);
        options.clean_env = true;
        options.env =
            pplx_space_common_core::types::EnvMap::try_from(std::collections::HashMap::from([(
                "PATH".to_owned(),
                "/usr/sbin:/usr/bin:/sbin:/bin".to_owned(),
            )]))
            .map_err(|_| Status::internal("invalid trust installation environment"))?;
        options.timeout = Some(Duration::from_secs(30));
        options.tag = Some("space-trust-install".into());
        let mut events = client.start_process_stream_with_options(options).await?;
        let mut stderr = Vec::new();
        while let Some(event) = events.message().await? {
            match event.event {
                Some(Event::Stderr(output)) => {
                    let available = 8192_usize.saturating_sub(stderr.len());
                    stderr.extend(output.data.into_iter().take(available));
                }
                Some(Event::Exit(exit)) => {
                    if matches!(exit.outcome, Some(Outcome::Exited(ref result)) if result.exit_code == 0)
                    {
                        return Ok(());
                    }
                    tracing::warn!(?exit.outcome, stderr = %String::from_utf8_lossy(&stderr), "guest trust installation failed");
                    return Err(Status::failed_precondition(
                        "guest trust installation did not succeed",
                    ));
                }
                _ => {}
            }
        }
        Err(Status::unavailable(
            "trust installation stream ended without process exit",
        ))
    };
    tokio::time::timeout(Duration::from_secs(40), operation)
        .await
        .map_err(|_| Status::deadline_exceeded("guest trust installation timed out"))?
}
