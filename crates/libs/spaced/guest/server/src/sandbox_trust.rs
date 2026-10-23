//! Install only the public sandbox anchor before activation is published.
use pplx_space_spaced_common::proto::bridge_guest::InstallSandboxTrustRequest;
use tonic::Status;
use x509_parser::prelude::{FromDer, X509Certificate};

static INSTALL_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub(crate) async fn install(request: InstallSandboxTrustRequest) -> Result<(), Status> {
    if request.certificate_der.len() > 4096 {
        return Err(Status::invalid_argument(
            "sandbox anchor certificate is too large",
        ));
    }
    let id = uuid::Uuid::parse_str(&request.sandbox_id)
        .map_err(|_| Status::invalid_argument("invalid sandbox ID"))?;
    if id.get_version_num() != 7 {
        return Err(Status::invalid_argument("sandbox anchor requires UUIDv7"));
    }
    let (trailing, cert) = X509Certificate::from_der(&request.certificate_der)
        .map_err(|_| Status::invalid_argument("invalid sandbox anchor DER"))?;
    if !trailing.is_empty() || cert.subject() != cert.issuer() || !cert.validity().is_valid() {
        return Err(Status::invalid_argument(
            "sandbox anchor must be self-issued",
        ));
    }
    cert.verify_signature(None)
        .map_err(|_| Status::invalid_argument("invalid sandbox anchor signature"))?;
    let constraints = cert
        .basic_constraints()
        .map_err(|_| Status::invalid_argument("invalid CA extension"))?
        .ok_or_else(|| Status::invalid_argument("missing CA extension"))?;
    if !constraints.critical
        || !constraints.value.ca
        || constraints.value.path_len_constraint != Some(1)
    {
        return Err(Status::invalid_argument(
            "sandbox anchor must be a pathLen:1 CA",
        ));
    }
    let usage = cert
        .key_usage()
        .map_err(|_| Status::invalid_argument("invalid key usage"))?
        .ok_or_else(|| Status::invalid_argument("missing key usage"))?;
    if !usage.value.key_cert_sign() {
        return Err(Status::invalid_argument(
            "sandbox anchor cannot sign certificates",
        ));
    }
    let certificate_pem = pem::encode(&pem::Pem::new("CERTIFICATE", request.certificate_der));
    // A cancelled RPC must not leave the update subprocess orphaned while a
    // second install begins. The owned task retains the lock to completion.
    tokio::spawn(async move {
        let _lock = INSTALL_LOCK.lock().await;
        let directory = "/usr/local/share/ca-certificates";
        tokio::fs::create_dir_all(directory)
            .await
            .map_err(io_status)?;
        let destination = format!("{directory}/space-sandbox-anchor.crt");
        if tokio::fs::read_to_string(&destination)
            .await
            .ok()
            .as_deref()
            == Some(&certificate_pem)
            && tokio::fs::read_to_string("/var/lib/space/sandbox-trust-id")
                .await
                .ok()
                .as_deref()
                == Some(&id.to_string())
            && tokio::fs::read_to_string("/etc/ssl/certs/ca-certificates.crt")
                .await
                .ok()
                .is_some_and(|bundle| bundle.contains(&certificate_pem))
            && !tokio::fs::try_exists(format!("{directory}/space"))
                .await
                .map_err(io_status)?
        {
            return Ok(());
        }
        let temporary = format!("{directory}/.space-sandbox-anchor.tmp");
        tokio::fs::write(&temporary, certificate_pem)
            .await
            .map_err(io_status)?;
        tokio::fs::rename(&temporary, &destination)
            .await
            .map_err(io_status)?;
        // The seed builder owns this fixed shared SPACE trust directory.
        // Public OS roots and unrelated customer-installed roots are retained.
        let shared = format!("{directory}/space");
        if tokio::fs::try_exists(&shared).await.map_err(io_status)? {
            tokio::fs::remove_dir_all(&shared)
                .await
                .map_err(io_status)?;
        }
        let status = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            tokio::process::Command::new("/usr/sbin/update-ca-certificates")
                .arg("--fresh")
                .kill_on_drop(true)
                .output(),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("trust store update timed out"))?
        .map_err(io_status)?;
        if !status.status.success() {
            return Err(Status::internal("guest trust store update failed"));
        }
        tokio::fs::create_dir_all("/var/lib/space")
            .await
            .map_err(io_status)?;
        tokio::fs::write("/var/lib/space/.sandbox-trust-id.tmp", id.to_string())
            .await
            .map_err(io_status)?;
        tokio::fs::rename(
            "/var/lib/space/.sandbox-trust-id.tmp",
            "/var/lib/space/sandbox-trust-id",
        )
        .await
        .map_err(io_status)?;
        Ok(())
    })
    .await
    .map_err(|_| Status::internal("sandbox trust installation task failed"))?
}

fn io_status(error: std::io::Error) -> Status {
    Status::internal(format!("sandbox trust installation I/O: {error}"))
}
