//! Additional publication proof for direct deployments. The shared node bearer
//! still authenticates the transport; this secret grants only build-scoped
//! publication authority, never authority to issue system build grants.

use std::io::Read as _;
use std::path::PathBuf;

pub const TOKEN_PATH_ENV: &str = "SPACE_TEMPLATE_PUBLICATION_TOKEN_PATH";
pub(super) const HEADER: &str = "x-space-template-publication-token";

#[derive(Debug, thiserror::Error)]
pub enum DirectPublicationError {
    #[error("cannot read direct publication credential at {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("direct publication credential at {path} must contain 32–4096 visible ASCII bytes")]
    Invalid { path: PathBuf },
    #[error("invalid direct publication metadata: {0}")]
    Metadata(#[from] tonic::metadata::errors::InvalidMetadataValue),
    #[error("direct publication credential must differ from the shared node credential")]
    SharedCredential,
    #[error("direct publication credentials cannot be combined with Kubernetes authentication")]
    KubernetesAuthentication,
}

pub(super) fn token_from_env() -> Result<Option<String>, DirectPublicationError> {
    let Some(path) = std::env::var_os(TOKEN_PATH_ENV).map(PathBuf::from) else {
        return Ok(None);
    };
    let read = || -> std::io::Result<String> {
        let mut contents = String::new();
        std::fs::File::open(&path)?
            .take(4097)
            .read_to_string(&mut contents)?;
        Ok(contents)
    };
    let contents = read().map_err(|source| DirectPublicationError::Read {
        path: path.clone(),
        source,
    })?;
    let token = contents.trim();
    if contents.len() > 4096
        || !(32..=4096).contains(&token.len())
        || !token.bytes().all(|byte| (b'!'..=b'~').contains(&byte))
    {
        return Err(DirectPublicationError::Invalid { path });
    }
    Ok(Some(token.to_owned()))
}

/// Attach direct publication proof only to a publication RPC. An unset path
/// preserves projected-token behavior; a configured, broken file fails closed.
/// Read each time so a retry sees an atomically replaced sender credential.
pub fn publication_request<T>(message: T) -> Result<tonic::Request<T>, DirectPublicationError> {
    let mut request = tonic::Request::new(message);
    if let Some(token) = token_from_env()? {
        // The parser accepts only visible ASCII, a subset of valid metadata.
        let mut value = token.parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>()?;
        value.set_sensitive(true);
        request.metadata_mut().insert(HEADER, value);
    }
    Ok(request)
}
