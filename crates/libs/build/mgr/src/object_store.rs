use pplx_space_credmgr_client::{
    AWS_CREDENTIALS_TOKEN_TYPE, BUILD_INPUT_SHA256_TOKEN_TYPE, STORAGE_READ_SCOPE,
    STORAGE_SESSION_TTL_SECONDS, STORAGE_TOKEN_AUDIENCE, STORAGE_WRITE_SCOPE,
    SUBJECT_ID_TOKEN_TYPE, TOKEN_EXCHANGE_GRANT_TYPE,
    proto::{ExchangeTokenRequest, exchange_token_response},
};
use pplx_space_object_store::ObjectCredentials;

use pplx_space_common_core::types::TemplateName;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObjectStoreConfig {
    pub(crate) bucket: String,
    pub(crate) region: String,
    pub(crate) key_prefix: String,
    pub(crate) endpoint: Option<String>,
    /// Enabled for custom endpoints.
    pub(crate) path_style: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreConfigError {
    #[error("object-store config requires {0}")]
    MissingEnv(&'static str),
}

impl ObjectStoreConfig {
    pub(crate) fn from_env_optional() -> Result<Option<Self>, ObjectStoreConfigError> {
        Self::from_values(
            optional_env("SPACE_OBJECT_STORE_BUCKET"),
            optional_env("SPACE_OBJECT_STORE_REGION"),
            None,
            optional_env("SPACE_OBJECT_STORE_ENDPOINT"),
        )
    }

    fn from_values(
        bucket: Option<String>,
        region: Option<String>,
        key_prefix: Option<String>,
        endpoint: Option<String>,
    ) -> Result<Option<Self>, ObjectStoreConfigError> {
        let has_any_setting =
            bucket.is_some() || region.is_some() || key_prefix.is_some() || endpoint.is_some();
        if !has_any_setting {
            return Ok(None);
        }

        let bucket = bucket.ok_or(ObjectStoreConfigError::MissingEnv(
            "SPACE_OBJECT_STORE_BUCKET",
        ))?;
        let region = region.ok_or(ObjectStoreConfigError::MissingEnv(
            "SPACE_OBJECT_STORE_REGION",
        ))?;
        let key_prefix = key_prefix.unwrap_or_default();
        let path_style = endpoint.is_some();

        Ok(Some(Self {
            bucket,
            region,
            key_prefix,
            endpoint,
            path_style,
        }))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CredentialExchangeError {
    #[error("credmgr connect failed: {0}")]
    Connect(#[source] pplx_space_credmgr_client::ConnectError),
    #[error("credential exchange failed: {0}")]
    Exchange(#[source] tonic::Status),
    #[error("credmgr returned unexpected payload: {0}")]
    UnexpectedPayload(&'static str),
    #[error("static object-store credentials must set both access key and secret key")]
    IncompleteStaticCredentials,
}

impl CredentialExchangeError {
    /// True when Credential Manager refused a write because the template
    /// release is already published. Decoded from the machine-readable reason
    /// metadata; an unrelated `PERMISSION_DENIED` is not this.
    pub(crate) fn is_release_already_published(&self) -> bool {
        matches!(
            self,
            Self::Exchange(status)
                if pplx_space_credmgr_client::status_is_release_already_published(status)
        )
    }

    pub(crate) fn is_retriable(&self) -> bool {
        match self {
            Self::Connect(error) => match error {
                pplx_space_credmgr_client::ConnectError::Transport(_)
                | pplx_space_credmgr_client::ConnectError::Connect(_)
                | pplx_space_credmgr_client::ConnectError::TcpConnect(_) => true,
                pplx_space_credmgr_client::ConnectError::TlsRead { source, .. } => matches!(
                    source.kind(),
                    std::io::ErrorKind::Interrupted
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::WouldBlock
                ),
                pplx_space_credmgr_client::ConnectError::InvalidUri(_)
                | pplx_space_credmgr_client::ConnectError::Config(_) => false,
            },
            Self::Exchange(status) => matches!(
                status.code(),
                tonic::Code::Cancelled
                    | tonic::Code::Unknown
                    | tonic::Code::DeadlineExceeded
                    | tonic::Code::ResourceExhausted
                    | tonic::Code::Aborted
                    | tonic::Code::Internal
                    | tonic::Code::Unavailable
                    | tonic::Code::DataLoss
            ),
            Self::UnexpectedPayload(_) => false,
            Self::IncompleteStaticCredentials => false,
        }
    }
}

pub(crate) enum CredentialSubject<'a> {
    Template(&'a TemplateName),
    BuildInput(&'a str),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StorageAccess {
    Read,
    Write,
    ReadWrite,
}

impl StorageAccess {
    fn scope(self) -> String {
        match self {
            Self::Read => STORAGE_READ_SCOPE.to_string(),
            Self::Write => STORAGE_WRITE_SCOPE.to_string(),
            Self::ReadWrite => format!("{STORAGE_READ_SCOPE} {STORAGE_WRITE_SCOPE}"),
        }
    }
}

pub(crate) async fn object_store_credentials(
    bucket: &str,
    key: &str,
    is_prefix: bool,
    subject: CredentialSubject<'_>,
    access: StorageAccess,
) -> Result<ObjectCredentials, CredentialExchangeError> {
    match (
        optional_env("SPACE_OBJECT_STORE_STATIC_ACCESS_KEY_ID"),
        optional_env("SPACE_OBJECT_STORE_STATIC_SECRET_ACCESS_KEY"),
    ) {
        (Some(access_key_id), Some(secret_access_key)) => {
            return Ok(ObjectCredentials {
                access_key_id,
                secret_access_key,
                session_token: optional_env("SPACE_OBJECT_STORE_STATIC_SESSION_TOKEN"),
            });
        }
        (None, None) => {}
        _ => return Err(CredentialExchangeError::IncompleteStaticCredentials),
    }
    let resource = if is_prefix {
        format!("arn:aws:s3:::{bucket}/{key}/*")
    } else {
        format!("arn:aws:s3:::{bucket}/{key}")
    };
    let (subject_token, subject_token_type) = match subject {
        CredentialSubject::Template(template) => (
            format!("template:{template}"),
            SUBJECT_ID_TOKEN_TYPE.to_string(),
        ),
        CredentialSubject::BuildInput(digest) => (
            digest.to_string(),
            BUILD_INPUT_SHA256_TOKEN_TYPE.to_string(),
        ),
    };
    let mut client = pplx_space_credmgr_client::connect_from_env()
        .await
        .map_err(CredentialExchangeError::Connect)?;
    let mut resp = client
        .exchange_token(ExchangeTokenRequest {
            grant_type: TOKEN_EXCHANGE_GRANT_TYPE.to_string(),
            resource,
            audience: Some(STORAGE_TOKEN_AUDIENCE.to_string()),
            scope: Some(access.scope()),
            requested_token_type: AWS_CREDENTIALS_TOKEN_TYPE.to_string(),
            subject_token: Some(subject_token),
            subject_token_type: Some(subject_token_type),
            actor_token: None,
            actor_token_type: None,
            requested_expires_in: Some(STORAGE_SESSION_TTL_SECONDS),
        })
        .await
        .map_err(CredentialExchangeError::Exchange)?
        .into_inner();

    match resp.payload.as_mut() {
        Some(exchange_token_response::Payload::AwsCredentials(creds)) => Ok(ObjectCredentials {
            access_key_id: std::mem::take(&mut creds.access_key_id),
            secret_access_key: std::mem::take(&mut creds.secret_access_key),
            session_token: std::mem::take(&mut creds.session_token),
        }),
        Some(exchange_token_response::Payload::AccessToken(_)) => {
            Err(CredentialExchangeError::UnexpectedPayload("access_token"))
        }
        None => Err(CredentialExchangeError::UnexpectedPayload("empty payload")),
    }
}

fn optional_env(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => Some(value),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
