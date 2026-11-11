use oci_spec::distribution::Reference as OciReference;

use super::client::RegistryManifestSource;
use super::ecr_auth::EcrPullAuth;
use super::{RegistryError, RegistrySession};

const ECR_REPOSITORIES_ENV: &str = "SPACE_BUILDER_ECR_REPOSITORIES";

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct EcrRepositoryConfig {
    reference: String,
    region: String,
}

#[derive(Debug)]
pub(super) struct EcrRepository {
    pub(super) reference: OciReference,
    pub(super) region: aws_config::Region,
}

#[derive(Debug, thiserror::Error)]
pub enum EcrConfigurationError {
    #[error("{ECR_REPOSITORIES_ENV} is not valid Unicode")]
    NotUnicode,
    #[error("{ECR_REPOSITORIES_ENV} is not valid JSON")]
    InvalidJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("{ECR_REPOSITORIES_ENV} contains invalid OCI reference {reference:?}")]
    InvalidReference {
        reference: String,
        #[source]
        source: oci_spec::distribution::ParseError,
    },
    #[error("{ECR_REPOSITORIES_ENV} contains an empty region for {reference}")]
    EmptyRegion { reference: OciReference },
}

impl TryFrom<EcrRepositoryConfig> for EcrRepository {
    type Error = EcrConfigurationError;

    fn try_from(config: EcrRepositoryConfig) -> Result<Self, Self::Error> {
        let reference = config.reference.parse::<OciReference>().map_err(|source| {
            EcrConfigurationError::InvalidReference {
                reference: config.reference.clone(),
                source,
            }
        })?;
        if config.region.is_empty() {
            return Err(EcrConfigurationError::EmptyRegion { reference });
        }
        Ok(Self {
            reference,
            region: aws_config::Region::new(config.region),
        })
    }
}

pub(super) fn repositories_from_json(
    raw: &str,
) -> Result<Vec<EcrRepository>, EcrConfigurationError> {
    serde_json::from_str::<Vec<EcrRepositoryConfig>>(raw)
        .map_err(|source| EcrConfigurationError::InvalidJson { source })?
        .into_iter()
        .map(EcrRepository::try_from)
        .collect()
}

fn repositories_from_env() -> Result<Vec<EcrRepository>, EcrConfigurationError> {
    let raw = match std::env::var(ECR_REPOSITORIES_ENV) {
        Ok(raw) => raw,
        Err(std::env::VarError::NotPresent) => return Ok(Vec::new()),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(EcrConfigurationError::NotUnicode);
        }
    };
    repositories_from_json(&raw)
}

pub(super) fn validate_configuration() -> Result<(), EcrConfigurationError> {
    repositories_from_env().map(drop)
}

pub(super) fn repository_for<'a>(
    reference: &OciReference,
    repositories: &'a [EcrRepository],
) -> Option<&'a EcrRepository> {
    repositories.iter().find(|repository| {
        repository.reference.registry() == reference.registry()
            && repository.reference.repository() == reference.repository()
    })
}

pub(super) async fn session_for(
    reference: &OciReference,
) -> Result<RegistrySession, RegistryError> {
    let repositories = repositories_from_env()?;
    let repository =
        repository_for(reference, &repositories).ok_or(RegistryError::EcrRepositoryNotAllowed)?;

    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(repository.region.clone())
        .load()
        .await;
    let response = aws_sdk_ecr::Client::new(&config)
        .get_authorization_token()
        .send()
        .await
        .map_err(|source| RegistryError::EcrAuthenticationRequest {
            source: Box::new(source),
        })?;
    let authorization_token = response
        .authorization_data()
        .first()
        .and_then(|authorization| authorization.authorization_token())
        .filter(|token| !token.is_empty())
        .ok_or(RegistryError::EmptyEcrAuthenticationToken)?
        .to_string();
    let source = RegistryManifestSource::ecr(reference.registry(), authorization_token.clone())?;
    let ecr_pull_auth = EcrPullAuth::new(reference.registry().to_string(), authorization_token);
    Ok(RegistrySession::new(source, Some(ecr_pull_auth)))
}
