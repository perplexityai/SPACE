use aws_config::{BehaviorVersion, retry::RetryConfig};
use aws_credential_types::Credentials;
use aws_sdk_kms::{
    Client,
    config::Region,
    error::SdkError,
    operation::generate_mac::{GenerateMacError, GenerateMacOutput},
    primitives::Blob,
    types::MacAlgorithmSpec,
};
use pplx_space_sandbox_anchor::EpochCatalog;
use std::{str::FromStr, time::Duration};
use zeroize::Zeroizing;

#[derive(Debug, thiserror::Error)]
pub enum HardwareError {
    #[error("hardware derivation configuration: {0}")]
    Configuration(&'static str),
    #[error("KMS GenerateMac failed: {0}")]
    Kms(#[source] Box<SdkError<GenerateMacError>>),
    #[error("KMS GenerateMac exceeded its one-second attempt budget")]
    Timeout,
    #[error("KMS returned the wrong key, algorithm, or output length")]
    InvalidResponse,
}

/// An immutable commercial-partition KMS key ARN, never an alias.
struct KeyArn {
    region: String,
    account: String,
    key_id: String,
}

impl FromStr for KeyArn {
    type Err = HardwareError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || HardwareError::Configuration("expected an immutable KMS key ARN");
        let rest = value.strip_prefix("arn:aws:kms:").ok_or_else(invalid)?;
        let fields: Vec<_> = rest.split(':').collect();
        let [region, account, resource] = fields.as_slice() else {
            return Err(invalid());
        };
        let key_id = resource.strip_prefix("key/").ok_or_else(invalid)?;
        let valid_id = if let Some(hex) = key_id.strip_prefix("mrk-") {
            hex.len() == 32
                && hex
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        } else {
            uuid::Uuid::parse_str(key_id).is_ok_and(|id| id.to_string() == key_id)
        };
        if !valid_id
            || account.len() != 12
            || !account.bytes().all(|b| b.is_ascii_digit())
            || region.is_empty()
            || !region
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(invalid());
        }
        Ok(Self {
            region: (*region).into(),
            account: (*account).into(),
            key_id: key_id.into(),
        })
    }
}

#[derive(Clone)]
pub(crate) struct HardwareDeriver {
    primary: Client,
    primary_region: String,
    replica: Option<(String, Client)>,
}

impl HardwareDeriver {
    pub async fn from_env() -> Result<Self, HardwareError> {
        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        // Kind can use a narrowly scoped HMAC identity independently of the
        // existing local storage STS bridge. Production uses workload identity.
        match (
            std::env::var("SPACE_ANCHOR_AWS_ACCESS_KEY_ID").ok(),
            std::env::var("SPACE_ANCHOR_AWS_SECRET_ACCESS_KEY").ok(),
            std::env::var("SPACE_ANCHOR_AWS_SESSION_TOKEN").ok(),
        ) {
            (Some(access), Some(secret), session) => {
                loader = loader.credentials_provider(Credentials::new(
                    access,
                    secret,
                    session,
                    None,
                    "sandbox-anchor-hardware",
                ));
            }
            (None, None, None) => {}
            _ => {
                return Err(HardwareError::Configuration(
                    "incomplete dedicated KMS credentials",
                ));
            }
        }
        let config = loader.load().await;
        let primary_region = config
            .region()
            .ok_or(HardwareError::Configuration("AWS region is required"))?
            .as_ref()
            .to_owned();
        let make_client = |region: String| {
            Client::from_conf(
                aws_sdk_kms::config::Builder::from(&config)
                    .region(Region::new(region))
                    // Retry at the explicit replica boundary, not nested SDK
                    // retry loops that multiply latency and request count.
                    .retry_config(RetryConfig::standard().with_max_attempts(1))
                    .build(),
            )
        };
        let replica = std::env::var("SPACE_ANCHOR_KMS_REPLICA_REGION")
            .ok()
            .filter(|region| !region.is_empty())
            .map(|region| (region.clone(), make_client(region)));
        if replica
            .as_ref()
            .is_some_and(|(region, _)| region == &primary_region)
        {
            return Err(HardwareError::Configuration(
                "replica must be a different region",
            ));
        }
        Ok(Self {
            primary: make_client(primary_region.clone()),
            primary_region,
            replica,
        })
    }

    pub fn validate_catalog(&self, catalog: &EpochCatalog) -> Result<(), HardwareError> {
        for epoch in catalog.epochs() {
            let key: KeyArn = epoch.key_id.parse()?;
            if key.region != self.primary_region
                || (self.replica.is_some() && !key.key_id.starts_with("mrk-"))
            {
                return Err(HardwareError::Configuration(
                    "catalog key region or multi-region identity does not match the provider",
                ));
            }
        }
        Ok(())
    }

    async fn attempt(
        client: &Client,
        key: &str,
        input: Vec<u8>,
    ) -> Result<Zeroizing<[u8; 48]>, HardwareError> {
        let response = tokio::time::timeout(
            Duration::from_secs(1),
            client
                .generate_mac()
                .key_id(key)
                .mac_algorithm(MacAlgorithmSpec::HmacSha384)
                .message(Blob::new(input))
                .send(),
        )
        .await
        .map_err(|_| HardwareError::Timeout)?
        .map_err(|error| HardwareError::Kms(Box::new(error)))?;
        Self::take_material(response, key)
    }

    fn take_material(
        mut response: GenerateMacOutput,
        key: &str,
    ) -> Result<Zeroizing<[u8; 48]>, HardwareError> {
        // Take ownership before checking metadata so even a rejected response
        // scrubs any returned derivation material.
        let bytes = Zeroizing::new(
            response
                .mac
                .take()
                .ok_or(HardwareError::InvalidResponse)?
                .into_inner(),
        );
        if response.key_id.as_deref() != Some(key)
            || response.mac_algorithm != Some(MacAlgorithmSpec::HmacSha384)
        {
            return Err(HardwareError::InvalidResponse);
        }
        let material =
            <[u8; 48]>::try_from(bytes.as_slice()).map_err(|_| HardwareError::InvalidResponse)?;
        Ok(Zeroizing::new(material))
    }

    pub async fn derive(
        &self,
        key: &str,
        input: Vec<u8>,
    ) -> Result<Zeroizing<[u8; 48]>, HardwareError> {
        match Self::attempt(&self.primary, key, input.clone()).await {
            Ok(material) => Ok(material),
            Err(primary_error) => {
                let Some((region, client)) = &self.replica else {
                    return Err(primary_error);
                };
                let key: KeyArn = key.parse()?;
                let replica = format!("arn:aws:kms:{region}:{}:key/{}", key.account, key.key_id);
                tracing::warn!(error = %primary_error, "trying configured anchor KMS replica");
                Self::attempt(client, &replica, input).await
            }
        }
    }
}

#[cfg(test)]
mod tests;
