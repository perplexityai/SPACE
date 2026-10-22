//! Asymmetrically signed, incarnation-bound SPACE API route capabilities.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

use aws_lc_rs::signature::{ED25519, Ed25519KeyPair, KeyPair, VerificationAlgorithm};
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use pplx_space_common_core::types::{ClusterId, GuestPort, SandboxRouteIdentity};
use serde::{Deserialize, Serialize};
use tonic::metadata::{Ascii, MetadataMap, MetadataValue};

use crate::capability::{SandboxRouteCapability, SpaceApiRouteProtocol, SpaceApiRoutePurpose};

pub use crate::capability::SPACE_API_ROUTE_ED25519_TOKEN_METADATA_KEY;
pub const SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE_ENV: &str =
    "SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE";
pub const SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE_PATH_ENV: &str =
    "SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE_PATH";
pub const SPACE_API_ROUTE_ED25519_SIGNING_KEY_ENV: &str = "SPACE_API_ROUTE_ED25519_SIGNING_KEY";
pub const SPACE_API_ROUTE_ED25519_SIGNING_KEY_PATH_ENV: &str =
    "SPACE_API_ROUTE_ED25519_SIGNING_KEY_PATH";
/// Path of a fixed Ed25519 PKCS#8 signing key file for tests, written once per
/// process. Tests that need an
/// authority point `SPACE_API_ROUTE_ED25519_SIGNING_KEY_PATH` at this file.
#[cfg(any(test, feature = "test-support"))]
pub fn test_signing_key_path() -> &'static std::path::Path {
    static PATH: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    PATH.get_or_init(|| {
        let path = std::env::temp_dir().join(format!(
            "space-route-ed25519-test-key-{}.pk8",
            std::process::id()
        ));
        std::fs::write(
            &path,
            "MC4CAQAwBQYDK2VwBCIEIJ1hsZ3v_VpguoRK9JLsLMREScVpezJpGXA7rAMcrn9g",
        )
        .expect("write test signing key");
        path
    })
}
pub const SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV: &str =
    "SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID";
pub const SPACE_API_ROUTE_ED25519_ISSUER_ENV: &str = "SPACE_API_ROUTE_ED25519_ISSUER";
pub const SPACE_API_ROUTE_ED25519_AUDIENCE: &str = "space-sandbox-route";
pub const SPACE_API_ROUTE_ED25519_CLAIMS_SCHEMA_VERSION: u8 = 2;
pub const DEFAULT_SPACE_API_ROUTE_ED25519_TTL_SECS: u64 = 120;
pub const DEFAULT_SPACE_API_ROUTE_ED25519_CLOCK_SKEW_SECS: u64 = 15;
pub const DEFAULT_SPACE_API_ROUTE_ED25519_MAX_LIFETIME_SECS: u64 = 135;

const TOKEN_PREFIX: &str = "ed25519";
const MAX_KEY_ID_BYTES: usize = 64;
const MAX_ISSUER_BYTES: usize = 128;
const MAX_VERIFICATION_KEYS: usize = 3;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SpaceApiRouteEd25519Issuer(String);

impl SpaceApiRouteEd25519Issuer {
    pub fn parse(value: impl Into<String>) -> Result<Self, SpaceApiRouteEd25519Error> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= MAX_ISSUER_BYTES
            && value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
            });
        valid
            .then_some(Self(value))
            .ok_or(SpaceApiRouteEd25519Error::InvalidIssuer)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SpaceApiRouteEd25519Issuer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for SpaceApiRouteEd25519Issuer {
    type Err = SpaceApiRouteEd25519Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for SpaceApiRouteEd25519Issuer {
    type Error = SpaceApiRouteEd25519Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<SpaceApiRouteEd25519Issuer> for String {
    fn from(value: SpaceApiRouteEd25519Issuer) -> Self {
        value.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct SpaceApiRouteEd25519VerificationGeneration(u64);

impl SpaceApiRouteEd25519VerificationGeneration {
    pub fn new(value: u64) -> Result<Self, SpaceApiRouteEd25519Error> {
        (value > 0)
            .then_some(Self(value))
            .ok_or(SpaceApiRouteEd25519Error::InvalidKeyGeneration)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for SpaceApiRouteEd25519VerificationGeneration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl TryFrom<u64> for SpaceApiRouteEd25519VerificationGeneration {
    type Error = SpaceApiRouteEd25519Error;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<SpaceApiRouteEd25519VerificationGeneration> for u64 {
    fn from(value: SpaceApiRouteEd25519VerificationGeneration) -> Self {
        value.get()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SpaceApiRouteEd25519KeyId(String);

impl SpaceApiRouteEd25519KeyId {
    pub fn parse(value: impl Into<String>) -> Result<Self, SpaceApiRouteEd25519Error> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= MAX_KEY_ID_BYTES
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'));
        if !valid {
            return Err(SpaceApiRouteEd25519Error::InvalidKeyId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SpaceApiRouteEd25519KeyId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for SpaceApiRouteEd25519KeyId {
    type Err = SpaceApiRouteEd25519Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for SpaceApiRouteEd25519KeyId {
    type Error = SpaceApiRouteEd25519Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<SpaceApiRouteEd25519KeyId> for String {
    fn from(value: SpaceApiRouteEd25519KeyId) -> Self {
        value.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SpaceApiRouteEd25519Claims {
    #[serde(rename = "version")]
    pub schema_version: u8,
    pub issuer: SpaceApiRouteEd25519Issuer,
    pub audience: String,
    pub cluster_id: ClusterId,
    pub purpose: SpaceApiRoutePurpose,
    pub route_identity: SandboxRouteIdentity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_port: Option<GuestPort>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<SpaceApiRouteProtocol>,
    pub issued_at_unix_secs: u64,
    pub not_before_unix_secs: u64,
    pub expires_at_unix_secs: u64,
}

impl SpaceApiRouteEd25519Claims {
    pub fn space_api_bridge(
        issuer: SpaceApiRouteEd25519Issuer,
        cluster_id: ClusterId,
        route_identity: SandboxRouteIdentity,
    ) -> Result<Self, SpaceApiRouteEd25519Error> {
        Self::new(
            issuer,
            cluster_id,
            SpaceApiRoutePurpose::SpaceApiBridge,
            route_identity,
            None,
            None,
        )
    }

    pub fn service_port_ingress(
        issuer: SpaceApiRouteEd25519Issuer,
        cluster_id: ClusterId,
        route_identity: SandboxRouteIdentity,
        service_port: GuestPort,
        protocol: SpaceApiRouteProtocol,
    ) -> Result<Self, SpaceApiRouteEd25519Error> {
        Self::new(
            issuer,
            cluster_id,
            SpaceApiRoutePurpose::ServicePortIngress,
            route_identity,
            Some(service_port),
            Some(protocol),
        )
    }

    fn new(
        issuer: SpaceApiRouteEd25519Issuer,
        cluster_id: ClusterId,
        purpose: SpaceApiRoutePurpose,
        route_identity: SandboxRouteIdentity,
        service_port: Option<GuestPort>,
        protocol: Option<SpaceApiRouteProtocol>,
    ) -> Result<Self, SpaceApiRouteEd25519Error> {
        let issued_at_unix_secs = unix_now_secs();
        Ok(Self {
            schema_version: SPACE_API_ROUTE_ED25519_CLAIMS_SCHEMA_VERSION,
            issuer,
            audience: SPACE_API_ROUTE_ED25519_AUDIENCE.to_string(),
            cluster_id,
            purpose,
            route_identity,
            service_port,
            protocol,
            issued_at_unix_secs,
            not_before_unix_secs: issued_at_unix_secs,
            expires_at_unix_secs: issued_at_unix_secs + DEFAULT_SPACE_API_ROUTE_ED25519_TTL_SECS,
        })
    }

    fn validate_profile(&self) -> Result<(), SpaceApiRouteEd25519Error> {
        if self.schema_version != SPACE_API_ROUTE_ED25519_CLAIMS_SCHEMA_VERSION {
            return Err(SpaceApiRouteEd25519Error::UnsupportedClaimsSchemaVersion);
        }
        match self.purpose {
            SpaceApiRoutePurpose::SpaceApiBridge
                if self.service_port.is_none() && self.protocol.is_none() =>
            {
                Ok(())
            }
            SpaceApiRoutePurpose::ServicePortIngress
                if self.service_port.is_some() && self.protocol.is_some() =>
            {
                Ok(())
            }
            _ => Err(SpaceApiRouteEd25519Error::InvalidClaimProfile),
        }
    }

    pub fn capability(&self) -> Result<SandboxRouteCapability, SpaceApiRouteEd25519Error> {
        match (self.purpose, self.service_port, self.protocol) {
            (SpaceApiRoutePurpose::SpaceApiBridge, None, None) => Ok(
                SandboxRouteCapability::space_api_bridge(self.route_identity.clone()),
            ),
            (SpaceApiRoutePurpose::ServicePortIngress, Some(service_port), Some(protocol)) => {
                Ok(SandboxRouteCapability::service_port_ingress(
                    self.route_identity.clone(),
                    service_port,
                    protocol,
                ))
            }
            _ => Err(SpaceApiRouteEd25519Error::InvalidClaimProfile),
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct SpaceApiRouteEd25519Token(String);

impl SpaceApiRouteEd25519Token {
    pub fn parse(value: impl Into<String>) -> Result<Self, SpaceApiRouteEd25519Error> {
        let value = value.into();
        Self::from_str(&value)
    }

    pub fn secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SpaceApiRouteEd25519Token {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SpaceApiRouteEd25519Token(<redacted>)")
    }
}

impl FromStr for SpaceApiRouteEd25519Token {
    type Err = SpaceApiRouteEd25519Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if value.is_empty() {
            return Err(SpaceApiRouteEd25519Error::Missing);
        }
        parse_token(value)?;
        Ok(Self(value.to_string()))
    }
}

pub struct SpaceApiRouteEd25519Signer {
    key_id: SpaceApiRouteEd25519KeyId,
    key_pair: Ed25519KeyPair,
}

pub struct SpaceApiRouteEd25519Authority {
    issuer: SpaceApiRouteEd25519Issuer,
    cluster_id: ClusterId,
    signer: SpaceApiRouteEd25519Signer,
}

impl SpaceApiRouteEd25519Authority {
    pub fn load_from_env() -> Result<Option<Self>, SpaceApiRouteEd25519Error> {
        Self::load_from_environment_impl(&pplx_space_common_core::env::ProcessEnvironment)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn load_from_environment(
        env: &dyn pplx_space_common_core::env::Environment,
    ) -> Result<Option<Self>, SpaceApiRouteEd25519Error> {
        Self::load_from_environment_impl(env)
    }

    fn load_from_environment_impl(
        env: &dyn pplx_space_common_core::env::Environment,
    ) -> Result<Option<Self>, SpaceApiRouteEd25519Error> {
        let key_path = env.var_os(SPACE_API_ROUTE_ED25519_SIGNING_KEY_PATH_ENV);
        let encoded_key = env.var(SPACE_API_ROUTE_ED25519_SIGNING_KEY_ENV).ok();
        let key_id = env.var(SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV).ok();
        let issuer = env.var(SPACE_API_ROUTE_ED25519_ISSUER_ENV).ok();
        if key_path.is_none() && encoded_key.is_none() && key_id.is_none() && issuer.is_none() {
            return Ok(None);
        }
        let key_id = SpaceApiRouteEd25519KeyId::parse(
            key_id.ok_or(SpaceApiRouteEd25519Error::IncompleteSigningConfiguration)?,
        )?;
        let issuer = SpaceApiRouteEd25519Issuer::parse(
            issuer.ok_or(SpaceApiRouteEd25519Error::IncompleteSigningConfiguration)?,
        )?;
        let pkcs8 = match (key_path, encoded_key) {
            (Some(_), Some(_)) | (None, None) => {
                return Err(SpaceApiRouteEd25519Error::IncompleteSigningConfiguration);
            }
            (Some(path), None) => {
                let path = PathBuf::from(path);
                let raw = std::fs::read(&path)
                    .map_err(|source| SpaceApiRouteEd25519Error::ReadSigningKey { path, source })?;
                decode_signing_key_file(raw)?
            }
            (None, Some(encoded)) => decode_signing_key_file(encoded.into_bytes())?,
        };
        let cluster_id =
            ClusterId::from_environment(env)?.ok_or(SpaceApiRouteEd25519Error::MissingClusterId)?;
        Ok(Some(Self {
            issuer,
            cluster_id,
            signer: SpaceApiRouteEd25519Signer::from_pkcs8(key_id, &pkcs8)?,
        }))
    }

    pub fn load_required_from_env() -> Result<Self, SpaceApiRouteEd25519Error> {
        Self::load_required_from_environment_impl(&pplx_space_common_core::env::ProcessEnvironment)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn load_required_from_environment(
        env: &dyn pplx_space_common_core::env::Environment,
    ) -> Result<Self, SpaceApiRouteEd25519Error> {
        Self::load_required_from_environment_impl(env)
    }

    fn load_required_from_environment_impl(
        env: &dyn pplx_space_common_core::env::Environment,
    ) -> Result<Self, SpaceApiRouteEd25519Error> {
        Self::load_from_environment_impl(env)?
            .ok_or(SpaceApiRouteEd25519Error::MissingSigningConfiguration)
    }

    pub fn mint_space_api_bridge(
        &self,
        route_identity: SandboxRouteIdentity,
    ) -> Result<SpaceApiRouteEd25519Token, SpaceApiRouteEd25519Error> {
        let claims = SpaceApiRouteEd25519Claims::space_api_bridge(
            self.issuer.clone(),
            self.cluster_id.clone(),
            route_identity,
        )?;
        self.signer.mint(&claims)
    }

    pub fn mint_service_port_ingress(
        &self,
        route_identity: SandboxRouteIdentity,
        service_port: GuestPort,
        protocol: SpaceApiRouteProtocol,
    ) -> Result<SpaceApiRouteEd25519Token, SpaceApiRouteEd25519Error> {
        let claims = SpaceApiRouteEd25519Claims::service_port_ingress(
            self.issuer.clone(),
            self.cluster_id.clone(),
            route_identity,
            service_port,
            protocol,
        )?;
        self.signer.mint(&claims)
    }
}

impl SpaceApiRouteEd25519Signer {
    pub fn from_pkcs8(
        key_id: SpaceApiRouteEd25519KeyId,
        pkcs8: &[u8],
    ) -> Result<Self, SpaceApiRouteEd25519Error> {
        let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8)
            .map_err(|_| SpaceApiRouteEd25519Error::InvalidPrivateKey)?;
        Ok(Self { key_id, key_pair })
    }

    pub fn public_key(&self) -> SpaceApiRouteEd25519VerificationKey {
        SpaceApiRouteEd25519VerificationKey {
            key_id: self.key_id.clone(),
            public_key: self.key_pair.public_key().as_ref().to_vec(),
        }
    }

    pub fn mint(
        &self,
        claims: &SpaceApiRouteEd25519Claims,
    ) -> Result<SpaceApiRouteEd25519Token, SpaceApiRouteEd25519Error> {
        claims.validate_profile()?;
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims)?);
        let signing_input = format!("{TOKEN_PREFIX}.{}.{payload}", self.key_id);
        let signature = self.key_pair.sign(signing_input.as_bytes());
        Ok(SpaceApiRouteEd25519Token(format!(
            "{signing_input}.{}",
            URL_SAFE_NO_PAD.encode(signature.as_ref())
        )))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpaceApiRouteEd25519VerificationKey {
    pub key_id: SpaceApiRouteEd25519KeyId,
    pub public_key: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpaceApiRouteEd25519VerificationBundle {
    pub generation: SpaceApiRouteEd25519VerificationGeneration,
    pub issuer: SpaceApiRouteEd25519Issuer,
    pub cluster_id: ClusterId,
    pub keys: Vec<SpaceApiRouteEd25519VerificationKey>,
    verifier: SpaceApiRouteEd25519Verifier,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpaceApiRouteEd25519VerificationBundleUpdate {
    Updated,
    Unchanged,
}

#[derive(Debug, thiserror::Error)]
pub enum SpaceApiRouteEd25519VerificationBundleUpdateError {
    #[error(
        "route verification bundle generation moved backwards: current={current}, incoming={incoming}"
    )]
    StaleGeneration {
        current: SpaceApiRouteEd25519VerificationGeneration,
        incoming: SpaceApiRouteEd25519VerificationGeneration,
    },
    #[error("route verification bundle changed without increasing generation {generation}")]
    GenerationConflict {
        generation: SpaceApiRouteEd25519VerificationGeneration,
    },
}

impl SpaceApiRouteEd25519VerificationBundle {
    pub fn new(
        generation: SpaceApiRouteEd25519VerificationGeneration,
        issuer: SpaceApiRouteEd25519Issuer,
        cluster_id: ClusterId,
        mut keys: Vec<SpaceApiRouteEd25519VerificationKey>,
    ) -> Result<Self, SpaceApiRouteEd25519Error> {
        keys.sort_by(|left, right| left.key_id.cmp(&right.key_id));
        let verifier =
            SpaceApiRouteEd25519Verifier::new(issuer.clone(), cluster_id.clone(), keys.clone())?;
        Ok(Self {
            generation,
            issuer,
            cluster_id,
            keys,
            verifier,
        })
    }

    pub fn verifier(&self) -> &SpaceApiRouteEd25519Verifier {
        &self.verifier
    }

    pub fn into_verifier(self) -> SpaceApiRouteEd25519Verifier {
        self.verifier
    }

    pub fn classify_update(
        &self,
        incoming: &Self,
    ) -> Result<
        SpaceApiRouteEd25519VerificationBundleUpdate,
        SpaceApiRouteEd25519VerificationBundleUpdateError,
    > {
        if incoming.generation < self.generation {
            return Err(
                SpaceApiRouteEd25519VerificationBundleUpdateError::StaleGeneration {
                    current: self.generation,
                    incoming: incoming.generation,
                },
            );
        }
        if incoming.generation == self.generation {
            if self.issuer == incoming.issuer
                && self.cluster_id == incoming.cluster_id
                && self.keys == incoming.keys
            {
                return Ok(SpaceApiRouteEd25519VerificationBundleUpdate::Unchanged);
            }
            return Err(
                SpaceApiRouteEd25519VerificationBundleUpdateError::GenerationConflict {
                    generation: self.generation,
                },
            );
        }
        Ok(SpaceApiRouteEd25519VerificationBundleUpdate::Updated)
    }
}

#[derive(Deserialize)]
struct SerializedRouteVerificationBundle {
    generation: u64,
    issuer: String,
    keys: Vec<SerializedRouteVerificationKey>,
}

#[derive(Deserialize)]
struct SerializedRouteVerificationKey {
    key_id: String,
    #[serde(default)]
    public_key_base64url: Option<String>,
    #[serde(default)]
    public_key_pem: Option<String>,
}

pub fn load_space_api_route_ed25519_verification_bundle()
-> Result<Option<SpaceApiRouteEd25519VerificationBundle>, SpaceApiRouteEd25519Error> {
    let raw = if let Some(path) =
        std::env::var_os(SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE_PATH_ENV)
    {
        let path = PathBuf::from(path);
        Some(
            std::fs::read_to_string(&path).map_err(|source| {
                SpaceApiRouteEd25519Error::ReadVerificationBundle { path, source }
            })?,
        )
    } else {
        std::env::var(SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE_ENV).ok()
    };
    let Some(raw) = raw.filter(|raw| !raw.trim().is_empty()) else {
        return Ok(None);
    };
    let serialized = serde_json::from_str::<SerializedRouteVerificationBundle>(&raw)?;
    let generation = SpaceApiRouteEd25519VerificationGeneration::new(serialized.generation)?;
    let issuer = SpaceApiRouteEd25519Issuer::parse(serialized.issuer)?;
    let cluster_id = ClusterId::from_env()?.ok_or(SpaceApiRouteEd25519Error::MissingClusterId)?;
    let keys = serialized
        .keys
        .into_iter()
        .map(|key| {
            Ok(SpaceApiRouteEd25519VerificationKey {
                key_id: SpaceApiRouteEd25519KeyId::parse(key.key_id)?,
                public_key: decode_public_verification_key(
                    key.public_key_base64url,
                    key.public_key_pem,
                )?,
            })
        })
        .collect::<Result<Vec<_>, SpaceApiRouteEd25519Error>>()?;
    Ok(Some(SpaceApiRouteEd25519VerificationBundle::new(
        generation, issuer, cluster_id, keys,
    )?))
}

pub fn load_required_space_api_route_ed25519_verification_bundle()
-> Result<SpaceApiRouteEd25519VerificationBundle, SpaceApiRouteEd25519Error> {
    load_space_api_route_ed25519_verification_bundle()?
        .ok_or(SpaceApiRouteEd25519Error::MissingVerificationKeys)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpaceApiRouteEd25519Verifier {
    expected_issuer: SpaceApiRouteEd25519Issuer,
    expected_cluster_id: ClusterId,
    keys: BTreeMap<SpaceApiRouteEd25519KeyId, Vec<u8>>,
    clock_skew_secs: u64,
    maximum_lifetime_secs: u64,
}

impl SpaceApiRouteEd25519Verifier {
    pub fn new(
        expected_issuer: SpaceApiRouteEd25519Issuer,
        expected_cluster_id: ClusterId,
        keys: impl IntoIterator<Item = SpaceApiRouteEd25519VerificationKey>,
    ) -> Result<Self, SpaceApiRouteEd25519Error> {
        let mut verified_keys = BTreeMap::new();
        for key in keys {
            if key.public_key.len() != 32 {
                return Err(SpaceApiRouteEd25519Error::InvalidPublicKey);
            }
            if verified_keys.insert(key.key_id, key.public_key).is_some() {
                return Err(SpaceApiRouteEd25519Error::DuplicateKeyId);
            }
        }
        if verified_keys.is_empty() || verified_keys.len() > MAX_VERIFICATION_KEYS {
            return Err(SpaceApiRouteEd25519Error::InvalidVerificationKeyCount);
        }
        Ok(Self {
            expected_issuer,
            expected_cluster_id,
            keys: verified_keys,
            clock_skew_secs: DEFAULT_SPACE_API_ROUTE_ED25519_CLOCK_SKEW_SECS,
            maximum_lifetime_secs: DEFAULT_SPACE_API_ROUTE_ED25519_MAX_LIFETIME_SECS,
        })
    }

    pub fn verify(
        &self,
        token: &SpaceApiRouteEd25519Token,
    ) -> Result<SpaceApiRouteEd25519Claims, SpaceApiRouteEd25519Error> {
        let parsed = parse_token(token.secret())?;
        let public_key = self
            .keys
            .get(&parsed.key_id)
            .ok_or(SpaceApiRouteEd25519Error::UnknownKeyId)?;
        ED25519
            .verify_sig(
                public_key,
                parsed.signing_input.as_bytes(),
                &parsed.signature,
            )
            .map_err(|_| SpaceApiRouteEd25519Error::InvalidSignature)?;
        let claims = serde_json::from_slice::<SpaceApiRouteEd25519Claims>(&parsed.payload)?;
        claims.validate_profile()?;
        if claims.issuer != self.expected_issuer {
            return Err(SpaceApiRouteEd25519Error::WrongIssuer);
        }
        if claims.audience != SPACE_API_ROUTE_ED25519_AUDIENCE {
            return Err(SpaceApiRouteEd25519Error::WrongAudience);
        }
        if claims.cluster_id != self.expected_cluster_id {
            return Err(SpaceApiRouteEd25519Error::WrongCluster);
        }
        let now = unix_now_secs();
        let skew = self.clock_skew_secs;
        if claims.not_before_unix_secs > now.saturating_add(skew) {
            return Err(SpaceApiRouteEd25519Error::NotYetValid);
        }
        if claims.expires_at_unix_secs.saturating_add(skew) <= now {
            return Err(SpaceApiRouteEd25519Error::Expired);
        }
        let lifetime = claims
            .expires_at_unix_secs
            .checked_sub(claims.issued_at_unix_secs)
            .ok_or(SpaceApiRouteEd25519Error::InvalidLifetime)?;
        if lifetime == 0 || lifetime > self.maximum_lifetime_secs {
            return Err(SpaceApiRouteEd25519Error::InvalidLifetime);
        }
        if claims.issued_at_unix_secs > now.saturating_add(skew)
            || claims.not_before_unix_secs < claims.issued_at_unix_secs
            || claims.not_before_unix_secs >= claims.expires_at_unix_secs
        {
            return Err(SpaceApiRouteEd25519Error::InvalidLifetime);
        }
        Ok(claims)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SpaceApiRouteEd25519Error {
    #[error("SPACE API Ed25519 route token is missing")]
    Missing,
    #[error("SPACE API Ed25519 route token is malformed")]
    Malformed,
    #[error("SPACE API Ed25519 route token key id is invalid")]
    InvalidKeyId,
    #[error("SPACE API Ed25519 route token issuer is invalid")]
    InvalidIssuer,
    #[error("SPACE API Ed25519 route token private key is invalid")]
    InvalidPrivateKey,
    #[error("SPACE API Ed25519 route token signing configuration is incomplete")]
    IncompleteSigningConfiguration,
    #[error("SPACE API Ed25519 route token signing configuration is missing")]
    MissingSigningConfiguration,
    #[error("SPACE API Ed25519 route token public key is invalid")]
    InvalidPublicKey,
    #[error("SPACE API Ed25519 route token verification key set is missing")]
    MissingVerificationKeys,
    #[error("SPACE API Ed25519 route token verification key count must be between one and three")]
    InvalidVerificationKeyCount,
    #[error("SPACE API Ed25519 route token verification key id is duplicated")]
    DuplicateKeyId,
    #[error("SPACE API Ed25519 route token verification key generation is invalid")]
    InvalidKeyGeneration,
    #[error("SPACE cluster id is required for Ed25519 route tokens")]
    MissingClusterId,
    #[error("SPACE cluster id is invalid: {0}")]
    InvalidClusterId(#[from] pplx_space_common_core::types::ClusterIdParseError),
    #[error("failed to read SPACE API route verification bundle {path}: {source}")]
    ReadVerificationBundle {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read SPACE API route signing key {path}: {source}")]
    ReadSigningKey {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("SPACE API Ed25519 route token key id is unknown")]
    UnknownKeyId,
    #[error("SPACE API Ed25519 route token signature is invalid")]
    InvalidSignature,
    #[error("SPACE API Ed25519 route token payload is invalid: {0}")]
    InvalidPayload(#[from] serde_json::Error),
    #[error("SPACE API Ed25519 route token claims schema version is unsupported")]
    UnsupportedClaimsSchemaVersion,
    #[error("SPACE API Ed25519 route token claim profile is invalid")]
    InvalidClaimProfile,
    #[error("SPACE API Ed25519 route token issuer does not match")]
    WrongIssuer,
    #[error("SPACE API Ed25519 route token audience does not match")]
    WrongAudience,
    #[error("SPACE API Ed25519 route token cluster does not match")]
    WrongCluster,
    #[error("SPACE API Ed25519 route token is not yet valid")]
    NotYetValid,
    #[error("SPACE API Ed25519 route token is expired")]
    Expired,
    #[error("SPACE API Ed25519 route token lifetime is invalid")]
    InvalidLifetime,
    #[error("SPACE API Ed25519 route token is not valid gRPC metadata: {0}")]
    InvalidMetadata(#[from] tonic::metadata::errors::InvalidMetadataValue),
}

pub fn insert_space_api_route_ed25519_token(
    metadata: &mut MetadataMap,
    token: &SpaceApiRouteEd25519Token,
) -> Result<(), SpaceApiRouteEd25519Error> {
    metadata.insert(
        SPACE_API_ROUTE_ED25519_TOKEN_METADATA_KEY,
        MetadataValue::<Ascii>::try_from(token.secret())?,
    );
    Ok(())
}

pub fn space_api_route_ed25519_token_from_metadata(
    metadata: &MetadataMap,
) -> Result<Option<SpaceApiRouteEd25519Token>, SpaceApiRouteEd25519Error> {
    metadata
        .get(SPACE_API_ROUTE_ED25519_TOKEN_METADATA_KEY)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::parse)
        .transpose()
}

/// Decodes claims without verifying their Ed25519 signature.
///
/// This is only for producer/transit scheduling such as cache refresh before
/// expiry. Authorization must use [`SpaceApiRouteEd25519Verifier::verify`].
pub fn decode_unverified_space_api_route_ed25519_claims(
    token: &SpaceApiRouteEd25519Token,
) -> Result<SpaceApiRouteEd25519Claims, SpaceApiRouteEd25519Error> {
    let parsed = parse_token(token.secret())?;
    let claims = serde_json::from_slice::<SpaceApiRouteEd25519Claims>(&parsed.payload)?;
    claims.validate_profile()?;
    Ok(claims)
}

pub fn append_space_api_route_ed25519_token_metadata(
    source: &MetadataMap,
    target: &mut MetadataMap,
) {
    if let Some(value) = source.get(SPACE_API_ROUTE_ED25519_TOKEN_METADATA_KEY) {
        target.insert(SPACE_API_ROUTE_ED25519_TOKEN_METADATA_KEY, value.clone());
    }
}

struct ParsedToken {
    key_id: SpaceApiRouteEd25519KeyId,
    payload: Vec<u8>,
    signature: Vec<u8>,
    signing_input: String,
}

fn parse_token(token: &str) -> Result<ParsedToken, SpaceApiRouteEd25519Error> {
    let mut parts = token.split('.');
    if parts.next() != Some(TOKEN_PREFIX) {
        return Err(SpaceApiRouteEd25519Error::Malformed);
    }
    let key_id = parts
        .next()
        .ok_or(SpaceApiRouteEd25519Error::Malformed)?
        .parse()?;
    let encoded_payload = parts.next().ok_or(SpaceApiRouteEd25519Error::Malformed)?;
    let encoded_signature = parts.next().ok_or(SpaceApiRouteEd25519Error::Malformed)?;
    if parts.next().is_some() {
        return Err(SpaceApiRouteEd25519Error::Malformed);
    }
    let payload = URL_SAFE_NO_PAD
        .decode(encoded_payload)
        .map_err(|_| SpaceApiRouteEd25519Error::Malformed)?;
    let signature = URL_SAFE_NO_PAD
        .decode(encoded_signature)
        .map_err(|_| SpaceApiRouteEd25519Error::Malformed)?;
    Ok(ParsedToken {
        signing_input: format!("{TOKEN_PREFIX}.{key_id}.{encoded_payload}"),
        key_id,
        payload,
        signature,
    })
}

fn decode_signing_key_file(raw: Vec<u8>) -> Result<Vec<u8>, SpaceApiRouteEd25519Error> {
    let Ok(text) = std::str::from_utf8(&raw) else {
        return Ok(raw);
    };
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(SpaceApiRouteEd25519Error::InvalidPrivateKey);
    }
    if trimmed.starts_with("-----BEGIN PRIVATE KEY-----") {
        return decode_pem_block(trimmed, "PRIVATE KEY")
            .map_err(|_| SpaceApiRouteEd25519Error::InvalidPrivateKey);
    }
    match URL_SAFE_NO_PAD.decode(trimmed) {
        Ok(decoded) => Ok(decoded),
        Err(_) => Ok(raw),
    }
}

fn decode_public_verification_key(
    encoded: Option<String>,
    pem: Option<String>,
) -> Result<Vec<u8>, SpaceApiRouteEd25519Error> {
    match (encoded, pem) {
        (Some(encoded), None) => URL_SAFE_NO_PAD
            .decode(encoded.trim())
            .map_err(|_| SpaceApiRouteEd25519Error::InvalidPublicKey),
        (None, Some(pem)) => {
            const ED25519_SPKI_PREFIX: &[u8] = &[
                0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
            ];
            let der = decode_pem_block(pem.trim(), "PUBLIC KEY")
                .map_err(|_| SpaceApiRouteEd25519Error::InvalidPublicKey)?;
            der.strip_prefix(ED25519_SPKI_PREFIX)
                .filter(|key| key.len() == 32)
                .map(<[u8]>::to_vec)
                .ok_or(SpaceApiRouteEd25519Error::InvalidPublicKey)
        }
        _ => Err(SpaceApiRouteEd25519Error::InvalidPublicKey),
    }
}

fn decode_pem_block(text: &str, label: &str) -> Result<Vec<u8>, ()> {
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let body = text
        .strip_prefix(&begin)
        .and_then(|value| value.strip_suffix(&end))
        .ok_or(())?;
    let encoded = body.lines().map(str::trim).collect::<String>();
    STANDARD.decode(encoded).map_err(|_| ())
}

fn unix_now_secs() -> u64 {
    u64::try_from(jiff::Timestamp::now().as_second()).unwrap_or_default()
}

#[cfg(test)]
mod tests;
