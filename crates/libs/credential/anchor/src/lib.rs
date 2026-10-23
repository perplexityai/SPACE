//! Prototype deterministic sandbox trust anchors.
//!
//! The encoding below is a persistent cryptographic protocol. Changing it
//! requires a new derivation scheme, not a new seed. RustCrypto is confined to
//! deterministic anchor signing; gateway certificates and TLS use AWS-LC.

use aws_lc_rs::{
    hkdf,
    rand::{SecureRandom, SystemRandom},
};
use p256::{
    ecdsa::{Signature, SigningKey, signature::Signer},
    elliptic_curve::bigint::{Encoding, NonZero, U384},
    pkcs8::EncodePrivateKey,
};
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose, PKCS_ECDSA_P256_SHA256, PublicKeyData, SerialNumber,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use uuid::Uuid;
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

pub const ANCHOR_SUBJECT: &str = "SPACE Sandbox Anchor";
pub const GATEWAY_LIFETIME_SECONDS: i64 = 365 * 24 * 60 * 60;
const ORDER_MINUS_ONE: U384 = U384::from_be_hex(
    "00000000000000000000000000000000ffffffff00000000ffffffffffffffffbce6faada7179e84f3b9cac2fc632550",
);

#[derive(Debug, thiserror::Error)]
pub enum AnchorError {
    #[error("sandbox id must be an RFC 9562 UUIDv7")]
    InvalidId,
    #[error("keyring is empty, unordered, or uses an unsupported derivation scheme")]
    InvalidKeyring,
    #[error("keyring has no seed for this sandbox; refusing to change its anchor")]
    MissingSeed,
    #[error("keyring update removes or changes an existing seed or activation time")]
    ChangedSeed,
    #[error("rotation activation must follow the current tail and propagation margin")]
    InvalidActivation,
    #[error("cryptographic operation failed")]
    Crypto,
    #[error("invalid keyring serialization: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("certificate operation failed: {0}")]
    Certificate(#[from] rcgen::Error),
}

/// Seed material is deliberately not Debug, and is scrubbed on drop.
#[derive(Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
#[serde(deny_unknown_fields)]
pub struct SeedVersion {
    pub version: u64,
    pub since_ms: u64,
    seed: [u8; 32],
}

impl SeedVersion {
    pub fn new(version: u64, since_ms: u64, seed: [u8; 32]) -> Self {
        Self {
            version,
            since_ms,
            seed,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Keyring {
    scheme: u32,
    versions: Vec<SeedVersion>,
}

pub fn sandbox_timestamp_ms(id: Uuid) -> Result<u64, AnchorError> {
    if id.get_version_num() != 7 || id.get_variant() != uuid::Variant::RFC4122 {
        return Err(AnchorError::InvalidId);
    }
    let bytes = id.as_bytes();
    Ok(u64::from_be_bytes([
        0, 0, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
    ]))
}

impl Keyring {
    pub fn new(versions: Vec<SeedVersion>) -> Result<Self, AnchorError> {
        let ring = Self {
            scheme: 1,
            versions,
        };
        ring.validate()?;
        Ok(ring)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, AnchorError> {
        let ring: Self = serde_json::from_slice(bytes)?;
        ring.validate()?;
        Ok(ring)
    }

    pub fn encode(&self) -> Result<Zeroizing<Vec<u8>>, AnchorError> {
        Ok(Zeroizing::new(serde_json::to_vec(self)?))
    }

    fn validate(&self) -> Result<(), AnchorError> {
        if self.scheme != 1
            || self.versions.is_empty()
            || self.versions.iter().any(|v| v.version == 0)
            || self
                .versions
                .iter()
                .zip(self.versions.iter().skip(1))
                .any(|(old, new)| old.since_ms >= new.since_ms || old.version >= new.version)
        {
            return Err(AnchorError::InvalidKeyring);
        }
        Ok(())
    }

    /// Updates may append new seeds, never remove or rewrite existing ones.
    /// This prototype has no retirement inventory and retains all versions.
    pub fn validate_successor(&self, next: &Self) -> Result<(), AnchorError> {
        next.validate()?;
        let tail = self.versions.last().ok_or(AnchorError::InvalidKeyring)?;
        let next_tail = next.versions.last().ok_or(AnchorError::InvalidKeyring)?;
        if next.versions.iter().any(|new| {
            !self.versions.iter().any(|old| old.version == new.version)
                && (new.version <= tail.version || new.since_ms <= tail.since_ms)
        }) {
            return Err(AnchorError::ChangedSeed);
        }
        for old in &self.versions {
            if let Some(new) = next.versions.iter().find(|new| new.version == old.version) {
                if new.since_ms != old.since_ms || new.seed != old.seed {
                    return Err(AnchorError::ChangedSeed);
                }
            } else {
                return Err(AnchorError::ChangedSeed);
            }
        }
        if next_tail.version < tail.version {
            return Err(AnchorError::ChangedSeed);
        }
        Ok(())
    }

    pub fn versions(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.versions.iter().map(|v| (v.version, v.since_ms))
    }

    pub fn hash(&self) -> Result<String, AnchorError> {
        Ok(hex::encode(Sha256::digest(&*self.encode()?)))
    }

    pub fn rotate(&self, now_ms: u64, margin_ms: u64) -> Result<Self, AnchorError> {
        let tail = self.versions.last().ok_or(AnchorError::InvalidKeyring)?;
        let since_ms = now_ms
            .checked_add(margin_ms)
            .ok_or(AnchorError::InvalidActivation)?;
        if margin_ms == 0 || since_ms <= tail.since_ms {
            return Err(AnchorError::InvalidActivation);
        }
        let mut seed = Zeroizing::new([0u8; 32]);
        SystemRandom::new()
            .fill(seed.as_mut())
            .map_err(|_| AnchorError::Crypto)?;
        let mut next = self.clone();
        next.versions.push(SeedVersion::new(
            tail.version
                .checked_add(1)
                .ok_or(AnchorError::InvalidActivation)?,
            since_ms,
            *seed,
        ));
        Ok(next)
    }

    pub fn derive(&self, id: Uuid) -> Result<Anchor, AnchorError> {
        let timestamp = sandbox_timestamp_ms(id)?;
        let version = self
            .versions
            .iter()
            .rev()
            .find(|version| version.since_ms <= timestamp)
            .ok_or(AnchorError::MissingSeed)?;
        let mut info = [0u8; 24];
        info[..8].copy_from_slice(&version.version.to_be_bytes());
        info[8..].copy_from_slice(id.as_bytes());

        struct OutputLength;
        impl hkdf::KeyType for OutputLength {
            fn len(&self) -> usize {
                48
            }
        }
        let salt = hkdf::Salt::new(hkdf::HKDF_SHA256, b"space-anchor");
        let prk = salt.extract(&version.seed);
        let mut okm = Zeroizing::new([0u8; 48]);
        prk.expand(&[&info], OutputLength)
            .map_err(|_| AnchorError::Crypto)?
            .fill(okm.as_mut())
            .map_err(|_| AnchorError::Crypto)?;
        let modulus = NonZero::new(ORDER_MINUS_ONE).expect("P-256 order minus one is nonzero");
        let mut reduced = (U384::from_be_slice(okm.as_ref()) % modulus)
            .wrapping_add(&U384::ONE)
            .to_be_bytes();
        let scalar = Zeroizing::new(
            <[u8; 32]>::try_from(reduced.get(16..).ok_or(AnchorError::Crypto)?)
                .map_err(|_| AnchorError::Crypto)?,
        );
        reduced.zeroize();
        let signing_key =
            SigningKey::from_slice(scalar.as_ref()).map_err(|_| AnchorError::Crypto)?;
        let public = signing_key.verifying_key().to_encoded_point(false);
        let signer = DeterministicSigner {
            signing_key,
            public,
        };

        let mut params = CertificateParams::default();
        params.distinguished_name = DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, ANCHOR_SUBJECT);
        params.is_ca = IsCa::Ca(BasicConstraints::Constrained(1));
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params.not_before = rcgen::date_time_ymd(2026, 1, 1);
        params.not_after = rcgen::date_time_ymd(2126, 1, 1);
        // RFC 5280 serials are positive and at most 20 octets. Freeze this
        // truncation as part of scheme 1 instead of emitting a 32-byte serial.
        let hash = Sha256::digest(info);
        let mut serial: [u8; 20] = hash
            .get(..20)
            .ok_or(AnchorError::Crypto)?
            .try_into()
            .map_err(|_| AnchorError::Crypto)?;
        serial[0] &= 0x7f;
        if serial == [0; 20] {
            serial[19] = 1;
        }
        params.serial_number = Some(SerialNumber::from_slice(&serial));
        let certificate = params.self_signed(&signer)?;
        let key = p256::SecretKey::from_slice(scalar.as_ref())
            .map_err(|_| AnchorError::Crypto)?
            .to_pkcs8_der()
            .map_err(|_| AnchorError::Crypto)?;
        Ok(Anchor {
            version: version.version,
            certificate_der: certificate.der().to_vec(),
            private_key_der: Zeroizing::new(key.as_bytes().to_vec()),
        })
    }
}

struct DeterministicSigner {
    signing_key: SigningKey,
    public: p256::EncodedPoint,
}

impl PublicKeyData for DeterministicSigner {
    fn der_bytes(&self) -> &[u8] {
        self.public.as_bytes()
    }
    fn algorithm(&self) -> &'static rcgen::SignatureAlgorithm {
        &PKCS_ECDSA_P256_SHA256
    }
}

impl rcgen::SigningKey for DeterministicSigner {
    fn sign(&self, message: &[u8]) -> Result<Vec<u8>, rcgen::Error> {
        let signature: Signature = self.signing_key.sign(message);
        Ok(signature.to_der().as_bytes().to_vec())
    }
}

/// Never Debug: contains an unencrypted per-sandbox private key.
pub struct Anchor {
    pub version: u64,
    pub certificate_der: Vec<u8>,
    pub private_key_der: Zeroizing<Vec<u8>>,
}

impl Anchor {
    /// Sign a validated P-256 CSR's public key. CSR validation remains at the
    /// credential manager boundary, not at this lower cryptographic layer.
    pub fn sign_gateway(
        &self,
        public_key: &rcgen::SubjectPublicKeyInfo,
        now: OffsetDateTime,
    ) -> Result<Vec<u8>, AnchorError> {
        let key = KeyPair::try_from(self.private_key_der.as_slice())?;
        let issuer = rcgen::Issuer::from_ca_cert_der(&self.certificate_der.as_slice().into(), key)?;
        let mut params = CertificateParams::default();
        params.distinguished_name = DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, "space-gateway-intermediate-ca");
        params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params.use_authority_key_identifier_extension = true;
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        params.not_before = now - time::Duration::minutes(5);
        params.not_after = (now + time::Duration::seconds(GATEWAY_LIFETIME_SECONDS))
            .min(rcgen::date_time_ymd(2126, 1, 1));
        if params.not_after <= now {
            return Err(AnchorError::Crypto);
        }
        let mut serial = [0u8; 16];
        SystemRandom::new()
            .fill(&mut serial)
            .map_err(|_| AnchorError::Crypto)?;
        serial[0] &= 0x7f;
        params.serial_number = Some(SerialNumber::from_slice(&serial));
        Ok(params.signed_by(public_key, &issuer)?.der().to_vec())
    }
}

#[cfg(test)]
mod tests;
