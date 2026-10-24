//! Prototype deterministic sandbox trust anchors.
//!
//! The encoding below is a persistent cryptographic protocol. Changing it
//! requires a new derivation scheme, not a new key epoch. RustCrypto is confined to
//! deterministic anchor signing; gateway certificates and TLS use AWS-LC.

use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use p256::{
    ecdsa::{Signature, SigningKey, signature::Signer},
    elliptic_curve::bigint::{Encoding, NonZero, U384},
    pkcs8::EncodePrivateKey,
};
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose, PKCS_ECDSA_P256_SHA256, PublicKeyData, SerialNumber,
};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use uuid::Uuid;
use zeroize::{Zeroize, Zeroizing};

mod catalog;
pub use catalog::{EpochCatalog, KeyEpoch};

pub const ANCHOR_SUBJECT: &str = "SPACE Sandbox Anchor";
pub const GATEWAY_LIFETIME_SECONDS: i64 = 365 * 24 * 60 * 60;
const ORDER_MINUS_ONE: U384 = U384::from_be_hex(
    "00000000000000000000000000000000ffffffff00000000ffffffffffffffffbce6faada7179e84f3b9cac2fc632550",
);

#[derive(Debug, thiserror::Error)]
pub enum AnchorError {
    #[error("sandbox id must be an RFC 9562 UUIDv7")]
    InvalidId,
    #[error("epoch catalog is empty, unordered, or uses an unsupported derivation scheme")]
    InvalidCatalog,
    #[error("catalog update removes or changes an existing epoch, key or activation time")]
    ChangedEpoch,
    #[error("cryptographic operation failed")]
    Crypto,
    #[error("invalid epoch catalog serialization: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("certificate operation failed: {0}")]
    Certificate(#[from] rcgen::Error),
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

/// Convert the provider's secret 384-bit KDF result into a stable anchor.
/// The caller binds the result to this catalog/identity and scrubs it on drop.
pub fn derive_anchor(
    catalog: &EpochCatalog,
    id: Uuid,
    material: &[u8; 48],
) -> Result<Anchor, AnchorError> {
    let info = catalog.derivation_input(id)?;
    let modulus = NonZero::new(ORDER_MINUS_ONE).expect("P-256 order minus one is nonzero");
    let mut reduced = (U384::from_be_slice(material) % modulus)
        .wrapping_add(&U384::ONE)
        .to_be_bytes();
    let scalar = Zeroizing::new(
        <[u8; 32]>::try_from(reduced.get(16..).ok_or(AnchorError::Crypto)?)
            .map_err(|_| AnchorError::Crypto)?,
    );
    reduced.zeroize();
    let signing_key = SigningKey::from_slice(scalar.as_ref()).map_err(|_| AnchorError::Crypto)?;
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
    // truncation as part of scheme 2 instead of emitting a 32-byte serial.
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
        certificate_der: certificate.der().to_vec(),
        private_key_der: Zeroizing::new(key.as_bytes().to_vec()),
    })
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
