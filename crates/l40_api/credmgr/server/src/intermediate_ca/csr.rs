//! The PKCS#10 certificate request a node offers, and the validation it has to
//! pass.
//!
//! The node holds the private half of the key being certified, so it signs a
//! real request; the authority never sees a private key. AWS Private CA's
//! `IssueCertificate` takes a CSR and nothing else, so this is also the only
//! shape that reaches the backend.
//!
//! AWS does not verify the request's signature and the `APIPassthrough`
//! template discards its subject and attributes, so every check here is ours
//! alone. They are exhaustive: a request must be a v1 PKCS#10 over a P-256 key,
//! self-signed with ecdsa-with-SHA256, carrying that one subject and no
//! attributes. Anything else is refused rather than ignored, so no field can
//! reach AWS unexamined.

use x509_parser::certification_request::X509CertificationRequest;
use x509_parser::oid_registry::{
    OID_EC_P256, OID_KEY_TYPE_EC_PUBLIC_KEY, OID_SIG_ECDSA_WITH_SHA256, OID_X509_COMMON_NAME,
};
use x509_parser::prelude::FromDer;
use x509_parser::x509::X509Version;

/// Checked before the DER parser sees the request. A P-256 CSR is ~250 bytes.
const MAX_CSR_LEN: usize = pplx_space_credmgr_client::MAX_INTERMEDIATE_CSR_BYTES;

/// The one subject a node request may carry. AWS rejects a CSR with neither
/// subject nor SAN, and the issued certificate's subject is set through
/// `ApiPassthrough`, so this value is never certified — it exists to satisfy
/// that rule with a constant rather than with caller-chosen text.
///
/// The node side ([`crate::node_ca`]) writes this same constant into the request
/// it offers, so the two halves of the contract cannot drift apart into a
/// validator that refuses every real request.
pub use pplx_space_credmgr_client::INTERMEDIATE_CSR_COMMON_NAME as CSR_COMMON_NAME;

/// PKCS#10 has one version, `v1`, encoded as zero.
const CSR_VERSION_V1: X509Version = X509Version(0);

/// Why a node's certificate request was refused. All terminal; the service
/// maps them to `INVALID_ARGUMENT`.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum CsrError {
    #[error("the certificate request is {len} bytes, over the {MAX_CSR_LEN}-byte limit")]
    TooLarge { len: usize },
    #[error("the certificate request is not a DER PKCS#10 CertificationRequest")]
    Malformed,
    #[error("the certificate request carries {len} trailing bytes after the DER structure")]
    TrailingBytes { len: usize },
    #[error("the certificate request is version {version}; PKCS#10 v1 (0) is the only version")]
    UnsupportedVersion { version: u32 },
    #[error("the offered key algorithm {oid} is not id-ecPublicKey; offer an ECDSA P-256 key")]
    UnsupportedKeyAlgorithm { oid: String },
    #[error("the offered key names curve {oid}; offer an ECDSA P-256 key")]
    UnsupportedCurve { oid: String },
    #[error("the offered key's algorithm identifier names no curve")]
    MissingCurve,
    #[error(
        "the certificate request is signed with {oid}; it must be signed with ecdsa-with-SHA256"
    )]
    UnsupportedSignatureAlgorithm { oid: String },
    #[error(
        "the certificate request's signature does not verify against the key it carries; sign it \
         with the private half of that key"
    )]
    BadSignature,
    #[error(
        "the certificate request carries {count} attributes; it must carry none, including \
         extension requests"
    )]
    UnexpectedAttributes { count: usize },
    #[error("the certificate request's subject must be exactly CN={CSR_COMMON_NAME}")]
    UnexpectedSubject,
}

/// A node certificate request that passed every check in this module.
#[derive(Clone, PartialEq, Eq)]
pub struct NodeCsr {
    der: Vec<u8>,
    public_key_der: Vec<u8>,
}

impl NodeCsr {
    pub fn parse(der: &[u8]) -> Result<Self, CsrError> {
        if der.len() > MAX_CSR_LEN {
            return Err(CsrError::TooLarge { len: der.len() });
        }

        let (rest, request) =
            X509CertificationRequest::from_der(der).map_err(|_| CsrError::Malformed)?;
        if !rest.is_empty() {
            return Err(CsrError::TrailingBytes { len: rest.len() });
        }

        let info = &request.certification_request_info;
        if info.version != CSR_VERSION_V1 {
            return Err(CsrError::UnsupportedVersion {
                version: info.version.0,
            });
        }

        // The key first: a curve we will not certify is a refusal whatever the
        // request was signed with. A `SubjectPublicKeyInfo` names its key type
        // by OID and, for an EC key, its curve by a second OID in the algorithm
        // parameters, so those two comparisons are the whole check.
        let algorithm = &info.subject_pki.algorithm;
        if algorithm.algorithm != OID_KEY_TYPE_EC_PUBLIC_KEY {
            return Err(CsrError::UnsupportedKeyAlgorithm {
                oid: algorithm.algorithm.to_id_string(),
            });
        }
        let curve = algorithm
            .parameters
            .as_ref()
            .and_then(|parameters| parameters.as_oid().ok())
            .ok_or(CsrError::MissingCurve)?;
        if curve != OID_EC_P256 {
            return Err(CsrError::UnsupportedCurve {
                oid: curve.to_id_string(),
            });
        }

        // Pin the algorithm before verifying, so verification cannot run under
        // one the caller chose.
        if request.signature_algorithm.algorithm != OID_SIG_ECDSA_WITH_SHA256 {
            return Err(CsrError::UnsupportedSignatureAlgorithm {
                oid: request.signature_algorithm.algorithm.to_id_string(),
            });
        }
        request
            .verify_signature()
            .map_err(|_| CsrError::BadSignature)?;

        let attributes = info.attributes().len();
        if attributes != 0 {
            return Err(CsrError::UnexpectedAttributes { count: attributes });
        }

        // Exactly one attribute, and it has to be the common name: the
        // iterator yields every distinguished-name attribute, so matching the
        // value alone would accept `O=space-node-intermediate-ca`.
        let mut subject = info.subject.iter_attributes();
        let common_name = subject.next().ok_or(CsrError::UnexpectedSubject)?;
        if subject.next().is_some()
            || common_name.attr_type() != &OID_X509_COMMON_NAME
            || common_name.as_str() != Ok(CSR_COMMON_NAME)
        {
            return Err(CsrError::UnexpectedSubject);
        }

        Ok(Self {
            der: der.to_vec(),
            // The caller's exact bytes: the certificate has to certify the key
            // offered, not a re-encoding of it.
            public_key_der: info.subject_pki.raw.to_vec(),
        })
    }

    /// DER `SubjectPublicKeyInfo` of the key this request asks to have
    /// certified.
    pub fn public_key_der(&self) -> &[u8] {
        &self.public_key_der
    }

    /// PEM, because `IssueCertificate` validates its `Csr` member against a
    /// `-----BEGIN CERTIFICATE REQUEST-----` regex and rejects raw DER with a
    /// `ValidationException` before reading it.
    pub fn pem(&self) -> String {
        use base64::Engine;
        let body = base64::engine::general_purpose::STANDARD.encode(&self.der);
        let mut pem = String::from("-----BEGIN CERTIFICATE REQUEST-----\n");
        for line in body.as_bytes().chunks(64) {
            pem.push_str(std::str::from_utf8(line).expect("base64 is ASCII"));
            pem.push('\n');
        }
        pem.push_str("-----END CERTIFICATE REQUEST-----\n");
        pem
    }
}

// Never the request bytes; they are long and say nothing at a glance.
impl std::fmt::Debug for NodeCsr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeCsr").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
