//! Reading the certificate AWS Private CA returns. No hand-rolled ASN.1;
//! `x509-parser` does the decoding.

use x509_parser::prelude::FromDer;

use super::IssueIntermediateError;

/// Decode the PEM body AWS returns into the DER the node receives, confirming
/// on the way that the backend returned a certificate with a sane validity
/// window rather than an opaque blob.
pub(super) fn certificate_der_from_pem(pem: &str) -> Result<Vec<u8>, IssueIntermediateError> {
    let (_, block) = x509_parser::pem::parse_x509_pem(pem.as_bytes()).map_err(|error| {
        IssueIntermediateError::MalformedCertificate(format!("not a PEM certificate: {error}"))
    })?;
    let (_, certificate) = x509_parser::certificate::X509Certificate::from_der(&block.contents)
        .map_err(|error| {
            IssueIntermediateError::MalformedCertificate(format!("not a DER certificate: {error}"))
        })?;
    let validity = certificate.validity();
    if validity.not_after <= validity.not_before {
        return Err(IssueIntermediateError::MalformedCertificate(
            "certificate validity ends at or before it starts".to_string(),
        ));
    }
    Ok(block.contents)
}

#[cfg(test)]
mod tests;
