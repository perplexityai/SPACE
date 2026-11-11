//! Coverage for the parts of the AWS backend that are ours rather than the
//! SDK's: the `ApiPassthrough` certificate profile and the
//! terminal-versus-retryable error split.
//!
//! The `IssueCertificate` → poll → `GetCertificate` sequence needs a live CA
//! and is not exercised by these unit tests.

use aws_sdk_acmpca::types::error::{
    InvalidArgsException, InvalidArnException, LimitExceededException, MalformedCsrException,
    RequestFailedException,
};

use super::*;

/// The `Blank` template asserts no key usage, so an omission here produces an
/// intermediate that cannot sign — failing at the first guest handshake, not at
/// issuance.
#[test]
fn the_passthrough_makes_the_intermediate_a_signing_ca_named_for_its_key() {
    let passthrough = intermediate_passthrough("abc123");
    let extensions = passthrough.extensions().expect("extensions are set");
    let key_usage = extensions.key_usage().expect("key usage is set");

    assert!(key_usage.key_cert_sign(), "must be able to sign leaves");
    assert!(key_usage.crl_sign());
    assert!(!key_usage.digital_signature());
    assert!(!key_usage.key_encipherment());
    assert!(!key_usage.data_encipherment());
    assert!(!key_usage.key_agreement());

    let eku = extensions.extended_key_usage();
    assert_eq!(eku.len(), 1);
    assert_eq!(
        eku[0].extended_key_usage_type(),
        Some(&ExtendedKeyUsageType::ServerAuth)
    );

    assert_eq!(
        passthrough
            .subject()
            .and_then(|subject| subject.common_name()),
        Some("abc123")
    );
}

/// AWS caps `IdempotencyToken` at 36 characters and the digest is passed there
/// verbatim, so an over-long one would fail every issuance.
#[test]
fn the_key_digest_fits_the_idempotency_token_limit() {
    let digest = key_digest(b"any spki bytes");
    assert_eq!(digest.len(), KEY_DIGEST_BYTES * 2);
    assert!(digest.len() <= 36);
    assert!(digest.chars().all(|c| c.is_ascii_hexdigit()));
}

/// The retry posture. A quota burst drains, so it has to be retryable; a
/// request AWS will never accept has to be terminal; and anything unmodelled
/// has to be retryable, or one transient takes a node out of service until it
/// restarts.
#[test]
fn aws_errors_split_into_terminal_and_retryable() {
    let terminal = [
        IssueCertificateError::InvalidArgsException(InvalidArgsException::builder().build()),
        IssueCertificateError::MalformedCsrException(MalformedCsrException::builder().build()),
        IssueCertificateError::InvalidArnException(InvalidArnException::builder().build()),
    ];
    for error in terminal {
        assert!(
            matches!(
                classify_issue_error(&error),
                IssueIntermediateError::Rejected(_)
            ),
            "{error:?} should be terminal"
        );
    }

    let retryable = [
        IssueCertificateError::LimitExceededException(LimitExceededException::builder().build()),
        IssueCertificateError::unhandled("something new"),
    ];
    for error in retryable {
        assert!(
            matches!(
                classify_issue_error(&error),
                IssueIntermediateError::Unavailable(_)
            ),
            "{error:?} should be retryable"
        );
    }

    assert!(matches!(
        classify_get_error(&GetCertificateError::RequestFailedException(
            RequestFailedException::builder().build()
        )),
        IssueIntermediateError::Unavailable(_)
    ));
}
