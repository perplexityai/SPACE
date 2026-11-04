use super::*;

#[test]
fn rejects_a_body_that_is_not_a_certificate() {
    assert!(matches!(
        certificate_der_from_pem("not a pem at all").expect_err("rejected"),
        IssueIntermediateError::MalformedCertificate(_)
    ));
}
