use super::*;

#[test]
fn secret_arn_requires_a_region_and_twelve_digit_account() {
    assert_eq!(
        parse_secret_arn("arn:aws:secretsmanager:us-west-2:123456789012:secret:smoke-AbCdEf")
            .unwrap(),
        ("123456789012", "us-west-2")
    );
    for arn in [
        "arn:aws:secretsmanager::123456789012:secret:smoke-AbCdEf",
        "arn:aws:secretsmanager:us-west-2::secret:smoke-AbCdEf",
        "arn:aws:secretsmanager:us-west-2:123:secret:smoke-AbCdEf",
    ] {
        assert!(
            matches!(parse_secret_arn(arn), Err(InvalidSecretArn)),
            "{arn}"
        );
    }
}
