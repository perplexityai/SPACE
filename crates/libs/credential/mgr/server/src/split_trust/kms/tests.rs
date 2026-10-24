use super::*;

#[test]
fn immutable_key_identity_rejects_aliases_and_ambiguous_arns() {
    for (value, accepted) in [
        (
            "arn:aws:kms:us-east-1:111122223333:key/mrk-0123456789abcdef0123456789abcdef",
            true,
        ),
        (
            "arn:aws:kms:us-east-1:111122223333:key/12345678-1234-4234-8234-123456789012",
            true,
        ),
        ("alias/sandbox-anchors", false),
        (
            "arn:aws:kms:us-east-1:111122223333:alias/sandbox-anchors",
            false,
        ),
        ("arn:aws:kms:us-east-1:111122223333:key/mrk-short", false),
        (
            "arn:aws:kms:us-east-1:1111:key/mrk-0123456789abcdef0123456789abcdef",
            false,
        ),
        (
            "arn:aws:kms::111122223333:key/mrk-0123456789abcdef0123456789abcdef",
            false,
        ),
    ] {
        assert_eq!(value.parse::<KeyArn>().is_ok(), accepted, "{value}");
    }
}

#[test]
fn hardware_result_is_bound_to_the_expected_key_algorithm_and_length() {
    let key = "arn:aws:kms:us-east-1:111122223333:key/mrk-0123456789abcdef0123456789abcdef";
    for (returned_key, algorithm, length, accepted) in [
        (key, MacAlgorithmSpec::HmacSha384, 48, true),
        ("wrong-key", MacAlgorithmSpec::HmacSha384, 48, false),
        (key, MacAlgorithmSpec::HmacSha256, 48, false),
        (key, MacAlgorithmSpec::HmacSha384, 32, false),
        (key, MacAlgorithmSpec::HmacSha384, 49, false),
    ] {
        let response = GenerateMacOutput::builder()
            .key_id(returned_key)
            .mac_algorithm(algorithm)
            .mac(Blob::new(vec![42; length]))
            .build();
        assert_eq!(
            HardwareDeriver::take_material(response, key).is_ok(),
            accepted
        );
    }
    HardwareDeriver::take_material(GenerateMacOutput::builder().build(), key).unwrap_err();
}
