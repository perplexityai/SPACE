use std::str::FromStr;

use super::*;

#[test]
fn content_hash_from_str_tolerates_bare_and_prefixed() {
    let hex = "ab".repeat(32);
    let prefixed = format!("sha256:{hex}");

    let bare_parsed: ContentHash = hex.parse().expect("bare 64-hex digest should parse");
    let prefixed_parsed: ContentHash = prefixed.parse().expect("prefixed digest should parse");

    assert_eq!(bare_parsed, prefixed_parsed);
    // A bare digest is canonicalized to the `sha256:`-prefixed form.
    assert_eq!(bare_parsed.to_string(), prefixed);
}

#[test]
fn content_hash_from_str_rejects_malformed() {
    // Not hex at all.
    "not-hex".parse::<ContentHash>().unwrap_err();
    // Right hex alphabet, wrong length — must not be blindly prefixed.
    "abc123".parse::<ContentHash>().unwrap_err();
    // Uppercase hex is rejected (digests are lowercase only).
    "AB".repeat(32).parse::<ContentHash>().unwrap_err();
    // Unknown algorithm prefix is still rejected, not auto-canonicalized.
    assert_eq!(
        format!("md5:{}", "ab".repeat(32)).parse::<ContentHash>(),
        Err(DigestParseError::UnsupportedAlgorithm("md5".to_string())),
    );
}

#[test]
fn digest_round_trips_canonical_encoding() {
    let digest = Digest::sha256([0xab; 32]);
    let encoded = digest.to_string();
    assert_eq!(Digest::from_str(&encoded).unwrap(), digest);
    assert!(matches!(
        Digest::from_str("ab"),
        Err(DigestParseError::MissingAlgorithmSeparator)
    ));
    assert!(matches!(
        Digest::from_str("sha512:abababababababababababababababababababababababababababababababab"),
        Err(DigestParseError::UnsupportedAlgorithm(_))
    ));
    assert!(matches!(
        Digest::from_str("sha256:ab"),
        Err(DigestParseError::InvalidHexLength { .. })
    ));
    assert!(matches!(
        Digest::from_str("sha256:AB00000000000000000000000000000000000000000000000000000000000000"),
        Err(DigestParseError::InvalidHex)
    ));
}

#[test]
fn digest_prefix_rejects_too_short_or_full_length() {
    assert!(matches!(
        DigestPrefix::from_str("sha256:111111"),
        Err(DigestPrefixParseError::PrefixTooShort { .. })
    ));
    assert!(matches!(
        DigestPrefix::from_str(
            "sha256:1111111111111111111111111111111111111111111111111111111111111111"
        ),
        Err(DigestPrefixParseError::PrefixTooLong { .. })
    ));
}
