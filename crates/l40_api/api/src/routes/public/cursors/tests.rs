use super::*;

const TEST_SECRET: &[u8] = b"test-public-cursor-secret";

#[test]
fn cursor_round_trips_payload() {
    let cursor =
        encode_cursor_with_secret(CursorKind::Sandbox, "123:abc".to_string(), TEST_SECRET).unwrap();

    assert_ne!(cursor.as_str(), "123:abc");
    assert_eq!(
        decode_cursor_with_secret(CursorKind::Sandbox, cursor.as_str(), TEST_SECRET).unwrap(),
        "123:abc"
    );
}

#[test]
fn cursor_rejects_wrong_kind() {
    let cursor =
        encode_cursor_with_secret(CursorKind::Sandbox, "123:abc".to_string(), TEST_SECRET).unwrap();

    decode_cursor_with_secret(CursorKind::Snapshot, cursor.as_str(), TEST_SECRET).unwrap_err();
}

#[test]
fn cursor_rejects_tampering() {
    let cursor =
        encode_cursor_with_secret(CursorKind::Template, "seed".to_string(), TEST_SECRET).unwrap();
    let mut raw = cursor.to_string();
    let last = raw.pop().unwrap();
    raw.push(if last == 'A' { 'B' } else { 'A' });

    decode_cursor_with_secret(CursorKind::Template, &raw, TEST_SECRET).unwrap_err();
}

#[test]
fn cursor_secret_requires_explicit_non_empty_value() {
    assert_eq!(
        cursor_secret_from_env(None),
        Err(PublicCursorConfigError::MissingSecret)
    );
    assert_eq!(
        cursor_secret_from_env(Some("   ".to_string())),
        Err(PublicCursorConfigError::MissingSecret)
    );
    assert_eq!(
        cursor_secret_from_env(Some("  shared-secret  ".to_string())).unwrap(),
        "shared-secret"
    );
}
