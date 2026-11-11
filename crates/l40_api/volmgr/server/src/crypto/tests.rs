use super::*;
use uuid::Uuid;

fn org() -> OrgId {
    OrgId::from_uuid(Uuid::parse_str("019e31d9-79a6-7132-b90f-007f1c62ddea").unwrap())
}
fn sbx(last: &str) -> SandboxId {
    SandboxId::from_uuid(
        Uuid::parse_str(&format!("019d84bd-280d-7331-99c6-b63377f3a3{last}")).unwrap(),
    )
}

#[test]
fn wrap_aad_is_deterministic_and_prefixed() {
    let a = wrap_aad(org(), &sbx("a2"));
    let b = wrap_aad(org(), &sbx("a2"));
    assert_eq!(a, b, "same inputs must produce the same AAD");
    assert_eq!(&a[..WRAP_AAD_PREFIX.len()], WRAP_AAD_PREFIX);
    assert_eq!(a.len(), WRAP_AAD_PREFIX.len() + 1 + 16 + 16);
}

#[test]
fn wrap_aad_differs_per_sandbox() {
    assert_ne!(wrap_aad(org(), &sbx("a2")), wrap_aad(org(), &sbx("aa")));
}

/// `wrap_aad` intentionally does NOT bind `artifact_id`: every
/// artifact already has its own DEK (fresh `IssueDek` per push),
/// so cross-artifact swap protection comes from key isolation,
/// not from the AAD. A wrap from one artifact can be supplied at
/// pull time even when the caller has lost the original
/// `artifact_id` — important because cluster-api's sidecar
/// persists the wrap but not the artifact_id. This compile-time
/// check pins the signature so a future change can't quietly
/// re-add the binding.
#[test]
fn entry_aad_differs_per_role_and_extends_wrap_aad() {
    let base = wrap_aad(org(), &sbx("a2"));
    let memfile = entry_aad(&base, &ArtifactEntryRole::parse("memfile").unwrap());
    let rootfs = entry_aad(&base, &ArtifactEntryRole::parse("rootfs").unwrap());
    assert_ne!(memfile, rootfs, "distinct roles must produce distinct AAD");
    assert_eq!(&memfile[..base.len()], base.as_slice());
    assert_eq!(
        memfile[base.len()],
        0,
        "role is NUL-separated from wrap_aad"
    );
}

/// Two entries of the same artifact share one DEK but must NOT be
/// cryptographically interchangeable. Encrypting under the `memfile`
/// entry AAD and decrypting under the `rootfs` entry AAD (same DEK,
/// same artifact) must fail authentication — so a `memfile`
/// ciphertext can't be served into the `rootfs` slot.
#[test]
fn entry_ciphertext_cannot_be_replayed_into_another_role_slot() {
    use std::io::Cursor;

    let base = wrap_aad(org(), &sbx("a2"));
    let memfile_aad = entry_aad(&base, &ArtifactEntryRole::parse("memfile").unwrap());
    let rootfs_aad = entry_aad(&base, &ArtifactEntryRole::parse("rootfs").unwrap());
    let dek = [7u8; KEY_LEN];
    let plaintext = b"the quick brown fox jumps over the lazy dog".repeat(100);

    let mut ciphertext = Vec::new();
    encrypt_stream_blocking(
        &mut Cursor::new(&plaintext),
        &mut ciphertext,
        &dek,
        &memfile_aad,
    )
    .expect("encrypt under memfile AAD");

    // Right role round-trips.
    let mut ok = Vec::new();
    decrypt_stream_blocking(&mut Cursor::new(&ciphertext), &mut ok, &dek, &memfile_aad)
        .expect("decrypt under the matching memfile AAD");
    assert_eq!(ok, plaintext);

    // Wrong role (same DEK, same artifact) is rejected by the AEAD.
    let mut wrong = Vec::new();
    let err = decrypt_stream_blocking(&mut Cursor::new(&ciphertext), &mut wrong, &dek, &rootfs_aad)
        .expect_err("decrypt under a different entry's AAD must fail");
    assert!(
        matches!(err, CryptoError::AuthFailed),
        "expected AuthFailed, got {err:?}"
    );
}
