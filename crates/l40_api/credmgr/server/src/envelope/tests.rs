use super::*;

fn dek() -> [u8; 32] {
    let mut dek = [0u8; 32];
    rand::fill(&mut dek[..]);
    dek
}

#[test]
fn seal_open_round_trips() {
    let dek = dek();
    let sealed = seal(&dek, b"super secret credential", b"aad-context");
    assert_eq!(
        open(&dek, &sealed, b"aad-context").unwrap(),
        b"super secret credential"
    );
}

#[test]
fn open_rejects_wrong_aad() {
    let dek = dek();
    let sealed = seal(&dek, b"payload", b"org-a");
    assert_eq!(open(&dek, &sealed, b"org-b"), Err(OpenError::AuthFailed));
}

#[test]
fn open_rejects_wrong_dek() {
    let sealed = seal(&dek(), b"payload", b"aad");
    assert_eq!(open(&dek(), &sealed, b"aad"), Err(OpenError::AuthFailed));
}

#[test]
fn open_rejects_tampered_ciphertext() {
    let dek = dek();
    let mut sealed = seal(&dek, b"payload", b"aad");
    let last = sealed.len() - 1;
    sealed[last] ^= 0x01;
    assert_eq!(open(&dek, &sealed, b"aad"), Err(OpenError::AuthFailed));
}

#[test]
fn open_rejects_unknown_version() {
    let dek = dek();
    let mut sealed = seal(&dek, b"payload", b"aad");
    sealed[0] = 9;
    assert_eq!(
        open(&dek, &sealed, b"aad"),
        Err(OpenError::UnsupportedVersion(9))
    );
}

#[test]
fn open_rejects_truncated_input() {
    let dek = dek();
    assert_eq!(open(&dek, &[], b"aad"), Err(OpenError::Malformed));
    assert_eq!(
        open(&dek, &[VERSION, 1, 2], b"aad"),
        Err(OpenError::Malformed)
    );
}

#[test]
fn wrapped_dek_envelope_round_trips() {
    let cmk_id = uuid::Uuid::now_v7();
    let encoded = encode_wrapped_dek(cmk_id, b"backend-wrap-bytes");
    assert_eq!(
        decode_wrapped_dek(&encoded).unwrap(),
        (cmk_id, b"backend-wrap-bytes".as_slice())
    );
}

#[test]
fn wrapped_dek_envelope_rejects_bad_version_and_truncation() {
    let cmk_id = uuid::Uuid::now_v7();
    let mut encoded = encode_wrapped_dek(cmk_id, b"wrap");
    encoded[0] = 7;
    assert_eq!(
        decode_wrapped_dek(&encoded),
        Err(DekEnvelopeError::UnsupportedVersion(7))
    );
    assert_eq!(decode_wrapped_dek(&[]), Err(DekEnvelopeError::Malformed));
    assert_eq!(
        decode_wrapped_dek(&[VERSION, 1, 2, 3]),
        Err(DekEnvelopeError::Malformed)
    );
}

#[test]
fn fresh_nonce_every_seal() {
    let dek = dek();
    let a = seal(&dek, b"payload", b"aad");
    let b = seal(&dek, b"payload", b"aad");
    assert_ne!(a, b);
}
