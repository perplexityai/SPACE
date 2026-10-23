use super::*;

#[test]
fn envelopes_round_trip_keyrings_larger_than_kms_plaintext_limit() {
    let plaintext = vec![42; 16 * 1024];
    let envelope = Envelope::seal(&plaintext, &[7; 32], vec![1, 2, 3]).unwrap();
    let encoded = envelope.encode().unwrap();
    let decoded: Envelope = serde_json::from_slice(&encoded[MAGIC.len()..]).unwrap();
    assert_eq!(&*decoded.open(&[7; 32]).unwrap(), &plaintext);
}

#[test]
fn tampering_and_wrong_wrapping_keys_are_rejected() {
    let mut corrupted = Envelope::seal(b"keyring", &[7; 32], vec![1]).unwrap();
    corrupted.ciphertext[0] ^= 1;
    assert!(matches!(corrupted.open(&[7; 32]), Err(BackupError::Crypto)));
    let wrong_key = Envelope::seal(b"keyring", &[7; 32], vec![1]).unwrap();
    assert!(matches!(wrong_key.open(&[8; 32]), Err(BackupError::Crypto)));
    assert!(matches!(
        Envelope::seal(&vec![0; MAX_KEYRING_BYTES + 1], &[7; 32], vec![1]),
        Err(BackupError::TooLarge),
    ));
}
