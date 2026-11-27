use super::{sandbox_id_matches, sandbox_label, sandbox_label_or_id};

#[test]
fn sandbox_label_uses_uuid_random_suffix() {
    assert_eq!(
        sandbox_label("019d84bd-280d-7331-99c6-b63377f3a3a2").as_deref(),
        Ok("77f3a3a2")
    );
}

#[test]
fn sandbox_label_handles_short_hex_ids() {
    assert_eq!(sandbox_label("abc12345").as_deref(), Ok("abc12345"));
}

#[test]
fn sandbox_label_or_id_handles_public_prefixed_ids_for_display_only() {
    assert_eq!(
        sandbox_label_or_id("sbx_019d84bd-280d-7331-99c6-b63377f3a3a2"),
        "77f3a3a2"
    );
    sandbox_label("sbx_019d84bd-280d-7331-99c6-b63377f3a3a2").unwrap_err();
}

#[test]
fn sandbox_id_matches_random_suffix_for_short_selectors() {
    let id = "019d84bd-280d-7331-99c6-b63377f3a3a2";
    assert!(sandbox_id_matches(id, "77f3a3a2"));
    assert!(sandbox_id_matches(id, "77f3"));
    assert!(!sandbox_id_matches(id, "019d84bd"));
}

#[test]
fn sandbox_id_matches_does_not_accept_public_prefixed_ids() {
    let id = "sbx_019d84bd-280d-7331-99c6-b63377f3a3a2";
    assert!(!sandbox_id_matches(id, "77f3a3a2"));
}

#[test]
fn sandbox_id_matches_long_uuid_prefixes() {
    let id = "019d84bd-280d-7331-99c6-b63377f3a3a2";
    assert!(sandbox_id_matches(id, "019d84bd-280d"));
    assert!(sandbox_id_matches(id, "019d84bd280d"));
}
