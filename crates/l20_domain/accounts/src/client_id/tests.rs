use super::*;

#[test]
fn a_header_becomes_its_id_or_unknown() {
    for (header, id) in [
        (Some("alpha"), "alpha"),
        (Some("  batch-7\t"), "batch-7"),
        (Some("a.b_c-d"), "a.b_c-d"),
        (None, ClientId::UNKNOWN),
        (Some(""), ClientId::UNKNOWN),
        (Some("   "), ClientId::UNKNOWN),
        (Some("Not A Label!"), ClientId::UNKNOWN),
        (Some("-edge"), ClientId::UNKNOWN),
        (Some(&"x".repeat(64)), ClientId::UNKNOWN),
        (Some("unknown"), ClientId::UNKNOWN),
        (Some("Native"), ClientId::UNKNOWN),
        (Some("OVERFLOW"), ClientId::UNKNOWN),
    ] {
        assert_eq!(ClientId::from_header(header).as_str(), id, "{header:?}");
    }
    assert_eq!(
        ClientId::from_header(Some(&"x".repeat(63))).as_str().len(),
        63
    );
}

#[test]
fn stored_ids_include_the_reserved_ones() {
    assert_eq!(ClientId::parse("native"), Ok(ClientId::native()));
    assert_eq!(ClientId::parse("unknown"), Ok(ClientId::unknown()));
    ClientId::parse("").unwrap_err();
    ClientId::parse("no spaces").unwrap_err();
}
