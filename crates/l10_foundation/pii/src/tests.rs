use super::Pii;

#[test]
fn pii_debug_does_not_forward_inner_debug() {
    let value = Pii::new("customer@example.com");
    let debug = format!("{value:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("customer@example.com"));
}
