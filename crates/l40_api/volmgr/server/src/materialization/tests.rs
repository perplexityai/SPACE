use super::{MaterializationLayout, MaterializationMode};

#[test]
fn materialization_vocabularies_have_stable_wire_values() {
    assert_eq!("disabled".parse(), Ok(MaterializationMode::Disabled));
    assert_eq!("shadow".parse(), Ok(MaterializationMode::Shadow));
    assert_eq!("enabled".parse(), Ok(MaterializationMode::Enabled));
    "sparse.".parse::<MaterializationMode>().unwrap_err();
    assert_eq!(MaterializationLayout::Dense.to_string(), "dense");
    assert_eq!(MaterializationLayout::Sparse.to_string(), "sparse");
}

#[test]
fn materialization_dedup_suffix_preserves_legacy_disabled_key() {
    assert_eq!(MaterializationMode::Disabled.dedup_suffix(), "");
    assert_eq!(MaterializationMode::Shadow.dedup_suffix(), "\0shadow");
    assert_eq!(MaterializationMode::Enabled.dedup_suffix(), "\0enabled");
}
