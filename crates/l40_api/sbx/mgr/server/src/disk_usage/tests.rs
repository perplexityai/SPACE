use super::*;

#[test]
fn gate_semantics_cover_both_extremes() {
    assert!(gate_engaged(0.0, 0.0), "engage 0 = always-on");
    assert!(gate_engaged(0.41, 0.40));
    assert!(!gate_engaged(0.39, 0.40));
    assert!(!gate_engaged(0.99, 1.0), "engage 1.0 = residue-only");
}

#[test]
fn misconfigured_pair_derives_target_below_engage() {
    let (engage, target) = validate_fraction_pair("test", 0.40, 0.40, "E", "T");
    assert!((engage - 0.40).abs() < 1e-9);
    assert!((target - 0.30).abs() < 1e-9);
    // Clamping out-of-range inputs.
    let (engage, target) = validate_fraction_pair("test", 7.0, -1.0, "E", "T");
    assert!((engage - 1.0).abs() < 1e-9);
    assert!(target.abs() < 1e-9);
    // engage 0 (always-on) never warns or derives.
    let (engage, target) = validate_fraction_pair("test", 0.0, 0.0, "E", "T");
    assert!(engage.abs() < 1e-9);
    assert!(target.abs() < 1e-9);
}
