use super::*;

fn gate(value: Option<&str>) -> bool {
    let mut env = pplx_space_test_env::TestEnv::default();
    if let Some(value) = value {
        env.set("SPACE_TEST_GATE", value);
    }
    EnvConfig(&env).destructive_gate("SPACE_TEST_GATE")
}

#[test]
fn parse_clamped_clamps_any_over_ceiling_value() {
    assert_eq!(parse_clamped::<u32>("7", 10), Some(7));
    assert_eq!(parse_clamped::<u32>("0", 10), Some(0));
    for over in ["11", "5000000000", "+5000000000", "99999999999999999999"] {
        assert_eq!(parse_clamped::<u32>(over, 10), Some(10), "{over:?}");
    }
    // An overflowing numeric prefix doesn't make garbage a number.
    for invalid in ["", "-5", "ten", " 7", "5000000000abc", "4294967296 "] {
        assert_eq!(parse_clamped::<u32>(invalid, u32::MAX), None, "{invalid:?}");
    }
}

#[test]
fn destructive_gate_is_on_when_unset_or_explicitly_truthy() {
    for value in [None, Some("1"), Some("true"), Some("TRUE"), Some(" yes ")] {
        assert!(gate(value), "{value:?} should keep the gate on");
    }
}

#[test]
fn destructive_gate_is_off_for_any_other_present_value() {
    for value in [
        Some("0"),
        Some("false"),
        Some(" false  "),
        Some("off"),
        Some("disabled"),
        Some("fasle"),
        Some(""),
        Some("   "),
    ] {
        assert!(!gate(value), "{value:?} should turn the gate off");
    }
}
