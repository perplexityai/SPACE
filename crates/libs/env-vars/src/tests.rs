use super::{env_destructive_gate, env_flag, valid_positive_f64};

#[test]
fn valid_positive_f64_accepts_only_finite_positive() {
    assert_eq!(valid_positive_f64(6.0), Some(6.0));
    assert_eq!(valid_positive_f64(0.0), None);
    assert_eq!(valid_positive_f64(-1.0), None);
    assert_eq!(valid_positive_f64(f64::NAN), None);
    assert_eq!(valid_positive_f64(f64::INFINITY), None);
}

#[test]
fn truthy_values_enable_regardless_of_default() {
    for value in ["1", "true", "yes", "on", "enabled", "TRUE", " On "] {
        assert!(env_flag(Some(value), false), "{value:?} must be truthy");
        assert!(env_flag(Some(value), true), "{value:?} must be truthy");
    }
}

#[test]
fn falsy_values_disable_regardless_of_default() {
    for value in ["0", "false", "no", "off", "disabled", "FALSE", " Off "] {
        assert!(!env_flag(Some(value), false), "{value:?} must be falsy");
        assert!(!env_flag(Some(value), true), "{value:?} must be falsy");
    }
}

#[test]
fn destructive_gate_is_enabled_only_when_unset_or_explicitly_truthy() {
    assert!(env_destructive_gate(None));
    for value in ["1", "true", "yes", "on", "enabled", " TRUE "] {
        assert!(env_destructive_gate(Some(value)), "{value:?} must enable");
    }
    for value in [
        "0", "false", "no", "off", "disabled", "", "  ", "flase", "2",
    ] {
        assert!(
            !env_destructive_gate(Some(value)),
            "{value:?} must fail safe to disabled"
        );
    }
}

#[test]
fn unset_empty_and_garbage_fall_back_to_the_default() {
    for raw in [None, Some(""), Some("   "), Some("garbage"), Some("2")] {
        assert!(!env_flag(raw, false), "{raw:?} must keep default false");
        assert!(env_flag(raw, true), "{raw:?} must keep default true");
    }
}
