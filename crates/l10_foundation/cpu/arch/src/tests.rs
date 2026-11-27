use super::Arch;
use std::str::FromStr;

#[test]
fn from_str_accepts_canonical_names_and_aliases() {
    assert_eq!(Arch::from_str("x86_64").unwrap(), Arch::X86_64);
    assert_eq!(Arch::from_str("amd64").unwrap(), Arch::X86_64);
    assert_eq!(Arch::from_str("aarch64").unwrap(), Arch::Aarch64);
    assert_eq!(Arch::from_str("arm64").unwrap(), Arch::Aarch64);
}

#[test]
fn as_str_and_display_emit_canonical_name_not_alias() {
    // Aliases are accepted on input but the canonical name is what we render,
    // so `amd64`/`arm64` never round-trip back out.
    assert_eq!(Arch::X86_64.as_str(), "x86_64");
    assert_eq!(Arch::Aarch64.as_str(), "aarch64");
    assert_eq!(Arch::X86_64.to_string(), "x86_64");
    assert_eq!(Arch::Aarch64.to_string(), "aarch64");
}

#[test]
fn canonical_name_round_trips_through_from_str() {
    for arch in [Arch::X86_64, Arch::Aarch64] {
        assert_eq!(Arch::from_str(arch.as_str()).unwrap(), arch);
    }
}

#[test]
fn from_str_rejects_unknown_and_wrong_case() {
    // Parsing is case-sensitive: only the exact lowercase spellings are valid.
    for value in [
        "X86_64", "AMD64", "ARM64", "x86", "arm", "", " amd64", "amd64 ",
    ] {
        let error = Arch::from_str(value).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!("unsupported CPU architecture '{value}'"),
        );
    }
}
