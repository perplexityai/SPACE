use crate::catalog::*;
use pplx_space_common_core::types::*;

#[test]
fn initial_identity_and_legacy_restore_do_not_depend_on_display_name() {
    use crate::runtime_inventory::ExecutionSelectionMode;
    use crate::selection::ExecutionPolicy;
    let initial = InitialEnvironment::for_architecture(Arch::X86_64).unwrap();
    let entry = initial
        .compiled_environment()
        .unwrap()
        .named_for_test("renamed-initial");
    let bound = entry.definition().execution_requirements().unwrap();
    let historical = ExecutionMetadata::new(
        bound.runtime().clone(),
        bound.guest(),
        bound.state_format().cloned(),
        vec![],
    )
    .unwrap();
    let policy = ExecutionPolicy::parse(Some("renamed-initial"), None, &[entry]).unwrap();
    assert!(!policy.legacy_unbound_allowed());
    assert_eq!(policy.default_reference(), bound.environment());
    policy
        .check_execution(&historical, ExecutionSelectionMode::PersistedStateRestore)
        .unwrap();
}

#[test]
fn firmware_and_arm_install_pins_do_not_enable_execution_defaults() {
    use crate::selection::ExecutionPolicy;
    let arm = CompiledEnvironment::for_architecture(Arch::Aarch64).unwrap();
    let policy = ExecutionPolicy::parse(None, None, &arm).unwrap();
    assert!(policy.default_reference().is_none());
    ExecutionPolicy::parse(Some("ch-54.0-linux"), None, &arm).unwrap_err();
    let environments = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let defaults = ExecutionPolicy::parse(None, None, &environments).unwrap();
    for entry in environments
        .iter()
        .filter(|entry| entry.release().release.kind == RuntimeKind::CloudHypervisor)
    {
        assert!(!defaults.allows(entry.definition().reference()));
    }
}
