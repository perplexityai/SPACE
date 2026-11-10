#[test]
fn ordinary_process_cannot_enter_the_system_recovery_namespace() {
    // Native tests run as the runner/operator account, never as the SCM service.
    // The real namespace boundary must reject even an elevated administrator.
    let error = super::ensure().expect_err("non-SYSTEM process entered workload namespace");
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
}
