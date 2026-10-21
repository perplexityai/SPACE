use super::*;

#[test]
fn identity_configuration_is_checked_before_construction() {
    for execution in [
        FilesystemExecutionMode::IdentityWorkers,
        FilesystemExecutionMode::Direct,
    ] {
        let result = ExecutionService::with_config(
            CancellationToken::new(),
            ExecutionServiceConfig {
                filesystem_execution: execution,
                identity_policy: IdentityPolicy::Restricted,
                ..Default::default()
            },
        );
        match execution {
            FilesystemExecutionMode::Direct => assert!(matches!(
                result,
                Err(CreateExecutionServiceError::RestrictedRequiresIdentityWorkers)
            )),
            FilesystemExecutionMode::IdentityWorkers => {
                #[cfg(target_os = "linux")]
                assert!(matches!(
                    result,
                    Err(CreateExecutionServiceError::MissingWorkerExecutable)
                ));
                #[cfg(not(target_os = "linux"))]
                assert!(matches!(
                    result,
                    Err(CreateExecutionServiceError::UnsupportedIdentityWorkers)
                ));
            }
        }
    }
}
