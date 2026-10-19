use super::*;

#[test]
fn identity_workers_require_explicit_configuration() {
    let result = ExecutionService::with_config(
        CancellationToken::new(),
        ExecutionServiceConfig {
            filesystem_execution: FilesystemExecutionMode::IdentityWorkers,
            ..Default::default()
        },
    );
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
