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

#[test]
fn process_retention_is_bounded() {
    for retention in [
        ProcessRetention {
            completed: Duration::ZERO,
            ..ProcessRetention::default()
        },
        ProcessRetention {
            idempotency: Duration::ZERO,
            ..ProcessRetention::default()
        },
        ProcessRetention {
            completed: MAX_RETENTION + Duration::from_secs(1),
            ..ProcessRetention::default()
        },
    ] {
        let result = ExecutionService::with_config(
            CancellationToken::new(),
            ExecutionServiceConfig {
                filesystem_execution: FilesystemExecutionMode::Direct,
                identity_policy: IdentityPolicy::Guest,
                process_retention: retention,
                ..Default::default()
            },
        );
        assert!(
            matches!(
                result,
                Err(CreateExecutionServiceError::InvalidProcessRetention)
            ),
            "{retention:?} must be refused"
        );
    }
    ExecutionService::with_config(
        CancellationToken::new(),
        ExecutionServiceConfig {
            filesystem_execution: FilesystemExecutionMode::Direct,
            identity_policy: IdentityPolicy::Guest,
            process_retention: ProcessRetention {
                completed: MAX_RETENTION,
                idempotency: MAX_RETENTION,
            },
            ..Default::default()
        },
    )
    .expect("the maximum window is accepted");
}
