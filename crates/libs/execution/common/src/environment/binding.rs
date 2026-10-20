//! Match execution observations to an already approved environment. Binary
//! integrity and host prerequisites remain the node resolver's responsibility.
use super::*;

#[derive(Debug, thiserror::Error)]
pub enum EnvironmentBindingError {
    #[error("execution names environment {actual}, expected {expected}")]
    Reference {
        actual: EnvironmentReference,
        expected: EnvironmentReference,
    },
    #[error("execution runtime or guest differs from the environment")]
    RuntimeOrGuest,
    #[error("execution state format differs from the environment")]
    StateFormat,
    #[error("execution boot components differ from the environment")]
    BootComponents,
}

#[derive(Debug, thiserror::Error)]
pub enum EnvironmentRequirementsError {
    #[error("invalid execution requirements: {0}")]
    Metadata(#[from] ExecutionMetadataError),
    #[error("invalid boot role: {0}")]
    Role(#[from] ArtifactComponentRoleParseError),
    #[error("invalid boot format: {0}")]
    Format(#[from] ArtifactFormatParseError),
}

impl EnvironmentDefinition {
    /// Construct a request for this binding. Only a verified node resolver can
    /// turn these requirements into evidence that the environment is usable.
    pub fn execution_requirements(
        &self,
    ) -> Result<ExecutionMetadata, EnvironmentRequirementsError> {
        let spec = self.descriptor().spec();
        let RuntimeBinding::ManagedRelease(release) = &spec.runtime;
        let BootProfile::LinuxDirectV1 { kernel } = &spec.boot;
        let ExecutionCompatibilityProfile::FirecrackerSnapshotV1 { state_format } =
            &spec.compatibility;
        Ok(ExecutionMetadata::new(
            release.runtime.clone(),
            spec.guest,
            Some(state_format.clone()),
            vec![ArtifactComponentMetadata {
                kind: kernel.kind,
                version: Some(kernel.version.clone()),
                digest: Some(kernel.digest),
                role: Some("boot".parse()?),
                format: Some("vmlinux".parse()?),
            }],
        )?
        .with_environment(self.reference()))
    }

    /// Bind observations only after checking their complete supported shape.
    /// Kernel bytes take precedence over historical directory-name aliases,
    /// matching the existing runtime component resolver's identity semantics.
    pub fn bind_execution(
        &self,
        execution: ExecutionMetadata,
    ) -> Result<ExecutionMetadata, EnvironmentBindingError> {
        self.check_execution(&execution)?;
        Ok(execution.with_environment(self.reference()))
    }

    /// Check observations without cloning or changing their binding.
    pub fn check_execution(
        &self,
        execution: &ExecutionMetadata,
    ) -> Result<(), EnvironmentBindingError> {
        if let Some(actual) = execution.environment()
            && actual != self.reference()
        {
            return Err(EnvironmentBindingError::Reference {
                actual,
                expected: self.reference(),
            });
        }
        let spec = self.descriptor().spec();
        let RuntimeBinding::ManagedRelease(release) = &spec.runtime;
        if execution.runtime() != &release.runtime || execution.guest() != spec.guest {
            return Err(EnvironmentBindingError::RuntimeOrGuest);
        }
        let ExecutionCompatibilityProfile::FirecrackerSnapshotV1 { state_format } =
            &spec.compatibility;
        if execution.state_format() != Some(state_format) {
            return Err(EnvironmentBindingError::StateFormat);
        }
        let BootProfile::LinuxDirectV1 { kernel } = &spec.boot;
        let [observed] = execution.components() else {
            return Err(EnvironmentBindingError::BootComponents);
        };
        if observed.kind != kernel.kind
            || observed.digest != Some(kernel.digest)
            || !observed
                .role
                .as_ref()
                .is_some_and(|role| role.as_str() == "boot")
        {
            return Err(EnvironmentBindingError::BootComponents);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
