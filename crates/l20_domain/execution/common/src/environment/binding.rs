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
    /// Authenticate runtime, guest and state-format provenance without requiring
    /// boot inputs. Snapshot restore does not consume a guest kernel file.
    pub fn check_runtime(
        &self,
        execution: &ExecutionMetadata,
    ) -> Result<(), EnvironmentBindingError> {
        let spec = self.descriptor().spec();
        let RuntimeBinding::ManagedRelease(release) = &spec.runtime;
        if execution.runtime() != &release.runtime || execution.guest() != spec.guest {
            return Err(EnvironmentBindingError::RuntimeOrGuest);
        }
        if execution.state_format() != spec.compatibility.state_format() {
            return Err(EnvironmentBindingError::StateFormat);
        }
        Ok(())
    }

    /// Construct a request for this binding. Only a verified node resolver can
    /// turn these requirements into evidence that the environment is usable.
    pub fn execution_requirements(
        &self,
    ) -> Result<ExecutionMetadata, EnvironmentRequirementsError> {
        let spec = self.descriptor().spec();
        let RuntimeBinding::ManagedRelease(release) = &spec.runtime;
        let components = match &spec.boot {
            BootProfile::LinuxDirectV1 { kernel } => vec![ArtifactComponentMetadata {
                kind: kernel.kind,
                version: Some(kernel.version.clone()),
                digest: Some(kernel.digest),
                role: Some(BootProfile::LINUX_DIRECT_ROLE.parse()?),
                format: Some(BootProfile::LINUX_DIRECT_FORMAT.parse()?),
            }],
            BootProfile::ArtifactsV1 { artifacts, .. } => artifacts
                .iter()
                .map(|artifact| ArtifactComponentMetadata {
                    kind: artifact.component().kind,
                    version: Some(artifact.component().version.clone()),
                    digest: Some(artifact.component().digest),
                    role: Some(artifact.role().clone()),
                    format: Some(artifact.format().clone()),
                })
                .collect(),
        };
        Ok(ExecutionMetadata::new(
            release.runtime.clone(),
            spec.guest,
            spec.compatibility.state_format().cloned(),
            components,
        )?
        .with_environment(self.reference()))
    }

    /// Check a cold-boot consumer against the source image contract. Runtime
    /// version, state format and environment reference describe the producer;
    /// only explicit compatible selection may replace those with a consumer.
    /// This check is not installation evidence or execution authorization.
    pub fn check_cold_boot_source(
        &self,
        source: &ExecutionMetadata,
    ) -> Result<(), EnvironmentBindingError> {
        let spec = self.descriptor().spec();
        let RuntimeBinding::ManagedRelease(release) = &spec.runtime;
        if source.runtime().kind != release.runtime.kind || source.guest() != spec.guest {
            return Err(EnvironmentBindingError::RuntimeOrGuest);
        }
        self.check_boot_components(source)
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
        self.check_runtime(execution)?;
        self.check_boot_components(execution)
    }

    fn check_boot_components(
        &self,
        execution: &ExecutionMetadata,
    ) -> Result<(), EnvironmentBindingError> {
        let spec = self.descriptor().spec();
        let required = spec.boot.components();
        if execution.components().len() != spec.boot.components().count() {
            return Err(EnvironmentBindingError::BootComponents);
        }
        let strict_format = self.identity_schema_version() != ENVIRONMENT_SCHEMA_VERSION;
        for (role, component, format) in required {
            if !execution.components().iter().any(|observed| {
                observed.kind == component.kind
                    && observed.digest == Some(component.digest)
                    && observed
                        .role
                        .as_ref()
                        .is_some_and(|observed| observed.as_str() == role)
                    && (!strict_format
                        || observed
                            .format
                            .as_ref()
                            .is_some_and(|observed| observed.as_str() == format))
            }) {
                return Err(EnvironmentBindingError::BootComponents);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
