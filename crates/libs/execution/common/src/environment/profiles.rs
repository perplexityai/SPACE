//! Runtime-neutral profiles share a wire shape; their semantics are implemented
//! and qualified by the catalog/adapter, never inferred from a profile's spelling.
use super::*;

#[cfg(test)]
mod tests;

pub(super) fn canonical_descriptor(descriptor: &EnvironmentDescriptor) -> String {
    let spec = descriptor.spec();
    let RuntimeBinding::ManagedRelease(release) = &spec.runtime;
    let mut text = format!(
        concat!(
            "space.execution.environment\nschema={}\n",
            "host.os={}\nhost.arch={}\nbackend={}\nguest.os={}\nguest.arch={}\n",
            "runtime.provider=managed_release\nruntime.kind={}\nruntime.version={}\nruntime.digest={}\n",
            "runtime.target.os={}\nruntime.target.arch={}\nruntime.layout={}\n"
        ),
        RUNTIME_NEUTRAL_ENVIRONMENT_SCHEMA_VERSION,
        spec.host.os,
        spec.host.architecture,
        spec.backend,
        spec.guest.os,
        spec.guest.architecture,
        release.runtime.kind,
        release.runtime.version,
        release.digest,
        release.target.os,
        release.target.architecture,
        release.layout,
    );
    if let BootProfile::ArtifactsV1 {
        contract,
        artifacts,
    } = &spec.boot
    {
        text.push_str(&format!(
            "boot.profile=artifacts_v1\nboot.contract={contract}\nboot.artifacts={}\n",
            artifacts.len()
        ));
        // Descriptor validation sorts by unique role; input order has no identity.
        for (index, artifact) in artifacts.iter().enumerate() {
            let component = artifact.component();
            text.push_str(&format!(
                "boot.{index}.role={}\nboot.{index}.kind={}\nboot.{index}.version={}\nboot.{index}.digest={}\nboot.{index}.format={}\n",
                artifact.role(), component.kind, component.version, component.digest, artifact.format(),
            ));
        }
    }
    text.push_str(&format!("isolation={}\n", spec.isolation));
    if let ExecutionCompatibilityProfile::RuntimeV1 {
        contract,
        state_format,
    } = &spec.compatibility
    {
        text.push_str(&format!(
            "compatibility.profile=runtime_v1\ncompatibility.contract={contract}\n"
        ));
        match state_format {
            Some(state) => text.push_str(&format!("compatibility.state=present\ncompatibility.state.format={}\ncompatibility.state.version={}\n", state.format, state.version)),
            None => text.push_str("compatibility.state=none\n"),
        }
    }
    text
}

impl From<&ComponentIdentity> for proto::EnvironmentComponent {
    fn from(value: &ComponentIdentity) -> Self {
        Self {
            kind: proto::ArtifactComponentKind::from(value.kind) as i32,
            version: value.version.to_string(),
            digest: value.digest.to_string(),
        }
    }
}

impl TryFrom<&proto::EnvironmentComponent> for ComponentIdentity {
    type Error = EnvironmentDecodeError;
    fn try_from(value: &proto::EnvironmentComponent) -> Result<Self, Self::Error> {
        Ok(Self {
            kind: proto::ArtifactComponentKind::try_from(value.kind)
                .map_err(|_| unsupported("component kind", value.kind))?
                .try_into()?,
            version: value.version.parse().map_err(ExecutionProtoError::from)?,
            digest: value.digest.parse()?,
        })
    }
}

impl From<&BootProfile> for proto::BootProfile {
    fn from(value: &BootProfile) -> Self {
        use proto::boot_profile::Profile;
        let profile = match value {
            BootProfile::LinuxDirectV1 { kernel } => {
                Profile::LinuxDirectV1(proto::LinuxDirectBootV1 {
                    kernel: Some(kernel.into()),
                })
            }
            BootProfile::ArtifactsV1 {
                contract,
                artifacts,
            } => Profile::ArtifactsV1(proto::ArtifactBootProfileV1 {
                contract: contract.to_string(),
                artifacts: artifacts
                    .iter()
                    .map(|artifact| proto::BootArtifact {
                        role: artifact.role().to_string(),
                        component: Some(artifact.component().into()),
                        format: artifact.format().to_string(),
                    })
                    .collect(),
            }),
        };
        Self {
            profile: Some(profile),
        }
    }
}

impl TryFrom<&proto::BootProfile> for BootProfile {
    type Error = EnvironmentDecodeError;
    fn try_from(value: &proto::BootProfile) -> Result<Self, Self::Error> {
        use proto::boot_profile::Profile;
        Ok(match required(value.profile.as_ref(), "boot profile")? {
            Profile::LinuxDirectV1(boot) => Self::LinuxDirectV1 {
                kernel: required(boot.kernel.as_ref(), "boot kernel")?.try_into()?,
            },
            Profile::ArtifactsV1(boot) => Self::ArtifactsV1 {
                contract: boot.contract.parse()?,
                artifacts: boot
                    .artifacts
                    .iter()
                    .map(|artifact| {
                        Ok(BootArtifact::new(
                            artifact.role.parse()?,
                            required(artifact.component.as_ref(), "boot artifact")?.try_into()?,
                            artifact.format.parse()?,
                        ))
                    })
                    .collect::<Result<_, EnvironmentDecodeError>>()?,
            },
        })
    }
}

impl From<&ExecutionCompatibilityProfile> for proto::ExecutionCompatibilityProfile {
    fn from(value: &ExecutionCompatibilityProfile) -> Self {
        use proto::execution_compatibility_profile::Profile;
        Self {
            profile: Some(match value {
                ExecutionCompatibilityProfile::FirecrackerSnapshotV1 { state_format } => {
                    Profile::FirecrackerSnapshotV1(proto::FirecrackerSnapshotCompatibilityV1 {
                        state_format: Some(state_format.into()),
                    })
                }
                ExecutionCompatibilityProfile::RuntimeV1 {
                    contract,
                    state_format,
                } => Profile::RuntimeV1(proto::RuntimeCompatibilityV1 {
                    contract: contract.to_string(),
                    state_format: state_format.as_ref().map(Into::into),
                }),
            }),
        }
    }
}

impl TryFrom<&proto::ExecutionCompatibilityProfile> for ExecutionCompatibilityProfile {
    type Error = EnvironmentDecodeError;
    fn try_from(value: &proto::ExecutionCompatibilityProfile) -> Result<Self, Self::Error> {
        use proto::execution_compatibility_profile::Profile;
        Ok(
            match required(value.profile.as_ref(), "compatibility profile")? {
                Profile::FirecrackerSnapshotV1(profile) => Self::FirecrackerSnapshotV1 {
                    state_format: required(profile.state_format.as_ref(), "state format")?
                        .try_into()?,
                },
                Profile::RuntimeV1(profile) => Self::RuntimeV1 {
                    contract: profile.contract.parse()?,
                    state_format: profile
                        .state_format
                        .as_ref()
                        .map(TryInto::try_into)
                        .transpose()?,
                },
            },
        )
    }
}
