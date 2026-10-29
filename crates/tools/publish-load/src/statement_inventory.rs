//! A populated installation claim for the synthetic row-comparison workload.

use pplx_space_execution_common::proto::{
    ArtifactComponent, ArtifactComponentInstallation, ArtifactComponentKind, CpuArchitecture,
    GuestOperatingSystem, GuestPlatform, NodeExecutionInventory, RuntimeIdentity,
    RuntimeInstallation, RuntimeKind, StateFormatCapability, StateFormatIdentity,
};

pub(super) fn installed() -> NodeExecutionInventory {
    NodeExecutionInventory {
        policy: None,
        runtimes: vec![RuntimeInstallation {
            runtime: Some(RuntimeIdentity {
                kind: RuntimeKind::Firecracker.into(),
                version: "1.15.1".to_owned(),
            }),
            supported_guests: vec![GuestPlatform {
                os: GuestOperatingSystem::Linux.into(),
                architecture: CpuArchitecture::X8664.into(),
            }],
            state_formats: vec![StateFormatCapability {
                identity: Some(StateFormatIdentity {
                    format: "firecracker-snapshot".to_owned(),
                    version: "9.0.0".to_owned(),
                }),
                can_create: true,
                can_load: true,
            }],
        }],
        components: vec![ArtifactComponentInstallation {
            component: Some(ArtifactComponent {
                kind: ArtifactComponentKind::Kernel.into(),
                role: None,
                version: Some("6.1.155".to_owned()),
                digest: Some(format!("sha256:{}", "42".repeat(32))),
                format: Some("vmlinux".to_owned()),
            }),
            default_for_new: true,
            legacy_default: true,
        }],
    }
}
