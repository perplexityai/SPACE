use pplx_space_common_core::types::*;

pub struct FirecrackerStateFormatRelease {
    pub runtime_version: &'static str,
    pub state_format_version: &'static str,
}

pub const FIRECRACKER_STATE_FORMAT_RELEASES: &[FirecrackerStateFormatRelease] = &[
    FirecrackerStateFormatRelease {
        runtime_version: "1.15.1",
        state_format_version: "9.0.0",
    },
    FirecrackerStateFormatRelease {
        runtime_version: "1.16.0",
        state_format_version: "10.0.0",
    },
    FirecrackerStateFormatRelease {
        runtime_version: "1.16.1",
        state_format_version: "10.0.0",
    },
    FirecrackerStateFormatRelease {
        runtime_version: "1.16.2",
        state_format_version: "11.0.0",
    },
    FirecrackerStateFormatRelease {
        runtime_version: "1.17.0",
        state_format_version: "12.0.0",
    },
];

/// Resolve a runtime release's qualified memory-state format.
pub type StateFormatResolver = fn(&RuntimeVersion) -> Result<StateFormatIdentity, String>;

/// What one runtime's release directory holds.
pub struct ReleaseLayout {
    /// Binaries under the release directory, all required. The first is the
    /// runtime's own and names the release's architecture.
    pub binaries: &'static [&'static str],
    /// A qualified persisted-state format, when SPACE supports memory state
    /// for this runtime. A valid installation need not support snapshots.
    pub state_format: Option<StateFormatResolver>,
    /// Releases that may be pinned by the catalog. Recognizing a directory's
    /// layout alone does not qualify that release for installation or execution.
    pub known_releases: fn() -> Vec<&'static str>,
}

impl ReleaseLayout {
    /// The layout of `runtime`'s releases, for the runtimes the scan knows.
    pub fn of(runtime: RuntimeKind) -> Option<&'static Self> {
        match runtime {
            RuntimeKind::Firecracker => Some(&FIRECRACKER_RELEASE_LAYOUT),
            RuntimeKind::CloudHypervisor => Some(&CLOUD_HYPERVISOR_RELEASE_LAYOUT),
            RuntimeKind::Gvisor => None,
        }
    }
}

/// `bin/firecracker` and `bin/jailer`, writing the snapshot version the
/// known-release map gives.
pub const FIRECRACKER_RELEASE_LAYOUT: ReleaseLayout = ReleaseLayout {
    binaries: &["bin/firecracker", "bin/jailer"],
    state_format: Some(firecracker_state_format),
    known_releases: firecracker_known_releases,
};

const CLOUD_HYPERVISOR_RELEASE_LAYOUT: ReleaseLayout = ReleaseLayout {
    binaries: &["bin/cloud-hypervisor"],
    state_format: None,
    // Install pins do not qualify this runtime for sandbox execution.
    known_releases: || vec![crate::catalog::CLOUD_HYPERVISOR_INSTALL_VERSION],
};

fn firecracker_known_releases() -> Vec<&'static str> {
    FIRECRACKER_STATE_FORMAT_RELEASES
        .iter()
        .map(|release| release.runtime_version)
        .collect()
}

fn firecracker_state_format(version: &RuntimeVersion) -> Result<StateFormatIdentity, String> {
    let version = version
        .as_str()
        .strip_prefix('v')
        .unwrap_or(version.as_str());
    semver::Version::parse(version).map_err(|error| error.to_string())?;
    let release = FIRECRACKER_STATE_FORMAT_RELEASES
        .iter()
        .find(|entry| entry.runtime_version == version)
        .ok_or_else(|| format!("unknown Firecracker snapshot format for {version}"))?;
    Ok(StateFormatIdentity {
        format: StateFormatName::parse(FIRECRACKER_STATE_FORMAT)
            .map_err(|error| error.to_string())?,
        version: StateFormatVersion::parse(release.state_format_version)
            .map_err(|error| error.to_string())?,
    })
}
