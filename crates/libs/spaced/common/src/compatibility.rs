//! Directional host support for guest releases, independent of host identity.

use std::sync::LazyLock;

/// Keep the legacy floor until live guests, templates, and dormant snapshots
/// have a documented migration path. Do not derive it from CARGO_PKG_VERSION.
pub const SUPPORTED_SPACED_GUEST_VERSIONS: &str = ">=0.1.0, <0.9.0";

static SUPPORTED_GUESTS: LazyLock<semver::VersionReq> = LazyLock::new(|| {
    semver::VersionReq::parse(SUPPORTED_SPACED_GUEST_VERSIONS)
        .expect("supported spaced guest requirement is valid SemVer")
});

#[derive(Debug, thiserror::Error)]
pub enum GuestCompatibilityError {
    #[error("missing spaced guest version or compatibility family")]
    MissingIdentity,
    #[error("invalid spaced guest version '{version}': {source}")]
    InvalidVersion {
        version: String,
        #[source]
        source: semver::Error,
    },
    #[error(
        "spaced guest version {version} is unsupported; supported versions: {SUPPORTED_SPACED_GUEST_VERSIONS} (stable releases only)"
    )]
    UnsupportedVersion { version: semver::Version },
    #[error(
        "spaced guest version {version} requires compatibility family {expected}, found '{found}'"
    )]
    InconsistentFamily {
        version: semver::Version,
        expected: String,
        found: String,
    },
}

/// Validate artifact metadata or a live guest's readiness identity. The family
/// describes the guest; it is not a requirement that host and guest match.
pub fn validate_spaced_guest_compatibility(
    version: Option<&str>,
    family: Option<&str>,
) -> Result<(), GuestCompatibilityError> {
    let (Some(version), Some(family)) = (version, family) else {
        return Err(GuestCompatibilityError::MissingIdentity);
    };
    let version = semver::Version::parse(version).map_err(|source| {
        GuestCompatibilityError::InvalidVersion {
            version: version.to_owned(),
            source,
        }
    })?;
    if !SUPPORTED_GUESTS.matches(&version) {
        return Err(GuestCompatibilityError::UnsupportedVersion { version });
    }
    let expected = compatibility_family(&version);
    if family != expected {
        return Err(GuestCompatibilityError::InconsistentFamily {
            version,
            expected,
            found: family.to_owned(),
        });
    }
    Ok(())
}

pub(super) fn compatibility_family(version: &semver::Version) -> String {
    if version.major == 0 {
        if version.minor == 0 {
            format!("0.0.{}", version.patch)
        } else {
            format!("0.{}", version.minor)
        }
    } else {
        version.major.to_string()
    }
}

#[cfg(test)]
mod tests;

/// Releases through 0.8.0 shipped before mandatory platform descriptors.
/// Keep their Linux compatibility while requiring explicit claims in later releases.
pub fn spaced_guest_requires_platform_descriptor(version: &str) -> Result<bool, semver::Error> {
    Ok(semver::Version::parse(version)? > semver::Version::new(0, 8, 0))
}
