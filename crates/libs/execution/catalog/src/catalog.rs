//! The runtime releases and guest kernels a node may install, pinned by the
//! SHA-256 of every byte that ends up executed or booted.
//!
//! The pins are the trust root for installs. A release source's own
//! checksums come from the same origin as the bytes, so they guard the
//! transfer, not the origin; a pin recorded here was reviewed when it was
//! added, as the image build's pins are. The runtime manager checks a
//! download against its pin before unpacking it, and node-manager checks
//! every byte it publishes into the node's store against the same pin, so a
//! compromised downloader cannot get other bytes executed.
//!
//! The compiled table covers the releases this build knows the
//! persisted-state format of and reviewed guest kernels. A deployment
//! adds entries with root-owned JSON files in a catalog directory (see
//! [`RuntimeCatalog::load`]); they may add pins and kernels, but not
//! releases whose state format the build does not know, since the execution
//! inventory refuses a node carrying one. An entry that contradicts the
//! compiled table is an error.

use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use pplx_space_common_core::types::{
    Arch, ArtifactVersion, Digest, DigestPrefix, RuntimeIdentity, RuntimeKind, RuntimeVersion,
};
use sha2::{Digest as _, Sha256};

use crate::layout::ReleaseLayout;

mod boot_artifact;
pub use boot_artifact::{BOOT_ARTIFACT_IMAGE, BootArtifactPinError, PinnedBootArtifact};
mod environment;
mod uefi;
mod verity;
pub use environment::{
    CompiledEnvironment, CompiledEnvironmentDefinitionError, ResolveEnvironmentError,
};
pub use verity::compiled_verity_digest;

/// The catalog file version this build reads.
pub const CATALOG_SCHEMA: u32 = 1;

/// Installation support does not qualify a runtime for sandbox execution.
pub(crate) const CLOUD_HYPERVISOR_INSTALL_VERSION: &str = "54.0";

struct CompiledRelease {
    runtime: RuntimeKind,
    version: &'static str,
    architecture: &'static str,
    archive_sha256: &'static str,
    /// `(release-layout path, sha256)`, in layout order.
    binaries: &'static [(&'static str, &'static str)],
}

/// Download and executable pins. Firecracker archives were checked against
/// their `.sha256.txt`; Cloud Hypervisor's static ARM64 and x86_64 payloads were
/// downloaded, hashed and compared with the upstream release asset digests.
const COMPILED_RELEASES: &[CompiledRelease] = &[
    CompiledRelease {
        runtime: RuntimeKind::Firecracker,
        version: "1.15.1",
        architecture: "x86_64",
        archive_sha256: "d4a32ab2322d887ca1bc4a4e7afa9cc35393e6362dfc2b3becb389d362e4275a",
        binaries: &[
            (
                "bin/firecracker",
                "7e8b57e88c459396d4680d83dcdd8c7f72305447cb55b11f4ac98ad70a3f7825",
            ),
            (
                "bin/jailer",
                "4830a9b1fc6cece036d8992ff12f1fe9c5247aacad77f42c7aba683c7a08622e",
            ),
        ],
    },
    CompiledRelease {
        runtime: RuntimeKind::Firecracker,
        version: "1.15.1",
        architecture: "aarch64",
        archive_sha256: "00654ac1e702a22744121ea9f10a4f792ebd7c3a744cba587dfac9fcb79b41a5",
        binaries: &[
            (
                "bin/firecracker",
                "e9ce7466c3b0d879d7a9158f4bf710dd5e131bbc5e580e5269fec66d5b5a0f0a",
            ),
            (
                "bin/jailer",
                "7faa581395fd1994ee005efc0a9c8826b4a9f0616dd942c2486adb8a8eac13f0",
            ),
        ],
    },
    CompiledRelease {
        runtime: RuntimeKind::Firecracker,
        version: "1.16.0",
        architecture: "x86_64",
        archive_sha256: "bd04e26952d4e158085778c6230a0b383d2619c319182e27eaa9d61a212e92d6",
        binaries: &[
            (
                "bin/firecracker",
                "9b15fcdc63f95262c40c9eacfb510fca4353b567dd7624086f759982158efa61",
            ),
            (
                "bin/jailer",
                "86314fcb7754e21ab7c05704188985932c3688b2fe27976b1de4434373d8fc33",
            ),
        ],
    },
    CompiledRelease {
        runtime: RuntimeKind::Firecracker,
        version: "1.16.0",
        architecture: "aarch64",
        archive_sha256: "531c713cdbc37d4b8bc2533d851aabc0267096afa1768086a37672abb668efd7",
        binaries: &[
            (
                "bin/firecracker",
                "24b0052659e9812040059effa00f3de25551558b9ac0079365e2da65f6a2b400",
            ),
            (
                "bin/jailer",
                "6af9dd0746fb675c0e2be933da7f8dbb8e1bf094382f1fb7f1e7e005c2d43062",
            ),
        ],
    },
    CompiledRelease {
        runtime: RuntimeKind::Firecracker,
        version: "1.16.1",
        architecture: "x86_64",
        archive_sha256: "382a02a869e4d6d5cb14c40577f9545e8458021ea8b0b2d3fc10ec14d9c242e6",
        binaries: &[
            (
                "bin/firecracker",
                "2fd0171309af7e24cf8dafc8a6f921c1434c49b5f9349bb996b7ed0a4deb8aa7",
            ),
            (
                "bin/jailer",
                "1f3a0c1fe86212d0001819bfe0819071c01208b3ccc9398c3b3bc1b84cf21edd",
            ),
        ],
    },
    CompiledRelease {
        runtime: RuntimeKind::Firecracker,
        version: "1.16.1",
        architecture: "aarch64",
        archive_sha256: "8d0e69f6d6f9a1724551f607f18504052c16c1828ee3d4d7b6e6c73380871e0e",
        binaries: &[
            (
                "bin/firecracker",
                "71ca0733576579a75cef268a8fd0ae0629b761b9844559c611f144132ac6038a",
            ),
            (
                "bin/jailer",
                "7db39d34991ccdd8d12aacab384b1dcbe35e79c27823e4e4d33725d4b504edd7",
            ),
        ],
    },
    CompiledRelease {
        runtime: RuntimeKind::Firecracker,
        version: "1.16.2",
        architecture: "x86_64",
        archive_sha256: "32e3cdcd4081f91fe2b024a266f57dcb3b4e5fec5033e0cb22467ad7f7820bda",
        binaries: &[
            (
                "bin/firecracker",
                "8227875ceda44177a4d501052dae4a2f9d7837362f5399f02cf0748e68418377",
            ),
            (
                "bin/jailer",
                "99832d2270741af67ffacd46da716b4b016338541af5a742907ab965b2e9ac9e",
            ),
        ],
    },
    CompiledRelease {
        runtime: RuntimeKind::Firecracker,
        version: "1.16.2",
        architecture: "aarch64",
        archive_sha256: "751365040ca3dde7616c5a1d97cc1304674fae469ce02993104eab22f5961950",
        binaries: &[
            (
                "bin/firecracker",
                "f7168507c37b2b047ea2b30433cbe4faa3986c36d1209c046fd1814118ce3d48",
            ),
            (
                "bin/jailer",
                "2e5288aebe6537cd77480c2dfa98f453e1d6d68b847aece44a29cce33ed00843",
            ),
        ],
    },
    CompiledRelease {
        runtime: RuntimeKind::Firecracker,
        version: "1.17.0",
        architecture: "aarch64",
        archive_sha256: "e351ebe4f7a16b5873bbd51005d2e6767103cff4d5ebc829df2d3f95a93e2256",
        binaries: &[
            (
                "bin/firecracker",
                "fe726e0b43c04363ac07e358be4dee982c3947c65ed3ae10c770fef5e1cd756c",
            ),
            (
                "bin/jailer",
                "4d8d2dd4dfc1d47932b2bd261479181dd0c54f0a2f32714421a7e7f9f07ddec8",
            ),
        ],
    },
    CompiledRelease {
        runtime: RuntimeKind::Firecracker,
        version: "1.17.0",
        architecture: "x86_64",
        archive_sha256: "06094a1108ae9e82aa4c23a775aa92758f53f1175d422270d9d6162cb9ade558",
        binaries: &[
            (
                "bin/firecracker",
                "99ad0f5cd0514a88aad0e9ae8cfdb3cc3b4ab9d190e1194602406c786b5de7a5",
            ),
            (
                "bin/jailer",
                "65ef226e96f0ceda55ba643f445801ef2cc0ea667ef67cad8ac4f406c9c8434f",
            ),
        ],
    },
    CompiledRelease {
        runtime: RuntimeKind::CloudHypervisor,
        version: CLOUD_HYPERVISOR_INSTALL_VERSION,
        architecture: "x86_64",
        archive_sha256: "4f4f98a3d661a40f3e5272627cc895a1b2e525f2f6bedf5f895f9cd21ba5c65b",
        binaries: &[(
            "bin/cloud-hypervisor",
            "4f4f98a3d661a40f3e5272627cc895a1b2e525f2f6bedf5f895f9cd21ba5c65b",
        )],
    },
    CompiledRelease {
        runtime: RuntimeKind::CloudHypervisor,
        version: CLOUD_HYPERVISOR_INSTALL_VERSION,
        architecture: "aarch64",
        archive_sha256: "e8a6d9786ea678e739f1a386682846e93742dcb9c7b6e3e0c62e3bda6cff0ef3",
        binaries: &[(
            "bin/cloud-hypervisor",
            "e8a6d9786ea678e739f1a386682846e93742dcb9c7b6e3e0c62e3bda6cff0ef3",
        )],
    },
];

struct CompiledKernel {
    version: &'static str,
    runtime_line: &'static str,
    architecture: &'static str,
    size: u64,
    sha256: &'static str,
}

/// Reviewed upstream direct-boot images; download transport is independent
/// of the node-manager content pins.
const CLOUD_HYPERVISOR_KERNELS: &[CompiledKernel] = &[
    CompiledKernel {
        version: "7.2.8+",
        runtime_line: "54.0",
        architecture: "x86_64",
        size: 48_205_848,
        sha256: "edfeb8a8a6c04ff2979e9f1c700feaa29b8f564f952261834ef0b7d2e673a853",
    },
    CompiledKernel {
        version: "7.2.8+",
        runtime_line: "54.0",
        architecture: "aarch64",
        size: 24_177_152,
        sha256: "8f8f3d5a983809f87a4e1e9a13053a3361fda47ef04e09c771a41c5e037cb6c2",
    },
];

/// Reviewed Firecracker CI guest kernels. The bucket publishes no SHA-256;
/// these pins identify inspected downloads. Source snapshots and verification
/// procedure are documented in `docs/runtime-manager-catalog.md`.
const FIRECRACKER_KERNELS: &[CompiledKernel] = &[
    CompiledKernel {
        version: "6.1.155",
        runtime_line: "1.15",
        architecture: "x86_64",
        size: 44_279_576,
        sha256: "e20e46d0c36c55c0d1014eb20576171b3f3d922260d9f792017aeff53af3d4f2",
    },
    CompiledKernel {
        version: "6.1.155",
        runtime_line: "1.15",
        architecture: "aarch64",
        size: 17_111_552,
        sha256: "e3544b10603acbf3db492cb52e000d22ba202cb4b63b9add027565683e11c591",
    },
    CompiledKernel {
        version: "5.10.253",
        runtime_line: "1.16",
        architecture: "aarch64",
        size: 17_248_768,
        sha256: "d272c5ba8bb35bd0e9f80d22107d9b4825cf29bb7fb8527b3372dfe51285f88c",
    },
    CompiledKernel {
        version: "6.1.172",
        runtime_line: "1.16",
        architecture: "aarch64",
        size: 17_912_320,
        sha256: "c3c6e5e0df50088ada201521dd334a57789723517d06fc4bd5e5ca830ba5104f",
    },
    CompiledKernel {
        version: "5.10.253",
        runtime_line: "1.16",
        architecture: "x86_64",
        size: 39_786_416,
        sha256: "2b28aa67c93048cba252dbb54a976bd844c463c221b4d4cfdae548bac8ecf0df",
    },
    CompiledKernel {
        version: "6.1.172",
        runtime_line: "1.16",
        architecture: "x86_64",
        size: 44_641_520,
        sha256: "2ee3ca4e6ee9d463e1c68e64602570dfb70cbaae6dfa120ed26b58200789ccc3",
    },
    CompiledKernel {
        version: "6.1.186",
        runtime_line: "1.17",
        architecture: "aarch64",
        size: 18_315_776,
        sha256: "85b23e1fc17848d90a7028c557b3d1ba71d676ff48ad1c357bc18f207302141a",
    },
    CompiledKernel {
        version: "6.18.44",
        runtime_line: "1.17",
        architecture: "aarch64",
        size: 19_397_120,
        sha256: "3b0233769ed8c89f1f47fdbcc4ff9300a2b1b5c618e25ade966a484481b151dc",
    },
    CompiledKernel {
        version: "6.1.186",
        runtime_line: "1.17",
        architecture: "x86_64",
        size: 44_965_016,
        sha256: "51565cd5d8bc6d7f3c856acdc8028ad1ef9996d581ac25f47a03429608c4f6ed",
    },
    CompiledKernel {
        version: "6.18.44",
        runtime_line: "1.17",
        architecture: "x86_64",
        size: 27_846_248,
        sha256: "d8ced68bd61e27b6813e2c993cc53a4029c59e13210672180591c84109684fe4",
    },
];

/// One file of a pinned release, at its release-layout path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PinnedFile {
    /// Relative to the release directory, as the release layout names it:
    /// `bin/firecracker`.
    pub path: &'static str,
    pub sha256: Digest,
}

/// A runtime release for one architecture, pinned.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PinnedRelease {
    pub release: RuntimeIdentity,
    pub architecture: Arch,
    /// The release archive the binaries come from.
    pub archive_sha256: Digest,
    /// Every binary the release layout names, in layout order.
    pub binaries: Vec<PinnedFile>,
}

impl PinnedRelease {
    /// The release's digest over its binaries; see [`release_digest`].
    pub fn digest(&self) -> Digest {
        release_digest(
            self.binaries
                .iter()
                .map(|binary| (binary.path, binary.sha256)),
        )
    }
}

/// A guest kernel built for a runtime line and architecture, pinned.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PinnedKernel {
    pub version: ArtifactVersion,
    pub runtime: RuntimeKind,
    /// The runtime's release line, such as `1.15`.
    pub runtime_line: RuntimeVersion,
    pub architecture: Arch,
    pub size: u64,
    pub sha256: Digest,
}

/// SHA-256 over a release's binaries: of the text `<name> <hex sha256>\n`
/// for each, in release-layout order, `<name>` being the path's file name.
/// The image build computes the same over what it bakes.
pub fn release_digest<'a>(binaries: impl IntoIterator<Item = (&'a str, Digest)>) -> Digest {
    let mut text = String::new();
    for (path, sha256) in binaries {
        let name = Path::new(path)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(path);
        text.push_str(name);
        text.push(' ');
        text.push_str(&sha256.hex_string());
        text.push('\n');
    }
    Digest::sha256(Sha256::digest(text.as_bytes()).into())
}

/// A managed directory is a compact locator, not a content identity. Full
/// digests remain authoritative for verification, publication, and removal.
/// This naming scheme is not used by external execution roots.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedDirectoryName {
    label: String,
    digest_prefix: DigestPrefix,
    full_digest: Option<Digest>,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "invalid managed artifact directory name {0:?}; expected <label>--<12 or 64 lowercase SHA-256 hex digits>"
)]
pub struct ManagedDirectoryNameParseError(String);

impl ManagedDirectoryName {
    pub fn release(release: &RuntimeIdentity, digest: Digest) -> Self {
        Self {
            label: format!("v{}", release.version),
            digest_prefix: digest.short_prefix(),
            full_digest: None,
        }
    }

    pub fn kernel(version: &ArtifactVersion, digest: Digest) -> Self {
        Self {
            label: version.to_string(),
            digest_prefix: digest.short_prefix(),
            full_digest: None,
        }
    }

    pub fn matches_digest(&self, digest: Digest) -> bool {
        self.full_digest
            .map_or_else(|| self.digest_prefix.matches(digest), |full| full == digest)
    }

    pub fn label(&self) -> &str {
        &self.label
    }
    pub fn digest_prefix(&self) -> &DigestPrefix {
        &self.digest_prefix
    }
}

impl std::fmt::Display for ManagedDirectoryName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}--{}",
            self.label,
            self.full_digest.map_or_else(
                || self.digest_prefix.hex_prefix().to_owned(),
                |digest| digest.hex_string()
            )
        )
    }
}

impl std::str::FromStr for ManagedDirectoryName {
    type Err = ManagedDirectoryNameParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || ManagedDirectoryNameParseError(value.to_owned());
        let (label, digest) = value.rsplit_once("--").ok_or_else(invalid)?;
        if ArtifactVersion::parse(label).is_err()
            && !label
                .strip_prefix('v')
                .is_some_and(|version| RuntimeVersion::parse(version).is_ok())
        {
            return Err(invalid());
        }
        if !matches!(digest.len(), 12 | 64) {
            return Err(invalid());
        }
        let full_digest: Option<Digest> = if digest.len() == 64 {
            Some(format!("sha256:{digest}").parse().map_err(|_| invalid())?)
        } else {
            None
        };
        let digest_prefix = match full_digest {
            Some(digest) => digest.short_prefix(),
            None => format!("sha256:{digest}").parse().map_err(|_| invalid())?,
        };
        Ok(Self {
            label: label.to_string(),
            digest_prefix,
            full_digest,
        })
    }
}

/// The immutable stock environment. Selection does not depend on mutable
/// aliases or on which other approved artifacts happen to be installed.
#[derive(Clone, Debug)]
pub struct InitialEnvironment {
    release: PinnedRelease,
    kernel: PinnedKernel,
}

impl InitialEnvironment {
    pub fn release(&self) -> &PinnedRelease {
        &self.release
    }
    pub fn kernel(&self) -> &PinnedKernel {
        &self.kernel
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn for_test(release: PinnedRelease, kernel: PinnedKernel) -> Self {
        Self { release, kernel }
    }

    pub fn for_architecture(architecture: Arch) -> Result<Self, CatalogError> {
        use pplx_space_common_core::types::{
            INITIAL_FIRECRACKER_KERNEL_VERSION, INITIAL_FIRECRACKER_RUNTIME_VERSION,
        };
        let catalog = RuntimeCatalog::compiled()?;
        let release = catalog
            .releases(RuntimeKind::Firecracker, architecture)
            .find(|pin| pin.release.version.as_str() == INITIAL_FIRECRACKER_RUNTIME_VERSION);
        let kernel = catalog.kernels.iter().find(|pin| {
            pin.architecture == architecture
                && pin.runtime == RuntimeKind::Firecracker
                && pin.version.as_str() == INITIAL_FIRECRACKER_KERNEL_VERSION
        });
        match (release, kernel) {
            (Some(release), Some(kernel)) => Ok(Self {
                release: release.clone(),
                kernel: kernel.clone(),
            }),
            _ => Err(CatalogError::UnsupportedInitialArchitecture(architecture)),
        }
    }
}

/// Why the catalog could not be loaded. Loading fails closed: a catalog
/// directory or file another account could write is refused, not skipped.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error(transparent)]
    EnvironmentDefinition(#[from] CompiledEnvironmentDefinitionError),
    #[error("compiled environment {runtime} / {kernel} is missing its release or kernel pins")]
    MissingEnvironmentPins {
        runtime: &'static str,
        kernel: &'static str,
    },
    #[error("the initial environment has no pins for host architecture {0}")]
    UnsupportedInitialArchitecture(Arch),
    #[error("compiled catalog entry {entry} is invalid: {field} {value:?}")]
    Compiled {
        entry: String,
        field: &'static str,
        value: &'static str,
    },
    #[error("inspect {}", path.display())]
    Inspect {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{} is {problem}; catalog paths must be root-owned and writable only by root", path.display())]
    Unsafe {
        path: PathBuf,
        problem: &'static str,
    },
    #[error("parse catalog file {}", path.display())]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error(
        "catalog file {} has schema {found}; this build reads schema {CATALOG_SCHEMA}",
        path.display()
    )]
    Schema { path: PathBuf, found: u32 },
    #[error("catalog file {} has an invalid {field} {value:?}", path.display())]
    Field {
        path: PathBuf,
        field: &'static str,
        value: String,
    },
    #[error(
        "catalog file {} pins {release}, whose persisted-state format this build does not know",
        path.display()
    )]
    UnknownRelease {
        path: PathBuf,
        release: RuntimeIdentity,
    },
    #[error(
        "catalog file {} pins {release} with binaries other than its release layout's",
        path.display()
    )]
    Layout {
        path: PathBuf,
        release: RuntimeIdentity,
    },
    #[error("catalog file {} pins {entry} differently from an earlier entry", path.display())]
    Conflict { path: PathBuf, entry: String },
}

/// The pinned releases and kernels a node may install.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeCatalog {
    releases: Vec<PinnedRelease>,
    kernels: Vec<PinnedKernel>,
    boot_artifacts: Vec<PinnedBootArtifact>,
}

impl RuntimeCatalog {
    /// A catalog of exactly these pins, as a test or a tool assembles one.
    /// What node-manager trusts is the catalog it loads itself.
    pub fn from_pins(releases: Vec<PinnedRelease>, kernels: Vec<PinnedKernel>) -> Self {
        Self {
            releases,
            kernels,
            boot_artifacts: Vec::new(),
        }
    }

    /// The compiled table alone.
    pub fn compiled() -> Result<Self, CatalogError> {
        let mut catalog = Self::default();
        for compiled in COMPILED_RELEASES {
            let entry = || {
                format!(
                    "{} {} {}",
                    compiled.runtime, compiled.version, compiled.architecture
                )
            };
            let field = |field, value| CatalogError::Compiled {
                entry: entry(),
                field,
                value,
            };
            let pinned = PinnedRelease {
                release: RuntimeIdentity {
                    kind: compiled.runtime,
                    version: RuntimeVersion::parse(compiled.version)
                        .map_err(|_| field("version", compiled.version))?,
                },
                architecture: compiled
                    .architecture
                    .parse()
                    .map_err(|_| field("architecture", compiled.architecture))?,
                archive_sha256: sha256_hex(compiled.archive_sha256)
                    .ok_or_else(|| field("archive_sha256", compiled.archive_sha256))?,
                binaries: compiled
                    .binaries
                    .iter()
                    .map(|(path, sha256)| {
                        Ok(PinnedFile {
                            path,
                            sha256: sha256_hex(sha256).ok_or_else(|| field("sha256", sha256))?,
                        })
                    })
                    .collect::<Result<_, CatalogError>>()?,
            };
            if !layout_matches(&pinned) {
                return Err(field("binaries", compiled.version));
            }
            catalog.releases.push(pinned);
        }
        for (runtime, compiled) in FIRECRACKER_KERNELS
            .iter()
            .map(|pin| (RuntimeKind::Firecracker, pin))
            .chain(
                CLOUD_HYPERVISOR_KERNELS
                    .iter()
                    .map(|pin| (RuntimeKind::CloudHypervisor, pin)),
            )
        {
            let entry = || format!("kernel {} {}", compiled.version, compiled.architecture);
            let field = |field, value| CatalogError::Compiled {
                entry: entry(),
                field,
                value,
            };
            catalog.kernels.push(PinnedKernel {
                version: ArtifactVersion::parse(compiled.version)
                    .map_err(|_| field("version", compiled.version))?,
                runtime,
                runtime_line: RuntimeVersion::parse(compiled.runtime_line)
                    .map_err(|_| field("runtime_line", compiled.runtime_line))?,
                architecture: compiled
                    .architecture
                    .parse()
                    .map_err(|_| field("architecture", compiled.architecture))?,
                size: compiled.size,
                sha256: sha256_hex(compiled.sha256)
                    .ok_or_else(|| field("sha256", compiled.sha256))?,
            });
        }
        catalog.boot_artifacts.push(uefi::firmware()?);
        Ok(catalog)
    }

    /// The compiled table plus every `*.json` file in `dir`. A missing
    /// directory adds nothing. The directory and its files must be owned by
    /// root and writable by no other account.
    pub fn load(dir: &Path) -> Result<Self, CatalogError> {
        Self::load_owned_by(dir, 0)
    }

    /// [`Self::load`], trusting files owned by `owner` instead of root.
    pub fn load_owned_by(dir: &Path, owner: u32) -> Result<Self, CatalogError> {
        let mut catalog = Self::compiled()?;
        let metadata = match std::fs::symlink_metadata(dir) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(catalog),
            Err(source) => {
                return Err(CatalogError::Inspect {
                    path: dir.to_path_buf(),
                    source,
                });
            }
        };
        check_trusted(dir, &metadata, owner, true)?;
        let inspect = |source| CatalogError::Inspect {
            path: dir.to_path_buf(),
            source,
        };
        let mut files = Vec::new();
        for entry in std::fs::read_dir(dir).map_err(inspect)? {
            let path = entry.map_err(inspect)?.path();
            if path
                .extension()
                .is_some_and(|extension| extension == "json")
            {
                files.push(path);
            }
        }
        // A stable order, so a conflict is reported the same way each time.
        files.sort();
        for path in files {
            let metadata =
                std::fs::symlink_metadata(&path).map_err(|source| CatalogError::Inspect {
                    path: path.clone(),
                    source,
                })?;
            check_trusted(&path, &metadata, owner, false)?;
            catalog.add_file(&path)?;
        }
        Ok(catalog)
    }

    fn add_file(&mut self, path: &Path) -> Result<(), CatalogError> {
        let text = std::fs::read(path).map_err(|source| CatalogError::Inspect {
            path: path.to_path_buf(),
            source,
        })?;
        let file: CatalogFile =
            serde_json::from_slice(&text).map_err(|source| CatalogError::Parse {
                path: path.to_path_buf(),
                source,
            })?;
        if file.schema != CATALOG_SCHEMA {
            return Err(CatalogError::Schema {
                path: path.to_path_buf(),
                found: file.schema,
            });
        }
        let field = |field, value: &str| CatalogError::Field {
            path: path.to_path_buf(),
            field,
            value: value.to_string(),
        };
        for entry in file.releases {
            let release = RuntimeIdentity {
                kind: RuntimeKind::parse(&entry.runtime)
                    .map_err(|_| field("runtime", &entry.runtime))?,
                version: RuntimeVersion::parse(&entry.version)
                    .map_err(|_| field("version", &entry.version))?,
            };
            let layout = ReleaseLayout::of(release.kind)
                .filter(|layout| (layout.known_releases)().contains(&release.version.as_str()))
                .ok_or_else(|| CatalogError::UnknownRelease {
                    path: path.to_path_buf(),
                    release: release.clone(),
                })?;
            if entry.binaries.len() != layout.binaries.len() {
                return Err(CatalogError::Layout {
                    path: path.to_path_buf(),
                    release,
                });
            }
            let mut binaries = Vec::new();
            for binary in layout.binaries {
                let sha256 = entry
                    .binaries
                    .get(*binary)
                    .ok_or_else(|| CatalogError::Layout {
                        path: path.to_path_buf(),
                        release: release.clone(),
                    })?;
                binaries.push(PinnedFile {
                    path: binary,
                    sha256: sha256.parse().map_err(|_| field("sha256", sha256))?,
                });
            }
            let pinned = PinnedRelease {
                architecture: entry
                    .architecture
                    .parse()
                    .map_err(|_| field("architecture", &entry.architecture))?,
                archive_sha256: entry
                    .archive_sha256
                    .parse()
                    .map_err(|_| field("archive_sha256", &entry.archive_sha256))?,
                release,
                binaries,
            };
            match self.release(&pinned.release, pinned.architecture) {
                Some(existing) if *existing == pinned => {}
                Some(_) => {
                    return Err(CatalogError::Conflict {
                        path: path.to_path_buf(),
                        entry: format!("{} {}", pinned.release, pinned.architecture),
                    });
                }
                None => self.releases.push(pinned),
            }
        }
        for entry in file.kernels {
            let pinned = PinnedKernel {
                version: ArtifactVersion::parse(&entry.version)
                    .map_err(|_| field("version", &entry.version))?,
                runtime: RuntimeKind::parse(&entry.runtime)
                    .map_err(|_| field("runtime", &entry.runtime))?,
                runtime_line: RuntimeVersion::parse(&entry.runtime_line)
                    .map_err(|_| field("runtime_line", &entry.runtime_line))?,
                architecture: entry
                    .architecture
                    .parse()
                    .map_err(|_| field("architecture", &entry.architecture))?,
                size: entry.size,
                sha256: entry
                    .sha256
                    .parse()
                    .map_err(|_| field("sha256", &entry.sha256))?,
            };
            let existing = self.kernels.iter().find(|kernel| {
                kernel.version == pinned.version && kernel.architecture == pinned.architecture
            });
            match existing {
                Some(existing) if *existing == pinned => {}
                // A kernel is installed by its version alone, so one
                // version names one image per architecture.
                Some(_) => {
                    return Err(CatalogError::Conflict {
                        path: path.to_path_buf(),
                        entry: format!("kernel {} {}", pinned.version, pinned.architecture),
                    });
                }
                None => self.kernels.push(pinned),
            }
        }
        for pin in file.boot_artifacts {
            let existing = self.boot_artifacts.iter().find(|existing| {
                existing.architecture() == pin.architecture()
                    && existing.identity().kind == pin.identity().kind
                    && existing.identity().version == pin.identity().version
            });
            match existing {
                Some(existing) if existing == &pin => {}
                Some(_) => {
                    return Err(CatalogError::Conflict {
                        path: path.to_path_buf(),
                        entry: format!(
                            "{} {} {}",
                            pin.identity().kind,
                            pin.identity().version,
                            pin.architecture()
                        ),
                    });
                }
                None => self.boot_artifacts.push(pin),
            }
        }
        Ok(())
    }

    pub fn boot_artifact(
        &self,
        identity: &pplx_space_common_core::types::ComponentIdentity,
        architecture: Arch,
    ) -> Option<&PinnedBootArtifact> {
        self.boot_artifacts
            .iter()
            .find(|pin| pin.identity() == identity && pin.architecture() == architecture)
    }

    pub fn boot_artifacts(&self, architecture: Arch) -> impl Iterator<Item = &PinnedBootArtifact> {
        self.boot_artifacts
            .iter()
            .filter(move |pin| pin.architecture() == architecture)
    }

    /// The pins of `release` for `architecture`.
    pub fn release(&self, release: &RuntimeIdentity, architecture: Arch) -> Option<&PinnedRelease> {
        self.releases
            .iter()
            .find(|pinned| pinned.release == *release && pinned.architecture == architecture)
    }

    /// Every pinned release of `runtime` for `architecture`.
    pub fn releases(
        &self,
        runtime: RuntimeKind,
        architecture: Arch,
    ) -> impl Iterator<Item = &PinnedRelease> {
        self.releases.iter().filter(move |pinned| {
            pinned.release.kind == runtime && pinned.architecture == architecture
        })
    }

    /// Every component candidate the Firecracker kernel adapter can resolve.
    pub fn all_kernels(&self, architecture: Arch) -> impl Iterator<Item = &PinnedKernel> {
        self.kernels
            .iter()
            .filter(move |pin| pin.architecture == architecture)
    }

    /// The pins of kernel `version` for `architecture`.
    pub fn kernel(&self, version: &ArtifactVersion, architecture: Arch) -> Option<&PinnedKernel> {
        self.kernels
            .iter()
            .find(|pinned| pinned.version == *version && pinned.architecture == architecture)
    }

    /// Every pinned kernel for a runtime line and architecture.
    pub fn kernels(
        &self,
        runtime: RuntimeKind,
        runtime_line: &RuntimeVersion,
        architecture: Arch,
    ) -> impl Iterator<Item = &PinnedKernel> {
        let runtime_line = runtime_line.clone();
        self.kernels.iter().filter(move |pinned| {
            pinned.runtime == runtime
                && pinned.runtime_line == runtime_line
                && pinned.architecture == architecture
        })
    }
}

/// A catalog file, as written.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogFile {
    schema: u32,
    #[serde(default)]
    releases: Vec<CatalogFileRelease>,
    #[serde(default)]
    kernels: Vec<CatalogFileKernel>,
    #[serde(default)]
    boot_artifacts: Vec<PinnedBootArtifact>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogFileRelease {
    runtime: String,
    version: String,
    architecture: String,
    archive_sha256: String,
    /// Release-layout path to `sha256:<hex>`.
    binaries: std::collections::BTreeMap<String, String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogFileKernel {
    version: String,
    runtime: String,
    runtime_line: String,
    architecture: String,
    size: u64,
    sha256: String,
}

fn sha256_hex(hex: &str) -> Option<Digest> {
    format!("sha256:{hex}").parse().ok()
}

fn layout_matches(pinned: &PinnedRelease) -> bool {
    ReleaseLayout::of(pinned.release.kind).is_some_and(|layout| {
        layout.binaries.len() == pinned.binaries.len()
            && layout
                .binaries
                .iter()
                .zip(&pinned.binaries)
                .all(|(path, binary)| *path == binary.path)
    })
}

/// Refuse a catalog path another account could write or swap.
fn check_trusted(
    path: &Path,
    metadata: &std::fs::Metadata,
    owner: u32,
    directory: bool,
) -> Result<(), CatalogError> {
    let unsafe_path = |problem| CatalogError::Unsafe {
        path: path.to_path_buf(),
        problem,
    };
    if metadata.file_type().is_symlink() {
        return Err(unsafe_path("a symlink"));
    }
    if directory && !metadata.is_dir() {
        return Err(unsafe_path("not a directory"));
    }
    if !directory && !metadata.is_file() {
        return Err(unsafe_path("not a regular file"));
    }
    if metadata.uid() != owner {
        return Err(unsafe_path("owned by another account"));
    }
    if metadata.permissions().mode() & 0o022 != 0 {
        return Err(unsafe_path("writable by its group or others"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
