use super::*;
use crate::catalog::PinnedFile;
use pplx_space_common_core::types::{Arch, RuntimeVersion};

pub(crate) struct Fixture {
    pub(crate) _dir: tempfile::TempDir,
    pub(crate) releases: PathBuf,
    pub(crate) kernels: PathBuf,
    pub(crate) staging: PathBuf,
    pub(crate) store: ArtifactStore,
}

impl Fixture {
    pub(crate) fn new() -> Self {
        Self::in_directory(tempfile::tempdir().unwrap())
    }

    pub(crate) fn in_directory(dir: tempfile::TempDir) -> Self {
        // Keep checked ancestors inside the fixture even when the system
        // temporary directory belongs to a different account.
        let root = dir.path().join("space");
        let node = root.join("node");
        let releases = node.join("firecracker");
        let kernels = node.join("kernels");
        let staging = dir.path().join("staging");
        for path in [&root, &node, &releases, &kernels, &staging] {
            std::fs::create_dir_all(path).unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let owner = std::fs::metadata(&releases).unwrap().uid();
        let store = ArtifactStore::new(&releases, &kernels, &staging, 1 << 20).owned_by(owner);
        let fixture = Self {
            _dir: dir,
            releases,
            kernels,
            staging,
            store,
        };
        // A managed copy of the image-provided release.
        fixture.place_release("1.15.1", 1);
        fixture
    }

    /// Stage release `version`'s binaries under job `job`.
    pub(crate) fn stage_release(&self, job: &str, version: &str, fill: u8) {
        let bin = self
            .staged_releases()
            .join(job)
            .join(format!("v{version}/bin"));
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::write(bin.join("firecracker"), binary(fill)).unwrap();
        std::fs::write(bin.join("jailer"), binary(fill.wrapping_add(1))).unwrap();
    }

    /// Where the runtime manager stages Firecracker releases.
    pub(crate) fn staged_releases(&self) -> PathBuf {
        self.staging.join("firecracker/releases")
    }

    pub(crate) fn release_path(&self, version: &str, fill: u8) -> PathBuf {
        self.releases.join(managed_release_directory(
            &firecracker(version),
            pin(version, fill).digest(),
        ))
    }

    /// Put a verified release copy in the managed store.
    pub(crate) fn place_release(&self, version: &str, fill: u8) {
        let bin = self.release_path(version, fill).join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        for (name, fill) in [("firecracker", fill), ("jailer", fill.wrapping_add(1))] {
            std::fs::write(bin.join(name), binary(fill)).unwrap();
            std::fs::set_permissions(bin.join(name), std::fs::Permissions::from_mode(0o755))
                .unwrap();
        }
    }
}

pub(crate) fn binary(fill: u8) -> Vec<u8> {
    vec![fill; 4096]
}

pub(crate) fn sha256(bytes: &[u8]) -> Digest {
    Digest::sha256(Sha256::digest(bytes).into())
}

pub(crate) fn firecracker(version: &str) -> RuntimeIdentity {
    RuntimeIdentity {
        kind: RuntimeKind::Firecracker,
        version: RuntimeVersion::parse(version).unwrap(),
    }
}

/// Release `version`'s pins for binaries made from `fill`.
pub(crate) fn pin(version: &str, fill: u8) -> PinnedRelease {
    PinnedRelease {
        release: firecracker(version),
        architecture: Arch::X86_64,
        archive_sha256: sha256(b"archive"),
        binaries: vec![
            PinnedFile {
                path: "bin/firecracker",
                sha256: sha256(&binary(fill)),
            },
            PinnedFile {
                path: "bin/jailer",
                sha256: sha256(&binary(fill.wrapping_add(1))),
            },
        ],
    }
}

pub(crate) fn staged(job: &str) -> StagedName {
    StagedName::parse(job).unwrap()
}
