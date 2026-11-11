//! Auxiliary boot files are opaque content. Listing does not infer a kernel
//! architecture, boot role or execution qualification from their bytes.
use super::*;
use crate::catalog::BOOT_ARTIFACT_IMAGE;

impl Scanner<'_> {
    pub(super) fn scan_boot_artifacts(&mut self, root: &Path, kind: ArtifactComponentKind) {
        let origin = ArtifactOrigin {
            kind: ArtifactKind::Component(kind),
            location: ArtifactLocation::Vendor,
            provenance: ArtifactProvenance::VersionDirectory,
        };
        let Some(names) = self.list_root(origin, root) else {
            return;
        };
        let prefix = format!("{kind}-");
        for name in names {
            let name = name.to_string_lossy().into_owned();
            let Some(suffix) = name.strip_prefix(&prefix) else {
                continue;
            };
            let path = root.join(&name);
            if self.expired(&path) {
                return;
            }
            let image = path.join(BOOT_ARTIFACT_IMAGE);
            let mut artifact = empty_artifact(origin, name.clone(), image.clone());
            let directory = match suffix.parse::<ManagedDirectoryName>() {
                Ok(directory) => directory,
                Err(error) => {
                    artifact.status = ArtifactStatus::Unrecognized;
                    artifact.detail =
                        Some(format!("invalid managed boot artifact directory: {error}"));
                    self.scan.artifacts.push(artifact);
                    continue;
                }
            };
            let version = match ArtifactVersion::parse(directory.label()) {
                Ok(version) => version,
                Err(error) => {
                    artifact.status = ArtifactStatus::Unrecognized;
                    artifact.detail = Some(error.to_string());
                    self.scan.artifacts.push(artifact);
                    continue;
                }
            };
            artifact.version = Some(ScannedVersion::Component(version));
            artifact.version_source = Some(VersionSource::DirectoryName);
            let inspected = (|| {
                if !std::fs::symlink_metadata(&path)?.is_dir() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "boot artifact entry is not a directory",
                    ));
                }
                let file = std::fs::OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                    .open(&image)?;
                let metadata = file.metadata()?;
                if !metadata.is_file() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "boot artifact image is not a regular file",
                    ));
                }
                Ok((file, metadata))
            })();
            match inspected {
                Ok((mut file, metadata)) => {
                    let measured = self.digest(origin.kind, &image, &mut file, &metadata);
                    if let Some(measure) = self.measured(measured, &image, &mut artifact) {
                        artifact.digest = Some(measure.digest);
                    }
                    check_directory_digest(&mut artifact, &directory);
                }
                Err(error) => {
                    artifact.status = match error.kind() {
                        std::io::ErrorKind::NotFound => ArtifactStatus::Incomplete,
                        std::io::ErrorKind::InvalidData => ArtifactStatus::Invalid,
                        _ => ArtifactStatus::Unreadable,
                    };
                    artifact.detail = Some(format!("inspect boot artifact: {error}"));
                }
            }
            self.scan.artifacts.push(artifact);
        }
    }
}
