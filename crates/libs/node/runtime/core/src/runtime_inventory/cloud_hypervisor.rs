//! CH execution discovery accepts only a release verified against compiled pins.
//! External diagnostic discovery is separate and never grants launch eligibility.
use super::*;

pub(super) struct CloudHypervisorDriver;

impl RuntimeDriver for CloudHypervisorDriver {
    fn restore_requires_boot_components(&self) -> bool {
        true
    }
    fn kind(&self) -> RuntimeKind {
        RuntimeKind::CloudHypervisor
    }

    #[cfg(any(feature = "artifact-store", test))]
    fn inspect_installation(
        &self,
        root: PathBuf,
        version: RuntimeVersion,
    ) -> Result<RuntimeInstallation, RuntimeInventoryError> {
        let binary = root.join("bin/cloud-hypervisor");
        if !probe_inventory_path(&binary)?.is_some_and(|metadata| metadata.is_file()) {
            return Err(RuntimeInventoryError::IncompleteInstallation {
                root,
                role: RuntimeFileRole::RuntimeExecutable,
            });
        }
        Ok(RuntimeInstallation {
            runtime: RuntimeIdentity {
                kind: self.kind(),
                version,
            },
            guest: GuestPlatform {
                os: GuestOperatingSystem::Linux,
                architecture: host_arch()?,
            },
            state_format: None,
            files: RuntimeInstallationFiles::new([(RuntimeFileRole::RuntimeExecutable, binary)]),
        })
    }

    fn discover_installations(
        &self,
        _root: &Path,
    ) -> Result<Vec<RuntimeInstallation>, RuntimeInventoryError> {
        // A file and version label are diagnostic evidence, not a qualification.
        Ok(Vec::new())
    }

    fn default_release(
        &self,
        _root: &Path,
    ) -> Result<Option<RuntimeVersion>, RuntimeInventoryError> {
        Ok(None)
    }

    fn compare_versions(
        &self,
        left: &RuntimeVersion,
        right: &RuntimeVersion,
    ) -> Result<Ordering, RuntimeInventoryError> {
        // Installed releases are exact inputs here; replacement preference is
        // owned by the qualification catalog, never inferred from directory names.
        // This ordering is only for diagnostic listing, not replacement selection.
        Ok(parse_version(left)?.cmp(&parse_version(right)?))
    }

    fn version_sort_key(&self, version: &RuntimeVersion) -> Option<VersionSortKey> {
        semver_sort_key(&parse_version(version).ok()?)
    }

    fn can_execute(
        &self,
        installed: &RuntimeInstallation,
        requested: &ExecutionMetadata,
        mode: ExecutionSelectionMode,
    ) -> Result<bool, RuntimeInventoryError> {
        Ok((mode != ExecutionSelectionMode::PersistedStateRestore
            || installed.state_format.is_some())
            && requested.environment().is_some()
            && installed.runtime.kind == self.kind()
            && installed.runtime == *requested.runtime()
            && installed.guest == requested.guest()
            && installed.state_format.as_ref() == requested.state_format())
    }
}

/// CH labels releases `major.minor`; read a missing patch as zero.
fn parse_version(version: &RuntimeVersion) -> Result<semver::Version, RuntimeInventoryError> {
    let text = version.as_str();
    let normalized = if text.split('.').count() == 2 {
        format!("{text}.0")
    } else {
        text.to_owned()
    };
    semver::Version::parse(&normalized).map_err(|source| {
        RuntimeInventoryError::InvalidVersionOutput {
            path: PathBuf::from("cloud-hypervisor"),
            output: format!("{version}: {source}"),
        }
    })
}
