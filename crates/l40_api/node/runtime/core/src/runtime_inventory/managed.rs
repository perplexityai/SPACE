use super::*;
use crate::artifact_store::{
    ArtifactStore, KERNEL_IMAGE, KernelSource, existing_artifact_path, kernel_directory_names,
    release_directory, release_directory_names,
};
use crate::catalog::InitialEnvironment;
use pplx_space_common_core::types::{ArtifactComponentRoleParseError, ArtifactFormatParseError};

impl RuntimeInventory {
    /// Build an executable snapshot from external layouts and the sole managed
    /// binding. Failure to verify E0 withdraws only its affected artifacts.
    pub fn discover_with_store(
        store: &ArtifactStore,
        initial: &InitialEnvironment,
        vendor: &Path,
        assets: &Path,
        kernels: &Path,
    ) -> Result<Self, RuntimeInventoryError> {
        let mut inventory = Self::discover(
            [RuntimeInstallationRoot::new(
                RuntimeKind::Firecracker,
                vendor,
            )],
            assets,
            kernels,
        )?;
        inventory.initial_environment = if initial.release().architecture == Arch::X86_64 {
            Some(initial.definition()?)
        } else {
            None
        };
        let stock_legacy = inventory.stock_legacy_binding(initial, assets, kernels);
        for default in inventory
            .defaults
            .iter()
            .filter(|runtime| *runtime != &initial.release().release)
        {
            tracing::warn!(%default,
                "external runtime default differs from compiled E0; managed new boots use E0; explicit historical bindings remain available"
            );
        }
        let managed_roots = [store.releases_root(), store.kernels_root()]
            .into_iter()
            .map(canonicalize_inventory_path)
            .collect::<Result<Vec<_>, _>>()?;
        let external = |path: &Path| -> Result<bool, RuntimeInventoryError> {
            Ok(canonicalize_inventory_path(path)?.is_some_and(|resolved| {
                !managed_roots
                    .iter()
                    .flatten()
                    .any(|root| resolved.starts_with(root))
            }))
        };
        // Legacy links must not introduce removable managed candidates into
        // execution. Overlapping prototype roots use a separate read-only path.
        let mut runtimes = Vec::new();
        for runtime in inventory.runtimes {
            let mut is_external = true;
            for file in runtime.files.files.values() {
                is_external &= external(file)?;
            }
            if is_external {
                runtimes.push(runtime);
            }
        }
        inventory.runtimes = runtimes;
        let mut components = Vec::new();
        for component in inventory.components {
            if external(&component.path)? {
                components.push(component);
            }
        }
        inventory.components = components;

        let baked_release = vendor.join(release_directory(&initial.release().release));
        let cache_parent = kernels
            .parent()
            .ok_or_else(|| RuntimeInventoryError::ParentlessKernelRoot(kernels.to_path_buf()))?;
        let mut watched = vec![
            store.releases_root().to_path_buf(),
            store.kernels_root().to_path_buf(),
            cache_parent.to_path_buf(),
            baked_release.join("bin/firecracker"),
            baked_release.join("bin/jailer"),
            kernels
                .join(initial.kernel().version.as_str())
                .join(KERNEL_IMAGE),
            kernels.join("default").join(KERNEL_IMAGE),
            kernels.join("legacy-default").join(KERNEL_IMAGE),
            assets.join("default/kernel/vmlinux.bin"),
            vendor.join("default"),
        ];
        watched.extend(binding_paths(
            store,
            cache_parent,
            initial.release(),
            Some(initial.kernel()),
        ));
        // Protected consumers check the full ancestry; repairing any rejected
        // ancestor must invalidate the cached observation too.
        watched.extend(cache_parent.ancestors().map(Path::to_path_buf));
        watched.extend(inventory.components.iter().map(|entry| entry.path.clone()));
        // A broken preferred copy never silently selects a fallback copy.
        inventory
            .runtimes
            .retain(|entry| entry.runtime != initial.release().release);
        let release = (|| {
            let managed_release = managed_path(
                store.releases_root(),
                release_directory_names(&initial.release().release, initial.release().digest()),
            )?;
            let cached_release =
                ArtifactStore::execution_release_path(cache_parent, initial.release())?;
            let source = if probe_inventory_path(&managed_release)?.is_some() {
                RuntimeSource::Store(&managed_release)
            } else {
                RuntimeSource::Baked(&baked_release)
            };
            if probe_inventory_path(source.path())?.is_some()
                || probe_inventory_path(&cached_release.join("bin/firecracker"))?.is_some()
                || probe_inventory_path(&cached_release.join("bin/jailer"))?.is_some()
            {
                inventory.runtimes.push(runtime_from_source(
                    store,
                    initial.release(),
                    source,
                    cache_parent,
                )?);
            }
            Ok::<_, RuntimeInventoryError>(())
        })();
        if let Err(error) = release {
            tracing::warn!(%error, "E0 runtime unavailable; retaining unrelated runtime bindings");
            let error = inventory.binding_error(error);
            inventory.unavailable.push(UnavailableArtifact::Runtime {
                identity: initial.release().release.clone(),
                error,
            });
        }
        let initial_kernels: Vec<_> = inventory
            .components
            .iter()
            .filter(|entry| entry.metadata.digest == Some(initial.kernel().sha256))
            .cloned()
            .collect();
        inventory
            .components
            .retain(|entry| entry.metadata.digest != Some(initial.kernel().sha256));
        let kernel = (|| {
            let managed_kernel = managed_path(
                store.kernels_root(),
                kernel_directory_names(&initial.kernel().version, initial.kernel().sha256),
            )?;
            let cached_kernel =
                ArtifactStore::execution_kernel_path(cache_parent, initial.kernel())?;
            let managed_present = probe_inventory_path(&managed_kernel)?.is_some();
            if managed_present
                || (initial_kernels.is_empty() && probe_inventory_path(&cached_kernel)?.is_some())
            {
                let image = managed_kernel.join(KERNEL_IMAGE);
                // A present managed source remains authoritative. When only a
                // cache copy survives, verify that copy without opening a missing store.
                let source = if managed_present {
                    KernelSource::Store(&image)
                } else {
                    KernelSource::ExecutionCache(&cached_kernel)
                };
                let staged = kernel_from_source(store, initial.kernel(), source, cache_parent)?;
                inventory.add_initial_kernel(initial, staged)?;
            } else {
                let mut failure = None;
                for component in initial_kernels {
                    let verified = (|| {
                        let target = std::fs::canonicalize(&component.path).map_err(|source| {
                            RuntimeInventoryError::ProbeInventoryPath {
                                path: component.path.clone(),
                                source,
                            }
                        })?;
                        kernel_from_source(
                            store,
                            initial.kernel(),
                            KernelSource::ExecutionCache(&target),
                            cache_parent,
                        )
                    })();
                    match verified {
                        Ok(target) => {
                            // Several external labels may identify the same pinned
                            // bytes. Publish one canonical component for this identity.
                            inventory.add_initial_kernel(initial, target)?;
                            return Ok(());
                        }
                        Err(error) => {
                            tracing::warn!(%error, "baked E0 kernel copy unavailable");
                            // Only exhaustion withdraws the identity. Keep a
                            // transient failure if any alternative may recover
                            // without an operator changing its files.
                            if failure.is_none() || error.is_transient() {
                                failure = Some(error);
                            }
                        }
                    }
                }
                if let Some(error) = failure {
                    return Err(error);
                }
            }
            Ok::<_, RuntimeInventoryError>(())
        })();
        if let Err(error) = kernel {
            tracing::warn!(%error, "E0 kernel unavailable; retaining unrelated component bindings");
            let error = inventory.binding_error(error);
            inventory.unavailable.push(UnavailableArtifact::Kernel {
                digest: initial.kernel().sha256,
                legacy: stock_legacy,
                error,
            });
        }
        inventory.defaults = vec![initial.release().release.clone()];
        for component in &mut inventory.components {
            let is_initial = component.metadata.kind == ArtifactComponentKind::Kernel
                && component.metadata.digest == Some(initial.kernel().sha256);
            component.default_for_new = is_initial;
            component.legacy_default = is_initial && stock_legacy;
        }
        inventory.remember_paths(watched);
        Ok(inventory)
    }

    /// Include pinned artifacts independently; never change new/legacy defaults.
    /// Runtime failures withdraw all operations for this binding; boot-component
    /// failures withdraw cold boots only. Other bindings remain independent.
    pub fn include_environment(
        &mut self,
        store: &ArtifactStore,
        environment: &crate::catalog::CompiledEnvironment,
        kernels: &Path,
    ) -> Result<(), RuntimeInventoryError> {
        let release = environment.release();
        let kernel = environment.kernel();
        let cache_parent = kernels
            .parent()
            .ok_or_else(|| RuntimeInventoryError::ParentlessKernelRoot(kernels.to_path_buf()))?;
        let runtime = (|| {
            if let Some(runtime) = self.runtimes.iter().find(|entry| {
                entry.runtime == release.release && self.defaults.contains(&entry.runtime)
            }) {
                return Ok(runtime.clone());
            }
            let release_root = managed_path(
                store.releases_root(),
                release_directory_names(&release.release, release.digest()),
            )?;
            let sealed_release_root = ArtifactStore::execution_release_path(cache_parent, release)?;
            let shared_runtime = {
                // Reuse only a verified managed copy of this exact release.
                // External installations share version keys but lack digest evidence.
                self.environments
                    .values()
                    .filter_map(|entry| entry.runtime.as_ref().ok())
                    .find(|entry| {
                        entry.runtime == release.release
                            && entry.files.paths().all(|path| {
                                path.starts_with(&release_root)
                                    || path.starts_with(&sealed_release_root)
                            })
                    })
            }
            .cloned();
            match shared_runtime {
                Some(runtime) => Ok(runtime),
                None => runtime_from_source(
                    store,
                    release,
                    RuntimeSource::Store(&release_root),
                    cache_parent,
                ),
            }
        })();
        // State support belongs to the qualified binding, not the shared
        // executable: the same CH release also serves historical cold-only
        // environments. Never mutate the installation reused by another binding.
        let runtime = runtime.map(|mut runtime| {
            if runtime.runtime.kind == RuntimeKind::CloudHypervisor {
                runtime.state_format = environment
                    .definition()
                    .descriptor()
                    .spec()
                    .compatibility
                    .state_format()
                    .cloned();
            }
            runtime
        });
        let runtime = runtime.map_err(|error| {
            tracing::warn!(%error, "environment runtime unavailable");
            let error = self.binding_error(error);
            self.unavailable.push(UnavailableArtifact::Runtime {
                identity: release.release.clone(),
                error: error.clone(),
            });
            error
        });
        let mut watched = binding_paths(store, cache_parent, release, kernel);
        let component = kernel
            .map(|kernel| {
                let shared_kernel = self
                    .components
                    .iter()
                    .find(|entry| {
                        entry.default_for_new && entry.metadata.digest == Some(kernel.sha256)
                    })
                    .cloned();
                match shared_kernel {
                    Some(kernel) => Ok(kernel),
                    None => {
                        let kernel_root = managed_path(
                            store.kernels_root(),
                            kernel_directory_names(&kernel.version, kernel.sha256),
                        )
                        .map_err(|error| self.binding_error(error))?;
                        watched.push(kernel_root.join(KERNEL_IMAGE));
                        watched.push(
                            ArtifactStore::execution_kernel_path(cache_parent, kernel)
                                .map_err(|error| self.binding_error(error.into()))?,
                        );
                        let component = (|| {
                            let source = kernel_root.join(KERNEL_IMAGE);
                            let image = kernel_from_source(
                                store,
                                kernel,
                                KernelSource::Store(&source),
                                cache_parent,
                            )?;
                            watched.push(image.path.clone());
                            let mut installation = kernel_installation(
                                kernel_metadata(kernel.version.clone(), kernel.sha256)?,
                                image.path,
                                false,
                                false,
                            );
                            if let Some((_, _, format)) = environment
                                .definition()
                                .descriptor()
                                .spec()
                                .boot
                                .components()
                                .find(|(_, component, _)| component.digest == kernel.sha256)
                            {
                                let parsed_format = format.parse().map_err(
                                    |source: pplx_space_common_core::types::ArtifactFormatParseError| {
                                        RuntimeInventoryError::InvalidStaticIdentifier {
                                            field: "boot format",
                                            value: format.to_owned(),
                                            reason: source.to_string(),
                                        }
                                    },
                                )?;
                                installation.metadata.format = Some(parsed_format);
                            }
                            installation.integrity = image.integrity;
                            Ok::<_, RuntimeInventoryError>(installation)
                        })();
                        component.map_err(|error| {
                            tracing::warn!(%error, "environment kernel unavailable");
                            let error = self.binding_error(error);
                            self.unavailable.push(UnavailableArtifact::Kernel {
                                digest: kernel.sha256,
                                legacy: false,
                                error: error.clone(),
                            });
                            error
                        })
                    }
                }
            })
            .transpose();
        let auxiliary =
            self.environment_boot_artifacts(store, environment, cache_parent, &mut watched);
        // A failed managed binding cannot remove or borrow a legacy installation.
        // Keep a failed entry too, so explicit requests cannot fall back to it.
        let installed = InstalledEnvironment {
            runtime: runtime.map(|mut runtime| {
                runtime.guest = environment.definition().descriptor().spec().guest;
                runtime
            }),
            components: component.and_then(|component| {
                auxiliary.map(|mut components| {
                    if let Some(component) = component {
                        components.insert(0, component);
                    }
                    components
                })
            }),
        };
        self.environments
            .insert(environment.definition().reference(), installed);
        let previous = std::mem::take(&mut self.fingerprints);
        self.remember_paths(watched);
        self.fingerprints.extend(previous);
        Ok(())
    }

    fn environment_boot_artifacts(
        &mut self,
        store: &ArtifactStore,
        environment: &crate::catalog::CompiledEnvironment,
        cache_parent: &Path,
        watched: &mut Vec<PathBuf>,
    ) -> Result<Vec<ArtifactComponentInstallation>, std::sync::Arc<RuntimeInventoryError>> {
        let mut components = Vec::new();
        let mut failure = None;
        for (role, identity, format) in environment
            .definition()
            .descriptor()
            .spec()
            .boot
            .components()
        {
            if identity.kind == ArtifactComponentKind::Kernel {
                continue;
            }
            // Watch every input even if an earlier one is absent or corrupt, so
            // publishing or repairing it invalidates the failed binding.
            let root = store.boot_artifacts_root();
            let image = root
                .join(crate::artifact_store::boot_artifact_directory(identity))
                .join(crate::artifact_store::BOOT_ARTIFACT_IMAGE);
            watched.extend(image.ancestors().map(Path::to_path_buf));
            let installed = (|| {
                let pin = environment.boot_artifact(identity).ok_or_else(|| {
                    RuntimeInventoryError::MissingBootArtifactPin(identity.clone())
                })?;
                let cached = ArtifactStore::execution_boot_artifact_path(cache_parent, pin);
                watched.extend(cached.ancestors().map(Path::to_path_buf));
                let staged = boot_artifact_from_source(store, pin, cache_parent)?;
                Ok::<_, RuntimeInventoryError>(ArtifactComponentInstallation {
                    metadata: ArtifactComponentMetadata {
                        kind: identity.kind,
                        role: Some(role.parse().map_err(
                            |error: ArtifactComponentRoleParseError| {
                                RuntimeInventoryError::InvalidStaticIdentifier {
                                    field: "boot role",
                                    value: role.to_owned(),
                                    reason: error.to_string(),
                                }
                            },
                        )?),
                        version: Some(identity.version.clone()),
                        digest: Some(identity.digest),
                        format: Some(format.parse().map_err(
                            |error: ArtifactFormatParseError| {
                                RuntimeInventoryError::InvalidStaticIdentifier {
                                    field: "boot format",
                                    value: format.to_owned(),
                                    reason: error.to_string(),
                                }
                            },
                        )?),
                    },
                    path: staged.path,
                    integrity: staged.integrity,
                    default_for_new: false,
                    legacy_default: false,
                })
            })();
            match installed {
                Ok(component) => components.push(component),
                Err(error) => {
                    tracing::warn!(%error, "environment boot artifact unavailable");
                    let error = self.binding_error(error);
                    if failure.is_none() || error.is_transient() {
                        failure = Some(error);
                    }
                }
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(components),
        }
    }

    fn binding_error(
        &mut self,
        error: RuntimeInventoryError,
    ) -> std::sync::Arc<RuntimeInventoryError> {
        if error.is_transient() {
            // Inventory queries retry recoverable staging I/O at most once per
            // interval. Changed files/permissions trigger an earlier rebuild.
            self.retry_after =
                Some(tokio::time::Instant::now() + std::time::Duration::from_secs(10));
        }
        std::sync::Arc::new(error)
    }

    /// A custom or unresolved external legacy default cannot be reinterpreted
    /// as E0. Empty managed-only nodes and stock-image defaults have no ambiguity.
    fn stock_legacy_binding(
        &self,
        initial: &InitialEnvironment,
        assets: &Path,
        kernels: &Path,
    ) -> bool {
        let legacy: Vec<_> = self
            .components
            .iter()
            .filter(|entry| entry.legacy_default)
            .collect();
        let explicit_marker = kernels.join("legacy-default");
        let broken_legacy =
            std::fs::symlink_metadata(&explicit_marker).is_ok() && !explicit_marker.is_dir();
        let stock = !broken_legacy
            && if legacy.is_empty() {
                self.components.is_empty() && [kernels.join("legacy-default"), kernels.join("default"), assets.join("default/kernel/vmlinux.bin")]
                .iter().all(|path| matches!(std::fs::symlink_metadata(path), Err(error) if error.kind() == std::io::ErrorKind::NotFound))
            } else {
                legacy
                    .iter()
                    .all(|entry| entry.metadata.digest == Some(initial.kernel().sha256))
            };
        if !stock {
            tracing::warn!(
                "external legacy kernel binding is not stock E0; unannotated existing boots/restores require an explicit kernel identity before migration"
            );
        }
        stock
    }

    fn add_initial_kernel(
        &mut self,
        initial: &InitialEnvironment,
        staged: StagedBootFile,
    ) -> Result<(), RuntimeInventoryError> {
        let mut installation = kernel_installation(
            kernel_metadata(initial.kernel().version.clone(), initial.kernel().sha256)?,
            staged.path,
            true,
            true,
        );
        installation.integrity = staged.integrity;
        self.components.push(installation);
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum RuntimeSource<'a> {
    Store(&'a Path),
    Baked(&'a Path),
}

impl<'a> RuntimeSource<'a> {
    fn path(self) -> &'a Path {
        match self {
            Self::Store(path) | Self::Baked(path) => path,
        }
    }
}

fn runtime_from_source(
    store: &ArtifactStore,
    pin: &crate::catalog::PinnedRelease,
    source: RuntimeSource<'_>,
    cache_parent: &Path,
) -> Result<RuntimeInstallation, RuntimeInventoryError> {
    if pin
        .binaries
        .iter()
        .try_fold(true, |present, file| {
            crate::catalog::compiled_verity_digest(file.sha256).map(|pin| present && pin.is_some())
        })
        .map_err(crate::artifact_store::StageExecutionError::Catalog)?
    {
        match store.stage_release_at(pin, source.path(), cache_parent) {
            Ok(root) => {
                let mut runtime = DRIVERS
                    .get(pin.release.kind)?
                    .inspect_installation(root, pin.release.version.clone())?;
                runtime.files.integrity = ExecutionSourceIntegrity::FsVerity;
                return Ok(runtime);
            }
            Err(error) if error.is_unsupported() => {
                tracing::warn!(integrity = "catalog-sha256", %error, "fs-verity unavailable; using catalog SHA-256 authentication");
                if pin.release.kind == RuntimeKind::CloudHypervisor {
                    let root = store.stage_catalog_release_at(pin, source.path(), cache_parent)?;
                    let mut runtime = DRIVERS
                        .get(pin.release.kind)?
                        .inspect_installation(root, pin.release.version.clone())?;
                    runtime.files.integrity = ExecutionSourceIntegrity::CatalogSha256;
                    return Ok(runtime);
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
    match source {
        RuntimeSource::Store(path) => store.verify_managed_release_at(pin, path)?,
        RuntimeSource::Baked(path) => store.verify_release_at(pin, path)?,
    };
    let mut runtime = DRIVERS
        .get(pin.release.kind)?
        .inspect_installation(source.path().to_owned(), pin.release.version.clone())?;
    runtime.files.integrity = ExecutionSourceIntegrity::CatalogSha256;
    Ok(runtime)
}

struct StagedBootFile {
    path: PathBuf,
    integrity: ExecutionSourceIntegrity,
}

fn boot_artifact_from_source(
    store: &ArtifactStore,
    pin: &crate::catalog::PinnedBootArtifact,
    cache_parent: &Path,
) -> Result<StagedBootFile, RuntimeInventoryError> {
    if crate::catalog::compiled_verity_digest(pin.identity().digest)
        .map_err(crate::artifact_store::StageExecutionError::Catalog)?
        .is_some()
    {
        match store.stage_sealed_boot_artifact(pin, cache_parent) {
            Ok(path) => {
                return Ok(StagedBootFile {
                    path,
                    integrity: ExecutionSourceIntegrity::FsVerity,
                });
            }
            Err(error) if error.is_unsupported() => {
                tracing::warn!(integrity = "catalog-sha256", %error, "boot artifact fs-verity unavailable; using catalog SHA-256 authentication");
            }
            Err(error) => return Err(error.into()),
        }
    }
    let publication = store.stage_boot_artifact(pin, cache_parent)?;
    Ok(StagedBootFile {
        path: publication
            .path
            .join(crate::artifact_store::BOOT_ARTIFACT_IMAGE),
        integrity: ExecutionSourceIntegrity::CatalogSha256,
    })
}

fn kernel_from_source(
    store: &ArtifactStore,
    pin: &crate::catalog::PinnedKernel,
    source: KernelSource<'_>,
    cache_parent: &Path,
) -> Result<StagedBootFile, RuntimeInventoryError> {
    if crate::catalog::compiled_verity_digest(pin.sha256)
        .map_err(crate::artifact_store::StageExecutionError::Catalog)?
        .is_some()
    {
        match store.stage_sealed_kernel_at(pin, source, cache_parent) {
            Ok(path) => {
                return Ok(StagedBootFile {
                    path,
                    integrity: ExecutionSourceIntegrity::FsVerity,
                });
            }
            Err(error) if error.is_unsupported() => {
                tracing::warn!(integrity = "catalog-sha256", %error, "kernel fs-verity unavailable; using catalog SHA-256 authentication");
            }
            Err(error) => return Err(error.into()),
        }
    }
    let path = match source {
        KernelSource::Store(_) => {
            let cache = store.prepare_execution_cache(cache_parent)?;
            store.stage_kernel(pin, &cache)?.path.join(KERNEL_IMAGE)
        }
        KernelSource::ExecutionCache(image) => {
            store.verify_kernel_at(pin, image)?;
            image.to_owned()
        }
    };
    Ok(StagedBootFile {
        path,
        integrity: ExecutionSourceIntegrity::CatalogSha256,
    })
}

#[cfg(test)]
mod tests;

fn managed_path(root: &Path, names: [String; 2]) -> Result<PathBuf, RuntimeInventoryError> {
    existing_artifact_path(root, &names).map_err(|source| {
        RuntimeInventoryError::ProbeInventoryPath {
            path: root.to_owned(),
            source,
        }
    })
}

// Watch both published layouts even when a locator is unreadable. Repair of one
// component must refresh its binding without withdrawing unrelated components.
fn binding_paths(
    store: &ArtifactStore,
    cache_parent: &Path,
    release: &crate::catalog::PinnedRelease,
    kernel: Option<&crate::catalog::PinnedKernel>,
) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    for root in [
        store.releases_root().to_owned(),
        cache_parent.join("managed/releases"),
    ] {
        for name in release_directory_names(&release.release, release.digest()) {
            for binary in &release.binaries {
                paths.push(root.join(&name).join(binary.path));
            }
        }
    }
    let Some(kernel) = kernel else { return paths };
    for root in [
        store.kernels_root().to_owned(),
        cache_parent.join("managed/kernels"),
    ] {
        for name in kernel_directory_names(&kernel.version, kernel.sha256) {
            paths.push(root.join(name).join(KERNEL_IMAGE));
        }
    }
    paths
}
