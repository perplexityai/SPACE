use super::*;
use crate::artifact_store::{
    ArtifactStore, KERNEL_IMAGE, KernelSource, managed_kernel_directory, managed_release_directory,
    release_directory,
};
use crate::catalog::InitialEnvironment;

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

        let managed_release = store.releases_root().join(managed_release_directory(
            &initial.release().release,
            initial.release().digest(),
        ));
        let baked_release = vendor.join(release_directory(&initial.release().release));
        let managed_kernel = store.kernels_root().join(managed_kernel_directory(
            &initial.kernel().version,
            initial.kernel().sha256,
        ));
        let cache_parent = kernels
            .parent()
            .ok_or_else(|| RuntimeInventoryError::ParentlessKernelRoot(kernels.to_path_buf()))?;
        let cached_kernel = ArtifactStore::execution_kernel_path(cache_parent, initial.kernel());
        let cached_release = ArtifactStore::execution_release_path(cache_parent, initial.release());
        let mut watched = vec![
            store.releases_root().to_path_buf(),
            store.kernels_root().to_path_buf(),
            cache_parent.to_path_buf(),
            managed_release.join("bin/firecracker"),
            managed_release.join("bin/jailer"),
            baked_release.join("bin/firecracker"),
            baked_release.join("bin/jailer"),
            cached_release.join("bin/firecracker"),
            cached_release.join("bin/jailer"),
            managed_kernel.join(KERNEL_IMAGE),
            cached_kernel.clone(),
            kernels
                .join(initial.kernel().version.as_str())
                .join(KERNEL_IMAGE),
            kernels.join("default").join(KERNEL_IMAGE),
            kernels.join("legacy-default").join(KERNEL_IMAGE),
            assets.join("default/kernel/vmlinux.bin"),
            vendor.join("default"),
        ];
        // Protected consumers check the full ancestry; repairing any rejected
        // ancestor must invalidate the cached observation too.
        watched.extend(cache_parent.ancestors().map(Path::to_path_buf));
        watched.extend(inventory.components.iter().map(|entry| entry.path.clone()));
        // A broken preferred copy never silently selects a fallback copy.
        inventory
            .runtimes
            .retain(|entry| entry.runtime != initial.release().release);
        let release = (|| {
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
        let release_root = store.releases_root().join(managed_release_directory(
            &release.release,
            release.digest(),
        ));
        let cache_parent = kernels
            .parent()
            .ok_or_else(|| RuntimeInventoryError::ParentlessKernelRoot(kernels.to_path_buf()))?;
        let sealed_release_root = ArtifactStore::execution_release_path(cache_parent, release);
        let shared_runtime = self
            .runtimes
            .iter()
            .find(|entry| {
                entry.runtime == release.release && self.defaults.contains(&entry.runtime)
            })
            .or_else(|| {
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
            })
            .cloned();
        let shared_kernel = self
            .components
            .iter()
            .find(|entry| entry.default_for_new && entry.metadata.digest == Some(kernel.sha256))
            .cloned();
        let kernel_root = store
            .kernels_root()
            .join(managed_kernel_directory(&kernel.version, kernel.sha256));
        let mut watched = vec![
            release_root.join("bin/firecracker"),
            release_root.join("bin/jailer"),
            sealed_release_root.join("bin/firecracker"),
            sealed_release_root.join("bin/jailer"),
            kernel_root.join(KERNEL_IMAGE),
            ArtifactStore::execution_kernel_path(cache_parent, kernel),
        ];
        let runtime = match shared_runtime {
            Some(runtime) => Ok(runtime),
            None => {
                let runtime = runtime_from_source(
                    store,
                    release,
                    RuntimeSource::Store(&release_root),
                    cache_parent,
                );
                runtime.map_err(|error| {
                    tracing::warn!(%error, "environment runtime unavailable");
                    let error = self.binding_error(error);
                    self.unavailable.push(UnavailableArtifact::Runtime {
                        identity: release.release.clone(),
                        error: error.clone(),
                    });
                    error
                })
            }
        };
        let component = match shared_kernel {
            Some(kernel) => Ok(kernel),
            None => {
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
        };
        // A failed managed binding cannot remove or borrow a legacy installation.
        // Keep a failed entry too, so explicit requests cannot fall back to it.
        let installed = InstalledEnvironment {
            runtime,
            components: component.map(|component| vec![component]),
        };
        self.environments
            .insert(environment.definition().reference(), installed);
        let previous = std::mem::take(&mut self.fingerprints);
        self.remember_paths(watched);
        self.fingerprints.extend(previous);
        Ok(())
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
        staged: StagedKernel,
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
                let mut runtime =
                    FirecrackerDriver.inspect_installation(root, pin.release.version.clone())?;
                runtime.files.integrity = ExecutionSourceIntegrity::FsVerity;
                return Ok(runtime);
            }
            Err(error) if error.is_unsupported() => {
                tracing::warn!(integrity = "publication-sha256", %error, "fs-verity unavailable; retaining verified legacy execution protection");
            }
            Err(error) => return Err(error.into()),
        }
    }
    match source {
        RuntimeSource::Store(_) => store.verify_release(pin)?,
        RuntimeSource::Baked(path) => store.verify_release_at(pin, path)?,
    };
    FirecrackerDriver.inspect_installation(source.path().to_owned(), pin.release.version.clone())
}

struct StagedKernel {
    path: PathBuf,
    integrity: ExecutionSourceIntegrity,
}

fn kernel_from_source(
    store: &ArtifactStore,
    pin: &crate::catalog::PinnedKernel,
    source: KernelSource<'_>,
    cache_parent: &Path,
) -> Result<StagedKernel, RuntimeInventoryError> {
    if crate::catalog::compiled_verity_digest(pin.sha256)
        .map_err(crate::artifact_store::StageExecutionError::Catalog)?
        .is_some()
    {
        match store.stage_sealed_kernel_at(pin, source, cache_parent) {
            Ok(path) => {
                return Ok(StagedKernel {
                    path,
                    integrity: ExecutionSourceIntegrity::FsVerity,
                });
            }
            Err(error) if error.is_unsupported() => {
                tracing::warn!(integrity = "publication-sha256", %error, "kernel fs-verity unavailable; retaining verified legacy execution protection");
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
    Ok(StagedKernel {
        path,
        integrity: ExecutionSourceIntegrity::Legacy,
    })
}

#[cfg(test)]
mod tests;
