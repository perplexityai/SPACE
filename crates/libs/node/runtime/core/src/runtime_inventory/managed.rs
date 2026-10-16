use super::*;
use crate::artifact_store::{
    ArtifactStore, KERNEL_IMAGE, managed_kernel_directory, managed_release_directory,
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
        let mut watched = vec![
            store.releases_root().to_path_buf(),
            store.kernels_root().to_path_buf(),
            cache_parent.to_path_buf(),
            managed_release.join("bin/firecracker"),
            managed_release.join("bin/jailer"),
            baked_release.join("bin/firecracker"),
            baked_release.join("bin/jailer"),
            managed_kernel.join(KERNEL_IMAGE),
            cached_kernel,
            kernels
                .join(initial.kernel().version.as_str())
                .join(KERNEL_IMAGE),
            kernels.join("default").join(KERNEL_IMAGE),
            kernels.join("legacy-default").join(KERNEL_IMAGE),
            assets.join("default/kernel/vmlinux.bin"),
            vendor.join("default"),
        ];
        watched.extend(inventory.components.iter().map(|entry| entry.path.clone()));
        // A broken preferred copy never silently selects a fallback copy.
        inventory
            .runtimes
            .retain(|entry| entry.runtime != initial.release().release);
        let release = (|| {
            if probe_inventory_path(&managed_release)?.is_some() {
                store.verify_release(initial.release())?;
                inventory.add_initial_release(initial, &managed_release)?;
            } else if probe_inventory_path(&baked_release)?.is_some() {
                store.verify_release_at(initial.release(), &baked_release)?;
                inventory.add_initial_release(initial, &baked_release)?;
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
            if probe_inventory_path(&managed_kernel)?.is_some() {
                let cache = store.prepare_execution_cache(cache_parent)?;
                let staged = store.stage_kernel(initial.kernel(), &cache)?;
                inventory.add_initial_kernel(initial, staged.path.join(KERNEL_IMAGE))?;
            } else {
                for mut component in initial_kernels {
                    // Legacy assets may be reached through a supported alias.
                    // Verify and retain its real target, while the watch list
                    // above still observes alias replacement and retargeting.
                    let verified = (|| {
                        let target = std::fs::canonicalize(&component.path).map_err(|source| {
                            RuntimeInventoryError::ProbeInventoryPath {
                                path: component.path.clone(),
                                source,
                            }
                        })?;
                        store.verify_kernel_at(initial.kernel(), &target)?;
                        Ok::<_, RuntimeInventoryError>(target)
                    })();
                    match verified {
                        Ok(target) => {
                            component.path = target;
                            inventory.components.push(component);
                        }
                        Err(error) => {
                            tracing::warn!(%error, "baked E0 kernel copy unavailable");
                            let error = inventory.binding_error(error);
                            inventory.unavailable.push(UnavailableArtifact::Kernel {
                                digest: initial.kernel().sha256,
                                legacy: stock_legacy,
                                error,
                            });
                        }
                    }
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

    /// Include a complete pinned environment; never change new/legacy defaults.
    /// Missing or invalid environment bytes withdraw that binding, not E0.
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
                    .filter_map(|entry| entry.as_ref().ok())
                    .map(|entry| &entry.runtime)
                    .find(|entry| {
                        entry.runtime == release.release
                            && entry
                                .files
                                .paths()
                                .all(|path| path.starts_with(&release_root))
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
        let cache_parent = kernels
            .parent()
            .ok_or_else(|| RuntimeInventoryError::ParentlessKernelRoot(kernels.to_path_buf()))?;
        let mut watched = vec![
            release_root.join("bin/firecracker"),
            release_root.join("bin/jailer"),
            kernel_root.join(KERNEL_IMAGE),
            ArtifactStore::execution_kernel_path(cache_parent, kernel),
        ];
        let runtime = match shared_runtime {
            Some(runtime) => Ok(runtime),
            None => {
                let runtime = (|| {
                    store.verify_release(release)?;
                    FirecrackerDriver
                        .inspect_installation(release_root, release.release.version.clone())
                })();
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
                    let cache = store.prepare_execution_cache(cache_parent)?;
                    let staged = store.stage_kernel(kernel, &cache)?;
                    let image = staged.path.join(KERNEL_IMAGE);
                    watched.push(image.clone());
                    Ok::<_, RuntimeInventoryError>(kernel_installation(
                        kernel_metadata(kernel.version.clone(), kernel.sha256)?,
                        image,
                        false,
                        false,
                    ))
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
        let installed = match (runtime, component) {
            (Ok(runtime), Ok(component)) => Ok(InstalledEnvironment {
                runtime,
                components: vec![component],
            }),
            (Err(error), _) | (_, Err(error)) => Err(error),
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

    fn add_initial_release(
        &mut self,
        initial: &InitialEnvironment,
        root: &Path,
    ) -> Result<(), RuntimeInventoryError> {
        let installation = FirecrackerDriver.inspect_installation(
            root.to_path_buf(),
            initial.release().release.version.clone(),
        )?;
        self.runtimes.push(installation);
        Ok(())
    }

    fn add_initial_kernel(
        &mut self,
        initial: &InitialEnvironment,
        path: PathBuf,
    ) -> Result<(), RuntimeInventoryError> {
        self.components.push(kernel_installation(
            kernel_metadata(initial.kernel().version.clone(), initial.kernel().sha256)?,
            path,
            true,
            true,
        ));
        Ok(())
    }
}

#[cfg(test)]
mod tests;
