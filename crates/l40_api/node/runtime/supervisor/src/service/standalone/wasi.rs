//! Translate an admitted Python/WASI boot contract, never caller-supplied
//! executable arguments. Catalog qualification remains an independent gate.
use super::*;
use pplx_space_execution_spec::{ArtifactComponentKind, BootProfile, ComponentIdentity};
use pplx_space_node_runtime_core::{artifact_store::ArtifactStore, catalog::PinnedBootArtifact};

pub(super) fn entrypoint(
    request: &LaunchRuntimeRequest,
    environment: &CompiledEnvironment,
    release_root: &Path,
    isolation_root: &Path,
) -> Result<crate::contained_launcher::Entrypoint, Status> {
    if !request.configuration_path.is_empty()
        || !request.log_path.is_empty()
        || !request.metrics_path.is_empty()
    {
        return Err(Status::invalid_argument(
            "WASI launch does not accept VM configuration, log or metrics paths",
        ));
    }
    let (module, library) = boot_inputs(
        &environment.definition().descriptor().spec().boot,
        |identity| environment.boot_artifact(identity).cloned(),
    )?;
    // Auxiliary files and the executable must belong to the same node-managed
    // execution cache. A download/staging directory is not an execution source.
    let releases = release_root
        .parent()
        .filter(|path| path.file_name() == Some(std::ffi::OsStr::new("releases")))
        .ok_or_else(|| Status::invalid_argument("WASI engine is not in an execution cache"))?;
    let managed = releases
        .parent()
        .filter(|path| path.file_name() == Some(std::ffi::OsStr::new("managed")))
        .ok_or_else(|| Status::invalid_argument("WASI engine is not in an execution cache"))?;
    let cache = managed
        .parent()
        .ok_or_else(|| Status::invalid_argument("missing execution cache parent"))?;
    let slot = isolation_root
        .parent()
        .ok_or_else(|| Status::invalid_argument("missing WASI slot directory"))?;
    // Required attachment point for the owning managed volume, outside the
    // immutable native root. The launcher opens it with the slot UID/GID.
    let workspace = slot.join("workspace");
    let limits = request
        .resource_limits
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("WASI launch requires resource limits"))?;
    let memory_mib = limits
        .memory_mib
        .ok_or_else(|| Status::invalid_argument("WASI launch requires a memory limit"))?;
    let memory_bytes = memory_mib
        .checked_mul(1024 * 1024)
        .ok_or_else(|| Status::invalid_argument("WASI memory limit overflows"))?;
    // Use the process adapter's owning validator rather than a second range.
    pplx_space_spaced_guest_server::platform::current::WasiProcessProfile::new(
        "/opt/space-wasi/bin/wasmtime".into(),
        "/opt/space-wasi/python/python.wasm".into(),
        "/workspace".into(),
        "/opt/space-wasi/python".into(),
        memory_bytes,
    )
    .map_err(|error| Status::invalid_argument(error.to_string()))?;
    Ok(crate::contained_launcher::Entrypoint::WasiGuest(Box::new(
        crate::contained_launcher::wasi_guest::Config {
            module: ArtifactStore::execution_boot_artifact_path(cache, &module),
            module_source: source_pins(&module)?,
            standard_library: ArtifactStore::execution_boot_artifact_path(cache, &library),
            standard_library_source: source_pins(&library)?,
            workspace,
            memory_bytes,
        },
    )))
}

fn source_pins(pin: &PinnedBootArtifact) -> Result<SourcePins, Status> {
    Ok(SourcePins {
        sha256: pin.identity().digest,
        verity: compiled_verity_digest(pin.identity().digest)
            .map_err(|error| Status::failed_precondition(error.to_string()))?
            .ok_or_else(|| {
                Status::failed_precondition("WASI boot input has no trusted verity measurement")
            })?,
    })
}

fn boot_inputs(
    boot: &BootProfile,
    lookup: impl Fn(&ComponentIdentity) -> Option<PinnedBootArtifact>,
) -> Result<(PinnedBootArtifact, PinnedBootArtifact), Status> {
    let unsupported = || Status::failed_precondition("unsupported WASI Python boot contract");
    let BootProfile::ArtifactsV1 {
        contract,
        artifacts,
    } = boot
    else {
        return Err(unsupported());
    };
    if contract.as_str() != "wasi-python-v1" || artifacts.len() != 2 {
        return Err(unsupported());
    }
    let mut module = None;
    let mut library = None;
    for artifact in artifacts {
        let destination = match (
            artifact.role().as_str(),
            artifact.component().kind,
            artifact.format().as_str(),
        ) {
            ("module", ArtifactComponentKind::WasmModule, "wasip1-command") => &mut module,
            ("standard-library", ArtifactComponentKind::GuestTools, "cpython315-zip") => {
                &mut library
            }
            _ => return Err(unsupported()),
        };
        let pin = lookup(artifact.component()).ok_or_else(unsupported)?;
        if pin.identity() != artifact.component() || destination.replace(pin).is_some() {
            return Err(unsupported());
        }
    }
    Ok((
        module.ok_or_else(unsupported)?,
        library.ok_or_else(unsupported)?,
    ))
}

#[cfg(test)]
mod tests;
