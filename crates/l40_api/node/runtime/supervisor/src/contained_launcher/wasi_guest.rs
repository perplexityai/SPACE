//! Native agent entrypoint inside the sandbox namespaces. Only the engine is
//! native task execution infrastructure; every admitted task command still
//! goes through the ordinary spaced Process service and its WASI adapter.
//!
//! This is a launch mechanism, not catalog or volume admission. Its caller must
//! derive this specification from an admitted execution and an attached volume.

use std::{
    fs::File,
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
};

use nix::mount::{MsFlags, mount};
use pplx_space_node_runtime_core::authenticated_source::{AuthenticatedFile, SourcePins};
use pplx_space_spaced_guest_server::platform::current::{
    FilesystemExecutionMode, HostConnectorType, SpacedGuestRunConfig, SpacedGuestServiceConfig,
    WasiProcessProfile,
};

use super::linux::{Error, io, sys};

const ENGINE: &str = "/opt/space-wasi/bin/wasmtime";
const MODULE: &str = "/opt/space-wasi/python/python.wasm";
const PYTHON_HOME: &str = "/opt/space-wasi/python";
const STDLIB: &str = "/opt/space-wasi/python/lib/python315.zip";
const WORKSPACE: &str = "/workspace";

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    pub module: PathBuf,
    pub module_source: SourcePins,
    /// CPython's standard-library zip, not a writable package directory.
    pub standard_library: PathBuf,
    pub standard_library_source: SourcePins,
    pub workspace: PathBuf,
    pub memory_bytes: u64,
}

pub(super) struct Prepared {
    profile: WasiProcessProfile,
}

impl Prepared {
    /// Called while single-threaded, in the new mount namespace and before
    /// pivot_root. No host code or arbitrary directory is selected by task argv.
    pub(super) fn prepare(
        config: &Config,
        launch: &super::Config,
        registration: &File,
    ) -> Result<Self, Error> {
        let profile = WasiProcessProfile::new(
            ENGINE.into(),
            MODULE.into(),
            WORKSPACE.into(),
            PYTHON_HOME.into(),
            config.memory_bytes,
        )
        .map_err(Error::WasiProfile)?;
        // The initial executable admission predates CLONE_NEWNS. Bind mounts
        // cannot use a source descriptor referring to that old mount tree.
        // Authenticate the descriptor opened in this namespace, then retain it
        // through the bind rather than reopening an unauthenticated pathname.
        let engine = crate::verified_launch::open_catalog_executable(
            &launch.executable,
            launch.executable_source,
        )
        .map_err(Error::Authentication)?;
        let module = AuthenticatedFile::open_catalog(&config.module, config.module_source)
            .map_err(Error::GuestInput)?;
        let library = AuthenticatedFile::open_catalog(
            &config.standard_library,
            config.standard_library_source,
        )
        .map_err(Error::GuestInput)?;
        let workspace = open_workspace(&config.workspace, launch.uid, launch.gid)?;

        // All mount targets are created afresh beneath the pinned root. The
        // root is protected against the sandbox identity before this is called.
        for directory in [
            "opt",
            "opt/space-wasi",
            "opt/space-wasi/bin",
            "opt/space-wasi/python",
            "opt/space-wasi/python/lib",
            "etc",
            "workspace",
            "run",
        ] {
            std::fs::create_dir(directory).map_err(io("create native guest directory"))?;
            std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o755))
                .map_err(io("protect native guest directory"))?;
        }
        bind_file(engine.file(), ENGINE.trim_start_matches('/'))?;
        bind_file(module.file(), MODULE.trim_start_matches('/'))?;
        bind_file(library.file(), STDLIB.trim_start_matches('/'))?;

        // Native runtime dependencies come only from the host's protected
        // system library trees. Never use the tenant's Linux rootfs as a loader
        // or a source of native shared objects.
        for directory in ["/lib", "/lib64", "/usr/lib", "/usr/lib64"] {
            let source = match std::fs::canonicalize(directory) {
                Ok(source) => source,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(io("resolve native library root")(error)),
            };
            pplx_space_node_runtime_core::artifact_store::verify_execution_directory(&source, 0)
                .map_err(Error::InputDirectory)?;
            let source = open_directory(&source)?;
            let destination = directory.trim_start_matches('/');
            if let Some(parent) = Path::new(destination).parent()
                && !parent.as_os_str().is_empty()
                && !parent.exists()
            {
                std::fs::create_dir(parent).map_err(io("create native library parent"))?;
                std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o755))
                    .map_err(io("protect native library parent"))?;
            }
            std::fs::create_dir(destination).map_err(io("create native library mountpoint"))?;
            bind_directory(&source, destination, true)?;
        }
        bind_directory(&workspace, "workspace", false)?;

        // The native guest has the slot's real unprivileged identity. Do not
        // manufacture a root account; identity workers retain the same account.
        std::fs::write(
            "etc/passwd",
            format!(
                "sandbox:x:{}:{}:WASI guest:/workspace:/nonexistent\n",
                launch.uid, launch.gid
            ),
        )
        .map_err(io("write contained identity"))?;
        std::fs::write("etc/group", format!("sandbox:x:{}:\n", launch.gid))
            .map_err(io("write contained group"))?;
        for path in ["etc/passwd", "etc/group"] {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o444))
                .map_err(io("protect contained identity"))?;
        }

        // Share only a dedicated child of the owning registration directory.
        // PID links and other native sidecars stay outside the guest root.
        // Both bridge listeners can appear after the directory is mounted.
        let namespace_registration = open_directory(&launch.registration_directory)?;
        let original = registration
            .metadata()
            .map_err(io("inspect admitted registration directory"))?;
        let reopened = namespace_registration
            .metadata()
            .map_err(io("inspect namespace registration directory"))?;
        if (original.dev(), original.ino()) != (reopened.dev(), reopened.ino()) {
            return Err(Error::Invalid(
                "registration directory changed during namespace setup",
            ));
        }
        let guest_run = format!(
            "/proc/self/fd/{}/wasi-guest",
            namespace_registration.as_raw_fd()
        );
        let guest_run = Path::new(&guest_run);
        std::fs::create_dir(guest_run).map_err(io("create native guest socket directory"))?;
        nix::unistd::chown(
            guest_run,
            Some(nix::unistd::Uid::from_raw(launch.uid)),
            Some(nix::unistd::Gid::from_raw(launch.gid)),
        )
        .map_err(sys("assign native guest socket directory"))?;
        std::fs::set_permissions(guest_run, std::fs::Permissions::from_mode(0o700))
            .map_err(io("protect native guest socket directory"))?;
        let guest_run = open_directory(guest_run)?;
        bind_directory(&guest_run, "run", false)?;
        Ok(Self { profile })
    }

    pub(super) fn run(self) -> Result<(), Error> {
        // Launcher mode has never initialized Tokio or telemetry. This is the
        // first runtime in the namespace child; no multithreaded fork occurs.
        pplx_space_spaced_guest_server::platform::current::run(SpacedGuestRunConfig {
            service_config: Some(SpacedGuestServiceConfig {
                wasi_process_profile: Some(self.profile),
                filesystem_execution: FilesystemExecutionMode::IdentityWorkers,
                default_operation_user: "sandbox"
                    .parse()
                    .map_err(|_| Error::Invalid("invalid compiled guest operation user"))?,
                default_working_dir: Some(WORKSPACE.into()),
                local_socket_path: None,
                bridge_socket_path: Some("/run/bridge-guest.sock".into()),
                host_connector_type: HostConnectorType::Uds("/run/bridge-host.sock".into()),
                ..Default::default()
            }),
        })
        .map_err(Error::WasiGuest)
    }
}

fn open_directory(path: &Path) -> Result<File, Error> {
    File::options()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(io("pin native guest directory"))
}

fn open_workspace(path: &Path, uid: u32, gid: u32) -> Result<File, Error> {
    let path = pplx_space_common_core::types::NodeSpacePath::parse(path)
        .map_err(|_| Error::Invalid("workspace must be an absolute canonical locator"))?;
    let parent = path
        .as_path()
        .parent()
        .ok_or(Error::Invalid("workspace must have a protected parent"))?;
    pplx_space_node_runtime_core::artifact_store::verify_execution_directory(parent, 0)
        .map_err(Error::InputDirectory)?;
    let file = open_directory(path.as_path())?;
    let metadata = file
        .metadata()
        .map_err(io("inspect native guest workspace"))?;
    if metadata.uid() != uid || metadata.gid() != gid || metadata.mode() & 0o007 != 0 {
        return Err(Error::Invalid(
            "workspace does not belong to the sandbox identity",
        ));
    }
    Ok(file)
}

fn bind_file(source: &File, destination: &str) -> Result<(), Error> {
    let target = File::options()
        .write(true)
        .create_new(true)
        .mode(0o444)
        .open(destination)
        .map_err(io("create native guest tool mountpoint"))?;
    bind(source, &target, destination, true)
}

fn bind_directory(source: &File, destination: &str, readonly: bool) -> Result<(), Error> {
    let target = open_directory(Path::new(destination))?;
    bind(source, &target, destination, readonly)
}

fn bind(source: &File, target: &File, destination: &str, readonly: bool) -> Result<(), Error> {
    let source = format!("/proc/self/fd/{}", source.as_raw_fd());
    let target = format!("/proc/self/fd/{}", target.as_raw_fd());
    mount(
        Some(source.as_str()),
        target.as_str(),
        None::<&str>,
        MsFlags::MS_BIND,
        None::<&str>,
    )
    .map_err(sys("mount pinned native guest input"))?;
    // A descriptor opened before mount refers to the old mount tree. Reopen
    // beneath the protected root to apply policy to the new bind, not its
    // covered inode.
    let mounted = File::options()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(destination)
        .map_err(io("pin mounted native guest input"))?;
    let target = format!("/proc/self/fd/{}", mounted.as_raw_fd());
    let mut flags = MsFlags::MS_BIND | MsFlags::MS_REMOUNT | MsFlags::MS_NOSUID | MsFlags::MS_NODEV;
    if readonly {
        flags |= MsFlags::MS_RDONLY;
    } else {
        flags |= MsFlags::MS_NOEXEC;
    }
    mount::<str, str, str, str>(None, target.as_str(), None, flags, None)
        .map_err(sys("protect native guest input mount"))
}

#[cfg(test)]
mod tests;
