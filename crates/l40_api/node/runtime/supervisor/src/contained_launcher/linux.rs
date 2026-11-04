use super::Config;
use nix::{
    sched::{CloneFlags, setns, unshare},
    sys::{
        signal::Signal,
        stat::{Mode, SFlag, makedev, mknod},
        wait::{WaitStatus, waitpid},
    },
    unistd::{ForkResult, Gid, Uid},
};
use pplx_space_node_runtime_core::authenticated_source::AuthenticatedFile;
use std::{
    ffi::CString,
    fs::File,
    io::Write,
    os::{
        fd::AsRawFd,
        unix::{
            fs::{MetadataExt, OpenOptionsExt},
            net::UnixStream,
        },
    },
    path::Path,
};

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("invalid contained process: {0}")]
    Invalid(&'static str),
    #[error("{operation}: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("{operation}: {source}")]
    System {
        operation: &'static str,
        #[source]
        source: nix::Error,
    },
    #[error("authenticate executable: {0}")]
    Authentication(tonic::Status),
}
fn io(operation: &'static str) -> impl FnOnce(std::io::Error) -> Error {
    move |source| Error::Io { operation, source }
}
fn sys(operation: &'static str) -> impl FnOnce(nix::Error) -> Error {
    move |source| Error::System { operation, source }
}

pub(crate) fn run(config: Config, stream: UnixStream) -> Result<(), String> {
    let mut stream = Some(stream);
    match prepare_and_exec(&config, &mut stream) {
        Ok(()) => Ok(()),
        Err(error) => {
            let error = error.to_string();
            crate::sidecar_launcher::write_error(
                stream.as_mut().expect("child retains status stream"),
                &error,
            )
            .map_err(|report| format!("{error}; report launch failure: {report}"))?;
            Err(error)
        }
    }
}

/// Called only by launcher mode, before Tokio or telemetry creates threads.
fn prepare_and_exec(config: &Config, stream: &mut Option<UnixStream>) -> Result<(), Error> {
    use nix::mount::{MsFlags, mount};
    if pplx_space_common_core::host::linux::gateway::gateway_uid(
        pplx_space_common_core::types::LinuxUid::new(config.uid),
    )
    .is_none()
        || config.uid != config.gid
    {
        return Err(Error::Invalid(
            "runtime must use its unprivileged sandbox identity",
        ));
    }
    let root = File::options()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&config.root)
        .map_err(io("open isolation root"))?;
    let parent = config
        .root
        .parent()
        .ok_or(Error::Invalid("isolation root has no parent"))?;
    pplx_space_node_runtime_core::artifact_store::verify_execution_directory(parent, 0)
        .map_err(|_| Error::Invalid("isolation root has unsafe ancestors"))?;
    if root.metadata().map_err(io("inspect isolation root"))?.uid() != config.uid {
        return Err(Error::Invalid("isolation root is not owned by the sandbox"));
    }
    let executable = crate::verified_launch::open_catalog_executable(
        &config.executable,
        config.executable_source,
    )
    .map_err(Error::Authentication)?;
    let network = File::options()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&config.network_namespace)
        .map_err(io("open network namespace"))?;
    let output = open_runtime_output(&config.output)?;
    let registration = open_registration_directory(&config.registration_directory, config.uid)?;
    let procs = config.cgroup.join("cgroup.procs");
    std::fs::write(procs, "0").map_err(io("join runtime cgroup"))?;
    setns(&network, CloneFlags::CLONE_NEWNET).map_err(sys("enter network namespace"))?;
    drop(network);
    unshare(
        CloneFlags::CLONE_NEWNS
            | CloneFlags::CLONE_NEWIPC
            | CloneFlags::CLONE_NEWUTS
            | CloneFlags::CLONE_NEWPID,
    )
    .map_err(sys("create runtime namespaces"))?;
    // Receive node-manager's cleanup unmounts without propagating child mounts
    // into the host namespace.
    mount::<str, str, str, str>(None, "/", None, MsFlags::MS_REC | MsFlags::MS_SLAVE, None)
        .map_err(sys("isolate mount propagation"))?;
    // An fd opened before CLONE_NEWNS still refers to the old mount tree.
    // Reopen in the cloned namespace and prove it names the pinned directory
    // before pivoting; mounting procfs through an old-tree fd returns EINVAL.
    let contained_root = File::options()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&config.root)
        .map_err(io("open isolation root in mount namespace"))?;
    let original = root
        .metadata()
        .map_err(io("inspect pinned isolation root"))?;
    let contained = contained_root
        .metadata()
        .map_err(io("inspect namespace isolation root"))?;
    if (original.dev(), original.ino()) != (contained.dev(), contained.ino()) {
        return Err(Error::Invalid(
            "isolation root changed during namespace setup",
        ));
    }
    drop(root);
    let root = contained_root;
    nix::unistd::fchdir(&root).map_err(sys("enter pinned isolation root"))?;
    // Make the selected root a mount point, then discard the cloned host tree.
    // Slave propagation on retained mounts still permits host-side cleanup.
    mount(
        Some("."),
        ".",
        None::<&str>,
        MsFlags::MS_BIND | MsFlags::MS_REC,
        None::<&str>,
    )
    .map_err(sys("bind isolation root"))?;
    nix::unistd::chdir(&config.root).map_err(sys("enter root mount"))?;
    let mounted = std::fs::metadata(".").map_err(io("inspect root mount"))?;
    if (mounted.dev(), mounted.ino()) != (contained.dev(), contained.ino()) {
        return Err(Error::Invalid("isolation root changed before pivot"));
    }
    // Create mount points without following guest-controlled entries. Mount via
    // pinned descriptors before dropping the host procfs; never chown an entry
    // in the sandbox-owned tree by pathname.
    let devices = private_mountpoint("dev")?;
    mount(
        Some("tmpfs"),
        format!("/proc/self/fd/{}", devices.as_raw_fd()).as_str(),
        Some("tmpfs"),
        MsFlags::MS_NOSUID | MsFlags::MS_NOEXEC,
        Some("mode=0755,size=64k"),
    )
    .map_err(sys("mount private devices"))?;
    // Reopen the mounted filesystem, then use only that descriptor for device
    // creation. The host-side sandbox UID can rename the mountpoint entry in
    // its own namespace, so privileged operations must not traverse /dev.
    let private_devices = File::options()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open("dev")
        .map_err(io("pin mounted private devices"))?;
    let device_metadata = private_devices
        .metadata()
        .map_err(io("inspect private devices"))?;
    if device_metadata.uid() != 0
        || device_metadata.mode() & 0o022 != 0
        || nix::sys::statfs::fstatfs(&private_devices)
            .map_err(sys("inspect device filesystem"))?
            .filesystem_type()
            != nix::sys::statfs::TMPFS_MAGIC
    {
        return Err(Error::Invalid("private device mount was replaced"));
    }
    nix::unistd::fchdir(&private_devices).map_err(sys("enter private devices"))?;
    prepare_devices(config.uid, config.gid)?;
    let null = open_private_null(&private_devices)?;
    nix::unistd::chdir(&config.root).map_err(sys("return to isolation root"))?;
    let returned = std::fs::metadata(".").map_err(io("inspect isolation root after devices"))?;
    if (returned.dev(), returned.ino()) != (contained.dev(), contained.ino()) {
        return Err(Error::Invalid("isolation root changed during device setup"));
    }
    drop(private_devices);
    let proc_directory = private_mountpoint("proc")?;
    nix::unistd::pivot_root(".", ".").map_err(sys("pivot isolation root"))?;
    nix::mount::umount2(".", nix::mount::MntFlags::MNT_DETACH)
        .map_err(sys("detach host mount tree"))?;
    nix::unistd::chdir("/").map_err(sys("enter contained root"))?;
    drop(root);
    drop(devices);
    nix::unistd::setsid().map_err(sys("isolate runtime session"))?;
    // Pin the runner before fork. PID 1 in the child namespace cannot see its
    // outside parent with getppid(), so a pidfd closes the PDEATHSIG setup race.
    let parent_handle =
        pplx_space_node_runtime_core::launch::ProcessObserver::current().map_err(|error| {
            Error::Io {
                operation: "pin namespace runner",
                source: std::io::Error::other(error),
            }
        })?;
    let (ready_read, ready_write) = nix::unistd::pipe2(nix::fcntl::OFlag::O_CLOEXEC)
        .map_err(sys("create runtime identity barrier"))?;
    // SAFETY: launcher mode is a fresh, single-threaded process. No runtime or
    // telemetry threads/locks exist. The parent only drops privilege and waits.
    match unsafe { nix::unistd::fork() }.map_err(sys("create PID namespace init"))? {
        ForkResult::Parent { child } => {
            drop(ready_read);
            // Publish before releasing the child to exec. A supervisor crash can
            // lose the reply, but cannot leave an executed VMM without its PID
            // registration. The sidecar directory is pinned outside the jail.
            let temporary = format!(".fc.pid.{}.tmp", child.as_raw());
            nix::unistd::symlinkat(
                format!("/proc/{}", child.as_raw()).as_str(),
                &registration,
                temporary.as_str(),
            )
            .map_err(sys("stage contained runtime identity"))?;
            nix::fcntl::renameat(&registration, temporary.as_str(), &registration, "fc.pid")
                .map_err(sys("publish contained runtime identity"))?;
            registration
                .sync_all()
                .map_err(io("persist contained runtime identity"))?;
            drop(registration);
            // Settle the runner's credentials before releasing its child. The
            // child installs PDEATHSIG only after this barrier and its own
            // privilege drop, so no credential transition races that contract.
            if let Err(error) = drop_privileges(config.uid, config.gid) {
                if let Err(kill_error) = nix::sys::signal::kill(child, Signal::SIGKILL) {
                    eprintln!(
                        "terminate contained child after privilege-drop failure: {kill_error}"
                    );
                }
                return Err(error);
            }
            stream
                .as_mut()
                .expect("runner retains status stream")
                .write_all(&[crate::sidecar_launcher::RUNTIME_PID_TAG])
                .map_err(io("report runtime PID tag"))?;
            stream
                .as_mut()
                .expect("runner retains status stream")
                .write_all(&child.as_raw().cast_unsigned().to_be_bytes())
                .map_err(io("report runtime PID"))?;
            nix::unistd::write(&ready_write, &[1])
                .map_err(sys("release runtime identity barrier"))?;
            drop(ready_write);
            // Only the child keeps the exec-status socket; consume the parent's
            // descriptor rather than closing behind its owner's back.
            drop(executable);
            drop(output);
            drop(null);
            drop(parent_handle);
            drop(stream.take());
            let code = loop {
                match waitpid(child, None) {
                    Ok(WaitStatus::Exited(_, code)) => break code,
                    Ok(WaitStatus::Signaled(_, signal, _)) => break 128 + signal as i32,
                    Ok(_) | Err(nix::errno::Errno::EINTR) => continue,
                    Err(_) => break 1,
                }
            };
            std::process::exit(code);
        }
        ForkResult::Child => {
            drop(registration);
            drop(ready_write);
            let mut ready = [0];
            if nix::unistd::read(&ready_read, &mut ready)
                .map_err(sys("wait for runtime identity"))?
                != 1
            {
                return Err(Error::Invalid(
                    "runner exited before publishing runtime identity",
                ));
            }
            drop(ready_read);
        }
    }
    // procfs must be mounted by the child after entering the new PID namespace.
    nix::unistd::fchdir(&proc_directory).map_err(sys("enter pinned proc mountpoint"))?;
    mount(
        Some("proc"),
        ".",
        Some("proc"),
        MsFlags::MS_NOSUID | MsFlags::MS_NODEV | MsFlags::MS_NOEXEC,
        None::<&str>,
    )
    .map_err(sys("mount contained procfs"))?;
    nix::unistd::chdir("/").map_err(sys("leave proc mountpoint"))?;
    drop(proc_directory);
    nix::unistd::dup2_stdout(&output).map_err(sys("redirect runtime stdout"))?;
    nix::unistd::dup2_stderr(&output).map_err(sys("redirect runtime stderr"))?;
    nix::unistd::dup2_stdin(&null).map_err(sys("disconnect runtime stdin"))?;
    drop(output);
    drop(null);
    // Bind after fork so SO_PEERCRED names the eventual VMM PID. The listener
    // inode stays root-owned and owner-only after setuid/exec: same-UID sidecars
    // cannot issue host-path-capable API requests. CH inherits only the fd.
    nix::sys::stat::umask(Mode::from_bits_truncate(0o077));
    let control = std::os::unix::net::UnixListener::bind("/control.sock")
        .map_err(io("bind privileged runtime control"))?;
    nix::fcntl::fcntl(
        &control,
        nix::fcntl::FcntlArg::F_SETFD(nix::fcntl::FdFlag::empty()),
    )
    .map_err(sys("inherit runtime control listener"))?;
    let mut arguments = config.arguments.clone();
    arguments.extend([
        "--api-socket".to_owned(),
        format!("fd={}", control.as_raw_fd()),
    ]);
    drop_privileges(config.uid, config.gid)?;
    nix::sys::prctl::set_pdeathsig(Signal::SIGKILL)
        .map_err(sys("bind runtime lifetime to runner"))?;
    if parent_handle
        .has_exited()
        .map_err(io("observe namespace runner"))?
    {
        return Err(Error::Invalid("runtime runner exited during preparation"));
    }
    drop(parent_handle);
    nix::sys::stat::umask(Mode::from_bits_truncate(0o077));
    stream
        .as_mut()
        .expect("child retains status stream")
        .write_all(&[crate::sidecar_launcher::PREPARED_TAG])
        .map_err(io("report contained launch ready"))?;
    exec(executable, &arguments)
}

fn drop_privileges(uid: u32, gid: u32) -> Result<(), Error> {
    use nix::sys::resource::{Resource, setrlimit};
    setrlimit(Resource::RLIMIT_CORE, 0, 0).map_err(sys("disable core dumps"))?;
    setrlimit(
        Resource::RLIMIT_NOFILE,
        pplx_space_common_core::host::linux::SANDBOX_PROCESS_NOFILE_LIMIT,
        pplx_space_common_core::host::linux::SANDBOX_PROCESS_NOFILE_LIMIT,
    )
    .map_err(sys("bound open files"))?;
    nix::unistd::setgroups(&[]).map_err(sys("drop supplementary groups"))?;
    nix::unistd::setgid(Gid::from_raw(gid)).map_err(sys("drop runtime group"))?;
    nix::unistd::setuid(Uid::from_raw(uid)).map_err(sys("drop runtime user"))?;
    nix::sys::prctl::set_no_new_privs().map_err(sys("disable privilege gains"))?;
    Ok(())
}
fn prepare_devices(uid: u32, gid: u32) -> Result<(), Error> {
    std::fs::create_dir("net").map_err(io("create contained device directory"))?;
    for (path, major, minor) in [
        ("kvm", 10, 232),
        ("net/tun", 10, 200),
        ("null", 1, 3),
        ("urandom", 1, 9),
    ] {
        mknod(
            Path::new(path),
            SFlag::S_IFCHR,
            Mode::from_bits_truncate(0o600),
            makedev(major, minor),
        )
        .map_err(sys("create contained device"))?;
        nix::unistd::chown(
            Path::new(path),
            Some(Uid::from_raw(uid)),
            Some(Gid::from_raw(gid)),
        )
        .map_err(sys("assign contained device"))?;
    }
    Ok(())
}
fn exec(executable: AuthenticatedFile, arguments: &[String]) -> Result<(), Error> {
    let args = std::iter::once("runtime")
        .chain(arguments.iter().map(String::as_str))
        .map(CString::new)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| Error::Invalid("argument contains NUL"))?;
    nix::unistd::execveat(
        executable.file(),
        c"",
        &args,
        &[] as &[CString],
        nix::fcntl::AtFlags::AT_EMPTY_PATH,
    )
    .map_err(sys("exec authenticated runtime"))?;
    unreachable!("execveat never returns on success")
}

/// Refuse existing entries instead of repairing an attacker-controlled tree.
/// The opened mountpoint is root-owned; an unprivileged process cannot alter
/// its contents even if it renames the entry in the surrounding sandbox root.
fn private_mountpoint(name: &str) -> Result<File, Error> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(name)
        .map_err(io("create private mountpoint"))?;
    let directory = File::options()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(name)
        .map_err(io("pin private mountpoint"))?;
    let metadata = directory
        .metadata()
        .map_err(io("inspect private mountpoint"))?;
    if metadata.uid() != 0 || metadata.mode() & 0o077 != 0 {
        return Err(Error::Invalid("private mountpoint was replaced"));
    }
    Ok(directory)
}

fn open_runtime_output(path: &Path) -> Result<File, Error> {
    let protected_parent = path
        .parent()
        .and_then(Path::parent)
        .ok_or(Error::Invalid("runtime output has no protected parent"))?;
    pplx_space_node_runtime_core::artifact_store::verify_execution_directory(protected_parent, 0)
        .map_err(|_| Error::Invalid("runtime output has unsafe ancestors"))?;
    // Never open an existing jail entry for writing. The sandbox UID can rename
    // even a root-owned boot-artifact hard link onto console.log. Create a fresh
    // root-owned 0600 inode outside the tenant-writable jail, then atomically
    // replace the directory entry and retain the same descriptor for output.
    let output = tempfile::Builder::new()
        .prefix(".runtime-console-")
        .tempfile_in(protected_parent)
        .map_err(io("create private runtime output"))?;
    nix::fcntl::fcntl(
        output.as_file(),
        nix::fcntl::FcntlArg::F_SETFL(nix::fcntl::OFlag::O_APPEND),
    )
    .map_err(sys("append runtime output"))?;
    output
        .persist(path)
        .map_err(|error| io("publish runtime output")(error.error))
}

fn open_registration_directory(path: &Path, sandbox_uid: u32) -> Result<File, Error> {
    let parent = path
        .parent()
        .ok_or(Error::Invalid("registration directory has no parent"))?;
    pplx_space_node_runtime_core::artifact_store::verify_execution_directory(parent, 0)
        .map_err(|_| Error::Invalid("runtime registration directory has unsafe ancestors"))?;
    let directory = File::options()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(io("pin runtime registration directory"))?;
    let metadata = directory
        .metadata()
        .map_err(io("inspect runtime registration directory"))?;
    // This is the shared sidecar run directory, not an immutable artifact
    // directory. PrepareSandbox owns it as root; sidecar launch uses the slot
    // UID. Its group-writable entries are locators, never process-ownership
    // evidence: recovery still checks the registered execution and cgroup.
    if ![0, sandbox_uid].contains(&metadata.uid())
        || metadata.mode() & 0o002 != 0
        || (metadata.mode() & 0o020 != 0
            && metadata.gid()
                != pplx_space_common_core::host::linux::identities::gids::SPACE_SIDECARCLIENTS)
    {
        return Err(Error::Invalid(
            "runtime registration directory has unsafe ownership or mode",
        ));
    }
    Ok(directory)
}

fn open_private_null(devices: &File) -> Result<File, Error> {
    let descriptor = nix::fcntl::openat(
        devices,
        "null",
        nix::fcntl::OFlag::O_RDONLY | nix::fcntl::OFlag::O_NOFOLLOW | nix::fcntl::OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(sys("open pinned private null"))?;
    Ok(File::from(descriptor))
}

#[cfg(test)]
mod tests;
