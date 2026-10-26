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
use pplx_space_node_runtime_core::verity::SealedFile;
use std::{
    ffi::CString,
    fs::File,
    io::Write,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
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

pub(crate) fn run(config: Config, mut stream: UnixStream) -> Result<(), String> {
    match prepare_and_exec(&config, &mut stream) {
        Ok(()) => Ok(()),
        Err(error) => {
            let error = error.to_string();
            crate::sidecar_launcher::write_error(&mut stream, &error)
                .map_err(|report| format!("{error}; report launch failure: {report}"))?;
            Err(error)
        }
    }
}

/// Called only by launcher mode, before Tokio or telemetry creates threads.
fn prepare_and_exec(config: &Config, stream: &mut UnixStream) -> Result<(), Error> {
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
    let executable = crate::verified_launch::open(&config.executable, config.executable_digest)
        .map_err(Error::Authentication)?;
    let network = File::options()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&config.network_namespace)
        .map_err(io("open network namespace"))?;
    let output = File::options()
        .append(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&config.output)
        .map_err(io("open runtime output"))?;
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
    // before chroot; otherwise mounting procfs beneath that root returns EINVAL.
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
    nix::unistd::chroot(".").map_err(sys("confine filesystem"))?;
    nix::unistd::chdir("/").map_err(sys("enter contained root"))?;
    drop(root);
    prepare_devices(config.uid, config.gid)?;
    // Pin the runner before fork. PID 1 in the child namespace cannot see its
    // outside parent with getppid(), so a pidfd closes the PDEATHSIG setup race.
    // SAFETY: pidfd_open takes only scalar arguments and returns an owned fd.
    let parent_fd =
        unsafe { libc::syscall(libc::SYS_pidfd_open, nix::unistd::getpid().as_raw(), 0) };
    if parent_fd < 0 {
        return Err(io("pin namespace runner")(std::io::Error::last_os_error()));
    }
    // SAFETY: successful pidfd_open returned a new descriptor owned here.
    let parent_fd = unsafe { OwnedFd::from_raw_fd(parent_fd as i32) };
    let (ready_read, ready_write) = nix::unistd::pipe2(nix::fcntl::OFlag::O_CLOEXEC)
        .map_err(sys("create runtime identity barrier"))?;
    // SAFETY: launcher mode is a fresh, single-threaded process. No runtime or
    // telemetry threads/locks exist. The parent only drops privilege and waits.
    match unsafe { nix::unistd::fork() }.map_err(sys("create PID namespace init"))? {
        ForkResult::Parent { child } => {
            drop(ready_read);
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
                .write_all(&[crate::sidecar_launcher::RUNTIME_PID_TAG])
                .map_err(io("report runtime PID tag"))?;
            stream
                .write_all(&child.as_raw().cast_unsigned().to_be_bytes())
                .map_err(io("report runtime PID"))?;
            nix::unistd::write(&ready_write, &[1])
                .map_err(sys("release runtime identity barrier"))?;
            drop(ready_write);
            // Only the child may keep the exec-status socket open. This runner
            // never returns to code owning the borrowed stream after closing it.
            drop(executable);
            drop(output);
            drop(parent_fd);
            let status_fd = stream.as_raw_fd();
            // SAFETY: this branch exits the process without dropping/reusing the
            // borrowed UnixStream; closing its descriptor permits exec EOF.
            unsafe {
                libc::close(status_fd);
            }
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
    std::fs::create_dir_all("/proc").map_err(io("create contained proc directory"))?;
    mount(
        Some("proc"),
        "/proc",
        Some("proc"),
        MsFlags::MS_NOSUID | MsFlags::MS_NODEV | MsFlags::MS_NOEXEC,
        None::<&str>,
    )
    .map_err(sys("mount contained procfs"))?;
    nix::unistd::dup2_stdout(&output).map_err(sys("redirect runtime stdout"))?;
    nix::unistd::dup2_stderr(&output).map_err(sys("redirect runtime stderr"))?;
    let null = File::open("/dev/null").map_err(io("open contained null"))?;
    nix::unistd::dup2_stdin(&null).map_err(sys("disconnect runtime stdin"))?;
    drop(output);
    drop(null);
    drop_privileges(config.uid, config.gid)?;
    nix::sys::prctl::set_pdeathsig(Signal::SIGKILL)
        .map_err(sys("bind runtime lifetime to runner"))?;
    let mut pollfd = libc::pollfd {
        fd: parent_fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: pollfd is a valid one-element array; timeout is nonblocking.
    let ready = unsafe { libc::poll(&mut pollfd, 1, 0) };
    if ready != 0 {
        return Err(Error::Invalid("runtime runner exited during preparation"));
    }
    drop(parent_fd);
    nix::sys::stat::umask(Mode::from_bits_truncate(0o077));
    stream
        .write_all(&[crate::sidecar_launcher::PREPARED_TAG])
        .map_err(io("report contained launch ready"))?;
    exec(executable, &config.arguments)
}

fn drop_privileges(uid: u32, gid: u32) -> Result<(), Error> {
    use nix::sys::resource::{Resource, setrlimit};
    nix::unistd::setgroups(&[]).map_err(sys("drop supplementary groups"))?;
    nix::unistd::setgid(Gid::from_raw(gid)).map_err(sys("drop runtime group"))?;
    nix::unistd::setuid(Uid::from_raw(uid)).map_err(sys("drop runtime user"))?;
    nix::sys::prctl::set_no_new_privs().map_err(sys("disable privilege gains"))?;
    setrlimit(Resource::RLIMIT_CORE, 0, 0).map_err(sys("disable core dumps"))?;
    setrlimit(Resource::RLIMIT_NOFILE, 4096, 4096).map_err(sys("bound open files"))?;
    Ok(())
}
fn prepare_devices(uid: u32, gid: u32) -> Result<(), Error> {
    std::fs::create_dir_all("/dev/net").map_err(io("create contained device directory"))?;
    for (path, major, minor) in [
        ("/dev/kvm", 10, 232),
        ("/dev/net/tun", 10, 200),
        ("/dev/null", 1, 3),
        ("/dev/urandom", 1, 9),
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
fn exec(executable: SealedFile, arguments: &[String]) -> Result<(), Error> {
    let args = std::iter::once("runtime")
        .chain(arguments.iter().map(String::as_str))
        .map(CString::new)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| Error::Invalid("argument contains NUL"))?;
    let pointers: Vec<_> = args
        .iter()
        .map(|v| v.as_ptr())
        .chain([std::ptr::null()])
        .collect();
    let env = [std::ptr::null::<libc::c_char>()];
    // SAFETY: executable owns the measured inode, argv/env are NUL terminated
    // pointer arrays backed by live CStrings. No pathname is reopened.
    unsafe {
        libc::syscall(
            libc::SYS_execveat,
            executable.file().as_raw_fd(),
            c"".as_ptr(),
            pointers.as_ptr(),
            env.as_ptr(),
            libc::AT_EMPTY_PATH,
        );
    }
    Err(io("exec authenticated runtime")(
        std::io::Error::last_os_error(),
    ))
}
