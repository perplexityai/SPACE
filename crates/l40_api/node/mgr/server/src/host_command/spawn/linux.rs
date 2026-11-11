use super::*;
use std::ffi::CString;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;

use nix::spawn::{PosixSpawnAttr, PosixSpawnFileActions, PosixSpawnFlags, posix_spawnp};
use nix::sys::signal::{SigSet, Signal};

fn cstring(value: &OsStr) -> io::Result<CString> {
    CString::new(value.as_bytes())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
}

fn pipe() -> io::Result<(File, File)> {
    let (read, write) = nix::unistd::pipe2(nix::fcntl::OFlag::O_CLOEXEC)?;
    Ok((
        File::from(above_stdio(read)?),
        File::from(above_stdio(write)?),
    ))
}

fn above_stdio(fd: OwnedFd) -> io::Result<OwnedFd> {
    if fd.as_raw_fd() > libc::STDERR_FILENO {
        return Ok(fd);
    }
    Ok(rustix::io::fcntl_dupfd_cloexec(&fd, 3)?)
}

pub(super) fn start(command: &mut Command, input: Input) -> io::Result<Child> {
    let program = cstring(command.get_program())?;
    let args = std::iter::once(command.get_program())
        .chain(command.get_args())
        .map(cstring)
        .collect::<io::Result<Vec<_>>>()?;
    let environment = std::env::vars_os()
        .map(|(key, value)| {
            let mut entry = key.into_encoded_bytes();
            entry.push(b'=');
            entry.extend(value.into_encoded_bytes());
            CString::new(entry).map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
        })
        .collect::<io::Result<Vec<_>>>()?;
    let mut attributes = PosixSpawnAttr::init()?;
    attributes.set_sigmask(&crate::process_signals::child_mask()?)?;
    let mut defaults = SigSet::empty();
    for signal in [Signal::SIGINT, Signal::SIGTERM, Signal::SIGPIPE] {
        defaults.add(signal);
    }
    attributes.set_sigdefault(&defaults)?;
    attributes.set_flags(
        PosixSpawnFlags::POSIX_SPAWN_SETSIGMASK | PosixSpawnFlags::POSIX_SPAWN_SETSIGDEF,
    )?;
    let mut actions = PosixSpawnFileActions::init()?;
    let (mut parent_stdin, mut parent_stdout, mut parent_stderr) = (None, None, None);
    // Keep descriptors alive through spawn. Closing their parent-side copies
    // afterward is also what allows the child's readers to observe EOF.
    let mut child_ends = Vec::new();
    if let Input::Captured = input {
        let (stdin, write) = pipe()?;
        parent_stdin = Some(write);
        let (stdout, stdout_writer) = pipe()?;
        let (stderr, stderr_writer) = pipe()?;
        parent_stdout = Some(stdout);
        parent_stderr = Some(stderr);
        for (file, target) in [(stdin, 0), (stdout_writer, 1), (stderr_writer, 2)] {
            actions.add_dup2(file.as_raw_fd(), target)?;
            child_ends.push(file);
        }
    }
    // Construct the child owner only after successful spawn: no failure before
    // this point owns a process or may signal any PID.
    let pid = posix_spawnp(&program, &actions, &attributes, &args, &environment)?;
    let mut child = Child {
        process: Process::Spawned {
            pid,
            pidfd: None,
            status: None,
        },
        stdin: parent_stdin,
        stdout: parent_stdout,
        stderr: parent_stderr,
    };
    drop(child_ends);
    let pid = rustix::process::Pid::from_raw(pid.as_raw())
        .ok_or_else(|| io::Error::other("spawn returned an invalid child PID"))?;
    let pidfd = rustix::process::pidfd_open(pid, rustix::process::PidfdFlags::empty())?;
    if let Process::Spawned { pidfd: owned, .. } = &mut child.process {
        *owned = Some(pidfd);
    }
    Ok(child)
}
