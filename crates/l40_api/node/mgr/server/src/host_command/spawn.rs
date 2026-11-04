//! Child ownership shared by fast plain commands and the namespace exception.
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io;
use std::process::{ExitStatus, Stdio};

/// Only program and arguments are configurable. Unsupported process settings
/// cannot be silently discarded when selecting the posix_spawn implementation.
#[derive(Debug)]
pub(crate) struct Command {
    program: OsString,
    args: Vec<OsString>,
}

impl Command {
    pub(crate) fn new(program: impl AsRef<OsStr>) -> Self {
        Self {
            program: program.as_ref().to_owned(),
            args: Vec::new(),
        }
    }

    pub(crate) fn arg(&mut self, arg: impl AsRef<OsStr>) -> &mut Self {
        self.args.push(arg.as_ref().to_owned());
        self
    }

    pub(crate) fn args<I, S>(&mut self, args: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.args
            .extend(args.into_iter().map(|arg| arg.as_ref().to_owned()));
        self
    }

    pub(crate) fn get_program(&self) -> &OsStr {
        &self.program
    }

    pub(crate) fn get_args(&self) -> impl Iterator<Item = &OsStr> {
        self.args.iter().map(OsString::as_os_str)
    }
}

pub(super) enum Input {
    Captured,
    Inherit,
}

pub(super) struct Child {
    process: Process,
    pub(super) stdin: Option<File>,
    pub(super) stdout: Option<File>,
    pub(super) stderr: Option<File>,
}

enum Process {
    #[cfg(target_os = "linux")]
    Spawned {
        pid: nix::unistd::Pid,
        pidfd: Option<std::os::fd::OwnedFd>,
        status: Option<ExitStatus>,
    },
    Forked(std::process::Child),
}

impl Child {
    #[cfg(test)]
    pub(super) fn id(&self) -> u32 {
        match &self.process {
            #[cfg(target_os = "linux")]
            Process::Spawned { pid, .. } => pid.as_raw().cast_unsigned(),
            Process::Forked(child) => child.id(),
        }
    }

    pub(super) fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.wait_inner(true)
    }

    pub(super) fn wait(&mut self) -> io::Result<ExitStatus> {
        self.wait_inner(false)?
            .ok_or_else(|| io::Error::other("blocking wait returned no child status"))
    }

    fn wait_inner(&mut self, nonblocking: bool) -> io::Result<Option<ExitStatus>> {
        match &mut self.process {
            #[cfg(target_os = "linux")]
            Process::Spawned { pid, status, .. } => {
                use nix::sys::wait::{WaitPidFlag, WaitStatus, waitpid};
                use std::os::unix::process::ExitStatusExt;
                if let Some(status) = *status {
                    return Ok(Some(status));
                }
                let flags = nonblocking.then_some(WaitPidFlag::WNOHANG);
                let result = loop {
                    match waitpid(*pid, flags) {
                        Err(nix::errno::Errno::EINTR) => continue,
                        result => break result?,
                    }
                };
                let raw = match result {
                    WaitStatus::StillAlive => return Ok(None),
                    WaitStatus::Exited(_, code) => code << 8,
                    WaitStatus::Signaled(_, signal, core) => {
                        signal as i32 | if core { 0x80 } else { 0 }
                    }
                    _ => return Err(io::Error::other("unexpected foreground child wait status")),
                };
                *status = Some(ExitStatus::from_raw(raw));
                Ok(*status)
            }
            Process::Forked(child) => {
                if nonblocking {
                    child.try_wait()
                } else {
                    child.wait().map(Some)
                }
            }
        }
    }

    fn kill(&mut self) -> io::Result<()> {
        match &mut self.process {
            #[cfg(target_os = "linux")]
            Process::Spawned { pid, pidfd, status } => {
                if status.is_some() {
                    return Ok(());
                }
                let result = if let Some(pidfd) = pidfd {
                    rustix::process::pidfd_send_signal(pidfd, rustix::process::Signal::KILL)
                        .map_err(io::Error::from)
                } else {
                    // Only used if pidfd acquisition failed immediately after
                    // spawn. This owner has not reaped the child, so its PID
                    // cannot be reused. No other code may reap owned children.
                    nix::sys::signal::kill(*pid, nix::sys::signal::Signal::SIGKILL)
                        .map_err(io::Error::from)
                };
                match result {
                    Err(error) if error.raw_os_error() == Some(libc::ESRCH) => Ok(()),
                    result => result,
                }
            }
            Process::Forked(child) => child.kill(),
        }
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        if let Err(error) = self.kill() {
            tracing::warn!(%error, "failed to terminate host command; waiting for exit");
        }
        if let Err(error) = self.wait() {
            tracing::warn!(%error, "failed to reap host command");
        }
    }
}

fn from_std(mut child: std::process::Child) -> Child {
    use std::os::fd::OwnedFd;
    Child {
        stdin: child.stdin.take().map(|fd| File::from(OwnedFd::from(fd))),
        stdout: child.stdout.take().map(|fd| File::from(OwnedFd::from(fd))),
        stderr: child.stderr.take().map(|fd| File::from(OwnedFd::from(fd))),
        process: Process::Forked(child),
    }
}

/// Mount namespace preparation is the intentional fork/pre-exec exception.
pub(super) fn in_namespace(
    command: &mut std::process::Command,
    input: Stdio,
    output: Stdio,
) -> io::Result<Child> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt;
        let mask = crate::process_signals::child_mask()?;
        let restore = move || {
            mask.thread_set_mask()?;
            for signal in [libc::SIGINT, libc::SIGTERM, libc::SIGPIPE] {
                // SAFETY: these valid signals are reset only in the child.
                if unsafe { libc::signal(signal, libc::SIG_DFL) } == libc::SIG_ERR {
                    return Err(io::Error::last_os_error());
                }
            }
            Ok(())
        };
        // SAFETY: restore uses only precomputed signal state and async-signal-safe
        // syscalls. No allocation or locking occurs after fork.
        unsafe {
            command.pre_exec(restore);
        }
    }
    command
        .stdin(input)
        .stdout(output)
        .stderr(Stdio::piped())
        .spawn()
        .map(from_std)
}

pub(super) fn start(command: &mut Command, input: Input) -> io::Result<Child> {
    #[cfg(target_os = "linux")]
    {
        linux::start(command, input)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let mut standard = std::process::Command::new(command.get_program());
        let command = standard.args(command.get_args());
        match input {
            Input::Inherit => command
                .stdin(Stdio::inherit())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit()),
            Input::Captured => command
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped()),
        };
        command.spawn().map(from_std)
    }
}

#[cfg(target_os = "linux")]
mod linux;
