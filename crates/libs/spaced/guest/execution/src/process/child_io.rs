//! Child input, PTY descriptors and process signaling.

use super::*;

pub(super) const DEFAULT_PTY_ROWS: u32 = 24;

pub(super) const DEFAULT_PTY_COLS: u32 = 80;

#[cfg(target_os = "linux")]
pub(super) type ExitObserver = ProcessExitObserver;

#[cfg(not(target_os = "linux"))]
pub(super) type ExitObserver = ();

pub(super) enum ProcessInput {
    Disabled,
    Pipe(InputDescriptor<tokio::process::ChildStdin>),
    Pty(PtyInput),
}

pub(super) struct PtyInput {
    pub(super) writer: InputDescriptor<PtyIo>,
    pub(super) resize_fd: std::sync::Mutex<Option<PtyIo>>,
}

/// A process input descriptor that [`InputDescriptor::release`] closes once
/// the process has exited. A write still in flight then keeps it only until
/// that write returns or is cancelled.
pub(super) struct InputDescriptor<T> {
    pub(super) descriptor: Mutex<Option<T>>,
    pub(super) released: std::sync::atomic::AtomicBool,
}

impl<T> InputDescriptor<T> {
    pub(super) fn new(descriptor: Option<T>) -> Self {
        Self {
            descriptor: Mutex::new(descriptor),
            released: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub(super) async fn lock(&self) -> HeldInput<'_, T> {
        HeldInput {
            guard: Some(self.descriptor.lock().await),
            input: self,
        }
    }

    pub(super) fn is_released(&self) -> bool {
        self.released.load(Ordering::SeqCst)
    }

    pub(super) fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        // Otherwise a write holds it, and closes it once it unlocks.
        if let Ok(mut descriptor) = self.descriptor.try_lock() {
            descriptor.take();
        }
    }
}

/// Exclusive use of an input descriptor for one write or close.
pub(super) struct HeldInput<'a, T> {
    pub(super) guard: Option<tokio::sync::MutexGuard<'a, Option<T>>>,
    pub(super) input: &'a InputDescriptor<T>,
}

impl<T> std::ops::Deref for HeldInput<'_, T> {
    type Target = Option<T>;

    fn deref(&self) -> &Option<T> {
        self.guard.as_ref().expect("held until dropped")
    }
}

impl<T> std::ops::DerefMut for HeldInput<'_, T> {
    fn deref_mut(&mut self) -> &mut Option<T> {
        self.guard.as_mut().expect("held until dropped")
    }
}

impl<T> Drop for HeldInput<'_, T> {
    fn drop(&mut self) {
        // Unlock first: a release that saw the lock held either set its flag
        // before this check or tries the lock again after this unlock.
        drop(self.guard.take());
        if self.input.is_released()
            && let Ok(mut descriptor) = self.input.descriptor.try_lock()
        {
            descriptor.take();
        }
    }
}

/// The answer for input to a process that has exited, as for one no longer
/// running when the request arrived.
pub(super) fn exited_input_status() -> Status {
    Status::not_found("process not found")
}

impl ProcessInput {
    pub(super) async fn write(&self, data: &[u8]) -> Result<(), Status> {
        match self {
            Self::Disabled => Err(Status::failed_precondition(
                "process stdin is not enabled or closed",
            )),
            Self::Pipe(input) => {
                let mut stdin = input.lock().await;
                let Some(stdin) = stdin.as_mut() else {
                    return Err(if input.is_released() {
                        exited_input_status()
                    } else {
                        Status::failed_precondition("process stdin is not enabled or closed")
                    });
                };
                stdin.write_all(data).await.map_err(map_io_error)?;
                stdin.flush().await.map_err(map_io_error)
            }
            Self::Pty(pty) => {
                let mut writer = pty.writer.lock().await;
                // Only the release closes a PTY writer.
                let writer = writer.as_mut().ok_or_else(exited_input_status)?;
                writer.write_all(data).await.map_err(map_io_error)?;
                writer.flush().await.map_err(map_io_error)
            }
        }
    }

    pub(super) async fn close(&self) -> Result<(), Status> {
        match self {
            Self::Disabled => Ok(()),
            Self::Pipe(input) => {
                // Already closed, by the caller or by the release: done.
                if let Some(mut stdin) = input.lock().await.take() {
                    stdin.shutdown().await.map_err(map_io_error)?;
                }
                Ok(())
            }
            Self::Pty(_) => Err(Status::failed_precondition(
                "PTY input cannot be half-closed; send terminal EOF or signal the process",
            )),
        }
    }

    pub(super) fn resize_pty(&self, size: PtySize) -> Result<(), Status> {
        match self {
            Self::Pty(pty) => {
                let resize_fd = pty
                    .resize_fd
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                // Only the release closes it.
                let resize_fd = resize_fd.as_ref().ok_or_else(exited_input_status)?;
                set_pty_size(resize_fd, size)
            }
            Self::Disabled | Self::Pipe(_) => {
                Err(Status::failed_precondition("process is not pty-backed"))
            }
        }
    }

    /// Closes the input descriptors once the process has exited, so a
    /// descendant still reading the stdin pipe sees EOF and one still writing
    /// to the terminal gets EIO, rather than either blocking for the record's
    /// retention. A write still in flight closes its descriptor when it
    /// returns.
    pub(super) fn release(&self) {
        match self {
            Self::Disabled => {}
            Self::Pipe(stdin) => stdin.release(),
            Self::Pty(pty) => {
                pty.writer.release();
                pty.resize_fd
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
            }
        }
    }
}

pub(super) struct PreparedPty {
    pub(super) reader: PtyIo,
    pub(super) writer: PtyIo,
    pub(super) resize_fd: PtyIo,
    pub(super) control_fd: OwnedFd,
}

// Outside Linux, `openpty` cannot create both descriptors with CLOEXEC
// atomically. PTY creation and every child spawn serialize on this lock there,
// so no concurrent fork can inherit a PTY during the openpty→fcntl window.
#[cfg(not(target_os = "linux"))]
pub(super) static PTY_INHERITANCE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub(super) fn spawn_child(command: &mut Command) -> io::Result<tokio::process::Child> {
    #[cfg(not(target_os = "linux"))]
    let _inheritance_guard = PTY_INHERITANCE_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    command.spawn()
}

pub(super) fn prepare_pty(size: Option<PtySize>) -> Result<PreparedPty, Status> {
    let winsize = pty_winsize(size)?;
    let (master, control_fd) = open_pty_pair(&winsize)?;
    let master = PtyIo::new(master).map_err(map_io_error)?;

    Ok(PreparedPty {
        reader: master.clone(),
        writer: master.clone(),
        resize_fd: master,
        control_fd,
    })
}

// Both descriptors are CLOEXEC from creation, so a child spawned concurrently
// on another thread cannot inherit either one and no lock is needed.
// `TIOCGPTPEER` needs Linux 4.13 or later; every SPACE guest kernel has it.
/// Registers a `pre_exec` hook that sets the child's `oom_score_adj` to 0.
/// Fails closed, like cgroup placement: a workload must not run with
/// spaced's protection.
#[cfg(target_os = "linux")]
pub(super) fn reset_workload_oom_score(command: &mut Command) {
    // SAFETY: the closure runs in the forked child before `exec`. It calls only
    // `open`, `write` and `close` (rustix, on a static NUL-terminated path and
    // a fixed buffer): async-signal-safe, no allocation, no locks.
    unsafe {
        command.pre_exec(|| {
            let fd = rustix::fs::open(
                c"/proc/self/oom_score_adj",
                rustix::fs::OFlags::WRONLY | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(),
            )?;
            if rustix::io::write(&fd, b"0")? != 1 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            Ok(())
        });
    }
}

#[cfg(not(target_os = "linux"))]
pub(super) fn reset_workload_oom_score(_command: &mut Command) {}

#[cfg(target_os = "linux")]
pub(super) fn open_pty_pair(winsize: &libc::winsize) -> Result<(OwnedFd, OwnedFd), Status> {
    use rustix::pty::{OpenptFlags, ioctl_tiocgptpeer, openpt, unlockpt};
    let io_status = |error: rustix::io::Errno| map_io_error(error.into());
    let flags = OpenptFlags::RDWR | OpenptFlags::NOCTTY | OpenptFlags::CLOEXEC;
    let master = openpt(flags).map_err(io_status)?;
    unlockpt(&master).map_err(io_status)?;
    let control_fd = ioctl_tiocgptpeer(&master, flags).map_err(io_status)?;
    rustix::termios::tcsetwinsize(
        &control_fd,
        rustix::termios::Winsize {
            ws_row: winsize.ws_row,
            ws_col: winsize.ws_col,
            ws_xpixel: winsize.ws_xpixel,
            ws_ypixel: winsize.ws_ypixel,
        },
    )
    .map_err(io_status)?;
    Ok((master, control_fd))
}

#[cfg(not(target_os = "linux"))]
pub(super) fn open_pty_pair(winsize: &libc::winsize) -> Result<(OwnedFd, OwnedFd), Status> {
    let _inheritance_guard = PTY_INHERITANCE_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let nix::pty::OpenptyResult { master, slave } =
        nix::pty::openpty(Some(winsize), None).map_err(|error| map_io_error(error.into()))?;
    set_cloexec(&master)?;
    set_cloexec(&slave)?;
    Ok((master, slave))
}

pub(super) fn pty_winsize(size: Option<PtySize>) -> Result<libc::winsize, Status> {
    let rows = size.as_ref().map_or(DEFAULT_PTY_ROWS, |size| size.rows);
    let cols = size.as_ref().map_or(DEFAULT_PTY_COLS, |size| size.cols);
    if rows == 0 || cols == 0 {
        return Err(Status::invalid_argument(
            "pty rows and cols must be non-zero",
        ));
    }
    let rows =
        u16::try_from(rows).map_err(|_| Status::invalid_argument("pty rows is too large"))?;
    let cols =
        u16::try_from(cols).map_err(|_| Status::invalid_argument("pty cols is too large"))?;
    Ok(libc::winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    })
}

pub(super) fn set_pty_size(fd: impl AsFd, size: PtySize) -> Result<(), Status> {
    let size = pty_winsize(Some(size))?;
    rustix::termios::tcsetwinsize(
        fd,
        rustix::termios::Winsize {
            ws_row: size.ws_row,
            ws_col: size.ws_col,
            ws_xpixel: size.ws_xpixel,
            ws_ypixel: size.ws_ypixel,
        },
    )
    .map_err(|error| map_io_error(error.into()))
}

pub(super) fn configure_child_pty(control_fd: RawFd) -> io::Result<()> {
    // SAFETY: `control_fd` is the pty slave fd inherited by the forked child.
    // `login_tty` creates a new session, makes the slave the controlling
    // terminal, duplicates it onto stdin/stdout/stderr, and closes the original
    // fd before exec.
    let result = unsafe { libc::login_tty(control_fd) };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

pub(super) struct ProcessSignalHandle {
    pub(super) pid: u32,
    #[cfg(target_os = "linux")]
    pub(super) pidfd: OwnedFd,
}

#[cfg(target_os = "linux")]
pub(super) struct ProcessExitObserver {
    pub(super) pidfd: tokio::io::unix::AsyncFd<OwnedFd>,
}

impl ProcessSignalHandle {
    #[cfg(target_os = "linux")]
    pub(super) fn open(pid: u32) -> io::Result<(Self, ProcessExitObserver)> {
        let raw_pid = i32::try_from(pid)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let process_pid = rustix::process::Pid::from_raw(raw_pid)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "pid must be positive"))?;
        let pidfd = rustix::process::pidfd_open(process_pid, rustix::process::PidfdFlags::empty())?;
        // Keep independent ownership for signal delivery and exit observation.
        let observer_fd = pidfd.try_clone()?;
        let observer = ProcessExitObserver {
            // SAFETY: AsyncFd takes the owned duplicate. ProcessExitObserver
            // never replaces or closes it; deregistration precedes its drop.
            pidfd: unsafe { tokio::io::unix::AsyncFd::register(observer_fd)? },
        };
        Ok((Self { pid, pidfd }, observer))
    }

    #[cfg(not(target_os = "linux"))]
    pub(super) fn open(pid: u32) -> io::Result<(Self, ())> {
        i32::try_from(pid).map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        Ok((Self { pid }, ()))
    }

    pub(super) fn signal(&self, signal: ProcessSignal) -> Result<(), Status> {
        #[cfg(target_os = "linux")]
        {
            match pplx_space_common_core::host::linux::signal_pidfd(&self.pidfd, signal) {
                Ok(()) => Ok(()),
                Err(error) if error.raw_os_error() == Some(libc::ESRCH) => {
                    Err(Status::not_found("process not found"))
                }
                Err(error) => Err(Status::internal(error.to_string())),
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(self.pid as i32),
                nix::sys::signal::Signal::try_from(signal.as_i32())
                    .map_err(|_| Status::invalid_argument("invalid signal"))?,
            )
            .map_err(|error| match error {
                nix::errno::Errno::ESRCH => Status::not_found("process not found"),
                error => Status::internal(error.to_string()),
            })
        }
    }

    pub(super) fn terminate_group(&self) -> io::Result<()> {
        let process_group = i32::try_from(self.pid)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        // Linux callers keep the leader unreaped, pinning the group identity.
        match nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(process_group),
            nix::sys::signal::Signal::SIGKILL,
        ) {
            Ok(()) | Err(nix::errno::Errno::ESRCH) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(target_os = "linux")]
impl ProcessExitObserver {
    pub(super) async fn exited(&self) -> io::Result<()> {
        // pidfds become readable when the child exits and stay readable. We do
        // not reap here, so its pid continues to pin the process-group id until
        // descendants have been terminated.
        self.pidfd.readable().await.map(|_| ())
    }
}

#[cfg(not(target_os = "linux"))]
pub(super) fn set_cloexec(fd: impl AsFd) -> Result<(), Status> {
    use nix::fcntl::{FcntlArg, FdFlag, fcntl};
    let flags = FdFlag::from_bits_retain(
        fcntl(fd.as_fd(), FcntlArg::F_GETFD).map_err(|error| map_io_error(error.into()))?,
    );
    fcntl(fd, FcntlArg::F_SETFD(flags | FdFlag::FD_CLOEXEC))
        .map_err(|error| map_io_error(error.into()))?;
    Ok(())
}
