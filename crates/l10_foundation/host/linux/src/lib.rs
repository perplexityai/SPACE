//! Linux host mechanisms that name nothing SPACE-specific: filesystem
//! capacity probes ([`disk`]), the permanence of host-path errors
//! ([`path_errors`]), pidfd signalling and the OOM-killer preference.

pub mod disk;
pub mod path_errors;

/// Signal the process pinned by a pidfd, including Linux real-time signals.
/// The kernel rejects descriptors that are not pidfds. `signal` is a raw signal
/// number, so numbers beyond nix's named variants work; rustix also requires an
/// unsafe constructor for these values, with additional libc-reserved-signal
/// constraints.
///
/// Callers pass a valid signal number, 1..=64. The kernel rejects a number
/// above 64 or below 0 with `EINVAL`, but accepts 0: it then sends nothing and
/// returns `Ok(())` while the process exists, so pass 0 only to probe.
#[cfg(target_os = "linux")]
pub fn signal_pidfd(pidfd: impl std::os::fd::AsFd, signal: libc::c_int) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: the descriptor stays borrowed for the call, the only pointer
    // argument is a null siginfo, which the kernel permits with flags = 0, and
    // no signal number makes the call touch memory.
    let result = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            pidfd.as_fd().as_raw_fd(),
            signal,
            std::ptr::null::<libc::siginfo_t>(),
            0u32,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Mark the calling process as the host OOM killer's first choice by setting
/// its `oom_score_adj` to the maximum, 1000, so host memory pressure kills it
/// before processes with a lower score. Raising the score needs no privilege.
///
/// Async-signal-safe: it calls only `open`, `write` and `close` on a static
/// path and buffer, so it may run in a `pre_exec` hook.
#[cfg(target_os = "linux")]
pub fn prefer_for_oom_kill() -> std::io::Result<()> {
    const SCORE: &[u8] = b"1000";
    // SAFETY: the path is a static NUL-terminated string.
    let fd = unsafe {
        libc::open(
            c"/proc/self/oom_score_adj".as_ptr(),
            libc::O_WRONLY | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: `fd` is open and the buffer is a static slice.
    let written = unsafe { libc::write(fd, SCORE.as_ptr().cast(), SCORE.len()) };
    let result = match usize::try_from(written) {
        Ok(len) if len == SCORE.len() => Ok(()),
        Ok(_) => Err(std::io::ErrorKind::WriteZero.into()),
        Err(_) => Err(std::io::Error::last_os_error()),
    };
    // SAFETY: `fd` is open and owned here; nothing uses it after close.
    unsafe { libc::close(fd) };
    result
}
