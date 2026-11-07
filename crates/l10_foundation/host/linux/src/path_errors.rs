//! Permanence of filesystem errors against host paths.
//!
//! Code that resolves a host path — an installation root, a kernel image, a
//! cache directory — has to answer one question before it reports the
//! failure: will reading this path fail the same way next time? A permanent
//! answer fails the operation for good; a transient one leaves it retryable.
//!
//! Getting that backwards is expensive in both directions, which is why the
//! decision lives in one place instead of at each `map_err`.

/// Whether a filesystem error against a host path can plausibly clear on its
/// own, without anyone installing, moving, or `chmod`ing anything.
///
/// The permanent set describes the *path*, not the read: it is absent, it is
/// the wrong kind of object, its name is unusable or malformed (an interior
/// NUL byte surfaces as `InvalidInput`), or the filesystem refuses the
/// operation outright. Retrying re-resolves the same path and fails
/// identically.
///
/// Everything else is treated as a read that a later attempt can win — an
/// interrupted or busy read, a hardware or filesystem I/O fault, a timeout or
/// stale handle on a network-backed mount, a file another process is still
/// writing.
///
/// **The default is deliberately transient.** Misjudging a permanent error as
/// transient costs a bounded retry loop that shows up loudly in logs.
/// Misjudging a transient error as permanent fails an operation that a retry
/// would have satisfied. The second is worse, so unrecognized kinds retry.
///
/// One consequence worth knowing: `io::ErrorKind` does not name every errno on
/// stable Rust. A symlink loop (`ELOOP`) has no matchable kind today, so it
/// lands in the transient set despite being permanent. Catching that shape is
/// the caller's job, not this predicate's.
pub fn host_path_io_error_is_transient(error: &std::io::Error) -> bool {
    !matches!(
        error.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::PermissionDenied
            | std::io::ErrorKind::NotADirectory
            | std::io::ErrorKind::IsADirectory
            | std::io::ErrorKind::InvalidFilename
            | std::io::ErrorKind::InvalidInput
            | std::io::ErrorKind::Unsupported
    )
}

#[cfg(test)]
mod tests;
