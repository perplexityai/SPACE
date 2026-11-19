//! Keep spaced's code resident.
//!
//! A guest has no swap, so under memory pressure the kernel reclaims file
//! pages, and spaced's text is file pages. Once evicted, requests fault it
//! back in from the disk the pressure is competing for. `memory.min` cannot
//! protect it: the pages read in at exec are charged to `spaced.service`,
//! whose usage includes every workload, so a floor there covers them only
//! while the workloads are small. Locking does not depend on where the pages
//! are charged.
//!
//! Only the read-only file mappings present at startup (the binary's text and
//! read-only data, and its shared libraries) are locked, and only as they are
//! faulted in (`MLOCK_ONFAULT`): pages spaced never touches are not read.
//! Writable mappings become anonymous memory when written, which is not
//! reclaimed without swap either. Best effort: a failure is logged and spaced
//! runs unlocked.

/// What [`lock_text`] did, kept until [`report`] logs it: locking runs before
/// telemetry, when a log line would be dropped.
static LOCK_REPORT: std::sync::Mutex<Option<Result<usize, String>>> = std::sync::Mutex::new(None);

/// Lock the read-only file mappings of this process; see the module docs.
/// Records the outcome for [`report`].
pub(crate) fn lock_text() {
    let outcome = lock_read_only_file_mappings();
    *LOCK_REPORT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(outcome);
}

/// Log what [`lock_text`] did, now that telemetry records it. Once; later
/// calls, or a process that never locked, log nothing.
pub(crate) fn report() {
    let outcome = LOCK_REPORT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    match outcome {
        Some(Ok(mapped_bytes)) => tracing::info!(mapped_bytes, "locked text"),
        Some(Err(error)) => tracing::warn!(%error, "text not fully locked; spaced runs unlocked"),
        None => {}
    }
}

/// The mapped bytes locked, or why not all of them were.
fn lock_read_only_file_mappings() -> Result<usize, String> {
    let maps = std::fs::read_to_string("/proc/self/maps")
        .map_err(|error| format!("reading /proc/self/maps: {error}"))?;
    let (mut locked, mut failed) = (0_usize, 0_usize);
    let mut last_error = None;
    for (start, end) in read_only_file_mappings(&maps) {
        // SAFETY: the range is a mapping of this process as listed by the
        // kernel; mlock2 only changes its residency, not its contents.
        let result = unsafe {
            libc::mlock2(
                start as *const libc::c_void,
                end - start,
                libc::MLOCK_ONFAULT,
            )
        };
        if result == 0 {
            locked += end - start;
        } else {
            failed += 1;
            last_error = Some(std::io::Error::last_os_error());
        }
    }
    match last_error {
        None => Ok(locked),
        Some(error) => Err(format!(
            "{failed} mappings not locked ({locked} bytes locked): {error}"
        )),
    }
}

/// `(start, end)` of every mapping in a `/proc/<pid>/maps` body that is backed
/// by a regular file and neither writable nor shared.
fn read_only_file_mappings(maps: &str) -> impl Iterator<Item = (usize, usize)> + '_ {
    maps.lines().filter_map(|line| {
        let mut fields = line.split_whitespace();
        let (range, perms) = (fields.next()?, fields.next()?);
        let path = fields.nth(3)?;
        let [_, write, _, sharing] = perms.as_bytes() else {
            return None;
        };
        if *write == b'w' || *sharing != b'p' {
            return None;
        }
        if !path.starts_with('/') || path.starts_with("/dev/") || path.starts_with("/memfd:") {
            return None;
        }
        if fields.next().is_some_and(|rest| rest == "(deleted)") {
            return None;
        }
        let (start, end) = range.split_once('-')?;
        let start = usize::from_str_radix(start, 16).ok()?;
        let end = usize::from_str_radix(end, 16).ok()?;
        (end > start).then_some((start, end))
    })
}

#[cfg(test)]
mod tests;
