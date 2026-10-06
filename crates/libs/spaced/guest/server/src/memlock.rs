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

/// Lock the read-only file mappings of this process; see the module docs.
pub(crate) fn lock_text() {
    let maps = match std::fs::read_to_string("/proc/self/maps") {
        Ok(maps) => maps,
        Err(error) => {
            tracing::warn!(%error, "text not locked: reading /proc/self/maps");
            return;
        }
    };
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
        None => tracing::info!(mapped_bytes = locked, "locked text"),
        Some(error) => {
            tracing::warn!(%error, failed, mapped_bytes = locked, "text not fully locked")
        }
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
mod tests {
    use super::read_only_file_mappings;

    #[test]
    fn selects_read_only_private_file_mappings() {
        let maps = "\
55d0c0000000-55d0c0100000 r--p 00000000 fe:01 1234 /usr/local/bin/spaced-svc
55d0c0100000-55d0c0800000 r-xp 00100000 fe:01 1234 /usr/local/bin/spaced-svc
55d0c0800000-55d0c0900000 rw-p 00800000 fe:01 1234 /usr/local/bin/spaced-svc
55d0c1000000-55d0c1100000 rw-p 00000000 00:00 0 [heap]
7f0000000000-7f0000200000 r-xp 00028000 fe:01 99 /usr/lib/x86_64-linux-gnu/libc.so.6
7f0000300000-7f0000301000 r--s 00000000 00:05 7 /dev/zero
7f0000400000-7f0000401000 r--s 00000000 fe:01 55 /var/lib/shared.db
7f0000500000-7f0000501000 r--p 00000000 00:01 8 /memfd:buffer (deleted)
7f0000600000-7f0000601000 r-xp 00000000 fe:01 77 /usr/local/bin/old (deleted)
7ffd00000000-7ffd00021000 rw-p 00000000 00:00 0 [stack]
7ffd00100000-7ffd00102000 r-xp 00000000 00:00 0 [vdso]
";
        let selected: Vec<_> = read_only_file_mappings(maps).collect();
        assert_eq!(
            selected,
            vec![
                (0x55d0c0000000, 0x55d0c0100000),
                (0x55d0c0100000, 0x55d0c0800000),
                (0x7f0000000000, 0x7f0000200000),
            ]
        );
    }
}
