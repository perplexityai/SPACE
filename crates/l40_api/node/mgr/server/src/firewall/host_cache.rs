//! Small, identity-pinned optimization. A name or prior success alone never
//! skips reconciliation: a hit also requires identical live nft evidence.

use std::fs::File;
use std::os::unix::fs::MetadataExt;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use super::{FirewallError, FirewallResult, SPACE_TABLE, nft_output};

const CAPACITY: usize = 8;
const MAX_RULE_BYTES: usize = 64 * 1024;
const MAX_AGE: Duration = Duration::from_secs(600);
static CACHE: OnceLock<Mutex<Vec<Entry>>> = OnceLock::new();

struct Entry {
    namespace: Option<String>,
    interface: String,
    // Holding the namespace inode prevents identity reuse while cached.
    file: File,
    rules: Vec<u8>,
    observed: Instant,
}

pub(super) struct Evidence {
    namespace: Option<String>,
    path: String,
    interface: String,
    file: File,
}

fn same_namespace(left: &File, right: &File) -> std::io::Result<bool> {
    let left = left.metadata()?;
    let right = right.metadata()?;
    Ok((left.dev(), left.ino()) == (right.dev(), right.ino()))
}

fn cached_rules(entries: &mut Vec<Entry>, evidence: &Evidence) -> Option<Vec<u8>> {
    entries.retain(|entry| entry.observed.elapsed() < MAX_AGE);
    entries
        .iter()
        .find(|entry| {
            entry.namespace == evidence.namespace
                && entry.interface == evidence.interface
                && same_namespace(&entry.file, &evidence.file).unwrap_or(false)
        })
        .map(|entry| entry.rules.clone())
}

fn remember(entries: &mut Vec<Entry>, entry: Entry) {
    entries.retain(|old| old.observed.elapsed() < MAX_AGE && old.namespace != entry.namespace);
    if entry.rules.len() <= MAX_RULE_BYTES {
        if entries.len() == CAPACITY {
            entries.remove(0);
        }
        entries.push(entry);
    }
}

impl Evidence {
    pub(super) fn open(namespace: Option<&str>, interface: &str) -> FirewallResult<Self> {
        let path = namespace
            .map(|name| format!("/run/netns/{name}"))
            .unwrap_or_else(|| "/proc/thread-self/ns/net".into());
        Ok(Self {
            file: File::open(&path).map_err(FirewallError::Exec)?,
            path,
            namespace: namespace.map(str::to_owned),
            interface: interface.into(),
        })
    }

    fn unchanged(&self) -> FirewallResult<()> {
        let current = File::open(&self.path).map_err(FirewallError::Exec)?;
        if !same_namespace(&self.file, &current).map_err(FirewallError::Exec)? {
            return Err(FirewallError::Exec(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "network namespace changed during firewall reconciliation",
            )));
        }
        Ok(())
    }

    pub(super) fn matches_live(&self) -> FirewallResult<bool> {
        let rules = {
            let mut entries = CACHE
                .get_or_init(Default::default)
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            cached_rules(&mut entries, self)
        };
        let Some(expected) = rules else {
            return Ok(false);
        };
        let actual = nft_output(
            self.namespace.as_deref(),
            &["list", "table", "inet", SPACE_TABLE],
        )
        .map_err(FirewallError::Exec)?;
        self.matches_output(&expected, &actual)
    }

    fn matches_output(
        &self,
        expected: &[u8],
        actual: &std::process::Output,
    ) -> FirewallResult<bool> {
        self.unchanged()?;
        Ok(actual.status.success() && actual.stdout == expected)
    }

    pub(super) fn remember(self) -> FirewallResult<()> {
        let output = nft_output(
            self.namespace.as_deref(),
            &["list", "table", "inet", SPACE_TABLE],
        )
        .map_err(FirewallError::Exec)?;
        self.unchanged()?;
        if !output.status.success() {
            return Err(FirewallError::ListFailed {
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            });
        }
        let mut entries = CACHE
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        remember(
            &mut entries,
            Entry {
                namespace: self.namespace,
                interface: self.interface,
                file: self.file,
                rules: output.stdout,
                observed: Instant::now(),
            },
        );
        Ok(())
    }
}

pub(super) fn invalidate(namespace: Option<&str>) {
    if let Some(cache) = CACHE.get() {
        cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|entry| entry.namespace.as_deref() != namespace);
    }
}

#[cfg(test)]
mod tests;
