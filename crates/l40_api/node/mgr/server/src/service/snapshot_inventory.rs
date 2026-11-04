//! Resumable identity enumeration. Retention does not need a materialized
//! whole-directory census or successful extent measurements to make progress.
use std::collections::HashMap;
use std::fs::ReadDir;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pplx_space_common_core::types::SandboxId;
use tonic::Status;
use uuid::Uuid;

const PAGE_ENTRIES: usize = 256;
const MAX_SCANS: usize = 16;
const IDLE_TTL: Duration = Duration::from_secs(600);

#[derive(Clone, Default)]
pub(super) struct SnapshotInventory(Arc<Mutex<HashMap<Uuid, Scan>>>);

struct Scan {
    sandbox: SandboxId,
    entries: ReadDir,
    started: Instant,
    touched: Instant,
}

impl SnapshotInventory {
    pub(super) fn page(
        &self,
        sandbox: SandboxId,
        root: &Path,
        token: &str,
    ) -> Result<(Vec<PathBuf>, String, Duration), Status> {
        let mut scans = self
            .0
            .try_lock()
            .map_err(|_| Status::resource_exhausted("snapshot inventory busy; retry"))?;
        scans.retain(|_, scan| scan.touched.elapsed() < IDLE_TTL);
        let mut scan = if token.is_empty() {
            let entries = match std::fs::read_dir(root.join("snapshots")) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok((Vec::new(), String::new(), Duration::ZERO));
                }
                Err(error) => return Err(Status::unavailable(format!("open snapshots: {error}"))),
            };
            Scan {
                sandbox,
                entries,
                started: Instant::now(),
                touched: Instant::now(),
            }
        } else {
            let id = token
                .parse::<Uuid>()
                .map_err(|_| Status::invalid_argument("invalid snapshot cursor"))?;
            let scan = scans.get(&id).ok_or_else(|| {
                Status::aborted("snapshot cursor expired or consumed; restart scan")
            })?;
            if scan.sandbox != sandbox {
                return Err(Status::invalid_argument(
                    "snapshot cursor belongs to another sandbox",
                ));
            }
            scans.remove(&id).expect("cursor checked under lock")
        };
        let started = Instant::now();
        let mut paths = Vec::new();
        for _ in 0..PAGE_ENTRIES {
            if started.elapsed() >= Duration::from_secs(2) {
                break;
            }
            let Some(entry) = scan.entries.next() else {
                return Ok((paths, String::new(), scan.started.elapsed()));
            };
            let entry =
                entry.map_err(|error| Status::unavailable(format!("read snapshots: {error}")))?;
            let kind = entry
                .file_type()
                .map_err(|error| Status::unavailable(format!("snapshot type: {error}")))?;
            if kind.is_dir() {
                paths.push(entry.path());
            }
        }
        // Lost replies cannot consume capacity forever. Evicted readers restart;
        // retention has already safely deleted only entries older than its keep set.
        if scans.len() >= MAX_SCANS
            && let Some(oldest) = scans
                .iter()
                .min_by_key(|(_, scan)| scan.touched)
                .map(|(id, _)| *id)
        {
            scans.remove(&oldest);
        }
        scan.touched = Instant::now();
        let elapsed = scan.started.elapsed();
        let id = Uuid::now_v7();
        scans.insert(id, scan);
        Ok((paths, id.to_string(), elapsed))
    }
}

#[cfg(test)]
mod tests;
