//! Read-only, bounded walks. Cursors retain directory iterators, not a growing
//! materialized inventory. Results are candidates, never deletion authority.

use std::collections::{HashMap, HashSet};
use std::fs::ReadDir;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pplx_space_common_core::types::SandboxId;
use tonic::Status;
use uuid::Uuid;

use super::{CACHE_DIR, RUN_DIR};
use crate::proto::{ListOrphanCandidatesRequest, ListOrphanCandidatesResponse};
use crate::slot_pool::SlotRecord;

const PAGE_ENTRIES: usize = 256;
const PAGE_BUDGET: Duration = Duration::from_secs(2);
const CURSOR_IDLE_TTL: Duration = Duration::from_secs(600);
const MAX_SCANS: usize = 4;
const MAX_WARNINGS: usize = 8;

#[derive(Clone, Default)]
pub(super) struct OrphanInventory(Arc<Mutex<HashMap<Uuid, Scan>>>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Source {
    Namespaces,
    Cgroups,
    RunDirs,
    GatewayRunDirs,
    CacheDirs,
    Processes,
    Nft,
    Done,
}

impl Source {
    fn next(self) -> Self {
        match self {
            Self::Namespaces => Self::Cgroups,
            Self::Cgroups => Self::RunDirs,
            Self::RunDirs => Self::GatewayRunDirs,
            Self::GatewayRunDirs => Self::CacheDirs,
            Self::CacheDirs => Self::Processes,
            Self::Processes => Self::Nft,
            Self::Nft | Self::Done => Self::Done,
        }
    }

    fn root(self) -> Option<usize> {
        match self {
            Self::Namespaces => Some(0),
            Self::Cgroups => Some(1),
            Self::RunDirs => Some(2),
            Self::CacheDirs => Some(3),
            Self::Processes => Some(4),
            Self::GatewayRunDirs => Some(5),
            Self::Nft | Self::Done => None,
        }
    }
}

struct Scan {
    sequence: u64,
    touched: Instant,
    started_at: i64,
    failed: bool,
    source: Source,
    entries: Option<ReadDir>,
    nft: Option<std::vec::IntoIter<String>>,
}

impl Scan {
    fn new() -> Self {
        Self {
            sequence: 0,
            touched: Instant::now(),
            started_at: jiff::Timestamp::now().as_millisecond(),
            failed: false,
            source: Source::Namespaces,
            entries: None,
            nft: None,
        }
    }

    fn warning(&mut self, page: &mut ListOrphanCandidatesResponse, error: impl std::fmt::Display) {
        self.failed = true;
        if page.warnings.len() < MAX_WARNINGS {
            page.warnings.push(format!("{:?}: {error}", self.source));
        }
    }

    fn advance(&mut self) {
        self.entries = None;
        self.source = self.source.next();
    }

    fn page(
        &mut self,
        evidence: &Evidence,
        roots: &[PathBuf; 6],
        nft: impl FnOnce() -> Result<Vec<String>, Status>,
    ) -> ListOrphanCandidatesResponse {
        let started = Instant::now();
        let mut page = ListOrphanCandidatesResponse {
            scan_started_at_unix_ms: self.started_at,
            observed_at_unix_ms: jiff::Timestamp::now().as_millisecond(),
            ..Default::default()
        };
        let mut nft = Some(nft);
        for _ in 0..PAGE_ENTRIES {
            if started.elapsed() >= PAGE_BUDGET || self.source == Source::Done {
                break;
            }
            if let Some(root) = self.source.root().and_then(|index| roots.get(index)) {
                if self.entries.is_none() {
                    match std::fs::read_dir(root) {
                        Ok(entries) => self.entries = Some(entries),
                        Err(error) => {
                            // Missing roots are incomplete evidence too: a
                            // missing /proc or cache mount is not an empty host.
                            self.warning(&mut page, error);
                            self.advance();
                            continue;
                        }
                    }
                }
                match self.entries.as_mut().and_then(Iterator::next) {
                    Some(Ok(entry)) => {
                        if let Err(error) = inspect_entry(self.source, entry, evidence, &mut page) {
                            self.warning(&mut page, error);
                        }
                    }
                    Some(Err(error)) => {
                        self.warning(&mut page, error);
                        self.advance();
                    }
                    None => self.advance(),
                }
            } else if self.source == Source::Nft {
                if self.nft.is_none() {
                    match nft.take().expect("nft source initialized once per page")() {
                        Ok(names) => {
                            self.nft = Some(names.into_iter());
                            // Listing itself can consume the page's time budget.
                            continue;
                        }
                        Err(error) => {
                            self.warning(&mut page, error);
                            self.advance();
                            continue;
                        }
                    }
                }
                match self.nft.as_mut().and_then(Iterator::next) {
                    Some(name) => match crate::net::try_link_exists_in(&name, None) {
                        Ok(false) => page.orphaned_nft_rules.push(name),
                        Ok(true) => {}
                        Err(error) => self.warning(&mut page, error),
                    },
                    None => self.advance(),
                }
            }
        }
        self.touched = Instant::now();
        self.sequence += 1;
        page.complete = self.source == Source::Done && !self.failed;
        page
    }
}

struct Evidence {
    sandboxes: HashSet<String>,
    slots: HashSet<String>,
    pids: HashSet<u32>,
}

impl Evidence {
    fn from_records(records: &[SlotRecord]) -> Self {
        Self {
            sandboxes: records
                .iter()
                .filter(|r| !r.info.sandbox_id.is_empty())
                .map(|r| r.info.sandbox_id.clone())
                .collect(),
            slots: records.iter().map(|r| r.info.slot_label()).collect(),
            pids: records.iter().filter_map(|r| r.info.pid).collect(),
        }
    }
}

fn inspect_entry(
    source: Source,
    entry: std::fs::DirEntry,
    evidence: &Evidence,
    page: &mut ListOrphanCandidatesResponse,
) -> io::Result<()> {
    let name = entry.file_name().to_string_lossy().into_owned();
    let path = entry.path().display().to_string();
    match source {
        Source::Namespaces => {
            if let Some(label) = name.strip_prefix("sns-")
                && !evidence.slots.contains(label)
            {
                page.orphaned_namespaces.push(name);
            }
        }
        Source::Cgroups => {
            let id = name.strip_prefix("sbx-").unwrap_or(&name);
            if id.parse::<SandboxId>().is_ok() && !evidence.sandboxes.contains(id) {
                page.orphaned_cgroups.push(path);
            }
        }
        Source::RunDirs | Source::GatewayRunDirs | Source::CacheDirs => {
            // Shared service/cache directories are not sandbox resources.
            if name.parse::<SandboxId>().is_ok() && !evidence.sandboxes.contains(&name) {
                match source {
                    Source::RunDirs | Source::GatewayRunDirs => page.orphaned_run_dirs.push(path),
                    Source::CacheDirs => page.orphaned_cache_dirs.push(path),
                    _ => unreachable!("directory source matched above"),
                }
            }
        }
        Source::Processes => {
            let Ok(pid) = name.parse::<u32>() else {
                return Ok(());
            };
            if evidence.pids.contains(&pid) {
                return Ok(());
            }
            let comm = match read_proc(&entry.path().join("comm")) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                other => other?,
            };
            let named_runtime = matches!(
                comm.trim(),
                "firecracker" | "cloud-hyperviso" | "space-spaced-ho" | "space-netgtw-sv"
            );
            if !named_runtime {
                // execveat may leave the descriptor number in comm. Resolve
                // the executable link without trusting the process's argv.
                let executable = match std::fs::read_link(entry.path().join("exe")) {
                    Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                    other => other?,
                };
                let name = executable.file_name().and_then(|name| name.to_str());
                if !matches!(
                    name.map(|name| name.trim_end_matches(" (deleted)")),
                    Some(
                        "firecracker"
                            | "cloud-hypervisor"
                            | "space-spaced-host-svc"
                            | "space-netgtw-svc"
                    )
                ) {
                    return Ok(());
                }
            }
            let cgroup = match read_proc(&entry.path().join("cgroup")) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                other => other?,
            };
            // Walk all ancestors, including sidecar sbx-<id> cgroups. A PID
            // missing from pidfiles can still belong to a tracked lease.
            let ids: Vec<_> = cgroup
                .lines()
                .flat_map(|line| line.trim_end_matches(" (deleted)").split('/'))
                .filter_map(|part| {
                    part.strip_prefix("sbx-")
                        .unwrap_or(part)
                        .parse::<SandboxId>()
                        .ok()
                })
                .collect();
            if !ids
                .iter()
                .any(|id| evidence.sandboxes.contains(&id.to_string()))
            {
                let id = ids
                    .first()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "unknown".into());
                page.orphaned_processes.push(format!("{pid}:{id}"));
            }
        }
        Source::Nft | Source::Done => unreachable!("not a directory source"),
    }
    Ok(())
}

fn read_proc(path: &Path) -> io::Result<String> {
    let mut text = String::new();
    std::fs::File::open(path)?
        .take(4097)
        .read_to_string(&mut text)?;
    if text.len() > 4096 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "process evidence exceeds budget",
        ));
    }
    Ok(text)
}

impl OrphanInventory {
    pub(super) fn page(
        &self,
        request: ListOrphanCandidatesRequest,
        records: &[SlotRecord],
    ) -> Result<ListOrphanCandidatesResponse, Status> {
        self.page_with(
            request,
            records,
            &[
                "/var/run/netns".into(),
                "/sys/fs/cgroup/space".into(),
                RUN_DIR.into(),
                CACHE_DIR.into(),
                "/proc".into(),
                pplx_space_common_core::host::linux::paths::NET_GATEWAY_RUN_DIR.into(),
            ],
            nft_candidates,
        )
    }

    fn page_with(
        &self,
        request: ListOrphanCandidatesRequest,
        records: &[SlotRecord],
        roots: &[PathBuf; 6],
        nft: impl FnOnce() -> Result<Vec<String>, Status>,
    ) -> Result<ListOrphanCandidatesResponse, Status> {
        let mut scans = self
            .0
            .try_lock()
            .map_err(|_| Status::resource_exhausted("orphan inventory is busy; retry"))?;
        scans.retain(|_, scan| scan.touched.elapsed() < CURSOR_IDLE_TTL);
        let id = if request.page_token.is_empty() {
            if scans.len() >= MAX_SCANS {
                // A lost response can strand its single-use cursor. Do not
                // lock out diagnostics until TTL expiry: evict the least
                // recently used scan; its next continuation fails Aborted.
                // The registry lock excludes any currently running page.
                if let Some(oldest) = scans
                    .iter()
                    .min_by_key(|(_, scan)| scan.touched)
                    .map(|(id, _)| *id)
                {
                    scans.remove(&oldest);
                }
            }
            let id = Uuid::now_v7();
            scans.insert(id, Scan::new());
            id
        } else {
            let (id, sequence) = request
                .page_token
                .split_once(':')
                .ok_or_else(|| Status::invalid_argument("invalid orphan page token"))?;
            let id: Uuid = id
                .parse()
                .map_err(|_| Status::invalid_argument("invalid orphan cursor id"))?;
            let sequence: u64 = sequence
                .parse()
                .map_err(|_| Status::invalid_argument("invalid orphan page sequence"))?;
            let scan = scans.get(&id).ok_or_else(|| {
                Status::aborted("orphan cursor expired or service restarted; restart scan")
            })?;
            if sequence != scan.sequence {
                return Err(Status::aborted(
                    "orphan cursor already consumed; restart scan",
                ));
            }
            id
        };
        let scan = scans
            .get_mut(&id)
            .expect("cursor inserted or checked above");
        let mut page = scan.page(&Evidence::from_records(records), roots, nft);
        let finished = scan.source == Source::Done;
        if !finished {
            page.next_page_token = format!("{id}:{}", scan.sequence);
        }
        if finished {
            scans.remove(&id);
        }
        Ok(page)
    }
}

fn nft_candidates() -> Result<Vec<String>, Status> {
    let output = crate::host_command::output(
        crate::host_command::Command::new("nft")
            .args(["list", "chain", "inet", "space", "forward"]),
        crate::host_command::PROBE_BUDGET,
        &[],
    )
    .map_err(|error| Status::unavailable(format!("nft inventory: {error}")))?;
    if !output.status.success() {
        return Err(Status::unavailable(format!(
            "nft inventory command failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
                .chars()
                .take(512)
                .collect::<String>()
        )));
    }
    Ok(super::validation::concrete_sveth_names(
        &String::from_utf8_lossy(&output.stdout),
    ))
}

#[cfg(test)]
mod tests;
