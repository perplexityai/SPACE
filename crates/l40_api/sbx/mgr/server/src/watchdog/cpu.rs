//! Host CPU accounting beside health probes, never an input to health.

use std::fs::File;
use std::io::{self, Read};
use std::num::NonZeroU64;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::time::{Duration, Instant};

use pplx_space_common_core::types::{SandboxActivationAttemptId, SandboxId};
use rustix::fs::{Mode, OFlags, openat};

use super::{SidecarInfo, diagnostics::HostReads};
use crate::state::SandboxState;

const READ_TIMEOUT: Duration = Duration::from_millis(250);
const MIN_WINDOW: Duration = Duration::from_secs(10);
const MAX_WINDOW: Duration = Duration::from_secs(90);
const MAX_FILE_BYTES: u64 = 16 * 1024;

#[derive(Clone)]
pub(super) struct Reader {
    reads: HostReads,
}

impl Reader {
    pub(super) fn new() -> Self {
        Self {
            reads: HostReads::new(8),
        }
    }

    /// Includes waiting for a blocking-pool slot. A timed-out blocking read
    /// keeps its permit until it finishes, bounding stranded threads.
    pub(super) async fn read(
        &self,
        parent: &Path,
        info: &SidecarInfo,
        lifecycle: Option<SandboxState>,
    ) -> Option<Reading> {
        if lifecycle != Some(SandboxState::Started) {
            return None;
        }
        let sandbox_id = info.sandbox_id.parse::<SandboxId>().ok()?;
        let runtime_pid = info.runtime_pid?;
        let execution_id = info.node_execution_id;
        let path = pplx_space_node_runtime_core::cgroup_paths::jailer_cgroup_path_in(
            parent,
            &sandbox_id.to_string(),
        );
        let read = self
            .reads
            .blocking(move || read_at(&path, runtime_pid, execution_id));
        match tokio::time::timeout(READ_TIMEOUT, read).await {
            Ok(Some(Ok(reading))) => Some(reading),
            result => {
                tracing::debug!(
                    sandbox_id = %info.sandbox_id,
                    ?result,
                    "host CPU observation unavailable"
                );
                None
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Budget {
    Unlimited,
    Limited {
        quota: NonZeroU64,
        period: NonZeroU64,
    },
}

impl Budget {
    fn parse(text: &str) -> Option<Self> {
        let mut fields = text.split_whitespace();
        let quota = fields.next()?;
        let period: NonZeroU64 = fields.next()?.parse().ok()?;
        if fields.next().is_some() {
            return None;
        }
        Some(if quota == "max" {
            // Even an unlimited budget must have a valid period.
            Self::Unlimited
        } else {
            Self::Limited {
                quota: quota.parse().ok()?,
                period,
            }
        })
    }

    fn cores(self) -> Option<f64> {
        match self {
            Self::Unlimited => None,
            Self::Limited { quota, period } => Some(quota.get() as f64 / period.get() as f64),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Identity {
    device: u64,
    inode: u64,
    runtime_pid: u32,
    execution_id: Option<SandboxActivationAttemptId>,
}

#[derive(Clone, Debug)]
pub(super) struct Reading {
    identity: Identity,
    at: Instant,
    usage_usec: u64,
    budget: Budget,
}

fn usage_usec(text: &str) -> Option<u64> {
    let mut usage = None;
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        if fields.next() != Some("usage_usec") {
            continue;
        }
        if usage.is_some() {
            return None;
        }
        usage = Some(fields.next()?.parse().ok()?);
        if fields.next().is_some() {
            return None;
        }
    }
    usage
}

fn read_file(directory: &File, name: &str) -> io::Result<String> {
    let file = File::from(openat(
        directory,
        name,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )?);
    let mut content = String::new();
    file.take(MAX_FILE_BYTES + 1).read_to_string(&mut content)?;
    if content.len() as u64 > MAX_FILE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "CPU file too large",
        ));
    }
    Ok(content)
}

fn read_at(
    path: &Path,
    runtime_pid: u32,
    execution_id: Option<SandboxActivationAttemptId>,
) -> io::Result<Reading> {
    // Hold the directory open so replacement at the same path cannot mix two
    // cgroups' files in one observation.
    let directory = File::open(path)?;
    let metadata = directory.metadata()?;
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, "invalid cgroup CPU accounting");
    let budget = Budget::parse(&read_file(&directory, "cpu.max")?).ok_or_else(invalid)?;
    let usage_usec = usage_usec(&read_file(&directory, "cpu.stat")?).ok_or_else(invalid)?;
    let at = Instant::now();
    // A resize observed during the read has no unambiguous denominator.
    if Budget::parse(&read_file(&directory, "cpu.max")?) != Some(budget) {
        return Err(invalid());
    }
    Ok(Reading {
        identity: Identity {
            device: metadata.dev(),
            inode: metadata.ino(),
            runtime_pid,
            execution_id,
        },
        at,
        usage_usec,
        budget,
    })
}

#[derive(Clone, Debug, Default)]
pub(super) struct Baseline(Option<Reading>);

#[derive(Debug)]
pub(super) struct Usage {
    pub(super) cores: f64,
    pub(super) pct_of_requested: Option<f64>,
}

impl Baseline {
    pub(super) fn clear(&mut self) {
        self.0 = None;
    }

    /// Missing readings clear the baseline; neither old readings nor windows
    /// crossing a lifecycle edge, replacement or observed resize are replayed.
    pub(super) fn observe(&mut self, reading: Option<Reading>) -> Option<Usage> {
        let previous = std::mem::replace(&mut self.0, reading);
        let current = self.0.as_ref()?;
        let previous = previous?;
        if current.identity != previous.identity || current.budget != previous.budget {
            return None;
        }
        let elapsed = current.at.checked_duration_since(previous.at)?;
        if !(MIN_WINDOW..=MAX_WINDOW).contains(&elapsed) {
            return None;
        }
        let used = current.usage_usec.checked_sub(previous.usage_usec)?;
        let cores = used as f64 / (elapsed.as_secs_f64() * 1_000_000.0);
        Some(Usage {
            cores,
            pct_of_requested: current
                .budget
                .cores()
                .map(|requested| cores / requested * 100.0),
        })
    }
}

#[cfg(test)]
mod tests;
