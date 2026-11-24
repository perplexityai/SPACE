use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Read};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use pplx_space_node_runtime_core::cgroup_paths::CGROUP_BASE;
use pplx_space_sandbox_model::SandboxId;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::metrics::node_manager::{OomCgroup, cgroup_oom_events};

const SAMPLE_INTERVAL: Duration = Duration::from_secs(15);

pub(crate) fn start(shutdown: CancellationToken, tasks: &TaskTracker) {
    tasks.spawn(async move {
        let mut sampler = Sampler::default();
        let mut interval = tokio::time::interval(SAMPLE_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                _ = interval.tick() => {},
            }
            match tokio::task::spawn_blocking(move || {
                let deltas = sampler.sample(Path::new(CGROUP_BASE));
                for (cgroup, counts) in [
                    (OomCgroup::Parent, deltas.parent),
                    (OomCgroup::Vm, deltas.vm),
                    (OomCgroup::Sidecar, deltas.sidecar),
                ] {
                    cgroup_oom_events(cgroup, counts.kills, counts.group_kills);
                }
                sampler
            })
            .await
            {
                Ok(next) => sampler = next,
                Err(error) => {
                    tracing::warn!(%error, "cgroup OOM sampler stopped");
                    return;
                }
            }
        }
    });
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Counts {
    kills: u64,
    group_kills: u64,
}

impl Counts {
    fn delta(self, previous: Self) -> Self {
        Self {
            kills: self.kills.checked_sub(previous.kills).unwrap_or(self.kills),
            group_kills: self
                .group_kills
                .checked_sub(previous.group_kills)
                .unwrap_or(self.group_kills),
        }
    }

    fn add(&mut self, other: Self) {
        self.kills = self.kills.saturating_add(other.kills);
        self.group_kills = self.group_kills.saturating_add(other.group_kills);
    }
}

#[derive(Clone, Copy)]
struct Sample {
    // The file identity changes when a sandbox cgroup is recreated at the same path.
    file_id: (u64, u64),
    counts: Counts,
}

impl Sample {
    fn delta(self, previous: Self) -> Counts {
        if self.file_id == previous.file_id {
            self.counts.delta(previous.counts)
        } else {
            self.counts
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Deltas {
    parent: Counts,
    vm: Counts,
    sidecar: Counts,
}

#[derive(Default)]
struct Sampler {
    parent: Option<Sample>,
    // An entry without a reading was present but unreadable. Baseline its first
    // good sample rather than counting historical kills after a read failure.
    sandboxes: Option<HashMap<PathBuf, Option<Sample>>>,
}

impl Sampler {
    fn sample(&mut self, parent: &Path) -> Deltas {
        let mut deltas = Deltas::default();
        if let Some(current) = read_sample(parent)
            && let Some(previous) = self.parent.replace(current)
        {
            deltas.parent = current.delta(previous);
        }

        // A failed listing must not erase the baselines and replay live counts.
        let entries = match fs::read_dir(parent).and_then(Iterator::collect::<io::Result<Vec<_>>>) {
            Ok(entries) => entries,
            Err(error) => {
                if error.kind() != io::ErrorKind::NotFound {
                    tracing::debug!(%error, "cannot list sandbox cgroups for OOM sampling");
                }
                return deltas;
            }
        };
        let mut next = HashMap::new();
        for entry in entries {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let (id, total) = match name.strip_prefix("sbx-") {
                Some(id) => (id, &mut deltas.sidecar),
                None => (name, &mut deltas.vm),
            };
            if id.parse::<SandboxId>().is_err() {
                continue;
            }

            let path = entry.path();
            let previous = self.sandboxes.as_ref().and_then(|seen| seen.get(&path));
            let current = read_sample(&path);
            if let Some(current) = current
                && self.sandboxes.is_some()
            {
                match previous {
                    Some(Some(previous)) => total.add(current.delta(*previous)),
                    None => total.add(current.counts),
                    Some(None) => {}
                }
            }
            next.insert(path, current.or_else(|| previous.copied().flatten()));
        }
        // Forget removed children. Their kills remain in the parent's totals.
        self.sandboxes = Some(next);
        deltas
    }
}

fn read_sample(cgroup: &Path) -> Option<Sample> {
    match read_events(&cgroup.join("memory.events")) {
        Ok(sample) => Some(sample),
        Err(error) => {
            if error.kind() != io::ErrorKind::NotFound {
                tracing::debug!(path = %cgroup.display(), %error, "cannot read cgroup OOM events");
            }
            None
        }
    }
}

fn read_events(path: &Path) -> io::Result<Sample> {
    let mut file = File::open(path)?;
    let metadata = file.metadata()?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, "invalid memory.events counter");
    let (mut kills, mut group_kills) = (None, None);
    for line in contents.lines() {
        let mut fields = line.split_whitespace();
        let target = match fields.next() {
            Some("oom_kill") => &mut kills,
            Some("oom_group_kill") => &mut group_kills,
            _ => continue,
        };
        *target = Some(
            fields
                .next()
                .and_then(|value| value.parse().ok())
                .ok_or_else(invalid)?,
        );
    }
    Ok(Sample {
        file_id: (metadata.dev(), metadata.ino()),
        counts: Counts {
            kills: kills.ok_or_else(invalid)?,
            group_kills: group_kills.unwrap_or(0),
        },
    })
}

#[cfg(test)]
mod tests;
