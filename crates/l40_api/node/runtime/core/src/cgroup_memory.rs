//! Read-only memory accounting of a cgroup v2 directory, such as the node's
//! sandbox cgroup parent, [`CGROUP_BASE`](crate::cgroup_paths::CGROUP_BASE).

use std::io;
use std::path::{Path, PathBuf};

/// One reading of a cgroup's memory files. Byte counts are as the kernel
/// reports them; a `memory.stat` key the running kernel does not report is
/// `None`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct CgroupMemoryState {
    /// `memory.max`; `None` when unlimited (`max`).
    pub max_bytes: Option<u64>,
    /// `memory.current`.
    pub current_bytes: u64,
    pub stat: CgroupMemoryStat,
    pub events: CgroupMemoryEvents,
    /// `memory.pressure`; `None` when PSI is unavailable.
    pub pressure: Option<CgroupMemoryPressure>,
}

impl CgroupMemoryState {
    /// Anonymous plus kernel memory: what reclaim cannot drop without
    /// swap, which sandbox cgroups do not have. `None` when the kernel
    /// reports no `anon` or `kernel`.
    pub fn unreclaimable_bytes(&self) -> Option<u64> {
        self.stat.anon?.checked_add(self.stat.kernel?)
    }

    /// `memory.max` minus [`Self::unreclaimable_bytes`], saturating at zero:
    /// what the cgroup can still fault in before it must reclaim file cache
    /// and then OOM-kill. `None` when unlimited or when unreclaimable memory
    /// is unknown.
    pub fn headroom_bytes(&self) -> Option<u64> {
        Some(self.max_bytes?.saturating_sub(self.unreclaimable_bytes()?))
    }
}

/// Selected `memory.stat` keys, in bytes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CgroupMemoryStat {
    pub anon: Option<u64>,
    pub file: Option<u64>,
    pub kernel: Option<u64>,
    pub pagetables: Option<u64>,
    pub sec_pagetables: Option<u64>,
    pub shmem: Option<u64>,
}

/// Cumulative `memory.events` counters, hierarchical over descendants.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CgroupMemoryEvents {
    pub low: u64,
    pub high: u64,
    pub max: u64,
    pub oom: u64,
    pub oom_kill: u64,
    pub oom_group_kill: u64,
}

/// `memory.pressure` stall averages, in percent, and totals, in microseconds.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct CgroupMemoryPressure {
    pub some_avg10: f64,
    pub full_avg10: f64,
    pub some_total_us: u64,
    pub full_total_us: u64,
}

/// A best-effort sample of the memory charged to one cgroup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CgroupMemoryUsage {
    pub current_bytes: u64,
    pub anon_bytes: Option<u64>,
    pub limit: CgroupMemoryLimit,
}

/// `memory.max`, distinguishing an unlimited cgroup from an unreadable limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CgroupMemoryLimit {
    Limited(u64),
    Unlimited,
    Unknown,
}

/// Reads only usage and limit files, without reading event counters or PSI.
/// An invalid `memory.current` is an error; missing or malformed limit and
/// anonymous memory readings do not hide an otherwise valid usage sample.
pub fn read_cgroup_memory_usage(cgroup: &Path) -> io::Result<CgroupMemoryUsage> {
    let read = |file| std::fs::read_to_string(cgroup.join(file));
    let current_bytes = parse_u64(cgroup, "memory.current", read("memory.current")?.trim())?;
    let anon_bytes = read("memory.stat")
        .and_then(|stat| parse_keyed_u64(cgroup, "memory.stat", &stat, "anon"))
        .ok()
        .flatten();
    let limit = match read("memory.max").and_then(|max| parse_memory_max(cgroup, &max)) {
        Ok(Some(bytes)) => CgroupMemoryLimit::Limited(bytes),
        Ok(None) => CgroupMemoryLimit::Unlimited,
        Err(_) => CgroupMemoryLimit::Unknown,
    };
    Ok(CgroupMemoryUsage {
        current_bytes,
        anon_bytes,
        limit,
    })
}

/// Reads the memory files of the cgroup at `cgroup`. Fails when `memory.max`,
/// `memory.current`, `memory.stat` or `memory.events` is unreadable, or when
/// one of their values read here is malformed; an unreadable
/// `memory.pressure` yields `pressure: None`.
pub fn read_cgroup_memory_state(cgroup: &Path) -> io::Result<CgroupMemoryState> {
    let read = |file: &str| std::fs::read_to_string(cgroup.join(file));
    let max = read("memory.max")?;
    let current = read("memory.current")?;
    let stat = read("memory.stat")?;
    let events = read("memory.events")?;
    let keyed = |file: &str, content: &str, key: &str| parse_keyed_u64(cgroup, file, content, key);
    // A counter the running kernel does not report has never fired.
    let event = |key: &str| Ok::<_, io::Error>(keyed("memory.events", &events, key)?.unwrap_or(0));
    Ok(CgroupMemoryState {
        max_bytes: parse_memory_max(cgroup, &max)?,
        current_bytes: parse_u64(cgroup, "memory.current", current.trim())?,
        stat: CgroupMemoryStat {
            anon: keyed("memory.stat", &stat, "anon")?,
            file: keyed("memory.stat", &stat, "file")?,
            kernel: keyed("memory.stat", &stat, "kernel")?,
            pagetables: keyed("memory.stat", &stat, "pagetables")?,
            sec_pagetables: keyed("memory.stat", &stat, "sec_pagetables")?,
            shmem: keyed("memory.stat", &stat, "shmem")?,
        },
        events: CgroupMemoryEvents {
            low: event("low")?,
            high: event("high")?,
            max: event("max")?,
            oom: event("oom")?,
            oom_kill: event("oom_kill")?,
            oom_group_kill: event("oom_group_kill")?,
        },
        pressure: read("memory.pressure")
            .ok()
            .and_then(|content| parse_pressure(&content)),
    })
}

fn parse_memory_max(cgroup: &Path, contents: &str) -> io::Result<Option<u64>> {
    match contents.trim() {
        "max" => Ok(None),
        value => parse_u64(cgroup, "memory.max", value).map(Some),
    }
}

fn parse_keyed_u64(cgroup: &Path, file: &str, content: &str, key: &str) -> io::Result<Option<u64>> {
    keyed_u64(content, key)
        .map(|value| parse_u64(cgroup, &format!("{file} {key}"), value))
        .transpose()
}

fn parse_u64(cgroup: &Path, file: &str, value: &str) -> io::Result<u64> {
    value.parse().map_err(|_| {
        let path: PathBuf = cgroup.join(file);
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid {}: {value:?}", path.display()),
        )
    })
}

/// The value of a `key value` line, as in `memory.stat` and `memory.events`.
fn keyed_u64<'a>(content: &'a str, key: &str) -> Option<&'a str> {
    content.lines().find_map(|line| {
        let (name, value) = line.split_once(' ')?;
        (name == key).then(|| value.trim())
    })
}

/// Parses `some avg10=.. avg60=.. avg300=.. total=..` and the matching
/// `full` line.
fn parse_pressure(content: &str) -> Option<CgroupMemoryPressure> {
    let field = |kind: &str, name: &str| {
        let line = content
            .lines()
            .find(|line| line.split_whitespace().next() == Some(kind))?;
        line.split_whitespace()
            .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
            .map(str::to_owned)
    };
    Some(CgroupMemoryPressure {
        some_avg10: field("some", "avg10")?.parse().ok()?,
        full_avg10: field("full", "avg10")?.parse().ok()?,
        some_total_us: field("some", "total")?.parse().ok()?,
        full_total_us: field("full", "total")?.parse().ok()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_cgroup(dir: &Path, files: &[(&str, &str)]) {
        for (name, content) in files {
            std::fs::write(dir.join(name), content).unwrap();
        }
    }

    const STAT: &str = "anon 3000\nfile 2000\nkernel 700\nkernel_stack 50\n\
        pagetables 300\nsec_pagetables 40\npercpu 10\nshmem 100\nfile_mapped 5\n";
    const EVENTS: &str = "low 0\nhigh 4\nmax 9\noom 2\noom_kill 1\noom_group_kill 0\n";
    const PRESSURE: &str = "some avg10=12.50 avg60=3.00 avg300=0.10 total=987654\n\
        full avg10=4.25 avg60=1.00 avg300=0.00 total=123456\n";

    #[test]
    fn reads_limited_cgroup_with_pressure() {
        let dir = tempfile::tempdir().unwrap();
        write_cgroup(
            dir.path(),
            &[
                ("memory.max", "8589934592\n"),
                ("memory.current", "6100\n"),
                ("memory.stat", STAT),
                ("memory.events", EVENTS),
                ("memory.pressure", PRESSURE),
            ],
        );

        let state = read_cgroup_memory_state(dir.path()).unwrap();

        assert_eq!(state.unreclaimable_bytes(), Some(3700));
        assert_eq!(state.headroom_bytes(), Some(8_589_934_592 - 3700));
        assert_eq!(
            state,
            CgroupMemoryState {
                max_bytes: Some(8_589_934_592),
                current_bytes: 6100,
                stat: CgroupMemoryStat {
                    anon: Some(3000),
                    file: Some(2000),
                    kernel: Some(700),
                    pagetables: Some(300),
                    sec_pagetables: Some(40),
                    shmem: Some(100),
                },
                events: CgroupMemoryEvents {
                    low: 0,
                    high: 4,
                    max: 9,
                    oom: 2,
                    oom_kill: 1,
                    oom_group_kill: 0,
                },
                pressure: Some(CgroupMemoryPressure {
                    some_avg10: 12.5,
                    full_avg10: 4.25,
                    some_total_us: 987_654,
                    full_total_us: 123_456,
                }),
            }
        );
    }

    #[test]
    fn unlimited_cgroup_on_older_kernel_without_psi() {
        let dir = tempfile::tempdir().unwrap();
        write_cgroup(
            dir.path(),
            &[
                ("memory.max", "max\n"),
                ("memory.current", "10\n"),
                ("memory.stat", "anon 7\nfile 3\n"),
                ("memory.events", "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n"),
            ],
        );

        let state = read_cgroup_memory_state(dir.path()).unwrap();

        assert_eq!(state.max_bytes, None);
        assert_eq!(state.headroom_bytes(), None);
        assert_eq!(state.unreclaimable_bytes(), None);
        assert_eq!(state.stat.anon, Some(7));
        assert_eq!(state.stat.kernel, None);
        assert_eq!(state.events.oom_group_kill, 0);
        assert_eq!(state.pressure, None);
    }

    #[test]
    fn rejects_missing_or_malformed_required_files() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            read_cgroup_memory_state(dir.path()).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );

        write_cgroup(
            dir.path(),
            &[
                ("memory.max", "lots\n"),
                ("memory.current", "10\n"),
                ("memory.stat", STAT),
                ("memory.events", EVENTS),
            ],
        );
        let error = read_cgroup_memory_state(dir.path()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);

        for (file, content) in [
            ("memory.stat", "anon invalid\n"),
            ("memory.events", "oom invalid\n"),
        ] {
            write_cgroup(
                dir.path(),
                &[
                    ("memory.max", "max\n"),
                    ("memory.stat", STAT),
                    ("memory.events", EVENTS),
                    (file, content),
                ],
            );
            let error = read_cgroup_memory_state(dir.path()).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{file}");
        }
    }
}
