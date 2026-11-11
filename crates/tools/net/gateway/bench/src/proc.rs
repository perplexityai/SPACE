//! Gateway CPU and memory, read straight out of `/proc`.
//!
//! CPU comes from `/proc/<pid>/task/*/schedstat`, summed over threads. The
//! per-process `stat` counters are the obvious source and the wrong one here:
//! they are quantised to clock ticks (10 ms), which is coarser than several of
//! the workloads take to run. `schedstat` is nanoseconds, and summing the
//! per-thread files is what makes it whole-process — the thread-group file
//! reports the leader alone, which for a Tokio runtime is nearly idle.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

#[derive(Debug, thiserror::Error)]
pub enum SampleError {
    #[error("failed to read {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{path} did not have the expected shape")]
    Shape { path: PathBuf },
}

/// One point-in-time read of the gateway process.
#[derive(Clone, Copy, Debug)]
pub struct Counters {
    pub cpu_ns: u64,
    pub rss_bytes: u64,
}

/// Resource use over one measured window.
#[derive(Clone, Copy, Debug)]
pub struct Usage {
    pub cpu_ns: u64,
    /// Highest RSS seen by the background sampler inside the window, including
    /// the reads that open and close it.
    pub rss_peak_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct Mark {
    cpu_ns: u64,
}

/// Samples one process, and tracks peak RSS between [`Monitor::begin`] and
/// [`Monitor::end`].
pub struct Monitor {
    pid: u32,
    peak_rss: Arc<AtomicU64>,
    shutdown: CancellationToken,
}

impl Monitor {
    /// Start the background RSS sampler. `interval` bounds how briefly a
    /// transient allocation can spike without being seen.
    pub fn spawn(pid: u32, interval: Duration) -> Result<Self, SampleError> {
        let peak_rss = Arc::new(AtomicU64::new(read_rss_bytes(pid)?));
        let shutdown = CancellationToken::new();
        let sampler = (peak_rss.clone(), shutdown.clone());
        tokio::spawn(async move {
            let (peak_rss, shutdown) = sampler;
            let mut ticker = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    _instant = ticker.tick() => {
                        if let Ok(rss) = read_rss_bytes(pid) {
                            peak_rss.fetch_max(rss, Ordering::Relaxed);
                        }
                    }
                }
            }
        });
        Ok(Self {
            pid,
            peak_rss,
            shutdown,
        })
    }

    pub fn read(&self) -> Result<Counters, SampleError> {
        Ok(Counters {
            cpu_ns: read_cpu_ns(self.pid)?,
            rss_bytes: read_rss_bytes(self.pid)?,
        })
    }

    pub fn begin(&self) -> Result<Mark, SampleError> {
        let counters = self.read()?;
        self.peak_rss.store(counters.rss_bytes, Ordering::Relaxed);
        Ok(Mark {
            cpu_ns: counters.cpu_ns,
        })
    }

    pub fn end(&self, mark: Mark) -> Result<Usage, SampleError> {
        let counters = self.read()?;
        Ok(Usage {
            cpu_ns: counters.cpu_ns.saturating_sub(mark.cpu_ns),
            rss_peak_bytes: self
                .peak_rss
                .fetch_max(counters.rss_bytes, Ordering::Relaxed)
                .max(counters.rss_bytes),
        })
    }
}

impl Drop for Monitor {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// Nanoseconds on CPU, summed over every thread of the process.
fn read_cpu_ns(pid: u32) -> Result<u64, SampleError> {
    let task_dir = PathBuf::from(format!("/proc/{pid}/task"));
    let entries = std::fs::read_dir(&task_dir).map_err(|source| SampleError::Read {
        path: task_dir.clone(),
        source,
    })?;
    let mut total = 0u64;
    for entry in entries {
        let entry = entry.map_err(|source| SampleError::Read {
            path: task_dir.clone(),
            source,
        })?;
        let path = entry.path().join("schedstat");
        // A thread can exit between the readdir and the read; its time is lost
        // either way, and the Tokio worker threads that carry the proxy's work
        // live for the whole process.
        let Ok(contents) = std::fs::read_to_string(&path) else {
            continue;
        };
        let on_cpu_ns = contents
            .split_whitespace()
            .next()
            .and_then(|field| field.parse::<u64>().ok())
            .ok_or(SampleError::Shape { path })?;
        total = total.saturating_add(on_cpu_ns);
    }
    Ok(total)
}

fn read_rss_bytes(pid: u32) -> Result<u64, SampleError> {
    let path = PathBuf::from(format!("/proc/{pid}/status"));
    let contents = std::fs::read_to_string(&path).map_err(|source| SampleError::Read {
        path: path.clone(),
        source,
    })?;
    let kib = contents
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or(SampleError::Shape { path })?;
    Ok(kib.saturating_mul(1024))
}
