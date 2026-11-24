//! Read-only per-node CPU pressure and parent sandbox cgroup metrics emitted
//! by node-manager.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use pplx_space_node_runtime_core::cgroup_paths::CGROUP_BASE;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{info, warn};

const EMIT_INTERVAL: Duration = Duration::from_secs(15);
const DEFAULT_PROC_PRESSURE_CPU_PATH: &str = "/proc/pressure/cpu";

/// Filesystem paths sampled for per-node CPU metrics.
#[derive(Debug, Clone)]
pub struct NodeCpuMetricSources {
    pub proc_pressure_cpu: PathBuf,
    pub parent_cgroup_cpu_stat: PathBuf,
}

impl Default for NodeCpuMetricSources {
    fn default() -> Self {
        Self {
            proc_pressure_cpu: PathBuf::from(DEFAULT_PROC_PRESSURE_CPU_PATH),
            parent_cgroup_cpu_stat: PathBuf::from(CGROUP_BASE).join("cpu.stat"),
        }
    }
}

/// Spawn the read-only per-node CPU metrics loop.
pub fn start_node_cpu_metrics(shutdown_token: CancellationToken, task_tracker: &TaskTracker) {
    task_tracker.spawn(run_loop(NodeCpuMetricSources::default(), shutdown_token));
}

async fn run_loop(sources: NodeCpuMetricSources, shutdown_token: CancellationToken) {
    info!(
        interval_secs = EMIT_INTERVAL.as_secs(),
        proc_pressure_cpu = %sources.proc_pressure_cpu.display(),
        parent_cgroup_cpu_stat = %sources.parent_cgroup_cpu_stat.display(),
        "node CPU metrics loop starting",
    );

    let mut interval = tokio::time::interval(EMIT_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut sampler = NodeCpuSampler::new(sources);

    loop {
        tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => return,
            _ = interval.tick() => {
                match tokio::task::spawn_blocking(move || {
                    sampler.sample_and_emit();
                    sampler
                })
                .await
                {
                    Ok(updated) => sampler = updated,
                    Err(error) => {
                        warn!(%error, "node CPU metrics sample task failed; stopping loop");
                        return;
                    }
                }
            }
        }
    }
}

/// Stateful sampler that tracks previous `cpu.stat` readings so each tick can
/// emit per-second delta rates alongside instantaneous CPU pressure.
#[derive(Debug)]
pub struct NodeCpuSampler {
    sources: NodeCpuMetricSources,
    prev_cgroup_stat: Option<(Instant, CgroupCpuStat)>,
    warned_pressure: bool,
    warned_cgroup_stat: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct NodeCpuTickSample {
    pub pressure_some_avg10: Option<f64>,
    pub cgroup_usage_usec_per_sec: Option<f64>,
    pub cgroup_nr_throttled_per_sec: Option<f64>,
    pub cgroup_throttled_usec_per_sec: Option<f64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct CgroupCpuStat {
    usage_usec: Option<u64>,
    nr_throttled: Option<u64>,
    throttled_usec: Option<u64>,
}

impl NodeCpuSampler {
    pub fn new(sources: NodeCpuMetricSources) -> Self {
        Self {
            sources,
            prev_cgroup_stat: None,
            warned_pressure: false,
            warned_cgroup_stat: false,
        }
    }

    pub fn sample_and_emit(&mut self) {
        let _ = self.sample_and_emit_at(Instant::now());
    }

    pub(crate) fn sample_and_emit_at(&mut self, now: Instant) -> NodeCpuTickSample {
        let mut tick = NodeCpuTickSample::default();

        match std::fs::read_to_string(&self.sources.proc_pressure_cpu) {
            Ok(content) => match parse_psi_some_avg10(&content) {
                Some(avg10) => {
                    if self.warned_pressure {
                        info!(path = %self.sources.proc_pressure_cpu.display(), "node CPU metrics PSI read recovered");
                        self.warned_pressure = false;
                    }
                    crate::metrics::node_cpu::pressure_some_avg10(avg10);
                    tick.pressure_some_avg10 = Some(avg10);
                }
                None => {
                    if !self.warned_pressure {
                        warn!(
                            path = %self.sources.proc_pressure_cpu.display(),
                            "node CPU metrics failed to parse CPU PSI some avg10"
                        );
                        self.warned_pressure = true;
                    }
                }
            },
            Err(error) => {
                if !self.warned_pressure {
                    warn!(
                        path = %self.sources.proc_pressure_cpu.display(),
                        %error,
                        "node CPU metrics failed to read CPU PSI"
                    );
                    self.warned_pressure = true;
                }
            }
        }

        match std::fs::read_to_string(&self.sources.parent_cgroup_cpu_stat) {
            Ok(content) => {
                let stat = parse_cgroup_cpu_stat(&content);
                if stat.is_empty() {
                    self.prev_cgroup_stat = None;
                    if !self.warned_cgroup_stat {
                        warn!(
                            path = %self.sources.parent_cgroup_cpu_stat.display(),
                            "node CPU metrics found no recognized keys in parent cgroup cpu.stat"
                        );
                        self.warned_cgroup_stat = true;
                    }
                } else {
                    if self.warned_cgroup_stat {
                        info!(
                            path = %self.sources.parent_cgroup_cpu_stat.display(),
                            "node CPU metrics parent cgroup cpu.stat read recovered"
                        );
                        self.warned_cgroup_stat = false;
                    }
                    if let Some((prev_at, prev_stat)) = self.prev_cgroup_stat.replace((now, stat)) {
                        if let Some(rate) =
                            counter_rate(prev_at, prev_stat.usage_usec, now, stat.usage_usec)
                        {
                            crate::metrics::node_cpu::space_cgroup_usage_usec_per_sec(rate);
                            tick.cgroup_usage_usec_per_sec = Some(rate);
                        }
                        if let Some(rate) =
                            counter_rate(prev_at, prev_stat.nr_throttled, now, stat.nr_throttled)
                        {
                            crate::metrics::node_cpu::space_cgroup_nr_throttled_per_sec(rate);
                            tick.cgroup_nr_throttled_per_sec = Some(rate);
                        }
                        if let Some(rate) = counter_rate(
                            prev_at,
                            prev_stat.throttled_usec,
                            now,
                            stat.throttled_usec,
                        ) {
                            crate::metrics::node_cpu::space_cgroup_throttled_usec_per_sec(rate);
                            tick.cgroup_throttled_usec_per_sec = Some(rate);
                        }
                    }
                }
            }
            Err(error) => {
                self.prev_cgroup_stat = None;
                if !self.warned_cgroup_stat {
                    warn!(
                        path = %self.sources.parent_cgroup_cpu_stat.display(),
                        %error,
                        "node CPU metrics failed to read parent cgroup cpu.stat"
                    );
                    self.warned_cgroup_stat = true;
                }
            }
        }

        tick
    }
}

impl CgroupCpuStat {
    fn is_empty(&self) -> bool {
        self.usage_usec.is_none() && self.nr_throttled.is_none() && self.throttled_usec.is_none()
    }
}

fn counter_rate(
    prev_at: Instant,
    prev_value: Option<u64>,
    now: Instant,
    curr_value: Option<u64>,
) -> Option<f64> {
    let elapsed_secs = now.checked_duration_since(prev_at)?.as_secs_f64();
    if elapsed_secs <= 0.0 {
        return None;
    }
    let delta = curr_value?.checked_sub(prev_value?)?;
    let rate = (delta as f64) / elapsed_secs;
    rate.is_finite().then_some(rate)
}

fn parse_psi_some_avg10(content: &str) -> Option<f64> {
    let line = content
        .lines()
        .find(|line| line.split_whitespace().next() == Some("some"))?;
    let avg10: f64 = line.split_whitespace().find_map(|pair| {
        let (name, value) = pair.split_once('=')?;
        (name == "avg10").then(|| value.parse().ok()).flatten()
    })?;
    (avg10.is_finite() && avg10 >= 0.0).then_some(avg10)
}

fn parse_cgroup_cpu_stat(content: &str) -> CgroupCpuStat {
    let mut stat = CgroupCpuStat::default();
    for line in content.lines() {
        let mut fields = line.split_whitespace();
        let (Some(key), Some(raw_value), None) = (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        let Ok(value) = raw_value.parse::<u64>() else {
            continue;
        };
        match key {
            "usage_usec" => stat.usage_usec = Some(value),
            "nr_throttled" => stat.nr_throttled = Some(value),
            "throttled_usec" => stat.throttled_usec = Some(value),
            _ => {}
        }
    }
    stat
}

#[cfg(test)]
mod tests;
