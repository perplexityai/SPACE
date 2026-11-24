//! Read-only per-node CPU utilization, pressure, and parent sandbox cgroup
//! metrics emitted by node-manager.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use pplx_space_node_runtime_core::cgroup_paths::CGROUP_BASE;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{info, warn};

const EMIT_INTERVAL: Duration = Duration::from_secs(15);
const DEFAULT_PROC_STAT_PATH: &str = "/proc/stat";
const DEFAULT_PROC_PRESSURE_CPU_PATH: &str = "/proc/pressure/cpu";

/// Filesystem paths sampled for per-node CPU metrics.
#[derive(Debug, Clone)]
pub struct NodeCpuMetricSources {
    pub proc_stat: PathBuf,
    pub proc_pressure_cpu: PathBuf,
    pub parent_cgroup_cpu_stat: PathBuf,
}

impl Default for NodeCpuMetricSources {
    fn default() -> Self {
        Self {
            proc_stat: PathBuf::from(DEFAULT_PROC_STAT_PATH),
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
        proc_stat = %sources.proc_stat.display(),
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

/// Stateful sampler that tracks previous `/proc/stat` and `cpu.stat` readings
/// so each tick can emit delta-based CPU rates alongside instantaneous gauges.
#[derive(Debug)]
pub struct NodeCpuSampler {
    sources: NodeCpuMetricSources,
    prev_proc_stat: Option<ProcStatSample>,
    prev_cgroup_usage: Option<(Instant, u64)>,
    warned_proc_stat: bool,
    warned_pressure: bool,
    warned_cgroup_stat: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct NodeCpuTickSample {
    pub host_cores: Option<HostCpuCores>,
    pub pressure_some_avg10: Option<f64>,
    pub cgroup_usage_usec_per_sec: Option<f64>,
    pub cgroup_nr_throttled: Option<u64>,
    pub cgroup_throttled_usec: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct HostCpuCores {
    pub busy: f64,
    pub user: f64,
    pub system: f64,
    pub iowait: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProcStatSample {
    user: u64,
    nice: u64,
    system: u64,
    idle: u64,
    iowait: u64,
    irq: u64,
    softirq: u64,
    steal: u64,
    cpu_count: u32,
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
            prev_proc_stat: None,
            prev_cgroup_usage: None,
            warned_proc_stat: false,
            warned_pressure: false,
            warned_cgroup_stat: false,
        }
    }

    pub fn sample_and_emit(&mut self) {
        let _ = self.sample_and_emit_at(Instant::now());
    }

    pub(crate) fn sample_and_emit_at(&mut self, now: Instant) -> NodeCpuTickSample {
        let mut tick = NodeCpuTickSample::default();

        match std::fs::read_to_string(&self.sources.proc_stat) {
            Ok(content) => match parse_proc_stat(&content) {
                Some(current) => {
                    if self.warned_proc_stat {
                        info!(path = %self.sources.proc_stat.display(), "node CPU metrics /proc/stat read recovered");
                        self.warned_proc_stat = false;
                    }
                    if let Some(previous) = self.prev_proc_stat.replace(current)
                        && let Some(cores) = previous.cores_delta_to(&current)
                    {
                        crate::metrics::node_cpu::host_cores(
                            cores.busy,
                            cores.user,
                            cores.system,
                            cores.iowait,
                        );
                        tick.host_cores = Some(cores);
                    }
                }
                None => {
                    self.prev_proc_stat = None;
                    if !self.warned_proc_stat {
                        warn!(
                            path = %self.sources.proc_stat.display(),
                            "node CPU metrics failed to parse /proc/stat"
                        );
                        self.warned_proc_stat = true;
                    }
                }
            },
            Err(error) => {
                self.prev_proc_stat = None;
                if !self.warned_proc_stat {
                    warn!(
                        path = %self.sources.proc_stat.display(),
                        %error,
                        "node CPU metrics failed to read /proc/stat"
                    );
                    self.warned_proc_stat = true;
                }
            }
        }

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
                    self.prev_cgroup_usage = None;
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
                    if let Some(usage_usec) = stat.usage_usec {
                        if let Some((prev_at, prev_usage)) =
                            self.prev_cgroup_usage.replace((now, usage_usec))
                            && let Some(rate) =
                                cgroup_usage_rate(prev_at, prev_usage, now, usage_usec)
                        {
                            crate::metrics::node_cpu::space_cgroup_usage_usec_per_sec(rate);
                            tick.cgroup_usage_usec_per_sec = Some(rate);
                        }
                    } else {
                        self.prev_cgroup_usage = None;
                    }
                    if let Some(nr_throttled) = stat.nr_throttled {
                        crate::metrics::node_cpu::space_cgroup_nr_throttled(nr_throttled);
                        tick.cgroup_nr_throttled = Some(nr_throttled);
                    }
                    if let Some(throttled_usec) = stat.throttled_usec {
                        crate::metrics::node_cpu::space_cgroup_throttled_usec(throttled_usec);
                        tick.cgroup_throttled_usec = Some(throttled_usec);
                    }
                }
            }
            Err(error) => {
                self.prev_cgroup_usage = None;
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

impl ProcStatSample {
    fn cores_delta_to(&self, next: &Self) -> Option<HostCpuCores> {
        let user = next.user.checked_sub(self.user)?;
        let nice = next.nice.checked_sub(self.nice)?;
        let system = next.system.checked_sub(self.system)?;
        let idle = next.idle.checked_sub(self.idle)?;
        let iowait = next.iowait.checked_sub(self.iowait)?;
        let irq = next.irq.checked_sub(self.irq)?;
        let softirq = next.softirq.checked_sub(self.softirq)?;
        let steal = next.steal.checked_sub(self.steal)?;

        let total = u128::from(user)
            + u128::from(nice)
            + u128::from(system)
            + u128::from(idle)
            + u128::from(iowait)
            + u128::from(irq)
            + u128::from(softirq)
            + u128::from(steal);
        if total == 0 || next.cpu_count == 0 {
            return None;
        }

        let user_all = u128::from(user) + u128::from(nice);
        let system_all = u128::from(system) + u128::from(irq) + u128::from(softirq);
        let busy_all = user_all + system_all + u128::from(steal);
        let scale = f64::from(next.cpu_count) / (total as f64);

        Some(HostCpuCores {
            busy: (busy_all as f64) * scale,
            user: (user_all as f64) * scale,
            system: (system_all as f64) * scale,
            iowait: (iowait as f64) * scale,
        })
    }
}

impl CgroupCpuStat {
    fn is_empty(&self) -> bool {
        self.usage_usec.is_none() && self.nr_throttled.is_none() && self.throttled_usec.is_none()
    }
}

fn cgroup_usage_rate(
    prev_at: Instant,
    prev_usage_usec: u64,
    now: Instant,
    curr_usage_usec: u64,
) -> Option<f64> {
    let elapsed_secs = now.checked_duration_since(prev_at)?.as_secs_f64();
    if elapsed_secs <= 0.0 {
        return None;
    }
    let delta_usec = curr_usage_usec.checked_sub(prev_usage_usec)?;
    let rate = (delta_usec as f64) / elapsed_secs;
    rate.is_finite().then_some(rate)
}

fn parse_proc_stat(content: &str) -> Option<ProcStatSample> {
    let mut aggregate: Option<ProcStatSample> = None;
    let mut cpu_count: u32 = 0;

    for line in content.lines() {
        let mut fields = line.split_whitespace();
        let Some(label) = fields.next() else {
            continue;
        };
        if label == "cpu" {
            aggregate = Some(ProcStatSample {
                user: fields.next()?.parse().ok()?,
                nice: fields.next()?.parse().ok()?,
                system: fields.next()?.parse().ok()?,
                idle: fields.next()?.parse().ok()?,
                iowait: parse_optional_u64(fields.next())?,
                irq: parse_optional_u64(fields.next())?,
                softirq: parse_optional_u64(fields.next())?,
                steal: parse_optional_u64(fields.next())?,
                cpu_count: 0,
            });
        } else if let Some(suffix) = label.strip_prefix("cpu")
            && !suffix.is_empty()
            && suffix.bytes().all(|byte| byte.is_ascii_digit())
        {
            cpu_count = cpu_count.saturating_add(1);
        }
    }

    let mut sample = aggregate?;
    if cpu_count == 0 {
        return None;
    }
    sample.cpu_count = cpu_count;
    Some(sample)
}

fn parse_optional_u64(field: Option<&str>) -> Option<u64> {
    match field {
        Some(raw) => raw.parse().ok(),
        None => Some(0),
    }
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
