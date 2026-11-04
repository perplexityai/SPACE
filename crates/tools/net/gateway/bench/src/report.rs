//! Result records, the derived per-workload summary, and the comparison table.
//!
//! Raw per-iteration samples are what gets written to disk; every rate and
//! percentile is derived when a report is rendered. That way a result file
//! recorded before a change to how a metric is computed stays comparable
//! against one recorded after.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::proc::Usage;
use crate::workload::Measured;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Run {
    /// Free-form name for this side of a comparison, e.g. a git rev.
    pub label: String,
    pub started_at: String,
    pub iterations: u32,
    pub warmup_iterations: u32,
    pub scale_percent: u32,
    pub host_kernel: String,
    pub workloads: Vec<WorkloadRun>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkloadRun {
    pub name: String,
    pub summary: String,
    /// Set when an iteration returned an error. The workload's own correctness
    /// checks are the smoke test, so this is a failure, not a note.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub iterations: Vec<Iteration>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Iteration {
    pub wall_us: u64,
    pub gateway_cpu_ns: u64,
    pub gateway_rss_peak_bytes: u64,
    pub bytes_to_upstream: u64,
    pub bytes_from_upstream: u64,
    pub connections: u64,
    pub operations: u64,
    /// Percentiles over the workload's own per-operation latencies, when it
    /// reported any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_p50_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_p99_us: Option<u64>,
}

impl Iteration {
    pub fn new(wall_us: u64, usage: Usage, measured: &Measured) -> Self {
        let mut latencies = measured.operation_latencies_us.clone();
        latencies.sort_unstable();
        Self {
            wall_us,
            gateway_cpu_ns: usage.cpu_ns,
            gateway_rss_peak_bytes: usage.rss_peak_bytes,
            bytes_to_upstream: measured.bytes_to_upstream,
            bytes_from_upstream: measured.bytes_from_upstream,
            connections: measured.connections,
            operations: measured.operations,
            operation_p50_us: percentile(&latencies, 50),
            operation_p99_us: percentile(&latencies, 99),
        }
    }

    fn total_bytes(&self) -> u64 {
        self.bytes_to_upstream
            .saturating_add(self.bytes_from_upstream)
    }
}

/// Per-workload numbers, derived from the raw iterations.
#[derive(Clone, Debug)]
pub struct Summary {
    pub name: String,
    pub error: Option<String>,
    pub iterations: usize,
    pub wall_p50_us: u64,
    pub wall_p99_us: u64,
    pub throughput_mib_s: Option<f64>,
    pub cpu_ms_per_iteration: Option<f64>,
    pub cpu_us_per_mib: Option<f64>,
    pub cpu_us_per_operation: Option<f64>,
    pub rss_peak_bytes: u64,
    pub operation_p50_us: Option<u64>,
    pub operation_p99_us: Option<u64>,
    /// How far the slowest iteration ran above the median, as a percentage of
    /// it. The run's own noise floor: a between-rev delta smaller than this is
    /// not distinguishable from the variation inside either run.
    pub wall_spread_pct: Option<f64>,
    pub cpu_spread_pct: Option<f64>,
    pub operation_spread_pct: Option<f64>,
    pub rss_spread_pct: Option<f64>,
}

pub fn summarize(workload: &WorkloadRun) -> Summary {
    let walls = sorted(workload.iterations.iter().map(|it| it.wall_us));
    let cpu_ns = sorted(workload.iterations.iter().map(|it| it.gateway_cpu_ns));
    let bytes = sorted(workload.iterations.iter().map(Iteration::total_bytes));
    let operations = sorted(workload.iterations.iter().map(|it| it.operations));

    let wall_p50_us = percentile(&walls, 50).unwrap_or(0);
    let median_cpu_ns = percentile(&cpu_ns, 50);
    let median_bytes = percentile(&bytes, 50).unwrap_or(0);
    let median_operations = percentile(&operations, 50).unwrap_or(0);
    let median_wall_seconds = ratio(wall_p50_us, 1_000_000);
    let median_mib = ratio(median_bytes, 1024 * 1024);
    let operation_p99s = sorted(
        workload
            .iterations
            .iter()
            .filter_map(|it| it.operation_p99_us),
    );
    let rss = sorted(
        workload
            .iterations
            .iter()
            .map(|it| it.gateway_rss_peak_bytes),
    );

    Summary {
        name: workload.name.clone(),
        error: workload.error.clone(),
        iterations: workload.iterations.len(),
        wall_p50_us,
        wall_p99_us: percentile(&walls, 99).unwrap_or(0),
        // A workload that carried nothing has no throughput at all. Dividing
        // zero bytes by real seconds would print `0.00 MiB/s`, which reads as a
        // measurement of a stalled relay rather than as "not applicable".
        throughput_mib_s: (median_bytes > 0)
            .then(|| divide(median_mib, median_wall_seconds))
            .flatten(),
        cpu_ms_per_iteration: median_cpu_ns.map(|ns| ratio(ns, 1_000_000)),
        cpu_us_per_mib: median_cpu_ns.and_then(|ns| divide(ratio(ns, 1_000), median_mib)),
        cpu_us_per_operation: median_cpu_ns
            .and_then(|ns| divide(ratio(ns, 1_000), ratio(median_operations, 1))),
        rss_peak_bytes: rss.last().copied().unwrap_or(0),
        operation_p50_us: median_of(
            workload
                .iterations
                .iter()
                .filter_map(|it| it.operation_p50_us),
        ),
        operation_p99_us: percentile(&operation_p99s, 50),
        wall_spread_pct: spread_pct(&walls),
        cpu_spread_pct: spread_pct(&cpu_ns),
        operation_spread_pct: spread_pct(&operation_p99s),
        rss_spread_pct: spread_pct(&rss),
    }
}

/// How far the highest sample sat above the median, as a percentage of it.
/// One-sided on purpose: a spurious "this got worse" delta is exactly an upward
/// excursion, so this is the size of excursion the run already produces on its
/// own. `None` below two samples, where there is no spread to speak of.
fn spread_pct(sorted: &[u64]) -> Option<f64> {
    if sorted.len() < 2 {
        return None;
    }
    let median = percentile(sorted, 50)?;
    let highest = sorted.last().copied()?;
    divide(
        ratio(highest.saturating_sub(median), 1) * 100.0,
        ratio(median, 1),
    )
}

/// Nearest-rank percentile over an already-sorted slice.
pub fn percentile(sorted: &[u64], pct: u64) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let count = sorted.len() as u64;
    let rank = count.saturating_mul(pct).div_ceil(100).max(1);
    let index = usize::try_from(rank - 1).unwrap_or(0).min(sorted.len() - 1);
    sorted.get(index).copied()
}

fn sorted(values: impl Iterator<Item = u64>) -> Vec<u64> {
    let mut values: Vec<u64> = values.collect();
    values.sort_unstable();
    values
}

fn median_of(values: impl Iterator<Item = u64>) -> Option<u64> {
    percentile(&sorted(values), 50)
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    numerator as f64 / denominator as f64
}

/// `None` rather than an infinity or a NaN, so a workload that moved no bytes
/// prints a dash instead of a number that looks meaningful.
fn divide(numerator: f64, denominator: f64) -> Option<f64> {
    (denominator > 0.0).then_some(numerator / denominator)
}

// --- rendering --------------------------------------------------------------

/// `cpu +` is the run's own upward spread in gateway CPU: how far the hungriest
/// iteration sat above the median. A workload showing a large one cannot support
/// a small between-rev conclusion.
const HEADER: &str = "workload              iters  wall p50   wall p99     MiB/s  cpu ms/it    cpu +  cpu us/MiB  cpu us/op   op p50   op p99  rss peak";

pub fn render(run: &Run) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "netgtw bench  label={}  iterations={}  warmup={}  scale={}%  kernel={}\n",
        run.label, run.iterations, run.warmup_iterations, run.scale_percent, run.host_kernel
    ));
    out.push_str(&format!("recorded {}\n\n{HEADER}\n", run.started_at));
    for workload in &run.workloads {
        out.push_str(&render_row(&summarize(workload)));
        out.push('\n');
    }
    out
}

fn render_row(summary: &Summary) -> String {
    if let Some(error) = &summary.error {
        return format!("{:<20}  FAILED: {error}", summary.name);
    }
    format!(
        "{:<20} {:>6} {:>9} {:>10} {:>9} {:>10} {:>8} {:>11} {:>10} {:>8} {:>8} {:>9}",
        summary.name,
        summary.iterations,
        duration(summary.wall_p50_us),
        duration(summary.wall_p99_us),
        number(summary.throughput_mib_s, 1),
        number(summary.cpu_ms_per_iteration, 2),
        summary
            .cpu_spread_pct
            .map_or_else(|| "-".to_string(), |spread| format!("{spread:.1}%")),
        number(summary.cpu_us_per_mib, 1),
        number(summary.cpu_us_per_operation, 1),
        summary.operation_p50_us.map_or("-".into(), duration),
        summary.operation_p99_us.map_or("-".into(), duration),
        bytes(summary.rss_peak_bytes),
    )
}

/// The smallest move worth flagging, before the run's own noise is taken into
/// account. A flag is a prompt to re-run, not a verdict.
const REGRESSION_PCT: f64 = 10.0;

/// One comparable number, carrying how to print it and which way is better.
#[derive(Clone, Copy)]
enum Metric {
    /// Elapsed time; lower is better.
    Micros(u64),
    /// The gateway process's resident memory. Reported, never flagged: it is one
    /// observation about the process rather than a per-iteration measurement, so
    /// it barely varies inside a run while varying substantially between runs —
    /// two runs of the same pair of revs produced +13% and -4%. Its within-run
    /// spread is near zero and would not suppress either.
    ProcessBytes(u64),
    /// Bytes per second; higher is better.
    Throughput(f64),
    /// Gateway CPU per unit of work; lower is better.
    Cost(f64),
}

impl Metric {
    fn value(self) -> f64 {
        match self {
            Self::Micros(value) | Self::ProcessBytes(value) => ratio(value, 1),
            Self::Throughput(value) | Self::Cost(value) => value,
        }
    }

    fn render(self) -> String {
        match self {
            Self::Micros(value) => duration(value),
            Self::ProcessBytes(value) => bytes(value),
            Self::Throughput(value) | Self::Cost(value) => format!("{value:.2}"),
        }
    }

    fn higher_is_better(self) -> bool {
        matches!(self, Self::Throughput(_))
    }

    fn flaggable(self) -> bool {
        !matches!(self, Self::ProcessBytes(_))
    }
}

/// One row of a comparison: the metric, and the noise floor of the series it was
/// derived from. Throughput and wall time come from the same wall samples; every
/// CPU-per-unit figure comes from the same CPU samples.
struct Row {
    metric: &'static str,
    value: Option<Metric>,
    noise_pct: Option<f64>,
}

fn rows(summary: &Summary) -> Vec<Row> {
    let row = |metric, value, noise_pct| Row {
        metric,
        value,
        noise_pct,
    };
    vec![
        row(
            "wall p50",
            Some(Metric::Micros(summary.wall_p50_us)),
            summary.wall_spread_pct,
        ),
        row(
            "throughput MiB/s",
            summary.throughput_mib_s.map(Metric::Throughput),
            summary.wall_spread_pct,
        ),
        row(
            "cpu ms/iteration",
            summary.cpu_ms_per_iteration.map(Metric::Cost),
            summary.cpu_spread_pct,
        ),
        row(
            "cpu us/MiB",
            summary.cpu_us_per_mib.map(Metric::Cost),
            summary.cpu_spread_pct,
        ),
        row(
            "cpu us/operation",
            summary.cpu_us_per_operation.map(Metric::Cost),
            summary.cpu_spread_pct,
        ),
        row(
            "op p99",
            summary.operation_p99_us.map(Metric::Micros),
            summary.operation_spread_pct,
        ),
        row(
            "rss peak",
            Some(Metric::ProcessBytes(summary.rss_peak_bytes)),
            summary.rss_spread_pct,
        ),
    ]
}

pub fn compare(baseline: &Run, candidate: &Run) -> String {
    let base: BTreeMap<&str, Summary> = baseline
        .workloads
        .iter()
        .map(|workload| (workload.name.as_str(), summarize(workload)))
        .collect();

    let mut out = format!(
        "netgtw bench comparison  baseline={}  candidate={}\n\
         flagged (!) when a metric moved more than {REGRESSION_PCT:.0}% in the worse direction \
         AND by more than the noise column\n\n",
        baseline.label, candidate.label
    );
    out.push_str(
        "workload              metric              baseline    candidate      delta    noise\n",
    );
    for workload in &candidate.workloads {
        let candidate = summarize(workload);
        let Some(baseline) = base.get(workload.name.as_str()) else {
            out.push_str(&format!("{:<20}  (not in baseline)\n", candidate.name));
            continue;
        };
        if let Some(error) = candidate.error.as_ref().or(baseline.error.as_ref()) {
            out.push_str(&format!("{:<20}  FAILED: {error}\n", candidate.name));
            continue;
        }
        let mut label = candidate.name.clone();
        for (before, after) in rows(baseline).into_iter().zip(rows(&candidate)) {
            let (Some(before_value), Some(after_value)) = (before.value, after.value) else {
                continue;
            };
            // The noise floor is the larger of the two runs': a delta only means
            // something if it exceeds the variation both runs already showed.
            let noise = match (before.noise_pct, after.noise_pct) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (single, None) | (None, single) => single,
            };
            out.push_str(&format!(
                "{:<20}  {:<18} {:>8} {:>12} {:>10} {:>8}\n",
                label,
                before.metric,
                before_value.render(),
                after_value.render(),
                delta(before_value, after_value, noise),
                noise.map_or_else(|| "-".to_string(), |noise| format!("{noise:.1}%")),
            ));
            label.clear();
        }
    }
    out
}

fn delta(before: Metric, after: Metric, noise_pct: Option<f64>) -> String {
    let Some(change) = divide((after.value() - before.value()) * 100.0, before.value()) else {
        return "-".to_string();
    };
    let worse_by = if before.higher_is_better() {
        -change
    } else {
        change
    };
    let floor = REGRESSION_PCT.max(noise_pct.unwrap_or(0.0));
    let flag = before.flaggable() && after.flaggable() && worse_by > floor;
    format!("{change:+.1}%{}", if flag { " !" } else { "" })
}

pub fn duration(micros: u64) -> String {
    match micros {
        0 => "-".to_string(),
        us if us < 1_000 => format!("{us}us"),
        us if us < 1_000_000 => format!("{:.2}ms", ratio(us, 1_000)),
        us => format!("{:.3}s", ratio(us, 1_000_000)),
    }
}

fn bytes(value: u64) -> String {
    let mib = ratio(value, 1024 * 1024);
    if mib < 1.0 {
        return format!("{:.0}KiB", ratio(value, 1024));
    }
    format!("{mib:.1}MiB")
}

fn number(value: Option<f64>, decimals: usize) -> String {
    value.map_or_else(|| "-".to_string(), |value| format!("{value:.decimals$}"))
}

#[cfg(test)]
mod tests;
