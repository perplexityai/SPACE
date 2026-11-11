//! Driving the registry: warm up, measure K times, record, report.

use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::Context;

use crate::DriveArgs;
use crate::proc::{Monitor, Usage};
use crate::report::{Iteration, Run, WorkloadRun, render, summarize};
use crate::workload::{self, Env, Workload};

/// How often the background sampler reads the gateway's RSS.
const SAMPLE_INTERVAL: Duration = Duration::from_millis(10);

/// Before each iteration, wait for the gateway to go quiet, so the previous
/// iteration's teardown is charged to neither window. A fixed sleep cannot do
/// this: retiring one flow and retiring two thousand take different amounts of
/// time, and the workloads that open thousands are exactly the ones whose
/// neighbours the leftover work would distort.
const SETTLE_POLL: Duration = Duration::from_millis(25);
/// Gateway CPU inside one poll window at or below which it counts as quiet.
const SETTLE_QUIET_NS: u64 = 1_000_000;
/// Consecutive quiet windows required before opening a measurement window.
const SETTLE_QUIET_WINDOWS: u32 = 4;
/// Cap on the wait, so a gateway that never goes quiet slows the run rather than
/// stopping it.
const SETTLE_MAX: Duration = Duration::from_secs(5);
/// Used when nothing is being sampled and quiet cannot be observed.
const SETTLE_UNSAMPLED: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    /// One iteration per workload; the workloads' own checks are the assertions.
    Smoke,
    /// Warm up, then measure `--iterations` times.
    Bench,
}

impl Mode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Bench => "bench",
        }
    }
}

pub fn load(path: &Path) -> anyhow::Result<Run> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&contents)
        .with_context(|| format!("failed to parse {} as a bench result", path.display()))
}

pub async fn drive(args: &DriveArgs, mode: Mode) -> anyhow::Result<()> {
    let selected = select(&args.workloads)?;
    let env = Env {
        upstream: args.upstream,
        raw_echo: args.raw_echo,
        denied: args.denied,
        scale_percent: args.scale,
    };
    let monitor = args
        .gateway_pid
        .map(|pid| Monitor::spawn(pid, SAMPLE_INTERVAL))
        .transpose()
        .context("failed to start sampling the gateway process")?;
    let (measured_iterations, warmup) = match mode {
        Mode::Smoke => (1, 0),
        Mode::Bench => (args.iterations, args.warmup),
    };
    let deadline = Duration::from_secs(args.timeout_secs);

    println!(
        "netgtw {} label={} workloads={} iterations={measured_iterations} warmup={warmup} scale={}%",
        mode.as_str(),
        args.label,
        selected.len(),
        args.scale
    );

    let mut recorded = Vec::new();
    for workload in &selected {
        let mut run = WorkloadRun {
            name: workload.name.to_string(),
            summary: workload.summary.to_string(),
            error: None,
            iterations: Vec::new(),
        };
        for iteration in 0..warmup.saturating_add(measured_iterations) {
            settle(monitor.as_ref()).await?;
            match one_iteration(workload, &env, monitor.as_ref(), deadline).await {
                Ok(sample) if iteration >= warmup => run.iterations.push(sample),
                Ok(_warmup) => {}
                Err(error) => {
                    run.error = Some(format!("{error:#}"));
                    break;
                }
            }
        }
        println!("{}", one_line(&run, mode));
        recorded.push(run);
    }

    let run = Run {
        label: args.label.clone(),
        started_at: jiff::Timestamp::now().to_string(),
        iterations: measured_iterations,
        warmup_iterations: warmup,
        scale_percent: args.scale,
        host_kernel: host_kernel(),
        workloads: recorded,
    };

    if let Some(path) = &args.out {
        let json = serde_json::to_string_pretty(&run).context("failed to encode the results")?;
        std::fs::write(path, format!("{json}\n"))
            .with_context(|| format!("failed to write {}", path.display()))?;
        println!("\nsamples written to {}", path.display());
    }
    if mode == Mode::Bench {
        println!("\n{}", render(&run));
    }

    let failed: Vec<&str> = run
        .workloads
        .iter()
        .filter(|workload| workload.error.is_some())
        .map(|workload| workload.name.as_str())
        .collect();
    anyhow::ensure!(
        failed.is_empty(),
        "{} failed: {}",
        failed.len(),
        failed.join(", ")
    );
    Ok(())
}

/// Block until the gateway has burned no meaningful CPU for
/// [`SETTLE_QUIET_WINDOWS`] consecutive polls, or [`SETTLE_MAX`] elapses.
async fn settle(monitor: Option<&Monitor>) -> anyhow::Result<()> {
    let Some(monitor) = monitor else {
        tokio::time::sleep(SETTLE_UNSAMPLED).await;
        return Ok(());
    };
    let give_up_at = Instant::now() + SETTLE_MAX;
    let mut previous = monitor.read()?.cpu_ns;
    let mut quiet_windows = 0;
    while quiet_windows < SETTLE_QUIET_WINDOWS {
        tokio::time::sleep(SETTLE_POLL).await;
        let cpu_ns = monitor.read()?.cpu_ns;
        let burned = cpu_ns.saturating_sub(previous);
        previous = cpu_ns;
        quiet_windows = if burned <= SETTLE_QUIET_NS {
            quiet_windows + 1
        } else {
            0
        };
        if Instant::now() >= give_up_at {
            tracing::warn!(
                burned_ns = burned,
                "gateway did not go quiet before the settle deadline; the next \
                 iteration may be charged for leftover work"
            );
            return Ok(());
        }
    }
    Ok(())
}

async fn one_iteration(
    workload: &Workload,
    env: &Env,
    monitor: Option<&Monitor>,
    deadline: Duration,
) -> anyhow::Result<Iteration> {
    let mark = monitor.map(Monitor::begin).transpose()?;
    let started = Instant::now();
    let measured = tokio::time::timeout(deadline, workload.run(env))
        .await
        .map_err(|_elapsed| anyhow::anyhow!("did not finish within {deadline:?}"))??;
    let wall_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    let usage = match (monitor, mark) {
        (Some(monitor), Some(mark)) => monitor.end(mark)?,
        _unsampled => Usage {
            cpu_ns: 0,
            rss_peak_bytes: 0,
        },
    };
    Ok(Iteration::new(wall_us, usage, &measured))
}

/// One line per workload, so a smoke run reads as a pass/fail list and a bench
/// run shows progress before the table.
fn one_line(run: &WorkloadRun, mode: Mode) -> String {
    if let Some(error) = &run.error {
        return format!("[FAIL] {:<22} {error}", run.name);
    }
    let summary = summarize(run);
    let detail = match mode {
        Mode::Smoke => run.summary.clone(),
        Mode::Bench => format!("{} iterations", summary.iterations),
    };
    format!(
        "[PASS] {:<22} wall p50 {:>9}  {detail}",
        run.name,
        crate::report::duration(summary.wall_p50_us)
    )
}

fn select(requested: &[String]) -> anyhow::Result<Vec<&'static Workload>> {
    if requested.is_empty() {
        return Ok(workload::REGISTRY.iter().collect());
    }
    requested
        .iter()
        .map(|name| {
            workload::lookup(name).with_context(|| {
                format!("unknown workload {name:?}; `netgtw-bench list` shows the registered names")
            })
        })
        .collect()
}

fn host_kernel() -> String {
    std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .map(|value| value.trim().to_string())
        .unwrap_or_else(|_unavailable| "unknown".to_string())
}
