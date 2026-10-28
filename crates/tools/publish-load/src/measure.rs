use std::time::Instant;

use anyhow::{Context, Result, ensure};
use nix::unistd::{SysconfVar, sysconf};
use pplx_space_sql_locator::sql;
use serde::Serialize;
use sqlx::PgPool;

use crate::Args;
use crate::workload::WorkloadOutcome;

pub(crate) struct Measurement {
    postgres_pid: u32,
    service_pid: u32,
    postgres_ticks: u64,
    service_ticks: u64,
    ticks_per_second: f64,
    started: Instant,
    log_offset: usize,
}

#[derive(Serialize, sqlx::FromRow)]
pub(crate) struct Statement {
    queryid: i64,
    query: String,
    calls: i64,
    total_exec_time: f64,
    total_plan_time: f64,
    rows: i64,
    shared_blks_hit: i64,
}

#[derive(Serialize)]
pub(crate) struct Report {
    schema: u32,
    seed: u64,
    nodes: u32,
    sandboxes_per_node: u32,
    simulated_minutes: u32,
    time_scale: f64,
    activity_epoch_seconds: i64,
    publishes: usize,
    wall_seconds: f64,
    postgres_cpu_ms_per_publish: f64,
    service_cpu_ms_per_publish: f64,
    statements_per_publish: f64,
    exec_ms_per_publish: f64,
    plan_ms_per_publish: f64,
    rows_per_publish: f64,
    shared_blks_hit_per_publish: f64,
    wal_bytes_per_publish: f64,
    rpc_p50_ms: f64,
    rpc_p99_ms: f64,
    // Never label RPC latency as transaction duration.
    transaction_p50_ms: f64,
    transaction_p99_ms: f64,
    statements: Vec<Statement>,
}

impl Measurement {
    pub(crate) async fn start(pool: &PgPool, service_pid: u32, args: &Args) -> Result<Self> {
        let postgres_pid = std::env::var("SPACE_PUBLISH_POSTGRES_PID")
            .context("use the disposable native Postgres launcher")?
            .parse()?;
        sqlx::query(sql!("CREATE EXTENSION IF NOT EXISTS pg_stat_statements"))
            .execute(pool)
            .await?;
        sqlx::query(sql!("SELECT pg_stat_statements_reset()"))
            .execute(pool)
            .await?;
        sqlx::query(sql!("SELECT pg_stat_reset_shared('wal')"))
            .execute(pool)
            .await?;
        let ticks_per_second = sysconf(SysconfVar::CLK_TCK)?.context("CLK_TCK unavailable")? as f64;
        Ok(Self {
            postgres_pid,
            service_pid,
            postgres_ticks: postgres_ticks(postgres_pid).await?,
            service_ticks: process_ticks(service_pid).await?.0,
            ticks_per_second,
            started: Instant::now(),
            log_offset: tokio::fs::metadata(args.output.join("service.log"))
                .await?
                .len() as usize,
        })
    }

    pub(crate) async fn finish(
        self,
        pool: &PgPool,
        args: &Args,
        outcome: &WorkloadOutcome,
    ) -> Result<Report> {
        let durations = &outcome.durations;
        let postgres_ticks = postgres_ticks(self.postgres_pid).await? - self.postgres_ticks;
        let service_ticks = process_ticks(self.service_pid).await?.0 - self.service_ticks;
        let wall_seconds = self.started.elapsed().as_secs_f64();
        let statements: Vec<Statement> = sqlx::query_as(sql!(
            "SELECT queryid, query, calls, total_exec_time, total_plan_time, rows, shared_blks_hit
             FROM pg_stat_statements
             WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
               AND userid = (SELECT oid FROM pg_roles WHERE rolname = 'space_user')
             ORDER BY queryid"
        ))
        .fetch_all(pool)
        .await?;
        // CPU and wall duration above are already stopped. Allow the final
        // asynchronous WAL-counter flush before reading shared statistics.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        sqlx::query(sql!("SELECT pg_stat_clear_snapshot()"))
            .execute(pool)
            .await?;
        let wal_bytes: f64 =
            sqlx::query_scalar(sql!("SELECT wal_bytes::double precision FROM pg_stat_wal"))
                .fetch_one(pool)
                .await?;
        let count = durations.len() as f64;
        ensure!(
            count > 0.0 && !statements.is_empty(),
            "no measured publications/statements"
        );
        let log = tokio::fs::read(args.output.join("service.log")).await?;
        let timings = transaction_timings(
            log.get(self.log_offset..)
                .context("service log was truncated")?,
        )?;
        ensure!(
            timings.len() == durations.len(),
            "expected one transaction timing per publish, got {} of {}",
            timings.len(),
            durations.len()
        );
        let cpu_ms = 1000.0 / self.ticks_per_second / count;
        Ok(Report {
            schema: 1,
            seed: args.seed,
            nodes: args.nodes,
            sandboxes_per_node: args.sandboxes_per_node,
            simulated_minutes: args.minutes,
            time_scale: args.time_scale,
            activity_epoch_seconds: outcome.activity_epoch_seconds,
            publishes: durations.len(),
            wall_seconds,
            postgres_cpu_ms_per_publish: postgres_ticks as f64 * cpu_ms,
            service_cpu_ms_per_publish: service_ticks as f64 * cpu_ms,
            statements_per_publish: statements.iter().map(|s| s.calls as f64).sum::<f64>() / count,
            exec_ms_per_publish: statements.iter().map(|s| s.total_exec_time).sum::<f64>() / count,
            plan_ms_per_publish: statements.iter().map(|s| s.total_plan_time).sum::<f64>() / count,
            rows_per_publish: statements.iter().map(|s| s.rows as f64).sum::<f64>() / count,
            shared_blks_hit_per_publish: statements
                .iter()
                .map(|s| s.shared_blks_hit as f64)
                .sum::<f64>()
                / count,
            wal_bytes_per_publish: wal_bytes / count,
            rpc_p50_ms: percentile(durations, 50)?,
            rpc_p99_ms: percentile(durations, 99)?,
            transaction_p50_ms: percentile(&timings, 50)?,
            transaction_p99_ms: percentile(&timings, 99)?,
            statements,
        })
    }
}

fn percentile(values: &[f64], percent: usize) -> Result<f64> {
    ensure!(
        (1..=100).contains(&percent) && !values.is_empty(),
        "invalid percentile input"
    );
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let rank = sorted
        .len()
        .checked_mul(percent)
        .context("percentile rank overflow")?
        .div_ceil(100);
    sorted
        .get(rank.saturating_sub(1))
        .copied()
        .context("missing percentile")
}

/// Own CPU and reaped-child CPU, in kernel ticks. Parentheses may contain
/// spaces, so field offsets start after the final ')' of the comm field.
async fn process_ticks(pid: u32) -> Result<(u64, u64)> {
    let text = tokio::fs::read_to_string(format!("/proc/{pid}/stat")).await?;
    parse_ticks(&text)
}

fn parse_ticks(text: &str) -> Result<(u64, u64)> {
    let (_, fields) = text.rsplit_once(')').context("malformed proc stat")?;
    let fields: Vec<&str> = fields.split_whitespace().collect();
    let number = |index: usize| -> Result<u64> {
        Ok(fields.get(index).context("short proc stat")?.parse()?)
    };
    Ok((number(11)? + number(12)?, number(13)? + number(14)?))
}

fn transaction_timings(log: &[u8]) -> Result<Vec<f64>> {
    let mut timings = Vec::new();
    for line in std::str::from_utf8(log)?.lines() {
        let event: serde_json::Value = serde_json::from_str(line)?;
        if event.get("target").and_then(serde_json::Value::as_str) == Some("space_publish_timing") {
            timings.push(
                event
                    .get("transaction_open_us")
                    .and_then(serde_json::Value::as_f64)
                    .context("transaction timing missing")?
                    / 1000.0,
            );
        }
    }
    Ok(timings)
}

/// Count live backends and reaped backends exactly once. PostgreSQL's
/// postmaster is their direct parent, and retains wait4 child CPU counters.
/// Retry if children are reaped or spawned while taking the snapshot.
async fn postgres_ticks(pid: u32) -> Result<u64> {
    let children_path = format!("/proc/{pid}/task/{pid}/children");
    for _ in 0..20 {
        let before = process_ticks(pid).await?;
        let children = tokio::fs::read_to_string(&children_path).await?;
        let mut live = 0;
        let mut vanished = false;
        for child in children.split_whitespace() {
            match process_ticks(child.parse()?).await {
                Ok((own, reaped)) => live += own + reaped,
                Err(_) => {
                    vanished = true;
                    break;
                }
            }
        }
        let after = process_ticks(pid).await?;
        if !vanished
            && before.1 == after.1
            && children == tokio::fs::read_to_string(&children_path).await?
        {
            return Ok(after.0 + after.1 + live);
        }
    }
    anyhow::bail!("Postgres process tree did not stabilize for CPU accounting")
}

#[cfg(test)]
mod tests;
