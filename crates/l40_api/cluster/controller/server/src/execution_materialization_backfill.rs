//! Stamps `sandbox_k8s_executions.pod_materialized_at` on active rows that
//! recorded a pod before the record existed.
//!
//! The Kubernetes substrate's execution store stamps the column in the same
//! statement that records or clears a `pod_uid`, so new rows carry it. A row
//! whose `pod_uid` was set before that, by an earlier release or by an older
//! writer still running during a rollout, has none, and an older writer can
//! still clear its `pod_uid` without stamping it, which forgets that the
//! execution ran a pod. Every writer that clears a `pod_uid` acts only on the
//! active row, and retirement keeps the UID, so only active rows can lose the
//! record. This loop stamps each active row that carries a `pod_uid` and no
//! stamp. Readers treat a non-blank `pod_uid` as materialized as well, so a row
//! this loop hasn't reached yet reads as it did before the record existed.
//!
//! The sweep walks the active rows in sandbox-id windows over the partial
//! index on active rows, a bounded number per tick, and starts a new lap when
//! it reaches the end. There's at most one active row per sandbox, so a lap is
//! about as many rows as there are live sandboxes. Laps continue for as long
//! as the loop runs: an older writer can add an unstamped row until every
//! writer runs this release. Rows another transaction holds are skipped, so
//! the loop never waits on a substrate writer; the next lap reaches them.
//!
//! Once every writer runs a release that stamps, no new unstamped row can
//! appear, and after one more full lap this loop has nothing left to do and
//! can be removed.

use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::SandboxId;
use sqlx::PgPool;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const DEFAULT_TICK: Duration = Duration::from_secs(60);
/// Active rows per window.
const DEFAULT_WINDOW_ROWS: i64 = 500;
/// Windows per tick. At these defaults a tick examines up to 2,000 active
/// rows, so a lap over 20,000 live sandboxes takes ten ticks.
const DEFAULT_WINDOWS_PER_TICK: u32 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconcilerConfig {
    pub tick: Duration,
    pub window_rows: i64,
    pub windows_per_tick: u32,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            tick: DEFAULT_TICK,
            window_rows: DEFAULT_WINDOW_ROWS,
            windows_per_tick: DEFAULT_WINDOWS_PER_TICK,
        }
    }
}

/// One window's outcome: where the next window starts, or `None` when this
/// one reached the end of the active rows, and how many rows it stamped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WindowReport {
    pub(crate) next_cursor: Option<SandboxId>,
    pub(crate) stamped: u64,
}

pub(crate) struct BackfillStore {
    pool: PgPool,
}

impl BackfillStore {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Stamp the active rows among the next `window_rows` sandboxes after
    /// `cursor` that carry a non-blank `pod_uid` and no stamp. Each statement
    /// is a range read of the partial index on active rows; the update locks
    /// what it stamps, skips rows another transaction holds, and re-checks both
    /// conditions on the row version it locks, so a writer that stamped or
    /// cleared the row first wins.
    pub(crate) async fn stamp_window(
        &self,
        cursor: SandboxId,
        window_rows: i64,
    ) -> Result<WindowReport, sqlx::Error> {
        let window_end: Option<SandboxId> = sqlx::query_scalar(sql!(
            "SELECT sandbox_id
               FROM (
                   SELECT sandbox_id
                     FROM sandbox_k8s_executions
                    WHERE active
                      AND sandbox_id > $1
                    ORDER BY sandbox_id
                    LIMIT $2
               ) window_ids
              ORDER BY sandbox_id DESC
              LIMIT 1"
        ))
        .bind(cursor)
        .bind(window_rows)
        .fetch_optional(&self.pool)
        .await?;
        let Some(window_end) = window_end else {
            return Ok(WindowReport {
                next_cursor: None,
                stamped: 0,
            });
        };
        let stamped = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET pod_materialized_at = NOW()
              WHERE id IN (
                  SELECT id
                    FROM sandbox_k8s_executions
                   WHERE active
                     AND sandbox_id > $1
                     AND sandbox_id <= $2
                     AND pod_materialized_at IS NULL
                     AND NULLIF(BTRIM(pod_uid), '') IS NOT NULL
                     FOR UPDATE SKIP LOCKED
              )"
        ))
        .bind(cursor)
        .bind(window_end)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(WindowReport {
            next_cursor: Some(window_end),
            stamped,
        })
    }
}

/// The cursor before the first sandbox: every real sandbox id sorts after it.
pub(crate) fn lap_start() -> SandboxId {
    SandboxId::from_uuid(Uuid::nil())
}

/// Run up to `windows_per_tick` windows from `cursor`, returning where the
/// next tick starts and how many rows this one stamped. Reaching the end of
/// the active rows starts the next tick on a new lap.
pub(crate) async fn run_tick(
    store: &BackfillStore,
    config: &ReconcilerConfig,
    mut cursor: SandboxId,
) -> Result<(SandboxId, u64), sqlx::Error> {
    let mut stamped = 0;
    for _ in 0..config.windows_per_tick {
        let window = store.stamp_window(cursor, config.window_rows).await?;
        stamped += window.stamped;
        match window.next_cursor {
            Some(next) => cursor = next,
            None => return Ok((lap_start(), stamped)),
        }
    }
    Ok((cursor, stamped))
}

pub async fn run_with_pool(
    config: ReconcilerConfig,
    pool: PgPool,
    shutdown_token: CancellationToken,
) {
    tracing::info!(
        tick_secs = config.tick.as_secs(),
        window_rows = config.window_rows,
        windows_per_tick = config.windows_per_tick,
        "starting execution-materialization-backfill reconciler"
    );
    let store = BackfillStore::new(pool);
    let mut cursor = lap_start();
    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => break,
            _ = interval.tick() => {
                match run_tick(&store, &config, cursor).await {
                    Ok((next, stamped)) => {
                        cursor = next;
                        if stamped > 0 {
                            tracing::info!(
                                stamped,
                                "stamped active executions that recorded a pod before the \
                                 materialization record"
                            );
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            "execution-materialization-backfill tick failed"
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
