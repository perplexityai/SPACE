//! Session setup for a `PgListener`'s connection.

use pplx_space_sql_locator::sql;
use sqlx::postgres::PgListener;

/// Set TCP keepalive and `tcp_user_timeout` on a listener's session so
/// Postgres reaps a dead or unreachable client promptly instead of letting
/// its backend linger for hours.
///
/// A listener holds one connection checked out of its pool for as long as it
/// lives and parks waiting for notifications, so the pool's idle-timeout and
/// lifetime recycling never reach it. Dropping the listener runs
/// `UNLISTEN *` and returns that connection to the pool with these settings
/// still applied. If the listener's process dies ungracefully (leaving a
/// half-open TCP connection) or the backend wedges in `ClientWrite` because
/// the client stopped reading it, Postgres would otherwise keep that backend
/// — and the snapshot it holds — alive until its own multi-hour TCP timeout.
/// A backend held that long pins the cluster's xmin horizon, which starves
/// autovacuum on high-churn tables such as `node_reconcile_queue` /
/// `node_reconcile_triggers` until they bloat enough that sequential scans
/// over the dead tuples dominate RDS CPU.
/// `tcp_user_timeout` bounds how long unacknowledged sends wait before the
/// connection is dropped, and the keepalive probes detect a silently-gone
/// peer, so the backend (and its xmin) is released within ~1 minute.
///
/// The settings are session `SET`s, so they hold only on the backend they
/// ran on. A `PgListener` replaces a lost connection by itself, whatever
/// `eager_reconnect` says, and replays only its `LISTEN` channels on the new
/// backend: `recv()` and `into_stream()` do this without returning, and so
/// does any later `try_recv()`, `listen()` or query on the listener. A caller
/// that relies on these settings therefore receives with `try_recv()` and,
/// whenever it returns `Ok(None)` or an error, drops the listener and
/// connects a new one, calling this again before `LISTEN`.
pub async fn set_listener_keepalives(listener: &mut PgListener) -> Result<(), sqlx::Error> {
    sqlx::raw_sql(sql!(
        "SET tcp_keepalives_idle = 30; \
         SET tcp_keepalives_interval = 10; \
         SET tcp_keepalives_count = 3; \
         SET tcp_user_timeout = 30000"
    ))
    .execute(listener)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
