//! Redis-backed store for the per-client concurrency limiter.
//!
//! Each client bucket is one ZSET of in-flight slots (member = per-request id,
//! score = an absolute deadline). A single atomic `EVALSHA` per acquire purges
//! expired slots, counts, and conditionally adds this request's slot, so the cap
//! is shared across every `space-api` replica with no client-clock skew and no
//! lock. Release is a `ZREM`; a crashed or timed-out holder's slot is reclaimed
//! automatically once its deadline passes (the `hold` TTL), so a lost release
//! never leaks a slot forever.
//!
//! Every Redis round trip is wrapped in a `redis.client` OTel span (via the
//! shared [`crate::redis_otel`] helpers) so command latency and fail-open errors
//! surface in Datadog like the other instrumented clients.

use std::sync::LazyLock;
use std::time::Duration;

use deadpool_redis::{Config as RedisPoolConfig, Pool, Runtime};
use redis::Script;
use tracing::Instrument as _;

use crate::redis_otel::{RedisEndpoint, record_redis_result, redis_client_span};

/// Hard wall-clock bound on a single Redis round trip (pool checkout +
/// `EVALSHA`). A Redis that accepts connections but stalls — or an exhausted
/// pool — must never hang the create/reconnect path; it surfaces as an error the
/// caller fails open on. ZSET ops are sub-millisecond in-region, so this is
/// generous.
const OP_TIMEOUT: Duration = Duration::from_millis(500);

/// Outcome of an acquire attempt against the shared ZSET.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct AcquireOutcome {
    /// The client was already at (or over) its cap before this request.
    pub over_cap: bool,
    /// A slot was added for this request (so it must later be released). Always
    /// true unless enforcement rejected an over-cap request.
    pub added: bool,
    /// In-flight slot count for the client after this call.
    pub count: u64,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum RedisConcurrencyError {
    #[error("redis pool build failed: {0}")]
    Build(#[from] deadpool_redis::CreatePoolError),
    #[error("redis pool checkout failed: {0}")]
    Pool(#[from] deadpool_redis::PoolError),
    #[error("redis endpoint parse failed: {0}")]
    Endpoint(#[source] redis::RedisError),
    #[error("redis command failed: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("redis op timed out after {0:?}")]
    Timeout(Duration),
}

/// Lazy connection pool for the per-client concurrency ZSETs.
#[derive(Clone)]
pub(super) struct RedisConcurrencyStore {
    pool: Pool,
    endpoint: RedisEndpoint,
}

// Lua source lives in a sibling `client_concurrency.lua` (KEYS/ARGV contract
// documented there), embedded at compile time so there is no runtime file
// dependency while keeping Lua highlighting/review in the GitHub UI.
const ACQUIRE_LUA: &str = include_str!("client_concurrency.lua");

/// Built once so `invoke_async` reuses the precomputed SHA1 for `EVALSHA`.
static ACQUIRE_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(ACQUIRE_LUA));

impl RedisConcurrencyStore {
    /// Build a lazy connection pool from a `redis://` / `rediss://` URL.
    /// Connections are created on first use, so this never blocks on Redis
    /// reachability; failures surface at acquire time and the caller fails open.
    pub(super) fn from_url(url: &str) -> Result<Self, RedisConcurrencyError> {
        let endpoint = RedisEndpoint::from_url(url).map_err(RedisConcurrencyError::Endpoint)?;
        let pool = RedisPoolConfig::from_url(url).create_pool(Some(Runtime::Tokio1))?;
        Ok(Self { pool, endpoint })
    }

    /// Atomically purge expired slots, count, and (unless enforcement rejects an
    /// over-cap request) add `member`'s slot to `key`.
    pub(super) async fn acquire(
        &self,
        key: &str,
        cap: usize,
        hold: Duration,
        key_ttl: Duration,
        member: &str,
        force: bool,
    ) -> Result<AcquireOutcome, RedisConcurrencyError> {
        let span = redis_client_span("EVALSHA", &self.endpoint);
        async move {
            let result = tokio::time::timeout(
                OP_TIMEOUT,
                self.acquire_inner(key, cap, hold, key_ttl, member, force),
            )
            .await
            .unwrap_or(Err(RedisConcurrencyError::Timeout(OP_TIMEOUT)));
            record_redis_result(&tracing::Span::current(), result.as_ref().map(|_| ()));
            result
        }
        .instrument(span)
        .await
    }

    async fn acquire_inner(
        &self,
        key: &str,
        cap: usize,
        hold: Duration,
        key_ttl: Duration,
        member: &str,
        force: bool,
    ) -> Result<AcquireOutcome, RedisConcurrencyError> {
        let mut conn = self.pool.get().await?;
        let (over_cap, added, count): (i64, i64, i64) = ACQUIRE_SCRIPT
            .key(key)
            .arg(cap as u64)
            .arg(hold.as_secs_f64())
            .arg(member)
            .arg(key_ttl.as_secs().max(1))
            .arg(i64::from(force))
            .invoke_async(&mut conn)
            .await?;
        Ok(AcquireOutcome {
            over_cap: over_cap == 1,
            added: added == 1,
            count: u64::try_from(count).unwrap_or(0),
        })
    }

    /// Release a previously-acquired slot. Best-effort: a failure just leaves the
    /// slot to expire via its `hold` deadline, so callers can fire-and-forget.
    pub(super) async fn release(
        &self,
        key: &str,
        member: &str,
    ) -> Result<(), RedisConcurrencyError> {
        let span = redis_client_span("ZREM", &self.endpoint);
        async move {
            let result = tokio::time::timeout(OP_TIMEOUT, self.release_inner(key, member))
                .await
                .unwrap_or(Err(RedisConcurrencyError::Timeout(OP_TIMEOUT)));
            record_redis_result(&tracing::Span::current(), result.as_ref().map(|_| ()));
            result
        }
        .instrument(span)
        .await
    }

    async fn release_inner(&self, key: &str, member: &str) -> Result<(), RedisConcurrencyError> {
        let mut conn = self.pool.get().await?;
        redis::cmd("ZREM")
            .arg(key)
            .arg(member)
            .query_async::<()>(&mut conn)
            .await?;
        Ok(())
    }

    /// Delete a bucket key outright. Test-only: production code only ever adds
    /// or reaps individual members. Some bucket keys (the shared `unspecified`
    /// bucket) are fixed by production semantics rather than test-namespaced, so
    /// a test exercising one must reset it first — otherwise a run against a
    /// shared/reused Redis (or a nextest retry of the same test) can see a
    /// leftover member from a prior attempt and misread the bucket as already
    /// over cap.
    #[cfg(test)]
    pub(super) async fn clear(&self, key: &str) -> Result<(), RedisConcurrencyError> {
        let mut conn = self.pool.get().await?;
        redis::cmd("DEL")
            .arg(key)
            .query_async::<()>(&mut conn)
            .await?;
        Ok(())
    }
}
