use std::sync::LazyLock;
use std::time::Duration;

use deadpool_redis::{Config as RedisPoolConfig, Pool, Runtime};
use redis::Script;
use tracing::Instrument as _;

use crate::redis_otel::{RedisEndpoint, record_redis_result, redis_client_span};

/// Hard wall-clock bound on a single consume (pool checkout + `EVALSHA`). A
/// Redis that accepts connections but stalls — or an exhausted pool behind
/// stalled commands — would otherwise hang the create/start/restore path
/// indefinitely instead of taking the fail-open `store_unavailable` path.
/// Redis token-bucket ops are sub-millisecond in-region, so this is generous.
const CONSUME_TIMEOUT: Duration = Duration::from_millis(500);

/// Outcome of a single atomic token-bucket consume against Redis.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum ConsumeOutcome {
    Admitted { remaining: f64 },
    RateLimited { remaining: f64 },
}

#[derive(Debug, thiserror::Error)]
pub(super) enum BucketError {
    #[error("redis pool build failed: {0}")]
    Build(#[from] deadpool_redis::CreatePoolError),
    #[error("redis pool checkout failed: {0}")]
    Pool(#[from] deadpool_redis::PoolError),
    #[error("redis endpoint parse failed: {0}")]
    Endpoint(#[source] redis::RedisError),
    #[error("redis command failed: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("redis returned an unparseable token count: {0:?}")]
    Parse(String),
    #[error("redis consume timed out after {0:?}")]
    Timeout(Duration),
}

/// A shared, cluster-wide token bucket backed by a single Redis key.
///
/// One atomic `EVALSHA` per admission performs refill + consume + TTL using the
/// Redis server clock, so every API and cluster-controller replica shares one
/// rate with no client-clock skew, no `SELECT … FOR UPDATE`, and no
/// transaction. The connection pool is lazy, so constructing this never blocks
/// on Redis reachability; connection failures surface at consume time and the
/// caller fails open.
#[derive(Clone)]
pub(super) struct RedisTokenBucket {
    pool: Pool,
    endpoint: RedisEndpoint,
}

// The Lua source lives in a sibling `token_bucket.lua` (KEYS/ARGV contract
// documented there) and is embedded at compile time — no runtime file
// dependency, but the script keeps Lua syntax highlighting and reviews as Lua
// in the GitHub UI.
const TOKEN_BUCKET_LUA: &str = include_str!("token_bucket.lua");

/// Built once so `invoke_async` reuses the precomputed SHA1 for `EVALSHA`
/// instead of re-hashing the script on every activation.
static TOKEN_BUCKET_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(TOKEN_BUCKET_LUA));

impl RedisTokenBucket {
    /// Build a lazy connection pool from a `redis://` / `rediss://` URL. TLS is
    /// selected by the URL scheme; the compiled-in rustls provider trusts the
    /// webpki root store, which covers the Amazon CA chain ElastiCache/Valkey
    /// present. Connections are created on first use, so this does not fail
    /// when Redis is unreachable.
    pub(super) fn from_url(url: &str) -> Result<Self, BucketError> {
        let endpoint = RedisEndpoint::from_url(url).map_err(BucketError::Endpoint)?;
        let pool = RedisPoolConfig::from_url(url).create_pool(Some(Runtime::Tokio1))?;
        Ok(Self { pool, endpoint })
    }

    /// Delete `key` so a test starts from an unconsumed bucket. The
    /// production bucket key is fixed (not test-namespaced), so on a shared
    /// test Redis a nextest retry of the same test would otherwise observe
    /// the previous attempt's consumed tokens.
    #[cfg(test)]
    pub(super) async fn clear(&self, key: &str) -> Result<(), BucketError> {
        let mut conn = self.pool.get().await?;
        deadpool_redis::redis::cmd("DEL")
            .arg(key)
            .query_async::<()>(&mut conn)
            .await?;
        Ok(())
    }

    /// Atomically refill and consume `requested` tokens from `key`. Returns the
    /// admit/reject outcome and the remaining token count after the operation.
    pub(super) async fn consume(
        &self,
        key: &str,
        refill_rps: f64,
        burst: f64,
        requested: f64,
    ) -> Result<ConsumeOutcome, BucketError> {
        let span = redis_client_span("EVALSHA", &self.endpoint);
        async move {
            // Bound the whole operation so a stalled Redis / exhausted pool
            // surfaces as an error the caller fails open on, rather than
            // hanging the create path.
            let result = tokio::time::timeout(
                CONSUME_TIMEOUT,
                self.consume_inner(key, refill_rps, burst, requested),
            )
            .await
            .unwrap_or(Err(BucketError::Timeout(CONSUME_TIMEOUT)));
            record_redis_result(&tracing::Span::current(), result.as_ref().map(|_| ()));
            result
        }
        .instrument(span)
        .await
    }

    async fn consume_inner(
        &self,
        key: &str,
        refill_rps: f64,
        burst: f64,
        requested: f64,
    ) -> Result<ConsumeOutcome, BucketError> {
        let mut conn = self.pool.get().await?;
        let (allowed, remaining): (i64, String) = TOKEN_BUCKET_SCRIPT
            .key(key)
            .arg(refill_rps)
            .arg(burst)
            .arg(requested)
            .invoke_async(&mut conn)
            .await?;
        let remaining = remaining
            .parse::<f64>()
            .map_err(|_| BucketError::Parse(remaining))?;
        Ok(if allowed == 1 {
            ConsumeOutcome::Admitted { remaining }
        } else {
            ConsumeOutcome::RateLimited { remaining }
        })
    }
}
