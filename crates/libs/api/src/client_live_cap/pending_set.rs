//! Redis client for the live cap's pending set. The script and its key layout
//! are in the sibling `live_pending.lua`; every call is one `EVALSHA`.

use std::collections::HashSet;
use std::sync::LazyLock;
use std::time::Duration;

use deadpool_redis::{Config as RedisPoolConfig, Pool, Runtime};
use pplx_space_common_core::types::{ClientId, SandboxId};
use redis::Script;
use tracing::Instrument as _;

use crate::controllers::substrate::CountedIds;
use crate::redis_otel::{RedisEndpoint, record_redis_result, redis_client_span};

/// Every key's prefix. The braces are a Redis Cluster hash tag, so every key
/// lands in one slot and a script may read any client's set.
const KEY_PREFIX: &str = "{space:live_pending}";

/// Hard bound on one round trip (pool checkout and `EVALSHA`), so a stalled
/// Redis surfaces as an error the cap fails open on.
const OP_TIMEOUT: Duration = Duration::from_millis(500);

const LIVE_PENDING_LUA: &str = include_str!("live_pending.lua");
static LIVE_PENDING_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(LIVE_PENDING_LUA));

#[derive(Debug, thiserror::Error)]
pub(super) enum PendingSetError {
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
    #[error("unexpected pending-set reply: {0}")]
    Reply(String),
}

/// What one read returned: the members live at `cutoff`, as the Redis server
/// clock reads it.
#[derive(Debug, Clone)]
pub(super) struct PendingRead {
    pub(super) cutoff: String,
    pub(super) members: HashSet<SandboxId>,
}

/// The script's verdict on one reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReserveVerdict {
    /// Under the cap: the member was added.
    Admitted,
    /// The activation's own sandbox was already pending: renewed.
    Renewed,
    /// At the cap.
    Refused,
    /// A refusal that counted members the read missed: redo the check.
    Retry,
    /// The read's cutoff is older than the grace: redo the check.
    Stale,
}

/// The timing a reservation is held with.
#[derive(Debug, Clone, Copy)]
pub(super) struct PendingTiming {
    /// How long a member counts after its last renewal.
    pub(super) ttl: Duration,
    /// How long after its expiry a member is kept before it may be deleted.
    pub(super) grace: Duration,
}

/// One reservation's identity in the set.
pub(super) struct Member<'a> {
    pub(super) client: &'a ClientId,
    pub(super) sandbox: SandboxId,
    pub(super) owner: &'a str,
}

#[derive(Clone)]
pub(super) struct PendingSet {
    pool: Pool,
    endpoint: RedisEndpoint,
    /// [`KEY_PREFIX`], or a per-test prefix so tests sharing one Redis
    /// can't see each other's sets.
    prefix: String,
}

fn mode_and_ids(ids: &CountedIds) -> (&'static str, String) {
    let (mode, listed) = match ids {
        CountedIds::Exactly(ids) => ("exactly", ids),
        CountedIds::AllExcept(ids) => ("all_except", ids),
    };
    let listed = listed
        .iter()
        .map(ClientId::as_str)
        .collect::<Vec<_>>()
        .join(",");
    (mode, listed)
}

fn secs(duration: Duration) -> String {
    format!("{:.3}", duration.as_secs_f64())
}

impl PendingSet {
    pub(super) fn from_url(url: &str) -> Result<Self, PendingSetError> {
        let endpoint = RedisEndpoint::from_url(url).map_err(PendingSetError::Endpoint)?;
        let pool = RedisPoolConfig::from_url(url).create_pool(Some(Runtime::Tokio1))?;
        Ok(Self {
            pool,
            endpoint,
            prefix: KEY_PREFIX.to_string(),
        })
    }

    /// The same store under its own hash-tagged prefix.
    #[cfg(test)]
    pub(super) fn isolated(mut self, name: &str) -> Self {
        self.prefix = format!("{{space:live_pending:{name}}}");
        self
    }

    async fn invoke<T>(&self, args: Vec<String>) -> Result<T, PendingSetError>
    where
        T: redis::FromRedisValue + Send,
    {
        let span = redis_client_span("EVALSHA", &self.endpoint);
        async move {
            let result = tokio::time::timeout(OP_TIMEOUT, async {
                let mut conn = self.pool.get().await?;
                let mut invocation = LIVE_PENDING_SCRIPT.key(&self.prefix);
                for arg in &args {
                    invocation.arg(arg);
                }
                Ok::<T, PendingSetError>(invocation.invoke_async(&mut conn).await?)
            })
            .await
            .unwrap_or(Err(PendingSetError::Timeout(OP_TIMEOUT)));
            record_redis_result(&tracing::Span::current(), result.as_ref().map(|_| ()));
            result
        }
        .instrument(span)
        .await
    }

    /// The members live now for the ids `ids` selects, and the cutoff the
    /// matching [`Self::reserve`] counts at.
    pub(super) async fn read(&self, ids: &CountedIds) -> Result<PendingRead, PendingSetError> {
        let (mode, listed) = mode_and_ids(ids);
        let reply: Vec<String> = self
            .invoke(vec!["read".into(), mode.into(), listed])
            .await?;
        let mut reply = reply.into_iter();
        let cutoff = reply
            .next()
            .ok_or_else(|| PendingSetError::Reply("read returned no cutoff".into()))?;
        let members = reply
            .filter_map(|member| member.parse::<SandboxId>().ok())
            .collect();
        Ok(PendingRead { cutoff, members })
    }

    /// Add `member` if `visible` plus the pending members at the read's cutoff
    /// is below `cap`.
    pub(super) async fn reserve(
        &self,
        ids: &CountedIds,
        read: &PendingRead,
        member: &Member<'_>,
        cap: u64,
        visible: u64,
        timing: PendingTiming,
    ) -> Result<ReserveVerdict, PendingSetError> {
        let (mode, listed) = mode_and_ids(ids);
        let read_members = read
            .members
            .iter()
            .map(SandboxId::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let (verdict, _total): (String, i64) = self
            .invoke(vec![
                "reserve".into(),
                mode.into(),
                listed,
                read.cutoff.clone(),
                member.client.as_str().into(),
                member.sandbox.to_string(),
                member.owner.into(),
                cap.to_string(),
                visible.to_string(),
                secs(timing.ttl),
                secs(timing.grace),
                read_members,
            ])
            .await?;
        match verdict.as_str() {
            "admitted" => Ok(ReserveVerdict::Admitted),
            "renewed" => Ok(ReserveVerdict::Renewed),
            "refused" => Ok(ReserveVerdict::Refused),
            "retry" => Ok(ReserveVerdict::Retry),
            "stale" => Ok(ReserveVerdict::Stale),
            other => Err(PendingSetError::Reply(format!("reserve verdict {other}"))),
        }
    }

    /// Push `member`'s expiry out by the TTL. `false` once the owner no longer
    /// holds a live member: released, forgotten by a teardown, or lapsed.
    pub(super) async fn renew(
        &self,
        member: &Member<'_>,
        timing: PendingTiming,
    ) -> Result<bool, PendingSetError> {
        let renewed: i64 = self
            .invoke(vec![
                "renew".into(),
                member.client.as_str().into(),
                member.sandbox.to_string(),
                member.owner.into(),
                secs(timing.ttl),
                secs(timing.grace),
            ])
            .await?;
        Ok(renewed == 1)
    }

    /// Remove `member`'s owner. The member goes with its last owner.
    pub(super) async fn release(&self, member: &Member<'_>) -> Result<(), PendingSetError> {
        let _: i64 = self
            .invoke(vec![
                "release".into(),
                member.client.as_str().into(),
                member.sandbox.to_string(),
                member.owner.into(),
            ])
            .await?;
        Ok(())
    }

    /// Move `member`'s owner to sandbox `to`, without a cap check.
    pub(super) async fn swap(
        &self,
        member: &Member<'_>,
        to: SandboxId,
        timing: PendingTiming,
    ) -> Result<bool, PendingSetError> {
        let moved: i64 = self
            .invoke(vec![
                "swap".into(),
                member.client.as_str().into(),
                member.sandbox.to_string(),
                to.to_string(),
                member.owner.into(),
                secs(timing.ttl),
                secs(timing.grace),
            ])
            .await?;
        Ok(moved == 1)
    }

    /// Remove `sandbox`'s member from every client's set.
    pub(super) async fn forget(&self, sandbox: SandboxId) -> Result<(), PendingSetError> {
        let _: i64 = self
            .invoke(vec!["forget".into(), sandbox.to_string()])
            .await?;
        Ok(())
    }
}
