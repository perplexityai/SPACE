use std::collections::BTreeSet;
use std::time::Duration;

use pplx_space_execution_spec::{RuntimeKind, RuntimeVersion};
use tokio_util::sync::CancellationToken;

use crate::db::{SandboxLifecycleCount, SandboxStatusCount};

const SAMPLE_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct LifecycleBucket {
    state: String,
    runtime: Option<RuntimeKind>,
    runtime_version: Option<RuntimeVersion>,
}

/// A `sandbox.worker.lifecycle.status_count` series: a stored lifecycle and
/// the cleanup state over it, `none` when no cleanup owns the rows.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct StatusBucket {
    state: String,
    cleanup_state: String,
}

/// A sampled count and the gauge series it is emitted under.
trait SampledCount {
    type Bucket: Clone + Ord;

    fn bucket(&self) -> Self::Bucket;
    fn count(&self) -> u64;
}

impl SampledCount for SandboxLifecycleCount {
    type Bucket = LifecycleBucket;

    fn bucket(&self) -> LifecycleBucket {
        LifecycleBucket {
            state: self.state.as_str().to_owned(),
            runtime: self.runtime,
            runtime_version: self.runtime_version.clone(),
        }
    }

    fn count(&self) -> u64 {
        self.count
    }
}

impl SampledCount for SandboxStatusCount {
    type Bucket = StatusBucket;

    fn bucket(&self) -> StatusBucket {
        StatusBucket {
            state: self.state.as_str().to_owned(),
            cleanup_state: self.cleanup_state.unwrap_or("none").to_owned(),
        }
    }

    fn count(&self) -> u64 {
        self.count
    }
}

/// Emits one metric's series and zeroes, once, each series that disappeared
/// since the previous sample. Each metric has its own sampler.
struct LifecycleGaugeSampler<B> {
    previous_buckets: BTreeSet<B>,
}

impl<B> Default for LifecycleGaugeSampler<B> {
    fn default() -> Self {
        Self {
            previous_buckets: BTreeSet::new(),
        }
    }
}

impl<B: Clone + Ord> LifecycleGaugeSampler<B> {
    fn emit_sample<C: SampledCount<Bucket = B>>(
        &mut self,
        counts: &[C],
        mut emit: impl FnMut(&B, u64),
    ) {
        let current = counts.iter().map(C::bucket).collect::<BTreeSet<_>>();

        for count in counts {
            emit(&count.bucket(), count.count());
        }
        for bucket in self.previous_buckets.difference(&current) {
            emit(bucket, 0);
        }

        self.previous_buckets = current;
    }
}

pub(crate) async fn run(db: crate::db::SharedConnection, shutdown_token: CancellationToken) {
    let mut lifecycle_sampler = LifecycleGaugeSampler::default();
    let mut status_sampler = LifecycleGaugeSampler::default();
    let mut interval = tokio::time::interval(SAMPLE_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => return,
            _ = interval.tick() => {}
        }

        let (lifecycle_counts, status_counts) = {
            let db = match db.lock() {
                Ok(db) => db,
                Err(error) => {
                    tracing::warn!(%error, "failed to lock sandbox database for lifecycle metrics");
                    continue;
                }
            };
            (
                crate::db::read::counts(&db, crate::db::read::ByLifecycle),
                crate::db::read::counts(&db, crate::db::read::ByStatus),
            )
        };
        match lifecycle_counts {
            Ok(counts) => {
                lifecycle_sampler.emit_sample(&counts, |bucket: &LifecycleBucket, count| {
                    crate::metrics::sandbox::worker::lifecycle_count(
                        count,
                        &bucket.state,
                        bucket.runtime,
                        bucket.runtime_version.as_ref(),
                    );
                })
            }
            Err(error) => {
                tracing::warn!(%error, "failed to sample durable sandbox lifecycle counts");
            }
        }
        match status_counts {
            Ok(counts) => status_sampler.emit_sample(&counts, |bucket: &StatusBucket, count| {
                crate::metrics::sandbox::worker::lifecycle_status_count(
                    count,
                    &bucket.state,
                    &bucket.cleanup_state,
                );
            }),
            Err(error) => {
                tracing::warn!(%error, "failed to sample durable sandbox status counts");
            }
        }
    }
}

#[cfg(test)]
mod tests;
