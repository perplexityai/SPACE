use std::collections::BTreeSet;
use std::time::Duration;

use pplx_space_execution_spec::{RuntimeKind, RuntimeVersion};
use tokio_util::sync::CancellationToken;

use crate::db::SandboxStatusCount;

const SAMPLE_INTERVAL: Duration = Duration::from_secs(60);

/// A `sandbox.worker.lifecycle.status_count` series: a stored lifecycle, the
/// cleanup state over it (`none` when no cleanup owns the rows) and the
/// recorded runtime.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct StatusBucket {
    state: String,
    cleanup_state: String,
    runtime: Option<RuntimeKind>,
    runtime_version: Option<RuntimeVersion>,
}

impl StatusBucket {
    fn of(count: &SandboxStatusCount) -> Self {
        Self {
            state: count.state.as_str().to_owned(),
            cleanup_state: count.cleanup_state.unwrap_or("none").to_owned(),
            runtime: count.runtime,
            runtime_version: count.runtime_version.clone(),
        }
    }
}

/// Emits the series and zeroes, once, each series that disappeared since the
/// previous sample.
#[derive(Default)]
struct StatusGaugeSampler {
    previous_buckets: BTreeSet<StatusBucket>,
}

impl StatusGaugeSampler {
    fn emit_sample(
        &mut self,
        counts: &[SandboxStatusCount],
        mut emit: impl FnMut(&StatusBucket, u64),
    ) {
        let current = counts.iter().map(StatusBucket::of).collect::<BTreeSet<_>>();

        for count in counts {
            emit(&StatusBucket::of(count), count.count);
        }
        for bucket in self.previous_buckets.difference(&current) {
            emit(bucket, 0);
        }

        self.previous_buckets = current;
    }
}

pub(crate) async fn run(db: crate::db::SharedConnection, shutdown_token: CancellationToken) {
    let mut sampler = StatusGaugeSampler::default();
    let mut interval = tokio::time::interval(SAMPLE_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => return,
            _ = interval.tick() => {}
        }

        let counts = {
            let db = match db.lock() {
                Ok(db) => db,
                Err(error) => {
                    tracing::warn!(%error, "failed to lock sandbox database for lifecycle metrics");
                    continue;
                }
            };
            crate::db::read::counts(&db, crate::db::read::ByStatus)
        };
        match counts {
            Ok(counts) => sampler.emit_sample(&counts, |bucket, count| {
                crate::metrics::sandbox::worker::lifecycle_status_count(
                    count,
                    &bucket.state,
                    &bucket.cleanup_state,
                    bucket.runtime,
                    bucket.runtime_version.as_ref(),
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
