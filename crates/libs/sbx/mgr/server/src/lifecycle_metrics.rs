use std::collections::BTreeSet;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::db::SandboxLifecycleCount;

const SAMPLE_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct LifecycleBucket {
    state: String,
}

impl From<&SandboxLifecycleCount> for LifecycleBucket {
    fn from(count: &SandboxLifecycleCount) -> Self {
        Self {
            state: count.state.as_str().to_owned(),
        }
    }
}

#[derive(Default)]
struct LifecycleGaugeSampler {
    previous_buckets: BTreeSet<LifecycleBucket>,
}

impl LifecycleGaugeSampler {
    fn emit_sample(
        &mut self,
        counts: &[SandboxLifecycleCount],
        mut emit: impl FnMut(&LifecycleBucket, u64),
    ) {
        let current = counts
            .iter()
            .map(LifecycleBucket::from)
            .collect::<BTreeSet<_>>();

        for count in counts {
            emit(&LifecycleBucket::from(count), count.count);
        }
        for bucket in self.previous_buckets.difference(&current) {
            emit(bucket, 0);
        }

        self.previous_buckets = current;
    }
}

pub(crate) async fn run(db: crate::db::SharedConnection, shutdown_token: CancellationToken) {
    let mut sampler = LifecycleGaugeSampler::default();
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
            crate::db::sandbox_lifecycle_counts(&db)
        };
        let counts = match counts {
            Ok(counts) => counts,
            Err(error) => {
                tracing::warn!(%error, "failed to sample durable sandbox lifecycle counts");
                continue;
            }
        };
        sampler.emit_sample(&counts, |bucket, count| {
            pplx_space_metrics::sandbox::worker::lifecycle_count(count, &bucket.state);
        });
    }
}

#[cfg(test)]
mod tests;
