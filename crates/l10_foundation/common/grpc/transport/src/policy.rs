use std::time::Duration;

pub const DEFAULT_LOCAL_GRPC_RETRY_POLICY: GrpcRetryPolicy = GrpcRetryPolicy {
    max_retries: 4,
    initial_delay: Duration::from_millis(20),
    backoff_multiplier: 2.0,
    max_delay: Duration::from_millis(100),
    max_elapsed: Duration::from_millis(300),
    jitter_factor: 0.2,
    max_server_retry_after: Duration::from_secs(2),
};

pub const DEFAULT_GRPC_CONNECT_RETRY_POLICY: GrpcRetryPolicy = GrpcRetryPolicy {
    max_retries: 12,
    initial_delay: Duration::from_millis(100),
    backoff_multiplier: 2.0,
    max_delay: Duration::from_secs(1),
    max_elapsed: Duration::from_secs(10),
    jitter_factor: 0.2,
    max_server_retry_after: Duration::from_secs(2),
};

pub const DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY: GrpcRetryPolicy =
    DEFAULT_GRPC_CONNECT_RETRY_POLICY;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GrpcRetryPolicy {
    pub max_retries: usize,
    pub initial_delay: Duration,
    pub backoff_multiplier: f64,
    pub max_delay: Duration,
    pub max_elapsed: Duration,
    pub jitter_factor: f64,
    pub max_server_retry_after: Duration,
}

impl Default for GrpcRetryPolicy {
    fn default() -> Self {
        DEFAULT_LOCAL_GRPC_RETRY_POLICY
    }
}

impl GrpcRetryPolicy {
    pub fn none() -> Self {
        Self {
            max_retries: 0,
            initial_delay: Duration::ZERO,
            backoff_multiplier: 1.0,
            max_delay: Duration::ZERO,
            max_elapsed: Duration::ZERO,
            jitter_factor: 0.0,
            max_server_retry_after: Duration::ZERO,
        }
    }

    pub fn backoff_delay(self, retry_index: usize) -> Option<Duration> {
        if retry_index >= self.max_retries {
            return None;
        }

        let multiplier = self.backoff_multiplier.powi(retry_index as i32);
        let secs =
            (self.initial_delay.as_secs_f64() * multiplier).min(self.max_delay.as_secs_f64());
        let base = Duration::from_secs_f64(secs);
        Some(jittered_delay(base, self.jitter_factor))
    }
}

fn jittered_delay(base: Duration, jitter_factor: f64) -> Duration {
    if base.is_zero() || jitter_factor <= 0.0 {
        return base;
    }
    let bounded_jitter = jitter_factor.clamp(0.0, 1.0);
    let offset = bounded_jitter * (rand::random::<f64>() * 2.0 - 1.0);
    Duration::from_secs_f64(base.as_secs_f64() * (1.0 + offset))
}

#[cfg(test)]
mod tests;
