use std::collections::HashMap;
use std::time::{Duration, Instant};

const RETRY_DELAY: Duration = Duration::from_secs(300);
const MAX_FAILURES: usize = 1024;

/// Failure hints only: success has no cooldown, and restart permits another try.
#[derive(Default)]
pub(crate) struct RetryBackoff {
    failures: HashMap<(String, String), Instant>,
}

impl RetryBackoff {
    pub(super) fn allows(&mut self, name: &str, hash: &str, now: Instant) -> bool {
        self.failures.retain(|_, until| *until > now);
        !self
            .failures
            .contains_key(&(name.to_owned(), hash.to_owned()))
    }

    pub(super) fn failed(&mut self, name: &str, hash: &str, now: Instant) {
        self.failures.retain(|_, until| *until > now);
        // Cache pressure may permit an extra retry, just like a daemon restart.
        // Keep tenant-supplied template names from growing this map indefinitely.
        if self.failures.len() >= MAX_FAILURES
            && let Some(oldest) = self
                .failures
                .iter()
                .min_by_key(|(_, until)| *until)
                .map(|(key, _)| key.clone())
        {
            self.failures.remove(&oldest);
        }
        self.failures
            .insert((name.to_owned(), hash.to_owned()), now + RETRY_DELAY);
    }
}

#[cfg(test)]
mod tests;
