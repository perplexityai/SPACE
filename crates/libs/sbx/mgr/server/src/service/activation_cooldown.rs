//! Passive admission after an actual activation owner rolled back.
use super::restart_admission::wait_for_activation_admission;
use super::*;

pub(super) fn ready_timeout_secs(remaining: Duration) -> Result<u32, Status> {
    if remaining.is_zero() {
        return Err(Status::deadline_exceeded(
            "activation readiness budget exhausted",
        ));
    }
    Ok(u32::try_from(
        remaining
            .as_secs()
            .saturating_add(u64::from(remaining.subsec_nanos() != 0)),
    )
    .unwrap_or(u32::MAX))
}

impl SandboxManagerService {
    pub(super) async fn wait_activation_cooldown(
        &self,
        sandbox_id: &str,
        budget: Duration,
    ) -> Result<Duration, Status> {
        let deadline = tokio::time::Instant::now() + budget;
        loop {
            super::restart_admission::check_activation_caller(sandbox_id)?;
            let delay = {
                let conn = self.db_conn()?;
                crate::db::activation_retry_delay(&conn, sandbox_id)
                    .map_err(|error| Status::internal(format!("activation cooldown: {error}")))?
            };
            if delay.is_zero() {
                return Ok(deadline.saturating_duration_since(tokio::time::Instant::now()));
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(Status::deadline_exceeded(
                    "activation cooldown exceeded request deadline",
                ));
            }
            // No activation has been admitted. Dropping the caller must cancel
            // this passive wait, even when the surrounding RPC task is owned.
            wait_for_activation_admission(
                sandbox_id,
                tokio::time::sleep(delay.min(remaining).min(Duration::from_millis(500))),
            )
            .await?;
        }
    }
}

#[cfg(test)]
mod tests;
