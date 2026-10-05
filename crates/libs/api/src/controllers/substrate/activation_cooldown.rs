//! Wait before new work, never before joining an accepted activation.
use super::*;

tokio::task_local! {
    static RETURN_TO_REQUEST: ();
}

/// An ingress owner must return a newly observed cooldown to its cancellable
/// request loop. Never sleep through it while retaining an activation slot.
pub(crate) async fn return_wait_to_request<T>(future: impl std::future::Future<Output = T>) -> T {
    RETURN_TO_REQUEST.scope((), future).await
}

pub(super) async fn wait(
    pool: &sqlx::PgPool,
    id: SandboxId,
    budget: Duration,
) -> Result<Duration, SubstrateError> {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let state = tokio::time::timeout_at(
            deadline,
            pplx_space_sandbox_store::activation_admission_state(pool, id),
        )
        .await
        .map_err(|_| SubstrateError::Timeout {
            message: "activation admission observation deadline exceeded".into(),
            phase: None,
        })?
        .map_err(|error| SubstrateError::Failed {
            message: format!("activation admission: {error}"),
        })?;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Some(delay) = state.cooldown else {
            return Ok(remaining);
        };
        if RETURN_TO_REQUEST.try_with(|()| ()).is_ok() {
            return Err(SubstrateError::ActivationCoolingDown { remaining: delay });
        }
        if remaining.is_zero() {
            return Err(SubstrateError::Timeout {
                message: "activation cooldown exceeded request deadline".into(),
                phase: None,
            });
        }
        tokio::time::sleep(delay.min(remaining).min(Duration::from_millis(500))).await;
    }
}

#[cfg(test)]
mod tests;
