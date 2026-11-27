//! `sandbox_ecw_credential_epochs`: the epoch of a sandbox's ECW-compatible
//! data-plane credential, one row per sandbox, removed with it by the
//! foreign key's cascade. Advancing the epoch revokes credentials minted
//! under an earlier one; the route and response reads join the row.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{EcwCredentialEpoch, SandboxId};
use pplx_space_sql_locator::sql;

/// Advance the sandbox's credential epoch and return the new one (an
/// intent). A sandbox with no row yet goes from the implicit epoch 1 to 2.
/// `None` when no such sandbox exists.
pub async fn advance(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<EcwCredentialEpoch>, DbError> {
    Ok(sqlx::query_scalar::<_, EcwCredentialEpoch>(sql!(
        "INSERT INTO sandbox_ecw_credential_epochs (sandbox_id, epoch)
         SELECT id, 2
           FROM sandboxes
          WHERE id = $1
         ON CONFLICT (sandbox_id) DO UPDATE
             SET epoch = sandbox_ecw_credential_epochs.epoch + 1,
                 updated_at = NOW()
         RETURNING epoch"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}
