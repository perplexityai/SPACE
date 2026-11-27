use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_store::debug_reads::{self, SandboxDebugRecord};

use sqlx::PgPool;

use crate::routes::debug::sources::begin_read_only;

pub(in crate::routes::debug) async fn fetch(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<Option<SandboxDebugRecord>, sqlx::Error> {
    let mut tx = begin_read_only(pool).await?;
    let sandbox = debug_reads::record(&mut *tx, sandbox_id).await?;
    tx.commit().await?;
    Ok(sandbox)
}
