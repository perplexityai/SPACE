use pplx_space_sandbox_model::SandboxId;

use pplx_space_sandbox_store::debug_reads;
use sqlx::PgPool;

use crate::routes::debug::{response::TimelineEvent, sources::begin_read_only};

pub(in crate::routes::debug) async fn fetch(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<Vec<TimelineEvent>, sqlx::Error> {
    let mut tx = begin_read_only(pool).await?;
    let timeline = debug_reads::timeline(&mut *tx, sandbox_id).await?;
    tx.commit().await?;
    Ok(timeline
        .into_iter()
        .map(|event| TimelineEvent {
            at: event.at,
            kind: event.kind,
            detail: event.detail,
        })
        .collect())
}
