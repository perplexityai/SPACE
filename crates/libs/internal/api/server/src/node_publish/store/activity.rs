use pplx_space_common_core::{sql, types::NodeId};
use sqlx::PgConnection;

use crate::node_publish::plan::activity::ActivityClaim;

#[derive(Debug, thiserror::Error)]
pub(crate) enum UpdateActivityError {
    #[error("update sandbox last_used_at from publish: {0}")]
    Update(#[source] sqlx::Error),
}

/// Advance activity only for the current publisher and only when its timestamp
/// clears the existing one-minute write threshold. The update locks matching
/// sandbox rows; no separate lock acquisition or client-clock comparison occurs.
/// False means no timestamp passed both the owner guard and write threshold.
pub(crate) async fn update(
    conn: &mut PgConnection,
    node_id: NodeId,
    claims: &[ActivityClaim],
) -> Result<bool, UpdateActivityError> {
    if claims.is_empty() {
        return Ok(false);
    }
    let result = sqlx::query(sql!(
        "WITH input AS (
         SELECT sandbox_id, last_used_at, last_access_at, last_command_at
           FROM UNNEST(
               $2::UUID[],
               $3::TEXT[]::TIMESTAMPTZ[],
               $4::TEXT[]::TIMESTAMPTZ[],
               $5::TEXT[]::TIMESTAMPTZ[]
           ) AS input(sandbox_id, last_used_at, last_access_at, last_command_at)
     ),
     activity AS (
         SELECT input.sandbox_id,
                input.last_access_at,
                input.last_command_at,
                (
                    SELECT MAX(value)
                    FROM (
                        VALUES
                            (input.last_used_at),
                            (input.last_access_at),
                            (input.last_command_at)
                    ) AS timestamps(value)
                ) AS last_used_at
         FROM input
     ),
     publishable AS (
         SELECT activity.sandbox_id, activity.last_access_at,
                activity.last_command_at, activity.last_used_at
         FROM activity
         JOIN sandboxes s ON s.id = activity.sandbox_id
         WHERE s.active_node_id = $1
           AND activity.last_used_at IS NOT NULL
     )
     UPDATE sandboxes s
     SET last_access_at = CASE
             WHEN publishable.last_access_at IS NOT NULL
              AND (
                  s.last_access_at IS NULL
                  OR s.last_access_at < publishable.last_access_at - INTERVAL '1 minute'
              )
             THEN publishable.last_access_at
             ELSE s.last_access_at
         END,
         last_command_at = CASE
             WHEN publishable.last_command_at IS NOT NULL
              AND (
                  s.last_command_at IS NULL
                  OR s.last_command_at < publishable.last_command_at - INTERVAL '1 minute'
              )
             THEN publishable.last_command_at
             ELSE s.last_command_at
         END,
         last_used_at = CASE
             WHEN publishable.last_used_at IS NOT NULL
              AND (
                  s.last_used_at IS NULL
                  OR s.last_used_at < publishable.last_used_at - INTERVAL '1 minute'
              )
             THEN publishable.last_used_at
             ELSE s.last_used_at
         END
     FROM publishable
     WHERE s.id = publishable.sandbox_id
       AND (
           (
               publishable.last_access_at IS NOT NULL
               AND (
                   s.last_access_at IS NULL
                   OR s.last_access_at < publishable.last_access_at - INTERVAL '1 minute'
               )
           )
           OR (
               publishable.last_command_at IS NOT NULL
               AND (
                   s.last_command_at IS NULL
                   OR s.last_command_at < publishable.last_command_at - INTERVAL '1 minute'
               )
           )
           OR (
               publishable.last_used_at IS NOT NULL
               AND (
                   s.last_used_at IS NULL
                   OR s.last_used_at < publishable.last_used_at - INTERVAL '1 minute'
               )
           )
       )"
    ))
    .bind(node_id)
    .bind(
        claims
            .iter()
            .map(|claim| claim.sandbox_id)
            .collect::<Vec<_>>(),
    )
    .bind(
        claims
            .iter()
            .map(|claim| {
                claim
                    .last_used_at
                    .as_ref()
                    .map(|timestamp| timestamp.as_str())
            })
            .collect::<Vec<_>>(),
    )
    .bind(
        claims
            .iter()
            .map(|claim| {
                claim
                    .last_access_at
                    .as_ref()
                    .map(|timestamp| timestamp.as_str())
            })
            .collect::<Vec<_>>(),
    )
    .bind(
        claims
            .iter()
            .map(|claim| {
                claim
                    .last_command_at
                    .as_ref()
                    .map(|timestamp| timestamp.as_str())
            })
            .collect::<Vec<_>>(),
    )
    .execute(&mut *conn)
    .await
    .map_err(UpdateActivityError::Update)?;
    Ok(result.rows_affected() != 0)
}
