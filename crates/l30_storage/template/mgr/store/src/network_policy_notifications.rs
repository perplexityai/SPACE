//! The `template_network_policy_notifications` outbox: one row per template
//! whose committed default network policy sandboxes have still to be told.

use rusqlite::{Connection, Result, params, types::Type};

use pplx_space_net_common::NetworkPolicyV1;
use pplx_space_policy_authority::PolicyAuthority;
use pplx_space_sql_locator::sql;

use crate::{
    codec::{nonnegative_i64_to_u64, parse_policy_authority, saturating_u64_to_i64},
    limit_to_i64,
};

/// A queued notification. `policy_json` is the persisted bytes, the
/// witness an acknowledgement must match (see [`acknowledge`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateNetworkPolicyNotificationRecord {
    pub template: String,
    pub policy: NetworkPolicyV1,
    pub generation: u64,
    pub authority: PolicyAuthority,
    pub policy_json: String,
}

/// Queue `template`'s committed default network policy from
/// `template_settings`, replacing any notification still pending for it.
/// Returns the number of rows changed: zero when the template has no settings
/// row.
pub fn enqueue(conn: &Connection, template: &str, requested_at: &str) -> Result<usize> {
    // Local generations are allocated by the owning schema. Publish the actual
    // committed witness, never the caller's requested (usually zero) generation.
    conn.execute(
        sql!("INSERT INTO template_network_policy_notifications
             (template, policy_json, generation, authority, requested_at, updated_at)
         SELECT settings.template, settings.default_network_policy_json,
                settings.default_network_policy_generation, settings.default_network_policy_authority,
                ?2, ?2
         FROM template_settings AS settings
         WHERE settings.template = ?1
         ON CONFLICT(template) DO UPDATE SET
             policy_json = excluded.policy_json,
             generation = excluded.generation,
             authority = excluded.authority,
             requested_at = excluded.requested_at,
             updated_at = excluded.updated_at"),
        params![template, requested_at],
    )
}

/// The `limit` oldest queued notifications, oldest first.
pub fn list(
    conn: &Connection,
    limit: usize,
) -> Result<Vec<TemplateNetworkPolicyNotificationRecord>> {
    let mut stmt = conn.prepare(sql!(
        "SELECT template, policy_json, generation, authority
         FROM template_network_policy_notifications
         ORDER BY requested_at ASC
         LIMIT ?1"
    ))?;
    let rows = stmt.query_map(params![limit_to_i64(limit)], |row| {
        let policy_json: String = row.get(1)?;
        let policy = serde_json::from_str(&policy_json).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(1, Type::Text, Box::new(error))
        })?;
        let generation = row
            .get::<_, i64>(2)
            .and_then(|value| nonnegative_i64_to_u64(value, 2))?;
        Ok(TemplateNetworkPolicyNotificationRecord {
            template: row.get(0)?,
            policy,
            generation,
            authority: parse_policy_authority(row.get::<_, String>(3)?.as_str(), 3)?,
            policy_json,
        })
    })?;
    rows.collect()
}

/// Acknowledge a sent notification: delete `template`'s row only while its
/// generation, authority and persisted policy bytes are still the ones sent.
/// Returns the number of rows deleted: zero when a newer notification
/// replaced the sent one, which stays queued, or when none is queued. Run it
/// in the caller's transaction to acknowledge a batch atomically.
pub fn acknowledge(
    conn: &Connection,
    template: &str,
    generation: u64,
    authority: PolicyAuthority,
    policy_json: &str,
) -> Result<usize> {
    conn.execute(
        sql!(
            "DELETE FROM template_network_policy_notifications
             WHERE template = ?1
               AND generation = ?2
               AND authority = ?3
               AND policy_json = ?4"
        ),
        params![
            template,
            saturating_u64_to_i64(generation),
            authority.as_str(),
            policy_json
        ],
    )
}

#[cfg(test)]
mod tests;
