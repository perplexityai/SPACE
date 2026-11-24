//! `template_settings`: each template's default auto-lifecycle and network
//! policies, its base-snapshot policy and the snapshot reconcile record.
//!
//! The policy writes check only `template` in their `WHERE`: the authority
//! and generation checks run in Rust on the row they read first. So
//! [`upsert_template_settings_tx`] and [`try_update_template_network_policy_tx`]
//! take the caller's transaction, which template-manager opens `IMMEDIATE` so
//! the write lock taken up front guards that read-then-write.

use std::collections::BTreeMap;

use rusqlite::params;

use pplx_space_auto_lifecycle::AutoLifecyclePolicy;
use pplx_space_net_common::NetworkPolicyV1;
use pplx_space_policy_authority::PolicyAuthority;
use pplx_space_sql_locator::sql;

use crate::{Connection, Error, Result, Transaction};

use crate::codec::{
    default_network_policy_json, network_policy_json, nonnegative_i64_to_u64,
    parse_network_policy_json, parse_policy_authority, saturating_u64_to_i64,
};
use crate::now;

pub struct TemplateSettingsRow {
    pub template: String,
    pub default_auto_lifecycle: AutoLifecyclePolicy,
    pub default_auto_lifecycle_authority: PolicyAuthority,
    pub default_network_policy: NetworkPolicyV1,
    pub default_network_policy_generation: u64,
    pub default_network_policy_authority: PolicyAuthority,
    pub ensure_base_snapshot: bool,
    pub snapshot_vcpu: u32,
    pub snapshot_memory_mib: u32,
    pub base_snapshot_policy_set_at: Option<String>,
    pub last_snapshot_reconcile_started_at: Option<String>,
    pub last_snapshot_reconcile_completed_at: Option<String>,
    pub last_snapshot_reconcile_error: Option<String>,
}

/// Write `default_auto_lifecycle` with `authority`, inserting the row if it
/// is missing, and return whether it wrote. A cluster write never replaces a
/// local policy.
pub fn upsert_template_settings_tx(
    tx: &Transaction<'_>,
    template: &str,
    default_auto_lifecycle: &AutoLifecyclePolicy,
    authority: PolicyAuthority,
) -> Result<bool> {
    let ts = now();
    let json = serde_json::to_string(default_auto_lifecycle)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let default_network_json = default_network_policy_json()?;
    match tx.0.query_row(
        sql!(
            "SELECT default_auto_lifecycle_json, default_auto_lifecycle_authority
         FROM template_settings
         WHERE template = ?1"
        ),
        params![template],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                parse_policy_authority(row.get::<_, String>(1)?.as_str(), 1)?,
            ))
        },
    ) {
        Ok((existing_json, existing_authority)) => {
            if authority == PolicyAuthority::Cluster && existing_authority == PolicyAuthority::Local
            {
                return Ok(false);
            }
            if existing_authority == authority && existing_json == json {
                return Ok(false);
            }
            tx.0.execute(
                sql!(
                    "UPDATE template_settings
                 SET default_auto_lifecycle_json = ?1,
                     default_auto_lifecycle_authority = ?2,
                     updated_at = ?3
                 WHERE template = ?4"
                ),
                params![json, authority.as_str(), ts, template],
            )?;
            Ok(true)
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            tx.0.execute(
                sql!("INSERT INTO template_settings
                     (template, default_auto_lifecycle_json, default_auto_lifecycle_authority,
                      default_network_policy_json, default_network_policy_authority, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)"),
                params![
                    template,
                    json,
                    authority.as_str(),
                    default_network_json,
                    PolicyAuthority::Cluster.as_str(),
                    ts
                ],
            )?;
            Ok(true)
        }
        Err(error) => Err(error.into()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemplateNetworkPolicyUpdate {
    Unspecified,
    Skip,
    Apply,
    Repair,
    Conflict,
}

#[derive(Debug, thiserror::Error)]
pub enum TemplateNetworkPolicyUpsertError {
    #[error("template network policy generation conflicts with existing policy")]
    GenerationConflict,
    #[error(transparent)]
    Db(#[from] Error),
}

/// Write `default_network_policy` with `authority`, inserting the row if it
/// is missing, and return `Apply` when it wrote or `Skip` when it didn't. A
/// cluster write never replaces a local policy, skips a generation older than
/// the stored one and an identical policy at the stored generation, fails with
/// `GenerationConflict` for a different policy at the same generation, and
/// otherwise stores `default_network_policy_generation`; it compares and
/// stores the generation saturated at `i64::MAX`. A local write applies
/// unless the stored local policy is identical, and the schema's triggers
/// assign its generation.
pub fn try_update_template_network_policy_tx(
    tx: &Transaction<'_>,
    template: &str,
    default_network_policy: &NetworkPolicyV1,
    default_network_policy_generation: u64,
    authority: PolicyAuthority,
) -> std::result::Result<TemplateNetworkPolicyUpdate, TemplateNetworkPolicyUpsertError> {
    let ts = now();
    let default_auto_json = serde_json::to_string(&AutoLifecyclePolicy::disabled())
        .map_err(|error| Error::from(rusqlite::Error::ToSqlConversionFailure(Box::new(error))))?;
    let network_json = network_policy_json(default_network_policy).map_err(Error::from)?;
    let generation = saturating_u64_to_i64(default_network_policy_generation);

    match tx.0.query_row(
        sql!("SELECT default_network_policy_json, default_network_policy_generation, default_network_policy_authority
         FROM template_settings
         WHERE template = ?1"),
        params![template],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                parse_policy_authority(row.get::<_, String>(2)?.as_str(), 2)?,
            ))
        },
    ) {
        Ok((existing_json, existing_generation, existing_authority)) => {
            if authority == PolicyAuthority::Cluster && existing_authority == PolicyAuthority::Local
            {
                Ok(TemplateNetworkPolicyUpdate::Skip)
            } else if authority == PolicyAuthority::Local {
                if existing_authority == PolicyAuthority::Local && existing_json == network_json {
                    Ok(TemplateNetworkPolicyUpdate::Skip)
                } else {
                    // The owning schema derives the committed local generation
                    // from its retained counter, including this legacy
                    // write shape when an older binary runs on the new schema.
                    tx.0.execute(
                        sql!(
                            "UPDATE template_settings
                         SET default_network_policy_json = ?1,
                             default_network_policy_generation = 0,
                             default_network_policy_authority = ?2,
                             updated_at = ?3
                         WHERE template = ?4"
                        ),
                        params![network_json, authority.as_str(), ts, template],
                    )
                    .map_err(Error::from)?;
                    Ok(TemplateNetworkPolicyUpdate::Apply)
                }
            } else if generation < existing_generation {
                Ok(TemplateNetworkPolicyUpdate::Skip)
            } else if generation == existing_generation {
                if existing_authority == authority && existing_json == network_json {
                    Ok(TemplateNetworkPolicyUpdate::Skip)
                } else {
                    Err(TemplateNetworkPolicyUpsertError::GenerationConflict)
                }
            } else {
                tx.0.execute(
                    sql!(
                        "UPDATE template_settings
                     SET default_network_policy_json = ?1,
                         default_network_policy_generation = ?2,
                         default_network_policy_authority = ?3,
                         updated_at = ?4
                     WHERE template = ?5"
                    ),
                    params![network_json, generation, authority.as_str(), ts, template],
                )
                .map_err(Error::from)?;
                Ok(TemplateNetworkPolicyUpdate::Apply)
            }
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            tx.0.execute(
                sql!(
                    "INSERT INTO template_settings
                     (template, default_auto_lifecycle_json, default_network_policy_json,
                      default_network_policy_generation, default_network_policy_authority,
                      default_auto_lifecycle_authority, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)"
                ),
                params![
                    template,
                    default_auto_json,
                    network_json,
                    generation,
                    authority.as_str(),
                    PolicyAuthority::Cluster.as_str(),
                    ts
                ],
            )
            .map_err(Error::from)?;
            Ok(TemplateNetworkPolicyUpdate::Apply)
        }
        Err(error) => Err(Error::from(error).into()),
    }
}

/// Maps the column list [`get_template_settings`] and
/// [`list_template_snapshot_policies`] both read.
fn template_settings_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TemplateSettingsRow> {
    let json: String = row.get(1)?;
    let default_auto_lifecycle = serde_json::from_str(&json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(error))
    })?;
    let default_network_policy = parse_network_policy_json(row.get(2)?, 2)?;
    Ok(TemplateSettingsRow {
        template: row.get(0)?,
        default_auto_lifecycle,
        default_auto_lifecycle_authority: parse_policy_authority(
            row.get::<_, String>(5)?.as_str(),
            5,
        )?,
        default_network_policy,
        default_network_policy_generation: nonnegative_i64_to_u64(row.get::<_, i64>(3)?, 3)?,
        default_network_policy_authority: parse_policy_authority(
            row.get::<_, String>(4)?.as_str(),
            4,
        )?,
        ensure_base_snapshot: row.get::<_, bool>(6)?,
        snapshot_vcpu: row.get(7)?,
        snapshot_memory_mib: row.get(8)?,
        base_snapshot_policy_set_at: row.get(9)?,
        last_snapshot_reconcile_started_at: row.get(10)?,
        last_snapshot_reconcile_completed_at: row.get(11)?,
        last_snapshot_reconcile_error: row.get(12)?,
    })
}

pub fn get_template_settings(
    conn: &Connection,
    template: &str,
) -> Result<Option<TemplateSettingsRow>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT template, default_auto_lifecycle_json, default_network_policy_json,
                default_network_policy_generation, default_network_policy_authority,
                default_auto_lifecycle_authority, ensure_base_snapshot, snapshot_vcpu, snapshot_memory_mib,
                base_snapshot_policy_set_at, last_snapshot_reconcile_started_at,
                last_snapshot_reconcile_completed_at, last_snapshot_reconcile_error
         FROM template_settings WHERE template = ?1"
    ))?;
    let mut rows = stmt.query_map(params![template], template_settings_row)?;
    match rows.next() {
        Some(Ok(row)) => Ok(Some(row)),
        Some(Err(error)) => Err(error.into()),
        None => Ok(None),
    }
}

pub fn update_template_snapshot_policy(
    conn: &Connection,
    template: &str,
    ensure_base_snapshot: bool,
    snapshot_vcpu: u32,
    snapshot_memory_mib: u32,
) -> Result<()> {
    let ts = now();
    // Snapshot policy may be pushed before any other settings exist for this
    // template. Seed the required lifecycle column with the disabled default.
    let default_json = serde_json::to_string(&AutoLifecyclePolicy::disabled())
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let default_network_json = default_network_policy_json()?;
    conn.0.execute(
        sql!(
            "INSERT INTO template_settings
             (template, default_auto_lifecycle_json, default_network_policy_json,
              ensure_base_snapshot, snapshot_vcpu, snapshot_memory_mib,
              base_snapshot_policy_set_at, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, ?7)
         ON CONFLICT(template) DO UPDATE SET
             ensure_base_snapshot = ?4,
             snapshot_vcpu = ?5,
             snapshot_memory_mib = ?6,
             base_snapshot_policy_set_at = ?7,
             updated_at = ?7"
        ),
        params![
            template,
            default_json,
            default_network_json,
            ensure_base_snapshot,
            snapshot_vcpu,
            snapshot_memory_mib,
            ts
        ],
    )?;
    Ok(())
}

pub fn list_template_snapshot_policies(conn: &Connection) -> Result<Vec<TemplateSettingsRow>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT template, default_auto_lifecycle_json, default_network_policy_json,
                default_network_policy_generation, default_network_policy_authority,
                default_auto_lifecycle_authority, ensure_base_snapshot, snapshot_vcpu, snapshot_memory_mib,
                base_snapshot_policy_set_at, last_snapshot_reconcile_started_at,
                last_snapshot_reconcile_completed_at, last_snapshot_reconcile_error
         FROM template_settings
         WHERE base_snapshot_policy_set_at IS NOT NULL
         ORDER BY template"
    ))?;
    let rows = stmt.query_map([], template_settings_row)?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

pub fn record_snapshot_reconcile_started(conn: &Connection, template: &str) -> Result<()> {
    let ts = now();
    conn.0.execute(
        sql!(
            "UPDATE template_settings
         SET last_snapshot_reconcile_started_at = ?1,
             last_snapshot_reconcile_error = NULL,
             updated_at = ?1
         WHERE template = ?2"
        ),
        params![ts, template],
    )?;
    Ok(())
}

pub fn record_snapshot_reconcile_completed(
    conn: &Connection,
    template: &str,
    error: Option<&str>,
) -> Result<()> {
    let ts = now();
    conn.0.execute(
        sql!(
            "UPDATE template_settings
         SET last_snapshot_reconcile_completed_at = ?1,
             last_snapshot_reconcile_error = ?2,
             updated_at = ?1
         WHERE template = ?3"
        ),
        params![ts, error, template],
    )?;
    Ok(())
}

pub struct TemplateSettingsPolicies {
    pub default_auto_lifecycle: AutoLifecyclePolicy,
    pub default_auto_lifecycle_authority: PolicyAuthority,
    pub default_network_policy: NetworkPolicyV1,
    pub default_network_policy_generation: u64,
    pub default_network_policy_authority: PolicyAuthority,
    pub ensure_base_snapshot: bool,
    pub snapshot_vcpu: u32,
    pub snapshot_memory_mib: u32,
}

pub fn list_template_settings(
    conn: &Connection,
) -> Result<BTreeMap<String, TemplateSettingsPolicies>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT template, default_auto_lifecycle_json, default_network_policy_json,
                default_network_policy_generation, default_network_policy_authority,
                default_auto_lifecycle_authority, ensure_base_snapshot, snapshot_vcpu, snapshot_memory_mib
         FROM template_settings ORDER BY template"
    ))?;
    let rows = stmt.query_map([], |row| {
        let template: String = row.get(0)?;
        let json: String = row.get(1)?;
        let default_auto_lifecycle = serde_json::from_str(&json).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                1,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?;
        let default_network_policy = parse_network_policy_json(row.get(2)?, 2)?;
        Ok((
            template,
            TemplateSettingsPolicies {
                default_auto_lifecycle,
                default_auto_lifecycle_authority: parse_policy_authority(
                    row.get::<_, String>(5)?.as_str(),
                    5,
                )?,
                default_network_policy,
                default_network_policy_generation: nonnegative_i64_to_u64(
                    row.get::<_, i64>(3)?,
                    3,
                )?,
                default_network_policy_authority: parse_policy_authority(
                    row.get::<_, String>(4)?.as_str(),
                    4,
                )?,
                ensure_base_snapshot: row.get(6)?,
                snapshot_vcpu: row.get(7)?,
                snapshot_memory_mib: row.get(8)?,
            },
        ))
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}
