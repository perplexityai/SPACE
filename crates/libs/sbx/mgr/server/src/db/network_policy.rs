//! Network-policy columns, the reconcile queue, and the node dirty-ref outbox.

use super::*;

const RECONCILE_FACET_NETWORK_POLICY: i32 = 1;

const RECONCILE_FACET_SANDBOX_INVENTORY: i32 = 2;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum NetworkPolicyAppliedSource {
    #[default]
    Unknown,
    Template,
    Override,
}

impl NetworkPolicyAppliedSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "",
            Self::Template => "template",
            Self::Override => "override",
        }
    }

    pub fn parse(value: &str) -> Self {
        value.parse().unwrap_or(Self::Unknown)
    }

    pub fn as_proto(self) -> i32 {
        match self {
            Self::Unknown => NetworkPolicySource::Unspecified as i32,
            Self::Template => NetworkPolicySource::Template as i32,
            Self::Override => NetworkPolicySource::Override as i32,
        }
    }
}

impl fmt::Display for NetworkPolicyAppliedSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for NetworkPolicyAppliedSource {
    type Err = NetworkPolicyAppliedSourceParseError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "" => Ok(Self::Unknown),
            "template" => Ok(Self::Template),
            "override" => Ok(Self::Override),
            _ => Err(NetworkPolicyAppliedSourceParseError),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
#[error("network policy applied source must be template or override")]
pub struct NetworkPolicyAppliedSourceParseError;

pub fn set_network_policy_fields(
    conn: &Connection,
    id: &str,
    network_policy_override: Option<&NetworkPolicy>,
    network_policy_generation: u64,
    network_policy_authority: PolicyAuthority,
) -> Result<()> {
    let network_policy_override_json = network_policy_override
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    conn.execute(
        "UPDATE sandboxes
         SET network_policy_override_json = ?1,
             network_policy_generation = ?2,
             network_policy_authority = ?3,
             updated_at = ?4
         WHERE id = ?5",
        params![
            network_policy_override_json.as_deref(),
            u64_to_sql_i64(network_policy_generation),
            network_policy_authority.as_str(),
            now()?,
            id
        ],
    )?;
    Ok(())
}

pub fn update_network_policy_override(
    conn: &Connection,
    id: &str,
    network_policy_override: Option<&NetworkPolicy>,
    network_policy_generation: u64,
    authority: PolicyAuthority,
    enqueue_live_gateway: bool,
    mark_dirty: bool,
) -> Result<NetworkPolicyOverrideUpdateOutcome> {
    let network_policy_override_json = network_policy_override
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let generation = u64_to_sql_i64(network_policy_generation);
    let tx = immediate_transaction(conn)?;
    let (existing_json, existing_generation, existing_authority) = tx.query_row(
        "SELECT network_policy_override_json, network_policy_generation, network_policy_authority
         FROM sandboxes
         WHERE id = ?1",
        params![id],
        |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, i64>(1)?,
                parse_policy_authority(row.get::<_, String>(2)?.as_str(), 2)?,
            ))
        },
    )?;

    let mut queued_live_gateway = false;
    let mut queued_dirty_ref = false;
    let plan = decide_network_policy_override_update(
        NetworkPolicyOverrideExisting {
            json: existing_json.as_deref(),
            generation: existing_generation,
            authority: existing_authority,
        },
        NetworkPolicyOverrideRequested {
            json: network_policy_override_json.as_deref(),
            generation,
            authority,
        },
    );
    let decision = match plan {
        NetworkPolicyOverrideUpdatePlan::Apply {
            generation,
            authority,
        } => {
            tx.execute(
                "UPDATE sandboxes
             SET network_policy_override_json = ?1,
                 network_policy_generation = ?2,
                 network_policy_authority = ?3,
                 updated_at = ?4
             WHERE id = ?5",
                params![
                    network_policy_override_json.as_deref(),
                    generation,
                    authority.as_str(),
                    now()?,
                    id
                ],
            )?;
            if enqueue_live_gateway {
                queued_live_gateway = enqueue_network_policy_reconcile_if_live_tx(&tx, id)?;
            }
            if mark_dirty {
                queued_dirty_ref = enqueue_node_dirty_network_policy_sandbox_tx(&tx, id)?;
            }
            NetworkPolicyOverrideUpdate::Apply
        }
        NetworkPolicyOverrideUpdatePlan::RepairIfLiveGateway => {
            if enqueue_live_gateway {
                queued_live_gateway = enqueue_network_policy_reconcile_if_live_tx(&tx, id)?;
            }
            if queued_live_gateway {
                NetworkPolicyOverrideUpdate::Repair
            } else {
                NetworkPolicyOverrideUpdate::Skip
            }
        }
        NetworkPolicyOverrideUpdatePlan::Skip => NetworkPolicyOverrideUpdate::Skip,
        NetworkPolicyOverrideUpdatePlan::Conflict => NetworkPolicyOverrideUpdate::Conflict,
    };
    tx.commit()?;
    Ok(NetworkPolicyOverrideUpdateOutcome {
        decision,
        queued_live_gateway,
        queued_dirty_ref,
    })
}

pub fn clear_local_network_policy(
    conn: &Connection,
    id: &str,
    enqueue_live_gateway: bool,
    mark_dirty: bool,
) -> Result<NetworkPolicyOverrideUpdateOutcome> {
    let tx = immediate_transaction(conn)?;
    let existing_authority = tx.query_row(
        "SELECT network_policy_authority FROM sandboxes WHERE id = ?1",
        params![id],
        |row| parse_policy_authority(row.get::<_, String>(0)?.as_str(), 0),
    )?;
    let mut queued_live_gateway = false;
    let mut queued_dirty_ref = false;
    let decision = if existing_authority == PolicyAuthority::Cluster {
        if enqueue_live_gateway {
            queued_live_gateway = enqueue_network_policy_reconcile_if_live_tx(&tx, id)?;
        }
        if queued_live_gateway {
            NetworkPolicyOverrideUpdate::Repair
        } else {
            NetworkPolicyOverrideUpdate::Skip
        }
    } else {
        tx.execute(
            "UPDATE sandboxes
             SET network_policy_override_json = NULL,
                 network_policy_generation = 0,
                 network_policy_authority = ?1,
                 updated_at = ?2
             WHERE id = ?3",
            params![PolicyAuthority::Cluster.as_str(), now()?, id],
        )?;
        if enqueue_live_gateway {
            queued_live_gateway = enqueue_network_policy_reconcile_if_live_tx(&tx, id)?;
        }
        if mark_dirty {
            queued_dirty_ref = enqueue_node_dirty_network_policy_sandbox_tx(&tx, id)?;
        }
        NetworkPolicyOverrideUpdate::Apply
    };
    tx.commit()?;
    Ok(NetworkPolicyOverrideUpdateOutcome {
        decision,
        queued_live_gateway,
        queued_dirty_ref,
    })
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum NetworkPolicyOverrideUpdate {
    Skip,
    Apply,
    Repair,
    Conflict,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct NetworkPolicyOverrideUpdateOutcome {
    pub decision: NetworkPolicyOverrideUpdate,
    pub queued_live_gateway: bool,
    pub queued_dirty_ref: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) struct NetworkPolicyOverrideExisting<'a> {
    pub(super) json: Option<&'a str>,
    pub(super) generation: i64,
    pub(super) authority: PolicyAuthority,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) struct NetworkPolicyOverrideRequested<'a> {
    pub(super) json: Option<&'a str>,
    pub(super) generation: i64,
    pub(super) authority: PolicyAuthority,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum NetworkPolicyOverrideUpdatePlan {
    Skip,
    RepairIfLiveGateway,
    Conflict,
    Apply {
        generation: i64,
        authority: PolicyAuthority,
    },
}

pub(super) fn decide_network_policy_override_update(
    existing: NetworkPolicyOverrideExisting<'_>,
    requested: NetworkPolicyOverrideRequested<'_>,
) -> NetworkPolicyOverrideUpdatePlan {
    if requested.authority == PolicyAuthority::Cluster
        && existing.authority == PolicyAuthority::Local
    {
        return NetworkPolicyOverrideUpdatePlan::Skip;
    }

    if requested.authority == PolicyAuthority::Local {
        if existing.authority == PolicyAuthority::Local && existing.json == requested.json {
            return NetworkPolicyOverrideUpdatePlan::RepairIfLiveGateway;
        }
        return NetworkPolicyOverrideUpdatePlan::Apply {
            generation: 0,
            authority: PolicyAuthority::Local,
        };
    }

    if requested.generation < existing.generation {
        return NetworkPolicyOverrideUpdatePlan::Skip;
    }
    if requested.generation == existing.generation {
        if existing.authority == requested.authority && existing.json == requested.json {
            NetworkPolicyOverrideUpdatePlan::RepairIfLiveGateway
        } else {
            NetworkPolicyOverrideUpdatePlan::Conflict
        }
    } else {
        NetworkPolicyOverrideUpdatePlan::Apply {
            generation: requested.generation,
            authority: requested.authority,
        }
    }
}

pub fn enqueue_network_policy_reconcile_if_live(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let queued = enqueue_network_policy_reconcile_if_live_tx(&tx, sandbox_id)?;
    tx.commit()?;
    Ok(queued)
}

fn enqueue_network_policy_reconcile_if_live_tx(
    tx: &rusqlite::Transaction<'_>,
    sandbox_id: &str,
) -> Result<bool> {
    let ts = now()?;
    let changed = tx.execute(
        "INSERT INTO network_policy_reconcile_queue (sandbox_id, requested_at, updated_at)
         SELECT ?1, ?2, ?2
         WHERE EXISTS (
             SELECT 1 FROM sandboxes
             WHERE id = ?1
               AND netgtw_pid IS NOT NULL
               AND pool_template IS NULL
         )
         ON CONFLICT(sandbox_id) DO UPDATE SET
             requested_at = excluded.requested_at,
             updated_at = excluded.updated_at",
        params![sandbox_id, ts],
    )?;
    Ok(changed != 0)
}

fn enqueue_node_dirty_network_policy_sandbox_tx(
    tx: &rusqlite::Transaction<'_>,
    sandbox_id: &str,
) -> Result<bool> {
    enqueue_node_dirty_ref_tx(
        tx,
        RECONCILE_FACET_NETWORK_POLICY,
        "network_policy_sandbox",
        sandbox_id,
    )
}

pub fn enqueue_node_dirty_sandbox_inventory(conn: &Connection, sandbox_id: &str) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let queued = enqueue_node_dirty_sandbox_inventory_tx(&tx, sandbox_id)?;
    tx.commit()?;
    Ok(queued)
}

fn enqueue_node_dirty_sandbox_inventory_tx(
    tx: &rusqlite::Transaction<'_>,
    sandbox_id: &str,
) -> Result<bool> {
    enqueue_node_dirty_ref_tx(
        tx,
        RECONCILE_FACET_SANDBOX_INVENTORY,
        "sandbox_inventory",
        sandbox_id,
    )
}

fn enqueue_node_dirty_ref_tx(
    tx: &rusqlite::Transaction<'_>,
    facet: i32,
    element_kind: &str,
    element_value: &str,
) -> Result<bool> {
    let ts = now()?;
    let changed = tx.execute(
        "INSERT INTO node_dirty_refs
             (facet, element_kind, element_value, requested_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?4)
         ON CONFLICT(facet, element_kind, element_value) DO UPDATE SET
             requested_at = excluded.requested_at,
             updated_at = excluded.updated_at",
        params![facet, element_kind, element_value, ts],
    )?;
    Ok(changed != 0)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDirtyRefRow {
    pub facet: i32,
    pub element_kind: String,
    pub element_value: String,
    /// Opaque send witness; timestamps cannot distinguish same-second writes.
    pub revision: i64,
}

pub fn list_node_dirty_refs(conn: &Connection, limit: usize) -> Result<Vec<NodeDirtyRefRow>> {
    let mut stmt = conn.prepare(
        "SELECT facet, element_kind, element_value, revision
         FROM node_dirty_refs
         ORDER BY requested_at ASC
         LIMIT ?1",
    )?;
    let rows = stmt.query_map(params![limit.min(i64::MAX as usize) as i64], |row| {
        Ok(NodeDirtyRefRow {
            facet: row.get(0)?,
            element_kind: row.get(1)?,
            element_value: row.get(2)?,
            revision: row.get(3)?,
        })
    })?;
    rows.collect()
}

/// Acknowledge only the obligations read before sending. A newer enqueue, even
/// for a previously deleted key, remains pending for the worker's next retry.
pub fn delete_node_dirty_refs(conn: &Connection, refs: &[NodeDirtyRefRow]) -> Result<usize> {
    let tx = immediate_transaction(conn)?;
    let mut deleted = 0;
    for reference in refs {
        deleted += tx.execute(
            "DELETE FROM node_dirty_refs
             WHERE facet = ?1
               AND element_kind = ?2
               AND element_value = ?3
               AND revision = ?4",
            params![
                reference.facet,
                reference.element_kind,
                reference.element_value,
                reference.revision
            ],
        )?;
    }
    tx.commit()?;
    Ok(deleted)
}

pub fn delete_network_policy_reconcile_rows<I>(conn: &Connection, sandbox_ids: I) -> Result<usize>
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let tx = immediate_transaction(conn)?;
    let mut deleted = 0;
    for sandbox_id in sandbox_ids {
        deleted += tx.execute(
            "DELETE FROM network_policy_reconcile_queue WHERE sandbox_id = ?1",
            params![sandbox_id.as_ref()],
        )?;
    }
    tx.commit()?;
    Ok(deleted)
}

pub fn mark_network_policy_applied(
    conn: &Connection,
    id: &str,
    applied_network_policy_source: NetworkPolicyAppliedSource,
    applied_network_policy_generation: u64,
) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET applied_network_policy_generation = ?1,
             applied_network_policy_source = ?2,
             updated_at = ?3
         WHERE id = ?4
           AND (
               ?1 > applied_network_policy_generation
               OR applied_network_policy_source != ?2
           )",
        params![
            u64_to_sql_i64(applied_network_policy_generation),
            applied_network_policy_source.as_str(),
            now()?,
            id
        ],
    )?;
    Ok(changed != 0)
}
