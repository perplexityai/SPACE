use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub use pplx_space_common_svc::types::{
    FACET_NETWORK_POLICY, FACET_NODE_STATUS, FACET_RUNTIME_PROVISIONING,
    FACET_SANDBOX_DESIRED_STATE, FACET_SANDBOX_INVENTORY, FACET_TEMPLATE_BASE_SNAPSHOT_POLICY,
    FACET_TEMPLATE_DESIRED_STATE, FACET_TEMPLATE_INVENTORY, NetworkPolicyTarget, ReconcileFacet,
};
use pplx_space_node_api_client::proto::{
    FacetDirection, FacetQueueItem, FacetRef as ProtoFacetRef, FacetSyncOverride,
    NetworkPolicyTargetRef, ReconcileFacet as ProtoReconcileFacet, facet_ref,
    network_policy_target_ref,
};
use rusqlite::{Connection, OptionalExtension, params};

pub const ELEMENT_ALL: &str = "";
pub const DIRECTION_INBOUND: &str = "inbound";
pub const DIRECTION_OUTBOUND: &str = "outbound";
const CLAIM_STALE_AFTER_SECS: i64 = 120;
const MAX_ATTEMPTS: i64 = 16;
const MAX_RETRY_DELAY_SECS: i64 = 300;
static CLAIM_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct FacetRef {
    pub facet: String,
    pub element: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct DirtyFacetBatch {
    claim_token: String,
    facets: BTreeMap<String, Option<BTreeMap<String, i64>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncDirection {
    Inbound,
    Outbound,
}

impl SyncDirection {
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Inbound => DIRECTION_INBOUND,
            Self::Outbound => DIRECTION_OUTBOUND,
        }
    }
}

impl TryFrom<FacetDirection> for SyncDirection {
    type Error = &'static str;

    fn try_from(value: FacetDirection) -> Result<Self, Self::Error> {
        match value {
            FacetDirection::Inbound => Ok(Self::Inbound),
            FacetDirection::Outbound => Ok(Self::Outbound),
            FacetDirection::Unspecified => Err("direction must be inbound or outbound"),
        }
    }
}

impl From<SyncDirection> for FacetDirection {
    fn from(value: SyncDirection) -> Self {
        match value {
            SyncDirection::Inbound => Self::Inbound,
            SyncDirection::Outbound => Self::Outbound,
        }
    }
}

impl FacetRef {
    pub fn new(facet: impl Into<String>, element: Option<impl Into<String>>) -> Self {
        Self {
            facet: facet.into(),
            element: element.map(Into::into),
        }
    }

    pub fn from_proto(value: ProtoFacetRef) -> Result<Self, String> {
        let facet = proto_facet_name(
            ProtoReconcileFacet::try_from(value.facet).unwrap_or(ProtoReconcileFacet::Unspecified),
        )?;
        let element = proto_facet_element(facet, value.element)?;
        Ok(Self {
            facet: facet.to_string(),
            element,
        })
    }

    pub fn to_proto(&self) -> Result<ProtoFacetRef, String> {
        let facet = ReconcileFacet::parse(&self.facet)
            .ok_or_else(|| format!("unknown facet {:?}", self.facet))?;
        Ok(ProtoFacetRef {
            facet: proto_facet(facet) as i32,
            element: self
                .element
                .as_deref()
                .map(|element| element_to_proto(facet, element))
                .transpose()?,
        })
    }
}

fn proto_facet(value: ReconcileFacet) -> ProtoReconcileFacet {
    match value {
        ReconcileFacet::NetworkPolicy => ProtoReconcileFacet::NetworkPolicy,
        ReconcileFacet::SandboxInventory => ProtoReconcileFacet::SandboxInventory,
        ReconcileFacet::TemplateInventory => ProtoReconcileFacet::TemplateInventory,
        ReconcileFacet::TemplateBaseSnapshotPolicy => {
            ProtoReconcileFacet::TemplateBaseSnapshotPolicy
        }
        ReconcileFacet::SandboxDesiredState => ProtoReconcileFacet::SandboxDesiredState,
        ReconcileFacet::TemplateDesiredState => ProtoReconcileFacet::TemplateDesiredState,
        ReconcileFacet::NodeStatus => ProtoReconcileFacet::NodeStatus,
        ReconcileFacet::RuntimeProvisioning => ProtoReconcileFacet::RuntimeProvisioning,
    }
}

fn proto_facet_name(value: ProtoReconcileFacet) -> Result<&'static str, String> {
    match value {
        ProtoReconcileFacet::NetworkPolicy => Ok(FACET_NETWORK_POLICY),
        ProtoReconcileFacet::SandboxInventory => Ok(FACET_SANDBOX_INVENTORY),
        ProtoReconcileFacet::TemplateInventory => Ok(FACET_TEMPLATE_INVENTORY),
        ProtoReconcileFacet::TemplateBaseSnapshotPolicy => Ok(FACET_TEMPLATE_BASE_SNAPSHOT_POLICY),
        ProtoReconcileFacet::SandboxDesiredState => Ok(FACET_SANDBOX_DESIRED_STATE),
        ProtoReconcileFacet::TemplateDesiredState => Ok(FACET_TEMPLATE_DESIRED_STATE),
        ProtoReconcileFacet::NodeStatus => Ok(FACET_NODE_STATUS),
        ProtoReconcileFacet::RuntimeProvisioning => Ok(FACET_RUNTIME_PROVISIONING),
        ProtoReconcileFacet::Unspecified => Err("facet must be specified".to_string()),
    }
}

fn proto_facet_element(
    facet: &str,
    element: Option<facet_ref::Element>,
) -> Result<Option<String>, String> {
    match (facet, element) {
        (_, None) => Ok(None),
        (FACET_NETWORK_POLICY, Some(facet_ref::Element::NetworkPolicyTarget(target))) => {
            network_policy_target_from_proto(target).map(Some)
        }
        (
            FACET_SANDBOX_INVENTORY | FACET_SANDBOX_DESIRED_STATE,
            Some(facet_ref::Element::SandboxId(sandbox_id)),
        ) => Ok(Some(sandbox_id)),
        (
            FACET_TEMPLATE_INVENTORY
            | FACET_TEMPLATE_BASE_SNAPSHOT_POLICY
            | FACET_TEMPLATE_DESIRED_STATE,
            Some(facet_ref::Element::Template(template)),
        ) => Ok(Some(template)),
        (FACET_NODE_STATUS | FACET_RUNTIME_PROVISIONING, Some(_)) => {
            Err("whole-node facet does not accept an element".into())
        }
        (facet, Some(_)) => Err(format!("element type does not match facet {facet:?}")),
    }
}

fn network_policy_target_from_proto(target: NetworkPolicyTargetRef) -> Result<String, String> {
    match target.target {
        Some(network_policy_target_ref::Target::Template(template)) => {
            let template = pplx_space_common_svc::core::types::TemplateName::parse(template)
                .map_err(|error| error.to_string())?;
            Ok(NetworkPolicyTarget::template(template).as_element_string())
        }
        Some(network_policy_target_ref::Target::SandboxId(sandbox_id)) => {
            let sandbox_id = sandbox_id
                .parse::<pplx_space_common_svc::core::types::SandboxId>()
                .map_err(|error| error.to_string())?;
            Ok(NetworkPolicyTarget::sandbox(sandbox_id).as_element_string())
        }
        None => Err("network policy target is required".to_string()),
    }
}

fn element_to_proto(facet: ReconcileFacet, element: &str) -> Result<facet_ref::Element, String> {
    match facet {
        ReconcileFacet::NetworkPolicy => {
            let target = element
                .parse::<NetworkPolicyTarget>()
                .map_err(|error| error.to_string())?;
            let target = match target {
                NetworkPolicyTarget::Template(template) => NetworkPolicyTargetRef {
                    target: Some(network_policy_target_ref::Target::Template(
                        template.to_string(),
                    )),
                },
                NetworkPolicyTarget::Sandbox(sandbox_id) => NetworkPolicyTargetRef {
                    target: Some(network_policy_target_ref::Target::SandboxId(
                        sandbox_id.to_string(),
                    )),
                },
            };
            Ok(facet_ref::Element::NetworkPolicyTarget(target))
        }
        ReconcileFacet::SandboxInventory | ReconcileFacet::SandboxDesiredState => {
            Ok(facet_ref::Element::SandboxId(element.to_string()))
        }
        ReconcileFacet::TemplateInventory
        | ReconcileFacet::TemplateBaseSnapshotPolicy
        | ReconcileFacet::TemplateDesiredState => {
            Ok(facet_ref::Element::Template(element.to_string()))
        }
        ReconcileFacet::NodeStatus | ReconcileFacet::RuntimeProvisioning => {
            Err("whole-node facet does not accept an element".into())
        }
    }
}

impl DirtyFacetBatch {
    fn claimed(claim_token: String) -> Self {
        Self {
            claim_token,
            facets: BTreeMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.facets.is_empty()
    }

    pub fn empty_with_claim(&self) -> Self {
        Self {
            claim_token: self.claim_token.clone(),
            facets: BTreeMap::new(),
        }
    }

    pub fn copy_scope_from(&mut self, source: &Self, facet: &str) {
        if let Some(elements) = source.facets.get(facet) {
            self.facets.insert(facet.to_string(), elements.clone());
        }
    }

    pub fn facets(&self) -> impl Iterator<Item = (&str, Option<Vec<String>>)> + '_ {
        self.facets.iter().map(|(facet, elements)| {
            let scope = elements.as_ref().and_then(|values| {
                if values.contains_key(ELEMENT_ALL) {
                    None
                } else {
                    Some(values.keys().cloned().collect::<Vec<_>>())
                }
            });
            (facet.as_str(), scope)
        })
    }

    pub fn push_ref(&mut self, reference: FacetRef) {
        match reference.element {
            Some(element) => self.insert(reference.facet, Some(element)),
            None => self.insert(reference.facet, None),
        }
    }

    fn insert(&mut self, facet: String, element: Option<String>) {
        self.insert_generation(facet, element, 0);
    }

    fn insert_generation(&mut self, facet: String, element: Option<String>, generation: i64) {
        match element {
            Some(element) => {
                if let Some(Some(elements)) = self.facets.get_mut(&facet) {
                    elements.insert(element, generation);
                } else {
                    self.facets.entry(facet).or_insert_with(|| {
                        let mut elements = BTreeMap::new();
                        elements.insert(element, generation);
                        Some(elements)
                    });
                }
            }
            None => {
                if let Some(Some(elements)) = self.facets.get_mut(&facet) {
                    elements.insert(ELEMENT_ALL.to_string(), generation);
                } else {
                    self.facets.entry(facet).or_insert_with(|| {
                        let mut elements = BTreeMap::new();
                        elements.insert(ELEMENT_ALL.to_string(), generation);
                        Some(elements)
                    });
                }
            }
        }
    }
}

pub fn mark_refs(conn: &mut Connection, refs: &[FacetRef]) -> rusqlite::Result<()> {
    let now = crate::now_secs();
    let tx = conn.transaction()?;
    for reference in refs {
        let element = reference.element.as_deref().unwrap_or(ELEMENT_ALL);
        if element == ELEMENT_ALL {
            tx.execute(
                "DELETE FROM dirty_facets WHERE facet = ?1 AND element != ''",
                params![reference.facet],
            )?;
        } else {
            let whole_facet_dirty: Option<i64> = tx
                .query_row(
                    "SELECT dirty FROM dirty_facets WHERE facet = ?1 AND element = ''",
                    params![reference.facet],
                    |row| row.get(0),
                )
                .optional()?;
            if whole_facet_dirty.is_some_and(|dirty| dirty != 0) {
                continue;
            }
        }
        tx.execute(
            "INSERT INTO dirty_facets
                 (facet, element, dirty, dirty_generation, attempts, last_error,
                  updated_at, last_attempt_at, next_attempt_at, claim_token, claimed_at)
             VALUES (?1, ?2, 1, 1, 0, '', ?3, 0, ?3, '', 0)
             ON CONFLICT(facet, element) DO UPDATE
             SET dirty = 1,
                 dirty_generation = dirty_generation + 1,
                 attempts = 0,
                 last_error = '',
                 dead_at = 0,
                 dead_reason = '',
                 updated_at = ?3,
                 next_attempt_at = ?3,
                 claim_token = '',
                 claimed_at = 0",
            params![reference.facet, element, now],
        )?;
    }
    tx.commit()
}

pub fn read(db: &Arc<Mutex<Connection>>) -> rusqlite::Result<DirtyFacetBatch> {
    let now = crate::now_secs();
    let claim_token = next_claim_token(now);
    let db = db
        .lock()
        .map_err(|_poisoned| rusqlite::Error::InvalidQuery)?;
    db.execute(
        "UPDATE dirty_facets
         SET claim_token = ?1,
             claimed_at = ?2,
             last_attempt_at = ?2
         WHERE dirty != 0
           AND next_attempt_at <= ?2
           AND dead_at = 0
           AND (
               claim_token = ''
               OR claimed_at <= ?3
           )",
        params![claim_token, now, now.saturating_sub(CLAIM_STALE_AFTER_SECS)],
    )?;
    let mut stmt = db.prepare(
        "SELECT facet, element, dirty_generation
         FROM dirty_facets
         WHERE dirty != 0
           AND claim_token = ?1
         ORDER BY facet ASC, element ASC",
    )?;
    let rows = stmt.query_map(params![claim_token], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })?;
    let mut batch = DirtyFacetBatch::claimed(claim_token);
    for row in rows {
        let (facet, element, generation) = row?;
        batch.insert_generation(
            facet,
            if element.is_empty() {
                None
            } else {
                Some(element)
            },
            generation,
        );
    }
    Ok(batch)
}

pub fn clear(db: &Arc<Mutex<Connection>>, batch: &DirtyFacetBatch) -> rusqlite::Result<()> {
    let mut db = db
        .lock()
        .map_err(|_poisoned| rusqlite::Error::InvalidQuery)?;
    let tx = db.transaction()?;
    for (facet, elements) in &batch.facets {
        if let Some(elements) = elements {
            for (element, generation) in elements {
                tx.execute(
                    "DELETE FROM dirty_facets
                     WHERE facet = ?1
                       AND element = ?2
                       AND dirty_generation = ?3
                       AND claim_token = ?4",
                    params![facet, element, generation, batch.claim_token],
                )?;
                tx.execute(
                    "UPDATE dirty_facets
                     SET claim_token = '',
                         claimed_at = 0
                     WHERE facet = ?1
                       AND element = ?2
                       AND claim_token = ?3",
                    params![facet, element, batch.claim_token],
                )?;
            }
        }
    }
    tx.commit()
}

pub fn mark_error(
    db: &Arc<Mutex<Connection>>,
    batch: &DirtyFacetBatch,
    error: &str,
) -> rusqlite::Result<()> {
    let now = crate::now_secs();
    let mut db = db
        .lock()
        .map_err(|_poisoned| rusqlite::Error::InvalidQuery)?;
    let tx = db.transaction()?;
    for (facet, elements) in &batch.facets {
        if let Some(elements) = elements {
            for (element, generation) in elements {
                mark_error_row(
                    &tx,
                    facet,
                    element,
                    *generation,
                    &batch.claim_token,
                    error,
                    now,
                )?;
            }
        }
    }
    tx.commit()
}

fn mark_error_row(
    tx: &rusqlite::Transaction<'_>,
    facet: &str,
    element: &str,
    generation: i64,
    claim_token: &str,
    error: &str,
    now: i64,
) -> rusqlite::Result<()> {
    let attempts_before_failure = current_attempts(tx, facet, element)?;
    let attempts_after_failure = attempts_before_failure.saturating_add(1);
    if attempts_after_failure >= MAX_ATTEMPTS {
        let changed = tx.execute(
            "UPDATE dirty_facets
             SET attempts = attempts + 1,
                 last_error = ?3,
                 last_attempt_at = ?4,
                 dead_at = ?4,
                 dead_reason = ?3,
                 claim_token = '',
                 claimed_at = 0,
                 updated_at = ?4
             WHERE facet = ?1
               AND element = ?2
               AND dirty_generation = ?5
               AND claim_token = ?6",
            params![facet, element, error, now, generation, claim_token],
        )?;
        if changed == 0 {
            release_refreshed_claim(tx, facet, element, claim_token, now)?;
        } else {
            pplx_space_metrics::reconcile::queue_dead_letter(facet);
        }
        return Ok(());
    }

    let changed = tx.execute(
        "UPDATE dirty_facets
         SET attempts = attempts + 1,
             last_error = ?3,
             last_attempt_at = ?4,
             next_attempt_at = ?5,
             claim_token = '',
             claimed_at = 0,
             updated_at = ?4
         WHERE facet = ?1
           AND element = ?2
           AND dirty_generation = ?6
           AND claim_token = ?7",
        params![
            facet,
            element,
            error,
            now,
            next_attempt_at(now, attempts_before_failure),
            generation,
            claim_token
        ],
    )?;
    if changed == 0 {
        release_refreshed_claim(tx, facet, element, claim_token, now)?;
    }
    Ok(())
}

fn release_refreshed_claim(
    tx: &rusqlite::Transaction<'_>,
    facet: &str,
    element: &str,
    claim_token: &str,
    now: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "UPDATE dirty_facets
         SET claim_token = '',
             claimed_at = 0,
             next_attempt_at = ?4,
             updated_at = ?4
         WHERE facet = ?1
           AND element = ?2
           AND claim_token = ?3",
        params![facet, element, claim_token, now],
    )?;
    Ok(())
}

pub fn set_disabled(
    db: &Arc<Mutex<Connection>>,
    facet: &str,
    direction: SyncDirection,
    disabled_until: Option<i64>,
    reason: &str,
) -> rusqlite::Result<()> {
    let now = crate::now_secs();
    let db = db
        .lock()
        .map_err(|_poisoned| rusqlite::Error::InvalidQuery)?;
    db.execute(
        "INSERT INTO facet_sync_overrides
             (facet, direction, disabled_until, reason, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(facet, direction) DO UPDATE
         SET disabled_until = excluded.disabled_until,
             reason = excluded.reason,
             updated_at = excluded.updated_at",
        params![facet, direction.as_db(), disabled_until, reason, now],
    )?;
    Ok(())
}

pub fn clear_disabled(
    db: &Arc<Mutex<Connection>>,
    facet: &str,
    direction: SyncDirection,
) -> rusqlite::Result<()> {
    let db = db
        .lock()
        .map_err(|_poisoned| rusqlite::Error::InvalidQuery)?;
    db.execute(
        "DELETE FROM facet_sync_overrides WHERE facet = ?1 AND direction = ?2",
        params![facet, direction.as_db()],
    )?;
    Ok(())
}

pub fn is_disabled(
    db: &Arc<Mutex<Connection>>,
    facet: &str,
    direction: SyncDirection,
) -> rusqlite::Result<bool> {
    let now = crate::now_secs();
    let db = db
        .lock()
        .map_err(|_poisoned| rusqlite::Error::InvalidQuery)?;
    let row: Option<Option<i64>> = db
        .query_row(
            "SELECT disabled_until
             FROM facet_sync_overrides
             WHERE facet = ?1 AND direction = ?2",
            params![facet, direction.as_db()],
            |row| row.get(0),
        )
        .optional()?;
    Ok(match row {
        Some(Some(disabled_until)) => disabled_until > now,
        Some(None) => true,
        None => false,
    })
}

pub fn list_overrides(db: &Arc<Mutex<Connection>>) -> rusqlite::Result<Vec<FacetSyncOverride>> {
    let now = crate::now_secs();
    let db = db
        .lock()
        .map_err(|_poisoned| rusqlite::Error::InvalidQuery)?;
    let mut stmt = db.prepare(
        "SELECT facet, direction, disabled_until, reason, updated_at
         FROM facet_sync_overrides
         WHERE disabled_until IS NULL OR disabled_until > ?1
         ORDER BY facet ASC, direction ASC",
    )?;
    let rows = stmt.query_map(params![now], |row| {
        let direction = match row.get::<_, String>(1)?.as_str() {
            DIRECTION_INBOUND => FacetDirection::Inbound,
            DIRECTION_OUTBOUND => FacetDirection::Outbound,
            _ => FacetDirection::Unspecified,
        };
        let disabled_until = row.get::<_, Option<i64>>(2)?;
        Ok(FacetSyncOverride {
            facet: row.get(0)?,
            direction: direction.into(),
            disabled_until_unix_secs: disabled_until
                .filter(|value| *value > now)
                .and_then(|value| u64::try_from(value).ok()),
            reason: row.get(3)?,
            updated_at_unix_secs: i64_to_u64(row.get(4)?),
        })
    })?;
    rows.collect()
}

pub fn list_queue(
    db: &Arc<Mutex<Connection>>,
    facet_filter: Option<&str>,
) -> rusqlite::Result<Vec<FacetQueueItem>> {
    let db = db
        .lock()
        .map_err(|_poisoned| rusqlite::Error::InvalidQuery)?;
    let mut rows = Vec::new();
    if let Some(facet) = facet_filter {
        let mut stmt = db.prepare(
            "SELECT facet, element, attempts, last_error, updated_at, last_attempt_at
             FROM dirty_facets
             WHERE dirty != 0 AND facet = ?1
             ORDER BY facet ASC, element ASC",
        )?;
        let mapped = stmt.query_map(params![facet], queue_item_from_row)?;
        for row in mapped {
            rows.push(row?);
        }
    } else {
        let mut stmt = db.prepare(
            "SELECT facet, element, attempts, last_error, updated_at, last_attempt_at
             FROM dirty_facets
             WHERE dirty != 0
             ORDER BY facet ASC, element ASC",
        )?;
        let mapped = stmt.query_map([], queue_item_from_row)?;
        for row in mapped {
            rows.push(row?);
        }
    }
    Ok(rows)
}

fn queue_item_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<FacetQueueItem> {
    let element = row.get::<_, String>(1)?;
    Ok(FacetQueueItem {
        facet: row.get(0)?,
        element: if element.is_empty() {
            None
        } else {
            Some(element)
        },
        attempts: i64_to_u64(row.get(2)?),
        last_error: row.get(3)?,
        updated_at_unix_secs: i64_to_u64(row.get(4)?),
        last_attempt_at_unix_secs: i64_to_u64(row.get(5)?),
    })
}

fn next_claim_token(now: i64) -> String {
    let seq = CLAIM_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}-{now}-{seq}", std::process::id())
}

fn current_attempts(db: &Connection, facet: &str, element: &str) -> rusqlite::Result<i64> {
    db.query_row(
        "SELECT attempts FROM dirty_facets WHERE facet = ?1 AND element = ?2",
        params![facet, element],
        |row| row.get(0),
    )
    .optional()
    .map(|value| value.unwrap_or(0))
}

fn next_attempt_at(now: i64, attempts_before_failure: i64) -> i64 {
    let attempts = attempts_before_failure.saturating_add(1).clamp(1, 16);
    let delay = (1_i64 << (attempts - 1)).min(MAX_RETRY_DELAY_SECS);
    now.saturating_add(delay)
}

fn i64_to_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

#[cfg(test)]
mod tests;
