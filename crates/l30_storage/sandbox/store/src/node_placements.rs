//! Reads of one node's placement history: the `sandboxes` rows whose
//! `active_node_id` or `target_node_id` names the node, in every lifecycle,
//! narrowed by stored lifecycle and by when the lifecycle last changed.
//!
//! Each statement's two branches read one placement column's rows for the
//! node, each bounded by the page size, and `UNION` deduplicates a row active
//! and targeted on the same node. They write nothing.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::StoredSandboxLifecycle;

/// A placement row's `sandboxes.lifecycle`. A value is recognized only in
/// the exact form the writers store and the lifecycle filter compares. Any
/// other text, another case or surrounding whitespace included, is listed as
/// stored, so a malformed row stands out instead of being normalized away,
/// and a lifecycle filter never matches it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodePlacementLifecycle {
    Stored(StoredSandboxLifecycle),
    Unrecognized(String),
}

impl NodePlacementLifecycle {
    /// The column's text: a recognized value's stored form, which is the
    /// text it was read from, or an unrecognized value as stored.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Stored(lifecycle) => lifecycle.as_str(),
            Self::Unrecognized(raw) => raw,
        }
    }

    fn from_stored(raw: String) -> Self {
        match raw.parse::<StoredSandboxLifecycle>() {
            Ok(lifecycle) if lifecycle.as_str() == raw => Self::Stored(lifecycle),
            _ => Self::Unrecognized(raw),
        }
    }
}

/// One placement row naming the node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodePlacementRow {
    pub id: SandboxId,
    pub lifecycle: NodePlacementLifecycle,
    pub active_node_id: Option<NodeId>,
    pub target_node_id: Option<NodeId>,
}

/// A [`NodePlacementRow`] with the time its lifecycle last changed, for the
/// newest-change-first order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodePlacementChangeRow {
    pub id: SandboxId,
    pub lifecycle: NodePlacementLifecycle,
    pub active_node_id: Option<NodeId>,
    pub target_node_id: Option<NodeId>,
    pub lifecycle_changed_at: OffsetDateTime,
}

/// A [`NodePlacementRow`] as read, its lifecycle the column's text.
#[derive(sqlx::FromRow)]
struct PlacementRecord {
    id: SandboxId,
    lifecycle: String,
    active_node_id: Option<NodeId>,
    target_node_id: Option<NodeId>,
}

impl From<PlacementRecord> for NodePlacementRow {
    fn from(record: PlacementRecord) -> Self {
        Self {
            id: record.id,
            lifecycle: NodePlacementLifecycle::from_stored(record.lifecycle),
            active_node_id: record.active_node_id,
            target_node_id: record.target_node_id,
        }
    }
}

/// A [`NodePlacementChangeRow`] as read, its lifecycle the column's text.
#[derive(sqlx::FromRow)]
struct PlacementChangeRecord {
    id: SandboxId,
    lifecycle: String,
    active_node_id: Option<NodeId>,
    target_node_id: Option<NodeId>,
    lifecycle_changed_at: OffsetDateTime,
}

impl From<PlacementChangeRecord> for NodePlacementChangeRow {
    fn from(record: PlacementChangeRecord) -> Self {
        Self {
            id: record.id,
            lifecycle: NodePlacementLifecycle::from_stored(record.lifecycle),
            active_node_id: record.active_node_id,
            target_node_id: record.target_node_id,
            lifecycle_changed_at: record.lifecycle_changed_at,
        }
    }
}

/// What a node's placement history narrows to. An unset field matches every
/// row; `lifecycles`, when set, should not be empty, since an empty list
/// matches no row.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodePlacementFilter {
    /// Stored `sandboxes.lifecycle` values, any of these.
    pub lifecycles: Option<Vec<StoredSandboxLifecycle>>,
    /// `lifecycle_changed_at` at or after this instant.
    pub changed_since: Option<OffsetDateTime>,
}

impl NodePlacementFilter {
    /// `lifecycles` in the column's form, for a `TEXT[]` bind.
    fn lifecycle_binds(&self) -> Option<Vec<&'static str>> {
        self.lifecycles.as_ref().map(|lifecycles| {
            lifecycles
                .iter()
                .map(|lifecycle| lifecycle.as_str())
                .collect()
        })
    }
}

/// The position after which a newest-change-first page starts: the last row
/// of the previous page. Rows sort by `(lifecycle_changed_at, id)`
/// descending, so rows that share a timestamp page by id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodePlacementChangeCursor {
    pub lifecycle_changed_at: OffsetDateTime,
    pub id: SandboxId,
}

/// The node's placement rows narrowed by `filter`, by id ascending, strictly
/// after `after`, at most `limit` of them.
pub async fn list_by_id(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    filter: &NodePlacementFilter,
    after: Option<SandboxId>,
    limit: i64,
) -> Result<Vec<NodePlacementRow>, sqlx::Error> {
    let records: Vec<PlacementRecord> = sqlx::query_as(sql!(
        "SELECT id, lifecycle, active_node_id, target_node_id FROM (
            (SELECT id, lifecycle, active_node_id, target_node_id
             FROM sandboxes
             WHERE active_node_id = $1 AND id > $2
               AND ($4::TEXT[] IS NULL OR lifecycle = ANY($4::TEXT[]))
               AND ($5::TIMESTAMPTZ IS NULL OR lifecycle_changed_at >= $5::TIMESTAMPTZ)
             ORDER BY id LIMIT $3)
            UNION
            (SELECT id, lifecycle, active_node_id, target_node_id
             FROM sandboxes
             WHERE target_node_id = $1 AND id > $2
               AND ($4::TEXT[] IS NULL OR lifecycle = ANY($4::TEXT[]))
               AND ($5::TIMESTAMPTZ IS NULL OR lifecycle_changed_at >= $5::TIMESTAMPTZ)
             ORDER BY id LIMIT $3)
         ) node_sandboxes ORDER BY id LIMIT $3"
    ))
    .bind(node_id)
    .bind(after.map_or(Uuid::nil(), |id| id.as_uuid()))
    .bind(limit)
    .bind(filter.lifecycle_binds())
    .bind(filter.changed_since)
    .fetch_all(executor)
    .await?;
    Ok(records.into_iter().map(Into::into).collect())
}

/// The node's placement rows narrowed by `filter`, newest lifecycle change
/// first, then by id descending, strictly after `cursor`, at most `limit` of
/// them. Each branch sorts one placement column's rows for the node, so the
/// cost is the node's own history.
pub async fn list_by_change(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    filter: &NodePlacementFilter,
    cursor: Option<NodePlacementChangeCursor>,
    limit: i64,
) -> Result<Vec<NodePlacementChangeRow>, sqlx::Error> {
    let records: Vec<PlacementChangeRecord> = sqlx::query_as(sql!(
        "SELECT id, lifecycle, active_node_id, target_node_id, lifecycle_changed_at FROM (
            (SELECT id, lifecycle, active_node_id, target_node_id, lifecycle_changed_at
             FROM sandboxes
             WHERE active_node_id = $1
               AND ($2::TIMESTAMPTZ IS NULL
                    OR (lifecycle_changed_at, id) < ($2::TIMESTAMPTZ, $3::UUID))
               AND ($5::TEXT[] IS NULL OR lifecycle = ANY($5::TEXT[]))
               AND ($6::TIMESTAMPTZ IS NULL OR lifecycle_changed_at >= $6::TIMESTAMPTZ)
             ORDER BY lifecycle_changed_at DESC, id DESC LIMIT $4)
            UNION
            (SELECT id, lifecycle, active_node_id, target_node_id, lifecycle_changed_at
             FROM sandboxes
             WHERE target_node_id = $1
               AND ($2::TIMESTAMPTZ IS NULL
                    OR (lifecycle_changed_at, id) < ($2::TIMESTAMPTZ, $3::UUID))
               AND ($5::TEXT[] IS NULL OR lifecycle = ANY($5::TEXT[]))
               AND ($6::TIMESTAMPTZ IS NULL OR lifecycle_changed_at >= $6::TIMESTAMPTZ)
             ORDER BY lifecycle_changed_at DESC, id DESC LIMIT $4)
         ) node_sandboxes ORDER BY lifecycle_changed_at DESC, id DESC LIMIT $4"
    ))
    .bind(node_id)
    .bind(cursor.map(|cursor| cursor.lifecycle_changed_at))
    .bind(cursor.map_or(Uuid::nil(), |cursor| cursor.id.as_uuid()))
    .bind(limit)
    .bind(filter.lifecycle_binds())
    .bind(filter.changed_since)
    .fetch_all(executor)
    .await?;
    Ok(records.into_iter().map(Into::into).collect())
}
