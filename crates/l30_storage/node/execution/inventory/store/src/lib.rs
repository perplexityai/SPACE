//! Owning store for each node's reported runtime inventory in control-plane
//! Postgres.
//!
//! * `node_runtime_installations`: the runtimes a node has installed.
//! * `node_runtime_guests`: the guest platforms each installed runtime runs.
//! * `node_runtime_state_formats`: the state formats each installed runtime
//!   can create or load.
//! * `node_artifact_components`: the artifact components a node has
//!   installed.
//!
//! A publication replaces a node's inventory with the one it reported: each
//! `prune_*` verb deletes the node's rows the report no longer names, and the
//! matching `insert_*` or `upsert_*` verb writes the rows it does, leaving
//! unchanged rows alone. The verbs take an executor, so the publication runs
//! all eight writes in its one transaction, under its node lock. Guests and
//! state formats reference their runtime installation, so the installations
//! are written before them.
//!
//! The write verbs bind their rows as one jsonb array and read it with
//! `jsonb_to_recordset`, so the input types' field names are the column names
//! those statements read: a renamed field reads as `NULL` there.
//!
//! [`list_installations_with_guests`] reads back each node's installations
//! and their guests.

use pplx_space_accounts::NodeId;
use pplx_space_sql_locator::sql;
use sqlx::types::Json;
use sqlx::{FromRow, PgExecutor};
use time::OffsetDateTime;

#[cfg(test)]
mod tests;

/// One reported runtime installation and its capabilities.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct RuntimeInstallation {
    pub runtime: String,
    pub runtime_version: String,
    pub guests: Vec<RuntimeGuest>,
    pub state_formats: Vec<RuntimeStateFormat>,
}

/// A guest platform a runtime installation runs.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct RuntimeGuest {
    pub guest_os: String,
    pub guest_arch: String,
}

/// A state format a runtime installation can create or load.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct RuntimeStateFormat {
    pub state_format_name: String,
    pub state_format_version: String,
    pub can_create: bool,
    pub can_load: bool,
}

/// One reported artifact component installation. `kind` and the `*_key`
/// fields identify the row within its node, with `""` for an absent role,
/// version, digest or format; `version`, `digest` and `format` keep an absent
/// value as `None`.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct ArtifactComponent {
    pub kind: String,
    pub role_key: String,
    pub version_key: String,
    pub digest_key: String,
    pub format_key: String,
    pub version: Option<String>,
    pub digest: Option<String>,
    pub format: Option<String>,
    pub default_for_new: bool,
    pub legacy_default: bool,
}

/// One guest platform of one runtime installation a node reported; an
/// installation with no guest row has `guest_os` and `guest_arch` `None`.
#[derive(Clone, Debug, Eq, PartialEq, FromRow)]
pub struct InstallationGuestRow {
    pub node_id: NodeId,
    pub runtime: String,
    pub runtime_version: String,
    pub observed_at: OffsetDateTime,
    pub guest_os: Option<String>,
    pub guest_arch: Option<String>,
}

/// The runtime installations each of `node_ids` last reported, one row per
/// guest platform. Ordered by node, runtime and version, then guest, so a
/// node's rows for one runtime version are adjacent.
pub async fn list_installations_with_guests(
    executor: impl PgExecutor<'_>,
    node_ids: &[NodeId],
) -> Result<Vec<InstallationGuestRow>, sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT i.node_id,
                i.runtime,
                i.runtime_version,
                i.observed_at,
                g.guest_os,
                g.guest_arch
         FROM node_runtime_installations i
         LEFT JOIN node_runtime_guests g
           ON g.node_id = i.node_id
          AND g.runtime = i.runtime
          AND g.runtime_version = i.runtime_version
         WHERE i.node_id = ANY($1)
         ORDER BY i.node_id, i.runtime, i.runtime_version, g.guest_os, g.guest_arch"
    ))
    .bind(node_ids)
    .fetch_all(executor)
    .await
}

/// Delete `node_id`'s runtime installations that `installations` doesn't
/// name. Their guests and state formats go with them (`ON DELETE CASCADE`).
pub async fn prune_runtime_installations(
    executor: impl PgExecutor<'_>,
    node_id: NodeId,
    installations: &[RuntimeInstallation],
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM node_runtime_installations installed
         WHERE node_id = $1 AND NOT EXISTS (
             SELECT 1 FROM jsonb_to_recordset($2::jsonb)
                 AS input(runtime TEXT, runtime_version TEXT)
             WHERE (input.runtime, input.runtime_version) =
                   (installed.runtime, installed.runtime_version)
         )"
    ))
    .bind(node_id)
    .bind(Json(installations))
    .execute(executor)
    .await?;
    Ok(())
}

/// Delete `node_id`'s artifact components that `components` doesn't name.
pub async fn prune_artifact_components(
    executor: impl PgExecutor<'_>,
    node_id: NodeId,
    components: &[ArtifactComponent],
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM node_artifact_components installed
         WHERE node_id = $1 AND NOT EXISTS (
             SELECT 1 FROM jsonb_to_recordset($2::jsonb) AS input(
                 kind TEXT, role_key TEXT, version_key TEXT, digest_key TEXT, format_key TEXT
             )
             WHERE (input.kind, input.role_key, input.version_key, input.digest_key, input.format_key) =
                   (installed.kind, installed.role_key, installed.version_key,
                    installed.digest_key, installed.format_key)
         )"
    ))
    .bind(node_id)
    .bind(Json(components))
    .execute(executor)
    .await?;
    Ok(())
}

/// Insert `node_id`'s runtime installations that aren't stored yet; a stored
/// one keeps its row and `observed_at`.
pub async fn insert_runtime_installations(
    executor: impl PgExecutor<'_>,
    node_id: NodeId,
    installations: &[RuntimeInstallation],
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "WITH input AS (
             SELECT * FROM jsonb_to_recordset($2::jsonb) AS input(
                 runtime TEXT, runtime_version TEXT, guests JSONB, state_formats JSONB
             )
         )
         INSERT INTO node_runtime_installations (node_id, runtime, runtime_version, observed_at)
         SELECT DISTINCT $1, runtime, runtime_version, NOW() FROM input WHERE TRUE
         ON CONFLICT (node_id, runtime, runtime_version) DO NOTHING"
    ))
    .bind(node_id)
    .bind(Json(installations))
    .execute(executor)
    .await?;
    Ok(())
}

/// Delete `node_id`'s runtime guests that `installations` doesn't name,
/// including those of a runtime it still names.
pub async fn prune_runtime_guests(
    executor: impl PgExecutor<'_>,
    node_id: NodeId,
    installations: &[RuntimeInstallation],
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM node_runtime_guests installed
         WHERE node_id = $1 AND NOT EXISTS (
             SELECT 1 FROM jsonb_to_recordset($2::jsonb)
                 AS input(runtime TEXT, runtime_version TEXT, guests JSONB)
             CROSS JOIN LATERAL jsonb_to_recordset(input.guests)
                 AS guest(guest_os TEXT, guest_arch TEXT)
             WHERE (input.runtime, input.runtime_version, guest.guest_os, guest.guest_arch) =
                   (installed.runtime, installed.runtime_version,
                    installed.guest_os, installed.guest_arch)
         )"
    ))
    .bind(node_id)
    .bind(Json(installations))
    .execute(executor)
    .await?;
    Ok(())
}

/// Insert `node_id`'s runtime guests that aren't stored yet. Run it after
/// [`insert_runtime_installations`], whose rows they reference.
pub async fn insert_runtime_guests(
    executor: impl PgExecutor<'_>,
    node_id: NodeId,
    installations: &[RuntimeInstallation],
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "WITH input AS (
             SELECT * FROM jsonb_to_recordset($2::jsonb) AS input(
                 runtime TEXT, runtime_version TEXT, guests JSONB
             )
         )
         INSERT INTO node_runtime_guests (
             node_id, runtime, runtime_version, guest_os, guest_arch
         )
         SELECT DISTINCT $1, input.runtime, input.runtime_version,
                guest.guest_os, guest.guest_arch
         FROM input
         CROSS JOIN LATERAL jsonb_to_recordset(COALESCE(input.guests, '[]'::jsonb))
             AS guest(guest_os TEXT, guest_arch TEXT)
         WHERE TRUE
         ON CONFLICT (node_id, runtime, runtime_version, guest_os, guest_arch) DO NOTHING"
    ))
    .bind(node_id)
    .bind(Json(installations))
    .execute(executor)
    .await?;
    Ok(())
}

/// Delete `node_id`'s runtime state formats that `installations` doesn't
/// name, including those of a runtime it still names.
pub async fn prune_runtime_state_formats(
    executor: impl PgExecutor<'_>,
    node_id: NodeId,
    installations: &[RuntimeInstallation],
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM node_runtime_state_formats installed
         WHERE node_id = $1 AND NOT EXISTS (
             SELECT 1 FROM jsonb_to_recordset($2::jsonb)
                 AS input(runtime TEXT, runtime_version TEXT, state_formats JSONB)
             CROSS JOIN LATERAL jsonb_to_recordset(input.state_formats)
                 AS state_format(state_format_name TEXT, state_format_version TEXT)
             WHERE (input.runtime, input.runtime_version,
                    state_format.state_format_name, state_format.state_format_version) =
                   (installed.runtime, installed.runtime_version,
                    installed.state_format_name, installed.state_format_version)
         )"
    ))
    .bind(node_id)
    .bind(Json(installations))
    .execute(executor)
    .await?;
    Ok(())
}

/// Write `node_id`'s runtime state formats, a format reported more than once
/// taking each capability any report grants. A stored row is rewritten only
/// when its capabilities change. Run it after
/// [`insert_runtime_installations`], whose rows they reference.
pub async fn upsert_runtime_state_formats(
    executor: impl PgExecutor<'_>,
    node_id: NodeId,
    installations: &[RuntimeInstallation],
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "WITH input AS (
             SELECT * FROM jsonb_to_recordset($2::jsonb) AS input(
                 runtime TEXT, runtime_version TEXT, state_formats JSONB
             )
         )
         INSERT INTO node_runtime_state_formats (
             node_id, runtime, runtime_version, state_format_name,
             state_format_version, can_create, can_load
         )
         SELECT $1, input.runtime, input.runtime_version,
                state_format.state_format_name, state_format.state_format_version,
                bool_or(state_format.can_create), bool_or(state_format.can_load)
         FROM input
         CROSS JOIN LATERAL jsonb_to_recordset(
             COALESCE(input.state_formats, '[]'::jsonb)
         ) AS state_format(
             state_format_name TEXT, state_format_version TEXT,
             can_create BOOLEAN, can_load BOOLEAN
         )
         GROUP BY input.runtime, input.runtime_version,
                  state_format.state_format_name, state_format.state_format_version
         ON CONFLICT (
             node_id, runtime, runtime_version,
             state_format_name, state_format_version
         ) DO UPDATE SET
             can_create = EXCLUDED.can_create,
             can_load = EXCLUDED.can_load
         WHERE (node_runtime_state_formats.can_create, node_runtime_state_formats.can_load)
               IS DISTINCT FROM (EXCLUDED.can_create, EXCLUDED.can_load)"
    ))
    .bind(node_id)
    .bind(Json(installations))
    .execute(executor)
    .await?;
    Ok(())
}

/// Write `node_id`'s artifact components, a component reported more than once
/// taking each default flag any report sets. A stored row is rewritten, with
/// a new `observed_at`, only when its flags or values change.
pub async fn upsert_artifact_components(
    executor: impl PgExecutor<'_>,
    node_id: NodeId,
    components: &[ArtifactComponent],
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "WITH input AS (
             SELECT * FROM jsonb_to_recordset($2::jsonb) AS input(
                 kind TEXT, role_key TEXT, version_key TEXT, digest_key TEXT,
                 format_key TEXT, version TEXT, digest TEXT, format TEXT,
                 default_for_new BOOLEAN, legacy_default BOOLEAN
             )
         )
         INSERT INTO node_artifact_components (
             node_id, kind, role_key, version_key, digest_key, format_key,
             version, digest, format, default_for_new, legacy_default, observed_at
         )
         SELECT $1, kind, role_key, version_key, digest_key, format_key,
                version, digest, format, bool_or(default_for_new), bool_or(legacy_default), NOW()
         FROM input
         GROUP BY kind, role_key, version_key, digest_key, format_key, version, digest, format
         ON CONFLICT (node_id, kind, role_key, version_key, digest_key, format_key)
         DO UPDATE SET
             default_for_new = EXCLUDED.default_for_new,
             legacy_default = EXCLUDED.legacy_default,
             version = EXCLUDED.version,
             digest = EXCLUDED.digest,
             format = EXCLUDED.format,
             observed_at = EXCLUDED.observed_at
         WHERE (node_artifact_components.default_for_new, node_artifact_components.legacy_default,
                node_artifact_components.version, node_artifact_components.digest,
                node_artifact_components.format)
               IS DISTINCT FROM (EXCLUDED.default_for_new, EXCLUDED.legacy_default,
                                 EXCLUDED.version, EXCLUDED.digest, EXCLUDED.format)"
    ))
    .bind(node_id)
    .bind(Json(components))
    .execute(executor)
    .await?;
    Ok(())
}
