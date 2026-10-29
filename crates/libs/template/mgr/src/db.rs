use std::collections::BTreeMap;
use std::path::Path;

use rusqlite::{Connection, Result, params, types::Type};

use pplx_space_common_core::sql;
use pplx_space_common_core::types::AutoLifecyclePolicy;
pub use pplx_space_common_core::types::PolicyAuthority;
use pplx_space_net_common::NetworkPolicyV1;

pub const DB_PATH: &str = pplx_space_common_core::host::linux::paths::TEMPLATE_MANAGER_DB;

pub fn open() -> Result<Connection, pplx_space_sqlite::SqliteError> {
    pplx_space_sqlite::open_rusqlite(
        Path::new(DB_PATH),
        pplx_space_sqlite::RusqliteOpenOptions::default(),
    )
}

fn immediate_transaction(conn: &Connection) -> Result<rusqlite::Transaction<'_>> {
    rusqlite::Transaction::new_unchecked(conn, rusqlite::TransactionBehavior::Immediate)
}

#[cfg(test)]
pub fn init_in_memory(conn: &mut Connection) {
    let service = sqlx::migrate!("../../../../db/template-manager");
    let common = sqlx::migrate!("../../../../db/common");
    for migration in common
        .iter()
        .chain(service.iter())
        .filter(|migration| migration.migration_type.is_up_migration())
    {
        conn.execute_batch(migration.sql.as_ref()).unwrap();
    }
}

/// In-memory SQLite connection with the template-manager schema applied, for tests.
#[cfg(test)]
pub(crate) fn test_conn() -> Connection {
    let mut conn = Connection::open_in_memory().unwrap();
    init_in_memory(&mut conn);
    conn
}

pub(crate) fn now() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs());
    format!("{now}")
}

pub(crate) fn mutex_poison_error(context: &'static str) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(format!(
        "{context}: sqlite mutex poisoned"
    ))))
}

fn nonnegative_i64_to_u64(value: i64, column: usize) -> Result<u64> {
    u64::try_from(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(column, Type::Integer, Box::new(error))
    })
}

fn saturating_u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn limit_to_i64(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}

fn network_policy_json(policy: &NetworkPolicyV1) -> Result<String> {
    serde_json::to_string(policy)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
}

fn default_network_policy_json() -> Result<String> {
    network_policy_json(&NetworkPolicyV1::deny_all())
}

fn parse_network_policy_json(value: Option<String>, column: usize) -> Result<NetworkPolicyV1> {
    match value {
        Some(json) => match serde_json::from_str(&json) {
            Ok(policy) => Ok(policy),
            Err(error) => Err(rusqlite::Error::FromSqlConversionFailure(
                column,
                Type::Text,
                Box::new(error),
            )),
        },
        None => Ok(NetworkPolicyV1::deny_all()),
    }
}

fn parse_policy_authority(value: &str, column: usize) -> Result<PolicyAuthority> {
    value.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(column, Type::Text, Box::new(error))
    })
}

// ── Template builds ────────────────────────────────────────────────────────

pub struct BuildRow {
    pub name: String,
    pub content_hash: String,
    pub build_date: String,
    pub manifest_json: String,
    pub runtime: String,
    pub arch: String,
    pub artifact_size_bytes: u64,
    pub state: String,
    pub owner_type: String,
    pub owner_id: String,
    pub execution_metadata_json: Option<String>,
}

pub struct BuildUpsert<'a> {
    pub name: &'a str,
    pub content_hash: &'a str,
    pub build_date: &'a str,
    pub manifest_json: &'a str,
    pub runtime: &'a str,
    pub arch: &'a str,
    pub artifact_size_bytes: u64,
    pub state: &'a str,
}

pub fn get_build(conn: &Connection, name: &str, content_hash: &str) -> Result<Option<BuildRow>> {
    let mut stmt = conn.prepare(
        "SELECT name, content_hash, build_date, manifest_json, runtime, arch, artifact_size_bytes, state, owner_type, owner_id, execution_metadata_json
         FROM template_builds WHERE name = ?1 AND content_hash = ?2",
    )?;
    let mut rows = stmt.query_map(params![name, content_hash], |row| {
        Ok(BuildRow {
            name: row.get(0)?,
            content_hash: row.get(1)?,
            build_date: row.get(2)?,
            manifest_json: row.get(3)?,
            runtime: row.get(4)?,
            arch: row.get(5)?,
            artifact_size_bytes: nonnegative_i64_to_u64(row.get::<_, i64>(6)?, 6)?,
            state: row.get(7)?,
            owner_type: row.get(8)?,
            owner_id: row.get(9)?,
            execution_metadata_json: row.get(10)?,
        })
    })?;
    rows.next().transpose()
}

/// Normalize an ISO 8601 build_date to UTC so lexicographic sort = chronological sort.
fn normalize_build_date(s: &str) -> String {
    if s.is_empty() {
        return s.to_string();
    }
    // Parse as jiff::Timestamp (handles offsets like +00:00, -07:00, Z)
    if let Ok(ts) = s.parse::<jiff::Timestamp>() {
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ").to_string();
    }
    // Try parsing as Zoned (has timezone info)
    if let Ok(zdt) = s.parse::<jiff::Zoned>() {
        return zdt.timestamp().strftime("%Y-%m-%dT%H:%M:%SZ").to_string();
    }
    // Can't parse — store as-is
    s.to_string()
}

pub fn upsert_build(conn: &Connection, b: &BuildUpsert<'_>) -> Result<()> {
    let manifest: pplx_space_common_core::types::TemplateManifest =
        serde_json::from_str(b.manifest_json)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let execution = manifest
        .execution_metadata()
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    if let Some(execution) = execution {
        let runtime_matches = pplx_space_common_core::types::RuntimeKind::parse(b.runtime)
            .is_ok_and(|runtime| runtime == execution.runtime().kind);
        let arch_matches = b
            .arch
            .parse::<pplx_space_common_core::types::Arch>()
            .is_ok_and(|arch| arch == execution.guest().architecture);
        if !runtime_matches || !arch_matches {
            return Err(rusqlite::Error::ToSqlConversionFailure(Box::new(
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "template build runtime/arch columns disagree with execution metadata",
                ),
            )));
        }
    }
    let execution_json = execution
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let runtime_version = execution.map(|value| value.runtime().version.as_str());
    let guest_os = execution.map(|value| value.guest().os.as_str());
    let state_format_name = execution
        .and_then(|value| value.state_format())
        .map(|value| value.format.as_str());
    let state_format_version = execution
        .and_then(|value| value.state_format())
        .map(|value| value.version.as_str());
    let ts = now();
    let build_date_utc = normalize_build_date(b.build_date);
    // Insert seeds `last_used_at` to `created_at` so a build that has
    // never been resolved sits at the correct LRU position. The
    // ON CONFLICT path intentionally leaves `last_used_at` alone: a
    // re-upsert refreshes manifest/size metadata for the same digest,
    // but it is *not* an access — only `touch_build` advances the
    // LRU clock.
    //
    // The `WHERE state != 'deleting'` guard on the ON CONFLICT path
    // is the back-door fix for the GC-vs-pull race: when eviction
    // Phase 1 has already flipped this row to `state='deleting'`, a
    // concurrent pull's final upsert must NOT clobber the marker
    // back to `'ready'`. Without this, the pull would silently
    // resurrect a doomed row, eviction's Phase 3 would then DELETE
    // it, and the on-disk artifacts would be in an indeterminate
    // half-deleted half-written state. With the guard, the upsert
    // is a no-op when the row is mid-eviction; the pull's caller
    // sees the build as still NotFound after re-resolution and
    // surfaces NotFound to its caller cleanly.
    //
    // The conflict target is omitted so the statement matches the table's
    // primary key whether or not it includes `cache_partition`: this binary
    // can keep serving a database a successor's migrator has already
    // migrated, as when a rollout runs `space-db-migrator` in an init
    // container beside the serving pod. The tables have no other unique
    // constraint, so the bare clause can only fire on the primary key.
    let tx = immediate_transaction(conn)?;
    let rows = tx.execute(
        "INSERT INTO template_builds (name, content_hash, build_date, manifest_json, runtime, arch, artifact_size_bytes, state, created_at, last_used_at, runtime_version, guest_os, state_format_name, state_format_version, execution_metadata_json)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9, ?10, ?11, ?12, ?13, ?14)
         ON CONFLICT DO UPDATE SET build_date = ?3, manifest_json = ?4, runtime = ?5, arch = ?6, artifact_size_bytes = ?7, state = ?8, runtime_version = ?10, guest_os = ?11, state_format_name = ?12, state_format_version = ?13, execution_metadata_json = ?14
         WHERE template_builds.state != 'deleting'",
        params![b.name, b.content_hash, build_date_utc, b.manifest_json, b.runtime, b.arch, saturating_u64_to_i64(b.artifact_size_bytes), b.state, ts, runtime_version, guest_os, state_format_name, state_format_version, execution_json],
    )?;
    if rows == 0 {
        tracing::warn!(
            template = b.name,
            content_hash = b.content_hash,
            "upsert_build no-op: row is in state='deleting'; concurrent eviction won the race"
        );
        tx.commit()?;
        return Ok(());
    }
    tx.execute(
        "DELETE FROM template_build_components WHERE name = ?1 AND content_hash = ?2",
        params![b.name, b.content_hash],
    )?;
    if let Some(execution) = execution {
        for component in execution.components() {
            tx.execute(
                "INSERT INTO template_build_components (name, content_hash, kind, role_key, version, digest, format)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    b.name,
                    b.content_hash,
                    component.kind.as_str(),
                    component.role.as_ref().map_or("", |role| role.as_str()),
                    component.version.as_ref().map(|value| value.as_str()),
                    component.digest.as_ref().map(ToString::to_string),
                    component.format.as_ref().map(|value| value.as_str()),
                ],
            )?;
        }
    }
    tx.commit()?;
    Ok(())
}

/// Whether the build for `(name, content_hash)` exists locally and
/// is currently in `state='deleting'` (i.e. the eviction loop has
/// flipped it via Phase 1 but Phase 3 hasn't deleted the row yet).
/// Callers use this as a pre-flight check before starting a fresh
/// pull — a pull during this window writes bytes the eviction is
/// about to delete.
pub fn is_build_marked_deleting(conn: &Connection, name: &str, content_hash: &str) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM template_builds
         WHERE name = ?1 AND content_hash = ?2 AND state = 'deleting'",
        params![name, content_hash],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

/// Advance the LRU clock on a build to "now". Called from every
/// successful resolution path (`GetTemplate`, `FindTemplateBuild`,
/// snapshot lookups) so the eviction planner sees real usage.
///
/// Silently no-ops on a missing row — a touch on a build that no
/// longer exists is not an error, and the eviction planner reads from
/// the same rows so a stale touch would have nothing to update
/// anyway.
pub fn touch_build(conn: &Connection, name: &str, content_hash: &str) -> Result<()> {
    let ts = now();
    conn.execute(
        "UPDATE template_builds SET last_used_at = ?3
         WHERE name = ?1 AND content_hash = ?2",
        params![name, content_hash, ts],
    )?;
    Ok(())
}

/// One row's worth of eviction-planner input. Times are stored in the
/// DB as Unix-epoch-seconds strings (via [`now`]); this struct hands
/// them to the planner as `u64` so the planner does not need to
/// know about the on-disk encoding.
pub struct BuildEvictionRow {
    pub name: String,
    pub content_hash: String,
    pub artifact_size_bytes: u64,
    pub created_at_secs: u64,
    pub last_used_at_secs: u64,
}

/// Return every `ready` build with the timestamps the eviction
/// planner needs. The planner is purely functional, so this is the
/// only DB read the GC loop performs against `template_builds`.
pub fn list_eviction_candidates(conn: &Connection) -> Result<Vec<BuildEvictionRow>> {
    let mut stmt = conn.prepare(
        "SELECT name, content_hash, artifact_size_bytes, created_at, last_used_at
         FROM template_builds WHERE state = 'ready'",
    )?;
    let rows = stmt.query_map([], |row| {
        let created_str: String = row.get(3)?;
        let last_used_str: String = row.get(4)?;
        Ok(BuildEvictionRow {
            name: row.get(0)?,
            content_hash: row.get(1)?,
            artifact_size_bytes: nonnegative_i64_to_u64(row.get::<_, i64>(2)?, 2)?,
            created_at_secs: created_str.parse::<u64>().unwrap_or(0),
            last_used_at_secs: last_used_str.parse::<u64>().unwrap_or(0),
        })
    })?;
    rows.collect()
}

/// Distinct `(name, content_hash)` pairs pinned by *any* prefetch
/// source — tag or release. UNION'd at the SQL layer so the GC
/// loop's pinned-set check is one query, not two.
pub fn list_pinned_build_keys(conn: &Connection) -> Result<Vec<(String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT name, content_hash FROM template_pinned_tags
         UNION
         SELECT name, content_hash FROM template_pinned_releases",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    rows.collect()
}

/// Test-only helper: forces `last_used_at` on an existing build to
/// the supplied unix-seconds string. Used by the LRU integration
/// tests to plant rows at a known time without sleeping.
#[cfg(test)]
pub(crate) fn set_last_used_at_for_test(
    conn: &Connection,
    name: &str,
    content_hash: &str,
    last_used_at_secs: u64,
) -> Result<()> {
    conn.execute(
        "UPDATE template_builds SET last_used_at = ?3
         WHERE name = ?1 AND content_hash = ?2",
        params![name, content_hash, last_used_at_secs.to_string()],
    )?;
    Ok(())
}

pub fn list_builds(conn: &Connection) -> Result<Vec<BuildRow>> {
    let mut stmt = conn.prepare(
        "SELECT name, content_hash, build_date, manifest_json, runtime, arch, artifact_size_bytes, state, owner_type, owner_id, execution_metadata_json
         FROM template_builds WHERE state = 'ready'",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(BuildRow {
            name: row.get(0)?,
            content_hash: row.get(1)?,
            build_date: row.get(2)?,
            manifest_json: row.get(3)?,
            runtime: row.get(4)?,
            arch: row.get(5)?,
            artifact_size_bytes: nonnegative_i64_to_u64(row.get::<_, i64>(6)?, 6)?,
            state: row.get(7)?,
            owner_type: row.get(8)?,
            owner_id: row.get(9)?,
            execution_metadata_json: row.get(10)?,
        })
    })?;
    rows.collect()
}

pub fn list_builds_by_name(conn: &Connection, name: &str) -> Result<Vec<BuildRow>> {
    let mut stmt = conn.prepare(
        "SELECT name, content_hash, build_date, manifest_json, runtime, arch, artifact_size_bytes, state, owner_type, owner_id, execution_metadata_json
         FROM template_builds WHERE name = ?1 AND state = 'ready'",
    )?;
    let rows = stmt.query_map(params![name], |row| {
        Ok(BuildRow {
            name: row.get(0)?,
            content_hash: row.get(1)?,
            build_date: row.get(2)?,
            manifest_json: row.get(3)?,
            runtime: row.get(4)?,
            arch: row.get(5)?,
            artifact_size_bytes: nonnegative_i64_to_u64(row.get::<_, i64>(6)?, 6)?,
            state: row.get(7)?,
            owner_type: row.get(8)?,
            owner_id: row.get(9)?,
            execution_metadata_json: row.get(10)?,
        })
    })?;
    rows.collect()
}

pub fn delete_build(conn: &Connection, name: &str, content_hash: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM template_build_components WHERE name = ?1 AND content_hash = ?2",
        params![name, content_hash],
    )?;
    conn.execute(
        "DELETE FROM template_snapshots WHERE name = ?1 AND content_hash = ?2",
        params![name, content_hash],
    )?;
    conn.execute(
        "DELETE FROM template_builds WHERE name = ?1 AND content_hash = ?2",
        params![name, content_hash],
    )?;
    Ok(())
}

pub fn delete_template_local_state(conn: &Connection, name: &str) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    delete_template_rows(&tx, name)?;
    tx.commit()?;
    Ok(())
}

/// [`delete_template_local_state`] once the template has no build row left,
/// so a build a concurrent pull just recorded keeps its settings. Returns
/// whether it deleted.
pub fn delete_template_local_state_without_builds(conn: &Connection, name: &str) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let has_builds = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM template_builds WHERE name = ?1)",
        params![name],
        |row| row.get::<_, i64>(0),
    )? != 0;
    if has_builds {
        return Ok(false);
    }
    delete_template_rows(&tx, name)?;
    tx.commit()?;
    Ok(true)
}

fn delete_template_rows(tx: &rusqlite::Transaction<'_>, name: &str) -> Result<()> {
    tx.execute(
        "DELETE FROM template_snapshots WHERE name = ?1",
        params![name],
    )?;
    tx.execute(
        "DELETE FROM template_build_components WHERE name = ?1",
        params![name],
    )?;
    tx.execute("DELETE FROM template_builds WHERE name = ?1", params![name])?;
    tx.execute("DELETE FROM pools WHERE template = ?1", params![name])?;
    tx.execute(
        "DELETE FROM template_settings WHERE template = ?1",
        params![name],
    )?;
    Ok(())
}

pub fn template_local_state_exists(conn: &Connection, name: &str) -> Result<bool> {
    let exists = conn.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM template_snapshots WHERE name = ?1
             UNION ALL
             SELECT 1 FROM template_builds WHERE name = ?1
             UNION ALL
             SELECT 1 FROM pools WHERE template = ?1
             UNION ALL
             SELECT 1 FROM template_settings WHERE template = ?1
         )",
        params![name],
        |row| row.get::<_, i64>(0),
    )?;
    Ok(exists != 0)
}

// ── Latest build (replaces tag-based resolution) ──────────────────────────

pub fn get_latest_build(conn: &Connection, name: &str) -> Result<Option<BuildRow>> {
    let mut stmt = conn.prepare(
        "SELECT name, content_hash, build_date, manifest_json, runtime, arch, artifact_size_bytes, state, owner_type, owner_id, execution_metadata_json
         FROM template_builds WHERE name = ?1 AND state = 'ready' ORDER BY build_date DESC, content_hash DESC LIMIT 1",
    )?;
    let mut rows = stmt.query_map(params![name], |row| {
        Ok(BuildRow {
            name: row.get(0)?,
            content_hash: row.get(1)?,
            build_date: row.get(2)?,
            manifest_json: row.get(3)?,
            runtime: row.get(4)?,
            arch: row.get(5)?,
            artifact_size_bytes: nonnegative_i64_to_u64(row.get::<_, i64>(6)?, 6)?,
            state: row.get(7)?,
            owner_type: row.get(8)?,
            owner_id: row.get(9)?,
            execution_metadata_json: row.get(10)?,
        })
    })?;
    rows.next().transpose()
}

// ── Snapshots ──────────────────────────────────────────────────────────────

pub struct SnapshotRow {
    pub name: String,
    pub content_hash: String,
    pub vcpu: u32,
    pub memory_mib: u32,
    pub snapshot_hash: String,
    pub built_at: String,
}

pub fn get_snapshot(
    conn: &Connection,
    name: &str,
    content_hash: &str,
    vcpu: u32,
    memory_mib: u32,
) -> Result<Option<SnapshotRow>> {
    let mut stmt = conn.prepare(
        "SELECT name, content_hash, vcpu, memory_mib, snapshot_hash, built_at
         FROM template_snapshots WHERE name = ?1 AND content_hash = ?2 AND vcpu = ?3 AND memory_mib = ?4",
    )?;
    let mut rows = stmt.query_map(params![name, content_hash, vcpu, memory_mib], |row| {
        Ok(SnapshotRow {
            name: row.get(0)?,
            content_hash: row.get(1)?,
            vcpu: row.get(2)?,
            memory_mib: row.get(3)?,
            snapshot_hash: row.get(4)?,
            built_at: row.get(5)?,
        })
    })?;
    match rows.next() {
        Some(Ok(row)) => Ok(Some(row)),
        _ => Ok(None),
    }
}

pub fn upsert_snapshot(
    conn: &Connection,
    name: &str,
    content_hash: &str,
    vcpu: u32,
    memory_mib: u32,
    snapshot_hash: &str,
) -> Result<()> {
    let ts = now();
    // Target-less for the same reason as `upsert_build`'s clause.
    conn.execute(
        "INSERT INTO template_snapshots (name, content_hash, vcpu, memory_mib, snapshot_hash, built_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT DO UPDATE SET snapshot_hash = ?5, built_at = ?6",
        params![name, content_hash, vcpu, memory_mib, snapshot_hash, ts],
    )?;
    Ok(())
}

pub fn list_snapshots(
    conn: &Connection,
    name: &str,
    content_hash: &str,
) -> Result<Vec<SnapshotRow>> {
    let mut stmt = conn.prepare(
        "SELECT name, content_hash, vcpu, memory_mib, snapshot_hash, built_at
         FROM template_snapshots WHERE name = ?1 AND content_hash = ?2 ORDER BY vcpu ASC, memory_mib ASC",
    )?;
    let rows = stmt.query_map(params![name, content_hash], |row| {
        Ok(SnapshotRow {
            name: row.get(0)?,
            content_hash: row.get(1)?,
            vcpu: row.get(2)?,
            memory_mib: row.get(3)?,
            snapshot_hash: row.get(4)?,
            built_at: row.get(5)?,
        })
    })?;
    rows.collect()
}

pub fn delete_snapshots_for_build(conn: &Connection, name: &str, content_hash: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM template_snapshots WHERE name = ?1 AND content_hash = ?2",
        params![name, content_hash],
    )?;
    Ok(())
}

pub fn delete_snapshot(
    conn: &Connection,
    name: &str,
    content_hash: &str,
    vcpu: u32,
    memory_mib: u32,
) -> Result<()> {
    conn.execute(
        "DELETE FROM template_snapshots WHERE name = ?1 AND content_hash = ?2 AND vcpu = ?3 AND memory_mib = ?4",
        params![name, content_hash, vcpu, memory_mib],
    )?;
    Ok(())
}

// ── Pools (unchanged from before) ──────────────────────────────────────────

pub fn insert_pool(
    conn: &Connection,
    template: &str,
    target_count: u32,
    boot_mode: &str,
    vcpu: u32,
    memory_mib: u32,
) -> Result<()> {
    let ts = now();
    conn.execute(
        "INSERT INTO pools (template, target_count, boot_mode, vcpu, memory_mib, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)
         ON CONFLICT(template) DO UPDATE SET target_count = ?2, boot_mode = ?3, vcpu = ?4, memory_mib = ?5, updated_at = ?6",
        params![template, target_count, boot_mode, vcpu, memory_mib, ts],
    )?;
    Ok(())
}

pub fn delete_pool(conn: &Connection, template: &str) -> Result<()> {
    conn.execute("DELETE FROM pools WHERE template = ?1", params![template])?;
    Ok(())
}

pub struct PoolRow {
    pub template: String,
    pub target_count: u32,
    pub boot_mode: String,
    pub vcpu: u32,
    pub memory_mib: u32,
}

impl PoolRow {
    pub fn from_snapshot(&self) -> bool {
        self.boot_mode == "snapshot"
    }
}

pub fn get_pool(conn: &Connection, template: &str) -> Result<Option<PoolRow>> {
    let mut stmt = conn.prepare(
        "SELECT template, target_count, boot_mode, vcpu, memory_mib FROM pools WHERE template = ?1",
    )?;
    let mut rows = stmt.query_map(params![template], |row| {
        Ok(PoolRow {
            template: row.get(0)?,
            target_count: row.get(1)?,
            boot_mode: row.get(2)?,
            vcpu: row.get(3)?,
            memory_mib: row.get(4)?,
        })
    })?;
    match rows.next() {
        Some(Ok(row)) => Ok(Some(row)),
        _ => Ok(None),
    }
}

pub fn get_all_pools(conn: &Connection) -> Result<Vec<PoolRow>> {
    let mut stmt =
        conn.prepare("SELECT template, target_count, boot_mode, vcpu, memory_mib FROM pools")?;
    let rows = stmt.query_map([], |row| {
        Ok(PoolRow {
            template: row.get(0)?,
            target_count: row.get(1)?,
            boot_mode: row.get(2)?,
            vcpu: row.get(3)?,
            memory_mib: row.get(4)?,
        })
    })?;
    rows.collect()
}

// ── Template settings ──────────────────────────────────────────────────────

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

pub fn upsert_template_settings(
    conn: &Connection,
    template: &str,
    default_auto_lifecycle: &AutoLifecyclePolicy,
) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    upsert_template_settings_tx(
        &tx,
        template,
        default_auto_lifecycle,
        PolicyAuthority::Cluster,
    )?;
    tx.commit()?;
    Ok(())
}

fn upsert_template_settings_tx(
    tx: &rusqlite::Transaction<'_>,
    template: &str,
    default_auto_lifecycle: &AutoLifecyclePolicy,
    authority: PolicyAuthority,
) -> Result<bool> {
    let ts = now();
    let json = serde_json::to_string(default_auto_lifecycle)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let default_network_json = default_network_policy_json()?;
    match tx.query_row(
        "SELECT default_auto_lifecycle_json, default_auto_lifecycle_authority
         FROM template_settings
         WHERE template = ?1",
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
            tx.execute(
                "UPDATE template_settings
                 SET default_auto_lifecycle_json = ?1,
                     default_auto_lifecycle_authority = ?2,
                     updated_at = ?3
                 WHERE template = ?4",
                params![json, authority.as_str(), ts, template],
            )?;
            Ok(true)
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            tx.execute(
                "INSERT INTO template_settings
                     (template, default_auto_lifecycle_json, default_auto_lifecycle_authority,
                      default_network_policy_json, default_network_policy_authority, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
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
        Err(error) => Err(error),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemplateSettingsUpdateOutcome {
    pub auto_lifecycle_applied: bool,
    pub network_policy: TemplateNetworkPolicyUpdate,
    pub queued_dirty_ref: bool,
    pub queued_sandbox_notification: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum TemplateNetworkPolicyUpsertError {
    #[error("template network policy generation conflicts with existing policy")]
    GenerationConflict,
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
}

pub fn try_upsert_template_network_policy(
    conn: &Connection,
    template: &str,
    default_network_policy: &NetworkPolicyV1,
    default_network_policy_generation: u64,
    authority: PolicyAuthority,
) -> std::result::Result<bool, TemplateNetworkPolicyUpsertError> {
    let tx = immediate_transaction(conn)?;
    let decision = try_update_template_network_policy_tx(
        &tx,
        template,
        default_network_policy,
        default_network_policy_generation,
        authority,
    )?;
    tx.commit()?;
    Ok(matches!(decision, TemplateNetworkPolicyUpdate::Apply))
}

fn try_update_template_network_policy_tx(
    tx: &rusqlite::Transaction<'_>,
    template: &str,
    default_network_policy: &NetworkPolicyV1,
    default_network_policy_generation: u64,
    authority: PolicyAuthority,
) -> std::result::Result<TemplateNetworkPolicyUpdate, TemplateNetworkPolicyUpsertError> {
    let ts = now();
    let default_auto_json = serde_json::to_string(&AutoLifecyclePolicy::disabled())
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let network_json = network_policy_json(default_network_policy)?;
    let generation = saturating_u64_to_i64(default_network_policy_generation);

    match tx.query_row(
        "SELECT default_network_policy_json, default_network_policy_generation, default_network_policy_authority
         FROM template_settings
         WHERE template = ?1",
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
                    tx.execute(
                        "UPDATE template_settings
                         SET default_network_policy_json = ?1,
                             default_network_policy_generation = 0,
                             default_network_policy_authority = ?2,
                             updated_at = ?3
                         WHERE template = ?4",
                        params![network_json, authority.as_str(), ts, template],
                    )?;
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
                tx.execute(
                    "UPDATE template_settings
                     SET default_network_policy_json = ?1,
                         default_network_policy_generation = ?2,
                         default_network_policy_authority = ?3,
                         updated_at = ?4
                     WHERE template = ?5",
                    params![network_json, generation, authority.as_str(), ts, template],
                )?;
                Ok(TemplateNetworkPolicyUpdate::Apply)
            }
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            tx.execute(
                "INSERT INTO template_settings
                     (template, default_auto_lifecycle_json, default_network_policy_json,
                      default_network_policy_generation, default_network_policy_authority,
                      default_auto_lifecycle_authority, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)",
                params![
                    template,
                    default_auto_json,
                    network_json,
                    generation,
                    authority.as_str(),
                    PolicyAuthority::Cluster.as_str(),
                    ts
                ],
            )?;
            Ok(TemplateNetworkPolicyUpdate::Apply)
        }
        Err(error) => Err(error.into()),
    }
}

pub struct TemplateSettingsUpdate<'a> {
    pub template: &'a str,
    pub default_auto_lifecycle: Option<&'a AutoLifecyclePolicy>,
    pub default_auto_lifecycle_authority: PolicyAuthority,
    pub default_network_policy: Option<&'a NetworkPolicyV1>,
    pub default_network_policy_generation: u64,
    pub default_network_policy_authority: PolicyAuthority,
    pub mark_dirty: bool,
}

pub fn update_template_settings_with_outbox(
    conn: &Connection,
    update: TemplateSettingsUpdate<'_>,
) -> std::result::Result<TemplateSettingsUpdateOutcome, TemplateNetworkPolicyUpsertError> {
    let tx = immediate_transaction(conn)?;
    let mut auto_lifecycle_applied = false;
    if let Some(policy) = update.default_auto_lifecycle {
        auto_lifecycle_applied = upsert_template_settings_tx(
            &tx,
            update.template,
            policy,
            update.default_auto_lifecycle_authority,
        )?;
    }
    let mut network_policy = TemplateNetworkPolicyUpdate::Unspecified;
    let mut queued_dirty_ref = false;
    let mut queued_sandbox_notification = false;
    if let Some(policy) = update.default_network_policy {
        match try_update_template_network_policy_tx(
            &tx,
            update.template,
            policy,
            update.default_network_policy_generation,
            update.default_network_policy_authority,
        ) {
            Ok(TemplateNetworkPolicyUpdate::Apply) => {
                network_policy = TemplateNetworkPolicyUpdate::Apply;
                queued_sandbox_notification =
                    enqueue_template_network_policy_notification_tx(&tx, update.template)?;
                if update.mark_dirty {
                    queued_dirty_ref =
                        enqueue_node_dirty_network_policy_template_tx(&tx, update.template)?;
                }
            }
            Ok(TemplateNetworkPolicyUpdate::Repair) => {
                network_policy = TemplateNetworkPolicyUpdate::Repair;
                queued_sandbox_notification =
                    enqueue_template_network_policy_notification_tx(&tx, update.template)?;
            }
            Ok(TemplateNetworkPolicyUpdate::Skip | TemplateNetworkPolicyUpdate::Unspecified) => {
                network_policy = TemplateNetworkPolicyUpdate::Skip;
            }
            Ok(TemplateNetworkPolicyUpdate::Conflict) => {
                network_policy = TemplateNetworkPolicyUpdate::Conflict;
            }
            Err(TemplateNetworkPolicyUpsertError::GenerationConflict) => {
                network_policy = TemplateNetworkPolicyUpdate::Conflict;
            }
            Err(error) => return Err(error),
        }
    }
    tx.commit()?;
    Ok(TemplateSettingsUpdateOutcome {
        auto_lifecycle_applied,
        network_policy,
        queued_dirty_ref,
        queued_sandbox_notification,
    })
}

const RECONCILE_FACET_NETWORK_POLICY: i32 = 1;
const RECONCILE_FACET_TEMPLATE_INVENTORY: i32 = 3;

fn enqueue_node_dirty_network_policy_template_tx(
    tx: &rusqlite::Transaction<'_>,
    template: &str,
) -> Result<bool> {
    let ts = now();
    let changed = tx.execute(
        "INSERT INTO node_dirty_refs
             (facet, element_kind, element_value, requested_at, updated_at)
         VALUES (?1, 'network_policy_template', ?2, ?3, ?3)
         ON CONFLICT(facet, element_kind, element_value) DO UPDATE SET
             requested_at = excluded.requested_at,
             updated_at = excluded.updated_at",
        params![RECONCILE_FACET_NETWORK_POLICY, template, ts],
    )?;
    Ok(changed != 0)
}

pub fn enqueue_node_dirty_template_inventory(conn: &Connection, template: &str) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let queued = enqueue_node_dirty_template_inventory_tx(&tx, template)?;
    tx.commit()?;
    Ok(queued)
}

fn enqueue_node_dirty_template_inventory_tx(
    tx: &rusqlite::Transaction<'_>,
    template: &str,
) -> Result<bool> {
    let ts = now();
    let changed = tx.execute(
        "INSERT INTO node_dirty_refs
             (facet, element_kind, element_value, requested_at, updated_at)
         VALUES (?1, 'template_inventory', ?2, ?3, ?3)
         ON CONFLICT(facet, element_kind, element_value) DO UPDATE SET
             requested_at = excluded.requested_at,
             updated_at = excluded.updated_at",
        params![RECONCILE_FACET_TEMPLATE_INVENTORY, template, ts],
    )?;
    Ok(changed != 0)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDirtyRefRow {
    pub facet: i32,
    pub element_kind: String,
    pub element_value: String,
}

pub fn list_node_dirty_refs(conn: &Connection, limit: usize) -> Result<Vec<NodeDirtyRefRow>> {
    let mut stmt = conn.prepare(
        "SELECT facet, element_kind, element_value
         FROM node_dirty_refs
         ORDER BY requested_at ASC
         LIMIT ?1",
    )?;
    let rows = stmt.query_map(params![limit_to_i64(limit)], |row| {
        Ok(NodeDirtyRefRow {
            facet: row.get(0)?,
            element_kind: row.get(1)?,
            element_value: row.get(2)?,
        })
    })?;
    rows.collect()
}

pub fn delete_node_dirty_refs(conn: &Connection, refs: &[NodeDirtyRefRow]) -> Result<usize> {
    let tx = immediate_transaction(conn)?;
    let mut deleted = 0;
    for reference in refs {
        deleted += tx.execute(
            "DELETE FROM node_dirty_refs
             WHERE facet = ?1
               AND element_kind = ?2
               AND element_value = ?3",
            params![
                reference.facet,
                reference.element_kind,
                reference.element_value
            ],
        )?;
    }
    tx.commit()?;
    Ok(deleted)
}

fn enqueue_template_network_policy_notification_tx(
    tx: &rusqlite::Transaction<'_>,
    template: &str,
) -> Result<bool> {
    let ts = now();
    // Local generations are allocated by the owning schema. Publish the actual
    // committed witness, never the caller's requested (usually zero) generation.
    let changed = tx.execute(
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
        params![template, ts],
    )?;
    Ok(changed != 0)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateNetworkPolicyNotificationRow {
    pub template: String,
    pub policy: NetworkPolicyV1,
    pub generation: u64,
    pub authority: PolicyAuthority,
    // Retain the persisted bytes for acknowledgement. Re-serializing after a
    // schema/version-normalizing decode is not the exact witness we observed.
    policy_json: String,
}

pub fn list_template_network_policy_notifications(
    conn: &Connection,
    limit: usize,
) -> Result<Vec<TemplateNetworkPolicyNotificationRow>> {
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
        Ok(TemplateNetworkPolicyNotificationRow {
            template: row.get(0)?,
            policy,
            generation,
            authority: parse_policy_authority(row.get::<_, String>(3)?.as_str(), 3)?,
            policy_json,
        })
    })?;
    rows.collect()
}

pub fn delete_template_network_policy_notifications(
    conn: &Connection,
    rows: &[TemplateNetworkPolicyNotificationRow],
) -> Result<usize> {
    let tx = immediate_transaction(conn)?;
    let mut deleted = 0;
    for row in rows {
        deleted += tx.execute(
            sql!(
                "DELETE FROM template_network_policy_notifications
             WHERE template = ?1
               AND generation = ?2
               AND authority = ?3
               AND policy_json = ?4"
            ),
            params![
                row.template,
                saturating_u64_to_i64(row.generation),
                row.authority.as_str(),
                row.policy_json
            ],
        )?;
    }
    tx.commit()?;
    Ok(deleted)
}

pub fn get_template_settings(
    conn: &Connection,
    template: &str,
) -> Result<Option<TemplateSettingsRow>> {
    let mut stmt = conn.prepare(
        "SELECT template, default_auto_lifecycle_json, default_network_policy_json,
                default_network_policy_generation, default_network_policy_authority,
                default_auto_lifecycle_authority, ensure_base_snapshot, snapshot_vcpu, snapshot_memory_mib,
                base_snapshot_policy_set_at, last_snapshot_reconcile_started_at,
                last_snapshot_reconcile_completed_at, last_snapshot_reconcile_error
         FROM template_settings WHERE template = ?1",
    )?;
    let mut rows = stmt.query_map(params![template], |row| {
        let json: String = row.get(1)?;
        let default_auto_lifecycle = serde_json::from_str(&json).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                1,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
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
    })?;
    match rows.next() {
        Some(Ok(row)) => Ok(Some(row)),
        Some(Err(error)) => Err(error),
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
    conn.execute(
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
             updated_at = ?7",
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
    let mut stmt = conn.prepare(
        "SELECT template, default_auto_lifecycle_json, default_network_policy_json,
                default_network_policy_generation, default_network_policy_authority,
                default_auto_lifecycle_authority, ensure_base_snapshot, snapshot_vcpu, snapshot_memory_mib,
                base_snapshot_policy_set_at, last_snapshot_reconcile_started_at,
                last_snapshot_reconcile_completed_at, last_snapshot_reconcile_error
         FROM template_settings
         WHERE base_snapshot_policy_set_at IS NOT NULL
         ORDER BY template",
    )?;
    let rows = stmt.query_map([], |row| {
        let json: String = row.get(1)?;
        let default_auto_lifecycle = serde_json::from_str(&json).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                1,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
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
    })?;
    rows.collect()
}

pub fn record_snapshot_reconcile_started(conn: &Connection, template: &str) -> Result<()> {
    let ts = now();
    conn.execute(
        "UPDATE template_settings
         SET last_snapshot_reconcile_started_at = ?1,
             last_snapshot_reconcile_error = NULL,
             updated_at = ?1
         WHERE template = ?2",
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
    conn.execute(
        "UPDATE template_settings
         SET last_snapshot_reconcile_completed_at = ?1,
             last_snapshot_reconcile_error = ?2,
             updated_at = ?1
         WHERE template = ?3",
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
    let mut stmt = conn.prepare(
        "SELECT template, default_auto_lifecycle_json, default_network_policy_json,
                default_network_policy_generation, default_network_policy_authority,
                default_auto_lifecycle_authority, ensure_base_snapshot, snapshot_vcpu, snapshot_memory_mib
         FROM template_settings ORDER BY template",
    )?;
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
    rows.collect()
}

pub fn ensure_template_settings(conn: &Connection, template: &str) -> Result<AutoLifecyclePolicy> {
    if let Some(settings) = get_template_settings(conn, template)? {
        return Ok(settings.default_auto_lifecycle);
    }

    let policy = AutoLifecyclePolicy::disabled();
    upsert_template_settings(conn, template, &policy)?;
    Ok(policy)
}

pub fn ensure_template_network_policy(
    conn: &Connection,
    template: &str,
) -> Result<NetworkPolicyV1> {
    if let Some(settings) = get_template_settings(conn, template)? {
        return Ok(settings.default_network_policy);
    }

    let policy = AutoLifecyclePolicy::disabled();
    upsert_template_settings(conn, template, &policy)?;
    Ok(NetworkPolicyV1::deny_all())
}

pub fn ensure_template_network_policy_with_seed(
    conn: &Connection,
    template: &str,
    seed_policy: Option<&NetworkPolicyV1>,
) -> Result<(NetworkPolicyV1, u64, bool)> {
    if let Some(settings) = get_template_settings(conn, template)? {
        return Ok((
            settings.default_network_policy,
            settings.default_network_policy_generation,
            false,
        ));
    }

    let policy = seed_policy
        .cloned()
        .unwrap_or_else(NetworkPolicyV1::deny_all);
    match try_upsert_template_network_policy(conn, template, &policy, 0, PolicyAuthority::Cluster) {
        Ok(_) => {}
        Err(TemplateNetworkPolicyUpsertError::Db(error)) => return Err(error),
        Err(TemplateNetworkPolicyUpsertError::GenerationConflict) => {
            unreachable!("template network policy generation conflict after missing settings row");
        }
    }
    Ok((policy, 0, true))
}

pub fn ensure_template_settings_for_builds(conn: &Connection) -> Result<()> {
    for build in list_builds(conn)? {
        drop(ensure_template_settings(conn, &build.name)?);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
