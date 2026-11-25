use std::path::Path;

use pplx_space_auto_lifecycle::AutoLifecyclePolicy;
use pplx_space_net_common::NetworkPolicyV1;
pub use pplx_space_policy_authority::PolicyAuthority;
use pplx_space_template_mgr_store::builds::{self, TemplateBuildRow};
use pplx_space_template_mgr_store::database;
use pplx_space_template_mgr_store::node_dirty_refs::{self, NodeDirtyRefRow};
use pplx_space_template_mgr_store::template_local_state;
pub(crate) use pplx_space_template_mgr_store::{
    Connection, ConnectionGuard, Error, Result, SharedConnection, Transaction,
};
use pplx_space_template_mgr_store::{network_policy_notifications, pins};

pub const DB_PATH: &str = pplx_space_host_layout::paths::TEMPLATE_MANAGER_DB;

pub fn open() -> Result<Connection, database::SqliteError> {
    database::open(Path::new(DB_PATH))
}

#[cfg(test)]
pub fn init_in_memory(conn: &mut Connection) {
    for migration in database::COMMON_MIGRATOR
        .iter()
        .chain(database::MIGRATOR.iter())
        .filter(|migration| migration.migration_type.is_up_migration())
    {
        conn.raw().execute_batch(migration.sql.as_ref()).unwrap();
    }
}

/// In-memory SQLite connection with the template-manager schema applied, for tests.
#[cfg(test)]
pub(crate) fn test_conn() -> Connection {
    let mut conn = Connection::open_in_memory();
    init_in_memory(&mut conn);
    conn
}

pub(crate) use pplx_space_template_mgr_store::now;

pub(crate) fn mutex_poison_error(context: &'static str) -> Error {
    Error::to_sql_conversion(std::io::Error::other(format!(
        "{context}: sqlite mutex poisoned"
    )))
}

fn saturating_u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

// ── Template builds ────────────────────────────────────────────────────────
//
// A build is keyed by `(name, content_hash, cache_partition)`. The content
// hash names rootfs bytes; the partition separates builds of one rootfs bound
// to different environments (see `TemplateCachePartition`). Readers that pass
// no partition get every partition of a hash and prefer the legacy one, which
// sorts first because its key is the empty string.

pub use pplx_space_template_model::TemplateCachePartition;

#[derive(Debug)]
pub struct BuildRow {
    pub name: String,
    pub content_hash: String,
    pub cache_partition: TemplateCachePartition,
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
    pub cache_partition: TemplateCachePartition,
    pub build_date: &'a str,
    pub manifest_json: &'a str,
    pub runtime: &'a str,
    pub arch: &'a str,
    pub artifact_size_bytes: u64,
    pub state: &'a str,
}

impl BuildRow {
    /// The stored execution metadata; `None` when absent or unreadable,
    /// which is logged.
    pub fn execution_metadata(&self) -> Option<pplx_space_execution_spec::ExecutionMetadata> {
        let raw = self.execution_metadata_json.as_deref()?;
        serde_json::from_str(raw)
            .inspect_err(|error| {
                tracing::warn!(
                    template = %self.name,
                    content_hash = %self.content_hash,
                    %error,
                    "stored template execution metadata is invalid"
                );
            })
            .ok()
    }

    /// The environment the build is bound to.
    pub fn environment(&self) -> Option<pplx_space_execution_spec::EnvironmentReference> {
        self.execution_metadata()?.environment()
    }

    /// The cache partition of the environment the build is bound to, by the
    /// rule sandbox-manager checks a build it boots against
    /// (`template_cache_partition` of the binding). It's the row's partition,
    /// except for a legacy row bound to another environment, as startup
    /// recovery adopts a legacy-layout build in place, and for a row whose
    /// stored execution metadata doesn't parse, which reads as unbound and so
    /// legacy. Sandbox-manager reads the binding from the build's own
    /// `metadata.json` and refuses a build of another partition.
    pub fn bound_partition(&self) -> TemplateCachePartition {
        pplx_space_node_runtime_core::catalog::template_cache_partition(self.environment())
    }
}

impl From<TemplateBuildRow> for BuildRow {
    fn from(row: TemplateBuildRow) -> Self {
        Self {
            name: row.name,
            content_hash: row.content_hash,
            cache_partition: row.cache_partition,
            build_date: row.build_date,
            manifest_json: row.manifest_json,
            runtime: row.runtime,
            arch: row.arch,
            artifact_size_bytes: row.artifact_size_bytes,
            state: row.state,
            owner_type: row.owner_type,
            owner_id: row.owner_id,
            execution_metadata_json: row.execution_metadata_json,
        }
    }
}

pub fn get_build(
    conn: &Connection,
    name: &str,
    content_hash: &str,
    cache_partition: TemplateCachePartition,
) -> Result<Option<BuildRow>> {
    Ok(builds::get_build(conn, name, content_hash, cache_partition)?.map(BuildRow::from))
}

/// The ready build of a digest, preferring `preferred`'s partition, then
/// the legacy partition, then any other.
pub fn find_ready_build(
    conn: &Connection,
    name: &str,
    content_hash: &str,
    preferred: Option<TemplateCachePartition>,
) -> Result<Option<BuildRow>> {
    Ok(builds::find_ready_build(conn, name, content_hash, preferred)?.map(BuildRow::from))
}

/// The ready build of a digest bound to `partition`'s environment
/// ([`BuildRow::bound_partition`]): the one in that partition, else an
/// adopted legacy build bound to it.
pub fn find_ready_build_bound_to(
    conn: &Connection,
    name: &str,
    content_hash: &str,
    partition: TemplateCachePartition,
) -> Result<Option<BuildRow>> {
    Ok(list_builds_for_hash(conn, name, content_hash)?
        .into_iter()
        .filter(|build| build.state == "ready" && build.bound_partition() == partition)
        .min_by_key(|build| build.cache_partition != partition))
}

/// Whether a row exists for the digest in any partition, in any state.
pub fn build_exists(conn: &Connection, name: &str, content_hash: &str) -> Result<bool> {
    builds::build_exists(conn, name, content_hash)
}

/// Builds left in `state='deleting'` by an interrupted eviction.
pub fn list_deleting_builds(
    conn: &Connection,
) -> Result<Vec<(String, String, TemplateCachePartition)>> {
    builds::list_deleting_builds(conn)
}

/// Every cached build of one rootfs digest, legacy partition first.
pub fn list_builds_for_hash(
    conn: &Connection,
    name: &str,
    content_hash: &str,
) -> Result<Vec<BuildRow>> {
    Ok(builds::list_builds_for_hash(conn, name, content_hash)?
        .into_iter()
        .map(BuildRow::from)
        .collect())
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

fn invalid_build_upsert(message: &'static str) -> Error {
    Error::to_sql_conversion(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message,
    ))
}

pub fn upsert_build(conn: &Connection, b: &BuildUpsert<'_>) -> Result<()> {
    let manifest: pplx_space_template_manifest::TemplateManifest =
        serde_json::from_str(b.manifest_json).map_err(Error::to_sql_conversion)?;
    let execution = manifest
        .execution_metadata()
        .map_err(Error::to_sql_conversion)?;
    if let Some(execution) = execution {
        let runtime_matches = pplx_space_execution_spec::RuntimeKind::parse(b.runtime)
            .is_ok_and(|runtime| runtime == execution.runtime().kind);
        let arch_matches = b
            .arch
            .parse::<pplx_space_cpu_arch::Arch>()
            .is_ok_and(|arch| arch == execution.guest().architecture);
        if !runtime_matches || !arch_matches {
            return Err(invalid_build_upsert(
                "template build runtime/arch columns disagree with execution metadata",
            ));
        }
    }
    // A build lives in the partition of the environment it's bound to. A
    // mismatch would let a lookup for one environment return a build bound to
    // another, so this function refuses it rather than trusting the caller. The
    // legacy partition is the exception: startup recovery adopts a
    // legacy-layout build bound to another environment in place, since a node
    // that ran that environment before partitions existed holds one there.
    // Registration, imports and pulls take the partition from the binding, so
    // only recovery files a build that way; a later refresh of that build,
    // such as compressing its rootfs for an export, keeps its legacy row.
    let bound_partition = pplx_space_node_runtime_core::catalog::template_cache_partition(
        execution.and_then(|execution| execution.environment()),
    );
    if bound_partition != b.cache_partition && b.cache_partition != TemplateCachePartition::Legacy {
        return Err(invalid_build_upsert(
            "template build cache partition disagrees with its environment binding",
        ));
    }
    let execution_json = execution
        .map(serde_json::to_string)
        .transpose()
        .map_err(Error::to_sql_conversion)?;
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
    let row = builds::BuildUpsertRow {
        name: b.name,
        content_hash: b.content_hash,
        cache_partition: b.cache_partition,
        build_date: &build_date_utc,
        manifest_json: b.manifest_json,
        runtime: b.runtime,
        arch: b.arch,
        artifact_size_bytes: saturating_u64_to_i64(b.artifact_size_bytes),
        state: b.state,
        created_at: &ts,
        runtime_version,
        guest_os,
        state_format_name,
        state_format_version,
        execution_metadata_json: execution_json.as_deref(),
    };
    let components: Vec<_> = execution
        .into_iter()
        .flat_map(|execution| execution.components())
        .map(|component| builds::BuildComponentRow {
            kind: component.kind.as_str(),
            role_key: component.role.as_ref().map_or("", |role| role.as_str()),
            version: component.version.as_ref().map(|value| value.as_str()),
            digest: component.digest.as_ref().map(ToString::to_string),
            format: component.format.as_ref().map(|value| value.as_str()),
        })
        .collect();
    let tx = Transaction::immediate(conn)?;
    if !builds::upsert_build(&tx, &row, &components)? {
        tracing::warn!(
            template = b.name,
            content_hash = b.content_hash,
            cache_partition = %b.cache_partition,
            "upsert_build no-op: row is in state='deleting'; concurrent eviction won the race"
        );
        tx.commit()?;
        return Ok(());
    }
    tx.commit()?;
    Ok(())
}

/// Whether a build of the digest, in any cache partition, exists locally
/// and is currently in `state='deleting'` (i.e. the eviction loop has
/// flipped it via Phase 1 but Phase 3 hasn't deleted the row yet). Callers
/// use this as a pre-flight check before starting a fresh pull — a pull
/// during this window writes bytes the eviction is about to delete. A pull
/// learns its partition only from the release manifest, so the check
/// covers every partition.
pub fn is_build_marked_deleting(conn: &Connection, name: &str, content_hash: &str) -> Result<bool> {
    builds::is_build_marked_deleting(conn, name, content_hash)
}

/// Advance the LRU clock on a build to "now". Called from every
/// successful resolution path (`GetTemplate`, `FindTemplateBuild`,
/// snapshot lookups) so the eviction planner sees real usage.
///
/// Silently no-ops on a missing row — a touch on a build that no
/// longer exists is not an error, and the eviction planner reads from
/// the same rows so a stale touch would have nothing to update
/// anyway.
pub fn touch_build(
    conn: &Connection,
    name: &str,
    content_hash: &str,
    cache_partition: TemplateCachePartition,
) -> Result<()> {
    let ts = now();
    builds::touch_build(conn, name, content_hash, cache_partition, &ts)
}

pub(crate) use pplx_space_template_mgr_store::builds::BuildEvictionRow;

/// Return every `ready` build with the timestamps the eviction
/// planner needs. The planner is purely functional, so this is the
/// only DB read the GC loop performs against `template_builds`.
pub fn list_eviction_candidates(conn: &Connection) -> Result<Vec<BuildEvictionRow>> {
    builds::list_eviction_candidates(conn)
}

/// Distinct `(name, content_hash)` pairs pinned by *any* prefetch
/// source — tag or release. UNION'd at the SQL layer so the GC
/// loop's pinned-set check is one query, not two. A pin names rootfs
/// bytes, so it protects every cache partition of the digest.
pub fn list_pinned_build_keys(conn: &Connection) -> Result<Vec<(String, String)>> {
    pins::list_pinned_build_keys(conn)
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
    conn.raw().execute(
        "UPDATE template_builds SET last_used_at = ?3
         WHERE name = ?1 AND content_hash = ?2",
        rusqlite::params![name, content_hash, last_used_at_secs.to_string()],
    )?;
    Ok(())
}

pub fn list_builds(conn: &Connection) -> Result<Vec<BuildRow>> {
    Ok(builds::list_builds(conn)?
        .into_iter()
        .map(BuildRow::from)
        .collect())
}

pub fn list_builds_by_name(conn: &Connection, name: &str) -> Result<Vec<BuildRow>> {
    Ok(builds::list_builds_by_name(conn, name)?
        .into_iter()
        .map(BuildRow::from)
        .collect())
}

pub fn delete_build(
    conn: &Connection,
    name: &str,
    content_hash: &str,
    cache_partition: TemplateCachePartition,
) -> Result<()> {
    builds::delete_build(conn, name, content_hash, cache_partition)
}

pub fn delete_template_local_state(conn: &Connection, name: &str) -> Result<()> {
    let tx = Transaction::immediate(conn)?;
    template_local_state::delete_template_rows(&tx, name)?;
    tx.commit()?;
    Ok(())
}

/// [`delete_template_local_state`] once the template has no build row left,
/// so a build a concurrent pull just recorded keeps its settings. Returns
/// whether it deleted.
pub fn delete_template_local_state_without_builds(conn: &Connection, name: &str) -> Result<bool> {
    let tx = Transaction::immediate(conn)?;
    if template_local_state::template_has_builds(&tx, name)? {
        return Ok(false);
    }
    template_local_state::delete_template_rows(&tx, name)?;
    tx.commit()?;
    Ok(true)
}

pub fn template_local_state_exists(conn: &Connection, name: &str) -> Result<bool> {
    template_local_state::template_local_state_exists(conn, name)
}

// ── Latest build (replaces tag-based resolution) ──────────────────────────

/// Newest ready build of `name` for a lookup that names no digest.
///
/// With `preferred`, the newest build bound to that partition wins, however
/// old: a build in the partition, or a legacy-layout build whose binding maps
/// to it (see [`get_latest_bound_build`]). Without one, or with none on the
/// node, it's the newest ready build in any partition, as before partitions
/// existed, which the caller checks as a compatible cold-boot source.
/// Same-date builds go to the larger content hash, as sandbox-manager's
/// catalog and node-ctl pick, and a release and its twin go to `preferred`'s
/// partition and then the legacy one.
pub fn get_latest_build(
    conn: &Connection,
    name: &str,
    preferred: Option<TemplateCachePartition>,
) -> Result<Option<BuildRow>> {
    if let Some(partition) = preferred
        && let Some(bound) = get_latest_bound_build(conn, name, partition)?
    {
        return Ok(Some(bound));
    }
    Ok(builds::get_latest_build(conn, name, preferred)?.map(BuildRow::from))
}

/// Newest ready build of `name` bound to `cache_partition`: one cached in
/// that partition, or one in the legacy partition whose binding maps to it,
/// which startup recovery adopts in place from the pre-partition layout. The
/// newest wins, a date tie goes to the larger digest, as in
/// [`get_latest_build`], and only a tie on both goes to the partition's own
/// row over an adopted one. A row whose
/// binding maps elsewhere is skipped, so for the legacy partition only unbound
/// and initial-environment builds count.
fn get_latest_bound_build(
    conn: &Connection,
    name: &str,
    cache_partition: TemplateCachePartition,
) -> Result<Option<BuildRow>> {
    builds::find_latest_build_in_partition_or_legacy(conn, name, cache_partition, |row| {
        let row = BuildRow::from(row);
        (row.bound_partition() == cache_partition).then_some(row)
    })
}

/// Newest ready build of `name` in exactly one partition that belongs there:
/// its binding maps to that partition, or the partition is the legacy one,
/// which also holds the legacy-layout builds bound to another environment
/// that startup recovery adopts in place.
pub fn get_latest_build_in_partition(
    conn: &Connection,
    name: &str,
    cache_partition: TemplateCachePartition,
) -> Result<Option<BuildRow>> {
    // A legacy row can be bound to any environment: the migration files every
    // pre-partition row there, and startup recovery adopts the ones bound to
    // another environment in place. An environment partition's row bound to
    // something else isn't its build, so an older one that is wins.
    builds::find_latest_build_in_partition(conn, name, cache_partition, |row| {
        let row = BuildRow::from(row);
        (cache_partition == TemplateCachePartition::Legacy
            || pplx_space_node_runtime_core::catalog::template_cache_partition(row.environment())
                == cache_partition)
            .then_some(row)
    })
}

/// Partitions that hold at least one ready build of `name`, legacy first.
pub fn list_build_partitions(conn: &Connection, name: &str) -> Result<Vec<TemplateCachePartition>> {
    builds::list_build_partitions(conn, name)
}

// ── Snapshots ──────────────────────────────────────────────────────────────

pub(crate) use pplx_space_template_mgr_store::snapshots::{
    SnapshotKey, SnapshotRow, delete_snapshot, delete_snapshots_for_build, get_snapshot,
    list_snapshots, upsert_snapshot,
};

// ── Pools ──────────────────────────────────────────────────────────────────

pub(crate) use pplx_space_template_mgr_store::pools::{
    PoolRow, delete_pool, get_all_pools, get_pool, insert_pool,
};

// ── Template settings ──────────────────────────────────────────────────────

pub(crate) use pplx_space_template_mgr_store::template_settings::{
    TemplateNetworkPolicyUpdate, TemplateNetworkPolicyUpsertError, TemplateSettingsRow,
    get_template_settings, list_template_settings, list_template_snapshot_policies,
    record_snapshot_reconcile_completed, record_snapshot_reconcile_started,
    update_template_snapshot_policy,
};
use pplx_space_template_mgr_store::template_settings::{
    try_update_template_network_policy_tx, upsert_template_settings_tx,
};

pub fn upsert_template_settings(
    conn: &Connection,
    template: &str,
    default_auto_lifecycle: &AutoLifecyclePolicy,
) -> Result<()> {
    let tx = Transaction::immediate(conn)?;
    upsert_template_settings_tx(
        &tx,
        template,
        default_auto_lifecycle,
        PolicyAuthority::Cluster,
    )?;
    tx.commit()?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemplateSettingsUpdateOutcome {
    pub auto_lifecycle_applied: bool,
    pub network_policy: TemplateNetworkPolicyUpdate,
    pub queued_dirty_ref: bool,
    pub queued_sandbox_notification: bool,
}

pub fn try_upsert_template_network_policy(
    conn: &Connection,
    template: &str,
    default_network_policy: &NetworkPolicyV1,
    default_network_policy_generation: u64,
    authority: PolicyAuthority,
) -> std::result::Result<bool, TemplateNetworkPolicyUpsertError> {
    let tx = Transaction::immediate(conn)?;
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
    let tx = Transaction::immediate(conn)?;
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

fn enqueue_node_dirty_network_policy_template_tx(
    tx: &Transaction<'_>,
    template: &str,
) -> Result<bool> {
    let ts = now();
    let changed = node_dirty_refs::enqueue_network_policy_template(tx, template, &ts)?;
    Ok(changed != 0)
}

pub fn enqueue_node_dirty_template_inventory(conn: &Connection, template: &str) -> Result<bool> {
    let tx = Transaction::immediate(conn)?;
    let queued = enqueue_node_dirty_template_inventory_tx(&tx, template)?;
    tx.commit()?;
    Ok(queued)
}

fn enqueue_node_dirty_template_inventory_tx(tx: &Transaction<'_>, template: &str) -> Result<bool> {
    let ts = now();
    let changed = node_dirty_refs::enqueue_template_inventory(tx, template, &ts)?;
    Ok(changed != 0)
}

/// Acknowledge only the obligations read before sending. A newer enqueue, even
/// for a previously deleted key, remains pending for the worker's next retry.
pub fn delete_node_dirty_refs(conn: &Connection, refs: &[NodeDirtyRefRow]) -> Result<usize> {
    let tx = Transaction::immediate(conn)?;
    let deleted = node_dirty_refs::delete_node_dirty_refs_at_revision(&tx, refs)?;
    tx.commit()?;
    Ok(deleted)
}

fn enqueue_template_network_policy_notification_tx(
    tx: &Transaction<'_>,
    template: &str,
) -> Result<bool> {
    let ts = now();
    let changed = network_policy_notifications::enqueue(tx, template, &ts)?;
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
    Ok(network_policy_notifications::list(conn, limit)?
        .into_iter()
        .map(|record| TemplateNetworkPolicyNotificationRow {
            template: record.template,
            policy: record.policy,
            generation: record.generation,
            authority: record.authority,
            policy_json: record.policy_json,
        })
        .collect())
}

pub fn delete_template_network_policy_notifications(
    conn: &Connection,
    rows: &[TemplateNetworkPolicyNotificationRow],
) -> Result<usize> {
    let tx = Transaction::immediate(conn)?;
    let mut deleted = 0;
    for row in rows {
        deleted += network_policy_notifications::acknowledge(
            &tx,
            &row.template,
            row.generation,
            row.authority,
            &row.policy_json,
        )?;
    }
    tx.commit()?;
    Ok(deleted)
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
mod partition_tests;
#[cfg(test)]
mod tests;
