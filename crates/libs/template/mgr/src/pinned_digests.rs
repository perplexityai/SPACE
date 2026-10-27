//! SQLite store for digest pins used by the template-manager
//! prefetcher and (future) garbage collector.
//!
//! Two pin tables back the same conceptual relation "this digest is
//! still wanted on this node":
//!
//! - `template_pinned_tags` — keyed on `(name, tag, cache_partition)`,
//!   so one tag can pin a different build per environment. The prefetch
//!   tag pass writes the current resolution; the GC unions it with the
//!   release pins to decide what not to evict. Pins protect a digest in
//!   every cache partition. Cluster tag resolution names no environment
//!   yet, so the prefetcher pins the legacy partition.
//! - `template_pinned_releases` — keyed on `(name, content_hash)`.
//!   The latest-N release pass writes one row per recent release.
//!
//! Both tables are owned by [`PinnedDigestsStore`]. The store mirrors
//! [`WarmPoolStore`] in `pplx-space-cluster-pool`: per-domain struct
//! holding a connection handle, methods that delegate to one query
//! each, no trait abstraction. Methods are synchronous because the
//! on-node SQLite database is wrapped in `Arc<Mutex<Connection>>` and
//! must be locked under a `tokio::task::spawn_blocking` boundary
//! anyway — async wrappers here would add a layer without removing
//! the boundary.
//!
//! [`WarmPoolStore`]: pplx_space_cluster_pool::WarmPoolStore

use std::sync::{Arc, Mutex, MutexGuard};

use rusqlite::{Connection, Result, params};

/// Pinned-digest store backing the template-manager prefetcher.
///
/// Cheap to clone — wraps the shared `Arc<Mutex<Connection>>` that
/// every template-manager subsystem already uses to serialize SQLite
/// access.
#[derive(Clone)]
pub struct PinnedDigestsStore {
    conn: Arc<Mutex<Connection>>,
}

impl PinnedDigestsStore {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Borrow the underlying connection handle. Exposed for callers
    /// like the volume puller that mutate `template_builds` against
    /// the same connection — having one shared `Arc<Mutex<…>>`
    /// avoids deadlocks from holding two locks against the same
    /// SQLite file.
    pub fn conn(&self) -> Arc<Mutex<Connection>> {
        Arc::clone(&self.conn)
    }

    fn lock_conn(&self) -> Result<MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|_| crate::db::mutex_poison_error("pinned digests store"))
    }

    /// Record (or refresh) a tag pin: "for template `name`, tag
    /// `tag` currently resolves to `content_hash` in
    /// `cache_partition`". The previous hash for the same
    /// `(name, tag, cache_partition)` loses its pin — that's correct,
    /// because the tag no longer protects it there.
    pub fn upsert_tag(
        &self,
        name: &str,
        tag: &str,
        cache_partition: crate::db::TemplateCachePartition,
        content_hash: &str,
    ) -> Result<()> {
        let conn = self.lock_conn()?;
        let ts = crate::db::now();
        conn.execute(
            "INSERT INTO template_pinned_tags (name, tag, cache_partition, content_hash, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(name, tag, cache_partition) DO UPDATE SET content_hash = ?4, updated_at = ?5",
            params![name, tag, cache_partition.key(), content_hash, ts],
        )?;
        Ok(())
    }

    /// Delete every tag-pin row whose `(name, tag)` is not in
    /// `keep`. Called at the end of each prefetch tick so the
    /// configured prefetch tag set is the source of
    /// truth — removing a tuple drops its pin on the next tick.
    pub fn prune_tags_outside(&self, keep: &[(String, String)]) -> Result<()> {
        let conn = self.lock_conn()?;
        if keep.is_empty() {
            // Empty keep set → no tracked tuples; drop everything.
            conn.execute("DELETE FROM template_pinned_tags", [])?;
            return Ok(());
        }
        // SQLite's parameter binding doesn't expand a `Vec` into a
        // tuple list, so build the predicate by enumerating
        // placeholders. The const is small, so the dynamic SQL is
        // fine.
        let mut sql =
            String::from("DELETE FROM template_pinned_tags WHERE (name, tag) NOT IN (VALUES ");
        for i in 0..keep.len() {
            if i > 0 {
                sql.push_str(", ");
            }
            let a = i * 2 + 1;
            let b = a + 1;
            sql.push_str(&format!("(?{a}, ?{b})"));
        }
        sql.push(')');
        let mut stmt = conn.prepare(&sql)?;
        let mut params_vec: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(keep.len() * 2);
        for (n, t) in keep {
            params_vec.push(n);
            params_vec.push(t);
        }
        stmt.execute(rusqlite::params_from_iter(params_vec.iter().copied()))?;
        Ok(())
    }

    pub fn prune_without_local_builds(&self) -> Result<usize> {
        let conn = self.lock_conn()?;
        let tx =
            rusqlite::Transaction::new_unchecked(&conn, rusqlite::TransactionBehavior::Immediate)?;
        let tags = tx.execute(
            "DELETE FROM template_pinned_tags
              WHERE NOT EXISTS (
                    SELECT 1
                      FROM template_builds b
                     WHERE b.name = template_pinned_tags.name
                       AND b.content_hash = template_pinned_tags.content_hash
                )",
            [],
        )?;
        let releases = tx.execute(
            "DELETE FROM template_pinned_releases
              WHERE NOT EXISTS (
                    SELECT 1
                      FROM template_builds b
                     WHERE b.name = template_pinned_releases.name
                       AND b.content_hash = template_pinned_releases.content_hash
                )",
            [],
        )?;
        tx.commit()?;
        Ok(tags.saturating_add(releases))
    }

    /// Record (or refresh) a release pin: "for template `name`, the
    /// digest `content_hash` is currently one of the latest-N
    /// releases tracked by the prefetcher". `release_id` and
    /// `release_created_at` are stored verbatim for diagnostics
    /// only; the pin key is `(name, content_hash)`.
    pub fn upsert_release(
        &self,
        name: &str,
        content_hash: &str,
        release_id: &str,
        release_created_at: &str,
    ) -> Result<()> {
        let conn = self.lock_conn()?;
        let ts = crate::db::now();
        conn.execute(
            "INSERT INTO template_pinned_releases (name, content_hash, release_id, release_created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(name, content_hash) DO UPDATE SET
                 release_id = ?3,
                 release_created_at = ?4,
                 updated_at = ?5",
            params![name, content_hash, release_id, release_created_at, ts],
        )?;
        Ok(())
    }

    /// Delete every release-pin row for templates in `templates`
    /// whose `(name, content_hash)` is not in `keep`. Templates not
    /// represented in `templates` are left untouched — this lets the
    /// prefetcher narrow the prune to the templates it actually
    /// resolved this tick, so a transient cluster-api failure for one
    /// template can't accidentally flush all its pins.
    pub fn prune_releases_outside(
        &self,
        templates: &[String],
        keep: &[(String, String)],
    ) -> Result<()> {
        if templates.is_empty() {
            return Ok(());
        }
        let conn = self.lock_conn()?;
        let tx =
            rusqlite::Transaction::new_unchecked(&conn, rusqlite::TransactionBehavior::Immediate)?;
        if keep.is_empty() {
            let mut sql = String::from("DELETE FROM template_pinned_releases WHERE name IN (");
            for i in 0..templates.len() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(&format!("?{}", i + 1));
            }
            sql.push(')');
            let mut stmt = tx.prepare(&sql)?;
            let params_vec: Vec<&dyn rusqlite::ToSql> = templates
                .iter()
                .map(|t| t as &dyn rusqlite::ToSql)
                .collect();
            stmt.execute(rusqlite::params_from_iter(params_vec.iter().copied()))?;
            drop(stmt);
            tx.commit()?;
            return Ok(());
        }
        let mut sql = String::from("DELETE FROM template_pinned_releases WHERE name IN (");
        for i in 0..templates.len() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!("?{}", i + 1));
        }
        sql.push_str(") AND (name, content_hash) NOT IN (VALUES ");
        let template_count = templates.len();
        for i in 0..keep.len() {
            if i > 0 {
                sql.push_str(", ");
            }
            let a = template_count + i * 2 + 1;
            let b = a + 1;
            sql.push_str(&format!("(?{a}, ?{b})"));
        }
        sql.push(')');
        let mut stmt = tx.prepare(&sql)?;
        let mut params_vec: Vec<&dyn rusqlite::ToSql> =
            Vec::with_capacity(template_count + keep.len() * 2);
        for t in templates {
            params_vec.push(t);
        }
        for (n, h) in keep {
            params_vec.push(n);
            params_vec.push(h);
        }
        stmt.execute(rusqlite::params_from_iter(params_vec.iter().copied()))?;
        drop(stmt);
        tx.commit()?;
        Ok(())
    }

    /// Distinct content hashes pinned for `name` by any tag pin or
    /// any release pin. Future GC pass joins against this to decide
    /// what not to evict. The UNION dedupes the (common) case where
    /// both pin reasons resolve to the same digest.
    pub fn list_pinned_hashes_for_template(&self, name: &str) -> Result<Vec<String>> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare(
            "SELECT content_hash FROM template_pinned_tags WHERE name = ?1
             UNION
             SELECT content_hash FROM template_pinned_releases WHERE name = ?1",
        )?;
        let rows = stmt.query_map(params![name], |row| row.get::<_, String>(0))?;
        rows.collect()
    }

    /// Hashes currently pinned by `template_pinned_releases` for
    /// `name`, ordered by `release_created_at` DESC. Used by tests
    /// and for diagnostics; GC should call
    /// [`Self::list_pinned_hashes_for_template`].
    pub fn list_release_hashes(&self, name: &str) -> Result<Vec<String>> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare(
            "SELECT content_hash FROM template_pinned_releases
             WHERE name = ?1
             ORDER BY release_created_at DESC, content_hash ASC",
        )?;
        let rows = stmt.query_map(params![name], |row| row.get::<_, String>(0))?;
        rows.collect()
    }
}

#[cfg(test)]
mod tests;
