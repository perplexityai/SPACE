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
//!   every cache partition. The environment-less tag pass pins the legacy
//!   partition and the environment pass each runnable environment's.
//! - `template_pinned_releases` — keyed on `(name, content_hash)`.
//!   The latest-N release pass writes one row per recent release, and the
//!   environment pass one per release bound to a runnable environment. A
//!   release and its twin share the digest, and so the row: a pin protects
//!   the digest in every partition, so the partition adds nothing to it.
//!
//! `pplx-space-template-mgr-store` owns both tables, and its [`pins`]
//! module holds their statements. [`PinnedDigestsStore`] mirrors
//! `WarmPoolStore` in `pplx-space-cluster-pool`: per-domain struct
//! holding a connection handle, methods that lock it and call the
//! store, no trait abstraction. Methods are synchronous
//! because the on-node SQLite database is wrapped in a
//! [`SharedConnection`] and must be locked under a
//! `tokio::task::spawn_blocking` boundary anyway — async wrappers here
//! would add a layer without removing the boundary.

use pplx_space_template_mgr_store::pins;

use crate::db::{ConnectionGuard, Result, SharedConnection, Transaction};

/// Pinned-digest store backing the template-manager prefetcher.
///
/// Cheap to clone — wraps the [`SharedConnection`] that
/// every template-manager subsystem already uses to serialize SQLite
/// access.
#[derive(Clone)]
pub struct PinnedDigestsStore {
    conn: SharedConnection,
}

impl PinnedDigestsStore {
    pub fn new(conn: SharedConnection) -> Self {
        Self { conn }
    }

    /// Borrow the underlying connection handle. Exposed for callers
    /// like the volume puller that mutate `template_builds` against
    /// the same connection — having one shared connection mutex
    /// avoids deadlocks from holding two locks against the same
    /// SQLite file.
    pub fn conn(&self) -> SharedConnection {
        self.conn.clone()
    }

    fn lock_conn(&self) -> Result<ConnectionGuard<'_>> {
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
        pins::upsert_tag_pin(&conn, name, tag, &cache_partition.key(), content_hash, &ts)?;
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
            pins::delete_all_tag_pins(&conn)?;
            return Ok(());
        }
        pins::delete_tag_pins_outside(&conn, keep)?;
        Ok(())
    }

    /// Delete `tag`'s pin in `cache_partition`, as when the tag's release has
    /// no build bound to that partition's environment any more: the pin
    /// would otherwise keep protecting the digest it pinned before.
    pub fn delete_tag(
        &self,
        name: &str,
        tag: &str,
        cache_partition: crate::db::TemplateCachePartition,
    ) -> Result<()> {
        let conn = self.lock_conn()?;
        pins::delete_tag_pin(&conn, name, tag, &cache_partition.key())?;
        Ok(())
    }

    /// Delete every tag-pin row of a cache partition outside `keep`, the
    /// partitions of the environments the node can still run, so a pin an
    /// environment's pass wrote stops protecting its digest once the node
    /// no longer runs it. The legacy partition is always kept: the
    /// environment-less tag pass writes it.
    pub fn prune_tag_partitions_outside(
        &self,
        keep: &[crate::db::TemplateCachePartition],
    ) -> Result<usize> {
        let conn = self.lock_conn()?;
        let keep: Vec<String> = keep.iter().map(|partition| partition.key()).collect();
        pins::delete_tag_pins_of_partitions_outside(&conn, &keep)
    }

    pub fn prune_without_local_builds(&self) -> Result<usize> {
        let conn = self.lock_conn()?;
        let tx = Transaction::immediate(&conn)?;
        let tags = pins::delete_tag_pins_without_local_builds(&tx)?;
        let releases = pins::delete_release_pins_without_local_builds(&tx)?;
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
        pins::upsert_release_pin(
            &conn,
            name,
            content_hash,
            release_id,
            release_created_at,
            &ts,
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
        let tx = Transaction::immediate(&conn)?;
        if keep.is_empty() {
            pins::delete_release_pins_of_templates(&tx, templates)?;
            tx.commit()?;
            return Ok(());
        }
        pins::delete_release_pins_of_templates_outside(&tx, templates, keep)?;
        tx.commit()?;
        Ok(())
    }

    /// Distinct content hashes pinned for `name` by any tag pin or
    /// any release pin. Future GC pass joins against this to decide
    /// what not to evict. The UNION dedupes the (common) case where
    /// both pin reasons resolve to the same digest.
    pub fn list_pinned_hashes_for_template(&self, name: &str) -> Result<Vec<String>> {
        let conn = self.lock_conn()?;
        pins::list_pinned_hashes_for_template(&conn, name)
    }

    /// Hashes currently pinned by `template_pinned_releases` for
    /// `name`, ordered by `release_created_at` DESC. Used by tests
    /// and for diagnostics; GC should call
    /// [`Self::list_pinned_hashes_for_template`].
    pub fn list_release_hashes(&self, name: &str) -> Result<Vec<String>> {
        let conn = self.lock_conn()?;
        pins::list_release_pin_hashes(&conn, name)
    }
}

#[cfg(test)]
mod tests;
