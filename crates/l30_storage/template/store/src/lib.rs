//! Data access for the control-plane template catalog in Postgres: the
//! `templates` rows and the per-template records keyed by their name.
//!
//! * [`templates`]: the catalog row (`templates`), its ownership, visibility,
//!   network policy and desired state.
//! * [`release_tags`]: the tags that name a release of a template
//!   (`template_release_tags`).
//! * [`build_jobs`]: the builds a builder accepted and their requested specs
//!   (`template_build_jobs`).
//! * [`build_base_grants`]: one build submission's authority to read its base
//!   template (`template_build_base_grants`).
//! * [`build_blobs`]: the uploaded file blobs an ECW build reads
//!   (`template_build_blobs`).
//! * [`publication_grants`]: one build submission's authority to publish
//!   (`template_system_publication_grants`,
//!   `template_tenant_publication_grants`).
//! * [`pool_policies`], [`default_auto_lifecycle_policies`] and
//!   [`base_snapshot_policies`]: the per-template policies of the same names.
//!
//! The releases themselves and their storage locations are
//! `pplx-space-template-release-store`'s.
//!
//! Every verb is a free function, so it runs in the caller's transaction.
//! Most take any executor. Some reads join other stores' tables, such as
//! the admin template list's node inventory, the create's builder-reported
//! auto-lifecycle default, and the node-facing policy reads' sandboxes,
//! pool configs and releases. Only [`release_tags`]' verbs write another
//! store's table: they set `template_releases.first_mutable_tag_at` on the
//! release a tag lands on, in the tag's statement, under a co-writer grant
//! in `ARCHITECTURE.toml`.
//!
//! * **Locks.** A verb whose row lock the caller relies on after the
//!   statement (`FOR UPDATE`, `FOR NO KEY UPDATE`, `FOR SHARE`, as the
//!   visibility flip, the node-group allowlist check, the template and
//!   release deletes, the tag move, release registration and the
//!   pool-config writes take them) is named for the lock and takes the
//!   caller's transaction; the lock lasts until that transaction ends. Lock
//!   timeouts stay with the caller.
//! * **Database clock.** Statements that compare against `NOW()`, such as
//!   the grant checks, consumption and sweeps, the node-facing policy
//!   reads' build-base grants, and tombstone retention's delete, keep the
//!   comparison in their SQL and run in the executor the caller passes.
//! * **Guarded writes.** Writes that re-check a precondition in their
//!   `WHERE` (the template's desired state, owner or network policy
//!   generation for the admin create and undelete, the update route's
//!   writes, the delete request and the policy upserts; the grant's target,
//!   owner, freshness and builder for grant consumption) bind the guard's
//!   values and return the outcome; matching nothing is an outcome, not an
//!   error. The caller keeps its transaction and decides what each outcome means.
//!
//! Other statements that also write another store's tables stay at their
//! call sites. Reads of these tables that interpolate a
//! `pplx_space_template_release_original` fragment aren't this store's, as
//! it is not one of that crate's users; among them, the admin release
//! delete's check of the tags a release serves is
//! `pplx-space-template-release-store`'s `lookup::release_serves_a_tag`.

pub mod base_snapshot_policies;
pub mod build_base_grants;
pub mod build_blobs;
pub mod build_jobs;
pub mod default_auto_lifecycle_policies;
pub mod pool_policies;
pub mod publication_grants;
pub mod release_tags;
pub mod templates;

#[cfg(test)]
mod release_tags_tests;
#[cfg(test)]
mod tests;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
