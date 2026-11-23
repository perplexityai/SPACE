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
//! Every verb is a free function over an executor, so it runs in the
//! caller's transaction. Some statements on these tables still run at
//! their call sites, among them: the tag move's compare-and-set; reads
//! that lock rows (`FOR UPDATE`, `FOR NO KEY UPDATE`, `FOR SHARE`); writes
//! guarded by the template's desired state, owner or network policy
//! generation; statements compared against the database clock, such as
//! the grant checks, consumption and sweeps, and tombstone retention's
//! delete; and statements that also read or write another store's
//! tables.

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
mod tests;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
