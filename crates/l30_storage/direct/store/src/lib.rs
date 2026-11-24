//! The Direct substrate's control-plane tables: `node_direct`, a Direct
//! node's registry row beside its `nodes` row, and each Direct sandbox's
//! current route identity (`sandbox_direct_route_identities`) and the
//! highest generations it has reserved or routed
//! (`sandbox_direct_route_provenance`).
//!
//! The Direct substrate (`controllers/substrate/direct.rs` in
//! `pplx-space-api`) runs the statements whose main table is a route-identity
//! or provenance table through these verbs; its reads that only join them
//! onto `sandboxes` stay in `direct.rs`. Each verb takes any executor, so it
//! composes into the caller's open transaction, and the caller keeps
//! everything around it: the transactions and their order, the `sandboxes`
//! row locks it takes first, the decoding of what a verb returns and the
//! mapping of its errors.
//!
//! A guarded write that matches no row returns `false`, whether another
//! writer won or the row never met the guard; the caller decides what that
//! means. No verb takes an explicit lock (`FOR UPDATE` or the like,
//! `LOCK TABLE`, an advisory lock). Their writes take PostgreSQL's ordinary
//! row locks, held until the caller's transaction ends: a write locks the
//! row it changes, a guarded upsert also locks the stored row its guard
//! refuses, and inserting a new row takes `FOR KEY SHARE` on its
//! `sandboxes` row through the foreign key. The caller orders these locks
//! across verbs.
//!
//! Not here yet: `node_direct` has no verbs. The admin node registry writes it
//! in the same statements as `nodes`, which `pplx-space-node-registration-store`
//! owns, and every read of it joins it onto a table another store owns.

pub mod route_identities;
pub mod route_provenance;

#[cfg(test)]
mod tests;
