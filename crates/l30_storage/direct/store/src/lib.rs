//! The Direct substrate's control-plane tables: `node_direct`, a Direct
//! node's registry row beside its `nodes` row, and each Direct sandbox's
//! current route identity (`sandbox_direct_route_identities`) and the
//! highest generations it has reserved or routed
//! (`sandbox_direct_route_provenance`).
//!
//! The Direct substrate (`controllers/substrate/direct.rs` in
//! `pplx-space-api`) runs the statements whose main table is a route-identity
//! or provenance table through these verbs; its reads that only join them
//! onto `sandboxes` are `pplx-space-sandbox-store`'s, in its `direct`
//! module. The admin node registry writes `node_direct` through [`nodes`],
//! in the transactions `pplx-space-direct-coordinator` runs with the node's
//! `nodes` row; every read of `node_direct` joins it onto a table another
//! store owns, so none is here. Each verb runs on the transaction, connection
//! or pool its caller passes, so it composes into the caller's open
//! transaction: most take any executor, and
//! [`route_identities::store_unless_newer`],
//! [`route_identities::accept_receipt`] and
//! [`route_provenance::retain_generations`] take a transaction or connection
//! and refuse a pool. The caller keeps everything around them: the
//! transactions and their order, the `sandboxes` or `nodes` row locks it
//! takes first, the decoding of what a verb returns and the mapping of its
//! errors.
//!
//! A guarded write that matches no row returns `false` or `None`, whether
//! another writer won or the row never met the guard; the caller decides what
//! that means. No verb takes an explicit lock (`FOR UPDATE` or the like,
//! `LOCK TABLE`, an advisory lock). Their writes take PostgreSQL's ordinary
//! row locks, held until the caller's transaction ends: a write locks the
//! row it changes, a guarded upsert also locks the stored row its guard
//! refuses, and inserting a new row takes `FOR KEY SHARE` on its
//! `sandboxes` or `nodes` row through the foreign key. The caller orders
//! these locks across verbs.

pub mod nodes;
pub mod route_identities;
pub mod route_provenance;

#[cfg(test)]
mod tests;
