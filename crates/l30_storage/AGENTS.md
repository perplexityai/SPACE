# `crates/l30_storage`

The only crates that reach a database or object storage, in two kinds. The
`space-data-store-development` skill owns the store boundary and when to add
a coordinator.

**Stores** (`<area>/store`, `pplx-space-<area>-store`) own their tables or
objects and hold all of their SQL; every statement writes only the store's
own tables, except where a `[rules.layers.state.co_writers]` grant allows
another store's table. Every other crate here is a store, whatever its name.
Each is private to the crates that own its state
(`[rules.layers.store_users]` in `ARCHITECTURE.toml`). A store depends on
Domain and Foundation crates, and on another store only when that store
lists it as a user; never on a coordinator.

- Belongs: `pplx-space-sandbox-store` (path `sandbox/store`), the
  control-plane `sandboxes` rows; `pplx-space-sbx-mgr-store` (path
  `sbx/mgr/store`), sbx-mgr's node-local SQLite database.

Ask: does it read and write one owner's tables or objects, and nothing else?

**Coordinators** (`<area>/coordinator`, `pplx-space-<area>-coordinator`) run
one transaction across two or more stores. A coordinator owns no tables
(no `[package.metadata.space.state]`), holds no SQL and depends on no
driver, storage helper or codec facade (`[rules.layers.storage]` in
`ARCHITECTURE.toml`): only Domain crates, other Foundation crates and the
stores that list it as a user, `pplx-space-postgres-pool` among them. It runs each store's verb, in
the caller's open transaction when the split statement is one step of it and
otherwise in one it opens, for that store's own rows, keeps which rows change
together and in what lock order, and documents that lock order. It takes the caller's decision as input. The services that use
it are listed in `[rules.layers.coordinator_users]`, as store users are.

Ask: does it keep two or more stores' rows consistent in one transaction,
owning no tables and holding no SQL?

- Does not belong, in either kind: the decision about what to write and
  when (`l40_api`); a driver wrapper that names nothing SPACE-specific
  (`l10_foundation`).

A crate's path below this directory is its package name without
`pplx-space-`, split on `-`. Crates that predate the layer directories are
listed by layer in `[rules.layers.unmoved]` until they move; see "Layers" and
"Coordinators" in `crates/tools/archcheck/README.md`.
