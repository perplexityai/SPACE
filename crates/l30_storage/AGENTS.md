# `crates/l30_storage`

Stores: the only crates that reach a database or object storage, each
private to the crates that own its state (`[rules.layers.store_users]` in
`ARCHITECTURE.toml`). A store depends on Domain and Foundation crates, and on
another store only when that store lists it as a user. The
`space-data-store-development` skill owns the store boundary.

- Belongs: `pplx-space-sandbox-store` (path `sandbox/store`), the
  control-plane `sandboxes` rows; `pplx-space-sbx-mgr-store` (path
  `sbx/mgr/store`), sbx-mgr's node-local SQLite database.
- Does not belong: the decision about what to write and when (`l40_api`); a
  driver wrapper that names nothing SPACE-specific (`l10_foundation`).

Ask: does it read and write one owner's tables or objects, and nothing else?

A crate's path below this directory is its package name without
`pplx-space-`, split on `-`. Crates that predate the layer directories are
listed by layer in `[rules.layers.unmoved]` until they move; see "Layers" in
`crates/tools/archcheck/README.md`.
