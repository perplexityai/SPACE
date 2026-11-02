# `crates/test_support`

Test doubles and fixtures, reachable from layer crates only through
`[dev-dependencies]`. A test_support crate may depend on any layer but
`tools`.

- Belongs: `pplx-space-db-test-fixtures` (path `db/test/fixtures`), the
  PostgreSQL and Redis fixtures; `pplx-space-node-api-sim` (path
  `node/api/sim`), an in-process node API simulator.
- Does not belong: a fixture only one crate uses (keep it in that crate's
  tests); anything a production binary links.

Ask: would a deployment ever run it?

A crate's path below this directory is its package name without
`pplx-space-`, split on `-`. Crates that predate the layer directories are
listed by layer in `[rules.layers.unmoved]` until they move; see "Layers" in
`crates/tools/archcheck/README.md`.
