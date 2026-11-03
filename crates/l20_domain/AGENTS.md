# `crates/l20_domain`

Models (SPACE types and the rules that keep them valid) and protocols (wire
schemas and the conversions between them and the models), without I/O. A
Domain crate depends on Domain and Foundation crates, never on a package in
`[rules.layers.domain_fences]`, and a model never depends on a protocol.

- Belongs: `pplx-space-cluster-id` (path `cluster/id`), a model;
  `pplx-space-sbx-mgr-protocol` (path `sbx/mgr/protocol`), a protocol. Name
  a protocol `*-protocol` or `*-proto`.
- Does not belong: anything that awaits a socket, file, clock-driven task or
  database (`l30_storage`, `l40_api`); a generic type with no SPACE meaning
  (`l10_foundation`).

Ask: does it say what a SPACE thing is, or how it is written on the wire,
without doing anything?

A crate's path below this directory is its package name without
`pplx-space-`, split on `-`. Crates that predate the layer directories are
listed by layer in `[rules.layers.unmoved]` until they move; see "Layers" in
`crates/tools/archcheck/README.md`.
