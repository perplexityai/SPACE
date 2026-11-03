# `crates/l10_foundation`

Generic building blocks that name nothing SPACE-specific: value types,
runtime, transport and observability plumbing, and the storage helpers that
wrap a driver (`[rules.layers.storage] helpers` in `ARCHITECTURE.toml`). A
Foundation crate depends only on other Foundation crates.

- Belongs: `pplx-space-byte-size` (path `byte/size`), byte-size types;
  `pplx-space-durations` (path `durations`), duration value types and
  parsers.
- Does not belong: a SPACE noun such as a sandbox, template or node, or a
  rule about one (`l20_domain`); a crate that reads or writes SPACE state
  (`l30_storage`).

Ask: could another project use this crate unchanged?

A crate's path below this directory is its package name without
`pplx-space-`, split on `-`. Crates that predate the layer directories are
listed by layer in `[rules.layers.unmoved]` until they move; see "Layers" in
`crates/tools/archcheck/README.md`.
