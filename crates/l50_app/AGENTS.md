# `crates/l50_app`

Thin mains that compose API crates into a binary: a service's `*-svc` or a
`*-cli`. An app depends on API, Domain and Foundation crates, never on
another app or a store. "Service binaries" in `docs/rust/space-conventions.md`
says what a thin main holds.

- Belongs: `pplx-space-api-svc` (path `api/svc`, binary `space-api-svc`);
  `pplx-space-admin-cli` (path `admin/cli`).
- Does not belong: the service itself, its configuration resolution or its
  tests (`l40_api`); code two apps share (an API crate).

Ask: is this only the seam between a process and a library?

A crate's path below this directory is its package name without
`pplx-space-`, split on `-`. Crates that predate the layer directories are
listed by layer in `[rules.layers.unmoved]` until they move; see "Layers" in
`crates/tools/archcheck/README.md`.
