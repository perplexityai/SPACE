# `crates/l40_api`

Use cases and their entry points (handlers, loops, sweepers, jobs), and each
service's `*-client`. An API crate depends on API, Domain and Foundation
crates and on the stores that list it as a user; a client depends only on
Domain, Foundation and other clients.

- Belongs: `pplx-space-sbx-mgr` (path `sbx/mgr/server`), the sandbox manager
  service; `pplx-space-sbx-mgr-client` (path `sbx/mgr/client`), its client. A
  service takes the `server` leaf when its client would nest inside it.
- Does not belong: `main`, flag parsing beyond the service's config
  (`l50_app`); SQL or object-storage calls (`l30_storage`); wire schemas
  shared by a server and its client (`l20_domain`).

Ask: does it decide what happens when a request, tick or job arrives?

A crate's path below this directory is its package name without
`pplx-space-`, split on `-`. Crates that predate the layer directories are
listed by layer in `[rules.layers.unmoved]` until they move; see "Layers" in
`crates/tools/archcheck/README.md`.
