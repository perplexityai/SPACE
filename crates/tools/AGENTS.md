# `crates/tools`

Repository tools that are never shipped: checkers, benchmarks and harnesses
run in development or CI. Nothing depends on a tools crate; a tool may use
test_support crates as normal dependencies.

- Belongs: `pplx-space-archcheck` (path `archcheck`), `cargo arch`;
  `pplx-space-spaced-loopback-test` (path `spaced/loopback/test`), an in-VM
  smoke of the spaced guest loopback bridge.
- Does not belong: a test double that tests depend on (`test_support`); a
  binary a deployment installs (`l50_app`).

Ask: does it run only in development or CI, and does nothing need it?

A crate's path below this directory is its package name without
`pplx-space-`, split on `-`. Crates that predate the layer directories are
listed by layer in `[rules.layers.unmoved]` until they move; see "Layers" in
`crates/tools/archcheck/README.md`.
