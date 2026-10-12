# `pplx-space-cluster-controller-server`

Every table named here is in the control-plane Postgres store.

## Changes

- Spawn a new loop from `run_reconcilers` (`src/reconcilers.rs`) on a child
  of its token, which the leader-election lease gates when election is
  enabled, and return when it is cancelled.
- The API, internal-api and several loops write the same `public.sandboxes`
  and `nodes` rows. A new write compares the state it decided on in its
  `WHERE` clause, or re-checks it under `FOR UPDATE`. The
  `space-data-store-development` skill owns the store rules.
- New `nodes.drain_phase` writes here compare-and-set the phase. Node-drain
  terminators delete a Kubernetes Node or its instance only after re-reading
  the phase and the stored `node_k8s.k8s_node_uid`; a mismatch refuses.
- A new store that joins a credential-fenced `*_encrypted_fields` sidecar
  runs on the credentials pool, as the two snapshot sweepers do.
- In dry-run, a loop classifies and reports through logs and dry-run-tagged
  metrics only: no writes and no `space.sandbox` events. Existing exception:
  `suspended_execution_reaper` emits its retire event tagged `dry_run`.
- Read a knob a deployment must change with a production `EnvConfig` reader
  (`src/env_config.rs`); its `test_*` readers return the default unless
  `test-hooks` is compiled in.
- `stuck_container_creating_gc` reads the `orphaned_sandboxes` and
  transient-lifecycle sweeper configs and runs only when both would retire
  the rows of the pods it deletes; changing those readers changes its gate.

## Tests

Follow "Privileged tests and the CI host" in `docs/rust/space-conventions.md`.

- Test loop logic against fakes of the loop's sink or backend trait, and SQL
  against `pplx_space_db_test_fixtures`.
- Parse config through `from_environment(&dyn Environment)` with a `TestEnv`.
  Add that seam to a loop that only has `from_env()`.

Module moves change log targets and `sql!` locators: see "Log targets" in
`docs/rust/space-conventions.md`.
