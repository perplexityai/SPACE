# `pplx-space-api`

Before changing either substrate, adding a substrate operation, or changing
shared code that depends on the substrate (`capabilities()`, `as_kubernetes()`,
`managed_by()`) or names a Direct-only table, read
`src/controllers/substrate/AGENTS.md`.

## Changes

- In `src/controllers/substrate/kubernetes/`, new substrate SQL goes in
  `store/` and maps `sqlx` errors with `DbContext::db_context`, so lock
  contention stays a 503. Warm-pool slot rows belong to
  `pplx_space_cluster_pool::WarmPoolStore`.
- `adapter/` implements `SubstrateAdapter`; `pod_ops/` holds pod create,
  read, delete, restore, wake and redrive.
- Cold create and warm-pool adoption both consume `PerSandboxSetup`
  (`kubernetes/per_sandbox_setup.rs`) by exhaustive destructure. Add
  per-sandbox setup there, not to one path.
- Native `/v1` operations come from the `pplx_space_public_api::routes`
  manifest. `src/routes/public_docs/compatibility/m1_openapi_baseline.json` is
  the M1 compatibility floor, not a snapshot to refresh.
- ECW handlers live in `src/routes/e2b/` and translate at the HTTP boundary.

## Tests

Follow "Privileged tests and the CI host" in `docs/rust/space-conventions.md`.

- `AppState::from_pool` and the `from_env` constructors read process env.
  Test a parser through its explicit-input form where one exists
  (`from_environment`; pass `TestEnv` where it takes an `Environment`). Use
  `enter_test_process` only for tests that need the process-env constructors.
- Point clients a test must not use at a closed port with a short acquire
  timeout, not a default port the host may serve.
- Another crate's tests build a `KubernetesSubstrate` in process with
  `KubernetesSubstrate::with_seeded_caches`
  (`src/controllers/substrate/kubernetes/test_substrate.rs`), behind
  `test-hooks`. Enable that feature only from `[dev-dependencies]`.

Module moves change log targets and `sql!` locators: see "Log targets" in
`docs/rust/space-conventions.md`.
