# Substrate Controller

## Kubernetes API Access

Do not issue direct, naked Kubernetes API LIST calls from request paths or
reconcile-tick paths, such as `Api::<T>::...list(&ListParams::default())` or
unfiltered `Api::all(...).list(...)`.

Prefer reading from an existing watcher-backed reflector `Store`, such as
`K8sCaches`, so hot paths reuse the controller's watch/list lifecycle instead of
adding per-request or per-tick apiserver load. If a LIST is truly unavoidable,
it must carry a bounding selector (`.labels(...)` or a field selector) and a
code comment explaining why a reflector read is not sufficient.

Use the warm-pool `cordoned_node_names` change as the canonical example: it
converted a per-tick full-cluster Node LIST into a reflector-store scan. Node
cordon state lives in `spec.unschedulable`, which cannot be pushed into a
Kubernetes list selector, so a reflector read is the right tool.

## Sandbox Pod Reads

Every substrate replica caches every sandbox pod, so the sandbox-pod reflector
stores `SandboxPodView` (`kubernetes/sandbox_pod_view.rs`), never a full `Pod`.
Uncached reads and create responses return the same view. To read a field the
view doesn't carry, add it to the view and its conversion; don't add a second
cache or fall back to a full-object GET to get it. Annotation and label reads go
through `SandboxPodKey`, which is also the set of keys the conversion keeps.

## Substrate contract

Both substrates are supported. Kubernetes runs production. Direct
(`direct.rs`: registered nodes, node-local VMs, no pod per sandbox) runs
single-host and non-Kubernetes deployments and is the planned base for
pod-less hosting on Kubernetes nodes. Breaking or silently degrading either
is a regression, whichever one the change was for.

An adapter should answer only two kinds of question. Nodes: which exist, how
to reach a node's node-api, whether a node may take new work, whether it is
still there, how to remove it. Executions: launch, start, wake, restore,
suspend, retire and delete one execution on a chosen node; connect to it;
what the infrastructure observes about it. Placement, admission, warm-pool
policy, lifecycle transitions, leases, generation allocation, retries and
failure exits, repair, drain and evacuation are core logic. It belongs once,
in shared code outside `direct.rs` and `kubernetes/` that calls the adapter,
such as the sandbox controller (`controllers/sandbox/`) and
`pplx-space-sandbox-store`. Much of it still lives in the adapters: Direct's
worker and restore-candidate retries and its restore lease, Kubernetes'
restore leases, started recovery and redrive, and the Kubernetes-only
`pplx-space-placement` machine. That code is debt: don't extend it, and put
new core logic in shared code.

`Substrate::from_env_with_lifecycle` returns Kubernetes only when built with
the `kubernetes` feature and `SPACE_K8S_CONTROL_PLANE` is truthy; an unset,
false or unrecognized value selects Direct, and a Kubernetes init failure is
an error, not a fallback. Shipped images and Kind enable the feature; a plain
`cargo build -p pplx-space-api-svc` does not.

For new work:

- Put behavior above the adapter. Add an adapter method only for an
  infrastructure action the existing ones cannot express, and implement it
  for both substrates in the same PR.
- Record every gap. If one substrate cannot support an operation yet, return
  `SubstrateError::Unsupported` from it and add a row to the gap table in its
  module doc (`direct.rs`, or `kubernetes/mod.rs`, whose first gap starts
  one): what is refused, its effect, who owns closing it, and the change that
  removes it. An `Unsupported` without a row, or a row without an owner and a
  removal plan, fails review; an existing row is not precedent for a new one.
  A PR that changes Direct's behavior updates the `direct.rs` module doc,
  and the PR that closes a gap deletes its row.
- No single-substrate reconcilers for sandbox or node state. A loop that
  repairs, retries, drains, evacuates or collects that state runs for both
  substrates and reaches it through the adapter. A loop that manages
  Kubernetes objects themselves (pods, Nodes, DaemonSets) belongs to the
  Kubernetes adapter side, and its module doc says so. The state loops
  `run_reconcilers` (`cluster/controller/server/src/reconcilers.rs`) starts
  only for Kubernetes are debt, not precedent or a Direct retry path.
- Make a new `SubstrateAdapter` operation a required method, so the compiler
  demands each implementation and its arm in `impl SubstrateAdapter for
  Substrate`: with a default, a forgotten forwarding arm silently gives both
  substrates the default instead of their implementations. Keep this rule
  until that dispatch is generated. Only a hook shared code calls on every
  path may default to "nothing to do", as `retire_fence_mismatch` does, and
  it still needs its arm.
- `Substrate`'s variants (`Inner` in `mod.rs`) stay private; shared code
  builds one with `Substrate::from_direct` or `Substrate::from_kubernetes`.
  For a static fact about the substrate, add a `SubstrateCapabilities` field
  with a doc comment, set it in `::DIRECT` and `::KUBERNETES`, and read it
  through `capabilities()`. A capability states an infrastructure fact, never
  "this SPACE feature is off here": a missing feature is a gap row.
- Code that needs Kubernetes-only state (its client or caches) calls
  `as_kubernetes()` only to bind and use the payload. A presence test on it,
  or comparing `capabilities()` with either const, asks which substrate is
  running: add a capability. archcheck's `pattern_ratchet.kubernetes_access`
  counts `as_kubernetes(` calls in production code outside this module, so a
  new caller needs a reviewed baseline entry. It cannot tell a presence test
  from a use; review catches that. `pattern_ratchet.capability_identity`
  flags either const named in production code outside the two
  `capabilities()` implementations.
- `managed_by()` names the running substrate for SQL row scope and wire
  labels; never parse it to decide behavior beyond the one existing parse,
  `failed_active_execution_scope` (`transient_lifecycle_sweeper/store.rs`).
- A decision about one row's substrate (its `managed_by`) calls a
  `SubstrateKind` method named for the question; add one if none asks it.
  Rust repeating a statement's fixed scope compares against the const the
  statement binds (e.g. `REGISTRY_SUBSTRATE`); mapping a variant to a value
  may match it. A fact about the running substrate reads `capabilities()`.
  Never compare the label string.
- Scoping a query to a substrate's rows is required, not debt. Bind the
  value (`managed_by = $n` from `substrate.managed_by()` or a fixed
  `SubstrateKind`), never a `'k8s'`/`'space'` literal (archcheck's
  `pattern_ratchet.substrate_literals` catches the common forms in
  production code, not all; its comment lists what it misses). When a
  fixed-scope statement may run under the other substrate, return early in
  Rust (a capability, or `failed_active_execution_scope` where it fits)
  instead of ANDing the running value. Never choose a runtime endpoint from
  a row's label.
- `node_direct`, `sandbox_direct_route_identities` and
  `sandbox_direct_route_provenance` are Direct's tables. Outside tests, write
  them only from `direct.rs` and the Direct node registry, and read them
  through an existing query that joins them (archcheck's
  `pattern_ratchet.direct_tables`, advisory today).
- Direct-table joins in shared code, `Unsupported` trait defaults
  (`list_pool_configs`, `upsert_pool_config`, `disable_pool_config`) and
  `supports_node_drain`, a capability for a missing feature, are debt, not
  precedent.

## Tests

Kubernetes pod rendering is owned by `pplx_space_common_core::kubernetes`.
Keep broad, human-readable golden snapshots for the shared pod shapes there
rather than in controller-specific tests, so reviewers can compare on-demand,
restore, and pooled manifests in one place.

Keep narrow tests only for behavior that is hard to express clearly in the broad
YAML contract test.

Test behavior whose path differs by substrate on both substrates. Kubernetes
runs through a `Substrate::from_kubernetes` built with the
`kubernetes::tests` helpers (`substrate_with_store` and its variants), as
`routes/public/sandboxes/admission_test_fixture.rs` does. Direct runs
through `crate::test_support::direct_substrate` with a `SimWorkerHarness`
worker, as `controllers/sandbox/lifecycle/characterization_tests.rs` does.
Behavior that does not depend on the substrate needs one substrate. A
substrate-dependent test that runs on one substrate names the gap row that
keeps it off the other. Tests of Direct itself, under `direct/`, use the
fixtures in `direct/tests.rs`. Adapter tests inside `kubernetes/` use
`scripted_test_doubles.rs`, as `adapter_characterization_tests/` does. The
Kubernetes helpers compile only with `--features kubernetes`: CI's
`--workspace --all-features` run includes them, but
`cargo test -p pplx-space-api` silently skips them.

A test that uses Postgres fails when Postgres is unavailable. Only the
optional fixtures (`try_pg_test_db` and its variants, which `direct/tests.rs`
uses) skip instead, and only when the run sets `SPACE_TEST_DEPS_OPTIONAL`.
Such a skip is not evidence the test passed ("Service-dependency tests" in
`docs/rust/space-conventions.md`).
