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

## Direct Substrate Contract

`DirectSubstrate` (`direct.rs`) exists for single-host deployments without
Kubernetes and for builds without the `kubernetes` feature.
`Substrate::from_env_with_lifecycle` returns Kubernetes only when built with
that feature and `SPACE_K8S_CONTROL_PLANE` is truthy; an unset, false or
unrecognized value selects Direct, and a Kubernetes init failure is an error,
not a fallback. Shipped images and Kind enable the feature; a plain
`cargo build -p pplx-space-api-svc` does not.

What Direct does today (its `Unsupported` and no-op methods, admin routes,
Kubernetes-only reconcilers and known gaps) is in the `direct.rs` module doc;
update it with any change to Direct. Don't cite a cluster-controller
reconciler as the retry path for a failed Direct execution retire or repair;
the `direct.rs` module doc lists the Kubernetes-only loops. Don't change a
known gap as a side effect of other work: rejecting or implementing one
changes Direct behavior and belongs in its own PR.

For new work:

- Implement new lifecycle operations for Kubernetes. On Direct, return
  `SubstrateError::Unsupported` unless the operation is one of the calls
  `examples/first_sandbox.py` makes (create, command session, list/inspect,
  delete).
- Make a new `SubstrateAdapter` operation a required method, so the compiler
  demands each substrate's implementation and its arm in
  `impl SubstrateAdapter for Substrate`; Direct returns
  `SubstrateError::Unsupported` explicitly. A default would let a forgotten
  arm give both substrates the default and skip the Kubernetes override. Only
  a hook shared code calls on every path may default to "nothing to do", as
  `retire_fence_mismatch` does, and it still needs its arm.
- Where shared code needs a static fact about the substrate, add a
  `SubstrateCapabilities` field, set it in both `SubstrateCapabilities::DIRECT`
  and `::KUBERNETES`, and read it through `capabilities()`; where the
  substrate must act, add a required method (above). Don't match
  `Substrate::Direct` or branch behavior on `managed_by()`. Matching
  `Substrate::Kubernetes(k)` to reach Kubernetes-only state is fine.
- Scoping a query to the rows a substrate owns is required, not debt. In new
  SQL, bind the substrate value instead of a `'k8s'`/`'space'` literal:
  `managed_by = $n` with `.bind(substrate.managed_by())` for the running
  substrate's rows, as the transient-lifecycle sweeper does, or a fixed
  `SubstrateKind::Kubernetes`/`::Space` where code targets one substrate's
  rows. When a fixed-scope statement may run under another substrate, return
  early in Rust instead of also ANDing the running substrate's value, as the
  sweeper's failed-active-execution cleanup does. Existing literals, such as
  the shared `routable_execution_or_non_k8s` fragment, are debt. Never choose
  a runtime endpoint from a row's label.
- `node_direct`, `sandbox_direct_route_identities` and
  `sandbox_direct_route_provenance` belong to Direct. Outside tests, write
  them only from `direct.rs` and the Direct node registry; read them through
  an existing query that already joins them rather than adding a join.
  archcheck's `pattern_ratchet.direct_tables` rule enforces this (advisory
  today).
- Other existing substrate matches, Direct-table joins in shared code and
  `Unsupported` trait defaults (`list_pool_configs`, `upsert_pool_config`,
  `disable_pool_config`) are debt, not precedent.

## Tests

Kubernetes pod rendering is owned by `pplx_space_common_core::kubernetes`.
Keep broad, human-readable golden snapshots for the shared pod shapes there
rather than in controller-specific tests, so reviewers can compare on-demand,
restore, and pooled manifests in one place.

Keep narrow tests only for behavior that is hard to express clearly in the broad
YAML contract test.

Use `Substrate::Direct` only to test Direct behavior (tests under `direct/`) or
behavior that genuinely doesn't depend on the substrate. Test shared lifecycle
behavior Kubernetes deployments depend on against a `Substrate::Kubernetes`
built with the `kubernetes::tests` helpers (`substrate_with_store` and its
variants), as `routes/public/sandboxes/admission_test_fixture.rs` does.
Adapter tests inside `kubernetes/` use `scripted_test_doubles.rs`, as
`adapter_characterization_tests/` does. Both compile only with
`--features kubernetes`: CI's `--workspace --all-features` run includes them,
but `cargo test -p pplx-space-api` silently skips them.
