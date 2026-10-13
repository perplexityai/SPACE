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

## Tests

Kubernetes pod rendering is owned by `pplx_space_common_core::kubernetes`.
Keep broad, human-readable golden snapshots for the shared pod shapes there
rather than in controller-specific tests, so reviewers can compare on-demand,
restore, and pooled manifests in one place.

Keep narrow tests only for behavior that is hard to express clearly in the broad
YAML contract test.
