//! Shared SQL fragment for the Kubernetes execution-routability contract.
//!
//! In the Kubernetes substrate, `sandboxes.lifecycle = 'started'` is a promise
//! that the control plane can route traffic to the sandbox: an **active**
//! `sandbox_k8s_executions` row in `state = 'running'` with a non-NULL
//! `pod_ip`. Two decision points must agree on that predicate, and a wedge
//! opens whenever they drift:
//!
//! * **Node published-state convergence** — the internal API's node report
//!   handler converges `restoring -> started` only when a routable execution
//!   exists; otherwise it deliberately leaves the row `restoring` so the next
//!   connect can wait on and hydrate the in-flight pod.
//! * **Transient-lifecycle sweeper** — the cluster controller's sweeper must
//!   apply the same test before converging an aged alive `restoring` row to
//!   `started`. Converging without it asserts a lifecycle the substrate can't
//!   back, and the wake path then refuses a fresh pod for a `started` gen>=2
//!   row without restore context — the sandbox wedges until something else
//!   heals the row.
//!
//! Centralising the fragment makes "routable" one question with one answer.
//! It is its own crate, with no dependencies, so a crate that composes the
//! fragment need not depend on `pplx-space-common-svc` for it.

/// Returns a self-contained `EXISTS(...)` SQL fragment evaluating to TRUE iff
/// the sandbox row aliased as `sandbox_alias` has an active
/// `sandbox_k8s_executions` row the control plane can route to
/// (`state = 'running'` and `pod_ip IS NOT NULL`).
///
/// This is the Kubernetes-substrate contract behind `lifecycle = 'started'`.
/// Rows of other substrates have no such contract and should be handled by
/// the caller — see [`routable_execution_or_non_k8s`].
///
/// `sandbox_alias` is the SQL alias for the `sandboxes` row in the calling
/// query — pass the bare table name (`"sandboxes"`) when no alias is set.
/// The fragment is safe to compose with `NOT`, `AND`, `OR`, etc.
pub fn routable_k8s_execution_exists(sandbox_alias: &str) -> String {
    format!(
        "EXISTS (\n\
             SELECT 1\n\
               FROM sandbox_k8s_executions rex\n\
              WHERE rex.sandbox_id = {sandbox_alias}.id\n\
                AND rex.active\n\
                AND rex.state = 'running'\n\
                AND rex.pod_ip IS NOT NULL\n\
         )"
    )
}

/// Returns a boolean SQL fragment evaluating to TRUE iff the sandbox row
/// aliased as `sandbox_alias` satisfies the routability contract *or* has no
/// such contract (its `managed_by` is not Kubernetes). The composition callers
/// gate `started`-convergence on.
///
/// `kubernetes_placeholder` is a parameter (`"$5"`) used nowhere else in the
/// calling statement, which binds it to `SubstrateKind::Kubernetes`: the
/// substrate value is bound rather than spelled in SQL (see "Substrate values
/// are bound parameters" in `docs/rust/space-conventions.md`), and each caller
/// numbers its own parameters. Reusing a placeholder that holds another value
/// (the sweeper's `$1` is the running substrate) would change which rows the
/// fragment exempts.
pub fn routable_execution_or_non_k8s(sandbox_alias: &str, kubernetes_placeholder: &str) -> String {
    let routable = routable_k8s_execution_exists(sandbox_alias);
    format!(
        "(\n\
             {sandbox_alias}.managed_by <> {kubernetes_placeholder}\n\
             OR {routable}\n\
         )"
    )
}

#[cfg(test)]
mod tests;
