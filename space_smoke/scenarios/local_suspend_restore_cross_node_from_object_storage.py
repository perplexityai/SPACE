"""A suspended sandbox restores from object storage onto a different node."""

import shlex

import space_smoke as smoke

PATH = "/tmp/space-smoke-cross-node-restore.txt"
MARKER = "space-smoke-cross-node-restore-ok"


# Why this needs a cordon, and why only on kind: a user restore prefers the
# node that holds the suspended bytes (a weight-100 scheduler preference, not a
# hard pin) and sbx-manager keeps those bytes until disk pressure reaps them,
# so a restore left to itself reuses the local artifact and never exercises the
# S3 copy. `ecw.pause_connect` already proves that same-node path. This
# scenario proves the durable one: it makes the source worker unschedulable for
# exactly the restore window, then requires sbx-manager's own
# `runtime_activated` event for the landed execution to report a healthy
# `remote_*` provenance with the storage URI present — a restore that merely
# changed nodes but was served from some other local copy would fail that
# check. Kubernetes exposes no per-sandbox "restore elsewhere" hint (only
# cordon and drain move placement), and cordoning a worker is not something to
# do to a shared cluster, which is why the fixture type pins this to kind.
#
# The portability wait before the cordon is load-bearing, not cosmetic: the
# snapshot upload is asynchronous behind the public paused state, and until a
# ready full snapshot with a storage location exists the API planner has only a
# source node and pre-binds the pod with `spec.nodeName` — which bypasses
# kube-scheduler, so the cordon would do nothing and the restore would land
# back on the source with `local_only` provenance. That false negative reads
# exactly like a product bug. The legacy shell suite guarded it the same way.
@smoke.scenario(
    "local.suspend_restore_cross_node_from_object_storage",
    # Cross-node restore pulls the full snapshot from the object store and re-adopts
    # the artifact identity on the landing node; give a slow KVM bench room.
    sandbox=smoke.SandboxConfig(timeout=900),
)
def suspend_restore_cross_node_from_object_storage(test: smoke.KindEcw) -> None:
    """A durably suspended sandbox restores onto a node other than the one
    holding its suspend bytes, pulls its snapshot from object storage to do so,
    and comes back with its filesystem intact and its data plane executing."""

    marker = f"{MARKER}-{test.sandbox.sandbox_id}"
    restore = test.cross_node_restore

    with test.phase("write state and record the source node before suspend"):
        written = test.run_command_when_ready(
            f"printf %s {shlex.quote(marker)} > {shlex.quote(PATH)}"
        )
        test.verify(
            written.exit_code == 0,
            "failed to write state before suspending the sandbox",
            command_stderr=written.stderr,
        )
        source = restore.current_placement()

    with test.phase("suspend durably"):
        # ECW pause is SPACE suspend: state is persisted and worker capacity
        # released. The public state settling to paused is what the fixture
        # waits on; the S3 upload is asynchronous behind it.
        test.pause()

    with test.phase("wait until the durable snapshot is what a restore would select"):
        restore.wait_until_restore_portable()

    with restore.cordoned(source.node_name):
        with test.phase("restore while the source worker is unschedulable"):
            test.reconnect_by_id()

        with test.phase("the restore pulled its snapshot from object storage"):
            landed = restore.current_placement()
            restore.expect_restored_from_object_storage(source=source, landed=landed)

    with test.phase("state and execution survive the cross-node restore"):
        observed = test.run_command_when_ready(f"cat {shlex.quote(PATH)}")
        test.expect_command(observed, stdout=marker)
