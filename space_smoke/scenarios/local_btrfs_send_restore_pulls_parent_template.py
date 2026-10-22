"""A btrfs-send restore on a worker lacking the parent template pulls and receives it."""

import shlex

import space_smoke as smoke

PATH = "/tmp/space-smoke-btrfs-send-pull.txt"
MARKER = "space-smoke-btrfs-send-pull-ok"


# The btrfs-send-v1 suspend codec is an incremental `btrfs send`
# against the template subvolume the sandbox was cloned from. That makes a
# restore depend on the landing worker holding that exact parent build as a
# btrfs subvolume stamped with the deterministic received UUID
# (`UUIDv5(NS, "name@hash")`), so a stream produced against one node's copy
# can be received against another node's independently installed copy. The
# happy path is covered since the kind chart resolves omitted formats to the
# stream codec; this scenario covers the cold-node branch: the landing worker
# has never seen the template.
#
# Forcing: the sandbox is created from a minimal run-owned template (the base
# image alone) rather than the seed. template-manager prefetches only the
# names in `SPACE_TEMPLATE_PREFETCH_TAGS` and never an org-specific release, so
# once the template is removed from the other sandbox-pool worker through
# template-manager's own `delete-template`, with the source cordoned, nothing
# puts it back before the restore asks for it. On the seed, a prefetch tick
# landing between that removal and the restore re-installed the parent first,
# and the restore then found it on disk (bench 2026-10-21). The template is also
# a fraction of the seed's size, so the landing worker's pull is short. The proof
# is then the landed pod on that node restoring from object storage, the parent
# build the snapshot names back on that node as a subvolume with the expected
# received UUID, the instance being a received subvolume, and the pre-suspend
# state intact. template-manager's own pull metric must count a successful
# by-hash `trigger=on_demand` pull of the parent on that worker above a
# baseline taken once the count had stopped moving before the removal — read
# from the bench's Prometheus, which only `otel` mode runs.
@smoke.scenario(
    "local.btrfs_send_restore_pulls_parent_template",
    telemetry_mode="otel",
    # The codec is read from the snapshot's objects through the AWS CLI.
    required_tools=("aws",),
    # A minimal template build, then a cross-node restore that pulls it on the
    # landing worker.
    sandbox=smoke.SandboxConfig(timeout=1200, kind_dedicated_template=True),
)
def btrfs_send_restore_pulls_parent_template(test: smoke.KindEcw) -> None:
    """A sandbox suspended with the production stream codec restores onto a
    worker that lacks its parent template: the worker pulls the template on
    demand, installs it as a btrfs subvolume with the deterministic received
    UUID, receives the stream into a subvolume instance, and the sandbox comes
    back from object storage with its state intact."""

    marker = f"{MARKER}-{test.sandbox.sandbox_id}"
    codec = test.suspend_codec
    restore = test.cross_node_restore
    template = test.sandbox_template_name()

    with test.phase("write state; locate the source worker and the cold target"):
        written = test.run_command_when_ready(f"printf %s {shlex.quote(marker)} > {shlex.quote(PATH)}")
        test.verify(written.exit_code == 0, "failed to write state before suspending", command_stderr=written.stderr)
        source = restore.current_placement()
        source_worker = source.node_name
        target_node = codec.other_sandbox_worker(source.node_name)
        test.record("target_node", target_node)

    with test.phase("the worker resolves an omitted suspend format to the stream codec"):
        codec.require_btrfs_send_default(source_worker)

    with test.phase("suspend without naming a format"):
        test.pause()

    with test.phase("the suspend was sealed as btrfs-send-v1"):
        codec.expect_suspend_codec()

    with test.phase("record the parent build the snapshot names"):
        digest = codec.template_hash(template)

    with test.phase("wait until the durable snapshot is what a restore would select"):
        restore.wait_until_restore_portable()

    with test.phase("settle the target's on-demand pull count before removing the parent"):
        on_demand_before = codec.quiescent_on_demand_pull_counts(target_node, template)

    with restore.cordoned(source.node_name):
        with test.phase("remove the parent template from the target worker"):
            codec.evict_template(target_node, template)

        with test.phase("restore while only the cold worker is schedulable"):
            test.reconnect_by_id()

        with test.phase("the restore landed on the cold worker from object storage"):
            landed = restore.current_placement()
            restore.expect_restored_from_object_storage(source=source, landed=landed)
            test.verify(
                landed.node_name == target_node,
                "the restore landed somewhere other than the cold sandbox-pool worker",
                landed_node=landed.node_name,
                target_node=target_node,
            )

    with test.phase("the target pulled the parent on demand and installed it as a portable subvolume"):
        codec.expect_parent_pulled_on_demand(target_node, template, digest, before=on_demand_before)
        codec.expect_template_is_portable_subvolume(target_node, template, digest)

    with test.phase("the instance was reconstructed by btrfs receive"):
        codec.expect_instance_is_subvolume(target_node)

    with test.phase("state and execution survive"):
        observed = test.run_command_when_ready(f"cat {shlex.quote(PATH)}")
        test.expect_command(observed, stdout=marker)
