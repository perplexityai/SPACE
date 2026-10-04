"""A btrfs-send restore with no way to obtain the parent template fails closed."""

import space_smoke as smoke


# The cold-node branch's failure half. When the landing worker lacks the parent
# template and the cluster has no storage location to pull it from,
# sbx-manager's `GetTemplate` fails and the restore must fail there: nothing
# received, nothing materialized, no template conjured from nowhere.
#
# Forcing, both kind-only and compensated: the fixture's own template is
# withheld from the cluster's `template_release_storage_locations` first,
# then removed from the other sandbox-pool worker (template-manager's
# `delete-template`). The rows are saved first, their restoration armed as a
# critical compensation before deletion, and put back inside the scenario
# body. With the source cordoned the restore can only try the
# cold worker. The rows go back only after the sandbox, its pods and its local
# rows are gone: the kubelet keeps retrying the failed restore pod, and a retry
# that can reach the parent again would bring the sandbox back up there.
@smoke.scenario(
    "local.btrfs_send_restore_fails_closed_without_parent_source",
    sandbox=smoke.SandboxConfig(timeout=1200),
)
def btrfs_send_restore_fails_closed_without_parent_source(test: smoke.KindEcw) -> None:
    """A sandbox suspended with the production stream codec, restored onto a
    worker that lacks its parent template while the cluster has no storage
    location for that template, fails: the client sees the failure, sbx-manager
    reports the restore failed at the template lookup, and no instance or
    template is materialized on that worker."""

    codec = test.suspend_codec
    restore = test.cross_node_restore
    template = test.sandbox_template_name()

    with test.phase("locate the source worker and the cold target"):
        source = restore.current_placement()
        source_pod = codec.worker_pod(source.node_name)
        target_node, target_pod = codec.other_sandbox_worker(source.node_name)
        test.record("target_node", target_node)

    with test.phase("the worker resolves an omitted suspend format to the stream codec"):
        codec.require_btrfs_send_default(source_pod)

    with test.phase("suspend without naming a format"):
        suspended_at = codec.mark_since()
        test.pause()

    with test.phase("the suspend was sealed as btrfs-send-v1"):
        codec.expect_suspend_codec(since_time=suspended_at)

    with test.phase("wait until the durable snapshot is what a restore would select"):
        restore.wait_until_restore_portable()

    with restore.cordoned(source.node_name):
        with test.phase("withhold the parent's cluster storage, then evict it from the target"):
            # Evicting first lets prefetch start a download that the restore
            # can join even after its storage location has been withheld.
            codec.withhold_release_storage(template)
            codec.evict_template(target_pod, template)
            since = codec.mark_since()

        with test.phase("the restore fails at the client"):
            codec.expect_reconnect_fails()

        with test.phase("sbx-manager failed the restore at the template lookup"):
            # The parent lookup runs inside the rootfs pull stage (the template
            # is only a prerequisite of the receive), so that is the stage the
            # event names; the error kind is what says it was the lookup.
            codec.expect_restore_failed(
                reason="pull_rootfs",
                error_kind="parent_template_lookup",
                since_time=since,
            )

        with test.phase("nothing was materialized on the target"):
            codec.expect_no_instance(target_pod)
            codec.expect_template_absent(target_pod, template)

        with test.phase("delete the sandbox, then put the cluster's storage rows back"):
            codec.restore_release_storage()
