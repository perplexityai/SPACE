"""A runtime upgrade boots in its sandbox's pod: a stopped sandbox stays stopped, a start runs it."""

import dataclasses
import shlex
from typing import cast

import space_smoke as smoke
from space_smoke.capabilities import StoppableLifecycleSandbox
from space_smoke.runtime_upgrade import (
    E0,
    E1,
    E2,
    refused_as_stranded,
    runtime_upgrade_pod_problems,
)

PATH = "/home/user/data/runtime-upgrade.txt"
MARKER = "space-smoke-runtime-upgrade-ok"


def _write_marker(
    test: smoke.CommandSandboxFixture, sandbox: StoppableLifecycleSandbox
) -> str:
    marker = f"{MARKER}-{sandbox.sandbox_id}"
    written = sandbox.run(
        f"mkdir -p {shlex.quote(PATH.rsplit('/', 1)[0])} && "
        f"printf %s {shlex.quote(marker)} > {shlex.quote(PATH)}"
    )
    test.verify(
        written.exit_code == 0,
        "failed to write state before stopping the sandbox",
        sandbox_id=sandbox.sandbox_id,
        command_stderr=written.stderr,
    )
    return marker


def _read_marker(
    test: smoke.CommandSandboxFixture, sandbox: StoppableLifecycleSandbox, marker: str
) -> None:
    observed = sandbox.run(f"cat {shlex.quote(PATH)}")
    test.verify(
        observed.exit_code == 0 and observed.stdout == marker,
        "the file written before the stop did not survive the upgrade",
        sandbox_id=sandbox.sandbox_id,
        stdout=observed.stdout,
        stderr=observed.stderr,
    )


# The decision, the claim, the pod mode and the outcome reads are crate tests
# (`kubernetes/tests/stopped_runtime_upgrade/`, `runtime_upgrade_dispatch.rs`),
# and the shim's UpgradeRuntime call and sandbox-manager's boot are the node
# half's (`shim/runtime_upgrade_test.go`, `runtime_upgrade/rpc/tests.rs`).
# This scenario is the seam between them on real workers: a real shim has to
# boot the target from the pod's annotations, and the node's publication and
# a later start have to show the sandbox on it.
#
# A native scenario because stop/start is a native-API lifecycle. It runs
# only when named: a stock Kind bench lacks the runtime-upgrade settings, so
# its workers never enforce the target and the run would fail as an
# infrastructure error, and it changes the cluster policy for every sandbox
# while it runs. Kind-only: on another backend the settings, catalog and
# policy controls raise a configuration error, so a run named there ends
# before it touches anything shared. Run it on a disposable bench brought up
# with the runtime-upgrade settings and nothing else of yours on it
# ("Runtime-upgrade bench" in infra/space/kind/README.md). Every worker must
# have Firecracker 1.17.0 / Linux 6.1.155 installed and certify it; a bench
# whose workers don't enforce it once the policy names it is an
# infrastructure error, never a pass.
@smoke.scenario(
    "native.runtime_upgrade_stopped_and_start",
    api_surfaces=("native",),
    tier="manual",
    manual_reason=(
        "requires a disposable Kind bench brought up with the runtime-upgrade "
        "settings; it changes the cluster policy for every sandbox"
    ),
    # The upgrade boots in the sandbox pod's containerd shim.
    substrates=("kubernetes",),
    test_hooks=(
        "SPACE_ADMIN_RUNTIME_UPGRADE_TEST_HOOK_ENABLED, which mounts the admin trigger of a "
        "stopped sandbox's runtime upgrade"
    ),
    sandboxes=2,
    # Two rollouts of the API and the controller, the policy reaching every
    # worker, two cold boots under the target and two starts.
    timeout_minutes=45,
)
def runtime_upgrade_stopped_and_start(test: smoke.CommandSandboxFixture) -> None:
    """Two sandboxes start on E0, get a file written and stop. With E0 blocked
    and a default E2 that no node certifies an edge to from E0, the admin trigger of a stopped
    sandbox's runtime upgrade is refused with 412 and the sandbox stays
    stopped on E0, with no pod placed. With E0 deprecated and its edge to E1
    certified, the trigger upgrades the first sandbox: every pod it places
    carries the runtime-upgrade mode targeting `stopped` under the operation
    it answers, its control-plane lifecycle reads `stopped` once the trigger
    answers, the node publishes it stopped under E1, a new disk snapshot row
    appears within a bounded wait (the trigger answers once the backup is
    enqueued, not finished), and no pod is left.
    A user start then runs it under E1 with the file intact. A plain start of
    the second sandbox places a pod targeting `started` and runs it under E1,
    file intact."""

    upgrade = test.runtime_upgrade
    first, second = (
        cast(StoppableLifecycleSandbox, sandbox) for sandbox in test.sandboxes.items
    )

    with test.phase("write state on both sandboxes and stop them on E0"):
        markers = {
            sandbox.sandbox_id: _write_marker(test, sandbox)
            for sandbox in (first, second)
        }
        for sandbox in (first, second):
            sandbox.stop()
            upgrade.expect_published(
                sandbox.sandbox_id, status="stopped", environment=E0, unbound_ok=True
            )

    with upgrade.scoped():
        with test.phase("turn the upgrade settings on for the API and the controller"):
            upgrade.enable()

        with test.phase(
            "negative: E0 blocked, default E2 with no edge from E0, refuses the upgrade"
        ):
            # Not "no edge to E1": the controller rebuilds the catalog's
            # edges from what the nodes certify, which includes E0 -> E1,
            # and could restore it before the trigger. Nothing certifies
            # E0 -> E2.
            upgrade.seed_policy(edge=False, e0_state="blocked", default=E2)
            # One trigger, expected to be refused: an API replica that read
            # the standing policy in the last cache TTL would decide on it
            # instead, so let every replica's cache expire first.
            upgrade.wait_policy_cache()
            upgrade.watch(first.sandbox_id)
            status, body = upgrade.upgrade_stopped(first.sandbox_id)
            test.verify(
                status == 412 and refused_as_stranded(body),
                "a stranded sandbox's upgrade was not refused as stranded",
                status=status,
                body=body,
            )
            created = upgrade.created_pods(expect_any=False)
            test.verify(
                upgrade.lifecycle(first.sandbox_id) == "stopped"
                and upgrade.published(first.sandbox_id).environment in (E0, None)
                and not created,
                "the refused upgrade moved the sandbox or placed a pod",
                published=dataclasses.asdict(upgrade.published(first.sandbox_id)),
                pods=len(created),
            )

        with test.phase("certify E0's edge to E1; every worker enforces E1"):
            upgrade.seed_policy(edge=True, e0_state="deprecated")
            upgrade.require_workers_enforce_target()

        with test.phase(
            "stopped arm: the trigger upgrades the sandbox and leaves it stopped"
        ):
            disks_before = set(upgrade.disk_snapshot_ids(first.sandbox_id))
            upgrade.watch(first.sandbox_id)
            status, body = upgrade.upgrade_stopped_once_policy_converges(
                first.sandbox_id
            )
            test.verify(
                status == 200
                and body.get("outcome") == "upgraded"
                and body.get("target_environment") == E1,
                "the stopped sandbox's runtime upgrade did not report the target",
                status=status,
                body=body,
            )
            test.verify(
                upgrade.lifecycle(first.sandbox_id) == "stopped",
                "the stopped sandbox's upgrade left it out of stopped",
            )
            problems = runtime_upgrade_pod_problems(
                upgrade.created_pods(expect_any=True),
                target_state="stopped",
                operation_id=str(body.get("operation_id")),
            )
            test.verify(
                not problems,
                "the upgrade's pods were not its runtime-upgrade pods",
                problems=problems,
            )
            upgrade.expect_published(first.sandbox_id, status="stopped", environment=E1)
            test.verify(
                upgrade.wait_new_disk_snapshot(first.sandbox_id, disks_before),
                "the upgraded disk was not backed up",
            )
            upgrade.wait_no_live_pod(first.sandbox_id)

        with test.phase("the upgraded sandbox starts under E1 with its file"):
            first.start()
            upgrade.expect_published(first.sandbox_id, status="started", environment=E1)
            _read_marker(test, first, markers[first.sandbox_id])

        with test.phase(
            "start arm: a start of a stopped sandbox on E0 boots it under E1"
        ):
            upgrade.watch(second.sandbox_id)
            second.start()
            problems = runtime_upgrade_pod_problems(
                upgrade.created_pods(expect_any=True),
                target_state="started",
                operation_id=None,
            )
            test.verify(
                not problems,
                "the start's pods were not runtime-upgrade pods",
                problems=problems,
            )
            upgrade.expect_published(
                second.sandbox_id, status="started", environment=E1
            )
            _read_marker(test, second, markers[second.sandbox_id])
