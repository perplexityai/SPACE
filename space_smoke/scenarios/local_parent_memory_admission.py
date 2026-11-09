"""Memory admission refuses at the sandbox parent cgroup's limit, not at host memory."""

import space_smoke as smoke
from space_smoke.parent_memory_admission import SLACK_MIB


# Each worker's `/sys/fs/cgroup/space/memory.max` is moved a few hundred MiB
# around today's admission sum while host memory stays far above it. Writes
# to the workers are fenced by one mutation Lease recording the standing
# limits, and restored in a phase. The refused create waits out the API's
# create deadline.
@smoke.scenario(
    "local.parent_memory_admission",
    sandbox=smoke.SandboxConfig(timeout=1500),
    ignored=(
        "Opt-in: lowers every worker's sandbox parent memory limit, which refuses "
        "the creates of any scenario running alongside it."
    ),
)
def parent_memory_admission(test: smoke.KindEcw) -> None:
    """With host memory far above the need, a create is admitted when every
    Ready worker's parent cgroup limit sits just above today's admission sum
    (parent use without reclaimable cache, plus each running sandbox's growth
    charge of at most `reserve_mib`, the new sandbox and the reserve) and
    refused just below it. With the limit still below the sum but the parent's
    `memory.stat` unreadable to node-manager, admission falls back to host
    memory and the create is admitted."""

    admission = test.parent_memory_admission

    with test.phase("take the Lease and record every worker's standing parent limit"):
        admission.capture()

    with test.phase("run a sandbox on every worker"):
        admission.occupy_every_worker()

    with test.phase("limits just above today's admission sum: the create is admitted"):
        admission.set_limits(SLACK_MIB, "above")
        admission.expect_admitted("admitted_above_sum")

    with test.phase("limits just below today's admission sum: the create is refused"):
        admission.set_limits(-SLACK_MIB, "below")
        admission.expect_refused("refused_below_sum")

    with test.phase("parent memory.stat unreadable: admission falls back to host memory"):
        admission.hide_parent_stat()
        admission.expect_admitted("admitted_without_parent_stat")

    with test.phase("restore every worker's limit and memory.stat"):
        admission.restore()
