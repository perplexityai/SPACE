"""Each suspend writes its own immutable snapshot row and storage prefix."""

import shlex

import space_smoke as smoke

PATH = "/tmp/space-smoke-per-attempt.txt"
MARKER_A = "space-smoke-per-attempt-v1"
MARKER_B = "space-smoke-per-attempt-v2"


# The transactional-snapshot contract: a suspend never updates an earlier
# snapshot in place. Each attempt gets its own `snapshots` row and its own
# storage prefix (named by that row's id); an earlier row's facts (state,
# timestamp, size) do not move when a later suspend publishes; and a restore
# reads the newest ready row, which is proven by the bytes it brings back.
#
# The compose suite also tried to catch the second suspend mid-publish
# (`creating` row visible, reader still on v1). Against a fast in-cluster
# object store that window is milliseconds and the suite itself accepted
# "never observed"; the post-publish invariants below are the ones that
# hold deterministically and they are what is asserted.
@smoke.scenario(
    "local.suspend_snapshot_rows_are_per_attempt",
    # The codec is read from the snapshot's objects through the AWS CLI.
    required_tools=("aws",),
    sandbox=smoke.SandboxConfig(timeout=900),
)
def suspend_snapshot_rows_are_per_attempt(test: smoke.KindEcw) -> None:
    """Two suspends of one sandbox leave two ready snapshot rows with distinct
    ids and distinct id-named storage prefixes; the first row's facts are
    unchanged by the second publish; and the restore after the second suspend
    brings back the second suspend's bytes."""

    codec = test.suspend_codec

    with test.phase("write v1 state and suspend"):
        written = test.run_command_when_ready(f"printf %s {MARKER_A} > {shlex.quote(PATH)}")
        test.verify(written.exit_code == 0, "failed to write v1 state", command_stderr=written.stderr)
        test.pause()
        codec.expect_suspend_codec()

    with test.phase("v1 reaches ready with an id-named storage prefix and a size"):
        rows = codec.wait_for_ready_snapshots(1)
        test.verify(len(rows) == 1, "expected exactly one snapshot row after the first suspend", rows=[(r.snapshot_id, r.state) for r in rows])
        v1 = rows[0]
        test.verify(
            v1.state == "ready" and v1.snapshot_id in v1.storage_uri and (v1.size_bytes or 0) > 0,
            "the first snapshot row is not a ready, sized row with an id-named prefix",
            snapshot_id=v1.snapshot_id,
            storage_uri=v1.storage_uri,
            size_bytes=v1.size_bytes,
        )

    with test.phase("restore, write v2 state, suspend again"):
        test.reconnect_by_id()
        written = test.run_command_when_ready(f"printf %s {MARKER_B} > {shlex.quote(PATH)}")
        test.verify(written.exit_code == 0, "failed to write v2 state", command_stderr=written.stderr)
        test.pause()
        codec.expect_suspend_codec()

    with test.phase("v2 is its own ready row and prefix; v1 is untouched"):
        facts = codec.wait_for_ready_snapshots(2)
        test.verify(len(facts) == 2, "expected exactly two snapshot rows", rows=[(f.snapshot_id, f.state) for f in facts])
        v1_after, v2 = facts
        test.verify(
            v1_after == v1,
            "the first snapshot row changed when the second suspend published",
            v1_at_ready=v1.__dict__,
            v1_after_v2=v1_after.__dict__,
        )
        test.verify(
            v2.snapshot_id != v1.snapshot_id
            and v2.state == "ready"
            and v2.snapshot_id in v2.storage_uri
            and v2.storage_uri != v1.storage_uri
            and (v2.size_bytes or 0) > 0,
            "the second suspend did not publish as a distinct, ready, id-named row",
            v1=v1.__dict__,
            v2=v2.__dict__,
        )
        test.record("snapshot_rows", [f.snapshot_id for f in facts])

    with test.phase("the restore brings back the second suspend's bytes"):
        test.reconnect_by_id()
        observed = test.run_command_when_ready(f"cat {shlex.quote(PATH)}")
        test.expect_command(observed, stdout=MARKER_B)
