"""template-manager's startup recovery adopts, confirms, prunes or removes correctly."""

import space_smoke as smoke


# Recovery (`crates/libs/template/mgr/src/service/startup_recovery.rs`) runs
# once at daemon start over every template name directory on the node, and its
# rule is a security boundary: a digest directory is adopted into the catalog
# only when its bootable artifact hashes to the directory's own name, and a
# directory whose files disagree with its manifest is removed even when a
# catalog row vouches for it. A `template_builds` row is the validity signal
# that lets the common case skip the hash.
#
# One restart proves the whole table. The scenario plants five small throwaway
# builds on the sandbox-pool worker that is *not* running its sandbox, puts
# each into the state one branch handles, deletes that worker's
# template-manager pod once, and reads the verdicts from the new pod's log —
# then checks each against disk and `space-tmplmgr get`. The plants have no cluster
# release, so a miss there cannot re-pull and the negative checks mean
# something.
#
# States are made without touching the daemon's SQLite: "no row" is produced
# by stashing the digest directory, running the daemon's own `delete-template`
# and putting the bytes back — the exact residue of a crash between the file
# rename and the catalog commit. The restart is the one new privilege and is
# kind-only twice over: by fixture type and by the adapter's isolation guard.
@smoke.scenario(
    "local.template_startup_recovery_decision_table",
    # Five 4 MiB registrations, a template-manager pod replacement, and
    # recovery hashing one small artifact; the sandbox is idle.
    sandbox=smoke.SandboxConfig(timeout=1500),
)
def template_startup_recovery_decision_table(test: smoke.KindEcw) -> None:
    """After a worker's template-manager restarts, startup recovery confirms a
    build whose row and bytes agree without hashing, prunes a row whose bytes
    are gone, adopts row-less bytes only when the artifact hashes to the
    directory name, removes row-less bytes whose hash disagrees, and removes
    bytes whose size disagrees with the manifest even with a row — leaving
    disk and catalog consistent and the sandbox on the other worker running."""

    recovery = test.startup_recovery

    with test.phase("locate a sandbox-pool worker that is not running this sandbox"):
        recovery.spare_worker()

    with test.phase("plant one build per branch of the decision table"):
        happy = recovery.plant("happy")
        stale = recovery.plant("stale")
        recovery.make_row_stale(stale)
        orphan = recovery.plant("orphan")
        recovery.orphan(orphan)
        tamper = recovery.plant("tamper")
        recovery.tamper(tamper)
        drift = recovery.plant("drift")
        recovery.drift(drift)

    with test.phase("restart that worker's template-manager and wait for recovery to finish"):
        report = recovery.restart_and_wait_for_recovery()

    with test.phase("a row with intact bytes is confirmed without hashing"):
        recovery.expect_confirmed(report, happy)

    with test.phase("a row whose bytes are gone is pruned"):
        recovery.expect_pruned(report, stale)

    with test.phase("row-less bytes are adopted when the artifact hashes to the directory name"):
        recovery.expect_registered(report, orphan)

    with test.phase("row-less bytes whose hash disagrees are removed, not adopted"):
        # The hash check's own words — the two manifest self-consistency checks
        # that run before it also say "does not match digest dir name".
        recovery.expect_rejected(report, tamper, reason_contains='bootable artifact sha256 "')

    with test.phase("bytes whose size disagrees with the manifest are removed despite the row"):
        recovery.expect_rejected(report, drift, reason_contains='file "rootfs.ext4" has size')

    with test.phase("the sandbox on the other worker is untouched"):
        observed = test.run_command_when_ready("printf %s space-smoke-recovery-alive")
        test.expect_command(observed, stdout="space-smoke-recovery-alive")
