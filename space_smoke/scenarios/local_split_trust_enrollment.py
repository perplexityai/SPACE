"""The one enrollment flag never changes an existing sandbox's trust mode."""

import space_smoke as smoke
from space_smoke.capabilities import KindColdCreateConfig
from space_smoke.scenarios.local_split_trust_lifecycle import ANCHOR, _anchor


@smoke.scenario(
    "local.split_trust_enrollment",
    ignored="Requires the owned real-KMS Kind fixture and local SDK flag provider",
    sandbox=smoke.SandboxConfig(
        timeout=1500, kind_cold_create=KindColdCreateConfig(vcpu=2, memory_mib=1024),
    ),
)
def enrollment_is_sticky(test: smoke.KindEcw) -> None:
    """New initialization obeys one flag; restores and forks preserve their producer's trust mode."""
    original = _anchor(test, test.sandbox)
    with test.sandbox_anchor_authority.enrollment(False):
        with test.phase("flag off leaves new sandboxes on the legacy path"):
            legacy = test.sandboxes.create()
            test.sandbox_anchor_authority.set_interception(legacy.sandbox_id, enabled=False)
            result = legacy.run(
                f"test ! -e {ANCHOR} && test ! -e /var/lib/space/sandbox-trust-id && "
                "curl --fail --silent --show-error --noproxy '*' --max-time 20 "
                "https://example.com/ -o /dev/null && printf legacy"
            )
            test.verify(result.exit_code == 0 and result.stdout == "legacy", "flag-off sandbox enrolled or broke direct HTTPS")
            test.record("new_legacy_sandbox", legacy.sandbox_id)
        with test.phase("an enrolled sandbox restores without consulting the now-false flag"):
            test.pause()
            test.reconnect_by_id()
            test.verify(_anchor(test, test.sandbox) == original, "flag-off restore changed enrolled trust")
        with test.phase("a fork inherits enrollment but gets its own identity-specific anchor"):
            child = test.sandboxes.fork(test.sandbox).sandbox
            test.verify(_anchor(test, child) != original, "flag-off fork retained its parent's key")
            test.record("scoped_child_while_flag_off", child.sandbox_id)
    with test.phase("flag on does not silently enroll a previously legacy snapshot"):
        legacy.pause()
        legacy = legacy.connect()
        result = legacy.run(
            f"test ! -e {ANCHOR} && test ! -e /var/lib/space/sandbox-trust-id && "
            "curl --fail --silent --show-error --noproxy '*' --max-time 20 "
            "https://example.com/ -o /dev/null && printf legacy"
        )
        test.verify(result.exit_code == 0 and result.stdout == "legacy", "flag-on restore changed legacy trust")
    with test.phase("new creation enrolls again after restoring the same single flag"):
        fresh = test.sandboxes.create()
        _anchor(test, fresh)
        test.record("new_enrolled_sandbox", fresh.sandbox_id)
    test.record("feature_flag", "space-sandbox-scoped-trust")
    test.record("scoped_restore_and_legacy_restore_preserved_their_modes", True)
