"""Observe attachment usability after an idempotent command retry."""
from space_smoke.registry import scenario
from space_smoke.native_hardening import NativeHardeningFixture

@scenario(
    "native_contract.command_replay_credentials",
    api_surfaces=("native",),
    ignored=(
        "Overlapping credential validity is not an agreed API guarantee; "
        "decision: docs/native-api-hardening.md#credential-replay-contract-deferred"
    ),
)
def command_replay_credentials(test: NativeHardeningFixture) -> None:
    """A command retry preserves its session and reports any credential invalidation."""
    sid = test.create_ready()
    key = test.new_key("command")
    first = test.command(sid, ["true"], idempotency_key=key)
    replay = test.command(sid, ["true"], idempotency_key=key)
    test.context.verify(first.status == replay.status == 201, "command creation failed")
    test.context.verify(first.body["session_id"] == replay.body["session_id"], "command retry created a second session")
    old = test.events(first.body, timeout=10)
    new = test.events(replay.body, timeout=10)
    test.context.verify(new.status == 200, "replayed command credential was unusable", replay_status=new.status)
    test.context.record("credential_replay", {"original_status": old.status, "replay_status": new.status})
    test.context.verify(old.status == 200, "unexpected original credential response", original_status=old.status)
