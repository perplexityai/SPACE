"""Native API command idempotency coverage."""

import time
import uuid
from collections.abc import Mapping
from dataclasses import asdict
from urllib.parse import quote

from space_smoke.native_concurrency import (
    ENVIRONMENT,
    CallerTrace,
    ConcurrencySettings,
    command_output,
    new_key,
    request_with_retries,
    required_string,
    run_command,
    run_fanout,
)
from space_smoke.native_hardening import NativeHardeningFixture
from space_smoke.registry import scenario


@scenario(
    "native_concurrency.command_idempotency",
    api_surfaces=("native",),
    rerun_environment=ENVIRONMENT,
)
def command_idempotency(test: NativeHardeningFixture) -> None:
    """Concurrent identical command requests share identity and execute their side effect once."""
    settings = ConcurrencySettings.load(test)
    test.context.record("concurrency_settings", asdict(settings))
    sandbox_id = test.create_ready()
    key = new_key("command-replay")
    marker = f"/tmp/native-hardening-{uuid.uuid4().hex}"
    body = {"command": ["sh", "-c", f"printf 'once\\n' >> {marker}; printf done"], "timeout": "60s"}
    path = f"/v1/sandboxes/{quote(sandbox_id, safe='')}/commands"

    def submit(index: int, trace: CallerTrace, deadline: float) -> Mapping[str, object]:
        response = request_with_retries(test, settings, trace, deadline, "command", "POST", path,
                            body=body, key=key, expected=201)
        return {"session_id": required_string(response, "session_id"), "process_id": required_string(response, "process_id")}

    outcomes = run_fanout(test, settings, "command_replay_fanout", submit)
    identities = {(item.value["session_id"], item.value["process_id"]) for item in outcomes if item.value}
    test.context.verify(len(identities) == 1, "same-key command retries produced multiple sessions or processes")
    # Fetch a fresh credential after concurrent POSTs drain; this case proves
    # execution count. native_contract.command_replay_credentials covers the
    # validity of credentials returned by earlier replays.
    response = test.request("POST", path, body=body, idempotency_key=key)
    test.context.verify(response.status == 201, "final command replay failed")
    test.context.verify((response.body.get("session_id"), response.body.get("process_id")) in identities,
                        "final command replay changed identity")
    stream = test.events(response.body)
    test.context.verify(stream.status == 200, "fresh command credential did not attach")
    stdout, stderr = command_output(stream.events, limit=1024)
    test.context.verify(stdout == b"done" and stderr == b"", "replayed command output was incorrect")
    trace = CallerTrace(0)
    observed, errors = run_command(test, settings, trace, time.monotonic() + settings.batch_seconds,
                               sandbox_id, f"wc -l < {marker}")
    test.context.record("execution_count", observed.decode().strip())
    test.context.record("credential_scope", "fresh credential after replay; overlapping credential validity is not asserted")
    test.context.verify(observed.strip() == b"1" and not errors, "idempotent command executed more than once")
