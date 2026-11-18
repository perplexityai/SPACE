"""The data plane refuses a Native file write over its Native upload admission with the Native 429."""

import dataclasses
import http.client
import json
import select
import time
from urllib.parse import quote, urlsplit

from space_smoke.native_files import data
from space_smoke.native_recovery import RECOVERY_ENVIRONMENT, KindNativeHardeningFixture
from space_smoke.registry import scenario
from space_smoke.runtime import SmokeConfigurationError

# The upload admission's server rounds each reservation up to whole KiB
# (crates/l40_api/data/plane/server/src/upload_admission.rs).
RESERVATION_UNIT_BYTES = 1024
# How long past the admission's wait for a slot a refused upload may take to
# be answered: a write over HTTP/1.1 waits out that wait first. An admitted
# one is answered only once its body ends.
REFUSAL_MARGIN_SECONDS = 3.0


def _held_upload(url: str, token: str, path: str, key: str, body: bytes, split: int):
    """A write whose headers and first `split` bytes are sent, the rest held."""
    target = urlsplit(url)
    connection = http.client.HTTPConnection(target.hostname, target.port, timeout=30)
    try:
        connection.putrequest("POST", path)
        connection.putheader("Authorization", f"Bearer {token}")
        connection.putheader("Content-Type", "application/json")
        connection.putheader("Idempotency-Key", key)
        connection.putheader("Content-Length", str(len(body)))
        connection.endheaders()
        connection.send(body[:split])
    except BaseException:
        connection.close()
        raise
    return connection


@scenario(
    "native_contract.file_upload_admission_429",
    tier="main",
    api_surfaces=("native",),
    required_tools=("kubectl", "kind"),
    rerun_environment=RECOVERY_ENVIRONMENT,
)
def file_upload_admission_429(test: KindNativeHardeningFixture) -> None:
    """With the Kind data plane's standing Native upload admission (by default
    8 writes per sandbox on each replica and a 2 s wait for a slot), more HTTP/1.1
    writes held open to one sandbox at once than its replicas at their
    autoscaler's maximum can admit leave at least one refused before its body
    is read, with 429 `rate_limited`, `retryable` and a `Retry-After` of 1 to
    5 seconds; and every admitted write lands once its body ends. The
    scenario changes no data-plane setting."""
    control = test.recovery
    with test.context.phase("read the data plane's standing upload admission"):
        admission, shape = control.standing_upload_admission()
        test.context.record("upload_admission", dataclasses.asdict(admission))
        test.context.record("data_plane", {"max_replicas": shape.max_replicas, "pods": len(shape.pods)})
        # The hold is sized from what the data plane runs, and every check
        # below then holds for those values or stops the run inconclusive.
        if not admission.enabled:
            raise SmokeConfigurationError("the data plane's Native upload admission is off")
        # One more than every replica the autoscaler may run can admit, so by
        # pigeonhole one replica gets a write past its sandbox's slots, while
        # no replica can get more than the org's or the replica's own slots:
        # the refusal can only be the sandbox's.
        held = shape.max_replicas * admission.slots_per_sandbox + 1
        if held > min(admission.slots_per_org, admission.slots_per_replica):
            raise SmokeConfigurationError(
                f"{held} held writes would reach the org's or the replica's slots, not only the sandbox's"
            )
        bodies = [
            json.dumps({
                "path": f"/tmp/{test.new_key('held')}-{index}",
                "data_base64": data(b"held write"),
            }).encode()
            for index in range(held)
        ]
        # Each write reserves its declared length, in whole KiB, from every
        # byte budget too.
        reserved = -(-max(map(len, bodies)) // RESERVATION_UNIT_BYTES) * RESERVATION_UNIT_BYTES
        if held * reserved > min(
            admission.bytes_per_sandbox, admission.bytes_per_org, admission.bytes_per_replica
        ):
            raise SmokeConfigurationError("the held writes would reach a byte budget, not only the slots")
    sandbox_id = test.create_ready()
    # The settings read above still hold: a data plane that rolled while the
    # sandbox was created stops the run before any write is sent.
    control.require_same_data_plane(shape)
    path = f"/v1/sandboxes/{quote(sandbox_id, safe='')}/files:write"
    # Closed however the scenario ends, so no data-plane pod is left serving one.
    uploads: list[http.client.HTTPConnection] = []
    try:
        with test.context.phase(f"hold {held} writes to the sandbox open at once"):
            opened = []
            for index, body in enumerate(bodies):
                opened.append(time.monotonic())
                uploads.append(_held_upload(
                    test.config.data_plane_url, test.config.token, path,
                    test.new_key(f"held-{index}"), body, len(body) // 2,
                ))
            # Refusals arrive after the wait and before the body; admitted
            # writes wait for it.
            deadline = time.monotonic() + admission.wait_ms / 1000 + REFUSAL_MARGIN_SECONDS
            answered: dict[int, float] = {}
            while time.monotonic() < deadline and len(answered) < len(uploads):
                sockets = {upload.sock: index for index, upload in enumerate(uploads) if index not in answered}
                readable, _, _ = select.select(list(sockets), [], [], 0.2)
                answered.update((sockets[sock], time.monotonic()) for sock in readable)
            # Before any is let go, so a pod still serving a held write counts.
            control.require_same_data_plane(shape)
        with test.context.phase("read the refusals, then finish the admitted writes"):
            refused = []
            for index in sorted(answered):
                response = uploads[index].getresponse()
                refused.append({
                    "seconds": round(answered[index] - opened[index], 2),
                    "status": response.status,
                    "retry_after": response.getheader("Retry-After"),
                    "body": json.loads(response.read() or b"{}"),
                })
            admitted = []
            for index, upload in enumerate(uploads):
                if index in answered:
                    continue
                upload.send(bodies[index][len(bodies[index]) // 2:])
                response = upload.getresponse()
                admitted.append({"status": response.status, "body": json.loads(response.read() or b"{}")})
            test.context.record("refused", refused)
            test.context.record("admitted", admitted)
    finally:
        for upload in uploads:
            upload.close()
    test.context.verify(
        refused,
        "no write was refused while more were held than the replicas could admit",
        held=held, max_replicas=shape.max_replicas, admitted=admitted,
    )
    for answer in refused:
        envelope = answer["body"]
        retry_after = answer["retry_after"]
        test.context.verify(
            answer["status"] == 429
            and envelope.get("code") == "rate_limited"
            and envelope.get("retryable") is True
            and retry_after is not None and retry_after.isdigit() and 1 <= int(retry_after) <= 5,
            "an upload over admission was not refused with the Native 429",
            answer=answer,
        )
    test.context.verify(
        admitted and all(
            answer["status"] == 200 and answer["body"].get("bytes_written") == len(b"held write")
            for answer in admitted
        ),
        "an admitted write did not land once its body ended",
        admitted=admitted,
    )
