"""The data plane refuses a Native file write over its Native upload admission with the Native 429."""

import http.client
import json
import select
import time
from urllib.parse import quote, urlsplit

from space_smoke.native_files import data
from space_smoke.native_recovery import RECOVERY_ENVIRONMENT, KindNativeHardeningFixture
from space_smoke.registry import scenario

# How long a refused upload may take to be answered; an admitted one is
# answered only once its body ends.
REFUSAL_SECONDS = 5.0


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
    api_surfaces=("native",),
    required_tools=("kubectl", "kind"),
    rerun_environment=RECOVERY_ENVIRONMENT,
)
def file_upload_admission_429(test: KindNativeHardeningFixture) -> None:
    """With the data plane's Native upload admission on at one write per sandbox
    and no wait, writes held open to one sandbox on more connections than
    there are data-plane replicas leave at least one refused before its body
    is read, with 429 `rate_limited`, `retryable` and a `Retry-After` of 1 to
    5 seconds; every admitted write lands once its body ends; and the data
    plane's standing admission settings are put back after."""
    control = test.recovery
    with control.owned_faults():
        sandbox_id = test.create_ready()
        replicas = control.data_plane_replicas()
        path = f"/v1/sandboxes/{quote(sandbox_id, safe='')}/files:write"
        with control.one_upload_per_sandbox():
            # Closed however the scenario ends: an open upload would keep the
            # data-plane pod it reached serving while the env is put back.
            uploads: list[http.client.HTTPConnection] = []
            try:
                with test.context.phase(f"hold {replicas + 1} writes to the sandbox open at once"):
                    bodies = []
                    for index in range(replicas + 1):
                        body = json.dumps({
                            "path": f"/tmp/{test.new_key('held')}-{index}",
                            "data_base64": data(b"held write"),
                        }).encode()
                        bodies.append(body)
                        uploads.append(_held_upload(
                            test.config.data_plane_url, test.config.token, path,
                            test.new_key(f"held-{index}"), body, len(body) // 2,
                        ))
                    # Refusals arrive before the body; admitted writes wait for it.
                    deadline = time.monotonic() + REFUSAL_SECONDS
                    answered: set[int] = set()
                    while time.monotonic() < deadline and len(answered) < len(uploads):
                        sockets = {upload.sock: index for index, upload in enumerate(uploads) if index not in answered}
                        readable, _, _ = select.select(list(sockets), [], [], 0.2)
                        answered.update(sockets[sock] for sock in readable)
                with test.context.phase("read the refusals, then finish the admitted writes"):
                    refused = []
                    for index in sorted(answered):
                        response = uploads[index].getresponse()
                        refused.append({
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
            "no write was refused while more were held than there are replicas",
            replicas=replicas, admitted=admitted,
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
