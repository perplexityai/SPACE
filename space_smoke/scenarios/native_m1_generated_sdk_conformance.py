"""The SDK generated from the served M1 document drives every M1 operation."""

from __future__ import annotations

import os
import shutil
import tempfile
from contextlib import ExitStack
from pathlib import Path

from space_smoke import m1_conformance as m1
from space_smoke.native import _URL_OPEN
from space_smoke.native_recovery import KindNativeHardeningFixture
from space_smoke.registry import scenario

_OUTPUT_TAIL_CHARACTERS = 4000


# The pinned generator runs in this runner's Docker daemon, which in-cluster
# runners do not have, so the scenario takes the Kind-only native fixture. It
# uses none of that fixture's recovery controls.
@scenario(
    "native.m1_generated_sdk_conformance",
    api_surfaces=("native",),
    required_tools=("curl", "docker", "python3"),
    rerun_environment=("SPACE_M1_GENERATOR_MIRROR", "SPACE_RUNTIME"),
)
def m1_generated_sdk_conformance(test: KindNativeHardeningFixture) -> None:
    """The pinned OpenAPI Generator turns the served /openapi-m1.json into a
    Python SDK that runs all 14 M1 operations against the API and data plane:
    create, read and list, command over SSE with a Last-Event-ID reconnect,
    process control, pause, suspend and wake, delete, and the documented error
    envelopes, including 400 for a malformed sandbox id."""

    config = test.config
    scope = test.context.scope
    work = Path(tempfile.mkdtemp(prefix="space-smoke-m1-"))
    label = m1.new_generator_label()

    def remove_generation() -> None:
        m1.remove_generator_containers(label, strict=True)
        shutil.rmtree(work, ignore_errors=True)

    scope.defer("remove the M1 generator and its output", remove_generation, critical=True)

    with test.context.phase("fetch the served M1 document"):
        spec = work / "openapi-m1.json"
        spec.write_bytes(
            m1.fetch_document(
                config.api_url, timeout=config.request_timeout_seconds, open_url=_URL_OPEN
            )
        )

    # With SPACE_M1_GENERATOR_MIRROR set the script takes the generator from
    # that mirror, and otherwise pulls the pinned Docker Hub index.
    with test.context.phase("generate the M1 SDK"):
        output = work / "generated"
        output.mkdir()
        try:
            generation = m1.generate_sdk(
                spec, output, timeout=m1.GENERATOR_TIMEOUT_SECONDS, label=label
            )
        except m1.GenerationTimedOut as error:
            test.context.record("generator_output", _tail(error.output))
            raise
        test.context.record("generator_exit", generation.returncode)
        test.context.record("generator_output", _tail(generation.output))
        generation.require_sdk()

    # The generated modules stay loaded until the sandbox cleanup below has run.
    modules = ExitStack()
    scope.defer("unload the generated M1 SDK", modules.close)
    sdk = modules.enter_context(m1.generated_sdk(output))
    conformance = m1.M1Conformance(
        sdk,
        api_url=config.api_url,
        data_plane_url=config.data_plane_url,
        token=config.token,
        template=config.template,
        runtime=os.environ.get("SPACE_RUNTIME", m1.DEFAULT_RUNTIME),
        timeout=config.ready_timeout_seconds,
        request_timeout=config.request_timeout_seconds,
        on_sandbox=lambda sandbox_id: test.context.record("sandbox_id", sandbox_id),
    )
    scope.defer("delete the M1 conformance sandbox", conformance.cleanup, critical=True)
    # Compensations run newest first within one cleanup budget: remove the
    # generator's leftovers before a slow sandbox delete can use it up. The
    # loaded SDK needs none of its files.
    scope.defer("remove the M1 generator and its output", remove_generation, critical=True)
    with test.context.phase("drive the 14 M1 operations"):
        try:
            conformance.run()
        finally:
            test.context.record("m1_operations_called", sorted(conformance.called))


def _tail(output: str) -> str:
    return output.strip()[-_OUTPUT_TAIL_CHARACTERS:]
