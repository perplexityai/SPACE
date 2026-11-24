"""With drain v2 on, every v1 drain is refused and writes nothing."""

from typing import cast

import space_smoke as smoke
from space_smoke.capabilities import DurableLifecycleSandbox
from space_smoke.drain_v2 import CAP_REFUSAL_CODE_FIELD, V1_REFUSAL_CODE, V1_REFUSAL_STATUS


# That the admin route refuses a v1 `:drain` only with the switch on, a repeat
# of a v1 drain in flight included, and still undrains one, is the route's
# test (`nodes/drain_handler_tests.rs`). This is the seam on a real worker
# of a bench whose values switched drain v2 on: v1 and v2 drains never run
# together, so each shape of a v1 `:drain` is refused, and the worker and its
# resident are left exactly as they were.
@smoke.scenario(
    "native.v2_refuses_v1_drains",
    drain_v2=True,
    api_surfaces=("native",),
    substrates=("kubernetes",),
)
def v2_refuses_v1_drains(test: smoke.CommandSandboxFixture) -> None:
    """With drain v2 on, a `:drain` of the fixture's worker with an empty
    body, with `mode: forced`, and naming `engine: v1` is each a 409 with the
    refusal's stable code; after each the worker has no drain phase, no v2
    drain status and no cordon, and its resident still runs commands."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin the fixture's worker"):
            drain.pin_to_resident_worker()

        with test.phase("each v1 :drain is refused, writing nothing"):
            for body in ({}, {"mode": "forced"}, {"engine": "v1"}):
                refused = drain.drain_answer(body)
                test.verify(
                    refused.status == V1_REFUSAL_STATUS
                    and refused.body.get(CAP_REFUSAL_CODE_FIELD) == V1_REFUSAL_CODE,
                    "a v1 :drain with drain v2 on was not the refusal's 409",
                    request=body,
                    status=refused.status,
                    body=refused.body,
                )
                phase = drain.drain_phase()
                test.verify(
                    phase is None and drain.status() == {} and not drain.unschedulable(),
                    "a refused v1 :drain left a drain, or a cordon, on its worker",
                    request=body,
                    drain_phase=phase,
                    status=drain.status(),
                    unschedulable=drain.unschedulable(),
                )

        with test.phase("the worker's resident still serves"):
            served = drain.run_on(cast(DurableLifecycleSandbox, test.sandbox), "echo served")
            test.verify(served.exit_code == 0, "the resident did not run a command")
