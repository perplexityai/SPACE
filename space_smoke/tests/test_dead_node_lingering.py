"""`DeadNodeAssertions.expect_nothing_lingers` reads every sandbox it promises to.

The bench fails it only when a row really lingers, which a healthy run never
shows; this pins which rows it reads.
"""

from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest import mock

from space_smoke.dead_node import STARTED, SUSPENDED_REMOTE, CreatedSandbox, DeadNodeAssertions

NODE_ID = "0b4f5c1e-7d2a-4c9b-9f31-2a6e8d0c5b17"
WRITTEN_OFF = "sbx-written-off"
KEPT = "sbx-kept"
REPLACEMENT = "sbx-on-the-replacement"


class _Verified(AssertionError):
    pass


def _assertions(quarantined: set[str]) -> tuple[DeadNodeAssertions, mock.Mock]:
    """Assertions over a dead worker whose control plane holds each asked-for
    sandbox's row, `quarantined` among them in that lifecycle."""

    def verify(condition: bool, message: str, **evidence: object) -> None:
        if not condition:
            raise _Verified(message, evidence)

    fixture = mock.Mock(verify=verify, sandbox=SimpleNamespace(sandbox_id="sbx-fixture"))
    control = mock.Mock()
    control.sandbox_worker_nodes.return_value = []
    control.other_live_rows_on_node.return_value = []
    control.sandbox_row_states.side_effect = lambda ids: [
        SimpleNamespace(sandbox_id=i, lifecycle="quarantined" if i in quarantined else "failed")
        for i in ids
    ]
    assertions = DeadNodeAssertions(fixture, control, mock.Mock())
    assertions._node = SimpleNamespace(id=NODE_ID, k8s_node_name="space-local-worker2")
    assertions._sandboxes = [
        CreatedSandbox(WRITTEN_OFF, STARTED),
        CreatedSandbox(KEPT, SUSPENDED_REMOTE),
    ]
    assertions._replacement_sandboxes = [REPLACEMENT]
    return assertions, control


class NothingLingers(unittest.TestCase):
    def test_it_reads_the_replacements_control_plane_row_too(self) -> None:
        assertions, control = _assertions(quarantined=set())

        assertions.expect_nothing_lingers(kept=[KEPT])

        control.sandbox_row_states.assert_called_once_with([WRITTEN_OFF, KEPT, REPLACEMENT])

    def test_a_quarantined_replacement_row_lingers(self) -> None:
        assertions, _ = _assertions(quarantined={REPLACEMENT})

        with self.assertRaises(_Verified) as raised:
            assertions.expect_nothing_lingers(kept=[KEPT])

        self.assertEqual(raised.exception.args[1]["quarantined"], [REPLACEMENT])


if __name__ == "__main__":
    unittest.main()
