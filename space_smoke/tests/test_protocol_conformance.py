"""Every control Protocol the kind wiring hands a scenario is implemented by the class it is cast from.

`services.py` wires `fixture._x_control = cast(Any, kind)` and
`fixture._x_probe = cast(Any, ecw)`; the cast hides a missing method until the
scenario reaches for it on a live cluster (batch 14 bench round 2: a method
declared on the control Protocol lived on the probe class instead). This pins,
off-bench, that every method a Protocol declares exists on the concrete class
the wiring uses for it.
"""

from __future__ import annotations

import inspect
import unittest

from space_smoke import adapters, capabilities

# (protocol name, implementing class); protocols added by later batches are
# included when present so the list needs no edit per branch.
PAIRS: tuple[tuple[str, type], ...] = tuple(
    (name, impl)
    for name, impl in (
        ("RateLimitProbe", adapters.EcwSdk),
        ("TemplateBuilder", adapters.EcwSdk),
        ("RateLimiterControl", adapters.KindHarness),
        ("NodeGroupControl", adapters.KindHarness),
        ("NodeDrainControl", adapters.KindHarness),
        ("PendingBackupControl", adapters.KindHarness),
        ("NodeSelectorExtrasControl", adapters.KindHarness),
        ("NodeFenceControl", adapters.KindHarness),
        ("OrphanedRestorePodGcControl", adapters.KindHarness),
        ("ReconcileQueueGcControl", adapters.KindHarness),
        ("UnansweredTemplateEvictionControl", adapters.KindHarness),
        ("TemplateBuildControl", adapters.KindHarness),
        ("SandboxNetworkIsolationControl", adapters.KindHarness),
        ("ConcurrentRestoreControl", adapters.KindHarness),
    )
    if hasattr(capabilities, name)
)


def _declared_methods(protocol: type) -> list[str]:
    names: list[str] = []
    for klass in protocol.__mro__:
        if klass is object or klass.__name__ in ("Protocol", "Generic"):
            continue
        for name, value in vars(klass).items():
            if name.startswith("_") or not inspect.isfunction(value):
                continue
            if name not in names:
                names.append(name)
    return names


class ControlProtocolsAreImplemented(unittest.TestCase):
    def test_every_declared_method_exists_on_the_wired_class(self) -> None:
        self.assertGreaterEqual(len(PAIRS), 5)
        missing: list[str] = []
        for name, impl in PAIRS:
            protocol = getattr(capabilities, name)
            for method in _declared_methods(protocol):
                if not callable(getattr(impl, method, None)):
                    missing.append(f"{impl.__name__} lacks {name}.{method}")
        self.assertEqual(missing, [])


if __name__ == "__main__":
    unittest.main()
