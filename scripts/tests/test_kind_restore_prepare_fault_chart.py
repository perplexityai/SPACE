#!/usr/bin/env python3
"""Guards for the kind sandbox-manager chart's restore-prepare fault hook.

sandbox-manager reads `SPACE_SBXMGR_TEST_RESTORE_PREPARE_FAULT_DIR` only in
`test-hooks` builds (`service/restore_prepare_fault.rs`). The smoke scenario
`local.restore_rollback_late_retry_keeps_original_snapshot` arms the hook
through this directory, so a worker render without it would make the
scenario fail on configuration rather than on the behavior it guards.

These cases check the chart's half of the contract: workers get the hook,
builders and an emptied value do not, and the directory stays under the
container's private /tmp so that a marker never outlives its pod.
"""

from __future__ import annotations

import unittest
from pathlib import PurePosixPath

from kind_chart_render import (
    REPO_ROOT,
    SANDBOX_MANAGER_CHART,
    env_by_name,
    object_named,
    render,
)

RELEASE = "space"
HOOK_ENV = "SPACE_SBXMGR_TEST_RESTORE_PREPARE_FAULT_DIR"


def sandbox_manager_env(*extra: str) -> dict[str, dict]:
    documents = render(SANDBOX_MANAGER_CHART, RELEASE, *extra)
    return env_by_name(object_named(documents, "DaemonSet", f"{RELEASE}-sandbox-manager"))


class RestorePrepareFaultChartTests(unittest.TestCase):
    def test_a_worker_render_points_the_hook_at_private_tmp(self) -> None:
        env = sandbox_manager_env()
        self.assertIn(HOOK_ENV, env)
        directory = PurePosixPath(env[HOOK_ENV]["value"])
        self.assertTrue(directory.is_absolute())
        self.assertEqual(directory.parts[:2], ("/", "tmp"))

    def test_a_builder_render_has_no_hook(self) -> None:
        env = sandbox_manager_env(
            "-f", str(SANDBOX_MANAGER_CHART / "values-builder.yaml"),
            "--set", "node.role=builder",
        )
        self.assertNotIn(HOOK_ENV, env)

    def test_an_empty_directory_renders_no_hook(self) -> None:
        env = sandbox_manager_env("--set", "testHooks.restorePrepareFaultDir=")
        self.assertNotIn(HOOK_ENV, env)

    def test_only_the_kind_chart_names_the_hook(self) -> None:
        """No chart or values file outside infra/space/kind may set the hook."""
        offenders = []
        for path in (REPO_ROOT / "infra").rglob("*"):
            if not path.is_file() or path.suffix not in {".yaml", ".yml", ".tpl", ".json"}:
                continue
            relative = path.relative_to(REPO_ROOT)
            if relative.parts[:3] == ("infra", "space", "kind"):
                continue
            if HOOK_ENV in path.read_text(errors="ignore"):
                offenders.append(str(relative))
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
