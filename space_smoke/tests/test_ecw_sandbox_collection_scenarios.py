"""Characterization tests for ECW sandbox collection scenarios."""

from __future__ import annotations

import unittest
from collections.abc import Mapping
from typing import cast
from unittest import mock

from space_smoke.capabilities import (
    EcwSandbox,
    EcwSandboxInfo,
    EcwSandboxPages,
    EcwScenarioSandboxes,
)
from space_smoke.fixtures import CommandSandboxFixture, Ecw
from space_smoke.runtime import InvariantViolation, ResourceScope, ScenarioContext
from space_smoke.scenarios.ecw_sandbox_list_pagination import (
    PAGE_SIZE,
    sandbox_list_pagination,
)


class _Sandbox:
    def __init__(self, sandbox_id: str) -> None:
        self.sandbox_id = sandbox_id
        self.deleted = False

    def kill(self) -> None:
        self.deleted = True

    def get_info(self) -> EcwSandboxInfo:
        return EcwSandboxInfo(self.sandbox_id, "seed")

    def try_get_info(self) -> EcwSandboxInfo | None:
        return None if self.deleted else self.get_info()


class _Sandboxes:
    def __init__(self, primary: _Sandbox) -> None:
        self.sandboxes = [primary]
        self.delete_is_noop = False
        self.pages: EcwSandboxPages | None = None
        self.created_metadata: list[Mapping[str, str]] = []

    @property
    def items(self) -> tuple[_Sandbox, ...]:
        return tuple(self.sandboxes)

    def create(self, metadata: Mapping[str, str] | None = None) -> _Sandbox:
        sandbox = _Sandbox(f"sandbox-{len(self.sandboxes) + 1}")
        self.sandboxes.append(sandbox)
        self.created_metadata.append(dict(metadata or {}))
        return sandbox

    def delete(self, sandbox: _Sandbox) -> int:
        if not self.delete_is_noop:
            sandbox.deleted = True
        return 1

    def list_pages(self, *, limit: int, max_pages: int) -> EcwSandboxPages:
        del limit, max_pages
        if self.pages is not None:
            return self.pages
        infos = [sandbox.get_info() for sandbox in self.sandboxes]
        return EcwSandboxPages(
            pages=(tuple(infos[:PAGE_SIZE]), tuple(infos[PAGE_SIZE:])),
            has_more=False,
        )


def _fixture(count: int = 1) -> tuple[Ecw, _Sandboxes, dict[str, object]]:
    evidence: dict[str, object] = {}
    primary = _Sandbox("sandbox-1")
    sandboxes = _Sandboxes(primary)
    sandboxes.sandboxes.extend(
        _Sandbox(f"sandbox-{ordinal}") for ordinal in range(2, count + 1)
    )
    return (
        Ecw(
            ScenarioContext(ResourceScope(), evidence),
            cast(EcwSandbox, primary),
            cast(EcwScenarioSandboxes, sandboxes),
        ),
        sandboxes,
        evidence,
    )


class EcwSandboxDeleteScenarioTests(unittest.TestCase):
    def test_delete_requires_public_absence(self) -> None:
        test, sandboxes, evidence = _fixture()

        test.delete_sandbox()
        test.wait_until_deleted()

        self.assertTrue(sandboxes.sandboxes[0].deleted)
        self.assertEqual(evidence["deleted_sandbox_id"], "sandbox-1")
        self.assertEqual(evidence["observed_state"], "deleted")

    def test_delete_fails_when_the_sandbox_remains_visible(self) -> None:
        test, sandboxes, _evidence = _fixture()
        sandboxes.delete_is_noop = True

        with (
            mock.patch.object(Ecw, "lifecycle_transition_timeout", 0),
            self.assertRaisesRegex(InvariantViolation, "remained visible"),
        ):
            test.delete_sandbox()
            test.wait_until_deleted()


class EcwSandboxListPaginationScenarioTests(unittest.TestCase):
    def test_list_requires_exact_once_coverage_across_multiple_pages(self) -> None:
        test, sandboxes, _evidence = _fixture(sandbox_list_pagination.sandbox_count)

        sandbox_list_pagination.verify(test)

        self.assertIs(sandbox_list_pagination.fixture_type, CommandSandboxFixture)
        self.assertEqual(sandbox_list_pagination.api_surfaces, {"ecw", "native"})
        self.assertEqual(len(sandboxes.sandboxes), 3)
        self.assertEqual(sandboxes.created_metadata, [])
        self.assertEqual(_evidence["requested_page_size"], PAGE_SIZE)

    def test_list_rejects_duplicate_sandbox_ids(self) -> None:
        test, sandboxes, _evidence = _fixture(sandbox_list_pagination.sandbox_count)
        duplicate = EcwSandboxInfo("sandbox-1", "seed")
        sandboxes.pages = EcwSandboxPages(
            pages=((duplicate, duplicate), (EcwSandboxInfo("sandbox-3", "seed"),)),
            has_more=False,
        )

        with self.assertRaisesRegex(InvariantViolation, "duplicate run-owned sandbox"):
            sandbox_list_pagination.verify(test)

    def test_list_rejects_unexpected_sandbox_ids(self) -> None:
        test, sandboxes, _evidence = _fixture(sandbox_list_pagination.sandbox_count)
        sandboxes.pages = EcwSandboxPages(
            pages=(
                (
                    EcwSandboxInfo("sandbox-1", "seed"),
                    EcwSandboxInfo("sandbox-2", "seed"),
                ),
                (
                    EcwSandboxInfo("sandbox-3", "seed"),
                    EcwSandboxInfo("sandbox-unexpected", "seed"),
                ),
            ),
            has_more=False,
        )

        with self.assertRaisesRegex(InvariantViolation, "exact run-owned set"):
            sandbox_list_pagination.verify(test)

    def test_list_rejects_duplicate_unexpected_sandbox_ids(self) -> None:
        test, sandboxes, _evidence = _fixture(sandbox_list_pagination.sandbox_count)
        unexpected = EcwSandboxInfo("sandbox-unexpected", "seed")
        sandboxes.pages = EcwSandboxPages(
            pages=(
                (
                    EcwSandboxInfo("sandbox-1", "seed"),
                    EcwSandboxInfo("sandbox-2", "seed"),
                ),
                (EcwSandboxInfo("sandbox-3", "seed"), unexpected, unexpected),
            ),
            has_more=False,
        )

        with self.assertRaisesRegex(InvariantViolation, "duplicate sandbox"):
            sandbox_list_pagination.verify(test)

    def test_list_rejects_missing_sandboxes(self) -> None:
        test, sandboxes, _evidence = _fixture(sandbox_list_pagination.sandbox_count)
        sandboxes.pages = EcwSandboxPages(
            pages=(
                (EcwSandboxInfo("sandbox-1", "seed"),),
                (EcwSandboxInfo("sandbox-2", "seed"),),
            ),
            has_more=False,
        )

        with self.assertRaisesRegex(InvariantViolation, "every run-owned sandbox"):
            sandbox_list_pagination.verify(test)

    def test_list_rejects_a_page_cap_before_exhaustion(self) -> None:
        test, sandboxes, _evidence = _fixture(sandbox_list_pagination.sandbox_count)
        sandboxes.pages = EcwSandboxPages(
            pages=(
                (
                    EcwSandboxInfo("sandbox-1", "seed"),
                    EcwSandboxInfo("sandbox-2", "seed"),
                ),
                (EcwSandboxInfo("sandbox-3", "seed"),),
            ),
            has_more=True,
        )

        with self.assertRaisesRegex(InvariantViolation, "exceeded its page cap"):
            sandbox_list_pagination.verify(test)


if __name__ == "__main__":
    unittest.main()
