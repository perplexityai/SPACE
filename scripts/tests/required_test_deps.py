"""Python tests' side of the test-dependency rule.

The same rule as ci/scripts/lib/test-deps.sh's space_test_deps_required and
pplx_space_db_test_fixtures::require_test_deps: a dependency is required
unless SPACE_TEST_DEPS_OPTIONAL is non-empty and not 0, SPACE_CI_REQUIRE_TEST_DEPS
non-empty and not 0 overrides that opt-out, and SPACE_CI_REQUIRE_TEST_DEPS set
to empty or 0 keeps its older meaning, optional.
"""

from __future__ import annotations

import os
import unittest
from typing import NoReturn


def _on(name: str) -> bool:
    return os.environ.get(name) not in (None, "", "0")


def unavailable_dependency(dependency: str, detail: str) -> NoReturn:
    """Fail the calling test because `dependency` is unavailable, or, when the
    run opted out, skip it with a SPACE-TEST-DEP-SKIP: marker.

    Raises unittest's exceptions, which pytest also reports as a failure or a
    skip, so it serves both kinds of test.
    """
    if _on("SPACE_CI_REQUIRE_TEST_DEPS"):
        raise AssertionError(
            f"test dependency {dependency} is unavailable: {detail}. Test dependencies "
            "are required because SPACE_CI_REQUIRE_TEST_DEPS is set, which overrides "
            "SPACE_TEST_DEPS_OPTIONAL."
        )
    if _on("SPACE_TEST_DEPS_OPTIONAL"):
        opted_out_by = "SPACE_TEST_DEPS_OPTIONAL is set"
    elif "SPACE_CI_REQUIRE_TEST_DEPS" in os.environ:
        opted_out_by = "SPACE_CI_REQUIRE_TEST_DEPS is empty or 0"
    else:
        raise AssertionError(
            f"test dependency {dependency} is unavailable: {detail}. Test dependencies "
            "are required by default; to skip the tests that need it instead, set "
            "SPACE_TEST_DEPS_OPTIONAL=1."
        )
    raise unittest.SkipTest(
        f"SPACE-TEST-DEP-SKIP: {dependency} unavailable ({opted_out_by}): {detail}"
    )
