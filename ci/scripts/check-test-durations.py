#!/usr/bin/env python3
"""Report the tests of a nextest run over the time limit, and fail the slow ones.

Reads the JUnit report nextest writes (`[profile.default.junit]` in
.config/nextest.toml) and ci/test-duration-budget.toml. A test should take up
to the budget's `limit_seconds`; each one over it is reported as a warning.
The run fails when a test takes over `fail_seconds`, or, for a test listed
under `[exceptions]`, over its own ceiling. The gap between the two absorbs
CI's run-to-run variance: on a loaded runner a test's slowest run is often
twice its median, so failing at the limit itself would fail pull requests
for code they did not touch. Make a listed test faster and delete its entry;
the report names the listed tests this run found under the limit.

Usage:
  check-test-durations.py JUNIT_XML BUDGET_TOML

Exits 1 when any test exceeds what the budget allows, 2 on unreadable input.
"""

from __future__ import annotations

import sys
import tomllib
import xml.etree.ElementTree as ET

# The elements nextest writes inside a retried test case, one per failed or
# errored attempt; the test case's own `time` is only its last attempt's.
RETRIED_ATTEMPTS = ("flakyFailure", "flakyError", "rerunFailure", "rerunError")


def durations(junit: str) -> dict[str, float]:
    """`<binary id> <test name>` to the seconds of its slowest attempt, for
    every test case in the report."""

    times = {}
    for suite in ET.parse(junit).getroot().iter("testsuite"):
        binary = suite.get("name", "")
        # nextest reports its setup scripts (`[scripts.setup]`) as suites too.
        if binary.startswith("@setup-script:"):
            continue
        for case in suite.iter("testcase"):
            attempts = [case] + [
                attempt for tag in RETRIED_ATTEMPTS for attempt in case.iter(tag)
            ]
            times[f"{binary} {case.get('name', '')}"] = max(
                float(attempt.get("time") or 0) for attempt in attempts
            )
    return times


def violations(
    times: dict[str, float],
    limit: float,
    fail: float,
    exceptions: dict[str, float],
) -> tuple[list[str], list[str], list[str]]:
    """The tests that fail the run, the unlisted ones over the limit but not
    failing it, and the listed tests now under the limit."""

    failing, slow, removable = [], [], []
    for test, seconds in sorted(times.items()):
        ceiling = exceptions.get(test)
        if ceiling is None and seconds > fail:
            failing.append(
                f"{test}: {seconds:.1f} s, over the {fail:g} s failure bound"
            )
        elif ceiling is None and seconds > limit:
            slow.append(f"{test}: {seconds:.1f} s, over the {limit:g} s limit")
        elif ceiling is not None and seconds > ceiling:
            failing.append(f"{test}: {seconds:.1f} s, over its {ceiling:g} s exception")
        elif ceiling is not None and seconds <= limit:
            removable.append(test)
    return failing, slow, removable


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(__doc__.split("Usage:")[1].split("Exits")[0].rstrip(), file=sys.stderr)
        return 2
    try:
        times = durations(argv[1])
        with open(argv[2], "rb") as budget_file:
            budget = tomllib.load(budget_file)
        limit = float(budget["limit_seconds"])
        fail = float(budget["fail_seconds"])
        if fail < limit:
            raise ValueError(f"fail_seconds {fail:g} is below limit_seconds {limit:g}")
        exceptions = {
            test: float(ceiling)
            for test, ceiling in budget.get("exceptions", {}).items()
        }
        loose = [test for test, ceiling in exceptions.items() if ceiling <= fail]
        if loose:
            raise ValueError(
                f"{len(loose)} exceptions are at or below fail_seconds and can go,"
                f" the first {loose[0]}"
            )
    except (
        OSError,
        ET.ParseError,
        tomllib.TOMLDecodeError,
        KeyError,
        ValueError,
    ) as error:
        print(f"cannot check test durations: {error}", file=sys.stderr)
        return 2
    failing, slow, removable = violations(times, limit, fail, exceptions)
    print(
        f"test durations: {len(times)} tests, limit {limit:g} s, failing over "
        f"{fail:g} s, {len(exceptions)} exceptions in {argv[2]}"
    )
    if slow:
        print(
            f"{len(slow)} tests ran over the {limit:g} s limit. They do not fail"
            " this run; make them faster:"
        )
        for line in slow:
            # A GitHub Actions annotation, also shown on the run's summary page.
            print(f"::warning title=Slow test::{line}")
    if removable:
        print(
            f"{len(removable)} listed tests ran under the limit this time; once one"
            " stays there, delete its exception. The first of them:"
        )
        for test in removable[:20]:
            print(f"  {test}")
    if failing:
        print(
            f"{len(failing)} tests exceeded their budget. Make them faster; a new"
            " slow test does not get an exception.",
            file=sys.stderr,
        )
        for line in failing:
            print(f"  {line}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
