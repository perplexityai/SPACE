#!/usr/bin/env python3
"""Fail a nextest run whose tests got slower than their budget.

Reads the JUnit report nextest writes (`[profile.default.junit]` in
.config/nextest.toml) and ci/test-duration-budget.toml. A test may take up to
the budget's `limit_seconds`; a test listed under `[exceptions]` may take up to
its own ceiling instead. Exceptions are the tests that were already slower
when the limit started, each capped at 1.5 times its slowest time across
recent CI runs, so a listed test cannot get slower and an unlisted one cannot
cross the limit. Make a listed test faster and delete its entry; the report
names the listed tests this run found under the limit.

Usage:
  check-test-durations.py JUNIT_XML BUDGET_TOML

Exits 1 when any test exceeds what the budget allows, 2 on unreadable input.
"""

from __future__ import annotations

import sys
import tomllib
import xml.etree.ElementTree as ET


def durations(junit: str) -> dict[str, float]:
    """`<binary id> <test name>` to seconds, for every test case in the report."""

    times = {}
    for suite in ET.parse(junit).getroot().iter("testsuite"):
        binary = suite.get("name", "")
        # nextest reports its setup scripts (`[scripts.setup]`) as suites too.
        if binary.startswith("@setup-script:"):
            continue
        for case in suite.iter("testcase"):
            times[f"{binary} {case.get('name', '')}"] = float(case.get("time") or 0)
    return times


def violations(
    times: dict[str, float], limit: float, exceptions: dict[str, float]
) -> tuple[list[str], list[str]]:
    """The tests over their budget, and the listed tests now under the limit."""

    over, removable = [], []
    for test, seconds in sorted(times.items()):
        ceiling = exceptions.get(test)
        if ceiling is None and seconds > limit:
            over.append(f"{test}: {seconds:.1f} s, over the {limit:g} s limit")
        elif ceiling is not None and seconds > ceiling:
            over.append(f"{test}: {seconds:.1f} s, over its {ceiling:g} s exception")
        elif ceiling is not None and seconds <= limit:
            removable.append(test)
    return over, removable


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(__doc__.split("Usage:")[1].split("Exits")[0].rstrip(), file=sys.stderr)
        return 2
    try:
        times = durations(argv[1])
        with open(argv[2], "rb") as budget_file:
            budget = tomllib.load(budget_file)
        limit = float(budget["limit_seconds"])
        exceptions = {
            test: float(ceiling)
            for test, ceiling in budget.get("exceptions", {}).items()
        }
    except (
        OSError,
        ET.ParseError,
        tomllib.TOMLDecodeError,
        KeyError,
        ValueError,
    ) as error:
        print(f"cannot check test durations: {error}", file=sys.stderr)
        return 2
    over, removable = violations(times, limit, exceptions)
    print(
        f"test durations: {len(times)} tests, limit {limit:g} s, "
        f"{len(exceptions)} exceptions in {argv[2]}"
    )
    if removable:
        print(
            f"{len(removable)} listed tests ran under the limit this time; once one"
            " stays there, delete its exception. The first of them:"
        )
        for test in removable[:20]:
            print(f"  {test}")
    if over:
        print(
            f"{len(over)} tests exceeded their budget. Make them faster; a new"
            " slow test does not get an exception.",
            file=sys.stderr,
        )
        for line in over:
            print(f"  {line}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
