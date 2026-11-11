"""ci/scripts/check-test-durations.py holds nextest's JUnit times to the budget."""

import pathlib
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ci/scripts/check-test-durations.py"

JUNIT = """<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="nextest-run" tests="5">
  <testsuite name="pplx-space-api" tests="4">
    <testcase name="fast::test" classname="pplx-space-api" time="0.4"/>
    <testcase name="listed::test" classname="pplx-space-api" time="{listed}"/>
    <testcase name="flaky::test" classname="pplx-space-api" time="0.1">
      <flakyFailure time="{flaky}" type="test failure">attempt 1 failed</flakyFailure>
    </testcase>
    <testcase name="errored::test" classname="pplx-space-api" time="0.1">
      <flakyError time="{errored}" type="test timeout">attempt 1 timed out</flakyError>
    </testcase>
  </testsuite>
  <testsuite name="@setup-script:postgres" tests="1">
    <testcase name="postgres" classname="@setup-script:postgres" time="9.0"/>
  </testsuite>
  <testsuite name="pplx-space-archcheck::repository" tests="1">
    <testcase name="new_test" classname="pplx-space-archcheck::repository" time="{new}"/>
  </testsuite>
</testsuites>
"""

BUDGET = """limit_seconds = 2.0
fail_seconds = 4.0
[exceptions]
"pplx-space-api listed::test" = 9
"""


def check(tmp_path, *, listed, new, flaky=0.2, errored=0.2, budget=BUDGET):
    junit = tmp_path / "junit.xml"
    junit.write_text(JUNIT.format(listed=listed, new=new, flaky=flaky, errored=errored))
    toml = tmp_path / "budget.toml"
    toml.write_text(budget)
    return subprocess.run(
        [sys.executable, "-I", str(SCRIPT), str(junit), str(toml)],
        capture_output=True,
        text=True,
        check=False,
    )


def test_every_test_within_its_budget_passes(tmp_path):
    # The 9 s setup script is not a test.
    result = check(tmp_path, listed=8.5, new=1.9)
    assert result.returncode == 0, result.stderr
    assert "::warning" not in result.stdout


def test_an_unlisted_test_over_the_limit_warns_without_failing(tmp_path):
    result = check(tmp_path, listed=8.5, new=3.9)
    assert result.returncode == 0, result.stderr
    assert (
        "::warning title=Slow test::pplx-space-archcheck::repository new_test:"
        " 3.9 s, over the 2 s limit" in result.stdout
    )


def test_an_unlisted_test_over_the_failure_bound_fails(tmp_path):
    result = check(tmp_path, listed=8.5, new=4.1)
    assert result.returncode == 1
    assert (
        "pplx-space-archcheck::repository new_test: 4.1 s, over the 4 s failure bound"
        in result.stderr
    )


def test_a_retried_test_is_held_to_its_slowest_attempt(tmp_path):
    # nextest records the passing retry as the test case's time and each
    # failed attempt in a flakyFailure element.
    result = check(tmp_path, listed=8.5, new=0.1, flaky=8.0)
    assert result.returncode == 1
    assert (
        "pplx-space-api flaky::test: 8.0 s, over the 4 s failure bound" in result.stderr
    )


def test_a_retried_test_that_errored_is_held_to_its_slowest_attempt(tmp_path):
    result = check(tmp_path, listed=8.5, new=0.1, errored=8.0)
    assert result.returncode == 1
    assert (
        "pplx-space-api errored::test: 8.0 s, over the 4 s failure bound"
        in result.stderr
    )


def test_a_listed_test_over_its_ceiling_fails(tmp_path):
    result = check(tmp_path, listed=9.6, new=0.1)
    assert result.returncode == 1
    assert "pplx-space-api listed::test: 9.6 s, over its 9 s exception" in result.stderr


def test_a_listed_test_under_the_limit_is_named_for_removal(tmp_path):
    result = check(tmp_path, listed=1.2, new=0.1)
    assert result.returncode == 0
    assert "  pplx-space-api listed::test" in result.stdout


@pytest.mark.parametrize(
    "budget",
    [
        "[exceptions]\n",
        "limit_seconds = 'two'\nfail_seconds = 4.0\n",
        "limit_seconds =",
        "limit_seconds = 2.0\n",
        "limit_seconds = 2.0\nfail_seconds = 1.0\n",
        # An exception at or below the failure bound changes nothing.
        'limit_seconds = 2.0\nfail_seconds = 4.0\n[exceptions]\n"a b" = 4\n',
    ],
)
def test_an_unreadable_budget_exits_2(tmp_path, budget):
    assert check(tmp_path, listed=1, new=1, budget=budget).returncode == 2


def test_a_missing_report_exits_2(tmp_path):
    toml = tmp_path / "budget.toml"
    toml.write_text(BUDGET)
    result = subprocess.run(
        [sys.executable, "-I", str(SCRIPT), str(tmp_path / "absent.xml"), str(toml)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
