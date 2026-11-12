#!/usr/bin/env python3
"""Reject malformed known-egress chart values before they reach the pod.

The runtime behavior is covered in `network_runtime/tests.rs`; these cases
exercise chart validation of wrong types, including falsy values that Helm
otherwise treats as absent.
"""

from __future__ import annotations

import subprocess
import unittest

from kind_chart_render import SANDBOX_MANAGER_CHART, render

RELEASE = "space"


def refusal(*extra: str) -> str:
    """The stderr of a render the chart is expected to fail."""
    try:
        render(SANDBOX_MANAGER_CHART, RELEASE, *extra)
    except subprocess.CalledProcessError as error:
        return error.stderr
    raise AssertionError(f"render with {extra} succeeded; the chart should refuse it")


class KnownEgressRefusalTests(unittest.TestCase):
    def test_a_non_string_domain_list_is_refused(self) -> None:
        """Only a comma-separated string is accepted.

        sandbox-manager parses the variable as one string, so a YAML list would
        reach it as the text `[a b]` and be rejected whole — diagnostics off
        while the values file looks configured. The falsy shapes get their own
        cases because `with` treats them as empty, so a guard written inside
        the `with` block would skip them and render nothing.
        """
        for shape, override in (
            ("a YAML list", "knownEgress.domains[0]=a.example.test"),
            ("a number", "knownEgress.domains=1234"),
            ("zero", "knownEgress.domains=0"),
            ("true", "knownEgress.domains=true"),
            ("false", "knownEgress.domains=false"),
        ):
            with self.subTest(shape=shape):
                self.assertIn(
                    "knownEgress.domains must be a comma-separated string",
                    refusal("--set", override),
                )

    def test_a_non_map_known_egress_is_refused(self) -> None:
        """Falsy non-maps are refused rather than read as unset.

        `default`, `with` and `if` all treat false, 0 and "" as empty, so any
        of them would hand these the chart default and render the master on,
        the reverse of what the operator wrote.
        """
        for shape, args in (
            ("a string", ("--set", "knownEgress=oops")),
            ("false", ("--set", "knownEgress=false")),
            ("zero", ("--set", "knownEgress=0")),
            ("an empty string", ("--set-string", "knownEgress=")),
        ):
            with self.subTest(shape=shape):
                self.assertIn("knownEgress must be a map", refusal(*args))


if __name__ == "__main__":
    unittest.main()
