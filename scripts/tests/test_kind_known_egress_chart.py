#!/usr/bin/env python3
"""Guards for the kind sandbox-manager chart's known-egress block.

sandbox-manager reads `SPACE_NET_GATEWAY_KNOWN_EGRESS_DOMAINS` to decide
which failed DNS or TCP connections net-gateway reports. The Rust side is
covered in `network_runtime/tests.rs`; these cases cover the chart's half of
the contract: what reaches the pod and which values files the chart refuses
rather than silently misrendering.

They assert the rendered DaemonSet env rather than that Helm echoes a value
back, and none of them skips when the block is absent — a missing block has
to fail here, not pass vacuously.
"""

from __future__ import annotations

import subprocess
import unittest

import yaml

from kind_chart_render import SANDBOX_MANAGER_CHART, env_by_name, object_named, render

RELEASE = "space"
DOMAINS_ENV = "SPACE_NET_GATEWAY_KNOWN_EGRESS_DOMAINS"


def sandbox_manager_env(*extra: str) -> dict[str, dict]:
    documents = render(SANDBOX_MANAGER_CHART, RELEASE, *extra)
    return env_by_name(object_named(documents, "DaemonSet", f"{RELEASE}-sandbox-manager"))


def refusal(*extra: str) -> str:
    """The stderr of a render the chart is expected to fail."""
    try:
        render(SANDBOX_MANAGER_CHART, RELEASE, *extra)
    except subprocess.CalledProcessError as error:
        return error.stderr
    raise AssertionError(f"render with {extra} succeeded; the chart should refuse it")


class KnownEgressDefaultsTests(unittest.TestCase):
    def test_the_shipped_domain_list_is_empty(self) -> None:
        """The chart ships no domains, and nothing renders a list by default.

        This is a boundary, not an unfinished default: a list here would ship
        one deployment's hostnames to every kind bench and every public
        consumer of the chart. Filling it in has to fail this test.
        """
        values = yaml.safe_load((SANDBOX_MANAGER_CHART / "values.yaml").read_text())
        self.assertEqual(values["knownEgress"]["domains"], "")
        self.assertNotIn(DOMAINS_ENV, sandbox_manager_env())

    def test_a_nulled_map_takes_the_default_rather_than_failing(self) -> None:
        """`knownEgress: null` is "unset", not a misconfiguration.

        Helm's coalescing deletes the key, so this is the one case that reaches
        the template's own fallback.
        """
        env = sandbox_manager_env("--set", "knownEgress=null")
        self.assertNotIn(DOMAINS_ENV, env)


class KnownEgressRenderingTests(unittest.TestCase):
    def test_master_on_with_a_local_list_renders_both(self) -> None:
        env = sandbox_manager_env(
            "--set", r"knownEgress.domains=a.example.test\,b.example.test"
        )
        self.assertEqual(env[DOMAINS_ENV]["value"], "a.example.test,b.example.test")

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
