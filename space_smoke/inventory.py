"""Refresh the built-in scenario index from its declarations."""

from __future__ import annotations

import argparse
import inspect
from pathlib import Path

from space_smoke.scenarios import REGISTRY

START = "<!-- BEGIN GENERATED SCENARIO INDEX -->"
END = "<!-- END GENERATED SCENARIO INDEX -->"
README = Path(__file__).with_name("scenarios") / "README.md"


def _cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


def render_index() -> str:
    rows = [
        START,
        "",
        "| Scenario | Tier | Required environment | Claim summary |",
        "|---|---|---|---|",
    ]
    for scenario in REGISTRY:
        # Caller-supplied packs are not part of the built-in documentation.
        if not scenario.source_module.startswith("space_smoke.scenarios."):
            continue
        source = scenario.source_module.rsplit(".", 1)[-1] + ".py"
        environment = (
            f"`{scenario.fixture_name}`; "
            f"{', '.join(sorted(scenario.backends))}; "
            f"{', '.join(sorted(scenario.substrates))}"
        )
        # The opening paragraph states the claim; the linked body retains
        # longer explanations and operational details.
        docstring = inspect.getdoc(scenario.verify) or scenario.invariant
        claim = _cell(" ".join(docstring.split("\n\n", 1)[0].split()))
        if scenario.manual_reason is not None:
            claim += "<br>Manual: " + _cell(scenario.manual_reason)
        if scenario.quarantined is not None:
            claim += "<br>Quarantined: " + _cell(scenario.quarantined)
        rows.append(
            f"| [{scenario.id}]({source}) | {scenario.tier} | {environment} | {claim} |"
        )
    return "\n".join((*rows, "", END))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="refuse a stale index without writing")
    args = parser.parse_args()
    current = README.read_text()
    before, separator, remaining = current.partition(START)
    _, ending, after = remaining.partition(END)
    if not separator or not ending or START in remaining or END in after:
        parser.error("README must contain exactly one scenario-index marker pair")
    updated = before + render_index() + after
    if args.check:
        if updated != current:
            print("Scenario index is stale; run python -m space_smoke.inventory")
            return 1
    else:
        README.write_text(updated)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
