#!/usr/bin/env python3
"""Compare captured service statement multisets for synthetic scenarios."""

import argparse
import json
from pathlib import Path


def load(path: Path) -> tuple[int, dict[str, dict[str, int]]]:
    data = json.loads(path.read_text())
    if data["schema"] != 1:
        raise ValueError(f"{path}: unsupported statement report schema")
    scenarios = {}
    for scenario in data["scenarios"]:
        name = scenario["name"]
        if name in scenarios:
            raise ValueError(f"{path}: duplicate scenario {name}")
        statements = scenario["statements"]
        if not statements or any(
            not isinstance(query, str)
            or type(calls) is not int
            or calls <= 0
            for query, calls in statements.items()
        ):
            raise ValueError(f"{path}: invalid multiset for {name}")
        scenarios[name] = statements
    if not scenarios:
        raise ValueError(f"{path}: no scenarios")
    return data["seed"], scenarios


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base", type=Path)
    parser.add_argument("head", type=Path)
    args = parser.parse_args()
    base_seed, base = load(args.base)
    head_seed, head = load(args.head)
    if base_seed != head_seed or base.keys() != head.keys():
        raise ValueError("seed or scenario set differs")
    differences = []
    for name in base:
        for query in sorted(base[name].keys() | head[name].keys()):
            before, after = base[name].get(query, 0), head[name].get(query, 0)
            if before != after:
                differences.append(
                    {"scenario": name, "query": query, "base": before, "head": after}
                )
    print(json.dumps({"equal": not differences, "differences": differences}, indent=2))
    if differences:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
