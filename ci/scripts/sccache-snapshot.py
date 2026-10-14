#!/usr/bin/env python3
"""Decide when main refreshes a Rust compiler-cache snapshot, and delete old ones.

Each Rust job restores one local sccache directory from the GitHub Actions
cache. Main saves a new snapshot, under a key unique to its run, when the
restored snapshot was built for other dependency inputs or when the job missed
at least a threshold of Rust compilations against it. Pull requests and the
merge queue restore the newest snapshot and never save.

  decide  log what main does with the job's snapshot, and write `save` and
          `trim` step outputs; every event logs it, only main acts on it
  prune   delete every snapshot on a ref except the newest of each job family;
          Cargo target-directory snapshots (ci/scripts/rust-target-cache.sh)
          keep the newest of each recent build-inputs hash instead
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Mapping

# Keys are <family>-<dependency inputs hash>-<run id>-<run attempt>, where the
# family names one job. Keys saved before run-unique keys end at the hash.
KEY_PREFIX = "space-sccache-v1-"
SNAPSHOT_KEY = re.compile(
    rf"^(?P<family>{re.escape(KEY_PREFIX)}.+)-[0-9a-f]{{64}}(?:-[0-9]+-[0-9]+)?$"
)
# Target directories: <family>-<build inputs hash>-<Cargo.lock hash>-<run id>-<run
# attempt>, both hashes cut to 16 characters. Restores cross Cargo.lock
# changes but never build inputs, so only the newest of each build-inputs hash
# is ever read. A pull request stacked on another branch builds main's older
# inputs until it rebases, so the newest of each of the last few is kept.
TARGET_KEY_PREFIX = "space-target-v2-"
TARGET_KEY = re.compile(
    rf"^(?P<family>{re.escape(TARGET_KEY_PREFIX)}.+)-(?P<inputs>[0-9a-f]{{16}})-[0-9a-f]{{16}}-[0-9]+-[0-9]+$"
)
TARGET_INPUTS_KEPT = 3


@dataclass(frozen=True)
class Decision:
    save: bool
    # Delete the entries this run neither read nor wrote before saving.
    trim: bool
    reason: str


def rust_counts(stats_json: str | None) -> tuple[int, int] | None:
    """Return (hits, misses) for Rust from `sccache --show-stats --stats-format=json`."""
    if not stats_json:
        return None
    try:
        stats = json.loads(stats_json)["stats"]
        hits = stats["cache_hits"]["counts"].get("Rust", 0)
        misses = stats["cache_misses"]["counts"].get("Rust", 0)
    except (ValueError, KeyError, TypeError, AttributeError):
        return None
    if not isinstance(hits, int) or not isinstance(misses, int):
        return None
    return hits, misses


def parse_threshold(value: str | None) -> int | None:
    try:
        threshold = int(value or "")
    except ValueError:
        return None
    return threshold if threshold >= 0 else None


def decide(
    restored_key: str,
    inputs_prefix: str,
    stats_json: str | None,
    refresh_misses: int | None,
) -> Decision:
    counts = rust_counts(stats_json)
    # sccache stamps every entry it reads or writes with the current time, so
    # entries older than the run are ones this commit did not use. Only trust
    # that when the server reported statistics and saw Rust compilations;
    # otherwise trimming could empty the snapshot.
    trim = counts is not None and sum(counts) > 0
    if not restored_key:
        return Decision(True, trim, "no snapshot was restored")
    if not restored_key.startswith(inputs_prefix):
        return Decision(True, trim, f"restored {restored_key}, built for other dependency inputs")
    if counts is None:
        return Decision(True, False, "sccache statistics are missing or unreadable")
    if refresh_misses is None:
        return Decision(True, trim, "no valid refresh threshold is configured")
    misses = counts[1]
    if misses >= refresh_misses:
        return Decision(
            True, trim, f"{misses} Rust misses reach the refresh threshold of {refresh_misses}"
        )
    return Decision(
        False, False, f"{misses} Rust misses are below the refresh threshold of {refresh_misses}"
    )


def created_at(cache: Mapping) -> datetime:
    return datetime.fromisoformat(cache["created_at"].replace("Z", "+00:00"))


def superseded(caches: Iterable[Mapping]) -> list[Mapping]:
    """Return the snapshots to delete.

    That is all but the most recently created compiler cache of each family,
    and all but the most recently created target directory of each of a
    family's TARGET_INPUTS_KEPT most recent build-inputs hashes.
    """
    newest: set[tuple[str, str]] = set()
    kept_inputs: dict[str, int] = {}
    stale = []
    for cache in sorted(caches, key=lambda c: (created_at(c), c["id"]), reverse=True):
        if match := SNAPSHOT_KEY.match(cache["key"]):
            family, inputs = match["family"], None
        elif match := TARGET_KEY.match(cache["key"]):
            family, inputs = match["family"], match["inputs"]
        else:
            continue
        group = (family, inputs or "")
        if group in newest:
            stale.append(cache)
            continue
        if inputs is not None:
            if kept_inputs.get(family, 0) == TARGET_INPUTS_KEPT:
                stale.append(cache)
                continue
            kept_inputs[family] = kept_inputs.get(family, 0) + 1
        newest.add(group)
    return stale


def github(method: str, path: str, token: str) -> dict | None:
    api = os.environ.get("GITHUB_API_URL", "https://api.github.com")
    request = urllib.request.Request(
        f"{api}{path}",
        method=method,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        body = response.read()
    return json.loads(body) if body else None


def list_snapshots(repository: str, ref: str, token: str) -> list[dict]:
    caches: list[dict] = []
    for prefix in (KEY_PREFIX, TARGET_KEY_PREFIX):
        page = 1
        while True:
            query = urllib.parse.urlencode(
                {"key": prefix, "ref": ref, "per_page": 100, "page": page}
            )
            batch = github("GET", f"/repos/{repository}/actions/caches?{query}", token)[
                "actions_caches"
            ]
            caches.extend(batch)
            if len(batch) < 100:
                break
            page += 1
    return caches


def run_decide(args: argparse.Namespace) -> int:
    try:
        with open(args.stats, encoding="utf-8") as stats_file:
            stats_json = stats_file.read()
    except OSError:
        stats_json = None
    decision = decide(
        args.restored_key, args.inputs_prefix, stats_json, parse_threshold(args.refresh_misses)
    )
    main_push = (
        os.environ.get("GITHUB_EVENT_NAME") == "push"
        and os.environ.get("GITHUB_REF") == "refs/heads/main"
    )
    if decision.save:
        action = f"save the Rust compiler cache as {args.key}"
        if decision.trim:
            action += ", without the entries this run did not use"
    else:
        action = "keep the restored Rust compiler cache"
    lead = "This push to main will" if main_push else "A push to main would"
    print(f"{lead} {action}: {decision.reason}")
    if not main_push:
        print("Pull requests and the merge queue never save the cache.")
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as output_file:
            output_file.write(f"save={str(decision.save).lower()}\n")
            output_file.write(f"trim={str(decision.trim).lower()}\n")
    return 0


def run_prune(args: argparse.Namespace) -> int:
    token = os.environ["GH_TOKEN"]
    snapshots = list_snapshots(args.repository, args.ref, token)
    stale = superseded(snapshots)
    print(f"{len(snapshots)} Rust compiler-cache snapshots on {args.ref}; {len(stale)} superseded")
    for cache in stale:
        size_gb = cache.get("size_in_bytes", 0) / 1e9
        print(f"Delete {cache['key']} ({size_gb:.2f} GB, created {cache['created_at']})")
        try:
            github("DELETE", f"/repos/{args.repository}/actions/caches/{cache['id']}", token)
        except urllib.error.HTTPError as error:
            # Another main run may have deleted it first.
            if error.code != 404:
                raise
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)

    decide_parser = commands.add_parser("decide")
    decide_parser.add_argument("--key", required=True)
    decide_parser.add_argument("--restored-key", default="")
    decide_parser.add_argument("--inputs-prefix", required=True)
    decide_parser.add_argument("--stats", required=True)
    decide_parser.add_argument("--refresh-misses", default="")
    decide_parser.set_defaults(run=run_decide)

    prune_parser = commands.add_parser("prune")
    prune_parser.add_argument("--repository", required=True)
    prune_parser.add_argument("--ref", required=True)
    prune_parser.set_defaults(run=run_prune)

    args = parser.parse_args(argv)
    return args.run(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
