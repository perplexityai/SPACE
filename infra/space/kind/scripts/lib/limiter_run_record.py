"""Decisions the kind Redis scripts make from the rate-limiter smoke's state.

The scenario (`space_smoke/rate_limiters.py`) writes a run record on the public
API Deployment, annotation `space.pplx.net/smoke-limiter-run`: the pre-smoke
limiter env and whether the run owns the Redis store. It holds the
`space-smoke-public-api-*` Lease with a holder identity starting `rate-limiter;`,
which is how a running rate-limiter smoke is told from another smoke on that
Lease (node groups, node-selector extras). This file holds the decisions so the
two shell scripts share one reading of both. It reads JSON files and prints a
plan; the scripts run kubectl.

    lease-name <kube-context>
        the public-API mutation Lease name, as `acquire_public_api_mutation`
        derives it.
    fence <lease.json> <holder> <lease-name> <namespace>
        how `remove-redis-store.sh` may delete, one line:
          aside <lease> <rate-limiter|other>
                             a live holder has the Lease: leave the store. A
                             rate-limiter smoke may be using it; any other
                             holder could hand the Lease to one before the
                             delete lands.
          take <lease-json>  the Lease is free or expired: take it for the
                             delete. An empty <lease.json> means it doesn't
                             exist yet; the JSON then has no resourceVersion
                             and is created rather than replaced.
    release <lease.json> <holder>
        the Lease JSON handing it back, or nothing when <holder> no longer holds it.
    claim <deployments.json>
        "<deployment> <resourceVersion> <record>" with redis_created flipped to
        false when a record claims the store for a smoke run; nothing otherwise.

A record that doesn't parse is reported on stderr and treated as absent, so a
hand-edited annotation can't fail a bring-up.
"""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone

ANNOTATION = "space.pplx.net/smoke-limiter-run"
# Mirrors `acquire_public_api_mutation(purpose="rate-limiter")` in space_smoke/adapters.py.
RATE_LIMITER_HOLDER_PREFIX = "rate-limiter;"
FENCE_SECONDS = 60


def _stamp(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _load(path: str) -> dict:
    with open(path) as handle:
        text = handle.read().strip()
    return json.loads(text) if text else {}


def records(deployments: dict) -> list[tuple[dict, dict]]:
    found = []
    for deploy in deployments.get("items", []):
        raw = (deploy["metadata"].get("annotations") or {}).get(ANNOTATION)
        if raw is None:
            continue
        try:
            record = json.loads(raw)
        except json.JSONDecodeError:
            record = None
        if not isinstance(record, dict):
            print(f"ignoring an unreadable {ANNOTATION} on {deploy['metadata']['name']}", file=sys.stderr)
            continue
        found.append((deploy, record))
    return found


def _live(lease: dict, now: datetime) -> bool:
    spec = lease.get("spec") or {}
    stamp = spec.get("renewTime") or spec.get("acquireTime")
    if not spec.get("holderIdentity") or not stamp:
        return False
    renewed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    return renewed + timedelta(seconds=spec.get("leaseDurationSeconds") or 0) > now


def fence(lease: dict, holder: str, name: str, namespace: str) -> str:
    now = datetime.now(timezone.utc)
    if lease and _live(lease, now):
        kind = "rate-limiter" if lease["spec"]["holderIdentity"].startswith(RATE_LIMITER_HOLDER_PREFIX) else "other"
        return f"aside {lease['metadata']['name']} {kind}"
    taken = (
        dict(lease)
        if lease
        else {
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {"name": name, "namespace": namespace},
        }
    )
    taken["spec"] = {
        **(lease.get("spec") or {}),
        "holderIdentity": holder,
        "leaseDurationSeconds": FENCE_SECONDS,
        "acquireTime": _stamp(now),
        "renewTime": _stamp(now),
    }
    return "take " + json.dumps(taken, separators=(",", ":"))


def release(lease: dict, holder: str) -> str:
    if (lease.get("spec") or {}).get("holderIdentity") != holder:
        return ""
    released = dict(lease)
    released["spec"] = {**lease["spec"], "holderIdentity": ""}
    return json.dumps(released, separators=(",", ":"))


def main(argv: list[str]) -> None:
    command = argv[1]
    if command == "lease-name":
        print("space-smoke-public-api-" + hashlib.sha256(argv[2].encode()).hexdigest()[:16])
    elif command == "fence":
        print(fence(_load(argv[2]), argv[3], argv[4], argv[5]))
    elif command == "release":
        plan = release(_load(argv[2]), argv[3])
        if plan:
            print(plan)
    elif command == "claim":
        for deploy, record in records(_load(argv[2])):
            if record.get("redis_created") is True:
                record["redis_created"] = False
                print(
                    deploy["metadata"]["name"],
                    deploy["metadata"]["resourceVersion"],
                    json.dumps(record, sort_keys=True, separators=(",", ":")),
                )
    else:
        raise SystemExit(f"unknown command: {command}")


if __name__ == "__main__":
    main(sys.argv)
