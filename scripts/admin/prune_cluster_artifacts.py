#!/usr/bin/env python3
"""Manual SPACE cluster artifact pruning.

This operator-facing script uses the Kubernetes/Postgres bastion path, writes
CSV audit manifests, and refuses mutation unless both --live and --run-id are
present.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import re
import subprocess
import sys
import urllib.parse
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, TypeVar


STALE_EXECUTION_POD_PHASES = frozenset({"Succeeded", "Failed", "Pending"})
TERMINAL_POD_PHASES = frozenset({"Succeeded", "Failed"})
DEFAULT_SPACE_NAMESPACE = "space"
DEFAULT_SANDBOX_NAMESPACE = "sandboxes"
DEFAULT_POSTGRES_BASTION = "deploy/space-postgres-bastion"
SANDBOX_MANAGER_LABEL = "app.kubernetes.io/component=sandbox-manager"
SANDBOX_ID_METADATA_KEY = "space.pplx.net/sandbox-id"
SBXMGR_BIN = "/opt/space/bin/space-sbxmgr"
T = TypeVar("T")


@dataclass(frozen=True)
class PodInfo:
    name: str
    phase: str
    node_name: str
    created_at: str
    labels: dict[str, str] = field(default_factory=dict)
    annotations: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ManagerPod:
    name: str
    node_name: str
    phase: str


@dataclass(frozen=True)
class OldGenerationCandidate:
    sandbox_id: str
    generation: int
    pod_name: str
    state: str
    active: str
    created_at: str
    updated_at: str
    live_pod_phase: str
    decision: str


@dataclass(frozen=True)
class SandboxCandidate:
    sandbox_id: str
    lifecycle: str
    created_at: str
    updated_at: str
    active_node_id: str
    target_node_id: str
    active_k8s_node_name: str
    target_k8s_node_name: str
    execution_rows: str
    execution_pod_names: str
    snapshot_rows: str
    attached_storage_locations: str
    decision: str
    decision_reason: str

    def pod_names(self) -> list[str]:
        if not self.execution_pod_names:
            return []
        return [name for name in self.execution_pod_names.split(",") if name]

    def cleanup_node_name(self) -> str:
        return self.target_k8s_node_name or self.active_k8s_node_name


@dataclass(frozen=True)
class SnapshotStorageRow:
    sandbox_id: str
    snapshot_id: str
    snapshot_storage_location_id: str
    storage_connection_string: str
    has_encrypted_fields: str
    sweepability_decision: str
    sweepability_reason: str


@dataclass(frozen=True)
class SnapshotGenerationCandidate:
    sandbox_id: str
    sandbox_updated_at: str
    snapshot_id: str
    snapshot_timestamp: str
    local_artifact_id: str
    artifact_kind: str
    current_local_artifact_id: str
    current_local_artifact_kind: str
    ready_full_snapshot_count: str
    newest_rank: str
    artifact_partition_rank: str
    is_current_artifact: str
    has_current_artifact: str
    has_creating_snapshot: str
    storage_location_count: str
    decision: str
    decision_reason: str


@dataclass(frozen=True)
class TemplateCandidate:
    template_release_id: str
    template_name: str
    runtime: str
    content_hash: str
    created_at: str
    newest_rank: str
    storage_location_count: str
    storage_uris: str
    has_encrypted_storage: str
    decision: str
    decision_reason: str

    def uri_list(self) -> list[str]:
        if not self.storage_uris:
            return []
        return [uri for uri in self.storage_uris.split("|") if uri]


@dataclass(frozen=True)
class TemplateStorageTarget:
    template_release_id: str
    template_name: str
    content_hash: str
    storage_uris: str

    def uri_list(self) -> list[str]:
        if not self.storage_uris:
            return []
        return [uri for uri in self.storage_uris.split("|") if uri]


@dataclass(frozen=True)
class PodReference:
    sandbox_id: str
    generation: str
    active: str


@dataclass(frozen=True)
class StalePodCandidate:
    pod_name: str
    phase: str
    node_name: str
    created_at: str
    decision: str
    decision_reason: str
    current_generation_sandbox_id: str
    current_generation: str
    current_generation_active: str


def eprint(message: str) -> None:
    print(message, file=sys.stderr)


def run(
    cmd: list[str],
    *,
    input_text: str | None = None,
    timeout: int | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        cmd,
        input=input_text,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if check and proc.returncode != 0:
        rendered = " ".join(cmd)
        raise RuntimeError(
            f"command failed ({proc.returncode}): {rendered}\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )
    return proc


def kubectl(args: argparse.Namespace, *parts: str) -> list[str]:
    return ["kubectl", "--context", args.context, *parts]


def psql_script(args: argparse.Namespace, script: str) -> str:
    cmd = kubectl(
        args,
        "-n",
        args.space_namespace,
        "exec",
        "-i",
        args.postgres_bastion,
        "--",
        "psql",
        "-X",
        "-q",
        "-v",
        "ON_ERROR_STOP=1",
    )
    return run(cmd, input_text=script).stdout


def psql_csv_query(args: argparse.Namespace, query: str) -> list[dict[str, str]]:
    script = f"COPY (\n{query}\n) TO STDOUT WITH CSV HEADER;\n"
    return parse_csv(psql_script(args, script))


def parse_csv(raw: str) -> list[dict[str, str]]:
    raw = raw.strip()
    if not raw:
        return []
    reader = csv.DictReader(io.StringIO(raw))
    return [dict(row) for row in reader]


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def parse_uuid(value: str) -> str:
    return str(uuid.UUID(value))


def parse_generation(value: str) -> int:
    generation = int(value)
    if generation < 0:
        raise ValueError(f"negative generation: {value}")
    return generation


def values_uuid(ids: Iterable[str]) -> str:
    rendered = []
    for sandbox_id in ids:
        rendered.append(f"({sql_literal(parse_uuid(sandbox_id))}::uuid)")
    if not rendered:
        raise ValueError("empty UUID values")
    return ",\n".join(rendered)


def insert_uuid_values_sql(table: str, column: str, ids: Iterable[str]) -> str:
    normalized = [parse_uuid(id_value) for id_value in ids]
    if not normalized:
        return ""
    return f"""
INSERT INTO {table} ({column})
VALUES
{values_uuid(normalized)};
"""


def values_uuid_generation(rows: Iterable[OldGenerationCandidate]) -> str:
    rendered = []
    for row in rows:
        sandbox_id = parse_uuid(row.sandbox_id)
        generation = parse_generation(str(row.generation))
        rendered.append(f"({sql_literal(sandbox_id)}::uuid, {generation}::bigint)")
    if not rendered:
        raise ValueError("empty generation values")
    return ",\n".join(rendered)


def parse_timestamp(value: str) -> dt.datetime:
    normalized = value.replace("Z", "+00:00")
    match = re.match(
        r"^(?P<prefix>.*?)(?:\.(?P<fraction>\d+))?(?P<offset>[+-]\d{2})(?::?(?P<offset_minutes>\d{2}))?$",
        normalized,
    )
    if match:
        fraction = match.group("fraction")
        if fraction is not None:
            fraction = f".{fraction[:6].ljust(6, '0')}"
        else:
            fraction = ""
        normalized = (
            f"{match.group('prefix')}{fraction}"
            f"{match.group('offset')}:{match.group('offset_minutes') or '00'}"
        )
    parsed = dt.datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urllib.parse.urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError("not an s3:// URI")
    key = parsed.path.lstrip("/")
    if not key:
        raise ValueError("refusing bucket-root S3 URI")
    return parsed.netloc, key


def parse_snapshot_sweeper_uri(uri: str) -> tuple[str, str]:
    rest = uri.removeprefix("s3://")
    if rest == uri:
        raise ValueError("not an s3:// URI")
    if not rest:
        raise ValueError("empty s3:// URI")

    path, separator, query = rest.partition("?")
    if "/" in path:
        bucket, key_prefix = path.split("/", 1)
        key_prefix = key_prefix.rstrip("/")
    else:
        bucket = path
        key_prefix = ""
    if not bucket:
        raise ValueError("s3:// URI is missing bucket")

    region = None
    if separator:
        for pair in query.split("&"):
            if not pair:
                continue
            if "=" not in pair:
                raise ValueError("malformed query param")
            key, value = pair.split("=", 1)
            if key == "region":
                region = value
            elif key == "endpoint":
                continue
            elif key == "path_style":
                if value not in {"true", "false"}:
                    raise ValueError("invalid path_style")
            else:
                raise ValueError("unsupported query param")
    if region is None:
        raise ValueError("missing region")
    return bucket, key_prefix


def canonical_s3_prefix_uri(uri: str) -> str:
    bucket, key = parse_s3_uri(uri)
    return f"s3://{bucket}/{key}"


def s3_uri_within_prefix(uri: str, allowed_prefix: str) -> bool:
    bucket, key = parse_s3_uri(uri)
    allowed_bucket, allowed_key = parse_s3_uri(allowed_prefix)
    if bucket != allowed_bucket:
        return False
    allowed_key = allowed_key.rstrip("/")
    return key == allowed_key or key.startswith(f"{allowed_key}/")


def template_allowed_prefixes(args: argparse.Namespace) -> list[str]:
    prefixes = list(args.template_s3_allowed_prefix or [])
    for file_name in args.template_s3_allowed_prefix_file or []:
        for line in Path(file_name).read_text().splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                prefixes.append(stripped)
    return prefixes


def validate_template_storage_uris(
    args: argparse.Namespace,
    uris: list[str],
) -> tuple[bool, str]:
    if not uris:
        return True, ""
    allowed_prefixes = template_allowed_prefixes(args)
    if args.live and not allowed_prefixes:
        return False, "missing_template_s3_allowed_prefix"
    for prefix in allowed_prefixes:
        try:
            parse_s3_uri(prefix)
        except ValueError as err:
            return False, f"invalid_allowed_prefix:{err}"
    for uri in uris:
        try:
            parse_s3_uri(uri)
        except ValueError as err:
            return False, f"invalid_storage_uri:{err}"
        if allowed_prefixes and not any(s3_uri_within_prefix(uri, prefix) for prefix in allowed_prefixes):
            return False, "storage_uri_outside_allowed_prefix"
    return True, ""


SNAPSHOT_STORAGE_FIELDNAMES = [
    "sandbox_id",
    "snapshot_id",
    "snapshot_storage_location_id",
    "storage_connection_string",
    "has_encrypted_fields",
    "sweepability_decision",
    "sweepability_reason",
]


def load_cluster_controller_env_value(args: argparse.Namespace, name: str) -> str:
    proc = run(
        kubectl(
            args,
            "-n",
            args.space_namespace,
            "get",
            "deploy",
            "space-cluster-controller",
            "-o",
            "json",
        )
    )
    data = json.loads(proc.stdout)
    containers = (
        data.get("spec", {})
        .get("template", {})
        .get("spec", {})
        .get("containers", [])
    )
    for container in containers:
        for env in container.get("env", []) or []:
            if env.get("name") == name:
                return str(env.get("value") or "").strip()
    return ""


def snapshot_s3_bucket(args: argparse.Namespace) -> str:
    configured = str(args.snapshot_s3_bucket or "").strip()
    if configured:
        return configured
    return load_cluster_controller_env_value(args, "SPACE_SNAPSHOT_BUCKET")


def classify_snapshot_storage_rows(
    rows: list[dict[str, str]],
    scoped_bucket: str,
) -> list[SnapshotStorageRow]:
    classified: list[SnapshotStorageRow] = []
    for row in rows:
        decision = "sweepable"
        reason = ""
        has_encrypted_fields = row.get("has_encrypted_fields", "").lower()
        storage_uri = row.get("storage_connection_string", "")
        if has_encrypted_fields == "true":
            decision = "skip"
            reason = "encrypted_unsupported"
        else:
            try:
                bucket, _key = parse_snapshot_sweeper_uri(storage_uri)
            except ValueError:
                decision = "skip"
                reason = "parse_uri"
            else:
                if not scoped_bucket:
                    decision = "skip"
                    reason = "bucket_scope_missing"
                elif bucket != scoped_bucket:
                    decision = "skip"
                    reason = "bucket_out_of_scope"
        classified.append(
            SnapshotStorageRow(
                sandbox_id=parse_uuid(row["sandbox_id"]),
                snapshot_id=parse_uuid(row["snapshot_id"]),
                snapshot_storage_location_id=parse_uuid(row["snapshot_storage_location_id"]),
                storage_connection_string=storage_uri,
                has_encrypted_fields="true" if has_encrypted_fields == "true" else "false",
                sweepability_decision=decision,
                sweepability_reason=reason,
            )
        )
    return classified


def snapshot_storage_rows_by_sandbox(
    rows: list[SnapshotStorageRow],
) -> dict[str, list[SnapshotStorageRow]]:
    by_sandbox: dict[str, list[SnapshotStorageRow]] = defaultdict(list)
    for row in rows:
        by_sandbox[row.sandbox_id].append(row)
    return by_sandbox


def snapshot_storage_blocking_reasons(
    rows: list[SnapshotStorageRow],
) -> dict[str, str]:
    reasons: dict[str, str] = {}
    for sandbox_id, sandbox_rows in snapshot_storage_rows_by_sandbox(rows).items():
        blocking = sorted(
            {
                row.sweepability_reason
                for row in sandbox_rows
                if row.sweepability_decision != "sweepable"
            }
        )
        if blocking:
            reasons[sandbox_id] = "|".join(blocking)
    return reasons


def filter_snapshot_storage_rows(
    rows: list[SnapshotStorageRow],
    candidates: list[SandboxCandidate],
) -> list[SnapshotStorageRow]:
    sandbox_ids = {candidate.sandbox_id for candidate in candidates}
    return [row for row in rows if row.sandbox_id in sandbox_ids]


def filter_snapshot_generation_storage_rows(
    rows: list[SnapshotStorageRow],
    candidates: list[SnapshotGenerationCandidate],
) -> list[SnapshotStorageRow]:
    snapshot_ids = {candidate.snapshot_id for candidate in candidates}
    return [row for row in rows if row.snapshot_id in snapshot_ids]


def snapshot_storage_blocking_reasons_by_snapshot(
    rows: list[SnapshotStorageRow],
) -> dict[str, str]:
    by_snapshot: dict[str, list[SnapshotStorageRow]] = defaultdict(list)
    for row in rows:
        by_snapshot[row.snapshot_id].append(row)
    reasons: dict[str, str] = {}
    for snapshot_id, snapshot_rows in by_snapshot.items():
        blocking = sorted(
            {
                row.sweepability_reason
                for row in snapshot_rows
                if row.sweepability_decision != "sweepable"
            }
        )
        if blocking:
            reasons[snapshot_id] = "|".join(blocking)
    return reasons


def snapshot_storage_scope_blocking_reasons(
    rows: list[SnapshotStorageRow],
) -> dict[str, str]:
    reasons: dict[str, str] = {}
    for row in rows:
        if row.sweepability_decision != "sweepable":
            continue
        try:
            _bucket, key_prefix = parse_snapshot_sweeper_uri(row.storage_connection_string)
        except ValueError:
            continue
        if key_prefix.rstrip("/").rsplit("/", 1)[-1] != row.snapshot_id:
            reasons[row.snapshot_id] = "storage_uri_not_snapshot_scoped"
    return reasons


def snapshot_is_current_artifact_sql(
    sandbox_alias: str = "s", snapshot_alias: str = "sn"
) -> str:
    """SQL for "this snapshot is its sandbox's tracked current artifact".

    Must stay identical to the Rust
    `sandbox_portability::snapshot_is_current_local_artifact` fragment, which is
    what the snapshot-retention sweeper enforces and what the restore selector
    resolves by. This script is the audit that authorises flipping that sweeper
    to live pruning, so if the two definitions drift the audit clears the flip
    on the wrong evidence.

    Note this covers only the marked-restore class. The sweeper additionally
    shields every row inside a `restoring` sandbox's pre-restore source window
    (the Rust `snapshot_is_live_restore_source` fragment); this script does not
    reproduce that second class.
    """
    return f"""(
    {snapshot_alias}.local_artifact_id IS NOT NULL
    AND {sandbox_alias}.current_local_artifact_id IS NOT NULL
    AND {snapshot_alias}.local_artifact_id = {sandbox_alias}.current_local_artifact_id
    AND {snapshot_alias}.artifact_kind = {sandbox_alias}.current_local_artifact_kind
)"""


def snapshot_generation_keep_predicate_sql(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return f"""(
    {prefix}newest_rank = 1
    OR ({prefix}is_current_artifact AND {prefix}artifact_partition_rank = 1)
    OR (
        NOT {prefix}is_current_artifact
        AND {prefix}artifact_partition_rank <= GREATEST(
            0,
            {prefix}keep_count - CASE WHEN {prefix}has_current_artifact THEN 1 ELSE 0 END
        )
    )
)"""


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def output_dir(args: argparse.Namespace) -> Path:
    if args.output_dir:
        path = Path(args.output_dir)
    else:
        ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        suffix = args.run_id or "dry-run"
        path = Path(f"cluster-artifact-prune-{ts}-{suffix}")
    path.mkdir(parents=True, exist_ok=True)
    return path


def require_live_guard(args: argparse.Namespace) -> None:
    if args.live and not args.run_id:
        raise SystemExit("--live requires --run-id")


def oldest_limit_clause(args: argparse.Namespace) -> str:
    limit = effective_oldest_limit(args)
    if limit <= 0:
        return ""
    return f"\nLIMIT {limit}"


def effective_oldest_limit(args: argparse.Namespace) -> int:
    return args.oldest_limit


def load_live_pods(args: argparse.Namespace) -> dict[str, PodInfo]:
    proc = run(
        kubectl(
            args,
            "-n",
            args.sandbox_namespace,
            "get",
            "pods",
            "-o",
            "json",
        )
    )
    data = json.loads(proc.stdout)
    pods: dict[str, PodInfo] = {}
    for item in data.get("items", []):
        metadata = item.get("metadata", {})
        status = item.get("status", {})
        spec = item.get("spec", {})
        name = metadata.get("name")
        if not name:
            continue
        pods[name] = PodInfo(
            name=name,
            phase=status.get("phase", ""),
            node_name=spec.get("nodeName", ""),
            created_at=metadata.get("creationTimestamp", ""),
            labels=dict(metadata.get("labels") or {}),
            annotations=dict(metadata.get("annotations") or {}),
        )
    return pods


def load_sandbox_manager_pods(args: argparse.Namespace) -> dict[str, ManagerPod]:
    proc = run(
        kubectl(
            args,
            "-n",
            args.space_namespace,
            "get",
            "pods",
            "-l",
            SANDBOX_MANAGER_LABEL,
            "-o",
            "json",
        )
    )
    data = json.loads(proc.stdout)
    managers: dict[str, ManagerPod] = {}
    for item in data.get("items", []):
        metadata = item.get("metadata", {})
        status = item.get("status", {})
        spec = item.get("spec", {})
        name = metadata.get("name")
        node_name = spec.get("nodeName", "")
        if not name or not node_name:
            continue
        managers[node_name] = ManagerPod(
            name=name,
            node_name=node_name,
            phase=status.get("phase", ""),
        )
    return managers


def old_generation_rows(args: argparse.Namespace) -> list[dict[str, str]]:
    query = f"""
WITH latest AS (
    SELECT sandbox_id, MAX(generation) AS latest_generation
    FROM sandbox_k8s_executions
    GROUP BY sandbox_id
)
SELECT e.sandbox_id::text AS sandbox_id,
       e.generation::text AS generation,
       e.pod_name,
       e.state,
       e.active::text AS active,
       e.created_at::text AS created_at,
       e.updated_at::text AS updated_at
FROM sandbox_k8s_executions e
JOIN latest l USING (sandbox_id)
WHERE e.generation < l.latest_generation
  AND e.active = false
ORDER BY e.created_at, e.sandbox_id, e.generation
{oldest_limit_clause(args)}
"""
    return psql_csv_query(args, query)


def classify_old_generations(
    rows: list[dict[str, str]],
    live_pods: dict[str, PodInfo],
) -> list[OldGenerationCandidate]:
    candidates: list[OldGenerationCandidate] = []
    for row in rows:
        pod_name = row["pod_name"]
        live = live_pods.get(pod_name)
        phase = live.phase if live else ""
        if phase == "Running":
            decision = "skip_live_running_pod"
        elif phase and phase not in STALE_EXECUTION_POD_PHASES:
            decision = "skip_live_pod_unknown_phase"
        elif phase:
            decision = "delete_pod_then_db"
        else:
            decision = "delete_db"
        candidates.append(
            OldGenerationCandidate(
                sandbox_id=parse_uuid(row["sandbox_id"]),
                generation=parse_generation(row["generation"]),
                pod_name=pod_name,
                state=row["state"],
                active=row["active"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                live_pod_phase=phase,
                decision=decision,
            )
        )
    return candidates


def delete_pod(args: argparse.Namespace, pod_name: str) -> tuple[bool, str]:
    proc = run(
        kubectl(
            args,
            "-n",
            args.sandbox_namespace,
            "delete",
            "pod",
            pod_name,
            "--ignore-not-found",
            "--wait=false",
        ),
        check=False,
    )
    if proc.returncode == 0:
        return True, proc.stdout.strip()
    return False, proc.stderr.strip()


def delete_old_generation_batch(
    args: argparse.Namespace,
    batch: list[OldGenerationCandidate],
) -> list[dict[str, str]]:
    values = values_uuid_generation(batch)
    script = f"""
BEGIN;
CREATE TEMP TABLE requested_old_generations (
    sandbox_id uuid NOT NULL,
    generation bigint NOT NULL,
    PRIMARY KEY (sandbox_id, generation)
) ON COMMIT DROP;
INSERT INTO requested_old_generations (sandbox_id, generation)
VALUES
{values};
CREATE TEMP TABLE deleted_old_generations (
    sandbox_id text NOT NULL,
    generation bigint NOT NULL,
    pod_name text NOT NULL
) ON COMMIT DROP;
WITH deleted AS (
    DELETE FROM sandbox_k8s_executions e
    USING requested_old_generations r
    WHERE e.sandbox_id = r.sandbox_id
      AND e.generation = r.generation
      AND e.active = false
      AND e.generation < (
          SELECT MAX(generation)
          FROM sandbox_k8s_executions
          WHERE sandbox_id = e.sandbox_id
      )
    RETURNING e.sandbox_id::text, e.generation, e.pod_name
)
INSERT INTO deleted_old_generations
SELECT sandbox_id, generation, pod_name FROM deleted;
COPY (
    SELECT sandbox_id, generation::text AS generation, pod_name
    FROM deleted_old_generations
    ORDER BY sandbox_id, generation
) TO STDOUT WITH CSV HEADER;
COMMIT;
"""
    return parse_csv(psql_script(args, script))


def chunks(values: list[T], size: int) -> Iterable[list[T]]:
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def count_selected(eligible: int, args: argparse.Namespace) -> int:
    limit = effective_oldest_limit(args)
    if limit > 0:
        return min(eligible, limit)
    return eligible


def without_candidate_limit(args: argparse.Namespace) -> argparse.Namespace:
    copied = argparse.Namespace(**vars(args))
    copied.oldest_limit = 0
    copied.candidate_limit = 0
    return copied


def run_old_generations(args: argparse.Namespace) -> int:
    out = output_dir(args)
    live_pods = load_live_pods(args)
    candidates = classify_old_generations(old_generation_rows(args), live_pods)
    candidate_rows = [candidate.__dict__ for candidate in candidates]
    write_csv(
        out / "old_generations_candidates.csv",
        candidate_rows,
        [
            "sandbox_id",
            "generation",
            "pod_name",
            "state",
            "active",
            "created_at",
            "updated_at",
            "live_pod_phase",
            "decision",
        ],
    )

    summary = Counter(candidate.decision for candidate in candidates)
    eligible = [
        candidate
        for candidate in candidates
        if candidate.decision in {"delete_db", "delete_pod_then_db"}
    ]
    selected = eligible[: count_selected(len(eligible), args)]
    eprint(f"old-generation candidates: {dict(summary)}")
    eprint(f"old-generation selected for this run: {len(selected)} of {len(eligible)} eligible")
    eprint(f"audit directory: {out}")
    if not args.live:
        write_json(
            out / "summary.json",
            {
                "mode": "old-executions",
                "dry_run": True,
                "decisions": dict(summary),
                "eligible": len(eligible),
                "selected": len(selected),
                "oldest_limit": effective_oldest_limit(args),
            },
        )
        return 0

    pod_results: list[dict[str, object]] = []
    db_eligible: list[OldGenerationCandidate] = []
    for candidate in selected:
        if candidate.decision == "delete_db":
            db_eligible.append(candidate)
            continue
        if candidate.decision != "delete_pod_then_db":
            continue
        ok, message = delete_pod(args, candidate.pod_name)
        pod_results.append(
            {
                "sandbox_id": candidate.sandbox_id,
                "generation": candidate.generation,
                "pod_name": candidate.pod_name,
                "phase": candidate.live_pod_phase,
                "deleted": ok,
                "message": message,
            }
        )
        if ok:
            db_eligible.append(candidate)
    write_csv(
        out / "old_generations_pod_deletes.csv",
        pod_results,
        ["sandbox_id", "generation", "pod_name", "phase", "deleted", "message"],
    )

    deleted_rows: list[dict[str, str]] = []
    max_batches = args.max_batches or None
    for batch_index, batch in enumerate(chunks(db_eligible, args.batch_size), start=1):
        if max_batches is not None and batch_index > max_batches:
            break
        deleted_rows.extend(delete_old_generation_batch(args, batch))
    write_csv(
        out / "old_generations_db_deletes.csv",
        deleted_rows,
        ["sandbox_id", "generation", "pod_name"],
    )
    write_json(
        out / "summary.json",
        {
            "mode": "old-generations",
            "dry_run": False,
            "decisions": dict(summary),
            "eligible": len(eligible),
            "selected": len(selected),
            "oldest_limit": effective_oldest_limit(args),
            "db_rows_deleted": len(deleted_rows),
            "pod_delete_attempts": len(pod_results),
            "pod_delete_failures": sum(1 for row in pod_results if not row["deleted"]),
        },
    )
    eprint(f"deleted old execution rows: {len(deleted_rows)}")
    return 0


def snapshot_generation_candidate_rows(args: argparse.Namespace) -> list[dict[str, str]]:
    keep_predicate = snapshot_generation_keep_predicate_sql()
    is_current_artifact_sql = snapshot_is_current_artifact_sql()
    query = f"""
WITH ready_full AS (
    SELECT sn.id AS snapshot_id,
           sn.sandbox_id,
           s.updated_at AS sandbox_updated_at,
           sn.timestamp,
           COALESCE(sn.local_artifact_id::text, '') AS local_artifact_id,
           COALESCE(sn.artifact_kind, '') AS artifact_kind,
           COALESCE(s.current_local_artifact_id::text, '') AS current_local_artifact_id,
           COALESCE(s.current_local_artifact_kind, '') AS current_local_artifact_kind,
           COUNT(ssl.id) AS storage_location_count,
           BOOL_AND(
               ssl.storage_connection_string LIKE '%/' || sn.id::text || '?%'
           ) AS storage_uri_snapshot_scoped,
           EXISTS (
               SELECT 1
               FROM snapshots creating
               WHERE creating.sandbox_id = sn.sandbox_id
                 AND creating.state = 'creating'
           ) AS has_creating_snapshot,
           {is_current_artifact_sql} AS is_current_artifact
    FROM snapshots sn
    JOIN sandboxes s ON s.id = sn.sandbox_id
    JOIN snapshot_storage_locations ssl
      ON ssl.snapshot_id = sn.id
     AND ssl.orphaned_at IS NULL
    WHERE sn.state = 'ready'
      AND sn.type = 'full'
      AND NOT EXISTS (
          SELECT 1
          FROM sandbox_k8s_executions e
          WHERE e.sandbox_id = s.id
            AND e.active = true
      )
    GROUP BY sn.id,
             sn.sandbox_id,
             s.updated_at,
             sn.timestamp,
             sn.local_artifact_id,
             sn.artifact_kind,
             s.current_local_artifact_id,
             s.current_local_artifact_kind
),
ranked AS (
    SELECT ready_full.*,
           {args.keep_snapshot_generations_per_sandbox}::int AS keep_count,
           COUNT(*) OVER (PARTITION BY sandbox_id) AS ready_full_snapshot_count,
           ROW_NUMBER() OVER (
               PARTITION BY sandbox_id
               ORDER BY timestamp DESC, snapshot_id DESC
           ) AS newest_rank,
           ROW_NUMBER() OVER (
               PARTITION BY sandbox_id, is_current_artifact
               ORDER BY timestamp DESC, snapshot_id DESC
           ) AS artifact_partition_rank,
           BOOL_OR(is_current_artifact) OVER (PARTITION BY sandbox_id) AS has_current_artifact
    FROM ready_full
)
SELECT sandbox_id::text AS sandbox_id,
       sandbox_updated_at::text AS sandbox_updated_at,
       snapshot_id::text AS snapshot_id,
       timestamp::text AS snapshot_timestamp,
       local_artifact_id,
       artifact_kind,
       current_local_artifact_id,
       current_local_artifact_kind,
       ready_full_snapshot_count::text AS ready_full_snapshot_count,
       newest_rank::text AS newest_rank,
       artifact_partition_rank::text AS artifact_partition_rank,
       is_current_artifact::text AS is_current_artifact,
       has_current_artifact::text AS has_current_artifact,
       has_creating_snapshot::text AS has_creating_snapshot,
       storage_location_count::text AS storage_location_count
FROM ranked
WHERE NOT {keep_predicate}
  AND timestamp < NOW() - make_interval(hours => {args.snapshot_min_age_hours})
  AND storage_uri_snapshot_scoped
ORDER BY sandbox_updated_at, sandbox_id, timestamp, snapshot_id
{oldest_limit_clause(args)}
"""
    return psql_csv_query(args, query)


def snapshot_generation_storage_rows(
    args: argparse.Namespace,
    candidates: list[SnapshotGenerationCandidate],
) -> list[dict[str, str]]:
    if not candidates:
        return []
    values = values_uuid(candidate.snapshot_id for candidate in candidates)
    script = f"""
BEGIN;
CREATE TEMP TABLE requested_snapshots (id uuid PRIMARY KEY) ON COMMIT DROP;
INSERT INTO requested_snapshots (id)
VALUES
{values};
COPY (
    SELECT sn.sandbox_id::text AS sandbox_id,
           sn.id::text AS snapshot_id,
           ssl.id::text AS snapshot_storage_location_id,
           ssl.storage_connection_string,
           EXISTS (
               SELECT 1
               FROM snapshot_storage_location_encrypted_fields encrypted
               WHERE encrypted.snapshot_storage_location_id = ssl.id
           )::text AS has_encrypted_fields
    FROM requested_snapshots requested
    JOIN snapshots sn ON sn.id = requested.id
    JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
    ORDER BY sn.sandbox_id, sn.id, ssl.id
) TO STDOUT WITH CSV HEADER;
COMMIT;
"""
    return parse_csv(psql_script(args, script))


def classify_snapshot_generation_candidates(
    rows: list[dict[str, str]],
    snapshot_storage: list[SnapshotStorageRow] | None = None,
) -> list[SnapshotGenerationCandidate]:
    candidates: list[SnapshotGenerationCandidate] = []
    storage_by_snapshot: dict[str, list[SnapshotStorageRow]] = defaultdict(list)
    for row in snapshot_storage or []:
        storage_by_snapshot[row.snapshot_id].append(row)
    storage_blocking_reasons = snapshot_storage_blocking_reasons_by_snapshot(
        snapshot_storage or []
    )
    scope_blocking_reasons = snapshot_storage_scope_blocking_reasons(snapshot_storage or [])
    for row in rows:
        snapshot_id = parse_uuid(row["snapshot_id"])
        has_creating_snapshot = row["has_creating_snapshot"].lower() == "true"
        storage_rows = storage_by_snapshot.get(snapshot_id, [])
        expected_storage_count = int(row["storage_location_count"])
        if has_creating_snapshot:
            decision = "skip"
            reason = "sandbox_has_creating_snapshot"
        elif not storage_rows:
            decision = "skip"
            reason = "no_attached_storage_location"
        elif len(storage_rows) != expected_storage_count:
            decision = "skip"
            reason = "storage_location_count_drift"
        elif snapshot_id in storage_blocking_reasons:
            decision = "skip"
            reason = f"snapshot_storage_not_sweepable:{storage_blocking_reasons[snapshot_id]}"
        elif snapshot_id in scope_blocking_reasons:
            decision = "skip"
            reason = scope_blocking_reasons[snapshot_id]
        else:
            decision = "orphan"
            reason = ""
        candidates.append(
            SnapshotGenerationCandidate(
                sandbox_id=parse_uuid(row["sandbox_id"]),
                sandbox_updated_at=row["sandbox_updated_at"],
                snapshot_id=snapshot_id,
                snapshot_timestamp=row["snapshot_timestamp"],
                local_artifact_id=row["local_artifact_id"],
                artifact_kind=row["artifact_kind"],
                current_local_artifact_id=row["current_local_artifact_id"],
                current_local_artifact_kind=row["current_local_artifact_kind"],
                ready_full_snapshot_count=row["ready_full_snapshot_count"],
                newest_rank=row["newest_rank"],
                artifact_partition_rank=row["artifact_partition_rank"],
                is_current_artifact=row["is_current_artifact"],
                has_current_artifact=row["has_current_artifact"],
                has_creating_snapshot=row["has_creating_snapshot"],
                storage_location_count=row["storage_location_count"],
                decision=decision,
                decision_reason=reason,
            )
        )
    return candidates


SNAPSHOT_GENERATION_FIELDNAMES = [
    "sandbox_id",
    "sandbox_updated_at",
    "snapshot_id",
    "snapshot_timestamp",
    "local_artifact_id",
    "artifact_kind",
    "current_local_artifact_id",
    "current_local_artifact_kind",
    "ready_full_snapshot_count",
    "newest_rank",
    "artifact_partition_rank",
    "is_current_artifact",
    "has_current_artifact",
    "has_creating_snapshot",
    "storage_location_count",
    "decision",
    "decision_reason",
]


SNAPSHOT_GENERATION_MUTATION_FIELDNAMES = [
    "record_type",
    "sandbox_id",
    "snapshot_id",
    "snapshot_timestamp",
    "snapshot_storage_location_id",
    "storage_connection_string",
]


def orphan_snapshot_generation_batch(
    args: argparse.Namespace,
    batch: list[SnapshotGenerationCandidate],
    snapshot_storage: list[SnapshotStorageRow],
) -> list[dict[str, str]]:
    values = values_uuid(candidate.snapshot_id for candidate in batch)
    sweepable_storage_ids = [
        row.snapshot_storage_location_id
        for row in snapshot_storage
        if row.sweepability_decision == "sweepable"
    ]
    sweepable_storage_insert = insert_uuid_values_sql(
        "sweepable_snapshot_storage_locations",
        "id",
        sweepable_storage_ids,
    )
    keep_predicate = snapshot_generation_keep_predicate_sql()
    is_current_artifact_sql = snapshot_is_current_artifact_sql()
    script = f"""
BEGIN;
CREATE TEMP TABLE requested_snapshots (id uuid PRIMARY KEY) ON COMMIT DROP;
INSERT INTO requested_snapshots (id)
VALUES
{values};
CREATE TEMP TABLE sweepable_snapshot_storage_locations (id uuid PRIMARY KEY) ON COMMIT DROP;
{sweepable_storage_insert}
CREATE TEMP TABLE requested_sandboxes (id uuid PRIMARY KEY) ON COMMIT DROP;
INSERT INTO requested_sandboxes (id)
SELECT DISTINCT sn.sandbox_id
FROM snapshots sn
JOIN requested_snapshots requested ON requested.id = sn.id;
CREATE TEMP TABLE locked_requested_snapshots (id uuid PRIMARY KEY) ON COMMIT DROP;
INSERT INTO locked_requested_snapshots (id)
SELECT sn.id
FROM snapshots sn
JOIN requested_snapshots requested ON requested.id = sn.id
FOR UPDATE OF sn;
CREATE TEMP TABLE selected_snapshots (
    id uuid PRIMARY KEY,
    sandbox_id uuid NOT NULL,
    snapshot_timestamp timestamptz NOT NULL
) ON COMMIT DROP;
WITH ready_full AS (
    SELECT sn.id AS snapshot_id,
           sn.sandbox_id,
           sn.timestamp,
           COUNT(ssl.id) AS storage_location_count,
           BOOL_AND(
               ssl.storage_connection_string LIKE '%/' || sn.id::text || '?%'
           ) AS storage_uri_snapshot_scoped,
           EXISTS (
               SELECT 1
               FROM snapshots creating
               WHERE creating.sandbox_id = sn.sandbox_id
                 AND creating.state = 'creating'
           ) AS has_creating_snapshot,
           {is_current_artifact_sql} AS is_current_artifact
    FROM snapshots sn
    JOIN requested_sandboxes requested_sandbox
      ON requested_sandbox.id = sn.sandbox_id
    JOIN sandboxes s ON s.id = sn.sandbox_id
    JOIN snapshot_storage_locations ssl
      ON ssl.snapshot_id = sn.id
     AND ssl.orphaned_at IS NULL
    WHERE sn.state = 'ready'
      AND sn.type = 'full'
      AND NOT EXISTS (
          SELECT 1
          FROM sandbox_k8s_executions e
          WHERE e.sandbox_id = s.id
            AND e.active = true
      )
    GROUP BY sn.id,
             sn.sandbox_id,
             sn.timestamp,
             sn.local_artifact_id,
             sn.artifact_kind,
             s.current_local_artifact_id,
             s.current_local_artifact_kind
),
ranked AS (
    SELECT ready_full.*,
           {args.keep_snapshot_generations_per_sandbox}::int AS keep_count,
           ROW_NUMBER() OVER (
               PARTITION BY sandbox_id
               ORDER BY timestamp DESC, snapshot_id DESC
           ) AS newest_rank,
           ROW_NUMBER() OVER (
               PARTITION BY sandbox_id, is_current_artifact
               ORDER BY timestamp DESC, snapshot_id DESC
           ) AS artifact_partition_rank,
           BOOL_OR(is_current_artifact) OVER (PARTITION BY sandbox_id) AS has_current_artifact
    FROM ready_full
)
INSERT INTO selected_snapshots (id, sandbox_id, snapshot_timestamp)
SELECT ranked.snapshot_id, ranked.sandbox_id, ranked.timestamp
FROM ranked
JOIN locked_requested_snapshots requested ON requested.id = ranked.snapshot_id
WHERE NOT {keep_predicate}
  AND ranked.timestamp < NOW() - make_interval(hours => {args.snapshot_min_age_hours})
  AND ranked.storage_uri_snapshot_scoped
  AND NOT ranked.has_creating_snapshot
  AND NOT EXISTS (
      SELECT 1
      FROM snapshot_storage_locations ssl
      WHERE ssl.snapshot_id = ranked.snapshot_id
        AND NOT EXISTS (
            SELECT 1
            FROM sweepable_snapshot_storage_locations sweepable
            WHERE sweepable.id = ssl.id
        )
  );
CREATE TEMP TABLE mutation_results (
    record_type text NOT NULL,
    sandbox_id text NOT NULL,
    snapshot_id text NOT NULL,
    snapshot_timestamp text NOT NULL,
    snapshot_storage_location_id text NOT NULL,
    storage_connection_string text NOT NULL
) ON COMMIT DROP;
WITH orphaned (
    sandbox_id,
    snapshot_id,
    snapshot_timestamp,
    snapshot_storage_location_id,
    storage_connection_string
) AS (
    UPDATE snapshot_storage_locations ssl
    SET snapshot_id = NULL,
        orphaned_at = NOW()
    FROM selected_snapshots selected
    WHERE ssl.snapshot_id = selected.id
      AND NOT EXISTS (
          SELECT 1
          FROM sandbox_k8s_executions e
          WHERE e.sandbox_id = selected.sandbox_id
            AND e.active = true
      )
    RETURNING selected.sandbox_id::text,
              selected.id::text,
              selected.snapshot_timestamp::text,
              ssl.id::text,
              ssl.storage_connection_string
)
INSERT INTO mutation_results (
    record_type,
    sandbox_id,
    snapshot_id,
    snapshot_timestamp,
    snapshot_storage_location_id,
    storage_connection_string
)
SELECT 'orphaned_storage',
       sandbox_id,
       snapshot_id,
       snapshot_timestamp,
       snapshot_storage_location_id,
       storage_connection_string
FROM orphaned;
WITH deleted (sandbox_id, snapshot_id, snapshot_timestamp) AS (
    DELETE FROM snapshots sn
    USING selected_snapshots selected
    WHERE sn.id = selected.id
      AND NOT EXISTS (
          SELECT 1
          FROM sandbox_k8s_executions e
          WHERE e.sandbox_id = selected.sandbox_id
            AND e.active = true
      )
      AND NOT EXISTS (
          SELECT 1
          FROM snapshot_storage_locations ssl
          WHERE ssl.snapshot_id = sn.id
      )
    RETURNING sn.sandbox_id::text, sn.id::text, sn.timestamp::text
)
INSERT INTO mutation_results (
    record_type,
    sandbox_id,
    snapshot_id,
    snapshot_timestamp,
    snapshot_storage_location_id,
    storage_connection_string
)
SELECT 'deleted_snapshot',
       sandbox_id,
       snapshot_id,
       snapshot_timestamp,
       '',
       ''
FROM deleted;
COPY (
    SELECT record_type,
           sandbox_id,
           snapshot_id,
           snapshot_timestamp,
           snapshot_storage_location_id,
           storage_connection_string
    FROM mutation_results
    ORDER BY record_type, sandbox_id, snapshot_id, snapshot_storage_location_id
) TO STDOUT WITH CSV HEADER;
COMMIT;
"""
    return parse_csv(psql_script(args, script))


def run_old_snapshot_generations(args: argparse.Namespace) -> int:
    out = output_dir(args)
    raw_candidate_rows = snapshot_generation_candidate_rows(args)
    initial_candidates = classify_snapshot_generation_candidates(raw_candidate_rows)
    scoped_snapshot_bucket = snapshot_s3_bucket(args) if initial_candidates else ""
    snapshot_storage = classify_snapshot_storage_rows(
        snapshot_generation_storage_rows(args, initial_candidates),
        scoped_snapshot_bucket,
    )
    candidates = classify_snapshot_generation_candidates(
        raw_candidate_rows,
        snapshot_storage,
    )
    write_csv(
        out / "old_snapshot_generations_candidates.csv",
        [candidate.__dict__ for candidate in candidates],
        SNAPSHOT_GENERATION_FIELDNAMES,
    )
    write_csv(
        out / "old_snapshot_generations_snapshot_storage.csv",
        [row.__dict__ for row in snapshot_storage],
        SNAPSHOT_STORAGE_FIELDNAMES,
    )
    eligible = [candidate for candidate in candidates if candidate.decision == "orphan"]
    selected = eligible[: count_selected(len(eligible), args)]
    selected_snapshot_storage = filter_snapshot_generation_storage_rows(
        snapshot_storage,
        selected,
    )
    write_csv(
        out / "old_snapshot_generations_selected.csv",
        [candidate.__dict__ for candidate in selected],
        SNAPSHOT_GENERATION_FIELDNAMES,
    )
    write_csv(
        out / "old_snapshot_generations_selected_snapshot_storage.csv",
        [row.__dict__ for row in selected_snapshot_storage],
        SNAPSHOT_STORAGE_FIELDNAMES,
    )
    decisions = Counter(candidate.decision for candidate in candidates)
    snapshot_storage_decisions = Counter(row.sweepability_decision for row in snapshot_storage)
    eprint(f"old-snapshot-generation candidates: {dict(decisions)}")
    eprint(
        "old-snapshot-generation selected for this run: "
        f"{len(selected)} of {len(eligible)} eligible"
    )
    eprint(f"audit directory: {out}")

    if not args.live:
        write_json(
            out / "summary.json",
            {
                "mode": "old-snapshot-generations",
                "dry_run": True,
                "decisions": dict(decisions),
                "eligible": len(eligible),
                "selected": len(selected),
                "oldest_limit": effective_oldest_limit(args),
                "keep_snapshot_generations_per_sandbox": (
                    args.keep_snapshot_generations_per_sandbox
                ),
                "snapshot_min_age_hours": args.snapshot_min_age_hours,
                "snapshot_s3_bucket": scoped_snapshot_bucket,
                "snapshot_storage": dict(snapshot_storage_decisions),
            },
        )
        return 0

    mutation_rows: list[dict[str, str]] = []
    max_batches = args.max_batches or None
    for batch_index, batch in enumerate(chunks(selected, args.batch_size), start=1):
        if max_batches is not None and batch_index > max_batches:
            break
        batch_snapshot_storage = filter_snapshot_generation_storage_rows(
            selected_snapshot_storage,
            batch,
        )
        mutation_rows.extend(orphan_snapshot_generation_batch(args, batch, batch_snapshot_storage))
    orphaned_storage_rows = [
        row for row in mutation_rows if row["record_type"] == "orphaned_storage"
    ]
    deleted_snapshot_rows = [
        row for row in mutation_rows if row["record_type"] == "deleted_snapshot"
    ]
    write_csv(
        out / "old_snapshot_generations_orphaned_storage.csv",
        orphaned_storage_rows,
        SNAPSHOT_GENERATION_MUTATION_FIELDNAMES,
    )
    write_csv(
        out / "old_snapshot_generations_deleted_snapshots.csv",
        deleted_snapshot_rows,
        SNAPSHOT_GENERATION_MUTATION_FIELDNAMES,
    )
    write_json(
        out / "summary.json",
        {
            "mode": "old-snapshot-generations",
            "dry_run": False,
            "decisions": dict(decisions),
            "eligible": len(eligible),
            "selected": len(selected),
            "oldest_limit": effective_oldest_limit(args),
            "keep_snapshot_generations_per_sandbox": (
                args.keep_snapshot_generations_per_sandbox
            ),
            "snapshot_min_age_hours": args.snapshot_min_age_hours,
            "snapshot_s3_bucket": scoped_snapshot_bucket,
            "snapshot_storage": dict(snapshot_storage_decisions),
            "snapshot_storage_rows_orphaned": len(orphaned_storage_rows),
            "snapshot_rows_deleted": len(deleted_snapshot_rows),
        },
    )
    eprint(f"orphaned snapshot storage rows: {len(orphaned_storage_rows)}")
    eprint(f"deleted snapshot rows: {len(deleted_snapshot_rows)}")
    return 0


def db_cutoff(args: argparse.Namespace) -> dict[str, str]:
    query = f"""
SELECT NOW()::text AS db_now,
       (NOW() - INTERVAL {sql_literal(str(args.cutoff_days) + " days")})::text AS cutoff
"""
    rows = psql_csv_query(args, query)
    if len(rows) != 1:
        raise RuntimeError("failed to read database cutoff")
    return rows[0]


def sandbox_candidate_rows(args: argparse.Namespace, cutoff: str) -> list[dict[str, str]]:
    query = f"""
SELECT s.id::text AS sandbox_id,
       s.lifecycle,
       s.created_at::text AS created_at,
       s.updated_at::text AS updated_at,
       COALESCE(s.active_node_id::text, '') AS active_node_id,
       COALESCE(s.target_node_id::text, '') AS target_node_id,
       COALESCE(active_nk.k8s_node_name, '') AS active_k8s_node_name,
       COALESCE(target_nk.k8s_node_name, '') AS target_k8s_node_name,
       (
           SELECT COUNT(*)::text
           FROM sandbox_k8s_executions e
           WHERE e.sandbox_id = s.id
       ) AS execution_rows,
       COALESCE((
           SELECT STRING_AGG(e.pod_name, ',' ORDER BY e.generation)
           FROM sandbox_k8s_executions e
           WHERE e.sandbox_id = s.id
       ), '') AS execution_pod_names,
       (
           SELECT COUNT(*)::text
           FROM snapshots sn
           WHERE sn.sandbox_id = s.id
       ) AS snapshot_rows,
       (
           SELECT COUNT(*)::text
           FROM snapshots sn
           JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
           WHERE sn.sandbox_id = s.id
       ) AS attached_storage_locations
FROM sandboxes s
LEFT JOIN node_k8s active_nk ON active_nk.node_id = s.active_node_id
LEFT JOIN node_k8s target_nk ON target_nk.node_id = s.target_node_id
WHERE s.updated_at < TIMESTAMPTZ {sql_literal(cutoff)}
  AND s.lifecycle = 'suspended'
  AND NOT EXISTS (
      SELECT 1
      FROM snapshots sn
      WHERE sn.sandbox_id = s.id
        AND sn.state = 'creating'
  )
ORDER BY s.updated_at, s.id
{oldest_limit_clause(args)}
"""
    return psql_csv_query(args, query)


def snapshot_storage_rows(
    args: argparse.Namespace,
    candidates: list[SandboxCandidate],
) -> list[dict[str, str]]:
    if not candidates:
        return []
    values = values_uuid(candidate.sandbox_id for candidate in candidates)
    script = f"""
BEGIN;
CREATE TEMP TABLE requested_sandboxes (id uuid PRIMARY KEY) ON COMMIT DROP;
INSERT INTO requested_sandboxes (id)
VALUES
{values};
COPY (
    SELECT sn.sandbox_id::text AS sandbox_id,
           sn.id::text AS snapshot_id,
           ssl.id::text AS snapshot_storage_location_id,
           ssl.storage_connection_string,
           EXISTS (
               SELECT 1
               FROM snapshot_storage_location_encrypted_fields encrypted
               WHERE encrypted.snapshot_storage_location_id = ssl.id
           )::text AS has_encrypted_fields
    FROM requested_sandboxes requested
    JOIN snapshots sn ON sn.sandbox_id = requested.id
    JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
    ORDER BY sn.sandbox_id, sn.id, ssl.id
) TO STDOUT WITH CSV HEADER;
COMMIT;
"""
    return parse_csv(psql_script(args, script))


def classify_sandbox_candidates(
    rows: list[dict[str, str]],
    live_pods: dict[str, PodInfo],
    snapshot_storage: list[SnapshotStorageRow] | None = None,
) -> list[SandboxCandidate]:
    candidates: list[SandboxCandidate] = []
    live_pod_names = set(live_pods)
    live_sandbox_ids = sandbox_ids_from_live_pods(live_pods)
    storage_blocking_reasons = snapshot_storage_blocking_reasons(snapshot_storage or [])
    for row in rows:
        pod_names = [name for name in row["execution_pod_names"].split(",") if name]
        live = [name for name in pod_names if name in live_pod_names]
        sandbox_id = parse_uuid(row["sandbox_id"])
        if live:
            decision = "skip"
            reason = "live_kubernetes_pod_by_name"
        elif sandbox_id in live_sandbox_ids:
            decision = "skip"
            reason = "live_kubernetes_pod_by_sandbox_id"
        elif sandbox_id in storage_blocking_reasons:
            decision = "skip"
            reason = f"snapshot_storage_not_sweepable:{storage_blocking_reasons[sandbox_id]}"
        else:
            decision = "delete"
            reason = ""
        candidates.append(
            SandboxCandidate(
                sandbox_id=sandbox_id,
                lifecycle=row["lifecycle"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                active_node_id=row["active_node_id"],
                target_node_id=row["target_node_id"],
                active_k8s_node_name=row["active_k8s_node_name"],
                target_k8s_node_name=row["target_k8s_node_name"],
                execution_rows=row["execution_rows"],
                execution_pod_names=row["execution_pod_names"],
                snapshot_rows=row["snapshot_rows"],
                attached_storage_locations=row["attached_storage_locations"],
                decision=decision,
                decision_reason=reason,
            )
        )
    return candidates


def sandbox_ids_from_live_pods(live_pods: dict[str, PodInfo]) -> set[str]:
    sandbox_ids: set[str] = set()
    for pod in live_pods.values():
        for metadata in (pod.labels, pod.annotations):
            sandbox_id = metadata.get(SANDBOX_ID_METADATA_KEY, "")
            if not sandbox_id:
                continue
            try:
                sandbox_ids.add(parse_uuid(sandbox_id))
            except ValueError:
                continue
    return sandbox_ids


def has_live_pod_for_sandbox(
    candidate: SandboxCandidate,
    live_pod_names: set[str],
    live_sandbox_ids: set[str],
) -> bool:
    return any(name in live_pod_names for name in candidate.pod_names()) or (
        candidate.sandbox_id in live_sandbox_ids
    )


def node_cleanup_shell() -> str:
    return f"""
set -eu
sbxmgr={sql_literal(SBXMGR_BIN)}
if [ ! -x "$sbxmgr" ]; then
  sbxmgr=space-sbxmgr
fi
while IFS= read -r sid; do
  [ -n "$sid" ] || continue
  tmp="/tmp/space-sbxmgr-delete-$sid.log"
  if "$sbxmgr" delete "$sid" --blocking >"$tmp" 2>&1; then
    printf '%s\\tok\\t\\n' "$sid"
  else
    msg="$(tr '\\n\\r\\t' '   ' <"$tmp" | cut -c 1-500)"
    printf '%s\\tfailed\\t%s\\n' "$sid" "$msg"
  fi
  rm -f "$tmp"
done
"""


def run_node_cleanup(
    args: argparse.Namespace,
    candidates: list[SandboxCandidate],
) -> list[dict[str, object]]:
    if not candidates:
        return []
    managers = load_sandbox_manager_pods(args)
    by_node: dict[str, list[SandboxCandidate]] = defaultdict(list)
    results: list[dict[str, object]] = []
    for candidate in candidates:
        node_name = candidate.cleanup_node_name()
        if not node_name:
            results.append(
                {
                    "sandbox_id": candidate.sandbox_id,
                    "node_name": "",
                    "manager_pod": "",
                    "status": "skipped",
                    "message": "no_pinned_k8s_node",
                }
            )
            continue
        by_node[node_name].append(candidate)

    for node_name, node_candidates in sorted(by_node.items()):
        manager = managers.get(node_name)
        if manager is None:
            for candidate in node_candidates:
                results.append(
                    {
                        "sandbox_id": candidate.sandbox_id,
                        "node_name": node_name,
                        "manager_pod": "",
                        "status": "skipped",
                        "message": "no_sandbox_manager_pod",
                    }
                )
            continue
        if manager.phase != "Running":
            for candidate in node_candidates:
                results.append(
                    {
                        "sandbox_id": candidate.sandbox_id,
                        "node_name": node_name,
                        "manager_pod": manager.name,
                        "status": "skipped",
                        "message": f"sandbox_manager_phase:{manager.phase}",
                    }
                )
            continue
        if not args.live or args.skip_node_cache:
            status = "dry_run" if not args.live else "skipped"
            message = "dry_run" if not args.live else "skip_node_cache"
            for candidate in node_candidates:
                results.append(
                    {
                        "sandbox_id": candidate.sandbox_id,
                        "node_name": node_name,
                        "manager_pod": manager.name,
                        "status": status,
                        "message": message,
                    }
                )
            continue

        for batch in chunks(node_candidates, args.node_cache_batch_size):
            ids = "\n".join(candidate.sandbox_id for candidate in batch) + "\n"
            proc = run(
                kubectl(
                    args,
                    "-n",
                    args.space_namespace,
                    "exec",
                    "-i",
                    manager.name,
                    "-c",
                    "sandbox-manager",
                    "--",
                    "sh",
                    "-lc",
                    node_cleanup_shell(),
                ),
                input_text=ids,
                timeout=args.node_cache_timeout_secs,
                check=False,
            )
            seen: set[str] = set()
            if proc.stdout:
                for line in proc.stdout.splitlines():
                    parts = line.split("\t", 2)
                    if len(parts) != 3:
                        continue
                    sandbox_id, status, message = parts
                    seen.add(sandbox_id)
                    results.append(
                        {
                            "sandbox_id": sandbox_id,
                            "node_name": node_name,
                            "manager_pod": manager.name,
                            "status": status,
                            "message": message,
                        }
                    )
            for candidate in batch:
                if candidate.sandbox_id in seen:
                    continue
                results.append(
                    {
                        "sandbox_id": candidate.sandbox_id,
                        "node_name": node_name,
                        "manager_pod": manager.name,
                        "status": "failed",
                        "message": (
                            f"kubectl_exec_returncode:{proc.returncode};"
                            f"{proc.stderr.strip()[:500]}"
                        ),
                    }
                )
    return results


def delete_sandbox_batch(
    args: argparse.Namespace,
    batch: list[SandboxCandidate],
    cutoff: str,
    snapshot_storage: list[SnapshotStorageRow] | None = None,
) -> list[dict[str, str]]:
    values = values_uuid(candidate.sandbox_id for candidate in batch)
    sweepable_storage_ids = [
        row.snapshot_storage_location_id
        for row in snapshot_storage or []
        if row.sweepability_decision == "sweepable"
    ]
    sweepable_storage_insert = insert_uuid_values_sql(
        "sweepable_snapshot_storage_locations",
        "id",
        sweepable_storage_ids,
    )
    script = f"""
BEGIN;
CREATE TEMP TABLE requested_sandboxes (id uuid PRIMARY KEY) ON COMMIT DROP;
INSERT INTO requested_sandboxes (id)
VALUES
{values};
CREATE TEMP TABLE sweepable_snapshot_storage_locations (id uuid PRIMARY KEY) ON COMMIT DROP;
{sweepable_storage_insert}
CREATE TEMP TABLE locked_sandboxes (id uuid PRIMARY KEY) ON COMMIT DROP;
INSERT INTO locked_sandboxes (id)
SELECT s.id
FROM sandboxes s
JOIN requested_sandboxes r ON r.id = s.id
FOR UPDATE;
CREATE TEMP TABLE deletable_sandboxes (id uuid PRIMARY KEY) ON COMMIT DROP;
INSERT INTO deletable_sandboxes (id)
SELECT s.id
FROM sandboxes s
JOIN locked_sandboxes locked ON locked.id = s.id
WHERE s.updated_at < TIMESTAMPTZ {sql_literal(cutoff)}
  AND s.lifecycle = 'suspended'
  AND NOT EXISTS (
      SELECT 1
      FROM snapshots sn
      WHERE sn.sandbox_id = s.id
        AND sn.state = 'creating'
  )
  AND NOT EXISTS (
      SELECT 1
      FROM snapshots sn
      JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
      WHERE sn.sandbox_id = s.id
        AND NOT EXISTS (
            SELECT 1
            FROM sweepable_snapshot_storage_locations sweepable
            WHERE sweepable.id = ssl.id
        )
  );
UPDATE sandboxes
SET placement = 'unplaced',
    active_node_id = NULL,
    target_node_id = NULL,
    placement_changed_at = NOW()
WHERE id IN (SELECT id FROM deletable_sandboxes);
DELETE FROM sandbox_observed_states
WHERE sandbox_id IN (SELECT id FROM deletable_sandboxes);
CREATE TEMP TABLE cleanup_refs (facet text NOT NULL, element text NOT NULL) ON COMMIT DROP;
INSERT INTO cleanup_refs (facet, element)
SELECT 'network-policy', 'sandbox:' || id::text FROM deletable_sandboxes
UNION ALL
SELECT 'sandbox-desired-state', id::text FROM deletable_sandboxes
UNION ALL
SELECT 'sandbox-inventory', id::text FROM deletable_sandboxes;
DELETE FROM node_reconcile_queue nrq
USING cleanup_refs refs
WHERE nrq.facet = refs.facet
  AND nrq.element = refs.element;
DELETE FROM node_facet_observed_status nfos
USING cleanup_refs refs
WHERE nfos.facet = refs.facet
  AND nfos.element = refs.element;
DELETE FROM node_facet_conditions nfc
USING cleanup_refs refs
WHERE nfc.facet = refs.facet
  AND nfc.element = refs.element;
UPDATE snapshot_storage_locations
SET snapshot_id = NULL,
    orphaned_at = NOW()
WHERE snapshot_id IN (
    SELECT sn.id
    FROM snapshots sn
    JOIN deletable_sandboxes deletable ON deletable.id = sn.sandbox_id
);
CREATE TEMP TABLE deleted_sandboxes (sandbox_id text PRIMARY KEY) ON COMMIT DROP;
WITH deleted AS (
    DELETE FROM sandboxes s
    USING deletable_sandboxes deletable
    WHERE s.id = deletable.id
    RETURNING s.id::text
)
INSERT INTO deleted_sandboxes
SELECT id FROM deleted;
COPY (
    SELECT sandbox_id
    FROM deleted_sandboxes
    ORDER BY sandbox_id
) TO STDOUT WITH CSV HEADER;
COMMIT;
"""
    return parse_csv(psql_script(args, script))


def run_old_sandboxes(args: argparse.Namespace, *, node_cache_only: bool = False) -> int:
    out = output_dir(args)
    cutoff_row = db_cutoff(args)
    cutoff = cutoff_row["cutoff"]
    live_pods = load_live_pods(args)
    raw_candidate_rows = sandbox_candidate_rows(args, cutoff)
    initial_candidates = classify_sandbox_candidates(raw_candidate_rows, live_pods)
    scoped_snapshot_bucket = snapshot_s3_bucket(args) if initial_candidates else ""
    snapshot_storage = classify_snapshot_storage_rows(
        snapshot_storage_rows(args, initial_candidates),
        scoped_snapshot_bucket,
    )
    candidates = classify_sandbox_candidates(raw_candidate_rows, live_pods, snapshot_storage)
    write_csv(
        out / "old_sandboxes_candidates.csv",
        [candidate.__dict__ for candidate in candidates],
        [
            "sandbox_id",
            "lifecycle",
            "created_at",
            "updated_at",
            "active_node_id",
            "target_node_id",
            "active_k8s_node_name",
            "target_k8s_node_name",
            "execution_rows",
            "execution_pod_names",
            "snapshot_rows",
            "attached_storage_locations",
            "decision",
            "decision_reason",
        ],
    )
    write_csv(
        out / "old_sandboxes_snapshot_storage.csv",
        [row.__dict__ for row in snapshot_storage],
        SNAPSHOT_STORAGE_FIELDNAMES,
    )
    eligible = [candidate for candidate in candidates if candidate.decision == "delete"]
    selected = eligible[: count_selected(len(eligible), args)]
    selected_snapshot_storage = filter_snapshot_storage_rows(snapshot_storage, selected)
    write_csv(
        out / "old_sandboxes_selected_snapshot_storage.csv",
        [row.__dict__ for row in selected_snapshot_storage],
        SNAPSHOT_STORAGE_FIELDNAMES,
    )
    decisions = Counter(candidate.decision for candidate in candidates)
    snapshot_storage_decisions = Counter(row.sweepability_decision for row in snapshot_storage)
    eprint(f"old-sandbox candidates: {dict(decisions)}")
    eprint(f"old-sandbox selected for this run: {len(selected)} of {len(eligible)} eligible")
    eprint(f"cutoff: {cutoff}")
    eprint(f"audit directory: {out}")

    if node_cache_only:
        node_results = run_node_cleanup(args, selected)
        write_csv(
            out / "old_sandboxes_node_cleanup.csv",
            node_results,
            ["sandbox_id", "node_name", "manager_pod", "status", "message"],
        )
        write_json(
            out / "summary.json",
            {
                "mode": "node-cache-audit",
                "dry_run": not args.live,
                "cutoff": cutoff_row,
                "decisions": dict(decisions),
                "eligible": len(eligible),
                "selected": len(selected),
                "oldest_limit": effective_oldest_limit(args),
                "db_rows_deleted": 0,
                "skip_db_delete": True,
                "node_cleanup": dict(Counter(str(row["status"]) for row in node_results)),
                "snapshot_s3_bucket": scoped_snapshot_bucket,
                "snapshot_storage": dict(snapshot_storage_decisions),
            },
        )
        return 0

    if not args.live or args.skip_db_delete:
        write_csv(
            out / "old_sandboxes_node_cleanup.csv",
            [],
            ["sandbox_id", "node_name", "manager_pod", "status", "message"],
        )
        write_json(
            out / "summary.json",
            {
                "mode": "old-sandboxes",
                "dry_run": not args.live,
                "cutoff": cutoff_row,
                "decisions": dict(decisions),
                "eligible": len(eligible),
                "selected": len(selected),
                "oldest_limit": effective_oldest_limit(args),
                "db_rows_deleted": 0,
                "skip_db_delete": args.skip_db_delete,
                "node_cleanup": {},
                "snapshot_s3_bucket": scoped_snapshot_bucket,
                "snapshot_storage": dict(snapshot_storage_decisions),
            },
        )
        return 0

    refreshed_live_pods = load_live_pods(args)
    refreshed_live_pod_names = set(refreshed_live_pods)
    refreshed_live_sandbox_ids = sandbox_ids_from_live_pods(refreshed_live_pods)
    db_eligible = [
        candidate
        for candidate in selected
        if not has_live_pod_for_sandbox(
            candidate,
            refreshed_live_pod_names,
            refreshed_live_sandbox_ids,
        )
    ]
    skipped_after_refresh = len(selected) - len(db_eligible)
    db_eligible_snapshot_storage = filter_snapshot_storage_rows(snapshot_storage, db_eligible)
    write_csv(
        out / "old_sandboxes_db_eligible_snapshot_storage.csv",
        [row.__dict__ for row in db_eligible_snapshot_storage],
        SNAPSHOT_STORAGE_FIELDNAMES,
    )
    deleted_rows: list[dict[str, str]] = []
    max_batches = args.max_batches or None
    for batch_index, batch in enumerate(chunks(db_eligible, args.batch_size), start=1):
        if max_batches is not None and batch_index > max_batches:
            break
        batch_snapshot_storage = filter_snapshot_storage_rows(db_eligible_snapshot_storage, batch)
        deleted_rows.extend(delete_sandbox_batch(args, batch, cutoff, batch_snapshot_storage))
    write_csv(out / "old_sandboxes_db_deletes.csv", deleted_rows, ["sandbox_id"])
    db_eligible_by_id = {candidate.sandbox_id: candidate for candidate in db_eligible}
    deleted_candidates = [
        db_eligible_by_id[row["sandbox_id"]]
        for row in deleted_rows
        if row["sandbox_id"] in db_eligible_by_id
    ]
    orphaned_snapshot_storage = filter_snapshot_storage_rows(
        db_eligible_snapshot_storage,
        deleted_candidates,
    )
    write_csv(
        out / "old_sandboxes_db_orphaned_snapshot_storage.csv",
        [row.__dict__ for row in orphaned_snapshot_storage],
        SNAPSHOT_STORAGE_FIELDNAMES,
    )
    node_results = run_node_cleanup(args, deleted_candidates)
    write_csv(
        out / "old_sandboxes_node_cleanup.csv",
        node_results,
        ["sandbox_id", "node_name", "manager_pod", "status", "message"],
    )
    write_json(
        out / "summary.json",
        {
            "mode": "old-sandboxes",
            "dry_run": False,
            "cutoff": cutoff_row,
            "decisions": dict(decisions),
            "eligible": len(eligible),
            "selected": len(selected),
            "oldest_limit": effective_oldest_limit(args),
            "skipped_after_live_pod_refresh": skipped_after_refresh,
            "db_rows_deleted": len(deleted_rows),
            "node_cleanup": dict(Counter(str(row["status"]) for row in node_results)),
            "snapshot_s3_bucket": scoped_snapshot_bucket,
            "snapshot_storage": dict(snapshot_storage_decisions),
        },
    )
    eprint(f"deleted sandbox rows: {len(deleted_rows)}")
    return 0


def template_candidate_rows(args: argparse.Namespace, cutoff: str) -> list[dict[str, str]]:
    query = f"""
WITH ranked AS (
    SELECT r.id,
           r.template_name,
           r.type AS runtime,
           r.hash AS content_hash,
           r.created_at,
           ROW_NUMBER() OVER (
               PARTITION BY r.template_name
               ORDER BY r.created_at DESC, r.id DESC
           ) AS newest_rank
    FROM template_releases r
)
SELECT r.id::text AS template_release_id,
       r.template_name,
       r.runtime,
       r.content_hash,
       r.created_at::text AS created_at,
       r.newest_rank::text AS newest_rank,
       (
           SELECT COUNT(*)::text
           FROM template_release_storage_locations sl
           WHERE sl.template_release_id = r.id
       ) AS storage_location_count,
       COALESCE((
           SELECT STRING_AGG(sl.storage_connection_string, '|' ORDER BY sl.storage_connection_string)
           FROM template_release_storage_locations sl
           WHERE sl.template_release_id = r.id
       ), '') AS storage_uris,
       EXISTS (
           SELECT 1
           FROM template_release_storage_locations sl
           JOIN template_release_storage_location_encrypted_fields encrypted
             ON encrypted.template_release_storage_location_id = sl.id
           WHERE sl.template_release_id = r.id
       )::text AS has_encrypted_storage
FROM ranked r
WHERE r.created_at < TIMESTAMPTZ {sql_literal(cutoff)}
  AND r.newest_rank > {args.keep_latest_per_template}
  AND NOT EXISTS (
      SELECT 1
      FROM template_release_tags tags
      WHERE tags.template_release_id = r.id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM kubernetes_pool_configs configs
      WHERE configs.template_release_id = r.id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM kubernetes_pool_slots slots
      WHERE slots.template_release_id = r.id
  )
ORDER BY r.created_at, r.template_name, r.id
{oldest_limit_clause(args)}
"""
    return psql_csv_query(args, query)


def classify_template_candidates(
    args: argparse.Namespace,
    rows: list[dict[str, str]],
) -> list[TemplateCandidate]:
    candidates: list[TemplateCandidate] = []
    for row in rows:
        release_id = parse_uuid(row["template_release_id"])
        candidate = TemplateCandidate(
            template_release_id=release_id,
            template_name=row["template_name"],
            runtime=row["runtime"],
            content_hash=row["content_hash"],
            created_at=row["created_at"],
            newest_rank=row["newest_rank"],
            storage_location_count=row["storage_location_count"],
            storage_uris=row["storage_uris"],
            has_encrypted_storage=row["has_encrypted_storage"],
            decision="delete",
            decision_reason="",
        )
        if candidate.has_encrypted_storage == "true":
            candidate = TemplateCandidate(
                **{
                    **candidate.__dict__,
                    "decision": "skip",
                    "decision_reason": "encrypted_storage_uri",
                }
            )
        else:
            ok, reason = validate_template_storage_uris(args, candidate.uri_list())
            if not ok:
                candidate = TemplateCandidate(
                    **{
                        **candidate.__dict__,
                        "decision": "skip",
                        "decision_reason": reason,
                    }
                )
        candidates.append(candidate)
    return candidates


def template_storage_prefix_rows(candidates: list[TemplateCandidate]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for candidate in candidates:
        for uri in candidate.uri_list():
            if uri in seen:
                continue
            seen.add(uri)
            bucket = ""
            key = ""
            valid = "true"
            error = ""
            try:
                bucket, key = parse_s3_uri(uri)
            except ValueError as err:
                valid = "false"
                error = str(err)
            rows.append(
                {
                    "storage_uri": uri,
                    "canonical_storage_uri": f"s3://{bucket}/{key}" if bucket and key else "",
                    "bucket": bucket,
                    "key": key,
                    "valid": valid,
                    "error": error,
                }
            )
    return rows


def aws_command(args: argparse.Namespace, *parts: str) -> list[str]:
    cmd = [args.aws_cli]
    if args.aws_profile:
        cmd.extend(["--profile", args.aws_profile])
    cmd.extend(parts)
    return cmd


def delete_template_s3_prefixes(
    args: argparse.Namespace,
    target: TemplateStorageTarget,
) -> tuple[bool, list[dict[str, object]]]:
    results: list[dict[str, object]] = []
    for uri in target.uri_list():
        ok, reason = validate_template_storage_uris(args, [uri])
        if not ok:
            results.append(
                {
                    "template_release_id": target.template_release_id,
                    "template_name": target.template_name,
                    "content_hash": target.content_hash,
                    "storage_uri": uri,
                    "status": "failed",
                    "message": reason,
                }
            )
            return False, results
        proc = run(
            aws_command(
                args,
                "s3",
                "rm",
                canonical_s3_prefix_uri(uri),
                "--recursive",
                "--only-show-errors",
            ),
            check=False,
        )
        message = (proc.stdout + proc.stderr).strip()[:1000]
        status = "ok" if proc.returncode == 0 else "failed"
        results.append(
            {
                "template_release_id": target.template_release_id,
                "template_name": target.template_name,
                "content_hash": target.content_hash,
                "storage_uri": uri,
                "status": status,
                "message": message,
            }
        )
        if proc.returncode != 0:
            return False, results
    return True, results


def delete_template_release_db(
    args: argparse.Namespace,
    candidate: TemplateCandidate,
    cutoff: str,
) -> list[dict[str, str]]:
    values = values_uuid([candidate.template_release_id])
    script = f"""
BEGIN;
CREATE TEMP TABLE requested_template_releases (id uuid PRIMARY KEY) ON COMMIT DROP;
INSERT INTO requested_template_releases (id)
VALUES
{values};
CREATE TEMP TABLE locked_template_releases (
    id uuid PRIMARY KEY,
    template_name text NOT NULL,
    content_hash text NOT NULL
) ON COMMIT DROP;
WITH ranked AS (
    SELECT r.id,
           ROW_NUMBER() OVER (
               PARTITION BY r.template_name
               ORDER BY r.created_at DESC, r.id DESC
           ) AS newest_rank
    FROM template_releases r
)
INSERT INTO locked_template_releases (id, template_name, content_hash)
SELECT r.id, r.template_name, r.hash
FROM template_releases r
JOIN requested_template_releases requested ON requested.id = r.id
JOIN ranked ON ranked.id = r.id
WHERE r.created_at < TIMESTAMPTZ {sql_literal(cutoff)}
  AND ranked.newest_rank > {args.keep_latest_per_template}
  AND NOT EXISTS (
      SELECT 1
      FROM template_release_tags tags
      WHERE tags.template_release_id = r.id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM kubernetes_pool_configs configs
      WHERE configs.template_release_id = r.id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM kubernetes_pool_slots slots
      WHERE slots.template_release_id = r.id
  )
FOR UPDATE OF r;
CREATE TEMP TABLE locked_template_storage (
    template_release_id uuid PRIMARY KEY,
    storage_uris text NOT NULL
) ON COMMIT DROP;
INSERT INTO locked_template_storage (template_release_id, storage_uris)
SELECT locked.id,
       COALESCE(
           STRING_AGG(
               storage.storage_connection_string,
               '|'
               ORDER BY storage.storage_connection_string
           ),
           ''
       )
FROM locked_template_releases locked
LEFT JOIN template_release_storage_locations storage
  ON storage.template_release_id = locked.id
GROUP BY locked.id;
DELETE FROM template_release_storage_locations storage
USING locked_template_releases locked
WHERE storage.template_release_id = locked.id;
DELETE FROM template_release_tags tags
USING locked_template_releases locked
WHERE tags.template_release_id = locked.id;
CREATE TEMP TABLE deleted_template_releases (
    template_release_id text PRIMARY KEY,
    template_name text NOT NULL,
    content_hash text NOT NULL,
    storage_uris text NOT NULL
) ON COMMIT DROP;
WITH deleted AS (
    DELETE FROM template_releases release
    USING locked_template_releases locked
    WHERE release.id = locked.id
    RETURNING release.id::text, release.template_name, release.hash
)
INSERT INTO deleted_template_releases (
    template_release_id,
    template_name,
    content_hash,
    storage_uris
)
SELECT deleted.id,
       deleted.template_name,
       deleted.hash,
       COALESCE(storage.storage_uris, '')
FROM deleted
LEFT JOIN locked_template_storage storage
  ON storage.template_release_id = deleted.id::uuid;
WITH affected AS (
    INSERT INTO node_reconcile_queue (
        node_id,
        facet,
        element,
        requested_at,
        updated_at
    )
    SELECT DISTINCT
           nodes.id,
           'template-desired-state',
           deleted.template_name,
           NOW(),
           NOW()
    FROM deleted_template_releases deleted
    JOIN nodes ON nodes.role IN ('worker', 'builder')
    LEFT JOIN node_direct direct ON direct.node_id = nodes.id
    WHERE nodes.managed_by != 'space'
       OR (direct.node_id IS NOT NULL AND direct.deleted_at IS NULL)
    ON CONFLICT (node_id, facet, element) DO UPDATE
    SET generation = node_reconcile_queue.generation + 1,
        requested_at = NOW(),
        next_attempt_at = NOW(),
        attempts = 0,
        last_error = '',
        dead_at = NULL,
        dead_reason = '',
        updated_at = NOW()
    RETURNING node_id
)
INSERT INTO node_reconcile_triggers (
    node_id,
    reason,
    reasons,
    requested_at,
    updated_at
)
SELECT DISTINCT
       node_id,
       'template_desired_state_updated',
       ARRAY['template_desired_state_updated'::state_reconciliation_reason],
       NOW(),
       NOW()
FROM affected
ON CONFLICT (node_id) DO UPDATE
SET generation = node_reconcile_triggers.generation + 1,
    reason = 'template_desired_state_updated',
    reasons = CASE
        WHEN 'template_desired_state_updated'::state_reconciliation_reason = ANY(node_reconcile_triggers.reasons)
        THEN node_reconcile_triggers.reasons
        ELSE ARRAY_APPEND(
            node_reconcile_triggers.reasons,
            'template_desired_state_updated'::state_reconciliation_reason
        )
    END,
    requested_at = NOW(),
    next_attempt_at = NOW(),
    attempts = 0,
    last_error = '',
    dead_at = NULL,
    dead_reason = '',
    updated_at = NOW();
COPY (
    SELECT template_release_id, template_name, content_hash, storage_uris
    FROM deleted_template_releases
    ORDER BY template_name, template_release_id
) TO STDOUT WITH CSV HEADER;
COMMIT;
"""
    return parse_csv(psql_script(args, script))


def run_superseded_templates(args: argparse.Namespace) -> int:
    out = output_dir(args)
    cutoff_row = db_cutoff(args)
    cutoff = cutoff_row["cutoff"]
    candidates = classify_template_candidates(args, template_candidate_rows(args, cutoff))
    write_csv(
        out / "superseded_templates_candidates.csv",
        [candidate.__dict__ for candidate in candidates],
        [
            "template_release_id",
            "template_name",
            "runtime",
            "content_hash",
            "created_at",
            "newest_rank",
            "storage_location_count",
            "storage_uris",
            "has_encrypted_storage",
            "decision",
            "decision_reason",
        ],
    )
    write_csv(
        out / "template_storage_prefixes.csv",
        template_storage_prefix_rows(candidates),
        ["storage_uri", "canonical_storage_uri", "bucket", "key", "valid", "error"],
    )
    eligible = [candidate for candidate in candidates if candidate.decision == "delete"]
    selected = eligible[: count_selected(len(eligible), args)]
    decisions = Counter(candidate.decision for candidate in candidates)
    eprint(f"superseded-template candidates: {dict(decisions)}")
    eprint(f"superseded-template selected for this run: {len(selected)} of {len(eligible)} eligible")
    eprint(f"cutoff: {cutoff}")
    eprint(f"audit directory: {out}")

    if not args.live:
        write_json(
            out / "summary.json",
            {
                "mode": "superseded-templates",
                "dry_run": True,
                "cutoff": cutoff_row,
                "keep_latest_per_template": args.keep_latest_per_template,
                "decisions": dict(decisions),
                "eligible": len(eligible),
                "selected": len(selected),
                "oldest_limit": effective_oldest_limit(args),
            },
        )
        return 0

    s3_results: list[dict[str, object]] = []
    db_deleted: list[dict[str, str]] = []
    failures: list[dict[str, object]] = []
    max_batches = args.max_batches or None
    stop = False
    for batch_index, batch in enumerate(chunks(selected, args.batch_size), start=1):
        if max_batches is not None and batch_index > max_batches:
            break
        for candidate in batch:
            deleted = delete_template_release_db(args, candidate, cutoff)
            if not deleted:
                failures.append(
                    {
                        "template_release_id": candidate.template_release_id,
                        "template_name": candidate.template_name,
                        "content_hash": candidate.content_hash,
                        "stage": "db_delete",
                        "message": "DB guard refused delete before S3 delete",
                    }
                )
                stop = True
                break
            db_deleted.extend(deleted)
            for row in deleted:
                target = TemplateStorageTarget(
                    template_release_id=row["template_release_id"],
                    template_name=row["template_name"],
                    content_hash=row["content_hash"],
                    storage_uris=row.get("storage_uris", ""),
                )
                ok, candidate_s3_results = delete_template_s3_prefixes(args, target)
                s3_results.extend(candidate_s3_results)
                if not ok:
                    failures.append(
                        {
                            "template_release_id": target.template_release_id,
                            "template_name": target.template_name,
                            "content_hash": target.content_hash,
                            "stage": "s3_delete",
                            "message": "stopping after failed S3 delete",
                        }
                    )
                    stop = True
                    break
            if stop:
                break
        if stop:
            break

    write_csv(
        out / "superseded_templates_s3_deletes.csv",
        s3_results,
        ["template_release_id", "template_name", "content_hash", "storage_uri", "status", "message"],
    )
    write_csv(
        out / "superseded_templates_db_deletes.csv",
        db_deleted,
        ["template_release_id", "template_name", "content_hash", "storage_uris"],
    )
    write_csv(
        out / "superseded_templates_failures.csv",
        failures,
        ["template_release_id", "template_name", "content_hash", "stage", "message"],
    )
    write_json(
        out / "summary.json",
        {
            "mode": "superseded-templates",
            "dry_run": False,
            "cutoff": cutoff_row,
            "keep_latest_per_template": args.keep_latest_per_template,
            "decisions": dict(decisions),
            "eligible": len(eligible),
            "selected": len(selected),
            "oldest_limit": effective_oldest_limit(args),
            "s3_delete_attempts": len(s3_results),
            "db_rows_deleted": len(db_deleted),
            "failures": len(failures),
        },
    )
    if failures:
        eprint("stopped after template pruning failure; inspect superseded_templates_failures.csv")
        return 2
    eprint(f"deleted template releases: {len(db_deleted)}")
    return 0


def current_generation_pod_refs(args: argparse.Namespace) -> dict[str, PodReference]:
    query = """
WITH latest AS (
    SELECT sandbox_id, MAX(generation) AS generation
    FROM sandbox_k8s_executions
    GROUP BY sandbox_id
)
SELECT e.pod_name,
       e.sandbox_id::text AS sandbox_id,
       e.generation::text AS generation,
       e.active::text AS active
FROM sandbox_k8s_executions e
JOIN latest
  ON latest.sandbox_id = e.sandbox_id
 AND latest.generation = e.generation
WHERE e.pod_name IS NOT NULL
"""
    refs: dict[str, PodReference] = {}
    for row in psql_csv_query(args, query):
        refs[row["pod_name"]] = PodReference(
            sandbox_id=row["sandbox_id"],
            generation=row["generation"],
            active=row["active"],
        )
    return refs


def classify_stale_pods(
    args: argparse.Namespace,
    live_pods: dict[str, PodInfo],
    current_refs: dict[str, PodReference],
    cutoff: str,
    *,
    apply_limit: bool = True,
) -> list[StalePodCandidate]:
    cutoff_ts = parse_timestamp(cutoff)
    candidates: list[StalePodCandidate] = []
    for pod in live_pods.values():
        if pod.phase not in TERMINAL_POD_PHASES or not pod.created_at:
            continue
        try:
            created_at = parse_timestamp(pod.created_at)
        except ValueError:
            continue
        if created_at >= cutoff_ts:
            continue
        ref = current_refs.get(pod.name)
        if ref:
            decision = "skip"
            reason = "current_generation_db_ref"
            sandbox_id = ref.sandbox_id
            generation = ref.generation
            active = ref.active
        else:
            decision = "delete"
            reason = ""
            sandbox_id = ""
            generation = ""
            active = ""
        candidates.append(
            StalePodCandidate(
                pod_name=pod.name,
                phase=pod.phase,
                node_name=pod.node_name,
                created_at=pod.created_at,
                decision=decision,
                decision_reason=reason,
                current_generation_sandbox_id=sandbox_id,
                current_generation=generation,
                current_generation_active=active,
            )
        )
    candidates.sort(key=lambda candidate: (candidate.created_at, candidate.pod_name))
    limit = effective_oldest_limit(args)
    if apply_limit and limit > 0:
        return candidates[:limit]
    return candidates


def run_stale_pods(args: argparse.Namespace) -> int:
    out = output_dir(args)
    cutoff_row = db_cutoff(args)
    cutoff = cutoff_row["cutoff"]
    live_pods = load_live_pods(args)
    candidates = classify_stale_pods(
        args,
        live_pods,
        current_generation_pod_refs(args),
        cutoff,
        apply_limit=False,
    )
    write_csv(
        out / "stale_pods_candidates.csv",
        [candidate.__dict__ for candidate in candidates],
        [
            "pod_name",
            "phase",
            "node_name",
            "created_at",
            "decision",
            "decision_reason",
            "current_generation_sandbox_id",
            "current_generation",
            "current_generation_active",
        ],
    )
    eligible = [candidate for candidate in candidates if candidate.decision == "delete"]
    selected = eligible[: count_selected(len(eligible), args)]
    decisions = Counter(candidate.decision for candidate in candidates)
    eprint(f"stale-pod candidates: {dict(decisions)}")
    eprint(f"stale-pod selected for this run: {len(selected)} of {len(eligible)} eligible")
    eprint(f"cutoff: {cutoff}")
    eprint(f"audit directory: {out}")
    if not args.live:
        write_json(
            out / "summary.json",
            {
                "mode": "stale-pods",
                "dry_run": True,
                "cutoff": cutoff_row,
                "decisions": dict(decisions),
                "eligible": len(eligible),
                "selected": len(selected),
                "oldest_limit": effective_oldest_limit(args),
            },
        )
        return 0

    deleted_rows: list[dict[str, object]] = []
    max_batches = args.max_batches or None
    for batch_index, batch in enumerate(chunks(selected, args.batch_size), start=1):
        if max_batches is not None and batch_index > max_batches:
            break
        for candidate in batch:
            ok, message = delete_pod(args, candidate.pod_name)
            deleted_rows.append(
                {
                    "pod_name": candidate.pod_name,
                    "phase": candidate.phase,
                    "node_name": candidate.node_name,
                    "created_at": candidate.created_at,
                    "deleted": ok,
                    "message": message,
                }
            )
    write_csv(
        out / "stale_pods_deletes.csv",
        deleted_rows,
        ["pod_name", "phase", "node_name", "created_at", "deleted", "message"],
    )
    write_json(
        out / "summary.json",
        {
            "mode": "stale-pods",
            "dry_run": False,
            "cutoff": cutoff_row,
            "decisions": dict(decisions),
            "eligible": len(eligible),
            "selected": len(selected),
            "oldest_limit": effective_oldest_limit(args),
            "pod_delete_attempts": len(deleted_rows),
            "pod_delete_failures": sum(1 for row in deleted_rows if not row["deleted"]),
        },
    )
    eprint(f"deleted stale pods: {sum(1 for row in deleted_rows if row['deleted'])}")
    return 0


def scan_database_totals(args: argparse.Namespace, cutoff: str) -> dict[str, str]:
    query = f"""
SELECT (SELECT COUNT(*) FROM sandboxes)::text AS total_sandboxes,
       (SELECT COUNT(*) FROM sandbox_k8s_executions)::text AS total_execution_rows,
       (SELECT COUNT(*) FROM templates)::text AS total_templates,
       (SELECT COUNT(*) FROM template_releases)::text AS total_template_releases,
       (SELECT COUNT(*) FROM template_release_storage_locations)::text AS total_template_storage_locations,
       (SELECT COUNT(*) FROM template_release_tags)::text AS total_template_tags,
       (SELECT COUNT(*) FROM snapshots)::text AS total_snapshots,
       (SELECT COUNT(*)
        FROM snapshots sn
        JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
        WHERE sn.state = 'ready'
          AND sn.type = 'full'
          AND ssl.orphaned_at IS NULL)::text AS total_ready_full_snapshot_storage_locations,
       (SELECT COUNT(*) FROM snapshot_storage_locations)::text AS total_snapshot_storage_locations,
       (SELECT COUNT(*) FROM snapshot_storage_locations WHERE orphaned_at IS NOT NULL)::text AS orphaned_storage_rows,
       (SELECT COUNT(*) FROM snapshot_storage_locations
        WHERE orphaned_at IS NOT NULL
          AND orphaned_at < NOW() - INTERVAL '5 minutes')::text AS sweep_eligible_storage_rows,
       (SELECT COUNT(*) FROM snapshots WHERE state = 'creating')::text AS creating_snapshots,
       {sql_literal(cutoff)}::text AS cutoff
"""
    rows = psql_csv_query(args, query)
    return rows[0] if rows else {}


def selected_dict(eligible: int, args: argparse.Namespace) -> dict[str, int]:
    return {
        "eligible": eligible,
        "selected": count_selected(eligible, args),
        "oldest_limit": effective_oldest_limit(args),
    }


def write_scan_candidates(
    out: Path,
    args: argparse.Namespace,
    old_execution_candidates: list[OldGenerationCandidate],
    sandbox_candidates: list[SandboxCandidate],
    sandbox_snapshot_storage: list[SnapshotStorageRow],
    snapshot_generation_candidates: list[SnapshotGenerationCandidate],
    snapshot_generation_storage: list[SnapshotStorageRow],
    template_candidates: list[TemplateCandidate],
    stale_pod_candidates: list[StalePodCandidate],
) -> None:
    write_csv(
        out / "old_executions_candidates.csv",
        [candidate.__dict__ for candidate in old_execution_candidates],
        [
            "sandbox_id",
            "generation",
            "pod_name",
            "state",
            "active",
            "created_at",
            "updated_at",
            "live_pod_phase",
            "decision",
        ],
    )
    write_csv(
        out / "dormant_sandboxes_candidates.csv",
        [candidate.__dict__ for candidate in sandbox_candidates],
        [
            "sandbox_id",
            "lifecycle",
            "created_at",
            "updated_at",
            "active_node_id",
            "target_node_id",
            "active_k8s_node_name",
            "target_k8s_node_name",
            "execution_rows",
            "execution_pod_names",
            "snapshot_rows",
            "attached_storage_locations",
            "decision",
            "decision_reason",
        ],
    )
    write_csv(
        out / "dormant_sandboxes_snapshot_storage.csv",
        [row.__dict__ for row in sandbox_snapshot_storage],
        SNAPSHOT_STORAGE_FIELDNAMES,
    )
    write_csv(
        out / "old_snapshot_generations_candidates.csv",
        [candidate.__dict__ for candidate in snapshot_generation_candidates],
        SNAPSHOT_GENERATION_FIELDNAMES,
    )
    write_csv(
        out / "old_snapshot_generations_snapshot_storage.csv",
        [row.__dict__ for row in snapshot_generation_storage],
        SNAPSHOT_STORAGE_FIELDNAMES,
    )
    write_csv(
        out / "superseded_templates_candidates.csv",
        [candidate.__dict__ for candidate in template_candidates],
        [
            "template_release_id",
            "template_name",
            "runtime",
            "content_hash",
            "created_at",
            "newest_rank",
            "storage_location_count",
            "storage_uris",
            "has_encrypted_storage",
            "decision",
            "decision_reason",
        ],
    )
    write_csv(
        out / "stale_pods_candidates.csv",
        [candidate.__dict__ for candidate in stale_pod_candidates],
        [
            "pod_name",
            "phase",
            "node_name",
            "created_at",
            "decision",
            "decision_reason",
            "current_generation_sandbox_id",
            "current_generation",
            "current_generation_active",
        ],
    )

    limit = effective_oldest_limit(args)
    if limit <= 0:
        return
    selected_old_executions = [
        candidate
        for candidate in old_execution_candidates
        if candidate.decision in {"delete_db", "delete_pod_then_db"}
    ][:limit]
    selected_sandboxes = [
        candidate for candidate in sandbox_candidates if candidate.decision == "delete"
    ][:limit]
    selected_sandbox_snapshot_storage = filter_snapshot_storage_rows(
        sandbox_snapshot_storage,
        selected_sandboxes,
    )
    selected_snapshot_generations = [
        candidate
        for candidate in snapshot_generation_candidates
        if candidate.decision == "orphan"
    ][:limit]
    selected_snapshot_generation_storage = filter_snapshot_generation_storage_rows(
        snapshot_generation_storage,
        selected_snapshot_generations,
    )
    selected_templates = [
        candidate for candidate in template_candidates if candidate.decision == "delete"
    ][:limit]
    selected_stale_pods = [
        candidate for candidate in stale_pod_candidates if candidate.decision == "delete"
    ][:limit]
    write_csv(
        out / "old_executions_selected.csv",
        [candidate.__dict__ for candidate in selected_old_executions],
        [
            "sandbox_id",
            "generation",
            "pod_name",
            "state",
            "active",
            "created_at",
            "updated_at",
            "live_pod_phase",
            "decision",
        ],
    )
    write_csv(
        out / "dormant_sandboxes_selected.csv",
        [candidate.__dict__ for candidate in selected_sandboxes],
        [
            "sandbox_id",
            "lifecycle",
            "created_at",
            "updated_at",
            "active_node_id",
            "target_node_id",
            "active_k8s_node_name",
            "target_k8s_node_name",
            "execution_rows",
            "execution_pod_names",
            "snapshot_rows",
            "attached_storage_locations",
            "decision",
            "decision_reason",
        ],
    )
    write_csv(
        out / "dormant_sandboxes_selected_snapshot_storage.csv",
        [row.__dict__ for row in selected_sandbox_snapshot_storage],
        SNAPSHOT_STORAGE_FIELDNAMES,
    )
    write_csv(
        out / "old_snapshot_generations_selected.csv",
        [candidate.__dict__ for candidate in selected_snapshot_generations],
        SNAPSHOT_GENERATION_FIELDNAMES,
    )
    write_csv(
        out / "old_snapshot_generations_selected_snapshot_storage.csv",
        [row.__dict__ for row in selected_snapshot_generation_storage],
        SNAPSHOT_STORAGE_FIELDNAMES,
    )
    write_csv(
        out / "superseded_templates_selected.csv",
        [candidate.__dict__ for candidate in selected_templates],
        [
            "template_release_id",
            "template_name",
            "runtime",
            "content_hash",
            "created_at",
            "newest_rank",
            "storage_location_count",
            "storage_uris",
            "has_encrypted_storage",
            "decision",
            "decision_reason",
        ],
    )
    write_csv(
        out / "stale_pods_selected.csv",
        [candidate.__dict__ for candidate in selected_stale_pods],
        [
            "pod_name",
            "phase",
            "node_name",
            "created_at",
            "decision",
            "decision_reason",
            "current_generation_sandbox_id",
            "current_generation",
            "current_generation_active",
        ],
    )


def run_scan(args: argparse.Namespace) -> int:
    out = output_dir(args)
    no_limit_args = without_candidate_limit(args)
    cutoff_row = db_cutoff(args)
    cutoff = cutoff_row["cutoff"]
    live_pods = load_live_pods(args)
    old_execution_candidates = classify_old_generations(
        old_generation_rows(no_limit_args),
        live_pods,
    )
    raw_sandbox_candidate_rows = sandbox_candidate_rows(no_limit_args, cutoff)
    initial_sandbox_candidates = classify_sandbox_candidates(
        raw_sandbox_candidate_rows,
        live_pods,
    )
    scoped_snapshot_bucket = snapshot_s3_bucket(args) if initial_sandbox_candidates else ""
    sandbox_snapshot_storage = classify_snapshot_storage_rows(
        snapshot_storage_rows(args, initial_sandbox_candidates),
        scoped_snapshot_bucket,
    )
    sandbox_candidates = classify_sandbox_candidates(
        raw_sandbox_candidate_rows,
        live_pods,
        sandbox_snapshot_storage,
    )
    raw_snapshot_generation_candidate_rows = snapshot_generation_candidate_rows(no_limit_args)
    initial_snapshot_generation_candidates = classify_snapshot_generation_candidates(
        raw_snapshot_generation_candidate_rows,
    )
    snapshot_generation_bucket = (
        scoped_snapshot_bucket
        or (snapshot_s3_bucket(args) if initial_snapshot_generation_candidates else "")
    )
    snapshot_generation_storage = classify_snapshot_storage_rows(
        snapshot_generation_storage_rows(args, initial_snapshot_generation_candidates),
        snapshot_generation_bucket,
    )
    snapshot_generation_candidates = classify_snapshot_generation_candidates(
        raw_snapshot_generation_candidate_rows,
        snapshot_generation_storage,
    )
    template_candidates = classify_template_candidates(
        no_limit_args,
        template_candidate_rows(no_limit_args, cutoff),
    )
    stale_pod_candidates = classify_stale_pods(
        no_limit_args,
        live_pods,
        current_generation_pod_refs(args),
        cutoff,
        apply_limit=False,
    )

    old_execution_decisions = Counter(candidate.decision for candidate in old_execution_candidates)
    sandbox_decisions = Counter(candidate.decision for candidate in sandbox_candidates)
    sandbox_snapshot_storage_decisions = Counter(
        row.sweepability_decision for row in sandbox_snapshot_storage
    )
    snapshot_generation_decisions = Counter(
        candidate.decision for candidate in snapshot_generation_candidates
    )
    snapshot_generation_storage_decisions = Counter(
        row.sweepability_decision for row in snapshot_generation_storage
    )
    template_decisions = Counter(candidate.decision for candidate in template_candidates)
    stale_pod_decisions = Counter(candidate.decision for candidate in stale_pod_candidates)

    old_execution_eligible = (
        old_execution_decisions["delete_db"] + old_execution_decisions["delete_pod_then_db"]
    )
    sandbox_eligible = sandbox_decisions["delete"]
    snapshot_generation_eligible = snapshot_generation_decisions["orphan"]
    template_eligible = template_decisions["delete"]
    stale_pod_eligible = stale_pod_decisions["delete"]
    totals = scan_database_totals(args, cutoff)
    report = {
        "mode": "scan",
        "cutoff": cutoff_row,
        "oldest_limit": effective_oldest_limit(args),
        "totals": totals,
        "artifacts": {
            "old_executions": {
                "total": int(totals.get("total_execution_rows", "0") or 0),
                "prunable": old_execution_eligible,
                "selected": selected_dict(old_execution_eligible, args)["selected"],
                "decisions": dict(old_execution_decisions),
                "s3_reclaim": "none",
                "oldest_order": "sandbox_k8s_executions.created_at, sandbox_id, generation",
            },
            "dormant_sandboxes": {
                "total": int(totals.get("total_sandboxes", "0") or 0),
                "prunable": sandbox_eligible,
                "selected": selected_dict(sandbox_eligible, args)["selected"],
                "decisions": dict(sandbox_decisions),
                "snapshot_s3_bucket": scoped_snapshot_bucket,
                "snapshot_storage": dict(sandbox_snapshot_storage_decisions),
                "oldest_order": "sandboxes.updated_at, sandboxes.id",
            },
            "old_snapshot_generations": {
                "total": int(
                    totals.get("total_ready_full_snapshot_storage_locations", "0") or 0
                ),
                "prunable": snapshot_generation_eligible,
                "selected": selected_dict(snapshot_generation_eligible, args)["selected"],
                "decisions": dict(snapshot_generation_decisions),
                "snapshot_s3_bucket": snapshot_generation_bucket
                if snapshot_generation_candidates
                else "",
                "snapshot_storage": dict(snapshot_generation_storage_decisions),
                "keep_snapshot_generations_per_sandbox": (
                    args.keep_snapshot_generations_per_sandbox
                ),
                "snapshot_min_age_hours": args.snapshot_min_age_hours,
                "oldest_order": "sandboxes.updated_at, sandbox_id, snapshots.timestamp, snapshot_id",
                "pruned_by": "snapshot_storage_sweeper_after_orphaning",
            },
            "superseded_templates": {
                "total": int(totals.get("total_template_releases", "0") or 0),
                "prunable": template_eligible,
                "selected": selected_dict(template_eligible, args)["selected"],
                "decisions": dict(template_decisions),
                "oldest_order": "template_releases.created_at, template_name, id",
            },
            "stale_pods": {
                "total": len(live_pods),
                "prunable": stale_pod_eligible,
                "selected": selected_dict(stale_pod_eligible, args)["selected"],
                "decisions": dict(stale_pod_decisions),
                "oldest_order": "pod.metadata.creationTimestamp, pod.metadata.name",
            },
            "snapshot_storage_sweeper_backlog": {
                "total": int(totals.get("total_snapshot_storage_locations", "0") or 0),
                "orphaned": int(totals.get("orphaned_storage_rows", "0") or 0),
                "sweep_eligible": int(totals.get("sweep_eligible_storage_rows", "0") or 0),
                "pruned_by": "snapshot_storage_sweeper",
            },
        },
        "audit_directory": str(out),
    }
    if args.export_candidates:
        write_scan_candidates(
            out,
            args,
            old_execution_candidates,
            sandbox_candidates,
            sandbox_snapshot_storage,
            snapshot_generation_candidates,
            snapshot_generation_storage,
            template_candidates,
            stale_pod_candidates,
        )
        report["candidate_exports"] = {
            "all": [
                "old_executions_candidates.csv",
                "dormant_sandboxes_candidates.csv",
                "dormant_sandboxes_snapshot_storage.csv",
                "old_snapshot_generations_candidates.csv",
                "old_snapshot_generations_snapshot_storage.csv",
                "superseded_templates_candidates.csv",
                "stale_pods_candidates.csv",
            ],
            "selected": [
                "old_executions_selected.csv",
                "dormant_sandboxes_selected.csv",
                "dormant_sandboxes_selected_snapshot_storage.csv",
                "old_snapshot_generations_selected.csv",
                "old_snapshot_generations_selected_snapshot_storage.csv",
                "superseded_templates_selected.csv",
                "stale_pods_selected.csv",
            ]
            if effective_oldest_limit(args) > 0
            else [],
        }
    write_json(out / "scan.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def run_verify(args: argparse.Namespace) -> int:
    out = output_dir(args)
    cutoff_row = db_cutoff(args)
    cutoff = cutoff_row["cutoff"]
    keep_predicate = snapshot_generation_keep_predicate_sql()
    live_pods = load_live_pods(args)
    pod_phases = Counter(pod.phase for pod in live_pods.values())
    stale_pods = classify_stale_pods(
        args,
        live_pods,
        current_generation_pod_refs(args),
        cutoff,
        apply_limit=False,
    )
    is_current_artifact_sql = snapshot_is_current_artifact_sql()
    query = f"""
WITH latest AS (
    SELECT sandbox_id, MAX(generation) AS latest_generation
    FROM sandbox_k8s_executions
    GROUP BY sandbox_id
),
old_generations AS (
    SELECT e.*
    FROM sandbox_k8s_executions e
    JOIN latest l USING (sandbox_id)
    WHERE e.generation < l.latest_generation
      AND e.active = false
),
old_sandboxes AS (
    SELECT s.id
    FROM sandboxes s
    WHERE s.updated_at < TIMESTAMPTZ {sql_literal(cutoff)}
      AND s.lifecycle = 'suspended'
      AND NOT EXISTS (
          SELECT 1
          FROM snapshots sn
          WHERE sn.sandbox_id = s.id
            AND sn.state = 'creating'
      )
),
ready_full_snapshots AS (
    SELECT sn.id AS snapshot_id,
           sn.sandbox_id,
           sn.timestamp,
           BOOL_AND(
               ssl.storage_connection_string LIKE '%/' || sn.id::text || '?%'
           ) AS storage_uri_snapshot_scoped,
           EXISTS (
               SELECT 1
               FROM snapshots creating
               WHERE creating.sandbox_id = sn.sandbox_id
                 AND creating.state = 'creating'
           ) AS has_creating_snapshot,
           {is_current_artifact_sql} AS is_current_artifact
    FROM snapshots sn
    JOIN sandboxes s ON s.id = sn.sandbox_id
    JOIN snapshot_storage_locations ssl
      ON ssl.snapshot_id = sn.id
     AND ssl.orphaned_at IS NULL
    WHERE sn.state = 'ready'
      AND sn.type = 'full'
    GROUP BY sn.id,
             sn.sandbox_id,
             sn.timestamp,
             sn.local_artifact_id,
             sn.artifact_kind,
             s.current_local_artifact_id,
             s.current_local_artifact_kind
),
ranked_snapshots AS (
    SELECT ready_full_snapshots.*,
           {args.keep_snapshot_generations_per_sandbox}::int AS keep_count,
           ROW_NUMBER() OVER (
               PARTITION BY sandbox_id
               ORDER BY timestamp DESC, snapshot_id DESC
           ) AS newest_rank,
           ROW_NUMBER() OVER (
               PARTITION BY sandbox_id, is_current_artifact
               ORDER BY timestamp DESC, snapshot_id DESC
           ) AS artifact_partition_rank,
           BOOL_OR(is_current_artifact) OVER (PARTITION BY sandbox_id) AS has_current_artifact
    FROM ready_full_snapshots
),
old_snapshot_generations AS (
    SELECT *
    FROM ranked_snapshots
    WHERE NOT {keep_predicate}
      AND timestamp < NOW() - make_interval(hours => {args.snapshot_min_age_hours})
      AND storage_uri_snapshot_scoped
      AND NOT has_creating_snapshot
),
ranked_template_releases AS (
    SELECT r.id,
           r.template_name,
           r.created_at,
           ROW_NUMBER() OVER (
               PARTITION BY r.template_name
               ORDER BY r.created_at DESC, r.id DESC
           ) AS newest_rank
    FROM template_releases r
),
superseded_template_candidates AS (
    SELECT r.id
    FROM ranked_template_releases r
    WHERE r.created_at < TIMESTAMPTZ {sql_literal(cutoff)}
      AND r.newest_rank > {args.keep_latest_per_template}
      AND NOT EXISTS (
          SELECT 1
          FROM template_release_tags tags
          WHERE tags.template_release_id = r.id
      )
      AND NOT EXISTS (
          SELECT 1
          FROM kubernetes_pool_configs configs
          WHERE configs.template_release_id = r.id
      )
      AND NOT EXISTS (
          SELECT 1
          FROM kubernetes_pool_slots slots
          WHERE slots.template_release_id = r.id
      )
)
SELECT (SELECT COUNT(*) FROM sandboxes)::text AS total_sandboxes,
       (SELECT COUNT(*) FROM sandbox_k8s_executions)::text AS total_execution_rows,
       (SELECT COUNT(*) FROM old_generations)::text AS old_generation_rows,
       (SELECT COUNT(*) FROM old_sandboxes)::text AS db_old_sandbox_candidates,
       (SELECT COUNT(*) FROM old_snapshot_generations)::text AS old_snapshot_generation_rows,
       (SELECT COUNT(*) FROM templates)::text AS total_templates,
       (SELECT COUNT(*) FROM template_releases)::text AS total_template_releases,
       (SELECT COUNT(*) FROM template_release_storage_locations)::text AS total_template_storage_locations,
       (SELECT COUNT(*) FROM template_release_tags)::text AS total_template_tags,
       (SELECT COUNT(*) FROM superseded_template_candidates)::text AS superseded_template_candidates,
       (SELECT COUNT(*) FROM snapshot_storage_locations WHERE orphaned_at IS NOT NULL)::text AS orphaned_storage_rows,
       (SELECT COUNT(*) FROM snapshot_storage_locations
        WHERE orphaned_at IS NOT NULL
          AND orphaned_at < NOW() - INTERVAL '5 minutes')::text AS sweep_eligible_storage_rows,
       (SELECT COUNT(*) FROM snapshots WHERE state = 'creating')::text AS creating_snapshots
"""
    rows = psql_csv_query(args, query)
    report = {
        "cutoff": cutoff_row,
        "database": rows[0] if rows else {},
        "live_pods": {
            "total": len(live_pods),
            "phases": dict(pod_phases),
        },
        "stale_pods": {
            "candidates": len(stale_pods),
            "decisions": dict(Counter(candidate.decision for candidate in stale_pods)),
        },
        "template_policy": {
            "keep_latest_per_template": args.keep_latest_per_template,
        },
        "snapshot_generation_policy": {
            "keep_snapshot_generations_per_sandbox": (
                args.keep_snapshot_generations_per_sandbox
            ),
            "snapshot_min_age_hours": args.snapshot_min_age_hours,
        },
    }
    write_json(out / "verify.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return parsed


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dry-run-first manual pruning for SPACE cluster artifacts."
    )
    parser.add_argument(
        "mode",
        choices=[
            "old-executions",
            "old-generations",
            "dormant-sandboxes",
            "old-sandboxes",
            "old-snapshot-generations",
            "old-snapshots",
            "node-cache-audit",
            "superseded-templates",
            "scan",
            "stale-pods",
            "verify",
        ],
    )
    parser.add_argument("--context", required=True)
    parser.add_argument("--space-namespace", default=DEFAULT_SPACE_NAMESPACE)
    parser.add_argument("--sandbox-namespace", default=DEFAULT_SANDBOX_NAMESPACE)
    parser.add_argument("--postgres-bastion", default=DEFAULT_POSTGRES_BASTION)
    parser.add_argument("--cutoff-days", type=positive_int, default=7)
    parser.add_argument("--keep-latest-per-template", type=positive_int, default=5)
    parser.add_argument("--keep-snapshot-generations-per-sandbox", type=positive_int, default=2)
    parser.add_argument("--snapshot-min-age-hours", type=non_negative_int, default=1)
    parser.add_argument("--batch-size", type=positive_int, default=500)
    parser.add_argument("--node-cache-batch-size", type=positive_int, default=100)
    parser.add_argument("--node-cache-timeout-secs", type=positive_int, default=300)
    parser.add_argument("--max-batches", type=int, default=0)
    parser.add_argument("--oldest-limit", type=int, default=0)
    parser.add_argument("--candidate-limit", type=int, default=0)
    parser.add_argument("--template-s3-allowed-prefix", action="append", default=[])
    parser.add_argument("--template-s3-allowed-prefix-file", action="append", default=[])
    parser.add_argument("--snapshot-s3-bucket")
    parser.add_argument("--aws-profile")
    parser.add_argument("--aws-cli", default="aws")
    parser.add_argument("--export-candidates", action="store_true")
    parser.add_argument("--output-dir")
    parser.add_argument("--run-id")
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--skip-node-cache", action="store_true")
    parser.add_argument("--skip-db-delete", action="store_true")
    args = parser.parse_args(argv)
    if args.max_batches < 0:
        parser.error("--max-batches must be non-negative")
    if args.oldest_limit < 0:
        parser.error("--oldest-limit must be non-negative")
    if args.candidate_limit < 0:
        parser.error("--candidate-limit must be non-negative")
    if args.oldest_limit and args.candidate_limit:
        parser.error("--oldest-limit and --candidate-limit are aliases; pass only one")
    if args.candidate_limit:
        args.oldest_limit = args.candidate_limit
        args.candidate_limit = 0
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    require_live_guard(args)
    if args.mode in {"old-executions", "old-generations"}:
        return run_old_generations(args)
    if args.mode in {"dormant-sandboxes", "old-sandboxes"}:
        return run_old_sandboxes(args)
    if args.mode in {"old-snapshot-generations", "old-snapshots"}:
        return run_old_snapshot_generations(args)
    if args.mode == "node-cache-audit":
        return run_old_sandboxes(args, node_cache_only=True)
    if args.mode == "superseded-templates":
        return run_superseded_templates(args)
    if args.mode == "scan":
        return run_scan(args)
    if args.mode == "stale-pods":
        return run_stale_pods(args)
    if args.mode == "verify":
        return run_verify(args)
    raise AssertionError(f"unhandled mode: {args.mode}")


if __name__ == "__main__":
    raise SystemExit(main())
