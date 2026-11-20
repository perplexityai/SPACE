#!/usr/bin/env python3

from __future__ import annotations

import argparse
import io
import importlib.util
import os
import sys
import tempfile
import threading
import time
import unittest
import urllib.parse
import uuid
from pathlib import Path
from unittest import mock

from required_test_deps import unavailable_dependency


REPO_ROOT = Path(__file__).resolve().parents[2]


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


prune = load_module(
    "prune_cluster_artifacts_under_test",
    REPO_ROOT / "scripts/admin/prune_cluster_artifacts.py",
)

POSTGRES_DRIVER_NAME = ""
POSTGRES_DRIVER = None
if importlib.util.find_spec("psycopg") is not None:
    POSTGRES_DRIVER_NAME = "psycopg"
    POSTGRES_DRIVER = importlib.import_module("psycopg")
elif importlib.util.find_spec("psycopg2") is not None:
    POSTGRES_DRIVER_NAME = "psycopg2"
    POSTGRES_DRIVER = importlib.import_module("psycopg2")


SANDBOX_ONE = "019de674-63d7-7ef0-a588-8e106a878093"
SANDBOX_TWO = "019e5f38-cd2e-7400-b3c8-e33ae36692d7"
TEMPLATE_RELEASE = "019e5f38-cd2e-7400-b3c8-e33ae36692d8"
SNAPSHOT_ONE = "019e5f38-cd2e-7400-b3c8-e33ae36692d9"
SNAPSHOT_TWO = "019e5f38-cd2e-7400-b3c8-e33ae36692db"
DORMANT_NEW_SNAPSHOT = "019e5f38-cd2e-7400-b3c8-e33ae36692dc"
ACTIVE_OLD_SNAPSHOT = "019e5f38-cd2e-7400-b3c8-e33ae36692dd"
ACTIVE_NEW_SNAPSHOT = "019e5f38-cd2e-7400-b3c8-e33ae36692de"
STORAGE_ONE = "019e5f38-cd2e-7400-b3c8-e33ae36692da"
DORMANT_NEW_STORAGE = "019e5f38-cd2e-7400-b3c8-e33ae36692df"
ACTIVE_OLD_STORAGE = "019e5f38-cd2e-7400-b3c8-e33ae36692e0"
ACTIVE_NEW_STORAGE = "019e5f38-cd2e-7400-b3c8-e33ae36692e1"


class PostgresRunner:
    def __init__(self, connection) -> None:
        self.connection = connection

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)

    def execute_script(self, script: str) -> None:
        for statement in script.split(";"):
            if statement.strip():
                self.execute(f"{statement};")

    def fetchone(
        self,
        sql: str,
        params: tuple[object, ...] | None = None,
    ) -> tuple[object, ...]:
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            row = cursor.fetchone()
        assert row is not None
        return tuple(row)

    def copy_stdout(self, sql: str) -> str:
        with self.connection.cursor() as cursor:
            if POSTGRES_DRIVER_NAME == "psycopg2":
                output = io.StringIO()
                cursor.copy_expert(sql, output)
                return output.getvalue()

            chunks: list[str] = []
            with cursor.copy(sql) as copy:
                while data := copy.read():
                    if isinstance(data, memoryview):
                        data = data.tobytes()
                    if isinstance(data, bytes):
                        chunks.append(data.decode())
                    else:
                        chunks.append(data)
            return "".join(chunks)


def postgres_runner_or_skip(test_case: unittest.TestCase) -> PostgresRunner:
    database_url = os.environ.get("PRUNE_CLUSTER_ARTIFACTS_TEST_DATABASE_URL")
    if database_url is None:
        database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        unavailable_dependency(
            "postgres",
            "set PRUNE_CLUSTER_ARTIFACTS_TEST_DATABASE_URL or DATABASE_URL, as "
            "ci/scripts/run-python-tests.sh does, to run the Postgres-backed prune "
            "mutation test",
        )
    if POSTGRES_DRIVER is None:
        test_case.fail("install psycopg or psycopg2 to run the Postgres-backed test")

    connection = POSTGRES_DRIVER.connect(database_url)
    connection.autocommit = True
    return PostgresRunner(connection)


def make_args(output_dir: str) -> argparse.Namespace:
    return argparse.Namespace(
        aws_cli="aws",
        aws_profile=None,
        batch_size=500,
        context="example-cluster",
        cutoff_days=7,
        export_candidates=False,
        keep_latest_per_template=5,
        keep_snapshot_generations_per_sandbox=2,
        live=True,
        max_batches=0,
        mode="dormant-sandboxes",
        node_cache_batch_size=100,
        node_cache_timeout_secs=300,
        oldest_limit=0,
        output_dir=output_dir,
        postgres_bastion="deploy/space-postgres-bastion",
        run_id="unit-test",
        sandbox_namespace="sandboxes",
        skip_db_delete=False,
        skip_node_cache=False,
        space_namespace="space",
        snapshot_s3_bucket="example-snapshots",
        snapshot_min_age_hours=1,
        template_s3_allowed_prefix=[],
        template_s3_allowed_prefix_file=[],
    )


def sandbox_row(
    sandbox_id: str,
    *,
    execution_pod_names: str = "",
    updated_at: str = "2026-01-01 00:00:00+00",
) -> dict[str, str]:
    return {
        "sandbox_id": sandbox_id,
        "lifecycle": "suspended",
        "created_at": "2025-12-01 00:00:00+00",
        "updated_at": updated_at,
        "active_node_id": "",
        "target_node_id": "",
        "active_k8s_node_name": "node-a",
        "target_k8s_node_name": "",
        "execution_rows": "0",
        "execution_pod_names": execution_pod_names,
        "snapshot_rows": "1",
        "attached_storage_locations": "1",
    }


def template_candidate() -> prune.TemplateCandidate:
    return prune.TemplateCandidate(
        template_release_id=TEMPLATE_RELEASE,
        template_name="code-runtime",
        runtime="firecracker",
        content_hash="sha256:abc",
        created_at="2026-01-01 00:00:00+00",
        newest_rank="6",
        storage_location_count="1",
        storage_uris="s3://example-template-artifacts/code-runtime/old/",
        has_encrypted_storage="false",
        decision="delete",
        decision_reason="",
    )


def snapshot_generation_row(
    snapshot_id: str = SNAPSHOT_ONE,
    *,
    sandbox_id: str = SANDBOX_ONE,
    has_creating_snapshot: str = "false",
    storage_location_count: str = "1",
) -> dict[str, str]:
    return {
        "sandbox_id": sandbox_id,
        "sandbox_updated_at": "2025-12-31 00:00:00+00",
        "snapshot_id": snapshot_id,
        "snapshot_timestamp": "2026-01-01 00:00:00+00",
        "local_artifact_id": "",
        "artifact_kind": "full",
        "current_local_artifact_id": "",
        "current_local_artifact_kind": "full",
        "ready_full_snapshot_count": "3",
        "newest_rank": "3",
        "artifact_partition_rank": "3",
        "is_current_artifact": "false",
        "has_current_artifact": "false",
        "has_creating_snapshot": has_creating_snapshot,
        "storage_location_count": storage_location_count,
    }


def snapshot_storage_row(
    snapshot_id: str = SNAPSHOT_ONE,
    storage_id: str = STORAGE_ONE,
    *,
    sandbox_id: str = SANDBOX_ONE,
    uri: str | None = None,
) -> prune.SnapshotStorageRow:
    if uri is None:
        uri = (
            "s3://example-snapshots/"
            f"snapshots/one/{snapshot_id}?region=us-west-2"
        )
    return prune.classify_snapshot_storage_rows(
        [
            {
                "sandbox_id": sandbox_id,
                "snapshot_id": snapshot_id,
                "snapshot_storage_location_id": storage_id,
                "storage_connection_string": uri,
                "has_encrypted_fields": "false",
            }
        ],
        "example-snapshots",
    )[0]


def keep_snapshot_for_sandbox(
    snapshots: list[tuple[str, bool]],
    keep_count: int,
) -> dict[str, bool]:
    """Python port of snapshot_generation_keep_predicate_sql for one sandbox.

    Each snapshot is (snapshot_id, is_current_artifact); the list order is
    newest-first by timestamp. Mirrors the window functions the SQL computes
    (newest_rank, artifact_partition_rank, has_current_artifact) so the keep
    invariants can be exercised without a live Postgres. A snapshot is pruned
    iff it is NOT kept.
    """
    has_current_artifact = any(is_current for _, is_current in snapshots)
    partition_rank: dict[bool, int] = {True: 0, False: 0}
    keep: dict[str, bool] = {}
    for newest_rank, (snapshot_id, is_current) in enumerate(snapshots, start=1):
        partition_rank[is_current] += 1
        artifact_partition_rank = partition_rank[is_current]
        kept = (
            newest_rank == 1
            or (is_current and artifact_partition_rank == 1)
            or (
                not is_current
                and artifact_partition_rank
                <= max(0, keep_count - (1 if has_current_artifact else 0))
            )
        )
        keep[snapshot_id] = kept
    return keep


def snapshot_storage_uri(snapshot_id: str) -> str:
    return (
        "s3://example-snapshots/"
        f"snapshots/{snapshot_id}?region=us-west-2"
    )


def setup_minimal_snapshot_pruning_schema(runner: PostgresRunner) -> None:
    runner.execute_script(
        """
CREATE TABLE sandboxes (
    id uuid PRIMARY KEY,
    current_local_artifact_id uuid,
    current_local_artifact_kind text
);
CREATE TABLE snapshots (
    id uuid PRIMARY KEY,
    sandbox_id uuid NOT NULL REFERENCES sandboxes(id),
    timestamp timestamptz NOT NULL,
    local_artifact_id uuid,
    artifact_kind text,
    state text NOT NULL,
    type text NOT NULL
);
CREATE TABLE snapshot_storage_locations (
    id uuid PRIMARY KEY,
    snapshot_id uuid REFERENCES snapshots(id),
    orphaned_at timestamptz,
    storage_connection_string text NOT NULL
);
CREATE TABLE sandbox_k8s_executions (
    sandbox_id uuid NOT NULL,
    active boolean NOT NULL
);
"""
    )


def insert_snapshot_fixture_row(
    runner: PostgresRunner,
    sandbox_id: str,
    snapshot_id: str,
    storage_id: str,
    timestamp: str,
) -> None:
    runner.execute(
        """
INSERT INTO snapshots (
    id,
    sandbox_id,
    timestamp,
    local_artifact_id,
    artifact_kind,
    state,
    type
) VALUES (%s, %s, %s, NULL, 'full', 'ready', 'full')
""",
        (snapshot_id, sandbox_id, timestamp),
    )
    runner.execute(
        """
INSERT INTO snapshot_storage_locations (
    id,
    snapshot_id,
    orphaned_at,
    storage_connection_string
) VALUES (%s, %s, NULL, %s)
""",
        (storage_id, snapshot_id, snapshot_storage_uri(snapshot_id)),
    )


def seed_snapshot_pruning_fixture(runner: PostgresRunner) -> None:
    runner.execute(
        """
INSERT INTO sandboxes (
    id,
    current_local_artifact_id,
    current_local_artifact_kind
) VALUES (%s, NULL, 'full'), (%s, NULL, 'full')
""",
        (SANDBOX_ONE, SANDBOX_TWO),
    )
    insert_snapshot_fixture_row(
        runner,
        SANDBOX_ONE,
        DORMANT_NEW_SNAPSHOT,
        DORMANT_NEW_STORAGE,
        "2026-01-02 00:00:00+00",
    )
    insert_snapshot_fixture_row(
        runner,
        SANDBOX_ONE,
        SNAPSHOT_ONE,
        STORAGE_ONE,
        "2026-01-01 00:00:00+00",
    )
    insert_snapshot_fixture_row(
        runner,
        SANDBOX_TWO,
        ACTIVE_NEW_SNAPSHOT,
        ACTIVE_NEW_STORAGE,
        "2026-01-02 00:00:00+00",
    )
    insert_snapshot_fixture_row(
        runner,
        SANDBOX_TWO,
        ACTIVE_OLD_SNAPSHOT,
        ACTIVE_OLD_STORAGE,
        "2026-01-01 00:00:00+00",
    )


TEMPLATE_PRUNE_TEMPLATE = "code-runtime"
TEMPLATE_PRUNE_BUCKET = "s3://example-template-artifacts/code-runtime"
# Each release: id, hash, created_at, storage prefix. With
# --keep-latest-per-template 1, only the lone release is safe to delete.
TEMPLATE_PRUNE_RELEASES = {
    # An original and its twin, bound to another environment, which names the
    # original's storage prefix. The twin is the newest row of the template.
    "twinned_original": ("019e5f38-cd2e-7400-b3c8-e33ae3669301", "sha256:aa", "2026-01-01", "aa/"),
    "twin": ("019e5f38-cd2e-7400-b3c8-e33ae3669302", "sha256:aa", "2026-01-06", "aa/"),
    # Nothing else shares its digest or storage: the one deletable release.
    "lone": ("019e5f38-cd2e-7400-b3c8-e33ae3669303", "sha256:bb", "2026-01-02", "bb/"),
    # Two releases whose storage prefixes nest.
    "parent_prefix": ("019e5f38-cd2e-7400-b3c8-e33ae3669304", "sha256:cc", "2026-01-03", "shared/"),
    "nested_prefix": ("019e5f38-cd2e-7400-b3c8-e33ae3669305", "sha256:dd", "2026-01-04", "shared/dd/"),
    # Both spellings of one digest.
    "bare_spelling": ("019e5f38-cd2e-7400-b3c8-e33ae3669306", "ee", "2026-01-02 12:00", "ee-bare/"),
    "prefixed_spelling": ("019e5f38-cd2e-7400-b3c8-e33ae3669307", "sha256:ee", "2026-01-03 12:00", "ee/"),
    # Two prefixes that differ only in the query, which deletion drops: both
    # releases resolve to one deletion target.
    "region_west": ("019e5f38-cd2e-7400-b3c8-e33ae3669309", "sha256:gg", "2026-01-02 06:00", "regional/?region=us-west-2"),
    "region_east": ("019e5f38-cd2e-7400-b3c8-e33ae366930a", "sha256:hh", "2026-01-02 07:00", "regional/?region=us-east-1"),
    # One key is a string prefix of the other without sharing a path segment.
    "short_key": ("019e5f38-cd2e-7400-b3c8-e33ae366930b", "sha256:ii", "2026-01-02 08:00", "split"),
    "longer_key": ("019e5f38-cd2e-7400-b3c8-e33ae366930c", "sha256:jj", "2026-01-02 09:00", "split-two/"),
    # Nested prefixes whose schemes differ in case, which deletion lowercases.
    "lower_scheme": ("019e5f38-cd2e-7400-b3c8-e33ae366930d", "sha256:kk", "2026-01-02 10:00", "cased/"),
    "upper_scheme": (
        "019e5f38-cd2e-7400-b3c8-e33ae366930e",
        "sha256:ll",
        "2026-01-02 11:00",
        "S3://example-template-artifacts/code-runtime/cased/ll/",
    ),
    # The newest original, kept by --keep-latest-per-template 1 even though the
    # twin was registered after it.
    "newest_original": ("019e5f38-cd2e-7400-b3c8-e33ae3669308", "sha256:ff", "2026-01-05", "ff/"),
}


def setup_minimal_template_pruning_schema(runner: PostgresRunner) -> None:
    runner.execute_script(
        """
CREATE TYPE state_reconciliation_reason AS ENUM ('template_desired_state_updated');
CREATE TABLE templates (name text PRIMARY KEY);
CREATE TABLE template_releases (
    id uuid PRIMARY KEY,
    template_name text NOT NULL REFERENCES templates(name),
    type text NOT NULL,
    hash text NOT NULL,
    created_at timestamptz NOT NULL
);
CREATE TABLE template_release_storage_locations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    template_release_id uuid NOT NULL REFERENCES template_releases(id),
    storage_connection_string text NOT NULL
);
CREATE TABLE template_release_storage_location_encrypted_fields (
    template_release_storage_location_id uuid NOT NULL
);
CREATE TABLE template_release_tags (
    template_release_id uuid NOT NULL REFERENCES template_releases(id),
    tag text NOT NULL
);
CREATE TABLE kubernetes_pool_configs (template_release_id uuid);
CREATE TABLE kubernetes_pool_slots (template_release_id uuid);
CREATE TABLE nodes (id uuid PRIMARY KEY, role text NOT NULL, managed_by text NOT NULL);
CREATE TABLE node_direct (node_id uuid PRIMARY KEY, deleted_at timestamptz);
CREATE TABLE node_reconcile_queue (
    node_id uuid NOT NULL,
    facet text NOT NULL,
    element text NOT NULL,
    generation bigint NOT NULL DEFAULT 0,
    requested_at timestamptz NOT NULL,
    next_attempt_at timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    last_error text NOT NULL DEFAULT '',
    dead_at timestamptz,
    dead_reason text NOT NULL DEFAULT '',
    updated_at timestamptz NOT NULL,
    UNIQUE (node_id, facet, element)
);
CREATE TABLE node_reconcile_triggers (
    node_id uuid PRIMARY KEY,
    generation bigint NOT NULL DEFAULT 0,
    reason text NOT NULL,
    reasons state_reconciliation_reason[] NOT NULL,
    requested_at timestamptz NOT NULL,
    next_attempt_at timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    last_error text NOT NULL DEFAULT '',
    dead_at timestamptz,
    dead_reason text NOT NULL DEFAULT '',
    updated_at timestamptz NOT NULL
);
"""
    )
    runner.execute("INSERT INTO templates (name) VALUES (%s)", (TEMPLATE_PRUNE_TEMPLATE,))
    for release_id, digest, created_at, prefix in TEMPLATE_PRUNE_RELEASES.values():
        runner.execute(
            """
INSERT INTO template_releases (id, template_name, type, hash, created_at)
VALUES (%s, %s, 'fc', %s, %s::timestamptz)
""",
            (release_id, TEMPLATE_PRUNE_TEMPLATE, digest, created_at),
        )
        runner.execute(
            """
INSERT INTO template_release_storage_locations (template_release_id, storage_connection_string)
VALUES (%s, %s)
""",
            (release_id, prefix if "://" in prefix else f"{TEMPLATE_PRUNE_BUCKET}/{prefix}"),
        )


def execute_script_with_copy(runner: PostgresRunner, script: str) -> str:
    before_copy, found, copy_and_suffix = script.partition("COPY (")
    assert found
    copy_body, found, suffix = copy_and_suffix.partition(") TO STDOUT WITH CSV HEADER;")
    assert found
    runner.execute_script(before_copy)
    output = runner.copy_stdout(f"COPY ({copy_body}) TO STDOUT WITH CSV HEADER;")
    runner.execute_script(suffix)
    return output


def execute_snapshot_mutation_with_activation_race(
    runner: PostgresRunner,
    script: str,
) -> str:
    mutation_marker = "CREATE TEMP TABLE mutation_results"
    before_mutation, found, after_mutation = script.partition(mutation_marker)
    assert found
    runner.execute_script(before_mutation)
    runner.execute(
        "INSERT INTO sandbox_k8s_executions (sandbox_id, active) VALUES (%s, true)",
        (SANDBOX_TWO,),
    )

    script_after_race = f"{mutation_marker}{after_mutation}"
    before_copy, found, copy_and_suffix = script_after_race.partition("COPY (")
    assert found
    copy_body, found, suffix = copy_and_suffix.partition(") TO STDOUT WITH CSV HEADER;")
    assert found
    runner.execute_script(before_copy)
    output = runner.copy_stdout(f"COPY ({copy_body}) TO STDOUT WITH CSV HEADER;")
    runner.execute_script(suffix)
    return output


class PruneClusterArtifactsTests(unittest.TestCase):
    def test_live_pod_sandbox_id_metadata_protects_candidate(self) -> None:
        live_pods = {
            "orphaned-live-pod": prune.PodInfo(
                name="orphaned-live-pod",
                phase="Running",
                node_name="node-a",
                created_at="2026-01-02T00:00:00Z",
                labels={prune.SANDBOX_ID_METADATA_KEY: SANDBOX_ONE},
                annotations={},
            )
        }

        candidates = prune.classify_sandbox_candidates(
            [sandbox_row(SANDBOX_ONE)],
            live_pods,
        )

        self.assertEqual(candidates[0].decision, "skip")
        self.assertEqual(candidates[0].decision_reason, "live_kubernetes_pod_by_sandbox_id")

    def test_dormant_sandbox_query_excludes_failed_lifecycle(self) -> None:
        args = make_args("/tmp")
        queries: list[str] = []

        def capture_query(_args: argparse.Namespace, query: str) -> list[dict[str, str]]:
            queries.append(query)
            return []

        with mock.patch.object(prune, "psql_csv_query", side_effect=capture_query):
            prune.sandbox_candidate_rows(args, "2026-01-08 00:00:00+00")

        self.assertIn("s.lifecycle = 'suspended'", queries[0])
        self.assertNotIn("'failed'", queries[0])

    def test_non_sweepable_snapshot_storage_blocks_sandbox_candidate(self) -> None:
        rows = prune.classify_snapshot_storage_rows(
            [
                {
                    "sandbox_id": SANDBOX_ONE,
                    "snapshot_id": "019e5f38-cd2e-7400-b3c8-e33ae36692d9",
                    "snapshot_storage_location_id": "019e5f38-cd2e-7400-b3c8-e33ae36692da",
                    "storage_connection_string": "s3://other-bucket/snapshots/one/?region=us-west-2",
                    "has_encrypted_fields": "false",
                }
            ],
            "example-snapshots",
        )

        candidates = prune.classify_sandbox_candidates(
            [sandbox_row(SANDBOX_ONE)],
            {},
            rows,
        )

        self.assertEqual(candidates[0].decision, "skip")
        self.assertEqual(
            candidates[0].decision_reason,
            "snapshot_storage_not_sweepable:bucket_out_of_scope",
        )

    def test_snapshot_storage_missing_region_blocks_sandbox_candidate(self) -> None:
        rows = prune.classify_snapshot_storage_rows(
            [
                {
                    "sandbox_id": SANDBOX_ONE,
                    "snapshot_id": "019e5f38-cd2e-7400-b3c8-e33ae36692d9",
                    "snapshot_storage_location_id": "019e5f38-cd2e-7400-b3c8-e33ae36692da",
                    "storage_connection_string": "s3://example-snapshots/snapshots/one/",
                    "has_encrypted_fields": "false",
                }
            ],
            "example-snapshots",
        )

        candidates = prune.classify_sandbox_candidates(
            [sandbox_row(SANDBOX_ONE)],
            {},
            rows,
        )

        self.assertEqual(rows[0].sweepability_decision, "skip")
        self.assertEqual(rows[0].sweepability_reason, "parse_uri")
        self.assertEqual(candidates[0].decision, "skip")
        self.assertEqual(
            candidates[0].decision_reason,
            "snapshot_storage_not_sweepable:parse_uri",
        )

    def test_snapshot_generation_query_keeps_current_plus_rollback(self) -> None:
        args = make_args("/tmp")
        queries: list[str] = []

        def capture_query(_args: argparse.Namespace, query: str) -> list[dict[str, str]]:
            queries.append(query)
            return []

        with mock.patch.object(prune, "psql_csv_query", side_effect=capture_query):
            prune.snapshot_generation_candidate_rows(args)

        query = queries[0]
        self.assertIn("BOOL_OR(is_current_artifact)", query)
        self.assertIn("is_current_artifact AND artifact_partition_rank = 1", query)
        self.assertIn(
            "keep_count - CASE WHEN has_current_artifact THEN 1 ELSE 0",
            query,
        )
        self.assertIn("make_interval(hours => 1)", query)

    def test_snapshot_generation_query_skips_active_execution_sandboxes(self) -> None:
        args = make_args("/tmp")
        queries: list[str] = []

        def capture_query(_args: argparse.Namespace, query: str) -> list[dict[str, str]]:
            queries.append(query)
            return []

        with mock.patch.object(prune, "psql_csv_query", side_effect=capture_query):
            prune.snapshot_generation_candidate_rows(args)

        query = queries[0]
        self.assertIn("FROM sandbox_k8s_executions e", query)
        self.assertIn("e.sandbox_id = s.id", query)
        self.assertIn("e.active = true", query)
        # The guard belongs in ready_full so active sandboxes never rank at all.
        self.assertLess(
            query.index("sandbox_k8s_executions e"),
            query.index("ranked AS"),
        )

    def test_snapshot_generation_live_sql_skips_active_execution_sandboxes(self) -> None:
        args = make_args("/tmp")
        candidate = prune.SnapshotGenerationCandidate(
            **snapshot_generation_row(),
            decision="orphan",
            decision_reason="",
        )
        scripts: list[str] = []

        def capture_script(_args: argparse.Namespace, script: str) -> str:
            scripts.append(script)
            return (
                "record_type,sandbox_id,snapshot_id,snapshot_timestamp,"
                "snapshot_storage_location_id,storage_connection_string\n"
            )

        with mock.patch.object(prune, "psql_script", side_effect=capture_script):
            prune.orphan_snapshot_generation_batch(
                args,
                [candidate],
                [snapshot_storage_row()],
            )

        script = scripts[0]
        self.assertIn("FROM sandbox_k8s_executions e", script)
        self.assertIn("e.sandbox_id = s.id", script)
        self.assertIn("e.active = true", script)
        # Guard sits inside ready_full, before selected_snapshots is populated.
        self.assertLess(
            script.index("sandbox_k8s_executions e"),
            script.index("INSERT INTO selected_snapshots"),
        )

    def test_snapshot_generation_live_sql_rechecks_active_execution_at_orphaning(
        self,
    ) -> None:
        args = make_args("/tmp")
        candidate = prune.SnapshotGenerationCandidate(
            **snapshot_generation_row(),
            decision="orphan",
            decision_reason="",
        )
        scripts: list[str] = []

        def capture_script(_args: argparse.Namespace, script: str) -> str:
            scripts.append(script)
            return (
                "record_type,sandbox_id,snapshot_id,snapshot_timestamp,"
                "snapshot_storage_location_id,storage_connection_string\n"
            )

        with mock.patch.object(prune, "psql_script", side_effect=capture_script):
            prune.orphan_snapshot_generation_batch(
                args,
                [candidate],
                [snapshot_storage_row()],
            )

        script = scripts[0]
        update_start = script.index("UPDATE snapshot_storage_locations ssl")
        delete_start = script.index("DELETE FROM snapshots sn")
        self.assertIn("WHERE e.sandbox_id = selected.sandbox_id", script[update_start:])
        self.assertIn("AND e.active = true", script[update_start:])
        self.assertIn(
            "WHERE e.sandbox_id = selected.sandbox_id",
            script[delete_start:],
        )
        self.assertIn("AND e.active = true", script[delete_start:])

    def test_keep_predicate_sql_always_keeps_newest_by_timestamp(self) -> None:
        predicate = prune.snapshot_generation_keep_predicate_sql()
        self.assertIn("newest_rank = 1", predicate)

    def test_keep_policy_never_prunes_newest_without_artifact_match(self) -> None:
        # No snapshot matches current_local_artifact_id: every row is_current=False.
        snapshots = [
            ("newest", False),
            ("middle", False),
            ("oldest", False),
        ]
        for keep_count in (2, 1):
            keep = keep_snapshot_for_sandbox(snapshots, keep_count)
            self.assertTrue(
                keep["newest"],
                f"newest snapshot must be retained at keep_count={keep_count}",
            )

    def test_keep_policy_retains_newest_when_stale_artifact_match_exists(self) -> None:
        # Genuine newest does NOT match the current artifact, but a stale older
        # snapshot does. At keep_count=1 the newest must still be retained.
        snapshots = [
            ("newest", False),
            ("stale_current", True),
        ]
        keep = keep_snapshot_for_sandbox(snapshots, keep_count=1)
        self.assertTrue(keep["newest"])
        self.assertTrue(keep["stale_current"])

    def test_snapshot_generation_classification_requires_sweepable_storage(self) -> None:
        candidate = prune.classify_snapshot_generation_candidates(
            [snapshot_generation_row()],
            [snapshot_storage_row()],
        )[0]

        self.assertEqual(candidate.decision, "orphan")
        self.assertEqual(candidate.decision_reason, "")

    def test_snapshot_generation_classification_blocks_creating_sandbox(self) -> None:
        candidate = prune.classify_snapshot_generation_candidates(
            [snapshot_generation_row(has_creating_snapshot="true")],
            [snapshot_storage_row()],
        )[0]

        self.assertEqual(candidate.decision, "skip")
        self.assertEqual(candidate.decision_reason, "sandbox_has_creating_snapshot")

    def test_snapshot_generation_classification_blocks_non_sweepable_storage(self) -> None:
        candidate = prune.classify_snapshot_generation_candidates(
            [snapshot_generation_row()],
            [
                snapshot_storage_row(
                    uri="s3://other-bucket/snapshots/one/?region=us-west-2",
                )
            ],
        )[0]

        self.assertEqual(candidate.decision, "skip")
        self.assertEqual(
            candidate.decision_reason,
            "snapshot_storage_not_sweepable:bucket_out_of_scope",
        )

    def test_snapshot_generation_classification_blocks_legacy_sandbox_prefix(self) -> None:
        candidate = prune.classify_snapshot_generation_candidates(
            [snapshot_generation_row()],
            [
                snapshot_storage_row(
                    uri=(
                        "s3://example-snapshots/"
                        f"suspend_snapshots/00/{SANDBOX_ONE}?region=us-west-2"
                    ),
                )
            ],
        )[0]

        self.assertEqual(candidate.decision, "skip")
        self.assertEqual(candidate.decision_reason, "storage_uri_not_snapshot_scoped")

    def test_snapshot_generation_live_sql_orphans_storage_before_snapshot_delete(self) -> None:
        args = make_args("/tmp")
        candidate = prune.SnapshotGenerationCandidate(
            **snapshot_generation_row(),
            decision="orphan",
            decision_reason="",
        )
        scripts: list[str] = []

        def capture_script(_args: argparse.Namespace, script: str) -> str:
            scripts.append(script)
            return (
                "record_type,sandbox_id,snapshot_id,snapshot_timestamp,"
                "snapshot_storage_location_id,storage_connection_string\n"
            )

        with mock.patch.object(prune, "psql_script", side_effect=capture_script):
            prune.orphan_snapshot_generation_batch(
                args,
                [candidate],
                [snapshot_storage_row()],
            )

        script = scripts[0]
        self.assertIn("CREATE TEMP TABLE sweepable_snapshot_storage_locations", script)
        self.assertIn("SET snapshot_id = NULL", script)
        self.assertIn("orphaned_at = NOW()", script)
        self.assertLess(script.index("UPDATE snapshot_storage_locations"), script.index("DELETE FROM snapshots"))
        self.assertIn("FROM snapshot_storage_locations ssl", script)
        self.assertIn("WHERE ssl.snapshot_id = sn.id", script)

    def test_snapshot_generation_mutation_rechecks_active_execution_in_postgres(
        self,
    ) -> None:
        runner = postgres_runner_or_skip(self)
        schema = f"prune_cluster_artifacts_test_{uuid.uuid4().hex}"
        try:
            runner.execute(f"CREATE SCHEMA {schema}")
            runner.execute(f"SET search_path TO {schema}")
            setup_minimal_snapshot_pruning_schema(runner)
            seed_snapshot_pruning_fixture(runner)

            args = make_args("/tmp")
            args.keep_snapshot_generations_per_sandbox = 1
            candidates = [
                prune.SnapshotGenerationCandidate(
                    **snapshot_generation_row(
                        SNAPSHOT_ONE,
                        sandbox_id=SANDBOX_ONE,
                    ),
                    decision="orphan",
                    decision_reason="",
                ),
                prune.SnapshotGenerationCandidate(
                    **snapshot_generation_row(
                        ACTIVE_OLD_SNAPSHOT,
                        sandbox_id=SANDBOX_TWO,
                    ),
                    decision="orphan",
                    decision_reason="",
                ),
            ]
            storage_rows = [
                snapshot_storage_row(
                    SNAPSHOT_ONE,
                    STORAGE_ONE,
                    sandbox_id=SANDBOX_ONE,
                ),
                snapshot_storage_row(
                    ACTIVE_OLD_SNAPSHOT,
                    ACTIVE_OLD_STORAGE,
                    sandbox_id=SANDBOX_TWO,
                ),
            ]
            scripts: list[str] = []

            def execute_script(_args: argparse.Namespace, script: str) -> str:
                scripts.append(script)
                return execute_snapshot_mutation_with_activation_race(runner, script)

            with mock.patch.object(prune, "psql_script", side_effect=execute_script):
                mutation_rows = prune.orphan_snapshot_generation_batch(
                    args,
                    candidates,
                    storage_rows,
                )

            self.assertEqual(
                {(row["record_type"], row["snapshot_id"]) for row in mutation_rows},
                {
                    ("orphaned_storage", SNAPSHOT_ONE),
                    ("deleted_snapshot", SNAPSHOT_ONE),
                },
            )
            dormant_snapshot_id, dormant_orphaned = runner.fetchone(
                """
SELECT snapshot_id::text, orphaned_at IS NOT NULL
FROM snapshot_storage_locations
WHERE id = %s
""",
                (STORAGE_ONE,),
            )
            active_snapshot_id, active_orphaned = runner.fetchone(
                """
SELECT snapshot_id::text, orphaned_at IS NOT NULL
FROM snapshot_storage_locations
WHERE id = %s
""",
                (ACTIVE_OLD_STORAGE,),
            )
            active_snapshot_exists = runner.fetchone(
                "SELECT EXISTS (SELECT 1 FROM snapshots WHERE id = %s)",
                (ACTIVE_OLD_SNAPSHOT,),
            )[0]
            dormant_snapshot_exists = runner.fetchone(
                "SELECT EXISTS (SELECT 1 FROM snapshots WHERE id = %s)",
                (SNAPSHOT_ONE,),
            )[0]

            self.assertIsNone(dormant_snapshot_id)
            self.assertTrue(dormant_orphaned)
            self.assertEqual(active_snapshot_id, ACTIVE_OLD_SNAPSHOT)
            self.assertFalse(active_orphaned)
            self.assertTrue(active_snapshot_exists)
            self.assertFalse(dormant_snapshot_exists)
        finally:
            try:
                runner.connection.rollback()
            except Exception:
                pass
            runner.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            runner.connection.close()

    def test_live_old_sandbox_prune_deletes_db_before_node_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            args = make_args(tmp)
            rows = [
                sandbox_row(SANDBOX_ONE, updated_at="2026-01-01 00:00:00+00"),
                sandbox_row(SANDBOX_TWO, updated_at="2026-01-02 00:00:00+00"),
            ]
            calls: list[tuple[str, list[str]]] = []

            def delete_sandbox_batch(
                _args: argparse.Namespace,
                batch: list[prune.SandboxCandidate],
                _cutoff: str,
                _storage_rows: list[prune.SnapshotStorageRow],
            ) -> list[dict[str, str]]:
                calls.append(("db", [candidate.sandbox_id for candidate in batch]))
                return [{"sandbox_id": SANDBOX_TWO}]

            def run_node_cleanup(
                _args: argparse.Namespace,
                candidates: list[prune.SandboxCandidate],
            ) -> list[dict[str, object]]:
                calls.append(("node", [candidate.sandbox_id for candidate in candidates]))
                return []

            with (
                mock.patch.object(
                    prune,
                    "db_cutoff",
                    return_value={"cutoff": "2026-01-08 00:00:00+00"},
                ),
                mock.patch.object(prune, "load_live_pods", return_value={}),
                mock.patch.object(prune, "sandbox_candidate_rows", return_value=rows),
                mock.patch.object(prune, "snapshot_storage_rows", return_value=[]),
                mock.patch.object(prune, "delete_sandbox_batch", side_effect=delete_sandbox_batch),
                mock.patch.object(prune, "run_node_cleanup", side_effect=run_node_cleanup),
            ):
                self.assertEqual(prune.run_old_sandboxes(args), 0)

            self.assertEqual(
                calls,
                [
                    ("db", [SANDBOX_ONE, SANDBOX_TWO]),
                    ("node", [SANDBOX_TWO]),
                ],
            )

    def test_live_old_sandbox_prune_exports_actually_orphaned_snapshot_storage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            args = make_args(tmp)
            rows = [
                sandbox_row(SANDBOX_ONE, updated_at="2026-01-01 00:00:00+00"),
                sandbox_row(SANDBOX_TWO, updated_at="2026-01-02 00:00:00+00"),
            ]
            storage_rows = [
                {
                    "sandbox_id": SANDBOX_ONE,
                    "snapshot_id": "019e5f38-cd2e-7400-b3c8-e33ae36692d9",
                    "snapshot_storage_location_id": "019e5f38-cd2e-7400-b3c8-e33ae36692da",
                    "storage_connection_string": (
                        "s3://example-snapshots/snapshots/one/?region=us-west-2"
                    ),
                    "has_encrypted_fields": "false",
                },
                {
                    "sandbox_id": SANDBOX_TWO,
                    "snapshot_id": "019e5f38-cd2e-7400-b3c8-e33ae36692db",
                    "snapshot_storage_location_id": "019e5f38-cd2e-7400-b3c8-e33ae36692dc",
                    "storage_connection_string": (
                        "s3://example-snapshots/snapshots/two/?region=us-west-2"
                    ),
                    "has_encrypted_fields": "false",
                },
            ]

            with (
                mock.patch.object(
                    prune,
                    "db_cutoff",
                    return_value={"cutoff": "2026-01-08 00:00:00+00"},
                ),
                mock.patch.object(prune, "load_live_pods", return_value={}),
                mock.patch.object(prune, "sandbox_candidate_rows", return_value=rows),
                mock.patch.object(prune, "snapshot_storage_rows", return_value=storage_rows),
                mock.patch.object(
                    prune,
                    "delete_sandbox_batch",
                    return_value=[{"sandbox_id": SANDBOX_TWO}],
                ),
                mock.patch.object(prune, "run_node_cleanup", return_value=[]),
            ):
                self.assertEqual(prune.run_old_sandboxes(args), 0)

            exported = (
                Path(tmp) / "old_sandboxes_db_orphaned_snapshot_storage.csv"
            ).read_text()
            self.assertIn(SANDBOX_TWO, exported)
            self.assertIn("019e5f38-cd2e-7400-b3c8-e33ae36692dc", exported)
            self.assertNotIn(SANDBOX_ONE, exported)

    def test_sandbox_delete_sql_locks_before_creating_snapshot_guard(self) -> None:
        args = make_args("/tmp")
        candidate = prune.SandboxCandidate(
            **sandbox_row(SANDBOX_ONE),
            decision="delete",
            decision_reason="",
        )
        scripts: list[str] = []

        def capture_script(_args: argparse.Namespace, script: str) -> str:
            scripts.append(script)
            return "sandbox_id\n"

        with mock.patch.object(prune, "psql_script", side_effect=capture_script):
            prune.delete_sandbox_batch(args, [candidate], "2026-01-08 00:00:00+00")

        script = scripts[0]
        self.assertLess(script.index("FOR UPDATE"), script.index("state = 'creating'"))
        self.assertIn("s.lifecycle = 'suspended'", script)
        self.assertNotIn("'failed'", script)

    def test_template_delete_sql_locks_the_template_before_its_checks(self) -> None:
        runner = postgres_runner_or_skip(self)
        registrar = postgres_runner_or_skip(self)
        schema = f"prune_cluster_artifacts_test_{uuid.uuid4().hex}"
        worker: threading.Thread | None = None
        try:
            runner.execute(f"CREATE SCHEMA {schema}")
            runner.execute(f"SET search_path TO {schema}")
            registrar.execute(f"SET search_path TO {schema}")
            setup_minimal_template_pruning_schema(runner)
            args = make_args("/tmp")
            cutoff = "2100-01-01 00:00:00+00"
            lone_id, lone_digest, _, _ = TEMPLATE_PRUNE_RELEASES["lone"]
            candidate = prune.TemplateCandidate(
                **{
                    **template_candidate().__dict__,
                    "template_release_id": lone_id,
                    "template_name": TEMPLATE_PRUNE_TEMPLATE,
                }
            )

            # A registration holds the template's row lock, as the internal API
            # takes it, while it inserts a twin of the candidate.
            registrar.execute("BEGIN")
            registrar.execute(
                "SELECT name FROM templates WHERE name = %s FOR UPDATE",
                (TEMPLATE_PRUNE_TEMPLATE,),
            )
            registrar.execute(
                """
INSERT INTO template_releases (id, template_name, type, hash, created_at)
VALUES (%s, %s, 'fc', %s, '2026-01-09'::timestamptz)
""",
                (str(uuid.uuid4()), TEMPLATE_PRUNE_TEMPLATE, lone_digest),
            )

            delete_pid = runner.fetchone("SELECT pg_backend_pid()")[0]
            outcome: dict[str, object] = {}

            def delete() -> None:
                try:
                    with mock.patch.object(
                        prune,
                        "psql_script",
                        side_effect=lambda _args, script: execute_script_with_copy(runner, script),
                    ):
                        outcome["rows"] = prune.delete_template_release_db(args, candidate, cutoff)
                except Exception as error:  # surfaced by the assertion below
                    outcome["error"] = error

            worker = threading.Thread(target=delete)
            worker.start()
            # Without a template lock taken before its checks, the delete
            # finishes here, before the twin commits.
            waited = False
            deadline = time.monotonic() + 30
            while worker.is_alive() and time.monotonic() < deadline:
                waited = bool(
                    registrar.fetchone(
                        "SELECT count(*) FROM pg_stat_activity"
                        " WHERE pid = %s AND wait_event_type = 'Lock'",
                        (delete_pid,),
                    )[0]
                )
                if waited:
                    break
                time.sleep(0.05)
            registrar.execute("COMMIT")
            worker.join(30)
            self.assertFalse(worker.is_alive(), "the delete didn't finish")
            self.assertTrue(waited, "the delete never waited for the registration's lock")
            self.assertNotIn("error", outcome)
            self.assertEqual(outcome["rows"], [])
            self.assertEqual(
                runner.fetchone("SELECT count(*) FROM template_releases WHERE id = %s", (lone_id,))[0],
                1,
            )
        finally:
            # Release the registration first, so a delete still waiting on its
            # lock can finish before its connection is reused.
            try:
                registrar.execute("ROLLBACK")
            except Exception:
                pass
            if worker is not None:
                worker.join(30)
            for connection in (registrar.connection, runner.connection):
                try:
                    connection.rollback()
                except Exception:
                    pass
            registrar.connection.close()
            runner.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            runner.connection.close()

    def test_sandbox_delete_sql_refuses_unclassified_storage_rows(self) -> None:
        args = make_args("/tmp")
        candidate = prune.SandboxCandidate(
            **sandbox_row(SANDBOX_ONE),
            decision="delete",
            decision_reason="",
        )
        storage_row = prune.SnapshotStorageRow(
            sandbox_id=SANDBOX_ONE,
            snapshot_id="019e5f38-cd2e-7400-b3c8-e33ae36692d9",
            snapshot_storage_location_id="019e5f38-cd2e-7400-b3c8-e33ae36692da",
            storage_connection_string=(
                "s3://example-snapshots/snapshots/one/?region=us-west-2"
            ),
            has_encrypted_fields="false",
            sweepability_decision="sweepable",
            sweepability_reason="",
        )
        scripts: list[str] = []

        def capture_script(_args: argparse.Namespace, script: str) -> str:
            scripts.append(script)
            return "sandbox_id\n"

        with mock.patch.object(prune, "psql_script", side_effect=capture_script):
            prune.delete_sandbox_batch(
                args,
                [candidate],
                "2026-01-08 00:00:00+00",
                [storage_row],
            )

        script = scripts[0]
        self.assertIn("sweepable_snapshot_storage_locations", script)
        self.assertIn("NOT EXISTS (", script)
        self.assertIn("snapshot_storage_locations ssl", script)

    def test_superseded_template_prune_deletes_db_before_s3(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            args = make_args(tmp)
            args.mode = "superseded-templates"
            candidate = template_candidate()
            calls: list[tuple[str, list[str]]] = []

            def delete_template_release_db(
                _args: argparse.Namespace,
                _candidate: prune.TemplateCandidate,
                _cutoff: str,
            ) -> list[dict[str, str]]:
                calls.append(("db", [candidate.template_release_id]))
                return [
                    {
                        "template_release_id": candidate.template_release_id,
                        "template_name": candidate.template_name,
                        "content_hash": candidate.content_hash,
                        "storage_uris": candidate.storage_uris,
                    }
                ]

            def delete_template_s3_prefixes(
                _args: argparse.Namespace,
                target: prune.TemplateStorageTarget,
            ) -> tuple[bool, list[dict[str, object]]]:
                calls.append(("s3", target.uri_list()))
                return True, []

            with (
                mock.patch.object(prune, "db_cutoff", return_value={"cutoff": "2026-01-08 00:00:00+00"}),
                mock.patch.object(prune, "template_candidate_rows", return_value=[]),
                mock.patch.object(prune, "classify_template_candidates", return_value=[candidate]),
                mock.patch.object(prune, "delete_template_release_db", side_effect=delete_template_release_db),
                mock.patch.object(prune, "delete_template_s3_prefixes", side_effect=delete_template_s3_prefixes),
            ):
                self.assertEqual(prune.run_superseded_templates(args), 0)

            self.assertEqual(
                calls,
                [
                    ("db", [TEMPLATE_RELEASE]),
                    ("s3", [candidate.storage_uris]),
                ],
            )

    def test_template_db_delete_returns_storage_connection_strings(self) -> None:
        args = make_args("/tmp")
        args.mode = "superseded-templates"
        scripts: list[str] = []

        def capture_script(_args: argparse.Namespace, script: str) -> str:
            scripts.append(script)
            return "template_release_id,template_name,content_hash,storage_uris\n"

        with mock.patch.object(prune, "psql_script", side_effect=capture_script):
            prune.delete_template_release_db(
                args,
                template_candidate(),
                "2026-01-08 00:00:00+00",
            )

        script = scripts[0]
        self.assertIn("storage.storage_connection_string", script)
        self.assertLess(
            script.index("CREATE TEMP TABLE locked_template_storage"),
            script.index("DELETE FROM template_release_storage_locations"),
        )

    def test_the_release_delete_waits_for_a_tag_on_the_other_spelling(self) -> None:
        # A tag writer's foreign-key check holds a key-share lock on the
        # release it tags until it commits. The delete locks every release of
        # the candidate's digest before its tag guard, so a tag being written
        # on the other spelling's release makes it wait instead of deleting
        # the release that tag resolves to.
        runner = postgres_runner_or_skip(self)
        writer = postgres_runner_or_skip(self)
        schema = f"prune_cluster_artifacts_test_{uuid.uuid4().hex}"
        bare, canonical = (str(uuid.uuid4()) for _ in range(2))
        try:
            runner.execute(f"CREATE SCHEMA {schema}")
            runner.execute(f"SET search_path TO {schema}")
            writer.execute(f"SET search_path TO {schema}")
            runner.execute_script(
                """
CREATE TABLE templates (name text PRIMARY KEY);
INSERT INTO templates VALUES ('code-runtime');
CREATE TABLE template_releases (
    id uuid PRIMARY KEY,
    template_name text NOT NULL,
    type text NOT NULL,
    hash text NOT NULL,
    created_at timestamptz NOT NULL
);
CREATE TABLE template_release_tags (
    template_release_id uuid NOT NULL REFERENCES template_releases (id),
    template_name text NOT NULL,
    tag text NOT NULL
);
CREATE TABLE kubernetes_pool_configs (template_release_id uuid);
CREATE TABLE kubernetes_pool_slots (template_release_id uuid)
"""
            )
            digest = "c" * 64
            for release, hash_ in [(bare, digest), (canonical, f"sha256:{digest}")]:
                runner.execute(
                    "INSERT INTO template_releases VALUES "
                    "(%s, 'code-runtime', 'fc', %s, NOW() - INTERVAL '30 days')",
                    (release, hash_),
                )
            scripts: list[str] = []

            def capture_script(_args: argparse.Namespace, script: str) -> str:
                scripts.append(script)
                return ""

            args = make_args("/tmp")
            args.keep_latest_per_template = 0
            candidate = prune.TemplateCandidate(
                template_release_id=bare,
                template_name="code-runtime",
                runtime="fc",
                content_hash=digest,
                created_at="",
                newest_rank="2",
                storage_location_count="0",
                storage_uris="",
                has_encrypted_storage="false",
                decision="delete",
                decision_reason="",
            )
            with mock.patch.object(prune, "psql_script", side_effect=capture_script):
                prune.delete_template_release_db(args, candidate, "2099-01-01 00:00:00+00")
            # Through the tag guard: the statements that lock rows.
            prefix = scripts[0].split("CREATE TEMP TABLE locked_template_storage")[0]
            statements = [
                statement
                for statement in prefix.split(";")
                if statement.strip() and statement.strip() != "BEGIN"
            ]

            writer.execute("BEGIN")
            writer.execute(
                "SELECT 1 FROM template_releases WHERE id = %s FOR KEY SHARE",
                (canonical,),
            )
            runner.execute("BEGIN")
            runner.execute("SET LOCAL lock_timeout = '500ms'")
            with self.assertRaises(Exception) as raised:
                for statement in statements:
                    runner.execute(f"{statement};")
            self.assertIn("lock timeout", str(raised.exception))
            runner.execute("ROLLBACK")
            writer.execute("ROLLBACK")
        finally:
            writer.connection.close()
            runner.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            runner.connection.close()

    def test_a_tag_on_either_spelling_protects_the_digest(self) -> None:
        # A tag written before the shared original-release rule can name the
        # other spelling's release of a digest, and lookups resolve it to the
        # digest original, so neither spelling's release is a candidate.
        runner = postgres_runner_or_skip(self)
        schema = f"prune_cluster_artifacts_test_{uuid.uuid4().hex}"
        bare, canonical, untagged = (str(uuid.uuid4()) for _ in range(3))
        try:
            runner.execute(f"CREATE SCHEMA {schema}")
            runner.execute(f"SET search_path TO {schema}")
            runner.execute_script(
                """
CREATE TABLE template_releases (
    id uuid PRIMARY KEY,
    template_name text NOT NULL,
    type text NOT NULL,
    hash text NOT NULL,
    created_at timestamptz NOT NULL
);
CREATE TABLE template_release_tags (
    template_release_id uuid NOT NULL,
    template_name text NOT NULL,
    tag text NOT NULL
);
CREATE TABLE kubernetes_pool_configs (template_release_id uuid);
CREATE TABLE kubernetes_pool_slots (template_release_id uuid);
CREATE TABLE template_release_storage_locations (
    id uuid PRIMARY KEY,
    template_release_id uuid NOT NULL,
    storage_connection_string text NOT NULL
);
CREATE TABLE template_release_storage_location_encrypted_fields (
    template_release_storage_location_id uuid NOT NULL
)
"""
            )
            digest = "a" * 64
            for release, hash_, days in [
                (bare, digest, 30),
                (canonical, f"sha256:{digest}", 20),
                (untagged, "sha256:" + "b" * 64, 30),
            ]:
                runner.execute(
                    "INSERT INTO template_releases VALUES "
                    "(%s, 'code-runtime', 'fc', %s, NOW() - %s * INTERVAL '1 day')",
                    (release, hash_, days),
                )
            runner.execute(
                "INSERT INTO template_release_tags VALUES (%s, 'code-runtime', 'latest')",
                (canonical,),
            )

            def query(_args: argparse.Namespace, script: str) -> str:
                body = script.strip().removesuffix(";")
                return runner.copy_stdout(body)

            args = make_args("/tmp")
            args.mode = "superseded-templates"
            args.keep_latest_per_template = 0
            with mock.patch.object(prune, "psql_script", side_effect=query):
                rows = prune.template_candidate_rows(
                    args, str(runner.fetchone("SELECT NOW()")[0])
                )
            self.assertEqual(
                [row["template_release_id"] for row in rows],
                [untagged],
            )
        finally:
            runner.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            runner.connection.close()

    def test_superseded_template_prune_skips_s3_when_db_refuses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            args = make_args(tmp)
            args.mode = "superseded-templates"
            candidate = template_candidate()
            s3_calls: list[prune.TemplateStorageTarget] = []

            with (
                mock.patch.object(prune, "db_cutoff", return_value={"cutoff": "2026-01-08 00:00:00+00"}),
                mock.patch.object(prune, "template_candidate_rows", return_value=[]),
                mock.patch.object(prune, "classify_template_candidates", return_value=[candidate]),
                mock.patch.object(prune, "delete_template_release_db", return_value=[]),
                mock.patch.object(
                    prune,
                    "delete_template_s3_prefixes",
                    side_effect=lambda _args, target: s3_calls.append(target),
                ),
            ):
                self.assertEqual(prune.run_superseded_templates(args), 2)

            self.assertEqual(s3_calls, [])

    def test_superseded_template_prune_spares_releases_that_share_artifacts(self) -> None:
        runner = postgres_runner_or_skip(self)
        schema = f"prune_cluster_artifacts_test_{uuid.uuid4().hex}"
        try:
            runner.execute(f"CREATE SCHEMA {schema}")
            runner.execute(f"SET search_path TO {schema}")
            setup_minimal_template_pruning_schema(runner)
            args = make_args("/tmp")
            args.mode = "superseded-templates"
            args.keep_latest_per_template = 1
            cutoff = "2100-01-01 00:00:00+00"
            ids = {name: release[0] for name, release in TEMPLATE_PRUNE_RELEASES.items()}

            def run_query(_args: argparse.Namespace, query: str) -> list[dict[str, str]]:
                return prune.parse_csv(
                    runner.copy_stdout(f"COPY ({query}) TO STDOUT WITH CSV HEADER")
                )

            with mock.patch.object(prune, "psql_csv_query", side_effect=run_query):
                rows = prune.template_candidate_rows(args, cutoff)
            self.assertEqual([row["template_release_id"] for row in rows], [ids["lone"]])

            # The report refuses a location with an empty canonical form, but
            # the delete doesn't, so there it has to overlap nothing.
            runner.execute(
                """
INSERT INTO template_release_storage_locations (template_release_id, storage_connection_string)
VALUES (%s, '?region=us-west-2')
""",
                (TEMPLATE_PRUNE_RELEASES["newest_original"][0],),
            )

            # The delete re-checks under lock, so it refuses every other release
            # even when asked for it directly.
            deleted: list[str] = []
            for name, release_id in ids.items():
                candidate = prune.TemplateCandidate(
                    **{
                        **template_candidate().__dict__,
                        "template_release_id": release_id,
                        "template_name": TEMPLATE_PRUNE_TEMPLATE,
                    }
                )
                with mock.patch.object(
                    prune,
                    "psql_script",
                    side_effect=lambda _args, script: execute_script_with_copy(runner, script),
                ):
                    rows = prune.delete_template_release_db(args, candidate, cutoff)
                deleted.extend(row["template_release_id"] for row in rows)
            self.assertEqual(deleted, [ids["lone"]])

            remaining = runner.fetchone(
                "SELECT ARRAY_AGG(id::text ORDER BY id) FROM template_releases"
            )[0]
            self.assertEqual(
                sorted(remaining),
                sorted(release_id for name, release_id in ids.items() if name != "lone"),
            )
        finally:
            try:
                runner.connection.rollback()
            except Exception:
                pass
            runner.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            runner.connection.close()

    def test_superseded_template_ranking_counts_both_spellings_of_a_digest_once(self) -> None:
        runner = postgres_runner_or_skip(self)
        schema = f"prune_cluster_artifacts_test_{uuid.uuid4().hex}"
        try:
            runner.execute(f"CREATE SCHEMA {schema}")
            runner.execute(f"SET search_path TO {schema}")
            setup_minimal_template_pruning_schema(runner)
            runner.execute("DELETE FROM template_release_storage_locations")
            runner.execute("DELETE FROM template_releases")
            # Two versions of the template: an older one, and a newer one
            # registered under both spellings of its digest.
            releases = {
                "older": ("019e5f38-cd2e-7400-b3c8-e33ae3669401", "sha256:mm", "2026-01-01"),
                "bare": ("019e5f38-cd2e-7400-b3c8-e33ae3669402", "nn", "2026-01-02"),
                "prefixed": ("019e5f38-cd2e-7400-b3c8-e33ae3669403", "sha256:nn", "2026-01-03"),
            }
            for name, (release_id, digest, created_at) in releases.items():
                runner.execute(
                    """
INSERT INTO template_releases (id, template_name, type, hash, created_at)
VALUES (%s, %s, 'fc', %s, %s::timestamptz)
""",
                    (release_id, TEMPLATE_PRUNE_TEMPLATE, digest, created_at),
                )
                runner.execute(
                    """
INSERT INTO template_release_storage_locations (template_release_id, storage_connection_string)
VALUES (%s, %s)
""",
                    (release_id, f"{TEMPLATE_PRUNE_BUCKET}/{name}/"),
                )
            args = make_args("/tmp")
            args.mode = "superseded-templates"

            def run_query(_args: argparse.Namespace, query: str) -> list[dict[str, str]]:
                return prune.parse_csv(
                    runner.copy_stdout(f"COPY ({query}) TO STDOUT WITH CSV HEADER")
                )

            selected = {}
            for keep in (1, 2):
                args.keep_latest_per_template = keep
                with mock.patch.object(prune, "psql_csv_query", side_effect=run_query):
                    rows = prune.template_candidate_rows(args, "2100-01-01 00:00:00+00")
                selected[keep] = [row["template_release_id"] for row in rows]
            self.assertEqual(selected, {1: [releases["older"][0]], 2: []})
        finally:
            try:
                runner.connection.rollback()
            except Exception:
                pass
            runner.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            runner.connection.close()

    def test_superseded_template_listing_refuses_a_bucket_root_location(self) -> None:
        runner = postgres_runner_or_skip(self)
        schema = f"prune_cluster_artifacts_test_{uuid.uuid4().hex}"
        try:
            runner.execute(f"CREATE SCHEMA {schema}")
            runner.execute(f"SET search_path TO {schema}")
            setup_minimal_template_pruning_schema(runner)
            # The bucket root is a prefix of every location in the bucket, so
            # without the refusal every release would be spared silently.
            runner.execute(
                """
INSERT INTO template_release_storage_locations (template_release_id, storage_connection_string)
VALUES (%s, 'S3://example-template-artifacts?region=us-west-2')
""",
                (TEMPLATE_PRUNE_RELEASES["newest_original"][0],),
            )
            args = make_args("/tmp")
            args.mode = "superseded-templates"
            args.keep_latest_per_template = 1

            def run_query(_args: argparse.Namespace, query: str) -> list[dict[str, str]]:
                return prune.parse_csv(
                    runner.copy_stdout(f"COPY ({query}) TO STDOUT WITH CSV HEADER")
                )

            with mock.patch.object(prune, "psql_csv_query", side_effect=run_query):
                with self.assertRaises(SystemExit) as raised:
                    prune.template_candidate_rows(args, "2100-01-01 00:00:00+00")
            message = str(raised.exception)
            self.assertIn(
                "'S3://example-template-artifacts?region=us-west-2' (refusing bucket-root S3 URI)",
                message,
            )
            self.assertIn(TEMPLATE_PRUNE_RELEASES["newest_original"][0], message)
        finally:
            try:
                runner.connection.rollback()
            except Exception:
                pass
            runner.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            runner.connection.close()

    def test_superseded_template_listing_refuses_a_location_the_s3_parser_rejects(self) -> None:
        runner = postgres_runner_or_skip(self)
        # None is a bucket root, but each names no bucket or no s3:// URI at
        # all, and the first two are string prefixes of every s3:// location,
        # so the storage check would spare every release in silence.
        malformed = [
            "s3:",
            "s3:/",
            "s3:///key",
            "example-template-artifacts/code-runtime/x",
            "?region=us-west-2",
        ]
        for uri in malformed:
            with self.subTest(uri=uri):
                schema = f"prune_cluster_artifacts_test_{uuid.uuid4().hex}"
                try:
                    runner.execute(f"CREATE SCHEMA {schema}")
                    runner.execute(f"SET search_path TO {schema}")
                    setup_minimal_template_pruning_schema(runner)
                    runner.execute(
                        """
INSERT INTO template_release_storage_locations (template_release_id, storage_connection_string)
VALUES (%s, %s)
""",
                        (TEMPLATE_PRUNE_RELEASES["newest_original"][0], uri),
                    )
                    args = make_args("/tmp")
                    args.mode = "superseded-templates"
                    args.keep_latest_per_template = 1

                    def run_query(_args: argparse.Namespace, query: str) -> list[dict[str, str]]:
                        return prune.parse_csv(
                            runner.copy_stdout(f"COPY ({query}) TO STDOUT WITH CSV HEADER")
                        )

                    with mock.patch.object(prune, "psql_csv_query", side_effect=run_query):
                        with self.assertRaises(SystemExit) as raised:
                            prune.template_candidate_rows(args, "2100-01-01 00:00:00+00")
                    message = str(raised.exception)
                    self.assertIn("refusing to list superseded templates", message)
                    self.assertIn(TEMPLATE_PRUNE_RELEASES["newest_original"][0], message)
                    self.assertIn(repr(uri), message)
                finally:
                    try:
                        runner.connection.rollback()
                    except Exception:
                        pass
                    runner.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        runner.connection.close()

    def test_verify_refuses_to_count_superseded_templates_with_a_bucket_root_location(self) -> None:
        runner = postgres_runner_or_skip(self)
        schema = f"prune_cluster_artifacts_test_{uuid.uuid4().hex}"
        try:
            runner.execute(f"CREATE SCHEMA {schema}")
            runner.execute(f"SET search_path TO {schema}")
            setup_minimal_template_pruning_schema(runner)
            runner.execute(
                """
INSERT INTO template_release_storage_locations (template_release_id, storage_connection_string)
VALUES (%s, 's3://example-template-artifacts/?region=us-west-2')
""",
                (TEMPLATE_PRUNE_RELEASES["newest_original"][0],),
            )

            def run_query(_args: argparse.Namespace, query: str) -> list[dict[str, str]]:
                return prune.parse_csv(
                    runner.copy_stdout(f"COPY ({query}) TO STDOUT WITH CSV HEADER")
                )

            with tempfile.TemporaryDirectory() as tmp:
                args = make_args(tmp)
                args.mode = "verify"
                with mock.patch.multiple(
                    prune,
                    psql_csv_query=mock.Mock(side_effect=run_query),
                    db_cutoff=mock.Mock(return_value={"cutoff": "2100-01-01 00:00:00+00"}),
                    load_live_pods=mock.Mock(return_value={}),
                    current_generation_pod_refs=mock.Mock(return_value=set()),
                    classify_stale_pods=mock.Mock(return_value=[]),
                ):
                    with self.assertRaises(SystemExit) as raised:
                        prune.run_verify(args)
                self.assertFalse((Path(tmp) / "verify.json").exists())
            message = str(raised.exception)
            self.assertIn("refusing to count superseded templates", message)
            self.assertIn(TEMPLATE_PRUNE_RELEASES["newest_original"][0], message)
            self.assertIn("'s3://example-template-artifacts/?region=us-west-2'", message)
        finally:
            try:
                runner.connection.rollback()
            except Exception:
                pass
            runner.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            runner.connection.close()

    def test_canonical_storage_locations_match_the_deletion_target(self) -> None:
        runner = postgres_runner_or_skip(self)
        schema = f"prune_cluster_artifacts_test_{uuid.uuid4().hex}"
        uris = [
            "s3://bucket/key",
            "s3://bucket//key/",
            "s3://bucket/key/?region=us-west-2",
            "s3://bucket/key#fragment",
            "s3://bucket/key?x#y",
            "s3://bucket/key#y?x",
            "S3://bucket/key",
            "s3://bucket/ke\ty\r\n",
            "s3://user@bucket:9000/key;p",
        ]
        # urlsplit strips leading C0 controls and spaces only where it has the
        # CVE-2023-24329 fix (3.11.4 and the matching 3.7 to 3.10 patch
        # releases). Without it such a URI isn't an s3:// URI at all, so
        # deletion refuses it and there's no target to match.
        if urllib.parse.urlsplit(" \x01s3://bucket/key").scheme == "s3":
            uris.append(" \x01s3://bucket/key")
        try:
            runner.execute(f"CREATE SCHEMA {schema}")
            runner.execute(f"SET search_path TO {schema}")
            setup_minimal_template_pruning_schema(runner)
            release_id = next(iter(TEMPLATE_PRUNE_RELEASES.values()))[0]
            runner.execute("DELETE FROM template_release_storage_locations")
            for uri in uris:
                runner.execute(
                    """
INSERT INTO template_release_storage_locations (template_release_id, storage_connection_string)
VALUES (%s, %s)
""",
                    (release_id, uri),
                )
            pairs = runner.fetchone(
                f"""
SELECT ARRAY_AGG(ARRAY[storage.storage_connection_string, canonical.uri])
FROM ({prune.canonical_storage_locations_sql()}
) canonical
JOIN template_release_storage_locations storage ON storage.id = canonical.location_id
"""
            )[0]
            self.assertEqual(
                dict(pairs),
                {uri: prune.canonical_s3_prefix_uri(uri) for uri in uris},
            )
        finally:
            runner.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            runner.connection.close()


if __name__ == "__main__":
    unittest.main()
