#!/usr/bin/env python3

from __future__ import annotations

import argparse
import io
import importlib.util
import os
import sys
import tempfile
import unittest
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


if __name__ == "__main__":
    unittest.main()
