#!/usr/bin/env python3
"""The control-plane schema admits one unfinished firewall rules rollout.

firewall_rules_rollouts enforces that itself, not through a writer's lock: an
unfinished header must hold unfinished_slot 1, which a unique key admits once.
These tests apply every db/postgres up migration to a new database, and the
first two race two connections to it. No rollout has items, so the rule holds
before membership does. Each refusal is checked by constraint name, and
the fixtures are valid revisions and receipts, so an unrelated constraint
can't stand in for the slot.
"""

from __future__ import annotations

import concurrent.futures
import os
import time
import unittest
import uuid
from pathlib import Path

import psycopg
from psycopg import errors, sql
from psycopg.conninfo import make_conninfo
from required_test_deps import unavailable_dependency

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "db" / "postgres"
SLOT_KEY = "firewall_rules_rollouts_unfinished_slot_key"
SLOT_CHECK = "firewall_rules_rollouts_unfinished_slot_check"
UNFINISHED_PHASES = ("canary", "rolling", "paused")
# Bounds every wait, so a regression fails instead of hanging the run.
WAIT_SECONDS = 30


def database_url() -> str:
    # ci/scripts/run-python-tests.sh starts one Postgres for the Python tests
    # and names it with PRUNE_CLUSTER_ARTIFACTS_TEST_DATABASE_URL. The tests
    # create a database of their own on it and drop it afterwards.
    for name in (
        "FIREWALL_RULES_TEST_DATABASE_URL",
        "PRUNE_CLUSTER_ARTIFACTS_TEST_DATABASE_URL",
    ):
        url = os.environ.get(name)
        if url:
            return url
    unavailable_dependency(
        "postgres",
        "set FIREWALL_RULES_TEST_DATABASE_URL to a Postgres where the tests may "
        "create a database, or run them through ci/scripts/run-python-tests.sh",
    )


def migration_files() -> list[Path]:
    files = sorted(
        MIGRATIONS_DIR.glob("*.up.sql"),
        key=lambda path: int(path.name.split("_", 1)[0]),
    )
    if not files:
        raise AssertionError(f"no Postgres migrations under {MIGRATIONS_DIR}")
    return files


def apply_migrations(url: str) -> None:
    # As SQLx does: each file in its own transaction, unless its first line
    # opts out.
    with psycopg.connect(url, autocommit=True) as conn:
        for path in migration_files():
            text = path.read_text()
            if text.startswith("-- no-transaction"):
                conn.execute(text)
            else:
                with conn.transaction():
                    conn.execute(text)


class FirewallRulesRolloutSlotTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        admin_url = database_url()
        cls.database = f"firewall_rules_slot_{uuid.uuid4().hex}"
        try:
            admin = psycopg.connect(admin_url, autocommit=True)
        except psycopg.OperationalError as error:
            unavailable_dependency("postgres", str(error))
        with admin:
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(cls.database)))
        cls.admin_url = admin_url
        cls.url = make_conninfo(admin_url, dbname=cls.database)
        try:
            apply_migrations(cls.url)
            with psycopg.connect(cls.url, autocommit=True) as conn:
                cls.revision = cls._insert_revision(conn)
        except BaseException:
            cls._drop_database()
            raise

    @classmethod
    def tearDownClass(cls) -> None:
        cls._drop_database()

    @classmethod
    def _drop_database(cls) -> None:
        with psycopg.connect(cls.admin_url, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(cls.database)
                )
            )

    @staticmethod
    def _insert_revision(conn: psycopg.Connection) -> uuid.UUID:
        revision = uuid.uuid4()
        conn.execute(
            "INSERT INTO firewall_rules_revisions (id, ruleset_format_version, ruleset)"
            " VALUES (%s, 1, '[]'::jsonb)",
            (revision,),
        )
        return revision

    def setUp(self) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM firewall_rules_rollouts")

    def connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self.url)
        # A writer blocked past this fails instead of waiting forever.
        conn.execute(f"SET lock_timeout = '{WAIT_SECONDS}s'")
        conn.commit()
        return conn

    def insert_header(
        self, conn: psycopg.Connection, phase: str, unfinished_slot: int | None
    ) -> uuid.UUID:
        """Insert a rollout header with its own new receipt, in conn's open
        transaction."""
        receipt = uuid.uuid4()
        conn.execute(
            "INSERT INTO firewall_rules_mutation_receipts"
            " (id, actor, idempotency_key, request_fingerprint, result)"
            " VALUES (%s, 'operator:test', %s, %s, '{}'::jsonb)",
            (receipt, str(receipt), "0" * 64),
        )
        rollout = uuid.uuid4()
        conn.execute(
            "INSERT INTO firewall_rules_rollouts"
            " (id, to_revision_id, created_by_receipt_id, phase, unfinished_slot,"
            "  max_in_flight_per_cluster, reason)"
            " VALUES (%s, %s, %s, %s, %s, 1, 'test')",
            (
                rollout,
                self.revision,
                receipt,
                phase,
                unfinished_slot,
            ),
        )
        return rollout

    def start(self, phase: str = "canary") -> uuid.UUID:
        """Commit a new unfinished rollout."""
        with self.connect() as conn:
            rollout = self.insert_header(conn, phase, 1)
            conn.commit()
        return rollout

    def assert_refused(self, error_type: type, constraint: str, action) -> None:
        with self.assertRaises(error_type) as caught:
            action()
        self.assertEqual(caught.exception.diag.constraint_name, constraint)

    def assert_start_refused(self) -> None:
        self.assert_refused(errors.UniqueViolation, SLOT_KEY, self.start)

    def unfinished(self) -> list[uuid.UUID]:
        with self.connect() as conn:
            return [
                row[0]
                for row in conn.execute(
                    "SELECT id FROM firewall_rules_rollouts WHERE unfinished_slot IS NOT NULL"
                )
            ]

    def race(self, first_commits: bool) -> tuple[uuid.UUID, concurrent.futures.Future]:
        """Hold a first start open on one connection while a second connection
        tries another, wait until the second is blocked on the first, then
        commit or roll back the first. Returns the first rollout's ID and the
        second's outcome."""
        with self.connect() as first, self.connect() as second, self.connect() as observer:
            observer.autocommit = True
            first_rollout = self.insert_header(first, "canary", 1)
            first_pid = first.info.backend_pid
            second_pid = second.info.backend_pid

            def second_start() -> uuid.UUID:
                rollout = self.insert_header(second, "canary", 1)
                second.commit()
                return rollout

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                outcome = pool.submit(second_start)
                deadline = time.monotonic() + WAIT_SECONDS
                while True:
                    (blocked,) = observer.execute(
                        "SELECT %s = ANY(pg_blocking_pids(%s))", (first_pid, second_pid)
                    ).fetchone()
                    if blocked:
                        break
                    if outcome.done():
                        self.fail(
                            "the second start finished without waiting for the first: "
                            f"{outcome.exception() or outcome.result()}"
                        )
                    if time.monotonic() > deadline:
                        self.fail("the second start never waited for the first")
                    time.sleep(0.01)
                if first_commits:
                    first.commit()
                else:
                    first.rollback()
                concurrent.futures.wait([outcome], timeout=WAIT_SECONDS)
                self.assertTrue(outcome.done(), "the second start never finished")
                second.rollback()
        return first_rollout, outcome

    def test_first_start_commits(self) -> None:
        first, outcome = self.race(first_commits=True)

        error = outcome.exception()
        self.assertIsInstance(error, errors.UniqueViolation)
        self.assertEqual(error.diag.constraint_name, SLOT_KEY)
        self.assertEqual(self.unfinished(), [first])

    def test_first_start_rolls_back(self) -> None:
        _, outcome = self.race(first_commits=False)

        self.assertEqual(self.unfinished(), [outcome.result()])

    def test_pause_retains_the_slot(self) -> None:
        rollout = self.start()
        for phase in ("rolling", "paused"):
            with self.connect() as conn:
                conn.execute(
                    "UPDATE firewall_rules_rollouts SET phase = %s WHERE id = %s",
                    (phase, rollout),
                )
                conn.commit()

            with self.subTest(phase=phase):
                self.assert_start_refused()
        self.assertEqual(self.unfinished(), [rollout])

    def test_terminal_phases_release_the_slot(self) -> None:
        finished = []
        rollout = self.start()
        for terminal in ("completed", "aborted"):
            with self.subTest(terminal=terminal):
                # The phase, the slot and finished_at change together or not
                # at all.
                with self.connect() as conn:
                    self.assert_refused(
                        errors.CheckViolation,
                        SLOT_CHECK,
                        lambda: conn.execute(
                            "UPDATE firewall_rules_rollouts"
                            " SET phase = %s, finished_at = now() WHERE id = %s",
                            (terminal, rollout),
                        ),
                    )
                    conn.rollback()
                    conn.execute(
                        "UPDATE firewall_rules_rollouts"
                        " SET phase = %s, unfinished_slot = NULL, finished_at = now()"
                        " WHERE id = %s",
                        (terminal, rollout),
                    )
                    conn.commit()
                finished.append(rollout)

                rollout = self.start()

        with self.connect() as conn:
            terminal_rows = conn.execute(
                "SELECT id, phase FROM firewall_rules_rollouts"
                " WHERE unfinished_slot IS NULL AND finished_at IS NOT NULL"
            ).fetchall()
        self.assertCountEqual(
            terminal_rows, list(zip(finished, ("completed", "aborted")))
        )
        self.assertEqual(self.unfinished(), [rollout])

    def test_null_slot_cannot_bypass_the_key(self) -> None:
        for phase in UNFINISHED_PHASES:
            with self.subTest(phase=phase), self.connect() as conn:
                self.assert_refused(
                    errors.CheckViolation,
                    SLOT_CHECK,
                    lambda: self.insert_header(conn, phase, None),
                )
                conn.rollback()
        self.assertEqual(self.unfinished(), [])


if __name__ == "__main__":
    unittest.main()
