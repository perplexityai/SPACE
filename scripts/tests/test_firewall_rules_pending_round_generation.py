#!/usr/bin/env python3
"""A pending firewall rules round can record the assignment generation it asks about.

Each test applies the db/postgres up migrations before this one to a new
database, seeds a round written before it (no generation) beside an
assignment and accepted evidence, then applies it. Refusals are checked by
constraint name, so an unrelated constraint can't stand in for the new one.
"""

from __future__ import annotations

import unittest
import uuid

import psycopg
from firewall_rules_postgres import (
    MIGRATIONS_DIR,
    apply_migrations,
    database_url,
    migration_files,
)
from psycopg import errors, sql
from psycopg.conninfo import make_conninfo
from required_test_deps import unavailable_dependency

VERSION = 20261123200458
UP = MIGRATIONS_DIR / f"{VERSION}_firewall_rules_pending_round_generation.up.sql"
DOWN = UP.with_name(UP.name.replace(".up.", ".down."))
GENERATION_CHECK = "firewall_rules_node_observations_pending_generation_check"
ROUND_CHECK = "firewall_rules_node_observations_pending_round_check"
ISSUED_AT = "2026-11-23 12:00:00.123456+00"


class PendingRoundGenerationTest(unittest.TestCase):
    def setUp(self) -> None:
        admin_url = database_url()
        database = f"firewall_rules_generation_{uuid.uuid4().hex}"
        try:
            admin = psycopg.connect(admin_url, autocommit=True)
        except psycopg.OperationalError as error:
            unavailable_dependency("postgres", str(error))
        with admin:
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))

        def drop() -> None:
            with psycopg.connect(admin_url, autocommit=True) as admin:
                admin.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                        sql.Identifier(database)
                    )
                )

        self.addCleanup(drop)
        self.url = make_conninfo(admin_url, dbname=database)
        earlier = [path for path in migration_files() if int(path.name.split("_", 1)[0]) < VERSION]
        apply_migrations(self.url, earlier)
        self.conn = psycopg.connect(self.url, autocommit=True)
        self.addCleanup(self.conn.close)

        self.legacy = uuid.uuid4()
        revision = uuid.uuid4()
        self.conn.execute(
            "INSERT INTO firewall_rules_revisions (id, ruleset_format_version, ruleset)"
            " VALUES (%s, 1, '[]'::jsonb)",
            (revision,),
        )
        self.conn.execute(
            "INSERT INTO firewall_rules_node_assignments"
            " (node_id, assignment_generation, revision_id) VALUES (%s, 12, %s)",
            (self.legacy, revision),
        )
        self.conn.execute(
            "INSERT INTO firewall_rules_node_observations (node_id, manager_epoch,"
            " pending_round, pending_round_issued_at,"
            " accepted_round, accepted_round_issued_at, accepted_report)"
            " VALUES (%s, 1, 7, %s, 6, %s, '{\"seen\": true}')",
            (self.legacy, ISSUED_AT, ISSUED_AT),
        )
        self.before = self.state()
        apply_migrations(self.url, [UP])

    def state(self) -> tuple:
        """The firewall rows, without the new column or update times, and the
        observation constraints."""
        return self.conn.execute(
            "SELECT"
            " (SELECT jsonb_agg(to_jsonb(o) - 'pending_assignment_generation'"
            "                   - 'updated_at' ORDER BY node_id)"
            "  FROM firewall_rules_node_observations o),"
            " (SELECT jsonb_agg(to_jsonb(a) ORDER BY node_id)"
            "  FROM firewall_rules_node_assignments a),"
            " (SELECT jsonb_object_agg(conname, pg_get_constraintdef(oid))"
            "  FROM pg_constraint"
            "  WHERE conrelid = 'firewall_rules_node_observations'::regclass)"
        ).fetchone()

    def generation(self) -> int | None:
        (generation,) = self.conn.execute(
            "SELECT pending_assignment_generation FROM firewall_rules_node_observations"
            " WHERE node_id = %s",
            (self.legacy,),
        ).fetchone()
        return generation

    def observe(self, round_: int | None, issued: bool, generation: int | None) -> None:
        """Insert a node's observation, beside older accepted evidence, and roll
        it back."""
        with self.conn.transaction(force_rollback=True):
            self.conn.execute(
                "INSERT INTO firewall_rules_node_observations (node_id, manager_epoch,"
                " pending_round, pending_round_issued_at, pending_assignment_generation,"
                " accepted_round, accepted_round_issued_at, accepted_report)"
                " VALUES (%s, 1, %s, %s, %s, 2, %s, '{}')",
                (uuid.uuid4(), round_, ISSUED_AT if issued else None, generation, ISSUED_AT),
            )

    def test_an_old_round_stays_unbound_and_a_bound_one_is_complete(self) -> None:
        """Catches a generation backfilled from the assignment or a dropped old
        round, and a CHECK that admits a generation without a round or below
        zero, or refuses generation 0."""
        self.assertEqual(self.state()[:2], self.before[:2])
        self.assertIsNone(self.generation())

        for case, args in {
            "no round": (None, False, None),
            "generation zero": (7, True, 0),
            "unbound": (7, True, None),
        }.items():
            with self.subTest(case):
                self.observe(*args)
        for case, args, constraint in [
            ("generation without a round", (None, False, 12), GENERATION_CHECK),
            ("negative generation", (7, True, -1), GENERATION_CHECK),
            ("round without its issue time", (7, False, 12), ROUND_CHECK),
        ]:
            with self.subTest(case), self.assertRaises(errors.CheckViolation) as caught:
                self.observe(*args)
            self.assertEqual(caught.exception.diag.constraint_name, constraint, case)

    def test_down_keeps_a_bound_round_and_restores_the_previous_shape(self) -> None:
        """Catches a down that drops the round with its generation, or changes
        another row, column or constraint."""
        self.conn.execute(
            "UPDATE firewall_rules_node_observations"
            " SET pending_assignment_generation = 0 WHERE node_id = %s",
            (self.legacy,),
        )
        apply_migrations(self.url, [DOWN])
        self.assertEqual(self.state(), self.before)

        apply_migrations(self.url, [UP])
        self.assertIsNone(self.generation())


if __name__ == "__main__":
    unittest.main()
