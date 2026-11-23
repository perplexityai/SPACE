#!/usr/bin/env python3
"""Every pending firewall rules round names the assignment generation it asks about.

Each test applies the db/postgres up migrations before this one to a new
database and seeds a round without a generation, which they still admit,
beside an assignment and accepted evidence. Refusals are checked by
constraint name.
"""

from __future__ import annotations

import unittest
import uuid

import psycopg
from firewall_rules_postgres import MIGRATIONS_DIR, apply_migrations, fresh_database
from psycopg import errors

VERSION = 20261123205747
UP = MIGRATIONS_DIR / f"{VERSION}_firewall_rules_pending_round_bound.up.sql"
DOWN = UP.with_name(UP.name.replace(".up.", ".down."))
BOUND_CHECK = "firewall_rules_node_observations_pending_bound_check"
ISSUED_AT = "2026-11-23 12:00:00.123456+00"


class PendingRoundBoundTest(unittest.TestCase):
    def setUp(self) -> None:
        self.url = fresh_database(self, before=VERSION)
        self.conn = psycopg.connect(self.url, autocommit=True)
        self.addCleanup(self.conn.close)

        self.node = uuid.uuid4()
        revision = uuid.uuid4()
        self.conn.execute(
            "INSERT INTO firewall_rules_revisions (id, ruleset_format_version, ruleset)"
            " VALUES (%s, 1, '[]'::jsonb)",
            (revision,),
        )
        self.conn.execute(
            "INSERT INTO firewall_rules_node_assignments"
            " (node_id, assignment_generation, revision_id) VALUES (%s, 12, %s)",
            (self.node, revision),
        )
        self.conn.execute(
            "INSERT INTO firewall_rules_node_observations (node_id, manager_epoch,"
            " pending_round, pending_round_issued_at,"
            " accepted_round, accepted_round_issued_at, accepted_report)"
            " VALUES (%s, 1, 7, %s, 6, %s, '{\"seen\": true}')",
            (self.node, ISSUED_AT, ISSUED_AT),
        )

    def replace_round(self) -> None:
        """Replace the unbound round with a new, bound one, as its recovery says."""
        self.conn.execute(
            "UPDATE firewall_rules_node_observations"
            " SET pending_round = 8, pending_round_issued_at = clock_timestamp(),"
            "     pending_assignment_generation = 12"
            " WHERE node_id = %s",
            (self.node,),
        )

    def state(self) -> tuple:
        """The observation rows and the table's constraints and column comment."""
        return self.conn.execute(
            "SELECT"
            " (SELECT jsonb_agg(to_jsonb(o) ORDER BY node_id)"
            "  FROM firewall_rules_node_observations o),"
            " (SELECT jsonb_object_agg(conname, pg_get_constraintdef(oid))"
            "  FROM pg_constraint"
            "  WHERE conrelid = 'firewall_rules_node_observations'::regclass),"
            " col_description('firewall_rules_node_observations'::regclass,"
            "  (SELECT attnum FROM pg_attribute"
            "   WHERE attrelid = 'firewall_rules_node_observations'::regclass"
            "     AND attname = 'pending_assignment_generation'))"
        ).fetchone()

    def observe(self, round_: int | None, generation: int | None) -> None:
        """Insert a node's observation and roll it back."""
        with self.conn.transaction(force_rollback=True):
            self.conn.execute(
                "INSERT INTO firewall_rules_node_observations (node_id, manager_epoch,"
                " pending_round, pending_round_issued_at, pending_assignment_generation)"
                " VALUES (%s, 1, %s, %s, %s)",
                (uuid.uuid4(), round_, ISSUED_AT if round_ is not None else None, generation),
            )

    def test_up_refuses_an_unbound_round_until_a_new_round_replaces_it(self) -> None:
        """Catches a CHECK left NOT VALID, which would admit the unbound round it
        exists to rule out."""
        with self.assertRaises(errors.CheckViolation) as caught:
            apply_migrations(self.url, [UP])
        self.assertEqual(caught.exception.diag.constraint_name, BOUND_CHECK)

        self.replace_round()
        apply_migrations(self.url, [UP])

    def test_a_round_needs_its_generation_and_down_restores_the_previous_shape(self) -> None:
        """Catches a CHECK that admits a round without a generation or refuses
        generation 0, and a down that leaves the CHECK or the column's new
        comment."""
        self.replace_round()
        before = self.state()
        apply_migrations(self.url, [UP])

        self.observe(None, None)
        self.observe(7, 0)
        with self.assertRaises(errors.CheckViolation) as caught:
            self.observe(7, None)
        self.assertEqual(caught.exception.diag.constraint_name, BOUND_CHECK)

        apply_migrations(self.url, [DOWN])
        self.assertEqual(self.state(), before)


if __name__ == "__main__":
    unittest.main()
