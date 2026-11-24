#!/usr/bin/env python3
"""Per-environment memory-retirement columns, checks, trigger scope, and guarded rollback.

Each test migrates a fresh database up to just before this migration, seeds
pre-existing allowed/default and deprecated environments, then applies the
migration. Constraint refusals are checked by constraint name so an unrelated
constraint can't stand in for the new ones.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import time
import unittest
import uuid

import psycopg
from firewall_rules_postgres import MIGRATIONS_DIR, apply_migrations, fresh_database
from psycopg import errors
from psycopg.conninfo import make_conninfo

VERSION = 20261124192451
UP = MIGRATIONS_DIR / f"{VERSION}_runtime_environment_states_memory_retired.up.sql"
DOWN = UP.with_name(UP.name.replace(".up.", ".down."))

LOCAL_ORG = uuid.UUID("00000000-0000-0000-0000-000000000001")
SEED_USER = uuid.UUID(int=0x10)
MEMORY_STATE_CHECK = "runtime_environment_states_memory_state_check"
MEMORY_CHANGE_CHECK = "runtime_environment_states_memory_change_check"
MEMORY_USER_FKEY = "runtime_environment_states_memory_updated_by_user_id_fkey"
PAST = datetime(2026, 11, 23, 12, 0, 0, tzinfo=timezone.utc)
CHANGE_AT = datetime(2026, 11, 23, 13, 0, 0, tzinfo=timezone.utc)


def env_ref(char: str) -> str:
    return f"sha256:{char * 64}"


class RuntimeEnvironmentStatesMemoryRetiredTest(unittest.TestCase):
    def setUp(self) -> None:
        self.url = fresh_database(self, before=VERSION)
        self.conn = psycopg.connect(self.url, autocommit=True)
        self.addCleanup(self.conn.close)

        self.allowed_env = env_ref("a")
        self.deprecated_env = env_ref("b")
        self.conn.execute(
            "INSERT INTO runtime_environment_states"
            " (environment_ref, state, updated_at, updated_by_user_id, reason)"
            " VALUES (%s, 'allowed', %s, %s, 'default env'),"
            "        (%s, 'deprecated', %s, %s, 'draining env')",
            (
                self.allowed_env,
                PAST,
                SEED_USER,
                self.deprecated_env,
                PAST,
                SEED_USER,
            ),
        )
        self.conn.execute(
            "UPDATE runtime_environment_policy"
            " SET generation = 1, default_environment_ref = %s,"
            "     updated_by_user_id = %s, reason = 'set default'",
            (self.allowed_env, SEED_USER),
        )
        self.before = self.shape()
        apply_migrations(self.url, [UP])

    def insert_user(self) -> uuid.UUID:
        user = uuid.uuid4()
        self.conn.execute(
            "INSERT INTO users (id, email, password_hash, display_name, org_id, role)"
            " VALUES (%s, %s, 'test', 'test', %s, 'member')",
            (user, f"{user}@example.com", LOCAL_ORG),
        )
        return user

    def shape(self) -> tuple:
        """Rows without the memory columns, plus constraints, non-internal
        triggers, and per-column metadata on runtime_environment_states."""
        return self.conn.execute(
            "SELECT"
            " (SELECT jsonb_agg(to_jsonb(s) - 'memory_retired' - 'memory_updated_at'"
            "                   - 'memory_updated_by_user_id' - 'memory_reason'"
            "                   ORDER BY environment_ref)"
            "  FROM runtime_environment_states s),"
            " (SELECT jsonb_object_agg(conname, pg_get_constraintdef(oid))"
            "  FROM pg_constraint"
            "  WHERE conrelid = 'runtime_environment_states'::regclass),"
            " (SELECT jsonb_object_agg(tgname, pg_get_triggerdef(oid))"
            "  FROM pg_trigger"
            "  WHERE tgrelid = 'runtime_environment_states'::regclass"
            "    AND NOT tgisinternal),"
            " (SELECT jsonb_object_agg("
            "    attname,"
            "    jsonb_build_object("
            "      'type', format_type(atttypid, atttypmod),"
            "      'not_null', attnotnull,"
            "      'default', pg_get_expr(adbin, adrelid),"
            "      'comment', col_description(attrelid, attnum)"
            "    )"
            "  )"
            "  FROM pg_attribute"
            "  LEFT JOIN pg_attrdef ON adrelid = attrelid AND adnum = attnum"
            "  WHERE attrelid = 'runtime_environment_states'::regclass"
            "    AND attnum > 0 AND NOT attisdropped)"
        ).fetchone()

    def memory_tuple(self, environment_ref: str) -> tuple:
        return self.conn.execute(
            "SELECT memory_retired, memory_updated_at, memory_updated_by_user_id, memory_reason"
            " FROM runtime_environment_states WHERE environment_ref = %s",
            (environment_ref,),
        ).fetchone()

    def probe_insert(
        self,
        *,
        state: str,
        memory_retired: bool | None,
        memory_updated_at: datetime | None,
        memory_updated_by_user_id: uuid.UUID | None,
        memory_reason: str | None,
    ) -> None:
        with self.conn.transaction(force_rollback=True):
            self.conn.execute(
                "INSERT INTO runtime_environment_states"
                " (environment_ref, state, reason, memory_retired,"
                "  memory_updated_at, memory_updated_by_user_id, memory_reason)"
                " VALUES (%s, %s, 'probe', %s, %s, %s, %s)",
                (
                    env_ref("f"),
                    state,
                    memory_retired,
                    memory_updated_at,
                    memory_updated_by_user_id,
                    memory_reason,
                ),
            )

    def test_existing_and_older_binary_rows_stay_unretired_and_constraints_hold(self) -> None:
        rows, _, _, columns = self.shape()
        self.assertEqual(rows, self.before[0])
        for col in (
            "memory_retired",
            "memory_updated_at",
            "memory_updated_by_user_id",
            "memory_reason",
        ):
            self.assertTrue(columns[col]["comment"], col)
        for env in (self.allowed_env, self.deprecated_env):
            self.assertEqual(self.memory_tuple(env), (False, None, None, None))

        # Older binaries insert without naming the memory columns.
        older_env = env_ref("c")
        self.conn.execute(
            "INSERT INTO runtime_environment_states"
            " (environment_ref, state, updated_by_user_id, reason)"
            " VALUES (%s, 'blocked', %s, 'older binary block')",
            (older_env, SEED_USER),
        )
        self.assertEqual(self.memory_tuple(older_env), (False, None, None, None))

        # VALIDATE CONSTRAINT is a no-op on already-validated constraints, so check
        # NOT VALID first and then prove existing rows pass validation.
        validated = dict(
            self.conn.execute(
                "SELECT conname, convalidated FROM pg_constraint"
                " WHERE conrelid = 'runtime_environment_states'::regclass"
                "   AND conname = ANY(%s)",
                ([MEMORY_USER_FKEY, MEMORY_STATE_CHECK, MEMORY_CHANGE_CHECK],),
            ).fetchall()
        )
        self.assertEqual(
            validated,
            {MEMORY_USER_FKEY: False, MEMORY_STATE_CHECK: False, MEMORY_CHANGE_CHECK: False},
        )
        with self.conn.transaction(force_rollback=True):
            for name in (MEMORY_USER_FKEY, MEMORY_STATE_CHECK, MEMORY_CHANGE_CHECK):
                self.conn.execute(f"ALTER TABLE runtime_environment_states VALIDATE CONSTRAINT {name}")

        valid_cases = {
            "untouched allowed": ("allowed", False, None, None, None),
            "retired deprecated with actor": ("deprecated", True, CHANGE_AT, SEED_USER, "x"),
            "retired blocked without actor and max multibyte reason": (
                "blocked",
                True,
                CHANGE_AT,
                None,
                "é" * 1024,
            ),
            "un-retired deprecated keeps audit": ("deprecated", False, CHANGE_AT, SEED_USER, "ok"),
            "un-retired allowed keeps audit": ("allowed", False, CHANGE_AT, None, "re-allowed"),
        }
        for case, (state, retired, updated_at, actor, reason) in valid_cases.items():
            with self.subTest(case):
                self.probe_insert(
                    state=state,
                    memory_retired=retired,
                    memory_updated_at=updated_at,
                    memory_updated_by_user_id=actor,
                    memory_reason=reason,
                )

        invalid_checks = [
            ("retired while allowed", ("allowed", True, CHANGE_AT, SEED_USER, "why"), MEMORY_STATE_CHECK),
            ("retired without audit metadata", ("deprecated", True, None, None, None), MEMORY_CHANGE_CHECK),
            ("retired without timestamp", ("deprecated", True, None, None, "why"), MEMORY_CHANGE_CHECK),
            ("retired with null reason", ("deprecated", True, CHANGE_AT, SEED_USER, None), MEMORY_CHANGE_CHECK),
            ("un-retired with timestamp but null reason", ("deprecated", False, CHANGE_AT, None, None), MEMORY_CHANGE_CHECK),
            ("untouched with stray reason", ("deprecated", False, None, None, "why"), MEMORY_CHANGE_CHECK),
            ("untouched with stray actor", ("deprecated", False, None, SEED_USER, None), MEMORY_CHANGE_CHECK),
            ("empty memory_reason", ("deprecated", True, CHANGE_AT, None, ""), MEMORY_CHANGE_CHECK),
            ("overlong memory_reason", ("deprecated", True, CHANGE_AT, None, "x" * 1025), MEMORY_CHANGE_CHECK),
        ]
        for case, (state, retired, updated_at, actor, reason), expected in invalid_checks:
            with self.subTest(case), self.assertRaises(errors.CheckViolation) as caught:
                self.probe_insert(
                    state=state,
                    memory_retired=retired,
                    memory_updated_at=updated_at,
                    memory_updated_by_user_id=actor,
                    memory_reason=reason,
                )
            self.assertEqual(caught.exception.diag.constraint_name, expected, case)

        with self.assertRaises(errors.NotNullViolation) as nn_caught:
            self.probe_insert(
                state="allowed",
                memory_retired=None,
                memory_updated_at=None,
                memory_updated_by_user_id=None,
                memory_reason=None,
            )
        self.assertEqual(nn_caught.exception.diag.column_name, "memory_retired")

        with self.assertRaises(errors.ForeignKeyViolation) as fk_caught:
            self.probe_insert(
                state="deprecated",
                memory_retired=True,
                memory_updated_at=CHANGE_AT,
                memory_updated_by_user_id=uuid.uuid4(),
                memory_reason="unknown actor",
            )
        self.assertEqual(fk_caught.exception.diag.constraint_name, MEMORY_USER_FKEY)

        # Pre-existing rows are also checked on UPDATE despite NOT VALID, and an older
        # binary's upsert can't move a memory-retired environment back to allowed.
        with self.assertRaises(errors.CheckViolation) as allowed_retire_caught:
            self.conn.execute(
                "UPDATE runtime_environment_states"
                " SET memory_retired = TRUE, memory_updated_at = %s, memory_reason = 'retire'"
                " WHERE environment_ref = %s",
                (CHANGE_AT, self.allowed_env),
            )
        self.assertEqual(allowed_retire_caught.exception.diag.constraint_name, MEMORY_STATE_CHECK)

        self.conn.execute(
            "UPDATE runtime_environment_states"
            " SET memory_retired = TRUE, memory_updated_at = %s, memory_reason = 'retire'"
            " WHERE environment_ref = %s",
            (CHANGE_AT, self.deprecated_env),
        )
        with self.assertRaises(errors.CheckViolation) as upsert_caught:
            self.conn.execute(
                "INSERT INTO runtime_environment_states"
                " (environment_ref, state, updated_by_user_id, reason)"
                " VALUES (%s, 'allowed', %s, 'older binary allow')"
                " ON CONFLICT (environment_ref) DO UPDATE"
                " SET state = EXCLUDED.state,"
                "     updated_by_user_id = EXCLUDED.updated_by_user_id,"
                "     reason = EXCLUDED.reason",
                (self.deprecated_env, SEED_USER),
            )
        self.assertEqual(upsert_caught.exception.diag.constraint_name, MEMORY_STATE_CHECK)

    def test_memory_updates_and_user_deletion_preserve_state_updated_at(self) -> None:
        memory_actor = self.insert_user()
        state_actor = self.insert_user()
        self.conn.execute(
            "UPDATE runtime_environment_states"
            " SET updated_by_user_id = %s WHERE environment_ref = %s",
            (state_actor, self.deprecated_env),
        )
        # Updating updated_at alone omits state, updated_by_user_id, and reason, so the narrowed trigger leaves PAST in place.
        self.conn.execute(
            "UPDATE runtime_environment_states SET updated_at = %s WHERE environment_ref = %s",
            (PAST, self.deprecated_env),
        )

        def state_updated_at() -> datetime:
            (ts,) = self.conn.execute(
                "SELECT updated_at FROM runtime_environment_states WHERE environment_ref = %s",
                (self.deprecated_env,),
            ).fetchone()
            return ts

        self.assertEqual(state_updated_at(), PAST)

        self.conn.execute(
            "UPDATE runtime_environment_states"
            " SET memory_retired = TRUE, memory_updated_at = %s,"
            "     memory_updated_by_user_id = %s, memory_reason = 'retire'"
            " WHERE environment_ref = %s",
            (CHANGE_AT, memory_actor, self.deprecated_env),
        )
        self.assertEqual(state_updated_at(), PAST)

        # Deleting the memory actor clears memory_updated_by_user_id without touching state's updated_at.
        self.conn.execute("DELETE FROM users WHERE id = %s", (memory_actor,))
        self.assertEqual(
            self.memory_tuple(self.deprecated_env),
            (True, CHANGE_AT, None, "retire"),
        )
        self.assertEqual(state_updated_at(), PAST)

        self.conn.execute(
            "UPDATE runtime_environment_states"
            " SET memory_retired = FALSE, memory_updated_at = now(), memory_reason = 'un-retire'"
            " WHERE environment_ref = %s",
            (self.deprecated_env,),
        )
        self.assertEqual(state_updated_at(), PAST)

        # Updating each of state's own columns (updated_by_user_id via ON DELETE SET NULL,
        # state alone, and reason alone) still bumps updated_at.
        self.conn.execute("DELETE FROM users WHERE id = %s", (state_actor,))
        self.assertGreater(state_updated_at(), PAST)

        for sql in (
            "UPDATE runtime_environment_states SET state = 'blocked' WHERE environment_ref = %s",
            "UPDATE runtime_environment_states SET reason = 'revised' WHERE environment_ref = %s",
        ):
            self.conn.execute(
                "UPDATE runtime_environment_states SET updated_at = %s WHERE environment_ref = %s",
                (PAST, self.deprecated_env),
            )
            self.conn.execute(sql, (self.deprecated_env,))
            self.assertGreater(state_updated_at(), PAST, sql)

    def test_rollback_refusal_restoration_and_migration_lock_order(self) -> None:
        users_oid, states_oid = self.conn.execute(
            "SELECT 'users'::regclass::oid, 'runtime_environment_states'::regclass::oid"
        ).fetchone()

        def wait_until_waiting(
            future: object, relation_oid: int, mode: str | None = None
        ) -> None:
            deadline = time.monotonic() + 5.0
            while (
                self.conn.execute(
                    "SELECT 1 FROM pg_locks"
                    " WHERE database = (SELECT oid FROM pg_database WHERE datname = current_database())"
                    "   AND relation = %s"
                    "   AND (%s::text IS NULL OR mode = %s)"
                    "   AND NOT granted",
                    (relation_oid, mode, mode),
                ).fetchone()
                is None
            ):
                if future.done():
                    future.result()
                if time.monotonic() >= deadline:
                    self.fail(f"migration never blocked on {mode or 'any'} (oid={relation_oid})")
                time.sleep(0.01)

        # Pre-lock check refuses already-committed retirements without taking ACCESS EXCLUSIVE on users.
        self.conn.execute(
            "UPDATE runtime_environment_states"
            " SET memory_retired = TRUE, memory_updated_at = %s,"
            "     memory_updated_by_user_id = %s, memory_reason = 'retire'"
            " WHERE environment_ref = %s",
            (CHANGE_AT, SEED_USER, self.deprecated_env),
        )
        after_retire = self.shape()
        with self.conn.transaction(force_rollback=True):
            self.conn.execute("LOCK TABLE users IN ACCESS SHARE MODE")
            with self.assertRaises(errors.RaiseException) as caught:
                apply_migrations(self.url, [DOWN])
        self.assertIn(
            "runtime_environment_states has memory_retired environments",
            str(caught.exception),
        )
        self.assertEqual(self.shape(), after_retire)
        self.assertEqual(
            self.memory_tuple(self.deprecated_env),
            (True, CHANGE_AT, SEED_USER, "retire"),
        )

        # Un-retired rows keep audit columns populated, yet permit rollback when none retire concurrently.
        self.conn.execute(
            "UPDATE runtime_environment_states"
            " SET memory_retired = FALSE, memory_updated_at = %s, memory_reason = 'un-retire'"
            " WHERE environment_ref = %s",
            (CHANGE_AT, self.deprecated_env),
        )

        # Post-lock check catches a retirement that commits while DOWN waits on LOCK TABLE,
        # even if the session's default_transaction_isolation is repeatable read.
        rr_url = make_conninfo(
            self.url,
            options=r"-c default_transaction_isolation=repeatable\ read",
        )
        with psycopg.connect(self.url, autocommit=True) as other, ThreadPoolExecutor(max_workers=1) as pool:
            with other.transaction():
                other.execute(
                    "UPDATE runtime_environment_states"
                    " SET memory_retired = TRUE, memory_updated_at = %s, memory_reason = 'race retire'"
                    " WHERE environment_ref = %s",
                    (CHANGE_AT, self.deprecated_env),
                )
                down_future = pool.submit(apply_migrations, rr_url, [DOWN])
                wait_until_waiting(down_future, states_oid, "AccessExclusiveLock")
            with self.assertRaises(errors.RaiseException) as caught:
                down_future.result(timeout=5.0)
            self.assertIn(
                "runtime_environment_states has memory_retired environments",
                str(caught.exception),
            )
            self.assertEqual(
                self.memory_tuple(self.deprecated_env),
                (True, CHANGE_AT, SEED_USER, "race retire"),
            )

            self.conn.execute(
                "UPDATE runtime_environment_states"
                " SET memory_retired = FALSE, memory_updated_at = %s, memory_reason = 'un-retire'"
                " WHERE environment_ref = %s",
                (CHANGE_AT, self.deprecated_env),
            )

            # DOWN locks users before runtime_environment_states so an in-flight user
            # deletion's ON DELETE SET NULL cascade doesn't deadlock with it.
            down_victim = self.insert_user()
            self.conn.execute(
                "UPDATE runtime_environment_states"
                " SET memory_updated_by_user_id = %s WHERE environment_ref = %s",
                (down_victim, self.deprecated_env),
            )
            with other.transaction(force_rollback=True):
                other.execute(
                    "UPDATE users SET display_name = 'retiring' WHERE id = %s",
                    (down_victim,),
                )
                down_future = pool.submit(apply_migrations, self.url, [DOWN])
                wait_until_waiting(down_future, users_oid)
                other.execute("SET LOCAL lock_timeout = '200ms'")
                other.execute("DELETE FROM users WHERE id = %s", (down_victim,))
                with self.assertRaises(errors.LockNotAvailable):
                    down_future.result(timeout=5.0)

            apply_migrations(self.url, [DOWN])
            self.assertEqual(self.shape(), self.before)

            # UP also locks users before runtime_environment_states, so an in-flight user
            # deletion's ON DELETE SET NULL cascade completes while UP hits retried 55P03.
            victim = self.insert_user()
            with other.transaction(force_rollback=True):
                other.execute("UPDATE users SET display_name = 'retiring' WHERE id = %s", (victim,))
                up_future = pool.submit(apply_migrations, self.url, [UP])
                wait_until_waiting(up_future, users_oid)
                other.execute("SET LOCAL lock_timeout = '200ms'")
                other.execute("DELETE FROM users WHERE id = %s", (victim,))
                with self.assertRaises(errors.LockNotAvailable):
                    up_future.result(timeout=5.0)

            # An in-flight policy writer holding ACCESS SHARE on runtime_environment_states
            # checks users FOR KEY SHARE without deadlocking while UP holds SHARE ROW EXCLUSIVE
            # on users and waits for ACCESS EXCLUSIVE on runtime_environment_states.
            writer_actor = self.insert_user()
            with other.transaction():
                other.execute(
                    "SELECT 1 FROM runtime_environment_states WHERE environment_ref = %s",
                    (self.allowed_env,),
                )
                up_future = pool.submit(apply_migrations, self.url, [UP])
                wait_until_waiting(up_future, states_oid, "AccessExclusiveLock")
                other.execute("SET LOCAL lock_timeout = '200ms'")
                other.execute(
                    "UPDATE runtime_environment_policy"
                    " SET generation = generation + 1, updated_by_user_id = %s,"
                    "     reason = 'concurrent policy write'",
                    (writer_actor,),
                )
            up_future.result(timeout=5.0)
            self.assertEqual(self.memory_tuple(self.deprecated_env), (False, None, None, None))


if __name__ == "__main__":
    unittest.main()
