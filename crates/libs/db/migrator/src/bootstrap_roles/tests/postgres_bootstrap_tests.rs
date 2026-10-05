use super::super::*;
use crate::pg_test_support::{PostgresFixture, TestResult, postgres_fixture};

const TEST_TIMEOUTS: SessionTimeouts<'static> = SessionTimeouts {
    app_statement: "750ms",
    app_idle_in_transaction: "2500ms",
    app_lock: Some("250ms"),
    migrator_statement: "5min",
    migrator_lock: "2min",
};

/// An explicit rollback to unbounded lock waits.
const APP_LOCK_DISABLED_TIMEOUTS: SessionTimeouts<'static> = SessionTimeouts {
    app_lock: Some("0"),
    ..TEST_TIMEOUTS
};

/// No `lock_timeout` supplied — the shape every entry point that carries no
/// environment config produces (the bare command, the EKS one-shot Job, the
/// devbox and kind bootstraps).
const APP_LOCK_UNSET_TIMEOUTS: SessionTimeouts<'static> = SessionTimeouts {
    app_lock: None,
    ..TEST_TIMEOUTS
};

fn test_passwords() -> BootstrapPasswords {
    BootstrapPasswords {
        superadmin: Some("superadmin-pw".to_string()),
        user: Some("user-pw".to_string()),
        ro: Some("ro-pw".to_string()),
        migrator: Some("migrator-pw".to_string()),
        creds: Some("creds-pw".to_string()),
    }
}

async fn role_has_table_privilege(
    fixture: &PostgresFixture,
    role: &str,
    table: &str,
    privilege: &str,
) -> TestResult<bool> {
    Ok(
        sqlx::query_scalar(sql!("SELECT has_table_privilege($1, $2::regclass, $3)"))
            .bind(role)
            .bind(table)
            .bind(privilege)
            .fetch_one(&fixture.pool)
            .await?,
    )
}

async fn role_settings(fixture: &PostgresFixture, role: &str) -> TestResult<Vec<String>> {
    Ok(sqlx::query_scalar(sql!(
        r"
        SELECT unnest(s.setconfig)
        FROM pg_db_role_setting s
        JOIN pg_roles r ON r.oid = s.setrole
        WHERE r.rolname = $1 AND s.setdatabase = 0
        "
    ))
    .bind(role)
    .fetch_all(&fixture.pool)
    .await?)
}

async fn run_bootstrap(fixture: &PostgresFixture) -> TestResult {
    let mut conn = fixture.connect_as("postgres").await?;
    bootstrap(&mut conn, &test_passwords(), &TEST_TIMEOUTS).await?;
    Ok(())
}

/// A table for the lock-wait tests. The fixture's database is unmigrated, and
/// `space_user`'s grants come from `space_migrator`'s default privileges, so an
/// admin-created scratch table needs an explicit grant.
async fn create_lock_test_table(fixture: &PostgresFixture, table: &str) -> TestResult {
    let mut conn = fixture.connect_as("postgres").await?;
    let create = format!("CREATE TABLE {} (id INT)", quote_ident(table));
    sqlx::query(sqlx::AssertSqlSafe(sql!(create.as_str())))
        .execute(&mut conn)
        .await?;
    let grant = format!("GRANT SELECT ON {} TO {USER_ROLE}", quote_ident(table));
    sqlx::query(sqlx::AssertSqlSafe(sql!(grant.as_str())))
        .execute(&mut conn)
        .await?;
    Ok(())
}

fn assert_sqlstate(error: &sqlx::Error, expected: &str) {
    let code = sqlstate(error);
    assert_eq!(code.as_deref(), Some(expected), "unexpected error: {error}");
}

#[tokio::test]
async fn fresh_database_bootstraps_migrates_as_migrator_and_fences() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };

    // Fresh database: bootstrap creates the roles first...
    run_bootstrap(&fixture).await?;

    // ...then the real `control-plane up` path applies every
    // migration. It connects as the admin but assumes space_migrator
    // via SET ROLE — including through the guarded legacy migration
    // that grants credential tables to the pre-created space_credmgr
    // role.
    crate::run_control_plane(crate::ControlPlaneArgs {
        job_deadline_seconds: crate::migration_retry::MIGRATION_JOB_ACTIVE_DEADLINE.as_secs(),
        database_url: fixture.database_url("postgres"),
        confirm_data_loss: false,
        command: crate::Command::Up,
    })
    .await?;

    // Migration-created tables are migrator-owned before any
    // ownership adoption ran: SET ROLE did its job.
    let sandboxes_owner: String = sqlx::query_scalar(sql!(
        "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE relname = 'sandboxes'"
    ))
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        sandboxes_owner, MIGRATOR_ROLE,
        "control-plane migrations must run as space_migrator"
    );

    // Converge grants and fencing over the migrated schema.
    run_bootstrap(&fixture).await?;

    // space_superadmin is a full superuser on self-managed Postgres;
    // the connecting master role is untouched.
    let superadmin_is_super: bool =
        sqlx::query_scalar(sql!("SELECT rolsuper FROM pg_roles WHERE rolname = $1"))
            .bind(SUPERADMIN_ROLE)
            .fetch_one(&fixture.pool)
            .await?;
    assert!(superadmin_is_super);

    // The connecting admin is never managed: no bootstrap role may
    // collide with the session user, and running the tool while
    // connected as a managed role is refused outright.
    let mut as_migrator = fixture.connect_as(MIGRATOR_ROLE).await?;
    let collision = bootstrap(&mut as_migrator, &test_passwords(), &TEST_TIMEOUTS)
        .await
        .expect_err("bootstrap must refuse to run as a managed role");
    assert!(
        collision.to_string().contains("refusing to touch"),
        "unexpected error: {collision}"
    );
    drop(as_migrator);

    // Structural invariant: every wrapped-DEK-bearing table in the
    // migrated schema must be in the credential fence list.
    let dek_tables: Vec<String> = sqlx::query_scalar(sql!(
        "SELECT DISTINCT table_name FROM information_schema.columns \
         WHERE table_schema = 'public' AND column_name = 'wrapped_dek'"
    ))
    .fetch_all(&fixture.pool)
    .await?;
    assert!(!dek_tables.is_empty());
    for table in &dek_tables {
        assert!(
            CREDENTIAL_TABLES.contains(&table.as_str()),
            "{table} carries wrapped DEKs but is not in CREDENTIAL_TABLES"
        );
    }

    // Behavioral fence: every credential table is inaccessible to
    // space_user and space_ro and fully writable by space_creds.
    for table in CREDENTIAL_TABLES {
        let exists: bool = sqlx::query_scalar(sql!("SELECT to_regclass($1) IS NOT NULL"))
            .bind(format!("public.{table}"))
            .fetch_one(&fixture.pool)
            .await?;
        assert!(exists, "credential table {table} missing from schema");
        for (role, privilege, expected) in [
            (USER_ROLE, "SELECT", false),
            (USER_ROLE, "INSERT", false),
            (RO_ROLE, "SELECT", false),
            (CREDS_ROLE, "SELECT", true),
            (CREDS_ROLE, "INSERT", true),
            (CREDS_ROLE, "UPDATE", true),
            (CREDS_ROLE, "DELETE", true),
        ] {
            assert_eq!(
                role_has_table_privilege(&fixture, role, table, privilege).await?,
                expected,
                "{role} {privilege} on {table}"
            );
        }
    }

    // Column fence: hash/bearer columns are hidden from space_ro
    // while the surrounding table stays readable and space_user
    // keeps full access.
    for (table, denied_columns) in RO_COLUMN_DENYLIST {
        let exists: bool = sqlx::query_scalar(sql!("SELECT to_regclass($1) IS NOT NULL"))
            .bind(format!("public.{table}"))
            .fetch_one(&fixture.pool)
            .await?;
        assert!(exists, "denylisted table {table} missing from schema");
        for column in *denied_columns {
            for (role, expected) in [(RO_ROLE, false), (USER_ROLE, true)] {
                let has: bool = sqlx::query_scalar(sql!(
                    "SELECT has_column_privilege($1, $2::regclass, $3, 'SELECT')"
                ))
                .bind(role)
                .bind(format!("public.{table}"))
                .bind(column)
                .fetch_one(&fixture.pool)
                .await?;
                assert_eq!(has, expected, "{role} SELECT on {table}.{column}");
            }
        }
    }
    let ro_reads_safe_column: bool = sqlx::query_scalar(sql!(
        "SELECT has_column_privilege($1, 'public.users'::regclass, 'id', 'SELECT')"
    ))
    .bind(RO_ROLE)
    .fetch_one(&fixture.pool)
    .await?;
    assert!(ro_reads_safe_column);

    // Structural invariant: every auth-hash/bearer column in the
    // schema is covered by the credential fence or the denylist.
    let sensitive: Vec<(String, String)> = sqlx::query_as(sql!(
        r"
        SELECT table_name, column_name FROM information_schema.columns
        WHERE table_schema = 'public'
          AND column_name ~ '(password_hash|token_hash|key_hash|grant_blob)$'
        "
    ))
    .fetch_all(&fixture.pool)
    .await?;
    assert!(!sensitive.is_empty());
    for (table, column) in &sensitive {
        let covered = CREDENTIAL_TABLES.contains(&table.as_str())
            || RO_COLUMN_DENYLIST
                .iter()
                .any(|(t, cols)| t == table && cols.contains(&column.as_str()));
        assert!(
            covered,
            "{table}.{column} holds auth material but is not fenced from space_ro"
        );
    }

    // Role-level GUCs do not survive SET ROLE, so the migration hook
    // installs the migrator timeouts explicitly.
    let mut hooked = fixture.connect_as("postgres").await?;
    assume_migrator_role(&mut hooked).await?;
    let statement_timeout: String = sqlx::query_scalar(sql!("SHOW statement_timeout"))
        .fetch_one(&mut hooked)
        .await?;
    assert_eq!(statement_timeout, "5min");
    let lock_timeout: String = sqlx::query_scalar(sql!("SHOW lock_timeout"))
        .fetch_one(&mut hooked)
        .await?;
    assert_eq!(lock_timeout, "2min");
    drop(hooked);

    // space_user reads and writes application tables but cannot
    // touch credential tables.
    let mut user = fixture.connect_as(USER_ROLE).await?;
    sqlx::query(sql!("SELECT COUNT(*) FROM sandboxes"))
        .fetch_one(&mut user)
        .await?;
    assert!(role_has_table_privilege(&fixture, USER_ROLE, "sandboxes", "INSERT").await?);
    let fence_error = sqlx::query(sql!("SELECT COUNT(*) FROM user_credentials"))
        .fetch_one(&mut user)
        .await
        .expect_err("space_user must not read credential tables");
    assert_sqlstate(&fence_error, "42501");
    let timeout_error = sqlx::query(sql!("SELECT pg_sleep(5)"))
        .execute(&mut user)
        .await
        .expect_err("space_user statement_timeout must fire");
    assert_sqlstate(&timeout_error, "57014");

    // Short session timeouts apply to all three application logins;
    // the migrator gets the minutes-scale DDL budget.
    for role in [USER_ROLE, RO_ROLE, CREDS_ROLE] {
        let settings = role_settings(&fixture, role).await?;
        assert!(
            settings.contains(&"statement_timeout=750ms".to_string()),
            "{role} settings: {settings:?}"
        );
        assert!(
            settings.contains(&"idle_in_transaction_session_timeout=2500ms".to_string()),
            "{role} settings: {settings:?}"
        );
    }
    // `space_creds` is in this list, not held out of it: the credential lane's
    // boundaries classify contention now, so the bound is the same improvement
    // there as on the other two. `space_migrator` stays out — DDL legitimately
    // needs to wait.
    for role in [USER_ROLE, RO_ROLE, CREDS_ROLE] {
        let settings = role_settings(&fixture, role).await?;
        assert!(
            settings.contains(&"lock_timeout=250ms".to_string()),
            "{role} settings: {settings:?}"
        );
    }
    let migrator_settings = role_settings(&fixture, MIGRATOR_ROLE).await?;
    assert!(migrator_settings.contains(&"statement_timeout=5min".to_string()));
    assert!(migrator_settings.contains(&"lock_timeout=2min".to_string()));

    // space_ro reads application tables, writes nothing, and cannot
    // read credential tables.
    let mut ro = fixture.connect_as(RO_ROLE).await?;
    sqlx::query(sql!("SELECT COUNT(*) FROM sandboxes"))
        .fetch_one(&mut ro)
        .await?;
    assert!(!role_has_table_privilege(&fixture, RO_ROLE, "sandboxes", "INSERT").await?);
    let ro_fence_error = sqlx::query(sql!("SELECT COUNT(*) FROM user_credentials"))
        .fetch_one(&mut ro)
        .await
        .expect_err("space_ro must not read credential tables");
    assert_sqlstate(&ro_fence_error, "42501");

    // space_creds reads and writes everything, credential tables
    // included.
    let mut creds = fixture.connect_as(CREDS_ROLE).await?;
    sqlx::query(sql!("SELECT COUNT(*) FROM user_credentials"))
        .fetch_one(&mut creds)
        .await?;
    assert!(role_has_table_privilege(&fixture, CREDS_ROLE, "user_credentials", "INSERT").await?);
    assert!(role_has_table_privilege(&fixture, CREDS_ROLE, "sandboxes", "UPDATE").await?);

    // space_migrator can run DDL on pre-existing and new tables, and
    // its new tables are writable by space_user and space_creds and
    // readable by space_ro via default privileges without another
    // bootstrap run.
    let mut migrator = fixture.connect_as(MIGRATOR_ROLE).await?;
    sqlx::query(sql!(
        "ALTER TABLE sandboxes ADD COLUMN bootstrap_probe BOOLEAN"
    ))
    .execute(&mut migrator)
    .await?;
    sqlx::query(sql!("CREATE TABLE bootstrap_probe (id BIGINT PRIMARY KEY)"))
        .execute(&mut migrator)
        .await?;
    sqlx::query(sql!("INSERT INTO bootstrap_probe (id) VALUES (1)"))
        .execute(&mut user)
        .await?;
    sqlx::query(sql!("INSERT INTO bootstrap_probe (id) VALUES (2)"))
        .execute(&mut creds)
        .await?;
    sqlx::query(sql!("SELECT COUNT(*) FROM bootstrap_probe"))
        .fetch_one(&mut ro)
        .await?;
    let ro_write_error = sqlx::query(sql!("INSERT INTO bootstrap_probe (id) VALUES (3)"))
        .execute(&mut ro)
        .await
        .expect_err("space_ro must not write application tables");
    assert_sqlstate(&ro_write_error, "42501");

    // A table created by another admin converges on the next run.
    sqlx::query(sql!("CREATE TABLE stray_probe (id BIGINT PRIMARY KEY)"))
        .execute(&fixture.pool)
        .await?;
    run_bootstrap(&fixture).await?;
    sqlx::query(sql!("SELECT COUNT(*) FROM stray_probe"))
        .fetch_one(&mut user)
        .await?;
    let stray_owner: String = sqlx::query_scalar(sql!(
        "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE relname = 'stray_probe'"
    ))
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(stray_owner, MIGRATOR_ROLE);

    Ok(())
}

#[tokio::test]
async fn first_bootstrap_adopts_admin_owned_tables_routines_and_types() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let mut admin = fixture.connect_as("postgres").await?;

    // A legacy admin-owned table exists before any bootstrap, and
    // the PG15+ locked-down public schema grants space_migrator
    // nothing yet: the first run must still adopt it, which requires
    // schema privileges to converge before ownership adoption
    // (ALTER ... OWNER TO needs the new owner to hold CREATE on the
    // schema).
    sqlx::query(sql!("CREATE TABLE legacy_probe (id BIGINT PRIMARY KEY)"))
        .execute(&mut admin)
        .await?;

    // The same legacy database's trigger functions and enum, created back
    // when migrations ran as the admin. Replacing a function or
    // extending an enum requires ownership — not table or schema
    // privileges — so bootstrap must adopt these exactly as it
    // adopts relations.
    sqlx::query(sql!(
        "CREATE FUNCTION legacy_touch() RETURNS trigger AS \
         $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql"
    ))
    .execute(&mut admin)
    .await?;
    sqlx::query(sql!(
        "CREATE FUNCTION legacy_add(a integer, b integer) RETURNS integer AS \
         $$ SELECT a + b $$ LANGUAGE sql"
    ))
    .execute(&mut admin)
    .await?;
    sqlx::query(sql!("CREATE TYPE legacy_mood AS ENUM ('happy')"))
        .execute(&mut admin)
        .await?;
    sqlx::query(sql!("CREATE TYPE legacy_pair AS (x integer, y integer)"))
        .execute(&mut admin)
        .await?;

    bootstrap(&mut admin, &test_passwords(), &TEST_TIMEOUTS).await?;

    let owner: String = sqlx::query_scalar(sql!(
        "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE relname = 'legacy_probe'"
    ))
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(owner, MIGRATOR_ROLE);
    for routine in ["legacy_touch", "legacy_add"] {
        let owner: String = sqlx::query_scalar(sql!(
            "SELECT pg_get_userbyid(proowner) FROM pg_proc WHERE proname = $1"
        ))
        .bind(routine)
        .fetch_one(&fixture.pool)
        .await?;
        assert_eq!(owner, MIGRATOR_ROLE, "{routine} must be adopted");
    }
    for type_name in ["legacy_mood", "legacy_pair"] {
        let type_owner: String = sqlx::query_scalar(sql!(
            "SELECT pg_get_userbyid(typowner) FROM pg_type WHERE typname = $1"
        ))
        .bind(type_name)
        .fetch_one(&fixture.pool)
        .await?;
        assert_eq!(type_owner, MIGRATOR_ROLE, "{type_name} must be adopted");
    }

    // The operations that motivated adoption now work as the
    // migrator: replacing the function body and altering the types.
    let mut migrator = fixture.connect_as(MIGRATOR_ROLE).await?;
    sqlx::query(sql!(
        "CREATE OR REPLACE FUNCTION legacy_touch() RETURNS trigger AS \
         $$ BEGIN NEW := NEW; RETURN NEW; END $$ LANGUAGE plpgsql"
    ))
    .execute(&mut migrator)
    .await?;
    sqlx::query(sql!("ALTER TYPE legacy_mood ADD VALUE 'grumpy'"))
        .execute(&mut migrator)
        .await?;
    sqlx::query(sql!("ALTER TYPE legacy_pair ADD ATTRIBUTE z integer"))
        .execute(&mut migrator)
        .await?;
    Ok(())
}

#[tokio::test]
async fn bootstrap_adopts_routines_behind_admin_run_migrations() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };

    // Exercise adoption when every migration was applied as the admin:
    // bootstrap converges, and a later migration running as space_migrator
    // replaces one of those admin-era trigger functions in place.
    crate::run_control_plane(crate::ControlPlaneArgs {
        job_deadline_seconds: crate::migration_retry::MIGRATION_JOB_ACTIVE_DEADLINE.as_secs(),
        database_url: fixture.database_url("postgres"),
        confirm_data_loss: false,
        command: crate::Command::Up,
    })
    .await?;

    // Before bootstrap ever runs, assume_migrator_role has no space_migrator to
    // assume: it must warn and fall back to the connecting admin rather than
    // failing the deploy, which is also the precondition the adoption
    // assertions below depend on.
    let owner: String = sqlx::query_scalar(sql!(
        "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE relname = 'sandboxes'"
    ))
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        owner, "postgres",
        "without bootstrap, migrations keep the connecting identity"
    );

    let mut admin = fixture.connect_as("postgres").await?;
    bootstrap(&mut admin, &test_passwords(), &TEST_TIMEOUTS).await?;

    let straggler_routines: i64 = sqlx::query_scalar(sql!(
        r"
        SELECT COUNT(*)
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        JOIN pg_roles owner ON owner.oid = p.proowner
        WHERE n.nspname = 'public'
          AND owner.rolname <> $1
          AND NOT EXISTS (
              SELECT FROM pg_depend d
              WHERE d.classid = 'pg_proc'::regclass
                AND d.objid = p.oid
                AND d.deptype = 'e'
          )
        "
    ))
    .bind(MIGRATOR_ROLE)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        straggler_routines, 0,
        "every migration-created routine must be migrator-owned after bootstrap"
    );
    let straggler_types: i64 = sqlx::query_scalar(sql!(
        r"
        SELECT COUNT(*)
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        JOIN pg_roles owner ON owner.oid = t.typowner
        WHERE n.nspname = 'public'
          AND (
              t.typtype IN ('e', 'd', 'r')
              OR (
                  t.typtype = 'c'
                  AND EXISTS (
                      SELECT FROM pg_class c
                      WHERE c.oid = t.typrelid
                        AND c.relkind = 'c'
                  )
              )
          )
          AND owner.rolname <> $1
          AND NOT EXISTS (
              SELECT FROM pg_depend d
              WHERE d.classid = 'pg_type'::regclass
                AND d.objid = t.oid
                AND d.deptype = 'e'
          )
        "
    ))
    .bind(MIGRATOR_ROLE)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        straggler_types, 0,
        "every migration-created standalone type must be migrator-owned after bootstrap"
    );

    // Ownership probe against a real migration-era trigger function:
    // COMMENT ON requires function ownership, the same check that
    // fails a CREATE OR REPLACE from a non-owner.
    let mut migrator = fixture.connect_as(MIGRATOR_ROLE).await?;
    sqlx::query(sql!(
        "COMMENT ON FUNCTION public.sandboxes_clear_restore_lease() IS NULL"
    ))
    .execute(&mut migrator)
    .await?;
    Ok(())
}

#[tokio::test]
async fn bootstrap_strips_preexisting_privileges() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let mut admin = fixture.connect_as("postgres").await?;

    // An upgrade scenario: managed roles already exist with
    // privileged attributes and memberships the model never grants.
    sqlx::query(sql!("CREATE ROLE space_migrator NOLOGIN"))
        .execute(&mut admin)
        .await?;
    sqlx::query(sql!("CREATE ROLE space_user WITH CREATEDB CREATEROLE"))
        .execute(&mut admin)
        .await?;
    sqlx::query(sql!("GRANT space_migrator TO space_user"))
        .execute(&mut admin)
        .await?;

    bootstrap(&mut admin, &test_passwords(), &TEST_TIMEOUTS).await?;

    let (createdb, createrole): (bool, bool) = sqlx::query_as(sql!(
        "SELECT rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname = $1"
    ))
    .bind(USER_ROLE)
    .fetch_one(&fixture.pool)
    .await?;
    assert!(!createdb && !createrole, "space_user attributes must reset");

    let membership_count: i64 = sqlx::query_scalar(sql!(
        r"
        SELECT COUNT(*) FROM pg_auth_members m
        JOIN pg_roles member ON member.oid = m.member
        JOIN pg_roles granted ON granted.oid = m.roleid
        WHERE member.rolname = $1 AND granted.rolname = $2
        "
    ))
    .bind(USER_ROLE)
    .bind(MIGRATOR_ROLE)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(membership_count, 0, "space_user must lose space_migrator");

    // The intended matrix survives.
    let expected_membership: i64 = sqlx::query_scalar(sql!(
        r"
        SELECT COUNT(*) FROM pg_auth_members m
        JOIN pg_roles member ON member.oid = m.member
        JOIN pg_roles granted ON granted.oid = m.roleid
        WHERE member.rolname = $1 AND granted.rolname = $2
        "
    ))
    .bind(CREDS_ROLE)
    .bind(CREDMGR_CAPABILITY_ROLE)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(expected_membership, 1);
    Ok(())
}

#[tokio::test]
async fn missing_passwords_provision_nologin_roles() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let mut conn = fixture.connect_as("postgres").await?;
    let no_passwords = BootstrapPasswords::default();
    bootstrap(&mut conn, &no_passwords, &TEST_TIMEOUTS).await?;
    // Idempotent re-run.
    bootstrap(&mut conn, &no_passwords, &TEST_TIMEOUTS).await?;

    for role in [
        SUPERADMIN_ROLE,
        USER_ROLE,
        RO_ROLE,
        MIGRATOR_ROLE,
        CREDS_ROLE,
        CREDMGR_CAPABILITY_ROLE,
    ] {
        let can_login: bool =
            sqlx::query_scalar(sql!("SELECT rolcanlogin FROM pg_roles WHERE rolname = $1"))
                .bind(role)
                .fetch_one(&mut conn)
                .await?;
        assert!(!can_login, "{role} must stay NOLOGIN without a password");
    }
    Ok(())
}

/// The role-level `lock_timeout` across its whole provisioning lifecycle, on
/// one server: a first bootstrap that omits it, enabling it, a config-less
/// re-provision, and an explicit rollback. Each phase's precondition is the
/// state the phase before it leaves behind, and the phases are ordered so that
/// the setting is exercised in every direction a real environment moves it.
///
/// Asserting the `pg_db_role_setting` row does not prove the bound: the
/// setting could be written and still not reach a pooled session, and it
/// covers lock classes that are easy to assume are exempt. So every phase that
/// claims a state also provokes a real wait and reads the SQLSTATE back.
///
/// The DDL case is what motivated the bound: a `space_user` read blocked
/// behind `ACCESS EXCLUSIVE` on a hot table holds its pooled connection for
/// the whole wait, so enough concurrent readers exhaust the pool while the
/// database itself is idle.
///
/// The advisory-lock case is the one that surprises: `pg_advisory_xact_lock`
/// goes through the same heavyweight lock manager as a table or row lock, so
/// it is bounded too. The public-API idempotency guards and the fork-source
/// lifecycle guard are built on it, and they *want* to wait for a peer
/// transaction — which is why their callers have to treat `55P03` as
/// retryable rather than fatal.
///
/// The omitted-value phases exist because `bootstrap-roles` is convergent and
/// runs from several entry points that carry no environment config: the bare
/// command documented in `db/README.md`,
/// `scripts/space-db-bootstrap-roles-eks.sh`, and the devbox and kind
/// bootstraps. A fresh database must not invent a bound none of them asked
/// for, and an already-enabled cluster must not lose one — if an omitted value
/// were treated as `0`, any one of those runs would silently un-bound every
/// application role, a fleet-wide behavior change with nothing in the output
/// naming it. Rollback stays possible, but only by asking for it: the explicit
/// `"0"` is written rather than skipped, because nothing else in the system
/// resets the setting and the alternative is a hand-run `ALTER ROLE`.
#[tokio::test]
async fn app_lock_timeout_converges_across_its_provisioning_lifecycle() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };

    // Fresh database, no `lock_timeout` asked for: the bound is opt-in per
    // environment, so a first bootstrap must not invent one.
    let mut conn = fixture.connect_as("postgres").await?;
    bootstrap(&mut conn, &test_passwords(), &APP_LOCK_UNSET_TIMEOUTS).await?;
    drop(conn);
    for role in [USER_ROLE, RO_ROLE, CREDS_ROLE] {
        let settings = role_settings(&fixture, role).await?;
        assert!(
            !settings.iter().any(|s| s.starts_with("lock_timeout=")),
            "{role} got an uninvited lock_timeout: {settings:?}"
        );
        // The always-written app timeouts are unaffected by the omission.
        assert!(
            settings.contains(&"statement_timeout=750ms".to_string()),
            "{role} settings: {settings:?}"
        );
    }

    // Enabling the bound writes it and, more importantly, enforces it.
    run_bootstrap(&fixture).await?;
    for role in [USER_ROLE, RO_ROLE] {
        let settings = role_settings(&fixture, role).await?;
        assert!(
            settings.contains(&"lock_timeout=250ms".to_string()),
            "{role} settings: {settings:?}"
        );
    }
    create_lock_test_table(&fixture, "lock_probe").await?;

    let mut blocker = fixture.connect_as("postgres").await?;
    sqlx::query(sql!("BEGIN")).execute(&mut blocker).await?;
    sqlx::query(sql!("LOCK TABLE lock_probe IN ACCESS EXCLUSIVE MODE"))
        .execute(&mut blocker)
        .await?;

    let mut user = fixture.connect_as(USER_ROLE).await?;
    let blocked = sqlx::query(sql!("SELECT COUNT(*) FROM lock_probe"))
        .fetch_one(&mut user)
        .await
        .expect_err("a read behind ACCESS EXCLUSIVE must hit space_user's lock_timeout");
    // 55P03, not 57014: the bound has to be the *lock* timeout, otherwise the
    // statement held its connection for the full statement_timeout first.
    assert_sqlstate(&blocked, "55P03");
    sqlx::query(sql!("ROLLBACK")).execute(&mut blocker).await?;

    sqlx::query(sql!("BEGIN")).execute(&mut blocker).await?;
    sqlx::query(sql!("SELECT pg_advisory_xact_lock(918273)"))
        .execute(&mut blocker)
        .await?;
    let contended = sqlx::query(sql!("SELECT pg_advisory_xact_lock(918273)"))
        .execute(&mut user)
        .await
        .expect_err("an advisory lock wait must hit space_user's lock_timeout");
    assert_sqlstate(&contended, "55P03");
    sqlx::query(sql!("ROLLBACK")).execute(&mut blocker).await?;

    // The same read succeeds once the blocker releases, so the abort was the
    // bound firing rather than a missing grant.
    sqlx::query(sql!("SELECT COUNT(*) FROM lock_probe"))
        .fetch_one(&mut user)
        .await?;
    drop(user);

    // A config-less re-provision over the enabled environment leaves it
    // enabled.
    let mut conn = fixture.connect_as("postgres").await?;
    bootstrap(&mut conn, &test_passwords(), &APP_LOCK_UNSET_TIMEOUTS).await?;
    drop(conn);
    for role in [USER_ROLE, RO_ROLE] {
        let settings = role_settings(&fixture, role).await?;
        assert!(
            settings.contains(&"lock_timeout=250ms".to_string()),
            "{role} kept no lock_timeout across a config-less re-provision: {settings:?}"
        );
    }

    // Still enforced, not merely still recorded: a re-provision that dropped
    // the setting and a re-provision that left it in place are indistinguishable
    // from `pg_db_role_setting` alone if the value never reached a session.
    create_lock_test_table(&fixture, "unset_lock_probe").await?;
    sqlx::query(sql!("BEGIN")).execute(&mut blocker).await?;
    sqlx::query(sql!("LOCK TABLE unset_lock_probe IN ACCESS EXCLUSIVE MODE"))
        .execute(&mut blocker)
        .await?;
    let mut user = fixture.connect_as(USER_ROLE).await?;
    let blocked = sqlx::query(sql!("SELECT COUNT(*) FROM unset_lock_probe"))
        .fetch_one(&mut user)
        .await
        .expect_err("the preserved bound must still fire");
    assert_sqlstate(&blocked, "55P03");
    sqlx::query(sql!("ROLLBACK")).execute(&mut blocker).await?;
    drop(user);

    // An explicit `"0"` rolls the environment back to unbounded waits.
    let mut conn = fixture.connect_as("postgres").await?;
    bootstrap(&mut conn, &test_passwords(), &APP_LOCK_DISABLED_TIMEOUTS).await?;
    drop(conn);
    for role in [USER_ROLE, RO_ROLE] {
        let settings = role_settings(&fixture, role).await?;
        assert!(
            settings.contains(&"lock_timeout=0".to_string()),
            "{role} settings: {settings:?}"
        );
        // The other app timeouts are untouched by the flip.
        assert!(
            settings.contains(&"statement_timeout=750ms".to_string()),
            "{role} settings: {settings:?}"
        );
    }

    // A disabled bound waits instead of aborting: with the table locked, the
    // read now runs out `statement_timeout` (57014) rather than `lock_timeout`.
    create_lock_test_table(&fixture, "unbounded_lock_probe").await?;
    sqlx::query(sql!("BEGIN")).execute(&mut blocker).await?;
    sqlx::query(sql!(
        "LOCK TABLE unbounded_lock_probe IN ACCESS EXCLUSIVE MODE"
    ))
    .execute(&mut blocker)
    .await?;
    let mut user = fixture.connect_as(USER_ROLE).await?;
    let waited = sqlx::query(sql!("SELECT COUNT(*) FROM unbounded_lock_probe"))
        .fetch_one(&mut user)
        .await
        .expect_err("an unbounded lock wait must end at statement_timeout");
    assert_sqlstate(&waited, "57014");
    sqlx::query(sql!("ROLLBACK")).execute(&mut blocker).await?;
    Ok(())
}

/// `ALTER ROLE ... SET` only loads at login, so `SET ROLE` alone leaves the
/// session on the connecting role's timeouts. `assume_migrator_role` replays
/// `pg_db_role_setting` to compensate, and that replay is the only mechanism
/// delivering the migrator's `lock_timeout` on the admin-connect path
/// production uses — which is also the bound `migration_retry`'s retry window
/// is sized against. Deleting the replay loop otherwise leaves every suite
/// green while that premise quietly evaporates.
#[tokio::test]
async fn assuming_the_migrator_role_installs_its_role_level_timeouts() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    run_bootstrap(&fixture).await?;

    // Connect as the admin, exactly as `control-plane up` does, and confirm the
    // bare `SET ROLE` gap this function exists to close.
    let mut conn = fixture.connect_as("postgres").await?;
    sqlx::query(sql!("SET ROLE space_migrator"))
        .execute(&mut conn)
        .await?;
    let without_replay: String = sqlx::query_scalar(sql!("SELECT current_setting('lock_timeout')"))
        .fetch_one(&mut conn)
        .await?;
    let statement_without_replay: String =
        sqlx::query_scalar(sql!("SELECT current_setting('statement_timeout')"))
            .fetch_one(&mut conn)
            .await?;
    // Derived, not the literal server defaults: what matters is that bare
    // `SET ROLE` does *not* produce the role's configured values. Comparing
    // against "0" would go green if someone set a test timeout to "0" too, and
    // then the assertions below would pass without the replay running at all.
    // Both settings need the guard, since both are asserted afterwards.
    assert_ne!(
        without_replay, TEST_TIMEOUTS.migrator_lock,
        "SET ROLE alone should not load the role's lock_timeout; if it does, the \
         replay in assume_migrator_role is no longer load-bearing"
    );
    assert_ne!(
        statement_without_replay, TEST_TIMEOUTS.migrator_statement,
        "SET ROLE alone should not load the role's statement_timeout; if it \
         does, the replay in assume_migrator_role is no longer load-bearing"
    );

    let mut conn = fixture.connect_as("postgres").await?;
    assume_migrator_role(&mut conn).await?;
    let assumed: String = sqlx::query_scalar(sql!("SELECT current_user"))
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(
        assumed, MIGRATOR_ROLE,
        "the SET ROLE half has to take effect too, or migration-created objects \
         land with the wrong owner"
    );
    let lock_timeout: String = sqlx::query_scalar(sql!("SELECT current_setting('lock_timeout')"))
        .fetch_one(&mut conn)
        .await?;
    let statement_timeout: String =
        sqlx::query_scalar(sql!("SELECT current_setting('statement_timeout')"))
            .fetch_one(&mut conn)
            .await?;
    assert_eq!(
        lock_timeout, TEST_TIMEOUTS.migrator_lock,
        "the replay must install the role's lock_timeout; the retry window in \
         migration_retry is sized against it"
    );
    assert_eq!(
        statement_timeout, TEST_TIMEOUTS.migrator_statement,
        "the replay must install the role's statement_timeout, or a slow DDL \
         dies at the connecting role's bound instead"
    );
    Ok(())
}

#[tokio::test]
async fn credential_rules_enforce_constraints_triggers_and_grants() -> TestResult {
    use pplx_space_common_svc::core::types::{CredentialReferenceId, SandboxId};
    use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID};
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    run_bootstrap(&fixture).await?;
    let mut conn = fixture.connect_as("postgres").await?;
    assume_migrator_role(&mut conn).await?;
    crate::POSTGRES_MIGRATOR.run(&mut conn).await?;
    let sandbox = SandboxId::new().as_uuid();
    let reference = CredentialReferenceId::new().as_uuid();
    sqlx::query(sql!("INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation) VALUES ($1, $2, $3, 1)"))
        .bind(sandbox).bind(SEED_USER_ID).bind(SEED_ORG_ID).execute(&mut conn).await?;
    sqlx::query(sql!("INSERT INTO credential_references (id, org_id, upstream_provider, upstream_provider_config) VALUES ($1, $2, 'aws_secrets_manager', '{}'::jsonb)"))
        .bind(reference).bind(SEED_ORG_ID).execute(&mut conn).await?;
    sqlx::query(sql!("INSERT INTO sandbox_credential_injection_rules (sandbox_id, version, rules) VALUES ($1, 7, '\\xff'::bytea)"))
        .bind(sandbox).execute(&mut conn).await?;
    drop(conn);
    run_bootstrap(&fixture).await?;
    let normalized: (bool, i64, i64) = sqlx::query_as(sql!(
        "SELECT to_regclass('sandbox_credential_references') IS NULL,
                (SELECT count(*) FROM sandbox_network_credential_injection_ruleset_version),
                (SELECT count(*) FROM sandbox_network_credential_injection_rules)"
    ))
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(normalized, (true, 0, 0));
    for table in [
        "credential_references",
        "sandbox_network_credential_injection_ruleset_version",
        "sandbox_network_credential_injection_rules",
        "sandbox_credential_injection_rules",
    ] {
        assert!(role_has_table_privilege(&fixture, "space_credmgr", table, "SELECT").await?);
        assert!(
            !role_has_table_privilege(&fixture, "space_credmgr", table, "INSERT,UPDATE,DELETE")
                .await?
        );
        assert!(
            role_has_table_privilege(&fixture, USER_ROLE, table, "INSERT,UPDATE,DELETE").await?
        );
    }
    let invalid_version = sqlx::query(sql!(
        "INSERT INTO sandbox_network_credential_injection_ruleset_version (sandbox_id, version) VALUES ($1, 0)"
    ))
    .bind(sandbox)
    .execute(&fixture.pool)
    .await
    .unwrap_err();
    assert_sqlstate(&invalid_version, "23514");
    sqlx::query(sql!(
        "INSERT INTO sandbox_network_credential_injection_ruleset_version (sandbox_id, version) VALUES ($1, 1)"
    ))
    .bind(sandbox)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!("INSERT INTO sandbox_network_credential_injection_rules (sandbox_id, rule_id, credential_reference_id, transformation_rule) VALUES ($1, 'one', $2, '{}'), ($1, 'two', $2, '{}')"))
        .bind(sandbox).bind(reference).execute(&fixture.pool).await?;
    for (statement, id) in [
        (
            "UPDATE credential_references SET state = state WHERE id = $1 RETURNING created_at < updated_at AND updated_at = NOW()",
            reference,
        ),
        (
            "UPDATE sandbox_network_credential_injection_ruleset_version SET version = version WHERE sandbox_id = $1 RETURNING created_at < updated_at AND updated_at = NOW()",
            sandbox,
        ),
        (
            "UPDATE sandbox_network_credential_injection_rules SET rule_id = rule_id WHERE sandbox_id = $1 RETURNING created_at < updated_at AND updated_at = NOW()",
            sandbox,
        ),
    ] {
        let timestamps: Vec<bool> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(statement)))
            .bind(id)
            .fetch_all(&fixture.pool)
            .await?;
        assert!(!timestamps.is_empty());
        assert!(timestamps.iter().all(|updated| *updated), "{statement}");
    }
    for (id, source, expected) in [
        ("one", reference, "23505"),
        ("missing", CredentialReferenceId::new().as_uuid(), "23503"),
    ] {
        let error = sqlx::query(sql!("INSERT INTO sandbox_network_credential_injection_rules (sandbox_id, rule_id, credential_reference_id, transformation_rule) VALUES ($1, $2, $3, '{}')"))
            .bind(sandbox).bind(id).bind(source).execute(&fixture.pool).await.unwrap_err();
        assert_sqlstate(&error, expected);
    }
    sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1"))
        .bind(sandbox)
        .execute(&fixture.pool)
        .await?;
    let remaining: (i64, i64, i64, i64) = sqlx::query_as(sql!(
        "SELECT (SELECT count(*) FROM sandbox_network_credential_injection_ruleset_version),
                (SELECT count(*) FROM sandbox_network_credential_injection_rules),
                (SELECT count(*) FROM sandbox_credential_injection_rules),
                (SELECT count(*) FROM credential_references)"
    ))
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(remaining, (0, 0, 0, 1));
    Ok(())
}

/// `space_ro` reads every `api_keys` column except `key_hash` and writes
/// nothing; `space_user` reads and writes the whole table. Privilege catalogs
/// alone do not prove what a role can actually query, so run the queries.
#[tokio::test]
async fn ro_reads_api_keys_without_key_hash() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };

    run_bootstrap(&fixture).await?;
    let mut conn = fixture.connect_as("postgres").await?;
    assume_migrator_role(&mut conn).await?;
    crate::POSTGRES_MIGRATOR.run(&mut conn).await?;
    drop(conn);
    run_bootstrap(&fixture).await?;

    let mut ro = fixture.connect_as(RO_ROLE).await?;
    sqlx::query(sql!(
        "SELECT COUNT(*) FROM (SELECT id, org_id, name, created_by, last_used_at, \
         created_at, updated_at, federated_owner_assertions_enabled, allowed_surfaces \
         FROM api_keys) visible"
    ))
    .fetch_one(&mut ro)
    .await?;
    for denied in [
        "SELECT key_hash FROM api_keys",
        "SELECT * FROM api_keys",
        "SELECT id FROM api_keys WHERE key_hash = 'x'",
        "UPDATE api_keys SET name = name",
    ] {
        let Err(error) = sqlx::query(sqlx::AssertSqlSafe(sql!(denied)))
            .execute(&mut ro)
            .await
        else {
            panic!("space_ro must be denied `{denied}`");
        };
        assert_sqlstate(&error, "42501");
    }

    let mut user = fixture.connect_as(USER_ROLE).await?;
    sqlx::query(sql!("SELECT key_hash FROM api_keys"))
        .fetch_all(&mut user)
        .await?;
    sqlx::query(sql!("UPDATE api_keys SET name = name"))
        .execute(&mut user)
        .await?;
    Ok(())
}
