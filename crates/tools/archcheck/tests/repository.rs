//! This repository's own `ARCHITECTURE.toml` and baseline stay loadable, and
//! no required-mode rule fails on the tree as committed.

use std::path::Path;

use pplx_space_archcheck::{Context, check};

#[test]
fn repository_config_and_baseline_pass_every_required_rule() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let ctx = Context::load(&root).unwrap();
    assert!(!ctx.files.is_empty());

    let required: Vec<_> = check(&ctx)
        .into_iter()
        .filter(|finding| finding.mode.is_required())
        .collect();
    assert!(required.is_empty(), "{required:#?}");
}

/// Repository-relative path of the one crate the `sbxmgr_sqlite` workspace
/// below has: inside the instance's `paths`, outside its `allow`.
const SBXMGR_SRC: &str = "crates/libs/sbx/mgr/server/src";

/// The database migrator's sources, outside `sbxmgr_sqlite_schema`'s `allow`.
const MIGRATOR_SRC: &str = "crates/libs/db/migrator/src";

/// [`ratchet_findings`] for `sbxmgr_sqlite` and a crate at [`SBXMGR_SRC`].
fn sbxmgr_sqlite_findings(
    entries: &[(&str, usize)],
    files: &[(&str, &str)],
) -> Vec<(&'static str, String)> {
    ratchet_findings("sbxmgr_sqlite", SBXMGR_SRC, entries, files)
}

/// A workspace with one crate whose sources are at `src`, checked by the
/// repository's own `[rules.pattern_ratchet.<instance>]`, its
/// `[rules.file_size]` and `[rules.crate_names]`, `entries` as the instance's
/// baseline, and `files` under `src`. Returns the instance's findings as
/// `(behaviour, message)`.
fn ratchet_findings(
    instance: &str,
    src: &str,
    entries: &[(&str, usize)],
    files: &[(&str, &str)],
) -> Vec<(&'static str, String)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let text = std::fs::read_to_string(root.join("ARCHITECTURE.toml"))
        .expect("read the repository's ARCHITECTURE.toml");
    let mut config: toml::Table = toml::from_str(&text).expect("ARCHITECTURE.toml is TOML");
    config
        .get_mut("rules")
        .and_then(|rules| rules.get_mut("pattern_ratchet"))
        .and_then(toml::Value::as_table_mut)
        .expect("ARCHITECTURE.toml has [rules.pattern_ratchet]")
        .retain(|name, _| name == instance);
    // The fences name members this one-crate workspace does not have.
    config
        .get_mut("rules")
        .and_then(toml::Value::as_table_mut)
        .expect("ARCHITECTURE.toml has [rules]")
        .remove("dependency_fence");

    let dir = tempfile::tempdir().expect("create a temporary workspace");
    let member = src.trim_end_matches("/src");
    let mut baseline = format!("[file_size]\n[crate_names]\n[pattern_ratchet.{instance}]\n");
    for (path, cap) in entries {
        baseline.push_str(&format!("\"{src}/{path}\" = {cap}\n"));
    }
    let mut written = vec![
        (
            "Cargo.toml".to_owned(),
            format!("[workspace]\nmembers = [\"{member}\"]\n"),
        ),
        (
            format!("{member}/Cargo.toml"),
            "[package]\nname = \"sbx\"\n[lib]\npath = \"src/lib.rs\"\n".to_owned(),
        ),
        (
            "ARCHITECTURE.toml".to_owned(),
            toml::to_string(&config).expect("serialize the configuration"),
        ),
        ("architecture-baseline.toml".to_owned(), baseline),
    ];
    written.extend(
        files
            .iter()
            .map(|(path, text)| (format!("{src}/{path}"), (*text).to_owned())),
    );
    for (path, text) in written {
        let path = dir.path().join(path);
        std::fs::create_dir_all(path.parent().expect("a parent directory")).expect("create it");
        std::fs::write(path, text).expect("write a workspace file");
    }
    let ctx = Context::load(dir.path()).expect("the workspace loads");
    check(&ctx)
        .into_iter()
        .filter(|finding| finding.instance.as_deref() == Some(instance))
        .map(|finding| (finding.behaviour, finding.message()))
        .collect()
}

/// Every way sbx-mgr code can reach its SQLite database outside the store is
/// a matching line, and ordinary code that shares a word with one is not.
#[test]
fn sbxmgr_sqlite_patterns_match_database_access_only() {
    let crossings = [
        "use rusqlite::Connection;",
        "use ::rusqlite::Connection;",
        "use rusqlite as sqlite;",
        "extern crate rusqlite;",
        "let conn = Connection::open(path)?;",
        "let conn = Connection::open_in_memory()?;",
        "let tx = Transaction::new_unchecked(&conn, behavior)?;",
        "let conn = pplx_space_common_svc::sql::sqlite::open_rusqlite(path, options)?;",
        "use pplx_space_common_jobs::db as jobs;",
        "use pplx_space_common_jobs::{JobStatus, db as jobs};",
        "let bound = params![id];",
        "let bound = named_params! { \":id\": id };",
        "conn.execute(sql, bound)?;",
        "let n: i64 = conn.query_one(sql, [], |row| row.get(0))?;",
        "conn.query_row_and_then(sql, [], decode)?;",
        "let mut rows = statement.query([id])?;",
        "let present = statement.exists([id])?;",
        "let tx = conn.unchecked_transaction()?;",
        "let tx = crate::db::immediate_transaction(&conn)?;",
        "let sql = \"SELECT state FROM sandboxes WHERE id = ?1\";",
        "let sql = \"UPDATE sandboxes",
        "    DELETE FROM sandboxes WHERE id = ?1",
    ];
    for line in crossings {
        let found = sbxmgr_sqlite_findings(&[], &[("lib.rs", &format!("{line}\n"))]);
        assert_eq!(found.len(), 1, "{line}: {found:#?}");
        assert_eq!(found[0].0, "no_crossing", "{line}");
    }

    let ordinary = [
        "use pplx_space_common_jobs::{JobPriority, JobStatus};",
        "let db = crate::db::SharedConnection::new(conn);",
        "if path.exists() {",
        "let query = uri.query();",
        "let worker = executor.execute_one(job);",
        "let method = \"DELETE\";",
        "let message = format!(\"select a sandbox: {id}\");",
        "tracing::warn!(%error, \"update failed\");",
    ];
    let text: String = ordinary.iter().map(|line| format!("{line}\n")).collect();
    let found = sbxmgr_sqlite_findings(&[], &[("lib.rs", &text)]);
    assert!(found.is_empty(), "{found:#?}");
}

/// A listed file grows when it opens a connection through a type it already
/// imported, as does a child module through its parent's import.
#[test]
fn sbxmgr_sqlite_counts_opens_through_imported_names() {
    let found = sbxmgr_sqlite_findings(
        &[("lib.rs", 1)],
        &[(
            "lib.rs",
            "use rusqlite::Connection;\nfn open() { let conn = Connection::open_in_memory(); }\n",
        )],
    );
    assert_eq!(found.len(), 1, "{found:#?}");
    assert_eq!(found[0].0, "listed_no_growth");
    assert!(found[0].1.contains("grew from 1 to 2"), "{}", found[0].1);

    let found = sbxmgr_sqlite_findings(
        &[("parent.rs", 1)],
        &[
            ("parent.rs", "use rusqlite::Connection;\nmod child;\n"),
            (
                "parent/child.rs",
                "use super::Connection;\nfn open(path: &str) { let conn = Connection::open(path); }\n",
            ),
        ],
    );
    assert_eq!(found.len(), 1, "{found:#?}");
    assert_eq!(found[0].0, "no_crossing");
    assert!(
        found[0]
            .1
            .starts_with(&format!("{SBXMGR_SRC}/parent/child.rs: 1 code line")),
        "{}",
        found[0].1
    );
}

/// Outside the store, the migrator may not name sbx-mgr's database: its
/// migrations, its path or file, or its own default-path helper. The other
/// services' databases it migrates are not matches.
#[test]
fn sbxmgr_sqlite_schema_matches_the_migrators_sandbox_manager_database() {
    let crossings = [
        "use pplx_space_sbxmgr_migrations::SANDBOX_MANAGER_MIGRATOR;",
        "default_path: paths::SANDBOX_MANAGER_DB,",
        "Database::SandboxManager(args) => run_sqlite(sqlite_sandbox_manager(), args).await,",
        "fn sqlite_sandbox_manager() -> SqliteDatabase {",
        "default_path: \"/space/run/sandbox-manager/sbx-manager.db\",",
        "static MIGRATOR: Migrator = sqlx::migrate!(\"../../../../db/sandbox-manager\");",
    ];
    for line in crossings {
        let found = ratchet_findings(
            "sbxmgr_sqlite_schema",
            MIGRATOR_SRC,
            &[],
            &[("lib.rs", &format!("{line}\n"))],
        );
        assert_eq!(found.len(), 1, "{line}: {found:#?}");
        assert_eq!(found[0].0, "no_crossing", "{line}");
    }

    let ordinary = [
        "Database::TemplateManager(args) => run_sqlite(sqlite_template_manager(), args).await,",
        "default_path: paths::TEMPLATE_MANAGER_DB,",
        "default_path: \"/space/run/template-manager/template-manager.db\",",
        "static MIGRATOR: Migrator = sqlx::migrate!(\"../../../../db/template-manager\");",
        "SandboxManager(SqliteArgs),",
    ];
    let text: String = ordinary.iter().map(|line| format!("{line}\n")).collect();
    let found = ratchet_findings(
        "sbxmgr_sqlite_schema",
        MIGRATOR_SRC,
        &[],
        &[("lib.rs", &text)],
    );
    assert!(found.is_empty(), "{found:#?}");
}
