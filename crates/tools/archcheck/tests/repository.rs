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

#[test]
fn node_manager_commands_reject_direct_spawns_without_restricting_other_services() {
    use pplx_space_archcheck::context::ProductionFile;
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let mut ctx = Context::load(&root).unwrap();
    // Each check below reads one instance's findings; the other instances and
    // the layers rule would rescan the whole repository for every case.
    ctx.config
        .rules
        .pattern_ratchet
        .retain(|name, _| name.as_str() == "node_manager_commands");
    assert_eq!(ctx.config.rules.pattern_ratchet.len(), 1);
    ctx.config.rules.layers = None;
    let cases = [
        (
            "crates/l40_api/node/mgr/server/src/spawn_regression.rs",
            "std::process::Command::new(\"tool\");",
            true,
        ),
        (
            "crates/l40_api/node/mgr/server/src/spawn_regression.rs",
            "use std::process::{Command as Process, Output};",
            true,
        ),
        (
            "crates/l40_api/node/mgr/server/src/spawn_regression.rs",
            "use std::process as child;",
            true,
        ),
        (
            "crates/l40_api/node/mgr/server/src/spawn_regression.rs",
            "use std::process::{\n Command,\n};",
            true,
        ),
        (
            "crates/l50_app/node/mgr/svc/src/spawn_regression.rs",
            "use std::process::Command;",
            true,
        ),
        (
            "crates/l40_api/node/mgr/server/src/spawn_regression.rs",
            "use crate::host_command::Command;\nCommand::new(\"tool\");",
            false,
        ),
        (
            "crates/libs/other_service/src/spawn_regression.rs",
            "std::process::Command::new(\"tool\");",
            false,
        ),
    ];
    for (path, text, rejected) in cases {
        ctx.files.push(ProductionFile {
            path: path.to_owned(),
            text: text.to_owned(),
        });
        let found = check(&ctx).iter().any(|finding| {
            finding.path == path
                && finding.code() == "pattern_ratchet.node_manager_commands.no_crossing"
        });
        assert_eq!(found, rejected, "{path}: {text}");
        ctx.files.pop();
    }
    let owner = "crates/l40_api/node/mgr/server/src/host_command/spawn.rs";
    let file = ctx
        .files
        .iter_mut()
        .find(|file| file.path == owner)
        .unwrap();
    file.text
        .push_str("\nstd::process::Command::new(\"another-tool\");\n");
    assert!(check(&ctx).iter().any(|finding| finding.path == owner
        && finding.code() == "pattern_ratchet.node_manager_commands.listed_no_growth"));
}

/// `sql_wrapped_column_predicate` counts a string function around a probed
/// column in a predicate, on the line that compares it or ends before the
/// comparison, and not the same call where nothing compares it.
#[test]
fn sql_wrapped_column_predicate_flags_wrapped_probes_not_projections() {
    use pplx_space_archcheck::context::ProductionFile;
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let mut ctx = Context::load(&root).unwrap();
    ctx.config
        .rules
        .pattern_ratchet
        .retain(|name, _| name.as_str() == "sql_wrapped_column_predicate");
    assert_eq!(ctx.config.rules.pattern_ratchet.len(), 1);
    ctx.config.rules.layers = None;
    // Only the case's own file: the instance's patterns over every production
    // file, once per case, would take seconds. Its baselined files then read
    // as stale, which the check below doesn't look at.
    ctx.files.clear();
    let path = "crates/l30_storage/template/release/store/src/probe_regression.rs";
    let cases = [
        // The comparison on the next line, as a statement spells a join.
        (
            "\"SELECT 1 FROM t o JOIN r ON o.k = r.k\n AND replace(o.hash, 'sha256:', '')\n = replace(r.hash, 'sha256:', '')\"",
            true,
        ),
        ("\"SELECT 1 FROM t o WHERE lower(o.name) = $1\"", true),
        (
            "\"SELECT 1 FROM t o WHERE o.a = 1 OR replace(o.hash, $1, $2) = $3\"",
            true,
        ),
        (
            "\"SELECT 1 FROM t o WHERE o.a = 1 AND\n    replace(o.hash, $1, $2) IN ($3, $4)\"",
            true,
        ),
        (
            "\"SELECT DISTINCT ON (lower(t.name)) t.name FROM templates t ORDER BY lower(t.name)\"",
            false,
        ),
        (
            "\"SELECT 1 FROM t o WHERE o.hash IN (replace($1, 'sha256:', ''), 'sha256:' || replace($1, 'sha256:', ''))\"",
            false,
        ),
        (
            "\"SELECT CASE WHEN starts_with(op.id, $1)\n AND substr(op.id, length($1) + 1) LIKE $2 THEN 1 END FROM op\"",
            false,
        ),
    ];
    for (text, rejected) in cases {
        ctx.files.push(ProductionFile {
            path: path.to_owned(),
            text: text.to_owned(),
        });
        let found = check(&ctx).iter().any(|finding| {
            finding.path == path
                && finding.code() == "pattern_ratchet.sql_wrapped_column_predicate.no_crossing"
        });
        assert_eq!(found, rejected, "{text}");
        ctx.files.pop();
    }
}

/// The database migrator's sources, outside `sbxmgr_sqlite_schema`'s `allow`.
const MIGRATOR_SRC: &str = "crates/l30_storage/db/migrate/src";

/// A workspace with one crate whose sources are at `src`, checked by the
/// repository's own `[rules.pattern_ratchet.<instance>]` and
/// `[rules.crate_names]`, `entries` as the instance's
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
    // The state checks' shared catalogs are directories it does not have.
    if let Some(layers) = config
        .get_mut("rules")
        .and_then(|rules| rules.get_mut("layers"))
        .and_then(toml::Value::as_table_mut)
    {
        layers.remove("state");
        layers.remove("unowned");
    }

    let dir = tempfile::tempdir().expect("create a temporary workspace");
    let member = src.trim_end_matches("/src");
    let mut baseline = format!("[crate_names]\n[pattern_ratchet.{instance}]\n");
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

/// Outside the store, the migrator may not name sbx-mgr's database: its
/// migrations, its path or file, or its own default-path helper. The other
/// services' databases it migrates are not matches.
#[test]
fn sbxmgr_sqlite_schema_matches_the_migrators_sandbox_manager_database() {
    let crossings = [
        "use pplx_space_sbx_mgr_migrations::SANDBOX_MANAGER_MIGRATOR;",
        // The migrations crate's name alone, so that its pattern is what matches.
        "use pplx_space_sbx_mgr_migrations as migrations;",
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
