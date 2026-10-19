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
    use pplx_space_archcheck::context::{ProductionFile, count_code_lines};
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let mut ctx = Context::load(&root).unwrap();
    let cases = [
        (
            "crates/libs/node/mgr/server/src/spawn_regression.rs",
            "std::process::Command::new(\"tool\");",
            true,
        ),
        (
            "crates/libs/node/mgr/server/src/spawn_regression.rs",
            "use std::process::{Command as Process, Output};",
            true,
        ),
        (
            "crates/libs/node/mgr/server/src/spawn_regression.rs",
            "use std::process as child;",
            true,
        ),
        (
            "crates/libs/node/mgr/server/src/spawn_regression.rs",
            "use std::process::{\n Command,\n};",
            true,
        ),
        (
            "crates/apps/node/mgr/svc/src/spawn_regression.rs",
            "use std::process::Command;",
            true,
        ),
        (
            "crates/libs/node/mgr/server/src/spawn_regression.rs",
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
            code_lines: count_code_lines(text),
        });
        let found = check(&ctx).iter().any(|finding| {
            finding.path == path
                && finding.code() == "pattern_ratchet.node_manager_commands.no_crossing"
        });
        assert_eq!(found, rejected, "{path}: {text}");
        ctx.files.pop();
    }
    let owner = "crates/libs/node/mgr/server/src/host_command/spawn.rs";
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
