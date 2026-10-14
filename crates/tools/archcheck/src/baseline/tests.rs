use std::fs;

use super::*;
use crate::{config::ArchitectureConfig, rules};

/// `file_size`, two `pattern_ratchet` instances and `crate_names`, every
/// behaviour advisory: a table of every registered rule with a baseline.
fn config() -> ArchitectureConfig {
    let behaviours = |table: &str| -> String {
        [
            "no_crossing",
            "no_new_entries",
            "no_raised_caps",
            "listed_no_growth",
            "stale_entries",
        ]
        .iter()
        .map(|behaviour| format!("[rules.{table}.{behaviour}]\nmode = \"advisory\"\n"))
        .collect()
    };
    let instance = |name: &str| {
        format!(
            "[rules.pattern_ratchet.{name}]\npatterns = ['x']\nallow = []\n{}",
            behaviours(&format!("pattern_ratchet.{name}"))
        )
    };
    let crate_names: String = ["no_new", "stale_entries", "policy_changed"]
        .iter()
        .map(|behaviour| format!("[rules.crate_names.{behaviour}]\nmode = \"advisory\"\n"))
        .collect();
    let text = format!(
        "[rules.file_size]\nlimit = 5\n{}{}{}\
         [rules.crate_names]\nbanned_tokens = [\"common\"]\nallow = []\n{crate_names}",
        behaviours("file_size"),
        instance("alpha"),
        instance("beta"),
    );
    toml::from_str(&text).expect("the configuration parses")
}

/// A rule's lenient reading of a base copy that is missing, or keyed under
/// another name, would compare every entry of its tables as new.
#[test]
fn every_configured_table_is_read_from_a_base_copy() {
    let tables = rules::tables(&config().rules);
    for rule in rules::registered()
        .iter()
        .filter_map(|rule| rule.baseline_rule())
    {
        assert!(
            tables.iter().any(|table| rule_of(table) == rule),
            "configure {rule} in this test: {tables:?}"
        );
    }
    // Each table with an entry of its own.
    let text: String = tables
        .iter()
        .enumerate()
        .map(|(cap, table)| format!("[{table}]\n\"{table}.rs\" = {cap}\n"))
        .collect();
    let dir = tempfile::tempdir().expect("create a temporary directory");
    let path = dir.path().join("base.toml");
    fs::write(&path, text).expect("write the base copy");

    let base = Baseline::load_base(&path).expect("every registered rule's tables are read");
    for (cap, table) in tables.iter().enumerate() {
        // `[crate_names]` maps names to paths: only its names are read.
        if table == CrateNames::NAME {
            let names = BTreeSet::from([format!("{table}.rs")]);
            assert_eq!(base.crate_names().ok(), Some(Some(&names)), "{table}");
            continue;
        }
        let entries = base.table(table).ok();
        let expected = BTreeMap::from([(format!("{table}.rs"), cap)]);
        assert_eq!(entries, Some(&expected), "{table}");
    }
}
