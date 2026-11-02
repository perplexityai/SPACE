use std::fs;

use super::*;
use crate::{config::ArchitectureConfig, rules};

/// Two `pattern_ratchet` instances, `crate_names` and `layers` with its
/// state checks, every behaviour advisory: a table of every registered rule
/// with a baseline.
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
    let layers: String = [
        "placement",
        "layout",
        "direction.no_crossing",
        "direction.no_new_entries",
        "direction.stale_entries",
        "dev_direction.no_crossing",
        "dev_direction.no_new_entries",
        "dev_direction.stale_entries",
        "storage_access.no_crossing",
        "storage_access.no_new_entries",
        "storage_access.stale_entries",
        "codec_features.no_crossing",
        "codec_features.no_new_entries",
        "codec_features.stale_entries",
        "domain_dependencies.no_crossing",
        "domain_dependencies.no_new_entries",
        "domain_dependencies.stale_entries",
        "model_protocol.no_crossing",
        "model_protocol.no_new_entries",
        "model_protocol.stale_entries",
        "test_only",
        "clients",
        "store_edges",
        "stale_entries",
        "policy_changed",
        "state.owners",
        "state.declarations",
        "state.writes",
        "state.pending",
        "unowned.no_crossing",
        "unowned.no_new_entries",
        "unowned.stale_entries",
    ]
    .iter()
    .map(|behaviour| format!("[rules.layers.{behaviour}]\nmode = \"advisory\"\n"))
    .chain(["[rules.layers.storage]\ndrivers = [\"sqlx\"]\n".to_owned()])
    .collect();
    let text = format!(
        "{}{}\
         [rules.crate_names]\nbanned_tokens = [\"common\"]\nallow = []\n{crate_names}{layers}",
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
    // Each table with an entry of its own; a `[layers.<check>]` entry is an
    // edge.
    let text: String = tables
        .iter()
        .enumerate()
        .map(|(cap, table)| match layers_check(table) {
            Some(_) => format!("[{table}]\nfrom = [\"{table}\"]\n"),
            None => format!("[{table}]\n\"{table}.rs\" = {cap}\n"),
        })
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
        if let Some(check) = layers_check(table) {
            let edges = BTreeMap::from([("from".to_owned(), BTreeSet::from([table.clone()]))]);
            let found = match base.layers_table(check) {
                Ok(BaseLayersTable::Table(found)) => Some(found),
                _ => None,
            };
            assert_eq!(found, Some(&edges), "{table}");
            continue;
        }
        let entries = base.table(table).ok();
        let expected = BTreeMap::from([(format!("{table}.rs"), cap)]);
        assert_eq!(entries, Some(&expected), "{table}");
    }
}

/// [`Baseline::write`] with and without a table of caps and `[layers.<check>]`
/// tables: the header first, then the caps, the layers comment and the
/// crate_names comment in that order, each part after exactly one blank line,
/// and one newline at the end.
#[test]
fn write_puts_one_blank_line_between_parts() {
    for (caps, layers) in [(false, false), (true, false), (false, true), (true, true)] {
        let shape = format!("caps: {caps}, layers: {layers}");
        let mut baseline = Baseline::default();
        if caps {
            baseline.insert_table(
                "pattern_ratchet.alpha".to_owned(),
                BTreeMap::from([("crates/a/src/x.rs".to_owned(), 2)]),
            );
        }
        if layers {
            let mut table = EdgeTable::new();
            table.insert("pplx-space-a", "pplx-space-b");
            baseline.layers.insert("direction".to_owned(), table);
        }
        baseline
            .crate_names
            .insert("pplx-space-common-io".to_owned(), "crates/x".to_owned());
        let dir = tempfile::tempdir().expect("create a temporary directory");
        let path = dir.path().join(BASELINE_FILE);

        baseline.write(&path).expect("write the baseline");

        let text = fs::read_to_string(&path).expect("read the written baseline");
        assert!(text.starts_with(HEADER), "{shape}:\n{text}");
        assert!(!text.contains("\n\n\n"), "{shape}:\n{text}");
        assert!(
            text.ends_with('\n') && !text.ends_with("\n\n"),
            "{shape}:\n{text}"
        );
        let parts: Vec<&str> = [
            caps.then_some("[pattern_ratchet.alpha]\n"),
            layers.then_some(LAYERS_COMMENT),
            Some(CRATE_NAMES_COMMENT),
        ]
        .into_iter()
        .flatten()
        .collect();
        let starts: Vec<usize> = parts
            .iter()
            .map(|part| {
                text.find(&format!("\n\n{part}"))
                    .unwrap_or_else(|| panic!("{shape}: no blank line before {part:?}:\n{text}"))
            })
            .collect();
        assert!(starts.is_sorted(), "{shape}:\n{text}");
    }
}
