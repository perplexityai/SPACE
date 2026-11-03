use pplx_space_common_core::types::SandboxLifecycle;

use std::collections::BTreeMap;
use std::time::Duration;

use super::{LifecycleAge, LifecycleCensus, census};
use crate::test_capture::{CapturedValue, capture_values};

fn transient() -> Vec<SandboxLifecycle> {
    SandboxLifecycle::all()
        .iter()
        .copied()
        .filter(|lifecycle| lifecycle.is_transient())
        .collect()
}

/// `rows` by `(lifecycle, age)` and `oldest_age_seconds` by lifecycle, each
/// asserted to be emitted exactly once.
type Emitted = (BTreeMap<(String, String), f64>, BTreeMap<String, f64>);

fn emitted(values: Vec<CapturedValue>) -> Emitted {
    let mut rows = BTreeMap::new();
    let mut oldest = BTreeMap::new();
    for value in values {
        let tag = |key: &str| {
            value
                .tags
                .iter()
                .find_map(|tag| tag.strip_prefix(&format!("{key}:")))
                .map(str::to_owned)
        };
        match value.name.as_str() {
            "sandbox.transient_lifecycle.rows" => {
                assert_eq!(value.tags.len(), 2, "{:?}", value.tags);
                let key = (
                    tag("lifecycle").expect("lifecycle tag"),
                    tag("age").expect("age tag"),
                );
                assert!(
                    rows.insert(key.clone(), value.value).is_none(),
                    "{key:?} emitted twice"
                );
            }
            "sandbox.transient_lifecycle.oldest_age_seconds" => {
                assert_eq!(value.tags.len(), 1, "{:?}", value.tags);
                let lifecycle = tag("lifecycle").expect("lifecycle tag");
                assert!(
                    oldest.insert(lifecycle.clone(), value.value).is_none(),
                    "{lifecycle} emitted twice"
                );
            }
            other => panic!("unexpected metric {other}"),
        }
    }
    (rows, oldest)
}

/// Every pair, as `(lifecycle, age)` tag values.
fn every_pair() -> Vec<(String, String)> {
    transient()
        .into_iter()
        .flat_map(|lifecycle| {
            LifecycleAge::ALL
                .into_iter()
                .map(move |age| (lifecycle.to_string(), age.to_string()))
        })
        .collect()
}

#[test]
fn an_empty_census_emits_every_pair_as_zero() {
    let (rows, oldest) = emitted(capture_values(|| census(&[])));

    assert_eq!(rows.keys().cloned().collect::<Vec<_>>(), {
        let mut pairs = every_pair();
        pairs.sort();
        pairs
    });
    assert!(rows.values().all(|value| *value == 0.0), "{rows:?}");
    let lifecycles: Vec<String> = transient().iter().map(ToString::to_string).collect();
    assert_eq!(oldest.len(), lifecycles.len());
    for lifecycle in &lifecycles {
        assert_eq!(oldest.get(lifecycle), Some(&0.0), "{lifecycle}");
    }
    assert!(lifecycles.contains(&"deleting".to_owned()));
}

#[test]
fn a_partial_census_still_emits_every_pair() {
    let restoring_rows = [0, 3, 0, 0, 0, 1];
    let short_rows = [2];
    let settled_rows = [9; 6];
    let entries = [
        LifecycleCensus {
            lifecycle: SandboxLifecycle::Restoring,
            rows_by_age: &restoring_rows,
            oldest_age: Some(Duration::from_secs(90_000)),
        },
        // Shorter than the bucket list: the rest read as zero.
        LifecycleCensus {
            lifecycle: SandboxLifecycle::Deleting,
            rows_by_age: &short_rows,
            oldest_age: Some(Duration::from_secs(5)),
        },
        // Not transient: ignored.
        LifecycleCensus {
            lifecycle: SandboxLifecycle::Started,
            rows_by_age: &settled_rows,
            oldest_age: Some(Duration::from_secs(1)),
        },
    ];

    let (rows, oldest) = emitted(capture_values(|| census(&entries)));

    let mut pairs = every_pair();
    pairs.sort();
    assert_eq!(rows.keys().cloned().collect::<Vec<_>>(), pairs);
    for ((lifecycle, age), value) in &rows {
        let expected = match (lifecycle.as_str(), age.as_str()) {
            ("restoring", "1m_10m") => 3.0,
            ("restoring", "ge_1d") => 1.0,
            ("deleting", "lt_1m") => 2.0,
            _ => 0.0,
        };
        assert_eq!(*value, expected, "{lifecycle}/{age}");
    }
    assert_eq!(oldest.get("restoring"), Some(&90_000.0));
    assert_eq!(oldest.get("deleting"), Some(&5.0));
    assert_eq!(oldest.get("creating"), Some(&0.0));
    assert!(!oldest.contains_key("started"));
}

#[test]
fn buckets_and_bounds_agree() {
    assert_eq!(
        LifecycleAge::LOWER_BOUNDS.len() + 1,
        LifecycleAge::ALL.len()
    );
    assert!(
        LifecycleAge::LOWER_BOUNDS
            .windows(2)
            .all(|pair| pair[0] < pair[1])
    );
    let tags: Vec<&str> = LifecycleAge::ALL.iter().map(|age| age.as_str()).collect();
    assert_eq!(
        tags,
        ["lt_1m", "1m_10m", "10m_1h", "1h_6h", "6h_1d", "ge_1d"]
    );
}
