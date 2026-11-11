use super::*;
use crate::types::Digest;

fn hash() -> String {
    Digest::blake3([7; 32]).to_string()
}

fn environment(byte: u8) -> EnvironmentReference {
    EnvironmentReference::new(Digest::sha256([byte; 32]))
}

#[test]
fn initial_and_unbound_builds_share_the_legacy_partition() {
    let initial = environment(1);
    assert_eq!(
        TemplateCachePartition::for_environment(None, Some(initial)),
        TemplateCachePartition::Legacy
    );
    assert_eq!(
        TemplateCachePartition::for_environment(Some(initial), Some(initial)),
        TemplateCachePartition::Legacy
    );
}

#[test]
fn other_environments_get_their_own_partition() {
    let partition =
        TemplateCachePartition::for_environment(Some(environment(2)), Some(environment(1)));
    assert_eq!(
        partition,
        TemplateCachePartition::Environment(environment(2))
    );
    // Without a known initial environment nothing is mistaken for it.
    assert_eq!(
        TemplateCachePartition::for_environment(Some(environment(1)), None),
        TemplateCachePartition::Environment(environment(1))
    );
}

#[test]
fn legacy_directory_and_key_are_unchanged() {
    let hash = hash();
    assert_eq!(TemplateCachePartition::Legacy.build_dir_name(&hash), hash);
    assert_eq!(TemplateCachePartition::Legacy.key(), "");
    assert_eq!(
        TemplateCachePartition::from_key("").unwrap(),
        TemplateCachePartition::Legacy
    );
}

#[test]
fn partitioned_directory_and_key_round_trip() {
    let hash = hash();
    let partition = TemplateCachePartition::Environment(environment(2));
    let dir = partition.build_dir_name(&hash);
    assert_eq!(dir, format!("{hash}@{}", environment(2)));
    assert_eq!(
        TemplateCachePartition::parse_build_dir_name(&dir),
        Some((hash.as_str(), partition))
    );
    assert_eq!(
        TemplateCachePartition::from_key(&partition.key()).unwrap(),
        partition
    );
}

#[test]
fn non_build_directory_names_are_rejected() {
    let hash = hash();
    for name in [
        "snapshots".to_string(),
        format!("{hash}.importing"),
        format!("{hash}@not-a-digest"),
        format!("{hash}@{}.replaced.x", environment(2)),
        format!("not-a-digest@{}", environment(2)),
    ] {
        assert_eq!(
            TemplateCachePartition::parse_build_dir_name(&name),
            None,
            "{name}"
        );
    }
    TemplateCachePartition::from_key("not-a-digest").unwrap_err();
}
