/// A template without a dir has no cached partitions, but a template dir that
/// can't be listed is an error, never read as none.
#[test]
fn cached_template_partitions_report_a_dir_they_cannot_list() {
    use pplx_space_common_core::types::TemplateCachePartition;
    use pplx_space_content_digest::Digest;
    use pplx_space_execution_spec::EnvironmentReference;
    let root = tempfile::tempdir().unwrap();
    let hash = format!("sha256:{}", "b".repeat(64));
    assert!(
        super::cached_template_partitions_in(root.path(), "absent", &hash)
            .unwrap()
            .is_empty()
    );

    let twin =
        TemplateCachePartition::Environment(EnvironmentReference::new(Digest::sha256([2; 32])));
    for partition in [twin, TemplateCachePartition::Legacy] {
        std::fs::create_dir_all(
            root.path()
                .join("seed")
                .join(partition.build_dir_name(&hash)),
        )
        .unwrap();
    }
    assert_eq!(
        super::cached_template_partitions_in(root.path(), "seed", &hash).unwrap(),
        [TemplateCachePartition::Legacy, twin]
    );

    std::fs::write(root.path().join("unlistable"), b"not a dir").unwrap();
    super::cached_template_partitions_in(root.path(), "unlistable", &hash).unwrap_err();
}
