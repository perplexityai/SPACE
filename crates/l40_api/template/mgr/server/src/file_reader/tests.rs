use super::*;
use std::io::{Seek, SeekFrom, Write};

#[test]
fn flag_selects_reader_and_preserves_logical_bytes() {
    for (flags, sparse) in [
        (FeatureFlagClient::disabled(), true),
        (FeatureFlagClient::fixed_bools([("unrelated", false)]), true),
        (
            FeatureFlagClient::from_provider(pplx_space_feature_flags::UnavailableProvider),
            true,
        ),
        (
            FeatureFlagClient::fixed_bools([(SPACE_TEMPLATE_SPARSE_AWARE_READS_FLAG.key(), false)]),
            false,
        ),
        (
            FeatureFlagClient::fixed_bools([(SPACE_TEMPLATE_SPARSE_AWARE_READS_FLAG.key(), true)]),
            true,
        ),
    ] {
        let mut file = tempfile::tempfile().unwrap();
        file.set_len(16384).unwrap();
        file.seek(SeekFrom::Start(8192)).unwrap();
        file.write_all(b"payload").unwrap();
        file.rewind().unwrap();
        let mut expected = Vec::new();
        file.read_to_end(&mut expected).unwrap();
        file.rewind().unwrap();
        let mut reader = TemplateReadMode::from_flags(&flags, "example-template")
            .reader(file)
            .unwrap();
        // Identical output alone cannot detect a kill switch that always uses
        // sparse reads, so also pin which concrete reader the assignment selects.
        assert_eq!(matches!(reader, TemplateFileReader::Sparse(_)), sparse);
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).unwrap();
        assert_eq!(actual, expected);
    }
}
