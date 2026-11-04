use super::parse_mem_total_bytes;

#[test]
fn mem_total_parser_converts_kibibytes_to_bytes() {
    assert_eq!(
        parse_mem_total_bytes("MemFree: 10 kB\nMemTotal: 1008640 kB\n"),
        Some(1_032_847_360)
    );
    assert_eq!(parse_mem_total_bytes("MemTotal: 1008640 MB\n"), None);
    assert_eq!(parse_mem_total_bytes("MemFree: 10 kB\n"), None);
}
