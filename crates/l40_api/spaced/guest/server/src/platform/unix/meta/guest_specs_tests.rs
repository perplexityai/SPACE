use super::{parse_mem_total_bytes, parse_memory_used_bytes};

#[test]
fn mem_total_parser_converts_kibibytes_to_bytes() {
    assert_eq!(
        parse_mem_total_bytes("MemFree: 10 kB\nMemTotal: 1008640 kB\n"),
        Some(1_032_847_360)
    );
    assert_eq!(parse_mem_total_bytes("MemTotal: 1008640 MB\n"), None);
    assert_eq!(parse_mem_total_bytes("MemFree: 10 kB\n"), None);
}

#[test]
fn memory_used_is_total_minus_available_in_bytes() {
    let meminfo = "MemTotal: 1000 kB\nMemFree: 100 kB\nMemAvailable: 400 kB\n";
    assert_eq!(parse_memory_used_bytes(meminfo), Some(600 * 1024));
    assert_eq!(
        parse_memory_used_bytes("MemTotal: 1000 kB\nMemAvailable: 1000 kB\n"),
        Some(0),
        "nothing used is a measurement"
    );
    assert_eq!(
        parse_memory_used_bytes("MemTotal: 1000 kB\nMemAvailable: 0 kB\n"),
        Some(1000 * 1024)
    );
}

#[cfg(target_os = "linux")]
#[test]
fn deployed_source_reads_used_memory_from_proc_meminfo() {
    assert!(super::guest_memory_used_bytes().is_some());
}

#[test]
fn memory_used_is_absent_for_an_unusable_meminfo() {
    let overflowing = (u64::MAX / 1024) + 1;
    for meminfo in [
        String::new(),
        "MemAvailable: 400 kB\n".to_string(),
        "MemTotal: 1000 kB\n".to_string(),
        "MemTotal: 1000 kB\nMemAvailable: many kB\n".to_string(),
        "MemTotal: -1000 kB\nMemAvailable: 400 kB\n".to_string(),
        "MemTotal: 1000 kB\nMemAvailable: 400 MB\n".to_string(),
        "MemTotal: 1000\nMemAvailable: 400 kB\n".to_string(),
        format!("MemTotal: {overflowing} kB\nMemAvailable: 400 kB\n"),
        format!("MemTotal: 1000 kB\nMemAvailable: {overflowing} kB\n"),
        "MemTotal: 0 kB\nMemAvailable: 0 kB\n".to_string(),
        "MemTotal: 1000 kB\nMemAvailable: 1001 kB\n".to_string(),
    ] {
        assert_eq!(parse_memory_used_bytes(&meminfo), None, "{meminfo:?}");
    }
}
