use super::*;

#[test]
fn cpu_offsets_handle_process_names_containing_parentheses() -> Result<()> {
    let stat = "42 (postgres ) worker) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14";
    assert_eq!(parse_ticks(stat)?, (23, 27));
    parse_ticks("42 (postgres) S 1").unwrap_err();
    Ok(())
}

#[test]
fn transaction_timings_use_only_committed_transaction_events() -> Result<()> {
    let log = br#"{"target":"unrelated","transaction_open_us":100000}
{"target":"space_publish_timing","transaction_open_us":2500}
"#;
    assert_eq!(transaction_timings(log)?, vec![2.5]);
    transaction_timings(br#"{"target":"space_publish_timing"}"#).unwrap_err();
    Ok(())
}

#[test]
fn latency_percentiles_use_nearest_rank() -> Result<()> {
    assert_eq!(percentile(&[9.0, 1.0, 5.0, 2.0], 50)?, 2.0);
    assert_eq!(percentile(&[9.0, 1.0, 5.0, 2.0], 99)?, 9.0);
    percentile(&[], 50).unwrap_err();
    Ok(())
}
