use super::*;

fn usage(unallocated: u64, data_total: u64, data_used: u64) -> FilesystemUsage {
    FilesystemUsage {
        device_unallocated_bytes: unallocated,
        metadata_total_bytes: 8 * GIB,
        metadata_used_bytes: 6 * GIB,
        data_total_bytes: data_total,
        data_used_bytes: data_used,
    }
}

#[test]
fn healthy_when_unallocated_at_or_above_low_watermark() {
    assert_eq!(
        evaluate(usage(UNALLOCATED_LOW_BYTES, 1000 * GIB, 100 * GIB)),
        BalanceDecision::Healthy
    );
    assert_eq!(
        evaluate(usage(2000 * GIB, 1000 * GIB, 999 * GIB)),
        BalanceDecision::Healthy
    );
}

#[test]
fn balance_needed_when_low_and_slack_reclaimable() {
    // A nearly fully allocated filesystem can still have reclaimable data slack.
    assert_eq!(
        evaluate(usage(4 << 20, 4000 * GIB, 3000 * GIB)),
        BalanceDecision::BalanceNeeded
    );
    // Slack exactly at the minimum still qualifies.
    assert_eq!(
        evaluate(usage(
            UNALLOCATED_LOW_BYTES - 1,
            1000 * GIB,
            1000 * GIB - MIN_DATA_SLACK_BYTES
        )),
        BalanceDecision::BalanceNeeded
    );
}

#[test]
fn insufficient_slack_when_low_and_data_chunks_packed() {
    // The wedge shape balance cannot fix: Data Size ~= Used.
    assert_eq!(
        evaluate(usage(4 << 20, 7000 * GIB, 7000 * GIB - GIB)),
        BalanceDecision::InsufficientSlack
    );
}
