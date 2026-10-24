use super::*;
use crate::proto::ReconciliationStatus;

fn node_ctl(reconciliations: &[(&str, u64)]) -> NodeCtlStatus {
    NodeCtlStatus {
        reachable: true,
        error: None,
        reconciliations: reconciliations
            .iter()
            .map(|&(name, poll_interval_secs)| ReconciliationStatus {
                name: name.to_string(),
                poll_interval_secs,
                ..Default::default()
            })
            .collect(),
        sync_overrides: Vec::new(),
    }
}

#[test]
fn default_window_is_two_ticks_plus_slack() {
    assert_eq!(DEFAULT_NODE_STATUS_FRESHNESS, Duration::from_secs(150));
}

#[test]
fn window_scales_with_the_interval_up_to_the_cap() {
    assert_eq!(
        node_status_freshness(Duration::from_secs(10)),
        Duration::from_secs(50)
    );
    assert_eq!(MAX_NODE_CTL_POLL_INTERVAL, Duration::from_secs(285));
    assert_eq!(
        node_status_freshness(MAX_NODE_CTL_POLL_INTERVAL),
        MAX_NODE_STATUS_FRESHNESS
    );
    assert_eq!(
        node_status_freshness(Duration::from_secs(300)),
        MAX_NODE_STATUS_FRESHNESS
    );
    assert_eq!(
        node_status_freshness(Duration::MAX),
        MAX_NODE_STATUS_FRESHNESS
    );
}

#[test]
fn reads_the_node_status_reconciler_interval() {
    let status = node_ctl(&[("network-policy", 5), (NODE_STATUS_OUTBOUND_RECONCILER, 20)]);
    assert_eq!(
        reported_status_poll_interval(&status),
        Some(Duration::from_secs(20))
    );
}

#[test]
fn publication_window_follows_the_reported_interval() {
    let reporting = node_ctl(&[(NODE_STATUS_OUTBOUND_RECONCILER, 20)]);
    assert_eq!(
        reported_freshness(Some(&reporting)),
        Some(Duration::from_secs(70))
    );
    assert_eq!(reported_freshness(Some(&node_ctl(&[]))), None);
    assert_eq!(reported_freshness(None), None);
}

#[test]
fn a_report_without_a_usable_interval_reads_as_none() {
    assert_eq!(reported_status_poll_interval(&node_ctl(&[])), None);
    assert_eq!(
        reported_status_poll_interval(&node_ctl(&[("network-policy", 20)])),
        None
    );
    assert_eq!(
        reported_status_poll_interval(&node_ctl(&[(NODE_STATUS_OUTBOUND_RECONCILER, 0)])),
        None
    );
}
