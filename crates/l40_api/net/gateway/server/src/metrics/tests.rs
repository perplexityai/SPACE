use super::with_reserved_dns_hickory_metrics;
use pplx_space_metrics_emit::test_capture::capture_counters_guard;

#[test]
fn absolute_updates_are_adapted_to_increments_without_double_counting() {
    let capture = capture_counters_guard();
    with_reserved_dns_hickory_metrics(|| {
        let counter = metrics::counter!("hickory_resolver_cache_hit_total");
        counter.increment(3);
        counter.absolute(10);
        counter.absolute(8);
        counter.absolute(13);
    });
    let captured = capture.finish();

    assert_eq!(
        captured
            .iter()
            .map(|counter| counter.value)
            .collect::<Vec<_>>(),
        vec![3, 7, 3]
    );
    assert!(captured.iter().all(|counter| {
        counter.name == "gateway.reserved_dns.dns_cache" && counter.tags == ["outcome:hit"]
    }));
}
