use super::*;

fn iteration(wall_us: u64, cpu_ns: u64, bytes: u64) -> Iteration {
    Iteration {
        wall_us,
        gateway_cpu_ns: cpu_ns,
        gateway_rss_peak_bytes: 8 * 1024 * 1024,
        bytes_to_upstream: bytes,
        bytes_from_upstream: 0,
        connections: 1,
        operations: 4,
        operation_p50_us: Some(10),
        operation_p99_us: Some(90),
    }
}

/// Derived rates are floats; compare them within a tolerance rather than by
/// equality.
fn approx(actual: Option<f64>, expected: f64) -> bool {
    actual.is_some_and(|value| (value - expected).abs() < 1e-6)
}

fn workload_run(iterations: Vec<Iteration>) -> WorkloadRun {
    WorkloadRun {
        name: "bulk-upload".to_string(),
        summary: "test".to_string(),
        error: None,
        iterations,
    }
}

#[test]
fn percentile_uses_nearest_rank_and_clamps_to_the_ends() {
    let values = [10, 20, 30, 40];
    assert_eq!(percentile(&values, 50), Some(20));
    assert_eq!(percentile(&values, 99), Some(40));
    assert_eq!(percentile(&values, 100), Some(40));
    // A zero percentile must still name a real sample rather than index -1.
    assert_eq!(percentile(&values, 0), Some(10));
    assert_eq!(percentile(&[], 50), None);
}

#[test]
fn throughput_and_cpu_cost_come_from_the_median_iteration() {
    // Two iterations: 1 s carrying 1 MiB with 500 ms of gateway CPU, and a
    // slower outlier. The median is the first.
    let summary = summarize(&workload_run(vec![
        iteration(1_000_000, 500_000_000, 1024 * 1024),
        iteration(4_000_000, 900_000_000, 1024 * 1024),
    ]));
    assert_eq!(summary.iterations, 2);
    assert_eq!(summary.wall_p50_us, 1_000_000);
    assert!(approx(summary.throughput_mib_s, 1.0));
    assert!(approx(summary.cpu_ms_per_iteration, 500.0));
    assert!(approx(summary.cpu_us_per_mib, 500_000.0));
    assert!(approx(summary.cpu_us_per_operation, 125_000.0));
}

/// A workload that moves no bytes — `idle`, `denied-destination` — must not
/// report a throughput at all. An infinity or a zero would both read as data.
#[test]
fn a_workload_that_moves_no_bytes_has_no_throughput_or_per_byte_cost() {
    let mut empty = iteration(1_000_000, 1_000_000, 0);
    empty.operations = 0;
    let summary = summarize(&workload_run(vec![empty]));
    assert_eq!(summary.throughput_mib_s, None);
    assert_eq!(summary.cpu_us_per_mib, None);
    assert_eq!(summary.cpu_us_per_operation, None);
    assert!(approx(summary.cpu_ms_per_iteration, 1.0));
}

#[test]
fn a_workload_with_no_iterations_summarizes_without_panicking() {
    let summary = summarize(&workload_run(Vec::new()));
    assert_eq!(summary.iterations, 0);
    assert_eq!(summary.wall_p50_us, 0);
    assert_eq!(summary.rss_peak_bytes, 0);
    assert_eq!(summary.cpu_ms_per_iteration, None);
}

fn run(label: &str, iterations: Vec<Iteration>) -> Run {
    Run {
        label: label.to_string(),
        started_at: "2026-01-01T00:00:00Z".to_string(),
        iterations: iterations.len() as u32,
        warmup_iterations: 0,
        scale_percent: 100,
        host_kernel: "test".to_string(),
        workloads: vec![workload_run(iterations)],
    }
}

#[test]
fn a_cpu_increase_beyond_the_threshold_is_flagged_and_a_small_one_is_not() {
    let baseline = run(
        "before",
        vec![iteration(1_000_000, 500_000_000, 1024 * 1024)],
    );
    let regressed = run(
        "after",
        vec![iteration(1_000_000, 600_000_000, 1024 * 1024)],
    );
    let flagged = compare(&baseline, &regressed);
    assert!(
        flagged.contains("+20.0% !"),
        "a 20% CPU increase must be flagged; got:\n{flagged}"
    );

    let within_noise = run(
        "after",
        vec![iteration(1_000_000, 520_000_000, 1024 * 1024)],
    );
    let quiet = cpu_row(&compare(&baseline, &within_noise));
    assert!(
        quiet.contains("+4.0%") && !quiet.contains('!'),
        "a 4% CPU increase must not be flagged; got: {quiet}"
    );
}

fn row_containing(output: &str, needle: &str) -> String {
    output
        .lines()
        .find(|line| line.contains(needle))
        .unwrap_or_else(|| panic!("comparison must include a {needle} row; got:\n{output}"))
        .to_string()
}

fn cpu_row(output: &str) -> String {
    row_containing(output, "cpu ms/iteration")
}

fn wall_row(output: &str) -> String {
    row_containing(output, "wall p50")
}

/// Throughput is the one metric where a drop is the regression, so the flag has
/// to key off direction rather than sign.
#[test]
fn a_throughput_drop_is_flagged_even_though_the_delta_is_negative() {
    let baseline = run(
        "before",
        vec![iteration(1_000_000, 500_000_000, 100 * 1024 * 1024)],
    );
    let slower = run(
        "after",
        vec![iteration(2_000_000, 500_000_000, 100 * 1024 * 1024)],
    );
    let throughput = row_containing(&compare(&baseline, &slower), "throughput");
    assert!(
        throughput.contains("-50.0% !"),
        "halved throughput must be flagged; got: {throughput}"
    );
}

/// The false positive this guards against was observed: `concurrent-fanout`
/// reported +13.9% wall and -12.2% throughput between two revs whose iterations
/// varied by more than that inside each run, and the sign flipped on the next
/// run. A delta smaller than the noise the run already showed must not be
/// flagged.
#[test]
fn a_delta_inside_the_runs_own_spread_is_not_flagged() {
    // Median 5.4ms, slowest 6.5ms: the run varies by 20% on its own.
    let baseline = run(
        "before",
        vec![
            iteration(5_000, 500_000_000, 1024 * 1024),
            iteration(5_400, 500_000_000, 1024 * 1024),
            iteration(6_500, 500_000_000, 1024 * 1024),
        ],
    );
    // Median 6.2ms: 14.8% slower, which is inside that 20%.
    let candidate = run(
        "after",
        vec![
            iteration(6_100, 500_000_000, 1024 * 1024),
            iteration(6_200, 500_000_000, 1024 * 1024),
            iteration(6_800, 500_000_000, 1024 * 1024),
        ],
    );
    let wall = wall_row(&compare(&baseline, &candidate));
    assert!(
        !wall.contains('!'),
        "a delta inside the noise floor must not be flagged; got: {wall}"
    );
    assert!(
        wall.contains("+14.8%") && wall.contains("20.4%"),
        "the delta and the noise floor must both still be reported: {wall}"
    );

    // The same-sized delta between two steady runs is a real signal.
    let steady_before = run(
        "before",
        vec![iteration(5_000, 500_000_000, 1024 * 1024); 3],
    );
    let steady_after = run("after", vec![iteration(6_000, 500_000_000, 1024 * 1024); 3]);
    let wall = wall_row(&compare(&steady_before, &steady_after));
    assert!(
        wall.contains("+20.0% !"),
        "a 20% move between steady runs must be flagged; got: {wall}"
    );
}

/// Resident memory barely varies inside a run and varies a lot between runs, so
/// the within-run spread cannot suppress a spurious move. Two runs of the same
/// pair of revs reported +13% and -4%; both would have been flagged. Report it,
/// never flag it — while an equally sized CPU move, which *is* a per-iteration
/// measurement, still is.
#[test]
fn resident_memory_is_reported_but_never_flagged() {
    let steady = |cpu_ns, rss| Iteration {
        gateway_rss_peak_bytes: rss,
        ..iteration(1_000_000, cpu_ns, 1024 * 1024)
    };
    let baseline = run("before", vec![steady(500_000_000, 47 * 1024 * 1024); 3]);
    let candidate = run("after", vec![steady(565_000_000, 53 * 1024 * 1024); 3]);
    let output = compare(&baseline, &candidate);

    let rss = row_containing(&output, "rss peak");
    assert!(
        rss.contains("+12.8%") && !rss.contains('!'),
        "resident memory must be reported without a flag; got: {rss}"
    );
    let cpu = cpu_row(&output);
    assert!(
        cpu.contains("+13.0% !"),
        "a CPU move of the same size must still be flagged; got: {cpu}"
    );
}

#[test]
fn spread_is_reported_only_once_there_is_more_than_one_sample() {
    let single = summarize(&workload_run(vec![iteration(1_000, 1_000, 1_000)]));
    assert_eq!(single.wall_spread_pct, None);
    let pair = summarize(&workload_run(vec![
        iteration(1_000, 1_000, 1_000),
        iteration(1_500, 1_000, 1_000),
    ]));
    assert!(approx(pair.wall_spread_pct, 50.0));
}

#[test]
fn a_workload_missing_from_the_baseline_is_called_out_rather_than_skipped() {
    let mut baseline = run("before", vec![iteration(1_000, 1_000, 1_000)]);
    baseline.workloads.clear();
    let candidate = run("after", vec![iteration(1_000, 1_000, 1_000)]);
    assert!(compare(&baseline, &candidate).contains("(not in baseline)"));
}
