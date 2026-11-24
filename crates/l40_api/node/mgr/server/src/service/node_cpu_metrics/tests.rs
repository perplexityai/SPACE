use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use pplx_space_metrics_emit::test_capture::{CapturedValue, capture_values};
use tempfile::tempdir;

use super::{NodeCpuMetricSources, NodeCpuSampler};

fn by_name(values: &[CapturedValue]) -> HashMap<&str, f64> {
    values
        .iter()
        .map(|entry| (entry.name.as_str(), entry.value))
        .collect()
}

fn assert_approx(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1e-6,
        "expected {expected}, got {actual}"
    );
}

#[test]
fn emits_node_cpu_gauges_from_synthetic_sources() {
    let dir = tempdir().expect("create tempdir");
    let proc_pressure_cpu = dir.path().join("pressure_cpu");
    let cgroup_cpu_stat = dir.path().join("cpu.stat");

    std::fs::write(
        &proc_pressure_cpu,
        "some avg10=1.25 avg60=0.50 avg300=0.10 total=1000\n\
         full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
    )
    .expect("write initial pressure");
    std::fs::write(
        &cgroup_cpu_stat,
        "usage_usec 10000000\n\
         user_usec 8000000\n\
         system_usec 2000000\n\
         nr_periods 100\n\
         nr_throttled 7\n\
         throttled_usec 25000\n",
    )
    .expect("write initial cpu.stat");

    let mut sampler = NodeCpuSampler::new(NodeCpuMetricSources {
        proc_pressure_cpu: proc_pressure_cpu.clone(),
        parent_cgroup_cpu_stat: cgroup_cpu_stat.clone(),
    });

    let t0 = Instant::now();
    let first_values = capture_values(|| {
        sampler.sample_and_emit_at(t0);
    });
    let first = by_name(&first_values);
    assert_approx(first["node_cpu.pressure_some_avg10"], 1.25);
    assert!(!first.contains_key("node_cpu.space_cgroup.usage_usec_per_sec"));
    assert!(!first.contains_key("node_cpu.space_cgroup.nr_throttled_per_sec"));
    assert!(!first.contains_key("node_cpu.space_cgroup.throttled_usec_per_sec"));

    std::fs::write(
        &proc_pressure_cpu,
        "some avg10=12.34 avg60=5.00 avg300=1.00 total=9999\n",
    )
    .expect("write second pressure");
    std::fs::write(
        &cgroup_cpu_stat,
        "usage_usec 35000000\n\
         nr_throttled 42\n\
         throttled_usec 987500\n",
    )
    .expect("write second cpu.stat");

    let second_values = capture_values(|| {
        sampler.sample_and_emit_at(t0 + Duration::from_secs(10));
    });
    let second = by_name(&second_values);
    assert_approx(second["node_cpu.pressure_some_avg10"], 12.34);
    assert_approx(
        second["node_cpu.space_cgroup.usage_usec_per_sec"],
        2_500_000.0,
    );
    assert_approx(second["node_cpu.space_cgroup.nr_throttled_per_sec"], 3.5);
    assert_approx(
        second["node_cpu.space_cgroup.throttled_usec_per_sec"],
        96_250.0,
    );
}

#[test]
fn drops_cgroup_counter_regressions_and_skips_missing_sources() {
    let dir = tempdir().expect("create tempdir");
    let proc_pressure_cpu = dir.path().join("pressure_cpu");
    let cgroup_cpu_stat = dir.path().join("cpu.stat");

    std::fs::write(
        &proc_pressure_cpu,
        "some avg10=3.50 avg60=1.00 avg300=0.50 total=42\n",
    )
    .expect("write pressure");
    std::fs::write(
        &cgroup_cpu_stat,
        "usage_usec 5000000\n\
         nr_throttled 20\n\
         throttled_usec 50000\n",
    )
    .expect("write initial cpu.stat");

    let mut sampler = NodeCpuSampler::new(NodeCpuMetricSources {
        proc_pressure_cpu,
        parent_cgroup_cpu_stat: cgroup_cpu_stat.clone(),
    });

    let t0 = Instant::now();
    sampler.sample_and_emit_at(t0);

    // Simulate cgroup recreation where cumulative counters drop below the
    // previous sample.
    std::fs::write(
        &cgroup_cpu_stat,
        "usage_usec 1000000\n\
         nr_throttled 2\n\
         throttled_usec 1000\n",
    )
    .expect("write regressed cpu.stat");

    let regressed_values = capture_values(|| {
        sampler.sample_and_emit_at(t0 + Duration::from_secs(5));
    });
    let regressed = by_name(&regressed_values);
    assert_approx(regressed["node_cpu.pressure_some_avg10"], 3.5);
    assert!(!regressed.contains_key("node_cpu.space_cgroup.usage_usec_per_sec"));
    assert!(!regressed.contains_key("node_cpu.space_cgroup.nr_throttled_per_sec"));
    assert!(!regressed.contains_key("node_cpu.space_cgroup.throttled_usec_per_sec"));

    // Next sample recovers from the post-reset baseline.
    std::fs::write(
        &cgroup_cpu_stat,
        "usage_usec 6000000\n\
         nr_throttled 7\n\
         throttled_usec 11000\n",
    )
    .expect("write recovered cpu.stat");

    let recovered_values = capture_values(|| {
        sampler.sample_and_emit_at(t0 + Duration::from_secs(10));
    });
    let recovered = by_name(&recovered_values);
    assert_approx(
        recovered["node_cpu.space_cgroup.usage_usec_per_sec"],
        1_000_000.0,
    );
    assert_approx(recovered["node_cpu.space_cgroup.nr_throttled_per_sec"], 1.0);
    assert_approx(
        recovered["node_cpu.space_cgroup.throttled_usec_per_sec"],
        2_000.0,
    );

    // Removing cpu.stat skips cgroup gauges without blocking PSI.
    std::fs::remove_file(&cgroup_cpu_stat).expect("remove cpu.stat");
    let missing_values = capture_values(|| {
        sampler.sample_and_emit_at(t0 + Duration::from_secs(15));
    });
    let missing = by_name(&missing_values);
    assert_approx(missing["node_cpu.pressure_some_avg10"], 3.5);
    assert!(!missing.contains_key("node_cpu.space_cgroup.usage_usec_per_sec"));
}

#[cfg(target_os = "linux")]
#[test]
fn samples_live_linux_host_sources() {
    let default_sources = NodeCpuMetricSources::default();
    let parent_cgroup_cpu_stat = if default_sources.parent_cgroup_cpu_stat.exists() {
        default_sources.parent_cgroup_cpu_stat
    } else {
        PathBuf::from("/sys/fs/cgroup/cpu.stat")
    };
    let sources = NodeCpuMetricSources {
        proc_pressure_cpu: default_sources.proc_pressure_cpu,
        parent_cgroup_cpu_stat,
    };
    let mut sampler = NodeCpuSampler::new(sources.clone());
    sampler.sample_and_emit();
    std::thread::sleep(Duration::from_millis(50));

    let mut sample = None;
    let values = capture_values(|| {
        sample = Some(sampler.sample_and_emit_at(Instant::now()));
    });
    let sample = sample.expect("second sample recorded");
    let gauges = by_name(&values);

    if sources.proc_pressure_cpu.exists() {
        let avg10 = sample
            .pressure_some_avg10
            .expect("live /proc/pressure/cpu produced some avg10");
        assert!(avg10.is_finite() && avg10 >= 0.0);
        assert!(gauges.contains_key("node_cpu.pressure_some_avg10"));
    }

    if sources.parent_cgroup_cpu_stat.exists() {
        let rate = sample
            .cgroup_usage_usec_per_sec
            .expect("live cgroup cpu.stat produced usage_usec rate");
        assert!(rate.is_finite() && rate >= 0.0);
        assert!(gauges.contains_key("node_cpu.space_cgroup.usage_usec_per_sec"));
        if let Some(nr_rate) = sample.cgroup_nr_throttled_per_sec {
            assert!(nr_rate.is_finite() && nr_rate >= 0.0);
            assert!(gauges.contains_key("node_cpu.space_cgroup.nr_throttled_per_sec"));
        }
        if let Some(throttled_rate) = sample.cgroup_throttled_usec_per_sec {
            assert!(throttled_rate.is_finite() && throttled_rate >= 0.0);
            assert!(gauges.contains_key("node_cpu.space_cgroup.throttled_usec_per_sec"));
        }
    }

    println!(
        "live node CPU sample: psi_some_avg10={:?} cgroup_usage_usec_per_sec={:?} nr_throttled_per_sec={:?} throttled_usec_per_sec={:?}",
        sample.pressure_some_avg10,
        sample.cgroup_usage_usec_per_sec,
        sample.cgroup_nr_throttled_per_sec,
        sample.cgroup_throttled_usec_per_sec,
    );
}
