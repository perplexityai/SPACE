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
    let proc_stat = dir.path().join("stat");
    let proc_pressure_cpu = dir.path().join("pressure_cpu");
    let cgroup_cpu_stat = dir.path().join("cpu.stat");

    std::fs::write(
        &proc_stat,
        "cpu  1000 100 500 10000 200 10 20 5 50 0\n\
         cpu0 250 25 125 2500 50 2 5 1 12 0\n\
         cpu1 250 25 125 2500 50 2 5 1 12 0\n\
         cpu2 250 25 125 2500 50 3 5 1 13 0\n\
         cpu3 250 25 125 2500 50 3 5 2 13 0\n",
    )
    .expect("write initial proc/stat");
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
        proc_stat: proc_stat.clone(),
        proc_pressure_cpu: proc_pressure_cpu.clone(),
        parent_cgroup_cpu_stat: cgroup_cpu_stat.clone(),
    });

    let t0 = Instant::now();
    let first_values = capture_values(|| {
        sampler.sample_and_emit_at(t0);
    });
    let first = by_name(&first_values);
    assert!(!first.contains_key("node_cpu.busy_cores"));
    assert!(!first.contains_key("node_cpu.space_cgroup.usage_usec_per_sec"));
    assert_approx(first["node_cpu.pressure_some_avg10"], 1.25);
    assert_approx(first["node_cpu.space_cgroup.nr_throttled"], 7.0);
    assert_approx(first["node_cpu.space_cgroup.throttled_usec"], 25_000.0);

    // Advance by 4000 total jiffies across 4 CPUs:
    // user+nice = 1500+500 = 2000 (2.0 cores)
    // system+irq+softirq = 400+50+50 = 500 (0.5 cores)
    // steal = 200 (0.2 cores) -> busy = 2.7 cores
    // iowait = 300 (0.3 cores), idle = 1000 (1.0 core)
    std::fs::write(
        &proc_stat,
        "cpu  2500 600 900 11000 500 60 70 205 150 0\n\
         cpu0 625 150 225 2750 125 15 17 51 37 0\n\
         cpu1 625 150 225 2750 125 15 17 51 37 0\n\
         cpu2 625 150 225 2750 125 15 18 51 38 0\n\
         cpu3 625 150 225 2750 125 15 18 52 38 0\n",
    )
    .expect("write second proc/stat");
    std::fs::write(
        &proc_pressure_cpu,
        "some avg10=12.34 avg60=5.00 avg300=1.00 total=9999\n",
    )
    .expect("write second pressure");
    std::fs::write(
        &cgroup_cpu_stat,
        "usage_usec 35000000\n\
         nr_throttled 42\n\
         throttled_usec 987654\n",
    )
    .expect("write second cpu.stat");

    let second_values = capture_values(|| {
        sampler.sample_and_emit_at(t0 + Duration::from_secs(10));
    });
    let second = by_name(&second_values);
    assert_approx(second["node_cpu.busy_cores"], 2.7);
    assert_approx(second["node_cpu.user_cores"], 2.0);
    assert_approx(second["node_cpu.system_cores"], 0.5);
    assert_approx(second["node_cpu.iowait_cores"], 0.3);
    assert_approx(second["node_cpu.pressure_some_avg10"], 12.34);
    assert_approx(
        second["node_cpu.space_cgroup.usage_usec_per_sec"],
        2_500_000.0,
    );
    assert_approx(second["node_cpu.space_cgroup.nr_throttled"], 42.0);
    assert_approx(second["node_cpu.space_cgroup.throttled_usec"], 987_654.0);
}

#[test]
fn skips_missing_or_regressed_sources_without_blocking_other_gauges() {
    let dir = tempdir().expect("create tempdir");
    let proc_stat = dir.path().join("stat");
    let proc_pressure_cpu = dir.path().join("pressure_cpu");
    let missing_cgroup = dir.path().join("does-not-exist");

    std::fs::write(
        &proc_stat,
        "cpu  500 0 100 1000 0 0 0 0\n\
         cpu0 500 0 100 1000 0 0 0 0\n",
    )
    .expect("write initial proc/stat");
    std::fs::write(
        &proc_pressure_cpu,
        "some avg10=3.50 avg60=1.00 avg300=0.50 total=42\n",
    )
    .expect("write pressure");

    let mut sampler = NodeCpuSampler::new(NodeCpuMetricSources {
        proc_stat: proc_stat.clone(),
        proc_pressure_cpu,
        parent_cgroup_cpu_stat: missing_cgroup,
    });

    let t0 = Instant::now();
    sampler.sample_and_emit_at(t0);

    // Simulate a counter reset / backward jump in /proc/stat.
    std::fs::write(
        &proc_stat,
        "cpu  100 0 50 200 0 0 0 0\n\
         cpu0 100 0 50 200 0 0 0 0\n",
    )
    .expect("write regressed proc/stat");

    let values = capture_values(|| {
        sampler.sample_and_emit_at(t0 + Duration::from_secs(5));
    });
    let gauges = by_name(&values);
    assert!(!gauges.contains_key("node_cpu.busy_cores"));
    assert!(!gauges.contains_key("node_cpu.space_cgroup.usage_usec_per_sec"));
    assert_approx(gauges["node_cpu.pressure_some_avg10"], 3.5);
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
        proc_stat: default_sources.proc_stat,
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

    let cores = sample
        .host_cores
        .expect("live /proc/stat produced host CPU core deltas");
    assert!(cores.busy.is_finite() && cores.busy >= 0.0);
    assert!(cores.user.is_finite() && cores.user >= 0.0);
    assert!(cores.system.is_finite() && cores.system >= 0.0);
    assert!(cores.iowait.is_finite() && cores.iowait >= 0.0);
    assert!(gauges.contains_key("node_cpu.busy_cores"));

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
    }

    println!(
        "live node CPU sample: busy={:.3} user={:.3} system={:.3} iowait={:.3} psi_some_avg10={:?} cgroup_usage_usec_per_sec={:?} nr_throttled={:?} throttled_usec={:?}",
        cores.busy,
        cores.user,
        cores.system,
        cores.iowait,
        sample.pressure_some_avg10,
        sample.cgroup_usage_usec_per_sec,
        sample.cgroup_nr_throttled,
        sample.cgroup_throttled_usec,
    );
}
