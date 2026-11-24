use std::path::Path;

use pplx_space_metrics_emit::test_capture::{
    CapturedValue, capture_counters_guard, capture_values,
};

use super::Sampler;

fn value(values: &[CapturedValue], name: &str) -> Option<f64> {
    values
        .iter()
        .find(|value| value.name == name)
        .map(|value| value.value)
}

fn write_parent(dir: &Path, events: &str) {
    for (file, content) in [
        ("memory.max", "1000\n"),
        ("memory.current", "900\n"),
        (
            "memory.stat",
            "anon 600\nfile 200\nkernel 90\npagetables 40\nshmem 5\n",
        ),
        ("memory.events", events),
        (
            "memory.pressure",
            "some avg10=2.50 avg60=0.00 avg300=0.00 total=10\nfull avg10=1.00 avg60=0.00 avg300=0.00 total=5\n",
        ),
    ] {
        std::fs::write(dir.join(file), content).unwrap();
    }
}

/// Ticks once and returns the gauges and the `events` counters as
/// `(event tag, count)`.
fn tick(sampler: &mut Sampler) -> (Vec<CapturedValue>, Vec<(String, u64)>) {
    let counters = capture_counters_guard();
    let values = capture_values(|| sampler.tick());
    let events = counters
        .finish()
        .into_iter()
        .filter(|counter| counter.name == "node_memory.space_cgroup.events")
        .map(|counter| (counter.tags.join(","), counter.value))
        .collect();
    (values, events)
}

#[test]
fn tick_reports_parent_cgroup_memory() {
    let dir = tempfile::tempdir().unwrap();
    write_parent(dir.path(), "low 0\nhigh 0\nmax 7\noom 1\noom_kill 1\n");
    let mut sampler = Sampler::new(dir.path().to_path_buf());

    let (values, events) = tick(&mut sampler);

    for (name, expected) in [
        ("node_memory.space_cgroup.max_bytes", 1000.0),
        ("node_memory.space_cgroup.current_bytes", 900.0),
        ("node_memory.space_cgroup.anon_bytes", 600.0),
        ("node_memory.space_cgroup.file_bytes", 200.0),
        ("node_memory.space_cgroup.kernel_bytes", 90.0),
        ("node_memory.space_cgroup.pagetables_bytes", 40.0),
        ("node_memory.space_cgroup.shmem_bytes", 5.0),
        ("node_memory.space_cgroup.psi_some_avg10", 2.5),
        ("node_memory.space_cgroup.psi_full_avg10", 1.0),
        ("node_memory.space_cgroup.read_success", 1.0),
    ] {
        assert_eq!(value(&values, name), Some(expected), "{name}");
    }
    // This kernel reports no sec_pagetables.
    assert_eq!(
        value(&values, "node_memory.space_cgroup.sec_pagetables_bytes"),
        None
    );
    // The first reading is only a baseline.
    assert_eq!(events, []);
}

#[test]
fn events_count_growth_and_rebaseline_after_a_reset() {
    let dir = tempfile::tempdir().unwrap();
    write_parent(dir.path(), "low 0\nhigh 0\nmax 7\noom 1\noom_kill 1\n");
    let mut sampler = Sampler::new(dir.path().to_path_buf());
    tick(&mut sampler);

    write_parent(dir.path(), "low 0\nhigh 0\nmax 10\noom 2\noom_kill 2\n");
    assert_eq!(
        tick(&mut sampler).1,
        [
            ("event:max".to_string(), 3),
            ("event:oom".to_string(), 1),
            ("event:oom_kill".to_string(), 1),
        ]
    );

    // A recreated cgroup restarts its counters below the previous reading.
    write_parent(dir.path(), "low 0\nhigh 0\nmax 1\noom 0\noom_kill 0\n");
    assert_eq!(tick(&mut sampler).1, []);
    write_parent(dir.path(), "low 0\nhigh 0\nmax 2\noom 0\noom_kill 0\n");
    assert_eq!(tick(&mut sampler).1, [("event:max".to_string(), 1)]);
}

#[test]
fn unreadable_parent_reports_only_failure() {
    let dir = tempfile::tempdir().unwrap();
    let mut sampler = Sampler::new(dir.path().to_path_buf());

    let (values, events) = tick(&mut sampler);

    assert_eq!(values.len(), 1);
    assert_eq!(
        value(&values, "node_memory.space_cgroup.read_success"),
        Some(0.0)
    );
    assert_eq!(events, []);
}
