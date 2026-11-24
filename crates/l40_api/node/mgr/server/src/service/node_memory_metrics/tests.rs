use pplx_space_metrics_emit::test_capture::capture_values;

use super::emit_tick;

fn value(
    values: &[pplx_space_metrics_emit::test_capture::CapturedValue],
    name: &str,
    tags: &[&str],
) -> Option<f64> {
    values
        .iter()
        .find(|value| value.name == name && value.tags == tags)
        .map(|value| value.value)
}

#[test]
fn tick_reports_parent_cgroup_memory() {
    let dir = tempfile::tempdir().unwrap();
    for (file, content) in [
        ("memory.max", "1000\n"),
        ("memory.current", "900\n"),
        (
            "memory.stat",
            "anon 600\nfile 200\nkernel 90\npagetables 40\nshmem 5\n",
        ),
        ("memory.events", "low 0\nhigh 0\nmax 7\noom 1\noom_kill 1\n"),
        (
            "memory.pressure",
            "some avg10=2.50 avg60=0.00 avg300=0.00 total=10\nfull avg10=1.00 avg60=0.00 avg300=0.00 total=5\n",
        ),
    ] {
        std::fs::write(dir.path().join(file), content).unwrap();
    }

    let values = capture_values(|| emit_tick(dir.path()));

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
        assert_eq!(value(&values, name, &[]), Some(expected), "{name}");
    }
    assert_eq!(
        value(&values, "node_memory.space_cgroup.events", &["event:max"]),
        Some(7.0)
    );
    assert_eq!(
        value(
            &values,
            "node_memory.space_cgroup.events",
            &["event:oom_kill"]
        ),
        Some(1.0)
    );
    // This kernel reports no sec_pagetables.
    assert_eq!(
        value(
            &values,
            "node_memory.space_cgroup.sec_pagetables_bytes",
            &[]
        ),
        None
    );
}

#[test]
fn unreadable_parent_reports_only_failure() {
    let dir = tempfile::tempdir().unwrap();

    let values = capture_values(|| emit_tick(dir.path()));

    assert_eq!(values.len(), 1);
    assert_eq!(
        value(&values, "node_memory.space_cgroup.read_success", &[]),
        Some(0.0)
    );
}
