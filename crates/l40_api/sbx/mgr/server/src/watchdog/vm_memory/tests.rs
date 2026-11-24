use super::*;
use pplx_space_node_runtime_core::cgroup::DEFAULT_SANDBOX_MEMORY_HEADROOM_MIB;
use pplx_space_node_runtime_core::cgroup_memory::CgroupMemoryLimit;

#[test]
fn samples_only_populated_cgroups_and_never_substitutes_zero_for_unreadable_memory() {
    let dir = tempfile::tempdir().expect("temporary cgroup");
    let root = dir.path();
    let write = |name: &str, value: &str| {
        std::fs::write(root.join(name), value).expect("write cgroup file");
    };
    assert_eq!(read_sample(root), None);

    write("cgroup.events", "populated 1\nfrozen 1\n");
    write("memory.current", "987654321\n");
    write("memory.stat", "anon_thp 1024\nfile 5678\nanon 123456789\n");
    let two_gib_limit = (2048 + DEFAULT_SANDBOX_MEMORY_HEADROOM_MIB) * 1024 * 1024;
    for (limit, bucket) in [
        (two_gib_limit, CgroupLimitBucket::UpTo2Gib),
        (two_gib_limit + 1, CgroupLimitBucket::UpTo4Gib),
    ] {
        write("memory.max", &format!("{limit}\n"));
        let sample = read_sample(root).expect("populated cgroup is sampled");
        assert_eq!(sample.current_bytes, 987654321);
        assert_eq!(sample.anon_bytes, Some(123456789));
        assert_eq!(CgroupLimitBucket::from_memory_max(sample.limit), bucket);
    }

    write("memory.stat", "anon_thp 1024\nfile 5678\n");
    write("memory.max", "max\n");
    let sample = read_sample(root).expect("current is available");
    assert_eq!(sample.anon_bytes, None);
    assert_eq!(sample.limit, CgroupMemoryLimit::Unlimited);

    std::fs::remove_file(root.join("memory.max")).expect("remove the limit fixture");
    let sample = read_sample(root).expect("missing limit does not hide available usage");
    assert_eq!(sample.limit, CgroupMemoryLimit::Unknown);

    write("memory.current", "invalid\n");
    assert_eq!(read_sample(root), None);
    write("memory.current", "987654321\n");
    write("cgroup.events", "populated 0\nfrozen 0\n");
    assert_eq!(read_sample(root), None);
}

#[tokio::test(start_paused = true)]
async fn repeated_truncated_sweeps_sample_the_tail_of_the_inventory() {
    let completed = std::sync::Mutex::new(Vec::new());
    let visit = |target| {
        let completed = &completed;
        async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            completed.lock().unwrap().push(target);
        }
    };
    let targets = [0, 1, 2, 3, 4, 5];
    let mut cursor = 0;
    for _ in 0..3 {
        assert!(sweep(targets.to_vec(), &mut cursor, visit).await);
    }
    let mut sampled = completed.into_inner().unwrap();
    sampled.sort_unstable();
    assert_eq!(sampled, targets);
}
