use super::*;
use crate::service::start::ResolvedSandboxMemoryPolicy;

#[test]
fn device_host_memory_covers_overhead_without_clamping_or_reducing_existing_reservations() {
    let original = ResolvedSandboxMemoryPolicy {
        requested_memory_mib: 4096,
        effective_memory_mib: 4096,
        cgroup_memory_mib: 4608,
        guest_memory_max_mib: 7680,
        cgroup_memory_max_mib: 8192,
        memory_headroom_mib: 512,
    };
    for (required, expected) in [
        (4352, Some(4608)),
        (6144, Some(6144)),
        (8192, Some(8192)),
        (8193, None),
    ] {
        let mut policy = original;
        let result = reserve_host_memory(&mut policy, required);
        if let Some(expected) = expected {
            result.unwrap();
            assert_eq!(policy.cgroup_memory_mib, expected);
            assert_eq!(policy.memory_headroom_mib, expected - 4096);
        } else {
            assert_eq!(result.unwrap_err().code(), tonic::Code::ResourceExhausted);
            assert_eq!(policy.cgroup_memory_mib, original.cgroup_memory_mib);
        }
        assert_eq!(policy.effective_memory_mib, 4096);
    }
}
