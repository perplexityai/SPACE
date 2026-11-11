use super::*;
use crate::state::SandboxState;

#[test]
fn status_buckets_name_the_cleanup_state_and_runtime_and_zero_a_disappeared_series_once() {
    let mut sampler = StatusGaugeSampler::default();
    let initial = [
        SandboxStatusCount {
            state: SandboxState::Stopping,
            cleanup_state: Some("failed"),
            runtime: Some(RuntimeKind::Firecracker),
            runtime_version: None,
            count: 2,
        },
        SandboxStatusCount {
            state: SandboxState::Stopping,
            cleanup_state: None,
            runtime: Some(RuntimeKind::Firecracker),
            runtime_version: None,
            count: 1,
        },
    ];
    let bucket = |cleanup_state: &str| StatusBucket {
        state: "stopping".to_owned(),
        cleanup_state: cleanup_state.to_owned(),
        runtime: Some(RuntimeKind::Firecracker),
        runtime_version: None,
    };
    let mut emitted = Vec::new();
    for sample in [&initial[..], &initial[1..], &initial[1..]] {
        sampler.emit_sample(sample, |bucket, count| {
            emitted.push((bucket.clone(), count));
        });
    }

    assert_eq!(
        emitted,
        vec![
            (bucket("failed"), 2),
            (bucket("none"), 1),
            (bucket("none"), 1),
            (bucket("failed"), 0),
            (bucket("none"), 1),
        ]
    );
}
