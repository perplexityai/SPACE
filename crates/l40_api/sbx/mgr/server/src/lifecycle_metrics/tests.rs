use super::*;
use crate::state::SandboxState;

#[test]
fn disappeared_lifecycle_buckets_are_zeroed_once() {
    let mut sampler = LifecycleGaugeSampler::default();
    let initial = [
        SandboxLifecycleCount {
            state: SandboxState::CleanupFailed,
            count: 2,
        },
        SandboxLifecycleCount {
            state: SandboxState::Started,
            count: 4,
        },
    ];
    let mut emitted = Vec::new();
    sampler.emit_sample(&initial, |bucket, count| {
        emitted.push((bucket.clone(), count));
    });
    sampler.emit_sample(&initial[1..], |bucket, count| {
        emitted.push((bucket.clone(), count));
    });
    sampler.emit_sample(&initial[1..], |bucket, count| {
        emitted.push((bucket.clone(), count));
    });

    assert_eq!(
        emitted,
        vec![
            (
                LifecycleBucket {
                    state: "cleanup_failed".to_owned(),
                },
                2,
            ),
            (
                LifecycleBucket {
                    state: "started".to_owned(),
                },
                4,
            ),
            (
                LifecycleBucket {
                    state: "started".to_owned(),
                },
                4,
            ),
            (
                LifecycleBucket {
                    state: "cleanup_failed".to_owned(),
                },
                0,
            ),
            (
                LifecycleBucket {
                    state: "started".to_owned(),
                },
                4,
            ),
        ]
    );
}
