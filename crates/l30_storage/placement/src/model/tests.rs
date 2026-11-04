use super::*;

fn generation(value: i64) -> ExecutionGeneration {
    ExecutionGeneration::new(value).expect("a positive generation is valid")
}

#[test]
fn for_generation_re_pairs_the_generation_and_keeps_lease_and_sandbox() {
    let sandbox = SandboxId::new();
    let lease = LeaseToken::mint();
    let fence = PlacementFence {
        sandbox,
        generation: generation(1),
        lease,
    };

    let re_paired = fence.for_generation(generation(2));

    assert_eq!(
        re_paired.generation(),
        generation(2),
        "the observed generation replaces the fence's"
    );
    assert_eq!(
        (re_paired.sandbox(), re_paired.lease()),
        (sandbox, lease),
        "ownership stays the lease: sandbox and token are untouched"
    );
}
