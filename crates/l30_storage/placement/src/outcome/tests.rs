use super::*;

#[test]
fn claim_kind_strings_are_stable() {
    const KINDS: [ClaimKind; 4] = [
        ClaimKind::OnDemandCreate,
        ClaimKind::UserRestore,
        ClaimKind::EvacuationRestore,
        ClaimKind::PooledClaim,
    ];
    let rendered: Vec<&str> = KINDS.into_iter().map(ClaimKind::as_str).collect();
    assert_eq!(
        rendered,
        [
            "on_demand_create",
            "user_restore",
            "evacuation_restore",
            "pooled_claim"
        ]
    );
}
