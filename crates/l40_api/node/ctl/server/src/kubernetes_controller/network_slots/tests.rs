use super::*;

#[test]
fn capacity_current_matches_exact_advertised_value() {
    assert!(network_slot_capacity_current(Some("40"), None, "40"));
    assert!(network_slot_capacity_current(Some("40"), Some("40"), "40"));
}

#[test]
fn capacity_current_does_not_trust_a_cache_behind_the_last_apply() {
    // Applied 56, then 60; the watch still shows 56 when 56 is wanted again.
    assert!(!network_slot_capacity_current(Some("56"), Some("60"), "56"));
}

#[test]
fn capacity_current_detects_missing_advertisement() {
    assert!(!network_slot_capacity_current(None, None, "40"));
}

#[test]
fn capacity_current_detects_changed_max_slots() {
    assert!(!network_slot_capacity_current(Some("32"), None, "40"));
    // A shrink from 40 to 0 (drained pool) must also re-advertise.
    assert!(!network_slot_capacity_current(Some("40"), Some("40"), "0"));
}

#[test]
fn capacity_apply_targets_status_capacity_extended_resource() {
    let apply = network_slot_capacity_apply("ip-10-0-0-1.ec2.internal", "40");

    assert_eq!(apply["apiVersion"], "v1");
    assert_eq!(apply["kind"], "Node");
    assert_eq!(apply["metadata"]["name"], "ip-10-0-0-1.ec2.internal");
    assert_eq!(
        apply["status"]["capacity"][SANDBOX_NETWORK_SLOTS_RESOURCE], "40",
        "capacity must be advertised as a string Quantity on the status subresource"
    );
}

#[test]
fn patch_params_force_owns_capacity_field_manager() {
    let params = network_slots_patch_params();

    assert_eq!(
        params.field_manager.as_deref(),
        Some(NETWORK_SLOTS_FIELD_MANAGER)
    );
    assert!(
        params.force,
        "force apply lets node-ctl reclaim the capacity field after a kubelet rewrite"
    );
}

#[test]
fn capacity_is_max_slots_while_memory_is_not_binding() {
    assert_eq!(desired_network_slot_capacity(64, 10, Some(54)), 64);
    assert_eq!(desired_network_slot_capacity(64, 10, Some(500)), 64);
    assert_eq!(desired_network_slot_capacity(64, 10, None), 64);
}

#[test]
fn capacity_shrinks_to_what_memory_admission_accepts() {
    // A node whose memory admission refuses every new sandbox advertises
    // only its leased slots, so the scheduler binds no more pods to it.
    assert_eq!(desired_network_slot_capacity(64, 56, Some(0)), 56);
    assert_eq!(desired_network_slot_capacity(64, 56, Some(3)), 59);
}

#[test]
fn capacity_never_exceeds_max_slots_or_overflows() {
    assert_eq!(
        desired_network_slot_capacity(64, u32::MAX, Some(u32::MAX)),
        64
    );
    assert_eq!(desired_network_slot_capacity(0, 0, Some(10)), 0);
}
