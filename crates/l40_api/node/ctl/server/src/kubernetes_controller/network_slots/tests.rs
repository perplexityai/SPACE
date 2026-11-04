use super::*;

#[test]
fn capacity_current_matches_exact_advertised_value() {
    assert!(network_slot_capacity_current(Some("40"), "40"));
}

#[test]
fn capacity_current_detects_missing_advertisement() {
    assert!(!network_slot_capacity_current(None, "40"));
}

#[test]
fn capacity_current_detects_changed_max_slots() {
    assert!(!network_slot_capacity_current(Some("32"), "40"));
    // A shrink from 40 to 0 (drained pool) must also re-advertise.
    assert!(!network_slot_capacity_current(Some("40"), "0"));
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
