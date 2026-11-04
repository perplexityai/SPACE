use super::*;

#[test]
fn exact_boundary_succeeds_and_overflow_cannot_spend_remaining_capacity() {
    let mut bytes = InventoryBudget::new(2, 5);
    bytes.charge(3).unwrap();
    bytes.charge(3).unwrap_err();
    bytes.charge(2).unwrap();
    bytes.charge(0).unwrap_err();
    let mut items = InventoryBudget::new(1, usize::MAX);
    items.charge(0).unwrap();
    items.charge(0).unwrap_err();
    items.charge_bytes(usize::MAX).unwrap();
    items.charge_bytes(1).unwrap_err();
}
