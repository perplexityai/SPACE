use super::*;

#[test]
fn vcpu_rejects_zero() {
    CpuLimit::vcpu(0).unwrap_err();
    CpuLimit::vcpu(1).unwrap();
}
