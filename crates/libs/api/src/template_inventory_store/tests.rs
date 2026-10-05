use super::*;

#[test]
fn a_template_build_shape_needs_both_values() {
    assert_eq!(
        TemplateBuildShape::from_inventory(Some(4), Some(4096)),
        Some(TemplateBuildShape {
            vcpu: VcpuCount::try_from_u32(4).unwrap(),
            memory: MemorySize::mib(4096).unwrap(),
        })
    );
    assert_eq!(
        TemplateBuildShape::from_inventory(Some(0), Some(4096)),
        None
    );
    assert_eq!(TemplateBuildShape::from_inventory(Some(4), Some(-1)), None);
    assert_eq!(TemplateBuildShape::from_inventory(None, Some(4096)), None);
}
