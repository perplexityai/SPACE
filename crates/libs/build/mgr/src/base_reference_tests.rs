use super::{
    BUILDER_SANDBOX_OWNER, BaseTemplateBuild, DEFAULT_BUILD_VM_ROOTFS_SIZE_MB,
    GuestRuntimeMetadata, build_vm_create_request, build_vm_rootfs_size_mb, build_vm_start_request,
    oci_source_message, template_guest_runtime_metadata,
};
use pplx_space_common_core::types::{ContentHash, TemplateBuildId, TemplateMetadata, TemplateName};
use rstest::{fixture, rstest};

const HASH: &str = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

#[fixture]
fn oci_guest_metadata_fixture() -> (BaseTemplateBuild, GuestRuntimeMetadata) {
    let base = BaseTemplateBuild {
        build_id: TemplateBuildId::new(
            TemplateName::parse("seed").expect("valid template name"),
            HASH.parse::<ContentHash>().expect("valid content hash"),
        ),
        resolved_reference: format!("seed@{HASH}"),
        rootfs_path: "/space/templates/seed/rootfs.ext4".to_string(),
        metadata: TemplateMetadata {
            spaced_version: Some("0.1.0".to_string()),
            spaced_compatibility_family: Some("0.1".to_string()),
            ..TemplateMetadata::default()
        },
    };
    let installed = GuestRuntimeMetadata {
        spaced_version: "0.2.0".to_string(),
        spaced_compatibility_family: "0.2".to_string(),
    };
    (base, installed)
}

#[rstest]
fn oci_template_metadata_tracks_the_guest_installed_during_assembly(
    oci_guest_metadata_fixture: (BaseTemplateBuild, GuestRuntimeMetadata),
) {
    let (base, installed) = oci_guest_metadata_fixture;

    let metadata = template_guest_runtime_metadata(&base, Some(&installed));

    assert_eq!(
        metadata,
        (
            Some(installed.spaced_version),
            Some(installed.spaced_compatibility_family),
        )
    );
}

#[test]
fn build_vm_create_request_pins_resolved_base_hash() {
    let request = build_vm_create_request(
        "code-runtime".to_string(),
        HASH.to_string(),
        Some(20 * 1024),
        None,
    );

    assert_eq!(request.template, "code-runtime");
    assert_eq!(request.content_hash.as_deref(), Some(HASH));
    assert_eq!(request.rootfs_size_mb, Some(20 * 1024));
    assert_eq!(request.owner, BUILDER_SANDBOX_OWNER.as_str());
    assert_eq!(request.route_identity, None);
}

#[test]
fn build_vm_start_request_delegates_direct_identity_to_sandbox_manager() {
    let request = build_vm_start_request("sandbox-id".to_string(), 2, 2048);

    assert_eq!(request.sandbox_id, "sandbox-id");
    assert_eq!(request.vcpu, Some(2));
    assert_eq!(request.memory_mib, Some(2048));
    assert_eq!(request.route_identity, None);
}

#[test]
fn build_vm_rootfs_size_uses_explicit_recipe_disk_or_twenty_gib_default() {
    assert_eq!(
        build_vm_rootfs_size_mb(None),
        DEFAULT_BUILD_VM_ROOTFS_SIZE_MB
    );
    assert_eq!(build_vm_rootfs_size_mb(Some(4096)), 4096);
    assert_eq!(build_vm_rootfs_size_mb(Some(32768)), 32768);
}

#[test]
fn oci_build_summary_does_not_include_the_caller_reference() {
    let reference = "docker.io/customer-secret/image:latest"
        .parse()
        .expect("valid OCI reference");

    let message = oci_source_message(Some(&reference)).expect("OCI source message");

    assert_eq!(message, "  Source: OCI image");
}

#[rstest]
#[case::local(false)]
#[case::tenant(true)]
fn base_lookup_preserves_local_execution_and_tenant_catalog_authority(#[case] tenant: bool) {
    use pplx_space_common_svc::template_lookup::TemplateLookupScope;
    let org = tenant.then(pplx_space_common_core::types::OrgId::new);
    let builder = super::TemplateBuilder::new().with_caller_org(org);
    let (_, request) =
        builder.base_lookup_request(pplx_space_template_mgr_client::proto::TemplateRef {
            name: "seed".into(),
            content_hash: Some(HASH.into()),
            tag: None,
        });

    let scope = TemplateLookupScope::from_request(&request).unwrap();

    assert_eq!(
        scope,
        Some(if tenant {
            TemplateLookupScope::Cluster
        } else {
            TemplateLookupScope::LocalExecution
        })
    );
    assert_eq!(
        request.get_ref().caller_org_id,
        org.map(|org| org.to_string())
    );
}
