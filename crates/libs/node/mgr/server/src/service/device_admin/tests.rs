use super::*;
use crate::service::tests::service_tests::{root_request, service_for_unit_tests};
use crate::slot_pool::devices::EnrollmentIdentity;
use pplx_space_common_core::types::Digest;

fn sandbox_caller<T>(message: T) -> Request<T> {
    let user = pplx_space_common_svc::core::host::linux::identities::users::SPACE_SBXMGR;
    let mut request = Request::new(message);
    request
        .extensions_mut()
        .insert(pplx_space_common_svc::PeerIdentity::new(
            user.uid,
            user.primary_group.gid,
            None,
        ));
    request
}

#[tokio::test]
async fn device_inventory_requires_operator_authority_and_reports_quarantine_without_cleaning_it() {
    let service = service_for_unit_tests().await;
    let locator: HostDeviceId = "pci:0000:01:00.0".parse().unwrap();
    let parent = service
        .pool
        .enroll_device_parent(
            &"compute".parse().unwrap(),
            "00000000-0000-4000-8000-000000000001".parse().unwrap(),
            Digest::sha256([1; 32]),
            &[(
                locator.clone(),
                EnrollmentIdentity::LinuxPciV1 {
                    locator: locator.clone(),
                    vendor: 1,
                    device: 2,
                    subsystem_vendor: 3,
                    subsystem_device: 4,
                    topology_digest: Digest::sha256([2; 32]),
                    sysfs_device: 1,
                    sysfs_inode: 1,
                },
            )],
        )
        .unwrap();
    let response = service
        .list_device_parents(root_request(ListDeviceParentsRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.parents.len(), 1);
    let observed = &response.parents[0];
    assert_eq!(observed.parent_id, parent.to_string());
    assert_eq!(observed.host_device_ids, vec![locator.to_string()]);
    assert_eq!(observed.state, DeviceParentState::Quarantined as i32);
    assert_eq!(observed.revision, 0);
    assert_eq!(
        observed.qualification_digest,
        Digest::sha256([1; 32]).to_string()
    );
    let error = service
        .list_device_parents(Request::new(ListDeviceParentsRequest {}))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unauthenticated);
    for error in [
        service
            .list_device_parents(sandbox_caller(ListDeviceParentsRequest {}))
            .await
            .unwrap_err(),
        service
            .enroll_device_parent(sandbox_caller(EnrollDeviceParentRequest::default()))
            .await
            .unwrap_err(),
        service
            .retire_device_parent(sandbox_caller(RetireDeviceParentRequest::default()))
            .await
            .unwrap_err(),
        service
            .requalify_device_parent(sandbox_caller(RequalifyDeviceParentRequest::default()))
            .await
            .unwrap_err(),
    ] {
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }
    assert_eq!(
        service.pool.device_parent(parent).unwrap().state(),
        ParentState::Quarantined
    );
}

#[test]
fn enrollment_member_input_is_bounded_and_duplicate_free() {
    assert_eq!(
        members(vec!["pci:b".into(), "pci:a".into()]).unwrap(),
        vec!["pci:a".parse().unwrap(), "pci:b".parse().unwrap()]
    );
    for raw in [
        vec![],
        vec!["pci:a".into(), "pci:a".into()],
        vec!["pci:a".into(); 33],
    ] {
        assert_eq!(
            members(raw).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
}
