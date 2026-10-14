use super::*;
use crate::db::pool_adoption::*;
use pplx_space_common_core::types::{OrgId, ROOT_SANDBOX_OWNER};

#[test]
fn encrypted_pool_claim_is_fenced_until_completed_and_replays_only_for_its_owner() {
    for volume_encryption in [false, true] {
        let conn = test_conn();
        let id = SandboxId::new().to_string();
        insert_test_sandbox(
            &conn,
            TestSandboxInsert {
                user: ROOT_SANDBOX_OWNER.to_string(),
                ..TestSandboxInsert::new(&id, "encpool1", SandboxState::Paused)
            },
        );
        let claim = PoolClaim {
            id: uuid::Uuid::now_v7(),
            org_id: OrgId::new(),
            volume_encryption,
        };
        let owner = adopter_owner();
        let env = BTreeMap::from([("EXAMPLE".to_string(), "adopted".to_string())]);

        assert!(!begin(&conn, &id, claim, &owner, &env).unwrap());
        assert!(!guest_access_blocked(&conn, &id).unwrap());
        register(&conn, &id).unwrap();
        assert!(guest_access_blocked(&conn, &id).unwrap());
        assert!(begin(&conn, &id, claim, &owner, &env).unwrap());

        assert!(
            !begin(
                &conn,
                &id,
                PoolClaim {
                    volume_encryption: !claim.volume_encryption,
                    ..claim
                },
                &owner,
                &env
            )
            .unwrap()
        );
        assert!(incomplete(&conn, &id).unwrap());
        assert!(guest_access_blocked(&conn, &id).unwrap());
        assert!(!get_sandbox(&conn, &id).unwrap().unwrap().volume_encryption);
        assert!(
            !begin(
                &conn,
                &id,
                PoolClaim {
                    id: uuid::Uuid::now_v7(),
                    ..claim
                },
                &owner,
                &env
            )
            .unwrap()
        );
        assert!(
            !begin(
                &conn,
                &id,
                PoolClaim {
                    org_id: OrgId::new(),
                    ..claim
                },
                &owner,
                &env
            )
            .unwrap()
        );
        assert!(!begin(&conn, &id, claim, &owner, &BTreeMap::new()).unwrap());
        assert!(
            !complete(
                &conn,
                &id,
                PoolClaim {
                    id: uuid::Uuid::now_v7(),
                    ..claim
                }
            )
            .unwrap()
        );
        assert!(begin(&conn, &id, claim, &owner, &env).unwrap());
        assert!(complete(&conn, &id, claim).unwrap());
        assert!(complete(&conn, &id, claim).unwrap());
        assert!(!incomplete(&conn, &id).unwrap());
        assert!(!guest_access_blocked(&conn, &id).unwrap());
        let adopted = get_sandbox(&conn, &id).unwrap().unwrap();
        assert_eq!(adopted.volume_encryption, volume_encryption);
        assert_eq!(adopted.user, owner);
        assert_eq!(adopted.process_env, env);
        assert_eq!(adopted.state, SandboxState::Paused);
    }
}

#[test]
fn encrypted_pool_claim_cannot_complete_while_the_member_is_running() {
    let conn = test_conn();
    let id = SandboxId::new().to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert {
            user: ROOT_SANDBOX_OWNER.to_string(),
            ..TestSandboxInsert::new(&id, "encpool2", SandboxState::Started)
        },
    );
    register(&conn, &id).unwrap();
    let claim = PoolClaim {
        id: uuid::Uuid::now_v7(),
        org_id: OrgId::new(),
        volume_encryption: true,
    };

    assert!(begin(&conn, &id, claim, &adopter_owner(), &BTreeMap::new()).unwrap());
    assert!(!complete(&conn, &id, claim).unwrap());

    assert!(incomplete(&conn, &id).unwrap());
    assert!(!get_sandbox(&conn, &id).unwrap().unwrap().volume_encryption);
}
