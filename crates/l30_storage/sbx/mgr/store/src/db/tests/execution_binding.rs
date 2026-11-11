use super::*;

#[test]
fn selected_create_and_cleanup_intent_commit_or_roll_back_together() {
    #[derive(Clone, Copy, Debug)]
    enum Fault {
        None,
        RuntimeMismatch,
        CleanupWrite,
    }
    for fault in [Fault::None, Fault::RuntimeMismatch, Fault::CleanupWrite] {
        let conn = test_conn();
        let id = SandboxId::new().to_string();
        let params = TestSandboxInsert::new(&id, "bind0001", SandboxState::Creating);
        let initial = pplx_space_node_runtime_core::catalog::InitialEnvironment::for_architecture(
            pplx_space_common_core::types::Arch::X86_64,
        )
        .unwrap();
        let execution = initial
            .definition()
            .unwrap()
            .execution_requirements()
            .unwrap();
        let execution = if matches!(fault, Fault::RuntimeMismatch) {
            ExecutionMetadata::new(
                pplx_space_common_core::types::RuntimeIdentity {
                    kind: RuntimeKind::CloudHypervisor,
                    version: "1.0.0".parse().unwrap(),
                },
                execution.guest(),
                None,
                vec![],
            )
            .unwrap()
        } else {
            execution
        };
        if matches!(fault, Fault::CleanupWrite) {
            conn.execute_batch("CREATE TEMP TRIGGER refuse_cleanup BEFORE INSERT ON sandbox_cleanup_episodes BEGIN SELECT RAISE(ABORT, 'scripted cleanup refusal'); END;").unwrap();
        }
        let cleanup_id = SandboxCleanupEpisodeId::new();
        let outcome = insert_sandbox_with_cleanup_episode_and_route_evidence(
            &conn,
            params.insert_params(
                &SandboxRuntimeLabel::parse(params.uuid8.clone()).unwrap(),
                &SandboxOwner::parse(params.user.clone()).unwrap(),
            ),
            CleanupEpisodeSpec {
                id: cleanup_id,
                completion: CleanupCompletion::DeleteRow,
                keep_filesystem: false,
                entered_from_state: None,
                activation_state: SandboxState::Creating,
                execution_generation: None,
                activation_attempt_id: None,
            },
            None,
            None,
            Some(&execution),
        );
        let row = get_sandbox(&conn, &id).unwrap();
        if matches!(fault, Fault::None) {
            outcome.unwrap();
            assert_eq!(row.unwrap().execution_metadata, Some(execution));
            assert!(cleanup_episode(&conn, &id).unwrap().is_some());
        } else {
            assert!(outcome.is_err(), "{fault:?}");
            assert!(row.is_none(), "{fault:?}");
            assert!(cleanup_episode(&conn, &id).unwrap().is_none());
        }
    }
}
