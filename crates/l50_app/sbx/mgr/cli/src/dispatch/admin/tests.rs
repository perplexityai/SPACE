use super::*;

#[tokio::test]
async fn cycle_state_collection_retries_oversized_batches_without_losing_order() {
    struct SizeLimitedReader {
        max_ids: usize,
        oversized_attempts: usize,
    }

    impl CycleStateReader for SizeLimitedReader {
        async fn read_cycle_state(
            &mut self,
            sandbox_ids: &[String],
        ) -> Result<Vec<SandboxInfo>, tonic::Status> {
            if sandbox_ids.len() > self.max_ids {
                self.oversized_attempts += 1;
                return Err(tonic::Status::out_of_range("oversized State reply"));
            }
            Ok(sandbox_ids
                .iter()
                .map(|sandbox_id| SandboxInfo {
                    sandbox_id: sandbox_id.clone(),
                    ..Default::default()
                })
                .collect())
        }
    }

    // The trailing three-ID batch forces an odd split; the one-ID limit also
    // exercises the two-ID boundary before collection can succeed.
    let ids = (0..pplx_space_common_core::inventory::READ_BATCH_ITEMS + 3)
        .map(|index| format!("019d84bd-280d-7331-99c6-{index:012x}"))
        .collect::<Vec<_>>();
    let mut reader = SizeLimitedReader {
        max_ids: 1,
        oversized_attempts: 0,
    };
    let states = collect_cycle_state(&mut reader, &ids).await.unwrap();

    assert!(reader.oversized_attempts > 0);
    assert_eq!(
        states
            .iter()
            .map(|sandbox| sandbox.sandbox_id.as_str())
            .collect::<Vec<_>>(),
        ids.iter().map(String::as_str).collect::<Vec<_>>()
    );

    let mut unreadable = SizeLimitedReader {
        max_ids: 0,
        oversized_attempts: 0,
    };
    let error = collect_cycle_state(&mut unreadable, &ids[..1])
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::OutOfRange);
    assert_eq!(unreadable.oversized_attempts, 1);
}

#[test]
fn cycle_configuration_refresh_includes_current_and_unprobed_live_gateways() {
    let sandboxes = [Some(true), Some(false), None]
        .into_iter()
        .flat_map(|netgtw_stale| {
            [true, false].map(|netgtw_alive| SandboxInfo {
                sandbox_id: format!("{netgtw_stale:?}-{netgtw_alive}"),
                netgtw_alive,
                netgtw_stale,
                ..Default::default()
            })
        })
        .collect::<Vec<_>>();
    for refresh in [false, true] {
        let selected = bulk_cycle_candidates(sandboxes.clone(), refresh);
        assert!(selected.iter().all(|sandbox| sandbox.netgtw_alive));
        assert_eq!(
            selected
                .iter()
                .map(|sandbox| sandbox.netgtw_stale)
                .collect::<Vec<_>>(),
            if refresh {
                vec![Some(true), Some(false), None]
            } else {
                vec![Some(true)]
            }
        );
    }
}

#[test]
fn cycle_selection_is_explicit_exclusive_and_dry_by_default() {
    for selection in ["--stale", "--refresh-config", "1234abcd"] {
        for output in [vec![], vec!["--ids-only"]] {
            let cli = Cli::try_parse_from(
                ["space-sbxmgr", "cycle-net-gateway", selection]
                    .into_iter()
                    .chain(output),
            )
            .unwrap();
            assert!(matches!(
                cli.command,
                Commands::CycleNetGateway { execute: false, .. }
            ));
        }
    }
    for args in [
        vec![],
        vec!["--stale", "--refresh-config"],
        vec!["--refresh-config", "1234abcd"],
        vec!["--refresh-config", "--ids-only", "--execute"],
    ] {
        assert!(
            Cli::try_parse_from(
                ["space-sbxmgr", "cycle-net-gateway"]
                    .into_iter()
                    .chain(args)
            )
            .is_err()
        );
    }
}

#[test]
fn explicit_cycle_selection_keeps_live_members_after_missing_or_stopped_members() {
    let ids = [
        "019d84bd-280d-7331-99c6-b63377f3a3a1",
        "019d84bd-280d-7331-99c6-b63377f3a3a2",
        "019d84bd-280d-7331-99c6-b63377f3a3a3",
    ];
    let selectors = ids.map(|id| id.parse().unwrap());
    let sandboxes = [false, true]
        .into_iter()
        .zip(&ids[1..])
        .map(|(netgtw_alive, id)| SandboxInfo {
            sandbox_id: (*id).to_owned(),
            netgtw_alive,
            ..Default::default()
        })
        .collect::<Vec<_>>();
    for states in [&sandboxes[..], &[][..]] {
        let selection = explicit_cycle_candidates(states, &selectors);
        if states.is_empty() {
            assert!(selection.selected.is_empty());
            assert_eq!(selection.missing, ids);
            assert!(selection.no_gateway.is_empty());
        } else {
            assert_eq!(
                selection
                    .selected
                    .iter()
                    .map(|s| s.sandbox_id.as_str())
                    .collect::<Vec<_>>(),
                vec![ids[2]]
            );
            assert_eq!(selection.missing, vec![ids[0]]);
            assert_eq!(selection.no_gateway, vec![ids[1]]);
        }
    }
}
