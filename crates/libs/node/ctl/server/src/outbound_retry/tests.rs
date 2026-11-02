use super::*;

fn unavailable() -> OutboundError {
    OutboundError::ClusterUnavailable("transport error: connection refused".to_string())
}

fn rejected() -> OutboundError {
    OutboundError::Other("cluster node state publish failed: permission denied".to_string())
}

/// One facet's full publish in a pass: published, or failed with `error`.
type Publish = (FacetKind, Option<fn() -> OutboundError>);

/// A named run of passes, and the facets its end reports dropped, or `None`
/// when it never ends.
type EpisodeCase = (&'static str, Vec<Vec<Publish>>, Option<BTreeSet<FacetKind>>);

/// How a retry episode ends, by the passes it runs. A retried facet that
/// then fails in a way a retry cannot fix is reported as dropped when the
/// episode ends, however many passes later, unless it published since; a
/// facet that was never retried does not end or colour an episode.
#[test]
fn an_episode_reports_the_retried_facets_it_dropped() {
    use FacetKind::{NodeStatus, SandboxInventory, TemplateInventory};
    let cases: [EpisodeCase; 6] = [
        (
            "every retried facet published",
            vec![
                vec![(SandboxInventory, Some(unavailable)), (NodeStatus, None)],
                vec![(SandboxInventory, None)],
            ],
            Some(BTreeSet::new()),
        ),
        (
            "one dropped while another kept retrying",
            vec![
                vec![
                    (SandboxInventory, Some(unavailable)),
                    (TemplateInventory, Some(unavailable)),
                ],
                vec![
                    (SandboxInventory, Some(rejected)),
                    (TemplateInventory, Some(unavailable)),
                ],
                vec![(TemplateInventory, None)],
            ],
            Some(BTreeSet::from([SandboxInventory])),
        ),
        (
            "a dropped facet that published before the end",
            vec![
                vec![
                    (SandboxInventory, Some(unavailable)),
                    (TemplateInventory, Some(unavailable)),
                ],
                vec![
                    (SandboxInventory, Some(rejected)),
                    (TemplateInventory, Some(unavailable)),
                ],
                vec![(SandboxInventory, None), (TemplateInventory, None)],
            ],
            Some(BTreeSet::new()),
        ),
        (
            "a facet that was never retried",
            vec![vec![(NodeStatus, Some(rejected))]],
            None,
        ),
        (
            "a never-retried facet failing in the closing pass",
            vec![
                vec![(SandboxInventory, Some(unavailable))],
                vec![(SandboxInventory, None), (NodeStatus, Some(rejected))],
            ],
            Some(BTreeSet::new()),
        ),
        (
            "a rejection before an episode does not carry into it",
            vec![
                vec![(NodeStatus, Some(rejected))],
                vec![(SandboxInventory, Some(unavailable))],
                vec![(SandboxInventory, None)],
            ],
            Some(BTreeSet::new()),
        ),
    ];
    for (name, passes, expected) in cases {
        let mut retry = OutboundRetry::default();
        let mut end = None;
        for (index, publishes) in passes.iter().enumerate() {
            let mut outcome = OutboundOutcome::default();
            for (facet, error) in publishes {
                match error {
                    Some(error) => outcome.failed(*facet, &error()),
                    None => outcome.settled(*facet),
                }
            }
            let pass = if index == 0 {
                OutboundPass::Scheduled
            } else {
                OutboundPass::Retry
            };
            let ended = retry.record(pass, outcome, Instant::now());
            assert!(
                ended.is_none() || index + 1 == passes.len(),
                "{name}: the episode ended at pass {index}: {ended:?}"
            );
            end = ended;
        }
        assert_eq!(
            end.map(|end| end.abandoned),
            expected,
            "{name}: the episode ended wrongly"
        );
    }
}
