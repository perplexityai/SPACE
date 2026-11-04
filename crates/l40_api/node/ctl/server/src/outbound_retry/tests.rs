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

/// Where a pass that fails facets with ClusterApi unavailable leaves the
/// pending retry. One that fails every facet still failing replaces it with
/// the next step, whenever it was due. One that fails only some of them can
/// bring it earlier but never puts it off.
#[test]
fn only_a_pass_that_fails_every_failing_facet_puts_the_retry_off() {
    use FacetKind::{NodeStatus, SandboxInventory, TemplateInventory};
    const ALL: &[FacetKind] = &[SandboxInventory, TemplateInventory, NodeStatus];
    // The facets failing before the pass, when their retry is pending in
    // seconds (`None`: none pending), the facets the pass fails, and when the
    // retry is due after it: one failed pass before, so the step is 2 s.
    type Case = (
        &'static str,
        &'static [FacetKind],
        Option<f64>,
        &'static [FacetKind],
        (f64, f64),
    );
    let cases: [Case; 7] = [
        (
            "partial, a sooner retry pending",
            ALL,
            Some(0.1),
            &[NodeStatus],
            (0.1, 0.1),
        ),
        (
            "partial, a later retry pending",
            ALL,
            Some(100.0),
            &[NodeStatus],
            (1.0, 2.0),
        ),
        (
            "partial, no retry pending",
            ALL,
            None,
            &[NodeStatus],
            (1.0, 2.0),
        ),
        (
            "every facet, a sooner retry pending",
            ALL,
            Some(0.1),
            ALL,
            (1.0, 2.0),
        ),
        (
            "every facet, a later retry pending",
            ALL,
            Some(100.0),
            ALL,
            (1.0, 2.0),
        ),
        (
            "every facet and a new one",
            &[SandboxInventory],
            Some(0.1),
            &[SandboxInventory, NodeStatus],
            (1.0, 2.0),
        ),
        (
            "only a new facet",
            &[SandboxInventory],
            Some(0.1),
            &[NodeStatus],
            (0.1, 0.1),
        ),
    ];
    for (name, failing, pending, fails, (low, high)) in cases {
        let now = Instant::now();
        let mut retry = OutboundRetry {
            failing: failing.iter().copied().collect(),
            failed_passes: 1,
            due: pending.map(|secs| now + Duration::from_secs_f64(secs)),
            ..OutboundRetry::default()
        };
        let mut outcome = OutboundOutcome::default();
        for facet in fails {
            outcome.failed(*facet, &unavailable());
        }
        assert_eq!(retry.record(OutboundPass::Scheduled, outcome, now), None);
        let due = retry
            .due
            .map(|due| (due - now).as_secs_f64())
            .expect("a retry is pending");
        assert!(
            (low - 1e-6..=high + 1e-6).contains(&due),
            "{name}: the retry is due in {due:.3} s, outside [{low}, {high}] s"
        );
        let expected = failing
            .iter()
            .chain(fails)
            .copied()
            .collect::<BTreeSet<_>>();
        assert_eq!(retry.failing, expected, "{name}: the facets to retry");
    }
}
