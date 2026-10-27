use super::*;

fn page(namespaces: &[&str], cgroups: &[&str], last: bool) -> ListOrphanCandidatesResponse {
    ListOrphanCandidatesResponse {
        orphaned_namespaces: namespaces.iter().map(ToString::to_string).collect(),
        orphaned_cgroups: cgroups.iter().map(ToString::to_string).collect(),
        next_page_token: if last { String::new() } else { "walk:1".into() },
        complete: last,
        ..Default::default()
    }
}

fn walk(observer: &mut Observer, pages: Vec<ListOrphanCandidatesResponse>) -> WalkSummary {
    let mut summary = None;
    for page in pages {
        assert!(summary.is_none(), "a walk ends with its last page");
        summary = observer.record_page(page);
    }
    summary.expect("the last page ends the walk")
}

/// `(candidates, persistent, new_persistent)` of a walk.
fn counts(summary: &WalkSummary) -> (usize, usize, usize) {
    (
        summary.candidates,
        summary.persistent,
        summary.new_persistent,
    )
}

/// A walk's pages make one summary. A candidate becomes newly persistent
/// once, in the first complete walk after the one that first found it: a
/// single sighting can be a sandbox created or torn down mid-walk.
#[test]
fn walk_finds_a_candidate_newly_persistent_once_it_outlasts_a_walk() {
    let mut observer = Observer::default();
    let cgroup = "/sys/fs/cgroup/space/sbx-00000000-0000-4000-8000-000000000001";

    let first = walk(
        &mut observer,
        vec![page(&["sns-7"], &[], false), page(&[], &[cgroup], true)],
    );
    let second = walk(
        &mut observer,
        vec![page(&["sns-7"], &[], false), page(&[], &[cgroup], true)],
    );
    let third = walk(&mut observer, vec![page(&["sns-7"], &[cgroup], true)]);
    let fourth = walk(&mut observer, vec![page(&[], &[], true)]);
    let fifth = walk(&mut observer, vec![page(&["sns-7"], &[], true)]);
    let sixth = walk(&mut observer, vec![page(&["sns-7"], &[], true)]);

    assert_eq!(counts(&first), (2, 0, 0));
    assert_eq!(counts(&second), (2, 2, 2));
    assert_eq!(counts(&third), (2, 2, 0));
    assert_eq!(counts(&fourth), (0, 0, 0));
    assert_eq!(counts(&fifth), (1, 0, 0), "gone and back is a new sighting");
    assert_eq!(counts(&sixth), (1, 1, 1));
    assert_eq!(sixth.samples, ["sns-7"]);
}

/// A walk that could not read every source does not count as a sighting
/// either way: a candidate it missed may still be there.
#[test]
fn incomplete_walk_does_not_change_persistence() {
    let mut observer = Observer::default();
    let mut unreadable = page(&[], &[], true);
    unreadable.complete = false;
    unreadable.warnings = vec!["Cgroups: permission denied".into()];

    let first = walk(&mut observer, vec![page(&["sns-7"], &[], true)]);
    let incomplete = walk(&mut observer, vec![unreadable]);
    let third = walk(&mut observer, vec![page(&["sns-7"], &[], true)]);

    assert_eq!(counts(&first), (1, 0, 0));
    assert!(!incomplete.complete);
    assert_eq!(incomplete.warnings, ["Cgroups: permission denied"]);
    assert_eq!(counts(&third), (1, 1, 1));
}

/// Memory stays bounded however many candidates a node has. Candidates past
/// the bound are counted, the tracked ones are still judged, and the walk
/// still read every source.
#[test]
fn walk_tracks_a_bounded_number_of_candidates() {
    let mut observer = Observer::default();
    let names: Vec<String> = (0..MAX_TRACKED + 5).map(|i| format!("sns-{i}")).collect();
    let pages = |observer: &mut Observer| {
        let mut pages: Vec<_> = names
            .chunks(256)
            .map(|chunk| {
                let chunk: Vec<&str> = chunk.iter().map(String::as_str).collect();
                page(&chunk, &[], false)
            })
            .collect();
        pages.push(page(&[], &[], true));
        walk(observer, pages)
    };

    let first = pages(&mut observer);
    let second = pages(&mut observer);

    assert_eq!((first.candidates, first.untracked), (MAX_TRACKED + 5, 5));
    assert!(first.complete);
    assert_eq!(observer.previous.len(), MAX_TRACKED);
    assert_eq!(counts(&second), (MAX_TRACKED + 5, MAX_TRACKED, MAX_TRACKED));
    assert_eq!(second.samples.len(), MAX_SAMPLES);
}
