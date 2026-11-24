use super::*;

const SANDBOX: &str = "018f0000-0000-7000-8000-000000000001";

fn events(cgroup: &Path, kills: u64, group_kills: u64) {
    fs::write(
        cgroup.join("memory.events"),
        format!("max 9\noom 6\noom_kill {kills}\noom_group_kill {group_kills}\n"),
    )
    .unwrap();
}

fn counts(kills: u64, group_kills: u64) -> Counts {
    Counts { kills, group_kills }
}

#[test]
fn tracks_oom_deltas_across_cgroup_recreation_and_removal() {
    let root = tempfile::tempdir().unwrap();
    let parent = root.path();
    let vm = parent.join(SANDBOX);
    let sidecar = parent.join(format!("sbx-{SANDBOX}"));
    fs::create_dir(&vm).unwrap();
    fs::create_dir(&sidecar).unwrap();
    events(parent, 5, 1);
    events(&vm, 2, 0);
    fs::write(sidecar.join("memory.events"), "oom_kill 0\n").unwrap();

    let mut sampler = Sampler::default();
    assert_eq!(sampler.sample(parent), Deltas::default());

    events(parent, 8, 2);
    events(&vm, 4, 1);
    fs::write(sidecar.join("memory.events"), "oom_kill 1\n").unwrap();
    assert_eq!(
        sampler.sample(parent),
        Deltas {
            parent: counts(3, 1),
            vm: counts(2, 1),
            sidecar: counts(1, 0),
        }
    );
    assert_eq!(sampler.sample(parent), Deltas::default());

    // A final unsampled VM kill survives removal in the parent total.
    fs::rename(&vm, parent.join("removed-vm")).unwrap();
    events(parent, 9, 2);
    assert_eq!(
        sampler.sample(parent),
        Deltas {
            parent: counts(1, 0),
            ..Deltas::default()
        }
    );

    fs::create_dir(&vm).unwrap();
    events(&vm, 1, 0);
    events(parent, 10, 2);
    assert_eq!(sampler.sample(parent).vm, counts(1, 0));

    // A same-path recreation entirely between polls is not a counter increment.
    fs::rename(&vm, parent.join("replaced-vm")).unwrap();
    fs::create_dir(&vm).unwrap();
    events(&vm, 3, 1);
    events(parent, 13, 3);
    assert_eq!(sampler.sample(parent).vm, counts(3, 1));
}

#[test]
fn preserves_baselines_on_read_failure_and_accepts_counter_reset() {
    let root = tempfile::tempdir().unwrap();
    let parent = root.path();
    let vm = parent.join(SANDBOX);
    fs::create_dir(&vm).unwrap();
    events(parent, 10, 3);
    events(&vm, 10, 3);

    let mut sampler = Sampler::default();
    assert_eq!(sampler.sample(parent), Deltas::default());
    fs::write(parent.join("memory.events"), "oom_kill broken\n").unwrap();
    fs::write(vm.join("memory.events"), "oom_kill broken\n").unwrap();
    assert_eq!(sampler.sample(parent), Deltas::default());

    events(parent, 11, 4);
    events(&vm, 11, 4);
    let delta = sampler.sample(parent);
    assert_eq!(delta.parent, counts(1, 1));
    assert_eq!(delta.vm, counts(1, 1));

    events(parent, 2, 1);
    events(&vm, 2, 1);
    let delta = sampler.sample(parent);
    assert_eq!(delta.parent, counts(2, 1));
    assert_eq!(delta.vm, counts(2, 1));
    assert_eq!(sampler.sample(parent), Deltas::default());
}
