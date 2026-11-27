use pplx_space_node_runtime_core::cgroup_memory::CgroupMemoryEvents;

use super::new_events;

#[test]
fn new_events_count_growth_and_rebaseline_after_a_reset() {
    let events = |max, oom, oom_kill| CgroupMemoryEvents {
        max,
        oom,
        oom_kill,
        ..CgroupMemoryEvents::default()
    };

    assert_eq!(
        new_events(&events(7, 1, 1), &events(10, 2, 2)),
        [("max", 3), ("oom", 1), ("oom_kill", 1)]
    );
    assert_eq!(new_events(&events(10, 2, 2), &events(10, 2, 2)), []);
    // A recreated cgroup restarts its counters below the previous reading.
    assert_eq!(new_events(&events(10, 2, 2), &events(1, 0, 0)), []);
    assert_eq!(new_events(&events(1, 0, 0), &events(2, 0, 0)), [("max", 1)]);
    assert_eq!(
        new_events(
            &CgroupMemoryEvents::default(),
            &CgroupMemoryEvents {
                low: 1,
                high: 2,
                oom_group_kill: 3,
                ..CgroupMemoryEvents::default()
            }
        ),
        [("low", 1), ("high", 2), ("oom_group_kill", 3)]
    );
}
