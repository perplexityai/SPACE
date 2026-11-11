use super::*;

/// Ids at the edges of the layout: the largest id below the tag, the highest
/// position id, and the last id below the first block without a position.
fn layout_edges() -> [u64; 3] {
    [
        PROCESS_EVENT_ID_TAG - 1,
        ProcessEventId::Position {
            position: PROCESS_EVENT_ID_MAX_POSITION,
            frame: PROCESS_EVENT_ID_MAX_FRAME,
        }
        .encode(),
        ProcessEventId::Unknown { block: 0, frame: 0 }.encode() - 1,
    ]
}

/// A fresh attach replays the guest journal from its start, and a resume
/// continues from the output position its id carries. An id without one,
/// including every id an earlier data plane issued, also replays from the
/// start, behind `resync_required`.
#[test]
fn process_sse_resume_starts_at_the_journal_position_its_id_carries() {
    let fresh = ProcessReplayState::load(None).unwrap();
    assert_eq!(fresh.event_id, ProcessEventId::START);
    assert_eq!(fresh.backend_output_cursor, 0);
    assert_eq!(fresh.resync, None);

    for carried in [
        ProcessEventId::Position {
            position: 3,
            frame: 2,
        },
        ProcessEventId::Position {
            position: PROCESS_EVENT_ID_MAX_POSITION,
            frame: PROCESS_EVENT_ID_MAX_FRAME,
        },
    ] {
        let resumed = ProcessReplayState::load(Some(carried.encode())).unwrap();
        assert_eq!(resumed.event_id, carried);
        let ProcessEventId::Position { position, .. } = carried else {
            unreachable!()
        };
        assert_eq!(resumed.backend_output_cursor, position);
        assert_eq!(resumed.resync, None);
    }

    // Earlier data planes count frames from 1, and one that counted on from a
    // tagged id fails its guard. Whatever the id, every id the replay sends
    // is above it.
    let last_accepted_block = ProcessEventId::Unknown {
        block: PROCESS_EVENT_ID_MAX_RESUME_BLOCK - 1,
        frame: PROCESS_EVENT_ID_MAX_FRAME,
    }
    .encode();
    for last_event_id in [
        0,
        41,
        layout_edges()[0],
        layout_edges()[1] + 1,
        layout_edges()[2],
        ProcessEventId::Position {
            position: 3,
            frame: 2,
        }
        .encode()
            + 1,
        ProcessEventId::Unknown { block: 4, frame: 1 }.encode(),
        ProcessEventId::Unknown { block: 4, frame: 1 }.encode() + 1,
        last_accepted_block,
        last_accepted_block + (1 << PROCESS_EVENT_ID_GUARD_BITS) - 1,
    ] {
        let replay = ProcessReplayState::load(Some(last_event_id)).unwrap();
        assert_eq!(replay.backend_output_cursor, 0, "{last_event_id}");
        assert_eq!(
            replay.resync,
            Some(DataPlaneResyncReason::CursorReplayUnavailable),
            "{last_event_id}"
        );
        assert!(replay.event_id.encode() > last_event_id, "{last_event_id}");
        let mut id = replay.event_id;
        for position in [Some(0), Some(1), None, Some(2)] {
            id = id.next(position);
            assert!(id.encode() > last_event_id, "{last_event_id}");
        }
    }

    // An id this close to the end, or past it, leaves a stream too few ids
    // to send.
    for forged in [
        ProcessEventId::Unknown {
            block: PROCESS_EVENT_ID_MAX_RESUME_BLOCK,
            frame: 0,
        }
        .encode(),
        (1 << 53) - 1,
        1 << 53,
        1 << 62,
        MAX_LAST_EVENT_ID,
    ] {
        assert!(ProcessReplayState::load(Some(forged)).is_none(), "{forged}");
    }
}

/// Process SSE ids increase on every frame, each decodes to the position the
/// client has received output up to, and once a stream loses that position no
/// later id of it carries one, however many frames follow.
#[test]
fn process_event_ids_increase_and_carry_the_output_position() {
    let mut previous = ProcessEventId::START;
    let mut lost = false;
    let positions = [Some(0), Some(0), Some(1), Some(2)]
        .into_iter()
        .chain(std::iter::repeat_n(
            Some(2),
            PROCESS_EVENT_ID_MAX_FRAME as usize + 1,
        ))
        .chain([Some(3), None, Some(4)])
        .chain(std::iter::repeat_n(
            None,
            PROCESS_EVENT_ID_MAX_FRAME as usize + 1,
        ));
    for position in positions {
        let id = previous.next(position);
        assert!(id.encode() > previous.encode(), "{id:?} after {previous:?}");
        assert!(id.encode() < 1 << 53);
        assert_eq!(ProcessEventId::decode(id.encode()), Some(id));
        lost |= !matches!(id, ProcessEventId::Position { .. });
        match id {
            ProcessEventId::Position {
                position: carried, ..
            } => {
                assert!(!lost, "{id:?} after the position was lost");
                assert_eq!(Some(carried), position);
            }
            ProcessEventId::Unknown { .. } => assert!(lost),
        }
        previous = id;
    }
    assert!(lost, "running out of frames at one position loses it");

    for (from, position) in [
        // A position the field cannot hold, and one behind the last.
        (
            ProcessEventId::START,
            Some(PROCESS_EVENT_ID_MAX_POSITION + 1),
        ),
        (
            ProcessEventId::Position {
                position: 5,
                frame: 0,
            },
            Some(4),
        ),
        // The highest position, out of frames.
        (
            ProcessEventId::Position {
                position: PROCESS_EVENT_ID_MAX_POSITION,
                frame: PROCESS_EVENT_ID_MAX_FRAME,
            },
            Some(PROCESS_EVENT_ID_MAX_POSITION),
        ),
    ] {
        let id = from.next(position);
        assert!(matches!(id, ProcessEventId::Unknown { .. }), "{id:?}");
        assert!(id.encode() > from.encode());
    }

    // Each edge sits where the layout puts it: the first tagged id is the
    // start, and the block without a position follows the highest position.
    assert_eq!(layout_edges()[0] + 1, ProcessEventId::START.encode());
    assert_eq!(
        ProcessEventId::decode(layout_edges()[2] + 1),
        Some(ProcessEventId::Unknown { block: 0, frame: 0 })
    );
    assert!(layout_edges()[1] < layout_edges()[2]);
    let highest = ProcessEventId::Unknown {
        block: PROCESS_EVENT_ID_MAX_POSITION,
        frame: PROCESS_EVENT_ID_MAX_FRAME,
    };
    assert_eq!(
        highest.encode(),
        (1 << 53) - (1 << PROCESS_EVENT_ID_GUARD_BITS)
    );
    assert_eq!(ProcessEventId::decode(highest.encode()), Some(highest));

    // An id off by less than the guard, as `+1` counting leaves it, carries
    // no position; neither does one off the layout.
    let id = ProcessEventId::Position {
        position: 7,
        frame: 3,
    }
    .encode();
    for mangled in [
        id + 1,
        id - 1,
        id + 96,
        id - 512,
        id + (1 << PROCESS_EVENT_ID_GUARD_BITS) - 1,
        id | 1 << 53,
        id & !PROCESS_EVENT_ID_TAG,
    ] {
        assert_eq!(ProcessEventId::decode(mangled), None, "{mangled}");
    }

    // A double, as a JavaScript `Number`, holds every id exactly and prints it
    // back unchanged.
    for id in [
        ProcessEventId::START.encode(),
        id,
        layout_edges()[1],
        highest.encode(),
    ] {
        assert_eq!((id as f64).to_string(), id.to_string());
    }
}

/// Ids carry the output position for a process's first 2^33 - 1 outputs, as
/// the API docs publish, and none after; the id the docs show for the first
/// output decodes to it.
#[test]
fn process_event_ids_carry_positions_up_to_the_published_limit() {
    let last = (1 << 33) - 1;
    let id = ProcessEventId::START.next(Some(last));
    assert_eq!(
        id,
        ProcessEventId::Position {
            position: last,
            frame: 0
        }
    );
    assert_eq!(ProcessEventId::decode(id.encode()), Some(id));
    assert!(matches!(
        id.next(Some(last + 1)),
        ProcessEventId::Unknown { .. }
    ));

    assert_eq!(
        ProcessEventId::decode(4_503_599_627_632_640),
        Some(ProcessEventId::Position {
            position: 1,
            frame: 0
        })
    );
}
