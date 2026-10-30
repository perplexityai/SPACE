//! How a pair's items are summarized in its findings; the scan itself is
//! tested in `scan`.

use super::{Reached, reached_text};

/// A finding lists the modules reached, most items first, and the items
/// under `proto` apart.
#[test]
fn a_pairs_summary() {
    let mut reached = Reached::default();
    for (item, line) in [
        ("b::Y", 3),
        ("proto::Request", 2),
        ("b::Z", 4),
        ("a::X", 5),
        ("proto::Response", 1),
        ("c", 6),
        ("d::*", 7),
        ("*", 8),
        ("e::W", 9),
    ] {
        reached
            .items
            .insert(item.to_owned(), ("crates/x/src/lib.rs".to_owned(), line));
    }
    assert_eq!(reached.count(), 9);
    assert_eq!(reached.proto(), 2);
    assert_eq!(
        reached_text("pplx-space-lib", &reached),
        "9 items inside pplx-space-lib's modules (b 2, proto 2, * (a root glob) 1, a 1, c 1 \
         and 2 more), 2 of them under proto"
    );
}
