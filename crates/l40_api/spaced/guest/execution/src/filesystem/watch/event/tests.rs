use pplx_space_spaced_guest_protocol::proto::bridge_guest::FileEventKind;

use super::*;

#[test]
fn watch_event_converts_to_proto() {
    let event = WatchEvent::new(
        PathBuf::from("/tmp/file.txt"),
        PathBuf::from("file.txt"),
        FileEventKind::Modified,
        42,
    )
    .with_is_regular_file(Some(true));
    let proto = event.to_proto();

    assert_eq!(proto.path, "file.txt");
    assert_eq!(event.absolute_path(), Path::new("/tmp/file.txt"));
    assert_eq!(proto.kind(), FileEventKind::Modified);
    assert_eq!(proto.observed_at, Some(unix_ms_to_timestamp(42)));
    assert_eq!(event.is_regular_file(), Some(true));
}
