pub(crate) fn timestamp_from_millis(milliseconds: u64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: (milliseconds / 1_000) as i64,
        nanos: ((milliseconds % 1_000) * 1_000_000) as i32,
    }
}
