use std::time::Duration;

pub(crate) fn duration_from_millis(milliseconds: u64) -> prost_types::Duration {
    Duration::from_millis(milliseconds)
        .try_into()
        .expect("fixture duration fits the wire range")
}
