//! Per-daemon payload budgets, independent of worker and retry-record counts.
// Eight requests leave room for decoder/transport buffers alongside the 4 MiB
// public message cap. Admission is held through execution cleanup, not just
// through the caller's lifetime. Metadata/watch/process RPCs use other budgets.
pub const PAYLOAD_SLOTS: usize = 8;
// Repeated protobuf entries amplify decoded heap beyond wire bytes. Charge a
// batch four slots (at most two batches), including during decoding and cleanup.
pub const BATCH_REQUEST_WEIGHT: u32 = 4;
pub const PAYLOAD_METHODS: &[(&str, u32)] = &[
    ("Write", 1),
    ("WriteBatch", BATCH_REQUEST_WEIGHT),
    ("Rename", 1),
];
pub const MAX_MESSAGE_BYTES: usize = pplx_space_common_core::spaced::MAX_MESSAGE_BYTES;
pub const WRITE_CHUNK_BYTES: usize = 64 * 1024;
pub const WRITE_BUFFER_FRAMES: usize = 2;
