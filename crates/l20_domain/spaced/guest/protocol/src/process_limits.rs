//! Predecode budgets for process request payloads, separate from live processes.
//! Start's repeated argv/environment fields amplify decoded heap; input can wait
//! on a blocked stdin writer. Overload rejects without decoding or applying work.
pub const PAYLOAD_SLOTS: usize = 8;
pub const START_PAYLOAD_SLOTS: u32 = 4;
pub const PAYLOAD_METHODS: &[(&str, u32)] = &[("Start", START_PAYLOAD_SLOTS), ("SendInput", 1)];
pub const MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
