//! Bounded tails of log files, read in fixed working memory: one read chunk,
//! one line, and the kept output, whatever the file's line structure.

use std::collections::VecDeque;
use std::io::Read;

const READ_CHUNK_BYTES: usize = 4 * 1024;

/// What a tail keeps of a log file: the newest whole lines, up to
/// `keep_bytes`, found in its last `scan_bytes`, skipping lines `noise`
/// accepts. Scanning more than is kept stops periodic noise from crowding out
/// the events around it.
#[derive(Clone, Copy)]
pub(super) struct TailWindow {
    pub(super) scan_bytes: u64,
    pub(super) keep_bytes: usize,
    pub(super) noise: fn(&mut NoiseFilter, &str) -> bool,
}

/// Per-tail state for a [`TailWindow::noise`] filter that pairs lines.
#[derive(Default)]
pub(super) struct NoiseFilter {
    skipped_request: bool,
}

/// Firecracker logs every API request and its response at `Info`. The
/// watchdog reads VM state (`GET /`) on every probe, so without this a tail
/// is little but those pairs. Any other request, such as a pause, is kept
/// with its response.
pub(super) fn firecracker_state_poll(filter: &mut NoiseFilter, line: &str) -> bool {
    let follows_skipped_request = std::mem::take(&mut filter.skipped_request);
    if line.contains(":fc_api] The API server received a Get request on \"/\".") {
        filter.skipped_request = true;
        return true;
    }
    follows_skipped_request && line.contains(":fc_api] The request was executed successfully.")
}

pub(super) fn no_noise(_: &mut NoiseFilter, _: &str) -> bool {
    false
}

/// The working memory of one tail, allocated once at its window's size.
pub(super) struct TailBuffers {
    chunk: Box<[u8; READ_CHUNK_BYTES]>,
    /// The current line's raw bytes, up to `keep_bytes`; a longer line is
    /// never kept, so its excess is not buffered.
    line: Vec<u8>,
    /// The current line after sanitizing, which never lengthens it.
    clean: String,
    /// Kept sanitized lines, oldest first. Every line but a final unterminated
    /// one ends in `\n`, so line boundaries need no separate index.
    kept: VecDeque<u8>,
}

impl TailBuffers {
    pub(super) fn new(window: TailWindow) -> Self {
        Self {
            chunk: Box::new([0; READ_CHUNK_BYTES]),
            line: Vec::with_capacity(window.keep_bytes),
            clean: String::with_capacity(window.keep_bytes),
            kept: VecDeque::with_capacity(window.keep_bytes),
        }
    }

    #[cfg(test)]
    pub(super) fn capacities(&self) -> [usize; 3] {
        [
            self.line.capacity(),
            self.clean.capacity(),
            self.kept.capacity(),
        ]
    }
}

/// The newest lines `reader` yields within `window`, read through `buffers`.
/// `starts_mid_line` drops the first, partial line of a scan that starts
/// mid-file. A line longer than `keep_bytes` is never kept, nor is anything
/// older. Invalid UTF-8 and control characters other than newlines and tabs
/// become `?`, which never lengthens the text.
pub(super) fn tail_lines(
    reader: impl Read,
    starts_mid_line: bool,
    window: TailWindow,
    buffers: &mut TailBuffers,
) -> std::io::Result<String> {
    let TailBuffers {
        chunk,
        line,
        clean,
        kept,
    } = buffers;
    line.clear();
    kept.clear();
    let mut reader = reader.take(window.scan_bytes);
    let mut filter = NoiseFilter::default();
    let mut skipping_partial_line = starts_mid_line;
    let mut line_too_long = false;
    loop {
        let read = match reader.read(&mut chunk[..]) {
            Ok(0) => break,
            Ok(read) => read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        let mut rest = chunk.get(..read).unwrap_or_default();
        while !rest.is_empty() {
            let line_end = rest.iter().position(|byte| *byte == b'\n');
            let (piece, after) = rest.split_at(line_end.map_or(rest.len(), |newline| newline + 1));
            rest = after;
            let ends_line = line_end.is_some();
            if skipping_partial_line {
                skipping_partial_line = !ends_line;
                continue;
            }
            let room = window.keep_bytes.saturating_sub(line.len());
            let fits = piece.get(..room).unwrap_or(piece);
            line_too_long |= fits.len() < piece.len();
            line.extend_from_slice(fits);
            if ends_line {
                finish_line(line, line_too_long, clean, kept, &mut filter, window);
                line_too_long = false;
            }
        }
    }
    if !line.is_empty() || line_too_long {
        finish_line(line, line_too_long, clean, kept, &mut filter, window);
    }
    Ok(String::from_utf8_lossy(kept.make_contiguous()).into_owned())
}

fn finish_line(
    line: &mut Vec<u8>,
    too_long: bool,
    clean: &mut String,
    kept: &mut VecDeque<u8>,
    filter: &mut NoiseFilter,
    window: TailWindow,
) {
    clean.clear();
    for chunk in line.utf8_chunks() {
        clean.extend(chunk.valid().chars().map(|character| {
            if character == char::REPLACEMENT_CHARACTER
                || (character.is_control() && character != '\n' && character != '\t')
            {
                '?'
            } else {
                character
            }
        }));
        if !chunk.invalid().is_empty() {
            clean.push('?');
        }
    }
    line.clear();
    if (window.noise)(filter, clean) {
        return;
    }
    if too_long {
        kept.clear();
        return;
    }
    while kept.len() + clean.len() > window.keep_bytes && !kept.is_empty() {
        while let Some(byte) = kept.pop_front() {
            if byte == b'\n' {
                break;
            }
        }
    }
    kept.extend(clean.as_bytes());
}

#[cfg(test)]
mod tests;
