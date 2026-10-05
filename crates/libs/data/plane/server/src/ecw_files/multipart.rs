//! Streaming `multipart/form-data` parsing for `POST /files`: each `file` part
//! is spooled to disk while multer's parse buffer stays bounded.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};

use axum::{
    body::{Body, Bytes},
    http::{HeaderMap, StatusCode, header},
};
use futures_util::{Stream, stream};
use pplx_space_common_core::types::{GuestUserName, MAX_GUEST_PATH_BYTES};

use super::{
    E2B_FILE_CONTENT_LIMIT_BYTES, EcwProxyError, ResponseKind, ResponseOrigin, SpooledUpload,
    e2b_request_root, file_too_large_error,
};

/// Request bytes multer may hold in its parse buffer before feeding pauses.
/// Legitimate part headers need a few hundred bytes, so a body that fills the
/// buffer without forming a part is malformed. Multer rescans the buffer for a
/// header terminator on every poll, so this also caps the cost of each scan
/// when a client trickles header bytes in tiny frames.
pub(super) const E2B_MULTIPART_PARSE_BUFFER_LIMIT_BYTES: usize = 64 * 1024;
/// The CRLF that ends each part's data before the next boundary.
pub(super) const MULTIPART_PART_DATA_TERMINATOR_BYTES: usize = 2;
/// Parts per request, `file` or not. Each `file` part is staged in its own
/// spool file before any is written. The cap also bounds how far
/// `multipart_part_framing_lower_bound`'s undercount accumulates for typical
/// clients: two headers per part at two bytes each is 4,000 bytes over 1000
/// parts, well under the parse-buffer cap.
pub(super) const E2B_MULTIPART_PART_LIMIT: usize = 1000;

#[derive(Debug)]
pub(super) struct MultipartUploadPartE2b {
    pub(super) filename: Option<String>,
    pub(super) upload: tempfile::TempPath,
}

fn too_many_multipart_parts_error() -> EcwProxyError {
    EcwProxyError::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        format!("multipart uploads are limited to {E2B_MULTIPART_PART_LIMIT} parts per request"),
        ResponseOrigin::Client,
        ResponseKind::Validation,
    )
}

/// RFC 2046 limits a boundary to 70 characters. Multer formats and searches
/// for the boundary on every poll, so its length adds to the cost of each one.
const MULTIPART_BOUNDARY_MAX_LEN: usize = 70;

pub(super) fn multipart_boundary(content_type: &str) -> Option<&str> {
    content_type
        .split(';')
        .skip(1)
        .find_map(|part| part.trim().strip_prefix("boundary="))
        .map(|boundary| boundary.trim_matches('"'))
        .filter(|boundary| (1..=MULTIPART_BOUNDARY_MAX_LEN).contains(&boundary.len()))
}

/// `filenames_name_paths` is false when a `path` query names the file, in
/// which case part filenames are neither kept nor validated.
pub(super) async fn spool_multipart_uploads(
    body: Body,
    boundary: &str,
    filenames_name_paths: bool,
) -> Result<Vec<MultipartUploadPartE2b>, EcwProxyError> {
    spool_multipart_uploads_with_limit(
        body,
        boundary,
        E2B_FILE_CONTENT_LIMIT_BYTES,
        filenames_name_paths,
    )
    .await
}

pub(super) async fn spool_multipart_uploads_with_limit(
    body: Body,
    boundary: &str,
    limit: usize,
    filenames_name_paths: bool,
) -> Result<Vec<MultipartUploadPartE2b>, EcwProxyError> {
    // Capping the whole body, not each part, keeps the pre-streaming contract
    // and bounds the spool disk a single request can use across all its parts.
    let constraints =
        multer::Constraints::new().size_limit(multer::SizeLimit::new().whole_stream(limit as u64));
    let consumed_bytes = Arc::new(AtomicUsize::new(0));
    let mut multipart = multer::Multipart::with_constraints(
        throttled_multipart_body(body, Arc::clone(&consumed_bytes)),
        boundary,
        constraints,
    );
    let mut uploads = Vec::new();
    let mut part_count = 0usize;
    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(multipart_upload_error)?
    {
        part_count += 1;
        if part_count > E2B_MULTIPART_PART_LIMIT {
            return Err(too_many_multipart_parts_error());
        }
        consumed_bytes.fetch_add(
            multipart_part_framing_lower_bound(boundary, field.headers()),
            Ordering::Relaxed,
        );
        let disposition = ContentDispositionE2b::parse(field.headers())?;
        let is_file = disposition.name.as_deref() == Some("file");
        let filename = disposition
            .filename
            .filter(|filename| filenames_name_paths && is_file && !filename.is_empty());
        // The filename becomes a guest path later; reject an impossible one
        // before holding it and spooling its part.
        if filename
            .as_ref()
            .is_some_and(|filename| filename.len() > MAX_GUEST_PATH_BYTES)
        {
            return Err(EcwProxyError::bad_request(format!(
                "multipart filename exceeds {MAX_GUEST_PATH_BYTES} bytes"
            )));
        }
        // Both are known from the part headers, so reject before spooling.
        if is_file && filenames_name_paths && filename.is_none() {
            return Err(EcwProxyError::bad_request(
                "multipart uploads require a filename or path query",
            ));
        }
        if is_file && !filenames_name_paths && !uploads.is_empty() {
            return Err(EcwProxyError::bad_request(
                "path query parameter only supports a single multipart upload",
            ));
        }
        let mut upload = if is_file {
            Some(SpooledUpload::new().await?)
        } else {
            None
        };
        while let Some(chunk) = field.chunk().await.map_err(multipart_upload_error)? {
            consumed_bytes.fetch_add(chunk.len(), Ordering::Relaxed);
            // The whole-stream cap also bounds each part, so this check in
            // `write_chunk` never trips here; octet-stream uploads rely on it.
            if let Some(upload) = upload.as_mut() {
                upload.write_chunk(&chunk).await?;
            }
        }
        // The CRLF before the next boundary. Counting it here also means every
        // field start, non-empty chunk, and field end advances the total, which
        // `throttled_multipart_body` reads as parser progress.
        consumed_bytes.fetch_add(MULTIPART_PART_DATA_TERMINATOR_BYTES, Ordering::Relaxed);
        if let Some(upload) = upload {
            let upload = upload.finish().await?;
            uploads.push(MultipartUploadPartE2b { filename, upload });
        }
    }
    Ok(uploads)
}

/// The `name` and `filename` of a part's Content-Disposition, parsed from the
/// raw header bytes. Multer 3.1.0's accessors match `name` and `filename`
/// wherever they follow whitespace, including inside another parameter's
/// quoted value (`filename="a name=b.txt"; name="file"`), so they are not used.
#[derive(Debug, Default, PartialEq)]
pub(super) struct ContentDispositionE2b {
    pub(super) name: Option<String>,
    pub(super) filename: Option<String>,
}

impl ContentDispositionE2b {
    /// Splits parameters on `;` outside quoted values. Clients escape quoted
    /// values differently: browsers, urllib3, curl, and undici send `"` as
    /// `%22` and backslashes as themselves; httpx doubles backslashes and sends
    /// `"` as `%22`; Go writes `\\` and `\"`. So `\\` and `\"` are read as
    /// escapes, except a `\"` that ends the value, which is a literal backslash
    /// before the closing quote (a browser filename ending in `\`). This reads
    /// httpx and Go filenames, and browser filenames without backslash runs,
    /// as sent. Two inputs stay ambiguous: a run of raw backslashes from a
    /// browser-style client collapses (`a\\b` reads as `a\b`), and a Go
    /// filename with `"` directly before `;` ends at that quote or is
    /// rejected. Other backslashes, such as Windows path separators, stay
    /// literal. The first occurrence of a parameter wins. Unbalanced quotes,
    /// a quote inside an unquoted value, and non-UTF-8 values reject the part
    /// instead of letting stray quotes reach the write path or a file part go
    /// unnamed.
    pub(super) fn parse(headers: &HeaderMap) -> Result<Self, EcwProxyError> {
        let value = headers
            .get(header::CONTENT_DISPOSITION)
            .ok_or_else(|| EcwProxyError::bad_request("Missing multipart Content-Disposition"))?
            .as_bytes();
        let malformed = || EcwProxyError::bad_request("Invalid multipart Content-Disposition");
        let mut params = Vec::new();
        let (mut start, mut index, mut in_quotes) = (0, 0, false);
        while let Some(&byte) = value.get(index) {
            match byte {
                b'\\' if in_quotes && is_escape(value, index) => index += 1,
                b'"' => in_quotes = !in_quotes,
                b';' if !in_quotes => {
                    params.extend(value.get(start..index));
                    start = index + 1;
                }
                _ => {}
            }
            index += 1;
        }
        if in_quotes {
            return Err(malformed());
        }
        params.extend(value.get(start..));

        let mut disposition = Self::default();
        // The first segment is the disposition type, e.g. `form-data`.
        for param in params.into_iter().skip(1) {
            let Some(separator) = param.iter().position(|&byte| byte == b'=') else {
                continue;
            };
            let key = param.get(..separator).unwrap_or_default().trim_ascii();
            let slot = if key.eq_ignore_ascii_case(b"name") {
                &mut disposition.name
            } else if key.eq_ignore_ascii_case(b"filename") {
                &mut disposition.filename
            } else {
                continue;
            };
            let raw = param.get(separator + 1..).unwrap_or_default().trim_ascii();
            let bytes = match raw.strip_prefix(b"\"") {
                Some(rest) => {
                    let inner = rest.strip_suffix(b"\"").ok_or_else(malformed)?;
                    // The closing quote is already stripped, so a trailing
                    // backslash here is the literal one `is_escape` left alone.
                    let mut unescaped = Vec::with_capacity(inner.len());
                    let mut bytes = inner.iter().peekable();
                    while let Some(&byte) = bytes.next() {
                        match (byte, bytes.peek()) {
                            (b'\\', Some(&&escaped @ (b'\\' | b'"'))) => {
                                bytes.next();
                                unescaped.push(escaped);
                            }
                            (b'"', _) => return Err(malformed()),
                            _ => unescaped.push(byte),
                        }
                    }
                    unescaped
                }
                None if raw.contains(&b'"') => return Err(malformed()),
                None => raw.to_vec(),
            };
            let text = String::from_utf8(bytes).map_err(|_| malformed())?;
            slot.get_or_insert(text);
        }
        Ok(disposition)
    }
}

/// Whether the backslash at `index`, inside a quoted value, escapes the next
/// byte (see `ContentDispositionE2b::parse`).
fn is_escape(value: &[u8], index: usize) -> bool {
    match value.get(index + 1) {
        Some(b'\\') => true,
        Some(b'"') => !value
            .get(index + 2..)
            .unwrap_or_default()
            .iter()
            .find(|byte| !byte.is_ascii_whitespace())
            .is_none_or(|&byte| byte == b';'),
        _ => false,
    }
}

fn multipart_upload_error(error: multer::Error) -> EcwProxyError {
    if matches!(error, multer::Error::StreamSizeExceeded { .. }) {
        return file_too_large_error();
    }
    EcwProxyError::bad_request("Invalid multipart body")
}

/// Bytes multer consumes to open a part: `--{boundary}\r\n`, each header at
/// its shortest accepted form `name:value\n` (httparse also takes bare-LF
/// lines), and the blank line. Never more than the real count, so bytes fed
/// minus the running total never understates multer's buffer. A typical
/// `Name: value\r\n` header is undercounted by two bytes.
pub(super) fn multipart_part_framing_lower_bound(boundary: &str, headers: &HeaderMap) -> usize {
    headers
        .iter()
        .fold(boundary.len() + 6, |len, (name, value)| {
            len + name.as_str().len() + value.len() + 2
        })
}

/// Feeds the request body to multer while bytes fed minus `consumed_bytes`, an
/// upper bound on multer's parse buffer, stays within
/// `E2B_MULTIPART_PARSE_BUFFER_LIMIT_BYTES`; the buffer then holds at most the
/// cap plus one frame. Multer appends every ready frame, so past the cap this
/// yields once to let it parse what it holds. Without progress by the next poll,
/// the request fails: normally the buffered bytes cannot complete a part. The
/// bound leaves some consumed bytes uncounted (header whitespace, duplicate
/// headers multer drops, preamble, transport padding), so an encoding padded
/// until those exceed the cap is rejected too; typical clients stay far below
/// it (see `E2B_MULTIPART_PART_LIMIT`).
pub(super) fn throttled_multipart_body(
    body: Body,
    consumed_bytes: Arc<AtomicUsize>,
) -> impl Stream<Item = Result<Bytes, axum::Error>> + Send + 'static {
    let mut body = Box::pin(body.into_data_stream());
    let mut fed_bytes = 0usize;
    let mut throttled_at = None;
    stream::poll_fn(move |context| {
        let consumed = consumed_bytes.load(Ordering::Relaxed);
        if fed_bytes.saturating_sub(consumed) > E2B_MULTIPART_PARSE_BUFFER_LIMIT_BYTES {
            if throttled_at == Some(consumed) {
                return Poll::Ready(Some(Err(axum::Error::new(
                    "multipart headers or preamble exceed the parse buffer limit",
                ))));
            }
            throttled_at = Some(consumed);
            context.waker().wake_by_ref();
            return Poll::Pending;
        }
        throttled_at = None;
        let next = body.as_mut().poll_next(context);
        if let Poll::Ready(Some(Ok(data))) = &next {
            fed_bytes = fed_bytes.saturating_add(data.len());
        }
        next
    })
}

pub(super) fn resolve_multipart_uploads(
    uploads: Vec<MultipartUploadPartE2b>,
    query_path: Option<String>,
    user: Option<&GuestUserName>,
) -> Result<Vec<(String, tempfile::TempPath)>, EcwProxyError> {
    uploads
        .into_iter()
        .map(|upload| {
            // The path query wins over the part's filename. Spooling has
            // already rejected parts with neither.
            let path = query_path.clone().or(upload.filename).ok_or_else(|| {
                EcwProxyError::bad_request("multipart uploads require a filename or path query")
            })?;
            Ok((e2b_request_root(&path, user)?, upload.upload))
        })
        .collect()
}
