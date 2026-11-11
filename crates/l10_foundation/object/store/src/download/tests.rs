use super::*;
use bytes::Bytes;

#[test]
fn reorder_window_releases_items_in_order() {
    let mut window = ReorderWindow::new();
    window.insert(2, Bytes::from_static(b"cc"));
    window.insert(1, Bytes::from_static(b"bb"));
    assert_eq!(window.pop_ready(), None, "chunk 0 has not arrived");
    assert_eq!(window.pending_len(), 2);

    window.insert(0, Bytes::from_static(b"aa"));
    assert_eq!(window.pop_ready(), Some(Bytes::from_static(b"aa")));
    assert_eq!(window.pop_ready(), Some(Bytes::from_static(b"bb")));
    assert_eq!(window.pop_ready(), Some(Bytes::from_static(b"cc")));
    assert_eq!(window.pop_ready(), None);
    assert_eq!(window.next_index(), 3);
    assert_eq!(window.pending_len(), 0);
}

fn chunked_write_roundtrip(dir: &Path, chunk_size: usize, total: usize) {
    let dst = dir.join("out.bin");
    let payload: Vec<u8> = (0..total).map(|i| (i % 251) as u8).collect();

    let mut writer = SequentialWriter::open(&dst).expect("open writer");
    assert!(
        !dst.exists(),
        "destination must not exist until commit renames the scratch"
    );
    for chunk in payload.chunks(chunk_size) {
        writer.write_chunk(chunk).expect("write chunk");
    }
    assert_eq!(writer.written(), total as u64);
    let writer = writer.prepare_commit(total as u64).expect("prepare commit");
    assert!(
        !dst.exists(),
        "destination must not exist until commit renames the scratch"
    );
    writer.commit().expect("commit");

    let read_back = std::fs::read(&dst).expect("read back");
    assert_eq!(read_back.len(), total);
    assert_eq!(read_back, payload, "byte-exact roundtrip");
    assert_eq!(
        std::fs::read_dir(dir).expect("list dir").count(),
        1,
        "no scratch residue after commit"
    );
}

#[test]
fn sequential_writer_roundtrips_with_short_tail() {
    let dir = tempfile::tempdir().expect("tempdir");
    chunked_write_roundtrip(dir.path(), 256 * 1024, 3 * 256 * 1024 + 5000);
}

#[test]
fn sequential_writer_roundtrips_single_short_chunk() {
    let dir = tempfile::tempdir().expect("tempdir");
    chunked_write_roundtrip(dir.path(), 256 * 1024, 4097);
}

#[test]
fn prepare_commit_rejects_byte_count_mismatch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dst = dir.path().join("out.bin");
    let mut writer = SequentialWriter::open(&dst).expect("open writer");
    writer.write_chunk(&[7u8; 100]).expect("write final chunk");
    let err = writer.prepare_commit(200).expect_err("mismatch must fail");
    assert!(err.to_string().contains("expected 200"));
    assert!(!dst.exists(), "failed commit must not install the file");
}

#[test]
fn open_reclaims_orphan_scratch_files_from_prior_attempts() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dst = dir.path().join("out.bin");
    // Orphans a dead process left behind (Drop never ran): same
    // destination, scratch naming scheme, arbitrary pid/nanos/seq.
    let orphan_a = dir.path().join("out.bin.dl-999-111-0.tmp");
    let orphan_b = dir.path().join("out.bin.dl-1-2-3.tmp");
    // Must survive: another destination's scratch, a decompress scratch,
    // and a pre-existing destination file.
    let other_dst_scratch = dir.path().join("other.bin.dl-999-111-0.tmp");
    let decompress_scratch = dir.path().join("out.bin.abc.dec.tmp");
    for f in [
        &orphan_a,
        &orphan_b,
        &other_dst_scratch,
        &decompress_scratch,
        &dst,
    ] {
        std::fs::write(f, b"stale").expect("plant file");
    }

    let writer = SequentialWriter::open(&dst).expect("open writer");
    assert!(!orphan_a.exists(), "orphan scratch must be reclaimed");
    assert!(!orphan_b.exists(), "orphan scratch must be reclaimed");
    assert!(
        other_dst_scratch.exists(),
        "other destinations' scratches must survive"
    );
    assert!(
        decompress_scratch.exists(),
        "non-download scratches must survive"
    );
    assert!(
        dst.exists(),
        "the destination itself must survive the sweep"
    );
    drop(writer);
}

/// A transform that uppercases ASCII — enough to prove the transform
/// runs inline, that `written` counts *input* bytes, and that `finish`
/// output lands before commit.
struct UppercasingSink {
    inner: File,
}

impl std::io::Write for UppercasingSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let upper: Vec<u8> = buf.iter().map(u8::to_ascii_uppercase).collect();
        self.inner.write_all(&upper)?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl FinishingWrite for UppercasingSink {
    fn finish(mut self: Box<Self>) -> std::io::Result<bool> {
        // Trailing marker proves finish() output is included in the
        // committed file.
        self.inner.write_all(b"!")?;
        self.inner.flush()?;
        Ok(true)
    }
}

#[test]
fn transformed_writer_commits_transformed_bytes_and_counts_input() {
    let dir = tempfile::tempdir().unwrap();
    let dst = dir.path().join("decoded.bin");
    let (file, scratch) = open_scratch(&dst).unwrap();
    let sink = Box::new(UppercasingSink { inner: file });
    let mut writer = SequentialWriter::from_parts(sink, scratch, dst.clone(), None);
    writer.write_chunk(b"abc").unwrap();
    writer.write_chunk(b"def").unwrap();
    assert_eq!(writer.written(), 6, "written must count input bytes");
    let sealed = writer
        .prepare_commit(6)
        .expect("input byte count must match");
    assert!(sealed.transformed(), "sink reported the stream transformed");
    sealed.commit().unwrap();
    assert_eq!(std::fs::read(&dst).unwrap(), b"ABCDEF!");
}

#[test]
fn transformed_writer_rejects_input_count_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let dst = dir.path().join("decoded.bin");
    let (file, scratch) = open_scratch(&dst).unwrap();
    let sink = Box::new(UppercasingSink { inner: file });
    let mut writer = SequentialWriter::from_parts(sink, scratch, dst.clone(), None);
    writer.write_chunk(b"abc").unwrap();
    // Output inflation must not satisfy the *input* byte-count check.
    writer
        .prepare_commit(4)
        .expect_err("expected-vs-input mismatch must fail");
    assert!(!dst.exists(), "failed attempt must not install dst");
}

#[test]
fn writer_holds_admission_guard_until_drop() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct GuardFlag(Arc<AtomicBool>);
    impl Drop for GuardFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let dst = dir.path().join("guarded.bin");
    let released = Arc::new(AtomicBool::new(false));
    let (file, scratch) = open_scratch(&dst).unwrap();
    let mut writer = SequentialWriter::from_parts(
        Box::new(file),
        scratch,
        dst.clone(),
        Some(Box::new(GuardFlag(released.clone()))),
    );
    writer.write_chunk(b"bytes").unwrap();
    assert!(
        !released.load(Ordering::SeqCst),
        "guard must be held across writes"
    );
    let sealed = writer.prepare_commit(5).unwrap();
    assert!(
        !released.load(Ordering::SeqCst),
        "guard must be held through the trailing finish"
    );
    sealed.commit().unwrap();
    assert!(
        released.load(Ordering::SeqCst),
        "guard must release when the sealed download is consumed"
    );
}

#[test]
fn dropped_writer_removes_its_scratch_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dst = dir.path().join("out.bin");
    let mut writer = SequentialWriter::open(&dst).expect("open writer");
    writer.write_chunk(&[7u8; 100]).expect("write final chunk");
    drop(writer);
    assert_eq!(
        std::fs::read_dir(dir.path()).expect("list dir").count(),
        0,
        "abandoned attempt must reclaim its scratch file"
    );
}
