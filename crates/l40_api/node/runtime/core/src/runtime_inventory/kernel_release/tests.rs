use super::*;

const BANNER: &[u8] =
    b"Linux version 6.1.155+ (builder@host) (gcc (GCC) 11.5.0) #1 SMP Mon Jan 1 00:00:00 UTC 2026\n";

fn image_with(banner: &[u8]) -> Vec<u8> {
    let mut image = vec![0u8; 4096];
    image.extend_from_slice(banner);
    image.extend_from_slice(&[0u8; 4096]);
    image
}

fn version(value: &str) -> Option<ArtifactVersion> {
    Some(ArtifactVersion::parse(value).unwrap())
}

#[test]
fn the_release_drops_the_modified_tree_marker() {
    assert_eq!(kernel_release(&image_with(BANNER)), version("6.1.155"));
    assert_eq!(
        kernel_release(&image_with(b"Linux version 5.10.245 (b@h) #1\n")),
        version("5.10.245")
    );
}

#[test]
fn a_banner_split_across_chunks_is_found_at_every_split() {
    let image = image_with(BANNER);
    for split in 4000..4200 {
        let mut scanner = KernelReleaseScanner::new();
        scanner.update(&image[..split]);
        scanner.update(&image[split..]);
        assert_eq!(scanner.finish(), version("6.1.155"), "split at {split}");
    }
    let mut scanner = KernelReleaseScanner::new();
    for byte in &image {
        scanner.update(std::slice::from_ref(byte));
    }
    assert_eq!(scanner.finish(), version("6.1.155"));
}

#[test]
fn strings_that_only_resemble_the_banner_are_skipped() {
    // A format string, a non-numeric release, and a release with no
    // builder after it come before the real banner.
    let mut image = image_with(b"Linux version %s (%s)\n");
    image.extend_from_slice(b"Linux version next (b@h)\n");
    image.extend_from_slice(b"Linux version 6.1.1\n");
    image.extend_from_slice(BANNER);
    assert_eq!(kernel_release(&image), version("6.1.155"));
}

#[test]
fn an_image_without_a_banner_has_no_release() {
    assert_eq!(kernel_release(&[0u8; 8192]), None);
    assert_eq!(kernel_release(b""), None);
    // The image ends inside the banner.
    assert_eq!(kernel_release(b"Linux version 6.1.155"), None);
    let overlong = format!("Linux version 6.{} (b@h)", "1".repeat(MAX_RELEASE_BYTES));
    assert_eq!(kernel_release(overlong.as_bytes()), None);
}

#[test]
fn the_window_stays_bounded() {
    let mut scanner = KernelReleaseScanner::new();
    for _ in 0..64 {
        scanner.update(&[b'L'; 1024]);
        assert!(scanner.window.len() < BANNER_PREFIX.len());
    }
    scanner.update(b"Linux version 6.1.155");
    assert!(scanner.window.len() <= BANNER_PREFIX.len() + MAX_RELEASE_BYTES + 1);
    scanner.update(b" (b@h)");
    assert_eq!(scanner.finish(), version("6.1.155"));
}
