use md5::{Digest as _, Md5};
use sha2::Sha256;

use super::*;
use crate::installs::test_support::boot_kernel;

struct Seeded {
    _dir: tempfile::TempDir,
    root: PathBuf,
    manifest: PathBuf,
    db: Connection,
}

impl Seeded {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("kernels");
        std::fs::create_dir_all(&root).unwrap();
        let db = store::open(&dir.path().join("state")).await.unwrap();
        Self {
            manifest: dir.path().join("seed.json"),
            root,
            db,
            _dir: dir,
        }
    }

    fn place(&self, version: &str, image: &[u8]) {
        std::fs::create_dir_all(self.root.join(version)).unwrap();
        std::fs::write(self.root.join(version).join(KERNEL_IMAGE), image).unwrap();
    }

    fn write_manifest(&self, manifest: &serde_json::Value) {
        std::fs::write(&self.manifest, serde_json::to_vec(manifest).unwrap()).unwrap();
    }

    fn import(&self) -> Result<SeedReport, SeedError> {
        import(
            &self.db,
            &self.root,
            crate::installs::source::host_arch().unwrap(),
            &self.manifest,
            jiff::Timestamp::now(),
        )
    }
}

/// The manifest entry for `image` as `version`, as an image build writes it.
fn entry(version: &str, image: &[u8]) -> serde_json::Value {
    serde_json::json!({
        "version": version,
        "line": { "runtime": "firecracker", "release": "1.15" },
        "source": format!(
            "s3://spec.ccfc.min/firecracker-ci/v1.15/x86_64/vmlinux-{version}?region=us-east-1&path_style=true"
        ),
        "size": image.len(),
        "md5": ContentMd5::new(Md5::digest(image).into()).to_string(),
        "sha256": Digest::sha256(Sha256::digest(image).into()).to_string(),
    })
}

fn manifest(kernels: Vec<serde_json::Value>) -> serde_json::Value {
    serde_json::json!({ "schema": SEED_MANIFEST_SCHEMA, "kernels": kernels })
}

#[tokio::test]
async fn a_listed_kernel_with_matching_bytes_becomes_space_managed() {
    let seeded = Seeded::new().await;
    let image = boot_kernel(4096, 7);
    seeded.place("6.1.155", &image);
    seeded.write_manifest(&manifest(vec![entry("6.1.155", &image)]));

    let report = seeded.import().unwrap();
    assert_eq!(report.seeded, vec!["6.1.155".parse().unwrap()]);
    assert!(report.refused.is_empty(), "{report:?}");
    let recorded = store::get_kernel_install(&seeded.db, &"6.1.155".parse().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(recorded.job_id, None);
    assert_eq!(recorded.path, seeded.root.join("6.1.155"));
    assert_eq!(
        recorded.digest,
        Digest::sha256(Sha256::digest(&image).into())
    );
    assert_eq!(
        recorded.line,
        crate::installs::test_support::fc_line("1.15")
    );

    // Every later start finds it recorded.
    let again = seeded.import().unwrap();
    assert!(again.seeded.is_empty());
    assert_eq!(again.recorded, vec!["6.1.155".parse().unwrap()]);
}

#[tokio::test]
async fn a_kernel_whose_bytes_do_not_match_stays_external() {
    let seeded = Seeded::new().await;
    let image = boot_kernel(4096, 7);
    // Other bytes, the wrong size, missing, and not a boot image.
    seeded.place("6.1.1", &boot_kernel(4096, 8));
    seeded.place("6.1.2", &boot_kernel(2048, 7));
    let not_boot = vec![0u8; 4096];
    seeded.place("6.1.4", &not_boot);
    seeded.write_manifest(&manifest(vec![
        entry("6.1.1", &image),
        entry("6.1.2", &image),
        entry("6.1.3", &image),
        entry("6.1.4", &not_boot),
    ]));

    let report = seeded.import().unwrap();
    assert!(report.seeded.is_empty(), "{report:?}");
    assert!(
        matches!(
            &report.refused[..],
            [
                SeedRefusal::Differs { .. },
                SeedRefusal::Differs { .. },
                SeedRefusal::Unmeasurable { .. },
                SeedRefusal::NotBootImage { .. },
            ]
        ),
        "{report:?}"
    );
    assert!(store::list_kernel_installs(&seeded.db).unwrap().is_empty());
}

#[tokio::test]
async fn a_recorded_kernel_with_other_bytes_is_not_overwritten() {
    let seeded = Seeded::new().await;
    let image = boot_kernel(4096, 7);
    seeded.place("6.1.155", &image);
    seeded.write_manifest(&manifest(vec![entry("6.1.155", &image)]));
    seeded.import().unwrap();
    seeded.write_manifest(&manifest(vec![entry("6.1.155", &boot_kernel(4096, 8))]));

    let report = seeded.import().unwrap();
    assert!(
        matches!(&report.refused[..], [SeedRefusal::RecordedDiffers { .. }]),
        "{report:?}"
    );
}

#[tokio::test]
async fn no_manifest_seeds_nothing_and_a_bad_one_is_an_error() {
    let seeded = Seeded::new().await;
    let report = seeded.import().unwrap();
    assert!(report.seeded.is_empty() && report.recorded.is_empty() && report.refused.is_empty());

    std::fs::write(&seeded.manifest, b"not json").unwrap();
    assert!(matches!(seeded.import(), Err(SeedError::Parse { .. })));

    seeded.write_manifest(&serde_json::json!({ "schema": 99, "kernels": [] }));
    assert!(matches!(
        seeded.import(),
        Err(SeedError::Schema { found: 99, .. })
    ));

    // Sections a later manifest adds are not this build's to read.
    seeded.write_manifest(&serde_json::json!({
        "schema": SEED_MANIFEST_SCHEMA,
        "kernels": [],
        "firecracker": [{ "version": "1.15.1" }],
    }));
    seeded.import().unwrap();
}

/// The manifest exactly as `04-firecracker.sh` prints it, for the kernel
/// directory it creates.
#[tokio::test]
async fn the_manifest_the_image_build_writes_seeds_its_kernel() {
    let seeded = Seeded::new().await;
    let image = boot_kernel(4096, 7);
    // The build strips `vmlinux-` from the object's name, as installs do.
    seeded.place("6.1.155", &image);
    let printed = format!(
        r#"{{"schema":1,"kernels":[{{"version":"{}","line":{{"runtime":"firecracker","release":"{}"}},"source":"{}/{}?{}","size":{},"md5":"{}","sha256":"sha256:{}"}}]}}"#,
        "6.1.155",
        "1.15",
        "s3://spec.ccfc.min",
        "firecracker-ci/v1.15/x86_64/vmlinux-6.1.155",
        "region=us-east-1&path_style=true",
        image.len(),
        ContentMd5::new(Md5::digest(&image).into()),
        Digest::sha256(Sha256::digest(&image).into())
            .to_string()
            .trim_start_matches("sha256:"),
    );
    std::fs::write(&seeded.manifest, printed).unwrap();

    let report = seeded.import().unwrap();
    assert_eq!(
        report.seeded,
        vec!["6.1.155".parse().unwrap()],
        "{report:?}"
    );
}
