fn main() {
    // sqlx::migrate! tracks existing files, but a newly added migration must
    // also invalidate the embedded schema.
    watch("../../../../db/postgres");
    watch("../../../../db/common");
    watch("../../../../db/template-manager");
    watch("../../../../db/node-manager");
    watch("../../../../db/node-ctl");
    watch("../../../../db/volume-manager");
    watch("../../../../db/build-manager");
}

// Cargo treats a missing watched path as changed on every build, so a wrong
// path fails here instead of silently rebuilding this crate forever.
fn watch(dir: &str) {
    assert!(
        std::path::Path::new(dir).is_dir(),
        "missing migration directory {dir}"
    );
    println!("cargo:rerun-if-changed={dir}");
}
