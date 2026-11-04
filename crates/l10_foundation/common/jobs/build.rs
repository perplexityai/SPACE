fn main() {
    let directory = "../../../../db/common";
    assert!(
        std::path::Path::new(directory).is_dir(),
        "missing migration directory {directory}"
    );
    println!("cargo:rerun-if-changed={directory}");
}
