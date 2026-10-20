use std::path::PathBuf;

use super::*;

/// Outside [`enter_kubernetes_process`]'s child, the substrate fixture
/// refuses before touching the file `KUBECONFIG` names.
#[tokio::test(flavor = "current_thread")]
async fn kubernetes_substrate_refuses_a_kubeconfig_it_does_not_own() -> TestResult {
    let parent = tempfile::tempdir()?;
    let kubeconfig = parent.path().join("config");
    if !pplx_space_test_env::enter_test_process(&[
        ("KUBECONFIG", Some(kubeconfig.as_os_str())),
        (KUBECONFIG_DOUBLE_ENV, None),
    ]) {
        return Ok(());
    }
    let kubeconfig =
        PathBuf::from(std::env::var_os("KUBECONFIG").ok_or("the parent sets KUBECONFIG")?);
    let existing = "a developer's kubeconfig\n";
    std::fs::write(&kubeconfig, existing)?;
    // Never dialled: the fixture refuses before it uses the pool.
    let pool = PgPoolOptions::new().connect_lazy("postgres://postgres@127.0.0.1:1/unused")?;

    let Err(error) = kubernetes_substrate(&pool).await else {
        return Err("built a substrate over a kubeconfig it does not own".into());
    };

    assert!(error.to_string().contains(KUBECONFIG_DOUBLE_ENV), "{error}");
    assert_eq!(std::fs::read_to_string(&kubeconfig)?, existing);
    Ok(())
}

/// The kubeconfig double is created only as a new file directly in the
/// owned directory. A path outside it, whether named directly or through
/// `..`, a symlink in it, and an existing file in it are each refused and
/// left as they were.
#[test]
fn kubeconfig_double_is_created_only_as_a_new_file_in_its_own_directory() -> TestResult {
    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    let root = tempfile::tempdir()?;
    let owned = root.path().join("owned");
    let outside = root.path().join("outside");
    std::fs::create_dir(&owned)?;
    std::fs::create_dir(&outside)?;
    let sentinel = outside.join("sentinel");
    let sentinel_bytes = b"a file the fixture must not touch\n";
    std::fs::write(&sentinel, sentinel_bytes)?;
    let existing = owned.join("existing");
    std::fs::write(&existing, sentinel_bytes)?;
    #[cfg(unix)]
    symlink(&sentinel, owned.join("linked"))?;

    let write = |kubeconfig: &Path| {
        write_owned_kubeconfig(
            Some(kubeconfig.as_os_str()),
            Some(owned.as_os_str()),
            "apiVersion: v1\n",
        )
    };

    // (case, the path, the file it may not create)
    let refusals = [
        (
            "outside the owned directory",
            outside.join("kubeconfig"),
            true,
        ),
        (
            "outside through ..",
            owned.join("..").join("outside").join("via-parent"),
            true,
        ),
        ("an existing file", existing.clone(), false),
        #[cfg(unix)]
        ("a symlink to an outside file", owned.join("linked"), false),
    ];
    for (case, path, absent) in refusals {
        assert!(write(&path).is_err(), "{case}: written");
        if absent {
            assert!(!path.exists(), "{case}: created");
        }
    }
    assert_eq!(std::fs::read(&sentinel)?, sentinel_bytes);
    assert_eq!(std::fs::read(&existing)?, sentinel_bytes);

    let accepted = owned.join("kubeconfig");
    write(&accepted)?;
    assert_eq!(std::fs::read_to_string(&accepted)?, "apiVersion: v1\n");
    Ok(())
}
