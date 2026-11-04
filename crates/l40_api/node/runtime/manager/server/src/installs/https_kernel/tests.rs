use super::*;

#[tokio::test]
async fn listed_kernels_match_architecture_and_reject_substituted_sources() {
    let source = CloudHypervisorKernels::new().unwrap();
    let catalog = pplx_space_node_runtime_core::catalog::RuntimeCatalog::compiled().unwrap();
    let line = RuntimeLine::new(RuntimeKind::CloudHypervisor, ReleaseLine::new(54, 0));
    for (architecture, asset) in [
        (Arch::Aarch64, "Image-arm64"),
        (Arch::X86_64, "vmlinux-x86_64"),
    ] {
        let kernels = source.list(line, architecture).await.unwrap();
        assert_eq!(kernels.len(), 1);
        let kernel = &kernels[0];
        assert_eq!(kernel.architecture, architecture);
        assert!(kernel.location.to_string().ends_with(asset));
        let pin = catalog
            .kernels(
                RuntimeKind::CloudHypervisor,
                &"54.0".parse().unwrap(),
                architecture,
            )
            .find(|pin| pin.version.as_str() == "7.2.8+")
            .unwrap();
        assert_eq!(kernel.size, pin.size);
        let mut request = VerifiableKernel {
            digest: pin.sha256,
            version: kernel.version.clone(),
            line,
            architecture,
            location: kernel.location.clone(),
            size: pin.size,
            md5: None,
        };
        request.architecture = match architecture {
            Arch::Aarch64 => Arch::X86_64,
            Arch::X86_64 => Arch::Aarch64,
        };
        assert!(matches!(
            source.open(&request).await,
            Err(SourceError::Foreign { .. })
        ));
    }
    let other = RuntimeLine::new(RuntimeKind::Firecracker, ReleaseLine::new(54, 0));
    assert!(source.list(other, Arch::X86_64).await.unwrap().is_empty());
}
