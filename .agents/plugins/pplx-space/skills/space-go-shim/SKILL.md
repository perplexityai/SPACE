---
name: space-go-shim
description: Develop and validate the Go containerd shim under `shim/` and its shared socket contract.
hooks:
  PostToolUse:
    - matcher: "Bash"
      hooks:
        - type: command
          command: "echo 'space-go-shim skill used'"
---

# Develop the SPACE Go shim

Keep the shim's `sbxMgrSocket` constant in `shim/space.go` synchronized with
Rust's `SANDBOX_MANAGER_SOCKET` in
`crates/libs/common/core/src/host/linux.rs`.

Run all Go validation from `shim/` before committing or opening a PR:

```bash
gofmt -l .
go vet ./...
go test ./...
```

Apply `$space-code-quality` when adding tests or durable comments.
