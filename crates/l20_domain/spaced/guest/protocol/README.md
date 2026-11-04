# Guest protocol

`pplx-space-spaced-guest-protocol` owns the full guest bridge schema (`Meta`,
`Networking`, `Process` and `Filesystem`), the filesystem-worker schema, and their
shared operation values, validation and payload limits. It generates the existing
protobuf packages without changing their wire names or fields.

The guest execution service consumes process/filesystem and worker contracts;
the guest daemon also consumes metadata and networking contracts. `spaced-common`
re-exports these types to preserve existing import paths and owns host/control
protocols, transport connectors and retry policy.
