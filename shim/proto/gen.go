// Package proto holds the go:generate directive for regenerating gRPC client
// code from proto files used by the shim.
//
// Generated files land under shim/proto/ to match the proto package paths,
// keeping generated identifiers consistent.
//
// To regenerate after changing the proto:
//
//	cd shim && go generate ./proto
package proto

//go:generate protoc --go_out=. --go_opt=paths=source_relative --go_opt=Mnode/api/v1/artifact.proto=github.com/perplexityai/SPACE/shim/proto/node/api/v1 -I ../../proto ../../proto/node/api/v1/artifact.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative -I ../../proto ../../proto/net/policy/v1/policy.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative -I ../../proto ../../proto/net/gateway/v1/gateway.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative -I ../../proto ../../proto/execution/v1/execution.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative --go_opt=Mspaced/operations/v1/operations.proto=github.com/perplexityai/SPACE/shim/proto/spaced/operations/v1 -I ../../proto ../../proto/spaced/operations/v1/operations.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative --go_opt=Mnode/api/v1/artifact.proto=github.com/perplexityai/SPACE/shim/proto/node/api/v1 --go_opt=Msandbox/manager/v1/manager.proto=github.com/perplexityai/SPACE/shim/proto/sandbox/manager/v1 --go_opt=Mspaced/operations/v1/operations.proto=github.com/perplexityai/SPACE/shim/proto/spaced/operations/v1 --go_opt=Mexecution/v1/execution.proto=github.com/perplexityai/SPACE/shim/proto/execution/v1 --go_opt=Mnet/gateway/v1/gateway.proto=github.com/perplexityai/SPACE/shim/proto/net/gateway/v1 --go-grpc_out=. --go-grpc_opt=paths=source_relative --go-grpc_opt=Mnode/api/v1/artifact.proto=github.com/perplexityai/SPACE/shim/proto/node/api/v1 --go-grpc_opt=Msandbox/manager/v1/manager.proto=github.com/perplexityai/SPACE/shim/proto/sandbox/manager/v1 --go-grpc_opt=Mspaced/operations/v1/operations.proto=github.com/perplexityai/SPACE/shim/proto/spaced/operations/v1 --go-grpc_opt=Mexecution/v1/execution.proto=github.com/perplexityai/SPACE/shim/proto/execution/v1 --go-grpc_opt=Mnet/gateway/v1/gateway.proto=github.com/perplexityai/SPACE/shim/proto/net/gateway/v1 -I ../../proto ../../proto/sandbox/manager/v1/manager.proto
