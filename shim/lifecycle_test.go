package main

import (
	"testing"

	pb "github.com/perplexityai/SPACE/shim/proto/sandbox/manager/v1"
)

func TestObservedSandboxLifecycleRequiresKnownPresentEnum(t *testing.T) {
	for raw := int32(1); raw <= 18; raw++ {
		expected := pb.SandboxLifecycleState(raw)
		got, err := observedSandboxLifecycle(&pb.SandboxInfo{Lifecycle: &expected})
		if err != nil || got != expected {
			t.Fatalf("%s: got %v, %v", expected, got, err)
		}
	}
	for _, raw := range []int32{0, -1, 19, 2147483647} {
		value := pb.SandboxLifecycleState(raw)
		if _, err := observedSandboxLifecycle(&pb.SandboxInfo{Lifecycle: &value}); err == nil {
			t.Fatalf("accepted unknown value %d", raw)
		}
	}
	for _, observation := range []*pb.SandboxInfo{nil, {}} {
		if _, err := observedSandboxLifecycle(observation); err == nil {
			t.Fatal("accepted missing lifecycle observation")
		}
	}
}
