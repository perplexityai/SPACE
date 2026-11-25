//go:build linux

package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// The redirect address is the first eligible address in interface index order,
// even when the default route uses another interface or a higher-index link is
// created first. The node manager's
// `external_address_is_the_shim_redirect_address` pins the same fixture.
func TestPodIPFromNetnsMatchesNodeManager(t *testing.T) {
	ns := fmt.Sprintf("space-test-shim-addr-%d", os.Getpid())
	if output, err := exec.Command("ip", "netns", "add", ns).CombinedOutput(); err != nil {
		t.Skipf("requires permission to create a network namespace: %v: %s", err, output)
	}
	t.Cleanup(func() { _ = exec.Command("ip", "netns", "delete", ns).Run() })
	ip := func(args string) {
		t.Helper()
		args = strings.ReplaceAll(args, "NS", ns)
		if output, err := exec.Command("ip", strings.Fields(args)...).CombinedOutput(); err != nil {
			t.Fatalf("ip %s: %v: %s", args, err, output)
		}
	}
	ip("-n NS link add high0 index 256 type dummy")
	ip("-n NS addr add 198.51.100.4/24 dev high0")
	ip("-n NS link set high0 up")
	for _, link := range [][2]string{{"other0", "192.0.2.9/24"}, {"eth0", "10.244.3.7/24"}} {
		ip("-n NS link add " + link[0] + " type dummy")
		ip("-n NS addr add " + link[1] + " dev " + link[0])
		ip("-n NS link set " + link[0] + " up")
	}
	ip("-n NS route add default via 10.244.3.1 dev eth0")

	if got := podIPFromNetns("/var/run/netns/" + ns); got != "192.0.2.9" {
		t.Fatalf("podIPFromNetns = %q, want 192.0.2.9", got)
	}
}
