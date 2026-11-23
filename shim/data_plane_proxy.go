package main

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/containerd/containerd/v2/pkg/sys/reaper"
	"go.opentelemetry.io/otel/attribute"
)

type shellCommand interface {
	Run() error
	CombinedOutput() ([]byte, error)
}

var execShellCommand = func(name string, args ...string) shellCommand {
	return reaperShellCommand{cmd: exec.Command(name, args...)}
}

type reaperShellCommand struct {
	cmd *exec.Cmd
}

var (
	startReaperCommand = reaper.Default.Start
	waitReaperCommand  = reaper.Default.Wait
)

func (c reaperShellCommand) Run() error {
	_, err := c.CombinedOutput()
	return err
}

func (c reaperShellCommand) CombinedOutput() ([]byte, error) {
	var output bytes.Buffer
	c.cmd.Stdout = &output
	c.cmd.Stderr = &output

	exitCh, err := startReaperCommand(c.cmd)
	if err != nil {
		return output.Bytes(), err
	}
	status, err := waitReaperCommand(c.cmd, exitCh)
	if err != nil {
		return output.Bytes(), err
	}
	if status != 0 {
		return output.Bytes(), fmt.Errorf("%s exited with status %d", c.cmd.String(), status)
	}
	return output.Bytes(), nil
}

const (
	nftTable            = "space_dataplane"
	nftPreroutingChain  = "prerouting"
	nftPostroutingChain = "postrouting"
)

type dataPlaneRedirectResult struct {
	podIP string
	err   error
}

func waitForPodIP(netnsPath string, timeout time.Duration) string {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	timeoutCh := time.After(timeout)
	for {
		if podIP := podIPFromNetns(netnsPath); podIP != "" {
			return podIP
		}

		select {
		case <-timeoutCh:
			return ""
		case <-ticker.C:
		}
	}
}

// podIPForDataPlane waits for CNI to assign the pod's address in netnsPath.
// It returns "" when the pod has no network namespace or the wait times out.
func podIPForDataPlane(ctx context.Context, netnsPath string) string {
	if netnsPath == "" {
		return ""
	}
	podIPSpan, _ := startSpan(ctx, "shim.pod_ip_wait")
	podIPStartedAt := time.Now()
	podIP := waitForPodIP(netnsPath, 5*time.Second)
	podIPSpan.SetAttributes(attribute.String("pod_ip", podIP))
	var err error
	podIPResult := "success"
	if podIP == "" {
		podIPResult = shimMetricResultTimeout
		err = fmt.Errorf("pod IP was not assigned in netns %s before data-plane redirect timeout", netnsPath)
	}
	podIPSpan.End(err)
	emitShimStepLatency("create", "pod_ip_wait", podIPStartedAt, podIPResult)
	return podIP
}

// reportedPodIP is the pod address sbx-manager reports for the activation of
// podUID, so the control plane need not wait for kubelet to publish it.
func reportedPodIP(podIP string, podUID *string) *string {
	if podIP == "" || podUID == nil {
		return nil
	}
	return ptr(podIP)
}

func prepareDataPlaneRedirect(ctx context.Context, sandboxID string, netnsPath string, podIP string, template string, resultCh chan<- dataPlaneRedirectResult) {
	span, ctx := startSpan(ctx, "shim.dataplane.prepare",
		attribute.String("sandbox_id", sandboxID),
		attribute.String("template", template),
	)
	var resultErr error
	defer func() { span.End(resultErr) }()

	if netnsPath == "" {
		resultCh <- dataPlaneRedirectResult{}
		return
	}
	if podIP == "" {
		resultErr = fmt.Errorf("pod IP was not assigned in netns %s before data-plane redirect timeout", netnsPath)
		resultCh <- dataPlaneRedirectResult{
			err: resultErr,
		}
		return
	}
	span.SetAttributes(attribute.String("pod_ip", podIP))

	redirectSpan, _ := startSpan(ctx, "shim.redirect.install", attribute.String("pod_ip", podIP))
	redirectStartedAt := time.Now()
	err := addDataPlaneRedirect(netnsPath, sandboxID, podIP)
	resultErr = err
	result := shimMetricResultSuccess
	if err != nil {
		result = shimMetricResultError
	}
	redirectSpan.SetAttributes(attribute.String("result", result))
	redirectSpan.End(err)
	emitShimStepLatency("create", "redirect_install", redirectStartedAt, result, metricTag("template", template))
	resultCh <- dataPlaneRedirectResult{podIP: podIP, err: err}
}

func nftCommand(netnsPath string, args ...string) shellCommand {
	netnsName := netnsPath
	if strings.HasPrefix(netnsPath, "/var/run/netns/") {
		netnsName = filepath.Base(netnsPath)
	}
	cmdArgs := append([]string{"netns", "exec", netnsName, "nft"}, args...)
	return execShellCommand("ip", cmdArgs...)
}

func ensureDataPlaneRedirectTable(netnsPath string) error {
	if netnsPath == "" {
		return fmt.Errorf("missing netns path for nft dataplane redirect")
	}
	if err := nftCommand(netnsPath, "list", "table", "ip", nftTable).Run(); err != nil {
		if output, addErr := nftCommand(netnsPath, "add", "table", "ip", nftTable).CombinedOutput(); addErr != nil {
			return fmt.Errorf("add nft table: %w: %s", addErr, strings.TrimSpace(string(output)))
		}
	}
	if err := ensureDataPlaneNatChain(netnsPath, nftPreroutingChain, "prerouting", "dstnat"); err != nil {
		return err
	}
	if err := ensureDataPlaneNatChain(netnsPath, nftPostroutingChain, "postrouting", "srcnat"); err != nil {
		return err
	}
	return nil
}

func ensureDataPlaneNatChain(netnsPath string, chain string, hook string, priority string) error {
	if err := nftCommand(netnsPath, "list", "chain", "ip", nftTable, chain).Run(); err == nil {
		return nil
	}
	output, addErr := nftCommand(
		netnsPath,
		"add",
		"chain",
		"ip",
		nftTable,
		chain,
		"{",
		"type",
		"nat",
		"hook",
		hook,
		"priority",
		priority,
		";",
		"}",
	).CombinedOutput()
	if addErr != nil {
		return fmt.Errorf("add nft %s chain: %w: %s", chain, addErr, strings.TrimSpace(string(output)))
	}
	return nil
}

func hostIPForPod(podIP string) (string, error) {
	output, err := execShellCommand("ip", "route", "get", podIP).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("ip route get %s: %w: %s", podIP, err, strings.TrimSpace(string(output)))
	}
	fields := strings.Fields(string(output))
	for i := 0; i < len(fields)-1; i++ {
		if fields[i] == "src" && fields[i+1] != "" {
			return fields[i+1], nil
		}
	}
	return "", fmt.Errorf("no src host IP found for pod %s in route output: %s", podIP, strings.TrimSpace(string(output)))
}

func addDataPlaneRedirect(netnsPath string, sandboxID string, podIP string) error {
	if podIP == "" {
		return nil
	}
	if err := ensureDataPlaneRedirectTable(netnsPath); err != nil {
		return err
	}
	removeDataPlaneRulesInChains(netnsPath, sandboxID, nftPreroutingChain, nftPostroutingChain)
	hostIP, err := hostIPForPod(podIP)
	if err != nil {
		return err
	}
	comment := fmt.Sprintf("space-sbx-%s", sandboxID)
	output, err := nftCommand(
		netnsPath,
		"add",
		"rule",
		"ip",
		nftTable,
		nftPreroutingChain,
		"ip",
		"daddr",
		podIP,
		"tcp",
		"dport",
		fmt.Sprint(proxyPort()),
		"dnat",
		"to",
		fmt.Sprintf("%s:%d", hostIP, nodeAPIProxyTargetPort()),
		"comment",
		comment,
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf("add nft prerouting dnat: %w: %s", err, strings.TrimSpace(string(output)))
	}
	output, err = nftCommand(
		netnsPath,
		"add",
		"rule",
		"ip",
		nftTable,
		nftPostroutingChain,
		"ip",
		"daddr",
		hostIP,
		"tcp",
		"dport",
		fmt.Sprint(nodeAPIProxyTargetPort()),
		"snat",
		"to",
		podIP,
		"comment",
		comment,
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf("add nft postrouting snat: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func removeDataPlaneRedirect(netnsPath string, sandboxID string) {
	removeDataPlaneRulesInChains(
		netnsPath,
		sandboxID,
		nftPreroutingChain,
		nftPostroutingChain,
	)
}

func removeDataPlaneRulesInChains(netnsPath string, sandboxID string, chains ...string) {
	if netnsPath == "" || sandboxID == "" {
		return
	}
	comment := fmt.Sprintf("space-sbx-%s", sandboxID)
	re := regexp.MustCompile(`# handle (\d+)`)
	for _, chain := range chains {
		output, err := nftCommand(netnsPath, "-a", "list", "chain", "ip", nftTable, chain).CombinedOutput()
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(output), "\n") {
			if !strings.Contains(line, comment) {
				continue
			}
			match := re.FindStringSubmatch(line)
			if len(match) != 2 {
				continue
			}
			_, _ = nftCommand(
				netnsPath,
				"delete",
				"rule",
				"ip",
				nftTable,
				chain,
				"handle",
				match[1],
			).CombinedOutput()
		}
	}
}
