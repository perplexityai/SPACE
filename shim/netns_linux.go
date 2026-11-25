//go:build linux

package main

import (
	"fmt"
	"net"
	"os"
	goruntime "runtime"
	"sort"

	"golang.org/x/sys/unix"
)

func podIPFromNetns(netnsPath string) string {
	if netnsPath == "" {
		return ""
	}

	var podIP string
	if err := withNetNS(netnsPath, func() error {
		podIP = podIPFromCurrentNetns()
		return nil
	}); err != nil {
		return ""
	}

	return podIP
}

func withNetNS(netnsPath string, fn func() error) (retErr error) {
	goruntime.LockOSThread()
	unlockThread := true
	defer func() {
		if unlockThread {
			goruntime.UnlockOSThread()
		}
	}()

	current, err := os.Open(fmt.Sprintf("/proc/self/task/%d/ns/net", unix.Gettid()))
	if err != nil {
		return err
	}
	defer current.Close()

	target, err := os.Open(netnsPath)
	if err != nil {
		return err
	}
	defer target.Close()

	if err := unix.Setns(int(target.Fd()), unix.CLONE_NEWNET); err != nil {
		return err
	}
	defer func() {
		if err := unix.Setns(int(current.Fd()), unix.CLONE_NEWNET); err != nil {
			unlockThread = false
			if retErr == nil {
				retErr = fmt.Errorf("restore netns: %w", err)
			}
		}
	}()

	return fn()
}

func podIPFromCurrentNetns() string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	sort.Slice(interfaces, func(i, j int) bool { return interfaces[i].Index < interfaces[j].Index })

	for _, iface := range interfaces {
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch value := addr.(type) {
			case *net.IPNet:
				ip = value.IP
			case *net.IPAddr:
				ip = value.IP
			}

			if ip = ip.To4(); ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
				continue
			}

			return ip.String()
		}
	}

	return ""
}
