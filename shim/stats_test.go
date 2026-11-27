package main

import (
	"context"
	"io"
	"testing"

	cgroupsv2 "github.com/containerd/cgroups/v3/cgroup2/stats"
	taskAPI "github.com/containerd/containerd/api/runtime/task/v3"
	"github.com/containerd/typeurl/v2"
	pb "github.com/perplexityai/SPACE/shim/proto/sandbox/manager/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type statsClient struct {
	pb.SandboxManagerClient
	response *pb.StatsResponse
	err      error
}

func (c *statsClient) Close() error { return nil }
func (c *statsClient) Stats(context.Context, *pb.StatsRequest, ...grpc.CallOption) (*pb.StatsResponse, error) {
	return c.response, c.err
}

func TestStatsReportsAFailedSampleAsAbsentNotZero(t *testing.T) {
	sample := &pb.StatsResponse{CpuUsageUsec: 7, MemoryCurrent: 1 << 20, MemoryMax: 1 << 30, PidsCurrent: 3, PidsMax: 64}
	for _, tc := range []struct {
		name   string
		client *statsClient
		want   *cgroupsv2.Metrics
	}{
		{"sampled", &statsClient{response: sample}, &cgroupsv2.Metrics{
			CPU:    &cgroupsv2.CPUStat{UsageUsec: 7},
			Memory: &cgroupsv2.MemoryStat{Usage: 1 << 20, UsageLimit: 1 << 30},
			Pids:   &cgroupsv2.PidsStat{Current: 3, Limit: 64},
		}},
		{"without_cpu_usage", &statsClient{response: &pb.StatsResponse{MemoryCurrent: 1 << 20, MemoryMax: 1 << 30, PidsCurrent: 3}}, &cgroupsv2.Metrics{
			Memory: &cgroupsv2.MemoryStat{Usage: 1 << 20, UsageLimit: 1 << 30},
			Pids:   &cgroupsv2.PidsStat{Current: 3},
		}},
		{"rejected", &statsClient{err: status.Error(codes.ResourceExhausted, "busy")}, &cgroupsv2.Metrics{}},
		{"without_cgroup_figures", &statsClient{response: &pb.StatsResponse{NetworkTxBytes: 9}}, &cgroupsv2.Metrics{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &spaceTaskService{
				tasks: map[string]*task{"pod": {sandboxID: "01900000-0000-7000-8000-000000000001"}},
				sandboxManagerClientFactory: func() (pb.SandboxManagerClient, io.Closer, error) {
					return tc.client, tc.client, nil
				},
			}
			resp, err := svc.Stats(context.Background(), &taskAPI.StatsRequest{ID: "pod"})
			if err != nil {
				t.Fatal(err)
			}
			// Decode as containerd's CRI does.
			decoded, err := typeurl.UnmarshalAny(resp.Stats)
			if err != nil {
				t.Fatal(err)
			}
			got := decoded.(*cgroupsv2.Metrics)
			if got.String() != tc.want.String() {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}
