package controller

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "distr-scheduling/api/proto/v1"
	"distr-scheduling/internal/cluster"
	"distr-scheduling/internal/node"
)

func startHeartbeatServer(t *testing.T, timeout, reapEvery time.Duration) (*cluster.Registry, pb.ControllerServiceClient, func()) {
	t.Helper()

	registry := cluster.Make()
	mon := cluster.MakeMonitor(registry, timeout, reapEvery, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go mon.Run(ctx)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcSrv := NewGRPCServer(Make(registry, mon))
	go func() { _ = grpcSrv.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		cancel()
		_ = conn.Close()
		grpcSrv.GracefulStop()
		_ = lis.Close()
	}
	return registry, pb.NewControllerServiceClient(conn), cleanup
}

func waitStatus(t *testing.T, r *cluster.Registry, id string, want cluster.Status, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		n, ok := r.Get(id)
		if ok && n.Status == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	n, _ := r.Get(id)
	t.Fatalf("node %s status = %s, want %s", id, n.Status, want)
}

func TestHeartbeatRPCTouchesRegistry(t *testing.T) {
	registry, client, cleanup := startHeartbeatServer(t, time.Second, 50*time.Millisecond)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: "n1"}); err == nil {
		t.Fatal("expected NotFound before register")
	} else if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want NotFound", status.Code(err))
	}

	reg, err := client.RegisterNode(ctx, &pb.RegisterNodeRequest{
		Spec: &pb.NodeSpec{Id: "n1", TotalCpus: 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	if reg.GetNodeId() != "n1" {
		t.Fatalf("node_id = %s", reg.GetNodeId())
	}

	before, _ := registry.Get("n1")
	resp, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.GetAcknowledged() {
		t.Fatal("expected acknowledged heartbeat")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		after, _ := registry.Get("n1")
		if after.Status == cluster.StatusOnline && after.LastHeartbeat.After(before.LastHeartbeat) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("gRPC heartbeat was not applied to the registry")
}

func TestAgentHeartbeatRPCTimeoutAndRecover(t *testing.T) {
	timeout := 80 * time.Millisecond
	registry, client, cleanup := startHeartbeatServer(t, timeout, 20*time.Millisecond)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	agent := node.Make(client, node.Config{ID: "n-grpc", TotalCPUs: 2, Interval: 20 * time.Millisecond})
	errCh := make(chan error, 1)
	go func() { errCh <- agent.Run(ctx) }()

	waitStatus(t, registry, "n-grpc", cluster.StatusOnline, time.Second)

	cancel()
	select {
	case <-errCh:
	case <-time.After(time.Second):
		t.Fatal("agent did not stop")
	}

	waitStatus(t, registry, "n-grpc", cluster.StatusOffline, time.Second)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	agent = node.Make(client, node.Config{ID: "n-grpc", TotalCPUs: 2, Interval: 20 * time.Millisecond})
	go func() { _ = agent.Run(ctx2) }()
	waitStatus(t, registry, "n-grpc", cluster.StatusOnline, time.Second)
}
