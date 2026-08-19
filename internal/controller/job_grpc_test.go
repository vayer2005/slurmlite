package controller

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	pb "distr-scheduling/api/proto/v1"
	"distr-scheduling/internal/cluster"
	"distr-scheduling/internal/job"
	"distr-scheduling/internal/node"
	"distr-scheduling/internal/scheduler"
)

func exampleScript(t *testing.T, name string) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	path, err := filepath.Abs(filepath.Join(filepath.Dir(file), "..", "..", "examples", name))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("example %s: %v", name, err)
	}
	return path
}

func startFullServer(t *testing.T) pb.ControllerServiceClient {
	t.Helper()

	registry := cluster.Make()
	jobs := job.Make()
	hub := NewHub()
	sched := scheduler.Make(jobs, registry, hub)
	mon := cluster.MakeMonitor(registry, time.Second, 20*time.Millisecond, Failer{Jobs: jobs, Hub: hub})

	ctx, cancel := context.WithCancel(context.Background())
	go mon.Run(ctx)
	go sched.Run(ctx)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcSrv := NewGRPCServer(New(registry, mon, jobs, hub, sched))
	go func() { _ = grpcSrv.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		_ = conn.Close()
		grpcSrv.GracefulStop()
		_ = lis.Close()
	})
	return pb.NewControllerServiceClient(conn)
}

func startAgent(t *testing.T, client pb.ControllerServiceClient, id string, cpus int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	agent := node.Make(client, node.Config{ID: id, Hostname: id, TotalCPUs: cpus, Interval: 20 * time.Millisecond})
	errCh := make(chan error, 1)
	go func() { errCh <- agent.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Errorf("agent %s did not stop", id)
		}
	})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.ListNodes(context.Background(), &pb.ListNodesRequest{})
		if err == nil {
			for _, n := range resp.GetNodes() {
				if n.GetId() == id && n.GetStatus() != pb.NodeStatus_NODE_STATUS_OFFLINE {
					return
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("agent %s did not register", id)
}

func startAgents(t *testing.T, client pb.ControllerServiceClient, n, cpus int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		startAgent(t, client, "n"+string(rune('0'+i)), cpus)
	}
}

func submitExample(t *testing.T, client pb.ControllerServiceClient, script string, args []string, nodes, cpus int, maxDur time.Duration) *pb.Job {
	t.Helper()
	spec := &pb.JobSpec{
		Command: exampleScript(t, script),
		Args:    args,
		Resources: &pb.ResourceRequest{
			NodesRequired: int32(nodes),
			CpusPerNode:   int32(cpus),
		},
	}
	if maxDur > 0 {
		spec.Resources.MaxDuration = durationpb.New(maxDur)
	}
	resp, err := client.SubmitJob(context.Background(), &pb.SubmitJobRequest{Spec: spec})
	if err != nil {
		t.Fatal(err)
	}
	j := resp.GetJob()
	if j.GetId() == "" {
		t.Fatal("empty job id")
	}
	return j
}

func waitJobStatus(t *testing.T, client pb.ControllerServiceClient, id string, want pb.JobStatus, d time.Duration) *pb.Job {
	t.Helper()
	deadline := time.Now().Add(d)
	var last *pb.Job
	for time.Now().Before(deadline) {
		resp, err := client.ListJobs(context.Background(), &pb.ListJobsRequest{})
		if err != nil {
			t.Fatal(err)
		}
		for _, j := range resp.GetJobs() {
			if j.GetId() == id {
				last = j
				if j.GetStatus() == want {
					return j
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	st := pb.JobStatus_JOB_STATUS_UNSPECIFIED
	if last != nil {
		st = last.GetStatus()
	}
	t.Fatalf("job %s status = %v, want %v", id, st, want)
	return last
}

func assertNodesFree(t *testing.T, client pb.ControllerServiceClient) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		nodes, err := client.ListNodes(context.Background(), &pb.ListNodesRequest{})
		if err != nil {
			t.Fatal(err)
		}
		free := true
		for _, n := range nodes.GetNodes() {
			if n.GetAllocatedCpus() != 0 || n.GetCurrentJobId() != "" {
				free = false
				break
			}
		}
		if free {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("nodes still reserved")
}

func TestSubmitJobRejectedWithoutCommand(t *testing.T) {
	client := startFullServer(t)

	_, err := client.SubmitJob(context.Background(), &pb.SubmitJobRequest{
		Spec: &pb.JobSpec{Resources: &pb.ResourceRequest{NodesRequired: 1, CpusPerNode: 1}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument (%v)", status.Code(err), err)
	}
}

func TestHelloCompletes(t *testing.T) {
	client := startFullServer(t)
	startAgents(t, client, 3, 1)

	j := submitExample(t, client, "hello.sh", nil, 3, 1, 0)
	got := waitJobStatus(t, client, j.GetId(), pb.JobStatus_JOB_STATUS_COMPLETED, 5*time.Second)
	if got.GetExitCode() != 0 {
		t.Fatalf("exit = %d", got.GetExitCode())
	}
	if len(got.GetAssignedNodeIds()) != 3 {
		t.Fatalf("assigned = %v", got.GetAssignedNodeIds())
	}
	assertNodesFree(t, client)
}

func TestFailReleasesNodes(t *testing.T) {
	client := startFullServer(t)
	startAgent(t, client, "n-fail", 1)

	j := submitExample(t, client, "fail.sh", nil, 1, 1, 0)
	got := waitJobStatus(t, client, j.GetId(), pb.JobStatus_JOB_STATUS_FAILED, 5*time.Second)
	if got.GetExitCode() != 1 {
		t.Fatalf("exit = %d, want 1", got.GetExitCode())
	}
	assertNodesFree(t, client)
}

func TestCancelHoldJob(t *testing.T) {
	client := startFullServer(t)
	startAgent(t, client, "n-cancel", 1)

	j := submitExample(t, client, "hold.sh", []string{"30"}, 1, 1, 0)
	waitJobStatus(t, client, j.GetId(), pb.JobStatus_JOB_STATUS_RUNNING, 5*time.Second)

	cancelled, err := client.CancelJob(context.Background(), &pb.CancelJobRequest{JobId: j.GetId()})
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.GetJob().GetStatus() != pb.JobStatus_JOB_STATUS_CANCELLED {
		t.Fatalf("status = %v", cancelled.GetJob().GetStatus())
	}
	assertNodesFree(t, client)
}

func TestTimeoutKillsJob(t *testing.T) {
	client := startFullServer(t)
	startAgent(t, client, "n-timeout", 1)

	j := submitExample(t, client, "timeout.sh", []string{"120"}, 1, 1, 300*time.Millisecond)
	got := waitJobStatus(t, client, j.GetId(), pb.JobStatus_JOB_STATUS_FAILED, 5*time.Second)
	if got.GetErrorMessage() == "" {
		t.Fatal("expected timeout error message")
	}
	assertNodesFree(t, client)
}

func TestSimulateCompletes(t *testing.T) {
	client := startFullServer(t)
	startAgents(t, client, 3, 2)

	j := submitExample(t, client, "simulate.sh", []string{"1"}, 3, 2, 0)
	got := waitJobStatus(t, client, j.GetId(), pb.JobStatus_JOB_STATUS_COMPLETED, 8*time.Second)
	if got.GetExitCode() != 0 {
		t.Fatalf("exit = %d", got.GetExitCode())
	}
	if len(got.GetAssignedNodeIds()) != 3 {
		t.Fatalf("assigned = %v", got.GetAssignedNodeIds())
	}
	assertNodesFree(t, client)
}

func TestBarrierOverlaps(t *testing.T) {
	client := startFullServer(t)
	startAgents(t, client, 3, 1)

	dir := t.TempDir()
	j := submitExample(t, client, "barrier.sh", []string{dir}, 3, 1, 0)
	got := waitJobStatus(t, client, j.GetId(), pb.JobStatus_JOB_STATUS_COMPLETED, 10*time.Second)
	if got.GetExitCode() != 0 {
		t.Fatalf("exit = %d err=%s", got.GetExitCode(), got.GetErrorMessage())
	}
	assertNodesFree(t, client)
}

func TestQueuePressureHold(t *testing.T) {
	// Same scenario as examples/queue-pressure.sh: 4 nodes, job A (3 nodes)
	// occupies the cluster so job B (2 nodes) stays pending until A finishes.
	client := startFullServer(t)
	startAgents(t, client, 4, 2)

	a := submitExample(t, client, "hold.sh", []string{"2"}, 3, 2, 0)
	waitJobStatus(t, client, a.GetId(), pb.JobStatus_JOB_STATUS_RUNNING, 5*time.Second)

	b := submitExample(t, client, "hold.sh", []string{"1"}, 2, 2, 0)
	pending := waitJobStatus(t, client, b.GetId(), pb.JobStatus_JOB_STATUS_PENDING, time.Second)
	if pending.GetStatus() != pb.JobStatus_JOB_STATUS_PENDING {
		t.Fatalf("job B status = %v, want pending while A holds 3/4 nodes", pending.GetStatus())
	}

	waitJobStatus(t, client, a.GetId(), pb.JobStatus_JOB_STATUS_COMPLETED, 8*time.Second)
	waitJobStatus(t, client, b.GetId(), pb.JobStatus_JOB_STATUS_COMPLETED, 8*time.Second)
	assertNodesFree(t, client)
}
