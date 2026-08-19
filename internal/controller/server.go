package controller

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "distr-scheduling/api/proto/v1"
	"distr-scheduling/internal/cluster"
	"distr-scheduling/internal/job"
	"distr-scheduling/internal/scheduler"
)

// MaxServerThreads (Slurm MAX_SERVER_THREADS) caps concurrent incoming RPC handlers.
const MaxServerThreads = 256

type Server struct {
	pb.UnimplementedControllerServiceServer
	registry *cluster.Registry
	monitor  *cluster.Monitor
	jobs     *job.JobManager
	hub      *Hub
	sched    *scheduler.Scheduler
	idSeq    atomic.Uint64
}

func Make(registry *cluster.Registry, monitor *cluster.Monitor) *Server {
	return New(registry, monitor, nil, nil, nil)
}

func New(registry *cluster.Registry, monitor *cluster.Monitor, jobs *job.JobManager, hub *Hub, sched *scheduler.Scheduler) *Server {
	if registry == nil {
		registry = cluster.Make()
	}
	if jobs == nil {
		jobs = job.Make()
	}
	if hub == nil {
		hub = NewHub()
	}
	if monitor == nil {
		monitor = cluster.MakeMonitor(registry, 0, 0, Failer{Jobs: jobs, Hub: hub})
	}
	return &Server{
		registry: registry,
		monitor:  monitor,
		jobs:     jobs,
		hub:      hub,
		sched:    sched,
	}
}

func NewGRPCServer(s *Server) *grpc.Server {
	gs := grpc.NewServer(grpc.MaxConcurrentStreams(uint32(MaxServerThreads)))
	pb.RegisterControllerServiceServer(gs, s)
	return gs
}

func (s *Server) kick() {
	if s.sched != nil {
		s.sched.Kick()
	}
}

func (s *Server) RegisterNode(ctx context.Context, req *pb.RegisterNodeRequest) (*pb.RegisterNodeResponse, error) {
	spec := req.GetSpec()
	if spec == nil {
		return nil, status.Error(codes.InvalidArgument, "spec is required")
	}
	id := spec.GetId()
	if id == "" {
		id = spec.GetHostname()
	}
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "node id or hostname is required")
	}

	cpus := int(spec.GetTotalCpus())
	if cpus < 1 {
		cpus = 1
	}

	s.registry.RegisterNode(cluster.Node{
		ID:        id,
		Hostname:  spec.GetHostname(),
		Address:   spec.GetAddress(),
		TotalCPUs: cpus,
		Status:    cluster.StatusOnline,
	})
	s.kick()
	return &pb.RegisterNodeResponse{NodeId: id}, nil
}

func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	id := req.GetNodeId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if _, ok := s.registry.Get(id); !ok {
		return nil, status.Error(codes.NotFound, "node not registered")
	}
	s.monitor.Record(id)
	return &pb.HeartbeatResponse{
		Acknowledged: true,
		CancelJobId:  s.hub.PendingCancel(id),
	}, nil
}

func (s *Server) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	spec, err := specFromProto(req.GetSpec())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	id := fmt.Sprintf("job-%d", s.idSeq.Add(1))
	j, err := job.New(id, spec)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.jobs.Submit(j); err != nil {
		return nil, status.Errorf(codes.Internal, "submit: %v", err)
	}
	s.kick()
	stored, err := s.jobs.Get(id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "submit: %v", err)
	}
	return &pb.SubmitJobResponse{Job: jobToProto(stored)}, nil
}

func (s *Server) ListJobs(ctx context.Context, req *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
	var filter []job.Status
	for _, st := range req.GetStatuses() {
		if js, ok := jobStatusFromProto(st); ok {
			filter = append(filter, js)
		}
	}
	jobs := s.jobs.List(filter...)
	sortJobs(jobs)
	out := make([]*pb.Job, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, jobToProto(j))
	}
	return &pb.ListJobsResponse{Jobs: out}, nil
}

func (s *Server) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (*pb.CancelJobResponse, error) {
	id := req.GetJobId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	j, err := s.jobs.Get(id)
	if err != nil {
		if errors.Is(err, job.ErrJobNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	signal, err := s.jobs.Cancel(id)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if signal {
		_ = s.registry.ReleaseNodes(j.AssignedNodeIDs, j.Spec.Resources.CPUsPerNode)
		s.hub.NotifyCancel(j.AssignedNodeIDs, id)
		s.kick()
	}
	cancelled, err := s.jobs.Get(id)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.CancelJobResponse{Job: jobToProto(cancelled)}, nil
}

func (s *Server) ListNodes(ctx context.Context, req *pb.ListNodesRequest) (*pb.ListNodesResponse, error) {
	nodes := s.registry.ListNodes()
	out := make([]*pb.Node, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, nodeToProto(n))
	}
	return &pb.ListNodesResponse{Nodes: out}, nil
}

func (s *Server) ReportJobStatus(ctx context.Context, req *pb.ReportJobStatusRequest) (*pb.ReportJobStatusResponse, error) {
	nodeID := req.GetNodeId()
	jobID := req.GetJobId()
	if nodeID == "" || jobID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and job_id are required")
	}
	if _, ok := s.registry.Get(nodeID); !ok {
		return nil, status.Error(codes.NotFound, "node not registered")
	}

	event := req.GetEvent()
	if event == pb.JobEvent_JOB_EVENT_STARTED || event == pb.JobEvent_JOB_EVENT_UNSPECIFIED {
		return &pb.ReportJobStatusResponse{Acknowledged: true}, nil
	}

	before, err := s.jobs.Get(jobID)
	if err != nil {
		if errors.Is(err, job.ErrJobNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	failed := event == pb.JobEvent_JOB_EVENT_FAILED || event == pb.JobEvent_JOB_EVENT_CANCELLED
	if event == pb.JobEvent_JOB_EVENT_CANCELLED {
		// Controller-initiated cancel already moved the job out of running.
		s.hub.ClearCancel(nodeID, jobID)
		return &pb.ReportJobStatusResponse{Acknowledged: true}, nil
	}

	if err := s.jobs.ReportNodeEvent(jobID, nodeID, failed, int(req.GetExitCode()), req.GetErrorMessage()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	s.hub.ClearCancel(nodeID, jobID)

	after, err := s.jobs.Get(jobID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if after.Status.Terminal() && !before.Status.Terminal() {
		_ = s.registry.ReleaseNodes(after.AssignedNodeIDs, after.Spec.Resources.CPUsPerNode)
		if after.Status == job.StatusFailed {
			others := make([]string, 0, len(after.AssignedNodeIDs))
			for _, id := range after.AssignedNodeIDs {
				if id != nodeID {
					others = append(others, id)
				}
			}
			s.hub.NotifyCancel(others, jobID)
		}
		s.kick()
	}
	return &pb.ReportJobStatusResponse{Acknowledged: true}, nil
}

func (s *Server) WatchWork(req *pb.WatchWorkRequest, stream grpc.ServerStreamingServer[pb.WatchWorkResponse]) error {
	nodeID := req.GetNodeId()
	if nodeID == "" {
		return status.Error(codes.InvalidArgument, "node_id is required")
	}
	if _, ok := s.registry.Get(nodeID); !ok {
		return status.Error(codes.NotFound, "node not registered")
	}

	ch := s.hub.Subscribe(nodeID)
	defer s.hub.Unsubscribe(nodeID, ch)

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(msg); err != nil {
				return err
			}
		}
	}
}
