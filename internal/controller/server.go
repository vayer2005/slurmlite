package controller

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "distr-scheduling/api/proto/v1"
	"distr-scheduling/internal/cluster"
)

// MaxServerThreads (Slurm MAX_SERVER_THREADS) caps concurrent incoming RPC handlers.
const MaxServerThreads = 256

type Server struct {
	pb.UnimplementedControllerServiceServer
	registry *cluster.Registry
	monitor  *cluster.Monitor
}

func Make(registry *cluster.Registry, monitor *cluster.Monitor) *Server {
	if registry == nil {
		registry = cluster.Make()
	}
	if monitor == nil {
		monitor = cluster.MakeMonitor(registry, 0, 0, nil)
	}
	return &Server{registry: registry, monitor: monitor}
}

func NewGRPCServer(s *Server) *grpc.Server {
	gs := grpc.NewServer(grpc.MaxConcurrentStreams(uint32(MaxServerThreads)))
	pb.RegisterControllerServiceServer(gs, s)
	return gs
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

	s.registry.RegisterNode(cluster.Node{
		ID:        id,
		TotalCPUs: int(spec.GetTotalCpus()),
		Status:    cluster.StatusOnline,
	})
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
	return &pb.HeartbeatResponse{Acknowledged: true}, nil
}
