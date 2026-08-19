package node

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "distr-scheduling/api/proto/v1"
)

const DefaultHeartbeatInterval = time.Second

type Config struct {
	ID        string
	Hostname  string
	Address   string
	TotalCPUs int
	Interval  time.Duration
}

type Agent struct {
	client pb.ControllerServiceClient
	cfg    Config
}

func Make(client pb.ControllerServiceClient, cfg Config) *Agent {
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultHeartbeatInterval
	}
	if cfg.TotalCPUs < 1 {
		cfg.TotalCPUs = 1
	}
	return &Agent{client: client, cfg: cfg}
}

func Dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func (a *Agent) Register(ctx context.Context) (string, error) {
	resp, err := a.client.RegisterNode(ctx, &pb.RegisterNodeRequest{
		Spec: &pb.NodeSpec{
			Id:        a.cfg.ID,
			Hostname:  a.cfg.Hostname,
			Address:   a.cfg.Address,
			TotalCpus: int32(a.cfg.TotalCPUs),
		},
	})
	if err != nil {
		return "", err
	}
	if resp.GetNodeId() != "" {
		a.cfg.ID = resp.GetNodeId()
	}
	return a.cfg.ID, nil
}

func (a *Agent) Heartbeat(ctx context.Context) error {
	_, err := a.client.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: a.cfg.ID})
	return err
}

// Run registers once, then sends Heartbeat RPCs until ctx is cancelled.
func (a *Agent) Run(ctx context.Context) error {
	if _, err := a.Register(ctx); err != nil {
		return err
	}
	if err := a.Heartbeat(ctx); err != nil {
		return err
	}

	tick := time.NewTicker(a.cfg.Interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			if err := a.Heartbeat(ctx); err != nil {
				if status.Code(err) == codes.NotFound {
					if _, regErr := a.Register(ctx); regErr != nil {
						return regErr
					}
					continue
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
			}
		}
	}
}
