package node

import (
	"context"
	"errors"
	"io"
	"log"
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
	exec   *Executor
}

func Make(client pb.ControllerServiceClient, cfg Config) *Agent {
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultHeartbeatInterval
	}
	if cfg.TotalCPUs < 1 {
		cfg.TotalCPUs = 1
	}
	return &Agent{client: client, cfg: cfg, exec: NewExecutor()}
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
	jobID, cpus := a.exec.Current()
	resp, err := a.client.Heartbeat(ctx, &pb.HeartbeatRequest{
		NodeId:        a.cfg.ID,
		AllocatedCpus: int32(cpus),
		CurrentJobId:  jobID,
	})
	if err != nil {
		return err
	}
	if cancelID := resp.GetCancelJobId(); cancelID != "" {
		a.exec.Stop(cancelID)
	}
	return nil
}

// Run registers once, then heartbeats and watches for work until ctx is cancelled.
func (a *Agent) Run(ctx context.Context) error {
	if _, err := a.Register(ctx); err != nil {
		return err
	}
	if err := a.Heartbeat(ctx); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 2)
	go func() { errCh <- a.heartbeatLoop(ctx) }()
	go func() { errCh <- a.watchLoop(ctx) }()

	err := <-errCh
	cancel()
	a.exec.Stop("")
	<-errCh
	return err
}

func (a *Agent) heartbeatLoop(ctx context.Context) error {
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
						if ctx.Err() != nil {
							return ctx.Err()
						}
						log.Printf("re-register: %v", regErr)
					}
					continue
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
				log.Printf("heartbeat: %v", err)
			}
		}
	}
}

func (a *Agent) watchLoop(ctx context.Context) error {
	backoff := 100 * time.Millisecond
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		stream, err := a.client.WatchWork(ctx, &pb.WatchWorkRequest{NodeId: a.cfg.ID})
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if status.Code(err) == codes.NotFound {
				if _, regErr := a.Register(ctx); regErr != nil && ctx.Err() == nil {
					log.Printf("re-register: %v", regErr)
				}
			} else {
				log.Printf("watch work: %v", err)
			}
			if err := sleepBackoff(ctx, backoff); err != nil {
				return err
			}
			if backoff < 2*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = 100 * time.Millisecond
		err = recvWork(ctx, stream, a)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil && !errors.Is(err, io.EOF) {
			log.Printf("watch work: %v", err)
		}
		if err := sleepBackoff(ctx, backoff); err != nil {
			return err
		}
	}
}

func recvWork(ctx context.Context, stream pb.ControllerService_WatchWorkClient, a *Agent) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		switch payload := msg.GetPayload().(type) {
		case *pb.WatchWorkResponse_Assignment:
			go a.handleAssignment(ctx, payload.Assignment)
		case *pb.WatchWorkResponse_Cancel:
			if payload.Cancel != nil {
				a.exec.Stop(payload.Cancel.GetJobId())
			}
		}
	}
}

func sleepBackoff(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (a *Agent) handleAssignment(ctx context.Context, asgn *pb.WorkAssignment) {
	if asgn == nil {
		return
	}
	jobID := asgn.GetJobId()
	a.report(ctx, jobID, pb.JobEvent_JOB_EVENT_STARTED, 0, "")
	res := a.exec.Run(ctx, asgn)
	a.report(ctx, jobID, res.Event, res.ExitCode, res.ErrMsg)
}

func (a *Agent) report(ctx context.Context, jobID string, event pb.JobEvent, exitCode int, errMsg string) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, err := a.client.ReportJobStatus(rctx, &pb.ReportJobStatusRequest{
		NodeId:       a.cfg.ID,
		JobId:        jobID,
		Event:        event,
		ExitCode:     int32(exitCode),
		ErrorMessage: errMsg,
	})
	if err != nil {
		log.Printf("report job %s: %v", jobID, err)
	}
}
