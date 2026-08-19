package controller

import (
	"fmt"
	"sync"

	pb "distr-scheduling/api/proto/v1"
	"distr-scheduling/internal/job"
	"distr-scheduling/internal/scheduler"

	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	workBuf  = 16
	maxQueue = 32
)

// Hub is a per-node mailbox for WatchWork. It implements scheduler.Dispatcher.
type Hub struct {
	mu     sync.Mutex
	sub    map[string]chan *pb.WatchWorkResponse
	queue  map[string][]*pb.WatchWorkResponse
	cancel map[string]string // nodeID -> jobID to stop via heartbeat fallback
}

func NewHub() *Hub {
	return &Hub{
		sub:    make(map[string]chan *pb.WatchWorkResponse),
		queue:  make(map[string][]*pb.WatchWorkResponse),
		cancel: make(map[string]string),
	}
}

func (h *Hub) Subscribe(nodeID string) chan *pb.WatchWorkResponse {
	h.mu.Lock()
	defer h.mu.Unlock()

	if old, ok := h.sub[nodeID]; ok {
		delete(h.sub, nodeID)
		close(old)
	}

	queued := h.queue[nodeID]
	ch := make(chan *pb.WatchWorkResponse, workBuf+len(queued))
	for _, msg := range queued {
		ch <- msg
	}
	h.queue[nodeID] = nil
	h.sub[nodeID] = ch
	return ch
}

func (h *Hub) Unsubscribe(nodeID string, ch chan *pb.WatchWorkResponse) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.sub[nodeID] == ch {
		delete(h.sub, nodeID)
	}
}

func (h *Hub) Dispatch(nodeID string, a scheduler.Assignment) error {
	if nodeID == "" {
		return fmt.Errorf("node id is empty")
	}
	h.mu.Lock()
	delete(h.cancel, nodeID)
	h.mu.Unlock()

	return h.send(nodeID, &pb.WatchWorkResponse{
		Payload: &pb.WatchWorkResponse_Assignment{
			Assignment: assignmentToProto(a),
		},
	})
}

func (h *Hub) NotifyCancel(nodeIDs []string, jobID string) {
	if jobID == "" {
		return
	}
	for _, id := range nodeIDs {
		if id == "" {
			continue
		}
		h.mu.Lock()
		h.cancel[id] = jobID
		h.mu.Unlock()
		_ = h.send(id, &pb.WatchWorkResponse{
			Payload: &pb.WatchWorkResponse_Cancel{
				Cancel: &pb.CancelWork{JobId: jobID},
			},
		})
	}
}

func (h *Hub) PendingCancel(nodeID string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.cancel[nodeID]
}

func (h *Hub) ClearCancel(nodeID, jobID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cancel[nodeID] == jobID {
		delete(h.cancel, nodeID)
	}
}

func (h *Hub) send(nodeID string, msg *pb.WatchWorkResponse) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if ch, ok := h.sub[nodeID]; ok {
		select {
		case ch <- msg:
			return nil
		default:
		}
	}
	q := append(h.queue[nodeID], msg)
	if len(q) > maxQueue {
		q = q[len(q)-maxQueue:]
	}
	h.queue[nodeID] = q
	return nil
}

func assignmentToProto(a scheduler.Assignment) *pb.WorkAssignment {
	wa := &pb.WorkAssignment{
		JobId:       a.JobID,
		Command:     a.Command,
		Args:        a.Args,
		Env:         a.Env,
		NodeRank:    int32(a.NodeRank),
		Nnodes:      int32(a.NNodes),
		CpusPerNode: int32(a.CPUsPerNode),
	}
	if a.MaxDuration > 0 {
		wa.MaxDuration = durationpb.New(a.MaxDuration)
	}
	return wa
}

// Failer fails running jobs on node loss and tells remaining nodes to stop.
type Failer struct {
	Jobs *job.JobManager
	Hub  *Hub
}

func (f Failer) FailRunningJob(id, reason string) ([]string, int, bool) {
	if f.Jobs == nil {
		return nil, 0, false
	}
	ids, cpus, ok := f.Jobs.FailRunningJob(id, reason)
	if ok && f.Hub != nil {
		f.Hub.NotifyCancel(ids, id)
	}
	return ids, cpus, ok
}
