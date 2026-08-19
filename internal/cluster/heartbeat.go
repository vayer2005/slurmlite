package cluster

import (
	"context"
	"time"
)

const (
	DefaultHeartbeatTimeout = 5 * time.Second
	heartbeatQueueSize      = 256
)

// JobFailure fails a running job when a node is lost. ok is false if the job
// was not running (already finished, cancelled, or unknown).
type JobFailure interface {
	FailRunningJob(jobID, reason string) (assigned []string, cpusPerNode int, ok bool)
}

// Monitor fans node heartbeats into one goroutine and reaps missed beats on a ticker.
type Monitor struct {
	registry   *Registry
	jobs       JobFailure
	heartbeats chan string
	timeout    time.Duration
	interval   time.Duration
}

func MakeMonitor(registry *Registry, timeout, interval time.Duration, jobs JobFailure) *Monitor {
	if registry == nil {
		registry = Make()
	}
	if timeout <= 0 {
		timeout = DefaultHeartbeatTimeout
	}
	if interval <= 0 {
		interval = timeout / 3
		if interval < time.Millisecond {
			interval = time.Millisecond
		}
	}
	return &Monitor{
		registry:   registry,
		jobs:       jobs,
		heartbeats: make(chan string, heartbeatQueueSize),
		timeout:    timeout,
		interval:   interval,
	}
}

// Record queues a heartbeat from a node. The controller clock is applied in Run.
// Drops the beat if the queue is full; the next one will refresh liveness.
func (m *Monitor) Record(nodeID string) {
	if nodeID == "" {
		return
	}
	select {
	case m.heartbeats <- nodeID:
	default:
	}
}

func (m *Monitor) Run(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case id := <-m.heartbeats:
			m.registry.Touch(id)
		case <-ticker.C:
			m.drainHeartbeats()
			dead := m.registry.ReapStale(time.Now(), m.timeout)
			handleDeadNodes(m.registry, m.jobs, dead)
		case <-ctx.Done():
			return
		}
	}
}

func (m *Monitor) drainHeartbeats() {
	for {
		select {
		case id := <-m.heartbeats:
			m.registry.Touch(id)
		default:
			return
		}
	}
}
