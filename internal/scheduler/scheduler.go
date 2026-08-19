// Gang scheduler: assigns jobs only when ALL required nodes are simultaneously available.
//
// Core scheduling loop (called periodically or on queue/node state change):
//   1. Peek at the head of the pending job queue (FIFO or priority)
//   2. Check if enough free nodes exist with sufficient CPUs per node
//   3. If yes: reserve those nodes, transition job to "running", dispatch work to each node
//   4. If no: leave job pending — do NOT partially allocate (gang scheduling guarantee)
//
// Must coordinate with:
//   - internal/job for queue access and job state transitions
//   - internal/cluster for node availability and reservation
//   - internal/scheduler/resources for accounting free vs allocated CPUs

package scheduler

import (
	"time"

	"distr-scheduling/internal/job"
)
// TODO: implement Scheduler type and scheduling loop


const SCHEDULER_INTERVAL = 1 * time.Second

type Scheduler struct {
	jobManager *job.JobManager
	
}


func (s *Scheduler) Run() {
	for {
		job, ok := s.jobManager.PeekPendingJob()
		if !ok {
			time.Sleep( SCHEDULER_INTERVAL)
			continue
		}
		
	}
}

func Make() *Scheduler {
	return &Scheduler{
		jobManager: job.Make(),
	}
}