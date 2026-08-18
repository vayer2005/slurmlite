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

// TODO: implement Scheduler type and scheduling loop
