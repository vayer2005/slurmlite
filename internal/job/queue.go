// Pending job queue.
//
// Responsibilities:
//   - Enqueue new jobs on submit (status = pending)
//   - Return next schedulable job (typically FIFO head)
//   - Remove or skip jobs that are cancelled while pending
//   - Thread-safe: scheduler loop and gRPC handlers access concurrently
//
// Optional future enhancements (not required for v1):
//   - Priority queue
//   - Fair-share scheduling across users

package job

// TODO: implement job queue with enqueue, dequeue/peek, and cancel-pending
