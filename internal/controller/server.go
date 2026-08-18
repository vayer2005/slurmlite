// gRPC server implementation for ControllerService.
//
// Implements the RPC handlers defined in api/proto/v1/scheduler.proto:
//   - SubmitJob, ListJobs, CancelJob, ListNodes (called by slctl)
//   - RegisterNode, Heartbeat, ReportJobStatus (called by nodes)
//
// Each handler should:
//   - Validate input
//   - Delegate to job.Manager, cluster.Registry, or scheduler as appropriate
//   - Return protobuf responses or gRPC errors
//
// Work dispatch: when scheduler gang-schedules a job, server (or scheduler)
// must push WorkAssignment to each assigned node — via outbound gRPC calls
// or a node-side pull/stream mechanism.

package controller

// TODO: implement gRPC ControllerService server
