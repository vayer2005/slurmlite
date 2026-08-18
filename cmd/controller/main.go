// Controller entry point (slurmctld equivalent).
//
// Responsibilities:
//   - Parse config flags (listen address, heartbeat timeout, etc.)
//   - Initialize cluster registry, job queue, and scheduler
//   - Start gRPC server exposing ControllerService
//   - Run background goroutines:
//       - Scheduler loop: poll queue, gang-schedule jobs when enough nodes are free
//       - Reaper: detect stale nodes that missed heartbeats and mark them offline
//   - Graceful shutdown: drain or cancel in-flight work on SIGINT/SIGTERM

package main

// TODO: implement controller main
