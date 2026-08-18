// Node agent entry point (slurmd equivalent).
//
// Responsibilities:
//   - Parse config flags (controller address, node ID, advertised CPU count, etc.)
//   - Register with the controller via RegisterNode RPC
//   - Send periodic heartbeats with current resource usage
//   - Listen for work assignments from the controller (via gRPC stream or polling)
//   - Execute assigned commands (subprocess) and report status back to controller
//   - Handle cancellation: kill subprocess when controller cancels the job
//   - Re-register on controller reconnect after network partition

package main

// TODO: implement node agent main
