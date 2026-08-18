// Node agent: controller communication and local job execution.
//
// Responsibilities:
//   - Register with controller on startup
//   - Run heartbeat loop at configured interval
//   - Receive work assignments (poll or server-stream from controller)
//   - Execute command as subprocess with assigned CPU affinity (optional for v1)
//   - Capture exit code and report completed/failed via ReportJobStatus
//   - Handle cancel: kill subprocess and report cancelled status
//   - Reconnect logic if controller is temporarily unreachable

package node

// TODO: implement NodeAgent type
