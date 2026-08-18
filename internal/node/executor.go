// Local job executor: runs assigned commands on this node.
//
// Responsibilities:
//   - Start subprocess for assigned command + args + env
//   - Track running process (PID) for cancellation
//   - Enforce max_duration timeout (kill process if exceeded)
//   - Return exit code and stderr on failure
//   - Ensure only one job runs per node at a time (or per allocated slot)
//
// For v1, a simple os/exec wrapper is sufficient.
// MPI-style multi-node jobs rely on the user's command coordinating across nodes
// (see examples/simulate.sh).

package node

// TODO: implement subprocess executor with cancel and timeout support
