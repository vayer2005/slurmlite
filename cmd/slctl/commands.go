// slctl subcommand implementations.
//
// Each command should:
//   - Validate CLI arguments
//   - Build the appropriate protobuf request
//   - Call the controller gRPC client
//   - Print results or errors to stdout/stderr
//
// Commands to implement:
//
//   submit [--nodes N] [--cpus-per-node N] [--time LIMIT] <command...>
//     Submit a job that requires N nodes with N CPUs each.
//     The command and its args are everything after the flags.
//
//   queue
//     List all jobs: ID, status, requested resources, assigned nodes, runtime.
//
//   cancel <job-id>
//     Cancel a pending or running job. Running jobs should be killed on nodes.
//
//   nodes
//     List all nodes: ID, status (online/offline), total CPUs, used CPUs, last heartbeat.

package main

// TODO: implement submit, queue, cancel, and nodes commands
