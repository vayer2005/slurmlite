// CLI client entry point (sbatch/squeue equivalent).
//
// Responsibilities:
//   - Parse global flags (controller address, default from env or config)
//   - Dispatch subcommands:
//       - submit:  submit a job (node count, CPUs, command, optional time limit)
//       - queue:   list pending and running jobs
//       - cancel:  cancel a job by ID
//       - nodes:   show cluster nodes and their status
//   - Connect to controller via gRPC and call ControllerService RPCs
//   - Format output for human-readable terminal display

package main

// TODO: implement slctl main and subcommand routing
