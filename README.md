# SlurmLite

A mini HPC job scheduler in Go. Submit a job that needs multiple machines at once. The controller waits until **all** nodes are free, assigns them together, and runs your program.

Inspired by [SLURM](https://slurm.schedmd.com/) (the scheduler used on most supercomputers)

## What Is SLURM? (Plain English)

On a supercomputer, hundreds of machines are shared by thousands of researchers. **SLURM** is the **traffic controller** that decides who gets which machines, and when.

You don't SSH into 64 machines yourself. You say:

> "I need **4 machines** with **8 CPUs each** to run my simulation for **2 hours**."

SLURM puts your job in a **queue**. When 4 machines are available, it starts your job on all 4 **at the same time**. If it can only find 3, it **waits**. It doesn't waste 3 machines sitting idle.

**SlurmLite** is a tiny version of that.

## What It Does

You submit a job via CLI. The **controller** tracks which compute **nodes** are online and what resources they have. When enough nodes are available, it **gang-schedules** the job (all nodes or none), dispatches a command to each node, and tracks the job until it finishes or fails.



**Planned commands:**


| Command             | What it does                        |
| ------------------- | ----------------------------------- |
| `slctl submit`      | Submit a job to the queue           |
| `slctl queue`       | List pending and running jobs       |
| `slctl cancel <id>` | Cancel a job                        |
| `slctl nodes`       | Show cluster nodes and their status |




## Architecture

```
You (slctl) → Controller → Nodes (×N) → run your program
                  ↑
            heartbeats
```


| Component  | SLURM name          | Role                                       |
| ---------- | ------------------- | ------------------------------------------ |
| `slctl`    | `sbatch` / `squeue` | CLI: submit and inspect jobs               |
| Controller | `slurmctld`         | Queue, schedule, dispatch, track jobs      |
| Node       | `slurmd`            | Register, heartbeat, execute assigned work |




## Project Structure

```
distr-scheduling/
├── api/proto/v1/       # gRPC service definitions
├── cmd/
│   ├── controller/     # Central scheduler (slurmctld)
│   ├── node/           # Per-machine agent (slurmd)
│   └── slctl/          # CLI client
├── internal/
│   ├── scheduler/      # Gang scheduling, resource accounting
│   ├── job/            # Job model, queue, lifecycle
│   └── cluster/        # Node registry, heartbeats, reaper
├── examples/
│   └── simulate.sh     # Dummy MPI-style job for demos
└── scripts/
    └── start-cluster.sh
```



## Tech Stack

- Go
- gRPC + Protobuf

## Resources

- [Too Many Cooks in the Kitchen: Gang Scheduling for Predictable Performance](https://people.eecs.berkeley.edu/~kubitron/courses/cs262a-F14/projects/reports/project3_report.pdf) (UC Berkeley CS262A, Fall 2014)

