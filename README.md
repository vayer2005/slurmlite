# SlurmLite

A mini HPC job scheduler in Go. Submit a job that needs multiple machines at once. The controller waits until **all** nodes are free, assigns them together, and runs your program.

Inspired by [SLURM](https://slurm.schedmd.com/) (the scheduler used on most supercomputers)

## What It Does

You submit a job via CLI. The **controller** tracks which compute **nodes** are online and what resources they have. When enough nodes are available, it **gang-schedules** the job (all nodes or none), dispatches a command to each node, and tracks the job until it finishes or fails.

| Command             | What it does                        |
| ------------------- | ----------------------------------- |
| `slctl submit`      | Submit a job to the queue           |
| `slctl queue`       | List pending and running jobs       |
| `slctl cancel <id>` | Cancel a job                        |
| `slctl nodes`       | Show cluster nodes and their status |

Example `slctl nodes` output with four online nodes:

```
$ slctl nodes

ID      STATUS  CPUS  USED  JOB  HEARTBEAT                  HOST
node-1  online  4     0     -    2026-08-19T14:24:28-07:00  Vittals-MacBook-Air.local
node-2  online  4     0     -    2026-08-19T14:24:28-07:00  Vittals-MacBook-Air.local
node-3  online  4     0     -    2026-08-19T14:24:29-07:00  Vittals-MacBook-Air.local
node-4  online  4     0     -    2026-08-19T14:24:28-07:00  Vittals-MacBook-Air.local
```

Example `slctl queue` output:

```
$ slctl queue

ID     STATUS     NODES  CPUS  TIME  ASSIGNED              COMMAND
job-1  completed  2      1     -     node-1,node-2         /bin/echo hello
job-2  completed  3      1     -     node-1,node-2,node-3  ./examples/hello.sh
job-3  running    2      2     -     node-1,node-2         ./examples/hold.sh 15
job-4  failed     1      1     5s    node-3                ./examples/timeout.sh
job-5  failed     1      1     -     node-4                ./examples/fail.sh
```

## Quick Start

### Prerequisites

- Go 1.25+ ([install](https://go.dev/doc/install))

### Build and run a local cluster

From the repo root:

```bash
chmod +x scripts/start-cluster.sh examples/*.sh
./scripts/start-cluster.sh 4
```

This builds `controller`, `node`, and `slctl` into `bin/`, starts the controller on `127.0.0.1:50051`, and launches four node agents (`node-1` … `node-4`, 4 CPUs each). Logs land in `.cluster/logs/`.

Add the CLI to your PATH (also printed by the script):

```bash
export PATH="$PWD/bin:$PATH"
export SLURMLITE_CONTROLLER=127.0.0.1:50051
```

Stop the cluster:

```bash
./scripts/stop-cluster.sh
```

Logs are under `.cluster/logs/` (controller and per-node output from scheduled jobs).

### Manual startup (optional)

If you prefer separate terminals:

```bash
# Terminal 1 — controller
go build -o bin/controller ./cmd/controller
bin/controller -listen :50051

# Terminals 2–5 — one node each
go build -o bin/node ./cmd/node
bin/node -controller 127.0.0.1:50051 -id node-1 -cpus 4
bin/node -controller 127.0.0.1:50051 -id node-2 -cpus 4
# ...

# Any terminal — CLI
go build -o bin/slctl ./cmd/slctl
export PATH="$PWD/bin:$PATH"
```

### Sample walkthrough (example scripts)

The `examples/` directory has small shell jobs that exercise gang scheduling, env injection, timeouts, and queue behavior. Run these after the cluster is up.

**1. Multi-node hello** — prints rank and hostname on every allocated node:

```bash
slctl submit --nodes 3 --cpus-per-node 1 -- ./examples/hello.sh
slctl queue
# check node logs: grep hello .cluster/logs/node-*.log
```

**2. Hold resources** — keeps nodes busy so you can inspect the queue:

```bash
slctl submit --nodes 2 --cpus-per-node 2 -- ./examples/hold.sh 15
slctl nodes    # USED and JOB columns show the allocation
slctl queue    # status: running
```

**3. Gang barrier** — proves all ranks ran at the same time (uses `/tmp/slurmlite/<job-id>` on a shared filesystem, which is fine for a local cluster):

```bash
slctl submit --nodes 3 --cpus-per-node 1 -- ./examples/barrier.sh
grep barrier .cluster/logs/node-1.log
```

**4. Queue pressure** — submits two jobs so the second waits for the first:

```bash
./examples/queue-pressure.sh
watch -n1 slctl queue
```

**5. Failure and timeout** — job exits non-zero or exceeds `--time`:

```bash
slctl submit --nodes 1 --cpus-per-node 1 -- ./examples/fail.sh
slctl submit --nodes 1 --cpus-per-node 1 --time 5s -- ./examples/timeout.sh
slctl queue
```

Each node receives `SLURMLITE_JOB_ID`, `SLURMLITE_NODE_RANK`, `SLURMLITE_NNODES`, and `SLURMLITE_CPUS_PER_NODE` in its environment (see `examples/hello.sh`).

## Tech Stack

- Go
- gRPC + Protobuf

## Resources

- [Too Many Cooks in the Kitchen: Gang Scheduling for Predictable Performance](https://people.eecs.berkeley.edu/~kubitron/courses/cs262a-F14/projects/reports/project3_report.pdf) (UC Berkeley CS262A, Fall 2014)
- [Architecture of the Slurm Workload Manager](https://jsspp.org/papers23/JSSPP_2023_keynote_SLURM.pdf) (Jette & Wickberg, JSSPP 2023 keynote)

