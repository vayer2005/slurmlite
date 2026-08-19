package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	pb "distr-scheduling/api/proto/v1"

	"google.golang.org/protobuf/types/known/durationpb"
)

func runSubmit(ctx context.Context, client pb.ControllerServiceClient, args []string) error {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	nodes := fs.Int("nodes", 1, "nodes required (gang size)")
	cpus := fs.Int("cpus-per-node", 1, "CPUs reserved on each allocated node")
	limit := fs.Duration("time", 0, "optional wall-clock limit (e.g. 5s, 2m)")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: slctl submit [flags] [--] <command> [args...]\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	cmd := fs.Args()
	if len(cmd) == 0 {
		return fmt.Errorf("submit: command is required")
	}

	spec := &pb.JobSpec{
		Command: cmd[0],
		Args:    cmd[1:],
		Resources: &pb.ResourceRequest{
			NodesRequired: int32(*nodes),
			CpusPerNode:   int32(*cpus),
		},
	}
	if *limit > 0 {
		spec.Resources.MaxDuration = durationpb.New(*limit)
	}

	resp, err := client.SubmitJob(ctx, &pb.SubmitJobRequest{Spec: spec})
	if err != nil {
		return err
	}
	j := resp.GetJob()
	fmt.Printf("submitted %s  %s  nodes=%d cpus/node=%d",
		j.GetId(), jobStatusName(j.GetStatus()), *nodes, *cpus)
	if *limit > 0 {
		fmt.Printf("  time=%s", *limit)
	}
	fmt.Printf("  %s\n", formatCommand(j.GetSpec()))
	return nil
}

func runQueue(ctx context.Context, client pb.ControllerServiceClient, args []string) error {
	fs := flag.NewFlagSet("queue", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: slctl queue\n")
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	resp, err := client.ListJobs(ctx, &pb.ListJobsRequest{})
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tSTATUS\tNODES\tCPUS\tTIME\tASSIGNED\tCOMMAND")
	for _, j := range resp.GetJobs() {
		res := j.GetSpec().GetResources()
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\t%s\t%s\n",
			j.GetId(),
			jobStatusName(j.GetStatus()),
			res.GetNodesRequired(),
			res.GetCpusPerNode(),
			formatDuration(res.GetMaxDuration().AsDuration()),
			joinIDs(j.GetAssignedNodeIds()),
			formatCommand(j.GetSpec()),
		)
	}
	return w.Flush()
}

func runCancel(ctx context.Context, client pb.ControllerServiceClient, args []string) error {
	fs := flag.NewFlagSet("cancel", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: slctl cancel <job-id>\n")
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) != 1 {
		return fmt.Errorf("cancel: job id is required")
	}

	resp, err := client.CancelJob(ctx, &pb.CancelJobRequest{JobId: rest[0]})
	if err != nil {
		return err
	}
	j := resp.GetJob()
	fmt.Printf("cancelled %s  %s\n", j.GetId(), jobStatusName(j.GetStatus()))
	return nil
}

func runNodes(ctx context.Context, client pb.ControllerServiceClient, args []string) error {
	fs := flag.NewFlagSet("nodes", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: slctl nodes\n")
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	resp, err := client.ListNodes(ctx, &pb.ListNodesRequest{})
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tSTATUS\tCPUS\tUSED\tJOB\tHEARTBEAT\tHOST")
	for _, n := range resp.GetNodes() {
		hb := "-"
		if n.GetLastHeartbeat() != nil {
			hb = n.GetLastHeartbeat().AsTime().Local().Format(time.RFC3339)
		}
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\t%s\t%s\n",
			n.GetId(),
			nodeStatusName(n.GetStatus()),
			n.GetTotalCpus(),
			n.GetAllocatedCpus(),
			dash(n.GetCurrentJobId()),
			hb,
			dash(n.GetHostname()),
		)
	}
	return w.Flush()
}

func jobStatusName(s pb.JobStatus) string {
	switch s {
	case pb.JobStatus_JOB_STATUS_PENDING:
		return "pending"
	case pb.JobStatus_JOB_STATUS_RUNNING:
		return "running"
	case pb.JobStatus_JOB_STATUS_COMPLETED:
		return "completed"
	case pb.JobStatus_JOB_STATUS_FAILED:
		return "failed"
	case pb.JobStatus_JOB_STATUS_CANCELLED:
		return "cancelled"
	default:
		return "unknown"
	}
}

func nodeStatusName(s pb.NodeStatus) string {
	switch s {
	case pb.NodeStatus_NODE_STATUS_ONLINE:
		return "online"
	case pb.NodeStatus_NODE_STATUS_OFFLINE:
		return "offline"
	case pb.NodeStatus_NODE_STATUS_BUSY:
		return "busy"
	default:
		return "unknown"
	}
}

func formatCommand(spec *pb.JobSpec) string {
	if spec == nil {
		return ""
	}
	out := spec.GetCommand()
	for _, a := range spec.GetArgs() {
		out += " " + a
	}
	return out
}

func formatDuration(d time.Duration) string {
	if d <= 0 {
		return "-"
	}
	return d.String()
}

func joinIDs(ids []string) string {
	if len(ids) == 0 {
		return "-"
	}
	out := ids[0]
	for _, id := range ids[1:] {
		out += "," + id
	}
	return out
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
